//! The front end's files: `SpaStaticFiles` over Starlette's `StaticFiles` and `FileResponse`.
//!
//! Python origin: `api.py` (`SpaStaticFiles`), `starlette/staticfiles.py` and the
//! `FileResponse` of `starlette/responses.py` (Starlette 1.6.0), with `html=True` and
//! symlinks resolved (`follow_symlink=False`). A path is normalized (`os.path.normpath`),
//! looked up under the root after `realpath` (nothing outside the root is served), and
//! answered with the file, its validators and byte ranges. A page path (not under `api/`,
//! no dot in its last segment) that finds nothing gets `index.html`, the history-API
//! fallback the React router needs.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::body::Body;
use bytes::Bytes;
use chrono::DateTime;
use embyx_core::posixpath;
use embyx_core::pyjson::float_repr;
use embyx_core::pytext::{is_py_digit, py_int, py_split, py_strip};
use http::header::{
    ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, HeaderName, HeaderValue, LAST_MODIFIED,
};
use http::{Method, StatusCode};
use md5::{Digest as _, Md5};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

use crate::http::{Answer, Internal, RequestHead, Response, detail, plain_text, with_body};
use crate::mimetypes;
use crate::routing::{absolute_url, redirect};

/// `FileResponse.max_ranges`.
const MAX_RANGES: usize = 100;

/// `app.mount('/', SpaStaticFiles(directory=frontend_dist, html=True))`.
#[derive(Debug, Clone)]
pub struct SpaStaticFiles {
    directory: Arc<PathBuf>,
    config_checked: Arc<AtomicBool>,
}

/// Why a lookup failed, as `get_response` maps it.
enum LookupError {
    /// `PermissionError`: 401.
    Permission,
    /// `ENAMETOOLONG`, or a NUL in the path (`ValueError`): 404.
    NotFound,
    /// Any other `OSError`, which nothing handles.
    Other(Internal),
}

/// What `os.stat` found.
#[derive(Debug, Clone, Copy)]
struct Stat {
    is_file: bool,
    is_dir: bool,
    size: u64,
    mtime_sec: i64,
    mtime_nsec: i64,
}

impl Stat {
    fn of(metadata: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt as _;
        Self {
            is_file: metadata.is_file(),
            is_dir: metadata.is_dir(),
            size: metadata.len(),
            mtime_sec: metadata.mtime(),
            mtime_nsec: metadata.mtime_nsec(),
        }
    }

    /// `st_mtime`: the float `CPython` makes of the two fields (`sec + nsec * 1e-9`).
    #[allow(clippy::cast_precision_loss, reason = "CPython computes the same double")]
    fn mtime(self) -> f64 {
        self.mtime_sec as f64 + self.mtime_nsec as f64 * 1e-9
    }
}

/// `StaticFiles.get_path`: `os.path.normpath(os.path.join(*route_path.split('/')))`.
pub fn get_path(route_path: &str) -> String {
    let joined = route_path
        .split('/')
        .fold(String::new(), |path, part| posixpath::join(&path, part));
    posixpath::normpath(&joined)
}

/// `SpaStaticFiles.get_response`'s test: not under `api/` and no dot in the last segment.
pub fn is_page_path(path: &str) -> bool {
    !path.starts_with("api/") && !path.rsplit('/').next().unwrap_or("").contains('.')
}

/// `posixpath.realpath(path)` (not strict): symlinks resolved as far as the path exists, the
/// rest appended as it is.
fn realpath(path: &str) -> String {
    let mut rest: Vec<Option<String>> = path.split('/').rev().map(|part| Some(part.to_owned())).collect();
    let mut part_count = rest.len();
    let mut resolved = if path.starts_with('/') {
        "/".to_owned()
    } else {
        std::env::current_dir().map_or_else(|_| "/".to_owned(), |dir| dir.to_string_lossy().into_owned())
    };
    let mut seen: std::collections::HashMap<String, Option<String>> = std::collections::HashMap::new();
    while part_count > 0 {
        let Some(name) = rest.pop() else { break };
        let Some(name) = name else {
            // A symlink's target is resolved: remember it.
            if let Some(Some(link)) = rest.pop() {
                seen.insert(link, Some(resolved.clone()));
            }
            continue;
        };
        part_count -= 1;
        if name.is_empty() || name == "." {
            continue;
        }
        if name == ".." {
            resolved = match resolved.rfind('/') {
                Some(0) | None => "/".to_owned(),
                Some(index) => resolved[..index].to_owned(),
            };
            continue;
        }
        let candidate = if resolved == "/" {
            format!("/{name}")
        } else {
            format!("{resolved}/{name}")
        };
        let Ok(metadata) = std::fs::symlink_metadata(&candidate) else {
            resolved = candidate;
            continue;
        };
        if !metadata.file_type().is_symlink() {
            resolved = candidate;
            continue;
        }
        if let Some(known) = seen.get(&candidate) {
            // A cached target, or a loop: the link is kept as it is.
            resolved = known.clone().unwrap_or(candidate);
            continue;
        }
        let Ok(target) = std::fs::read_link(&candidate) else {
            resolved = candidate;
            continue;
        };
        let target = target.to_string_lossy().into_owned();
        if target.starts_with('/') {
            "/".clone_into(&mut resolved);
        }
        seen.insert(candidate.clone(), None);
        rest.push(Some(candidate));
        rest.push(None);
        let parts: Vec<&str> = target.split('/').collect();
        part_count += parts.len();
        rest.extend(parts.into_iter().rev().map(|part| Some(part.to_owned())));
    }
    resolved
}

/// `os.path.commonpath([full_path, directory]) == directory` for two absolute, normalized
/// paths: `directory` is `full_path` or one of its ancestors, component by component.
fn is_within(full_path: &str, directory: &str) -> bool {
    let parts = |path: &str| -> Vec<String> {
        path.split('/')
            .filter(|part| !part.is_empty() && *part != ".")
            .map(str::to_owned)
            .collect()
    };
    let (full, root) = (parts(full_path), parts(directory));
    full.len() >= root.len() && full[..root.len()] == root[..]
}

impl SpaStaticFiles {
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory: Arc::new(directory),
            config_checked: Arc::new(AtomicBool::new(false)),
        }
    }

    /// `StaticFiles.check_config`, once: a root that went missing is a server error.
    fn check_config(&self) -> Result<(), Internal> {
        if self.config_checked.load(Ordering::SeqCst) {
            return Ok(());
        }
        let directory = self.directory.display();
        match std::fs::metadata(self.directory.as_path()) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(Internal(format!(
                    "RuntimeError: StaticFiles path '{directory}' is not a directory."
                )));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(Internal(format!(
                    "RuntimeError: StaticFiles directory '{directory}' does not exist."
                )));
            }
            Err(error) => return Err(Internal(format!("OSError: {error}"))),
        }
        self.config_checked.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// `StaticFiles.lookup_path(path)`: the resolved file and what `os.stat` says of it, or
    /// `None` when it does not exist or lies outside the root.
    fn lookup_path(&self, path: &str) -> Result<Option<(PathBuf, Stat)>, LookupError> {
        if path.starts_with('/') || path.starts_with('\\') {
            return Ok(None);
        }
        if path.contains('\0') {
            return Err(LookupError::NotFound);
        }
        let directory = self.directory.to_string_lossy();
        let full_path = realpath(&posixpath::join(&directory, path));
        let directory = realpath(&directory);
        if !is_within(&full_path, &directory) {
            return Ok(None);
        }
        match std::fs::metadata(&full_path) {
            Ok(metadata) => Ok(Some((PathBuf::from(full_path), Stat::of(&metadata)))),
            Err(error) => match error.kind() {
                ErrorKind::NotFound | ErrorKind::NotADirectory => Ok(None),
                ErrorKind::PermissionDenied => Err(LookupError::Permission),
                // `ENAMETOOLONG` is `InvalidFilename`.
                ErrorKind::InvalidFilename | ErrorKind::InvalidInput => Err(LookupError::NotFound),
                _ => Err(LookupError::Other(Internal(format!("OSError: {error}")))),
            },
        }
    }

    /// `StaticFiles.get_response(path, scope)`: `Ok(Err(status))` is the `HTTPException` it
    /// raises.
    async fn get_response(&self, path: &str, request: &RequestHead) -> Result<Result<Response, StatusCode>, Internal> {
        if request.method != Method::GET && request.method != Method::HEAD {
            return Ok(Err(StatusCode::METHOD_NOT_ALLOWED));
        }
        let found = match self.lookup_path(path) {
            Ok(found) => found,
            Err(LookupError::Permission) => return Ok(Err(StatusCode::UNAUTHORIZED)),
            Err(LookupError::NotFound) => return Ok(Err(StatusCode::NOT_FOUND)),
            Err(LookupError::Other(error)) => return Err(error),
        };
        if let Some((full_path, stat)) = &found {
            if stat.is_file {
                return file_response(full_path, *stat, request, StatusCode::OK, true)
                    .await
                    .map(Ok);
            }
            if stat.is_dir {
                let index_path = posixpath::join(path, "index.html");
                let index = match self.lookup_path(&index_path) {
                    Ok(index) => index,
                    Err(LookupError::Permission) => return Ok(Err(StatusCode::UNAUTHORIZED)),
                    Err(LookupError::NotFound) => return Ok(Err(StatusCode::NOT_FOUND)),
                    Err(LookupError::Other(error)) => return Err(error),
                };
                if let Some((index_path, index_stat)) = index
                    && index_stat.is_file
                {
                    if !request.path.ends_with('/') {
                        // Directory URLs always end in `/`.
                        let url = absolute_url(request, &request.path);
                        return Ok(Ok(redirect(&insert_trailing_slash(&url))));
                    }
                    return file_response(&index_path, index_stat, request, StatusCode::OK, true)
                        .await
                        .map(Ok);
                }
            }
        }
        // HTML mode: a `404.html` is served with status 404 (and no validator check).
        match self.lookup_path("404.html") {
            Ok(Some((full_path, stat))) if stat.is_file => {
                file_response(&full_path, stat, request, StatusCode::NOT_FOUND, false)
                    .await
                    .map(Ok)
            }
            Ok(_) | Err(LookupError::NotFound) => Ok(Err(StatusCode::NOT_FOUND)),
            Err(LookupError::Permission) => Ok(Err(StatusCode::UNAUTHORIZED)),
            Err(LookupError::Other(error)) => Err(error),
        }
    }

    /// The mount's answer to one request.
    pub async fn serve(&self, request: &RequestHead) -> Answer {
        self.check_config()?;
        let path = get_path(&request.path);
        let page = is_page_path(&path);
        let answer = match self.get_response(&path, request).await? {
            Err(StatusCode::NOT_FOUND) if page => self.get_response("index.html", request).await?,
            Ok(response) if response.status() == StatusCode::NOT_FOUND && page => {
                self.get_response("index.html", request).await?
            }
            other => other,
        };
        Ok(answer.unwrap_or_else(|status| detail(status, &[])))
    }
}

/// `url.replace(path=url.path + '/')`: a slash where the path ends.
fn insert_trailing_slash(url: &str) -> String {
    let after_scheme = url.find("://").map_or(0, |index| index + 3);
    let path_end = url[after_scheme..]
        .find(['?', '#'])
        .map_or(url.len(), |index| after_scheme + index);
    format!("{}/{}", &url[..path_end], &url[path_end..])
}

/// `email.utils.formatdate(timeval, usegmt=True)`: `datetime.fromtimestamp` rounds the
/// microseconds half to even, which can carry into the next second.
#[allow(clippy::cast_possible_truncation, reason = "file times are far inside i64 seconds")]
fn formatdate(timestamp: f64) -> String {
    let whole = timestamp.trunc();
    let mut micros = ((timestamp - whole) * 1e6).round_ties_even();
    let mut seconds = whole as i64;
    if micros >= 1e6 {
        micros -= 1e6;
        seconds += 1;
    } else if micros < 0.0 {
        micros += 1e6;
        seconds -= 1;
    }
    let _ = micros;
    DateTime::from_timestamp(seconds, 0)
        .map_or_else(String::new, |time| time.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
}

/// The headers `FileResponse` sets from the file's `stat`: `content-length`,
/// `last-modified`, and `etag` (`md5(str(st_mtime) + '-' + str(st_size))`).
fn stat_headers(stat: Stat) -> (String, String, String) {
    let mtime = stat.mtime();
    let etag_base = format!("{}-{}", float_repr(mtime), stat.size);
    let etag = format!("\"{}\"", hex::encode(Md5::digest(etag_base.as_bytes())));
    (stat.size.to_string(), formatdate(mtime), etag)
}

/// `FileResponse`'s content type: the guess, with `; charset=utf-8` for `text/*`.
fn content_type(path: &Path) -> String {
    let media_type = mimetypes::media_type(&path.to_string_lossy());
    if media_type.starts_with("text/") {
        format!("{media_type}; charset=utf-8")
    } else {
        media_type.to_owned()
    }
}

fn header(value: &str) -> HeaderValue {
    HeaderValue::from_str(value).expect("a valid header value")
}

/// `email.utils.parsedate(text)`: the first six fields of the time tuple (the rest is
/// always `0, 1, -1`), or `None`.
#[allow(clippy::too_many_lines, reason = "one Python function, ported line by line")]
pub fn parsedate(data: &str) -> Option<[i128; 6]> {
    const MONTHS: [&str; 24] = [
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "oct",
        "nov",
        "dec",
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ];
    const DAYS: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
    let mut data: Vec<String> = py_split(data).map(str::to_owned).collect();
    if data.is_empty() {
        return None;
    }
    if data[0].ends_with(',') || DAYS.contains(&data[0].to_lowercase().as_str()) {
        data.remove(0);
    } else if let Some(index) = data[0].rfind(',') {
        let rest = data[0][index + 1..].to_owned();
        data[0] = rest;
    }
    if data.len() == 3 {
        let stuff: Vec<String> = data[0].split('-').map(str::to_owned).collect();
        if stuff.len() == 3 {
            data = stuff.into_iter().chain(data[1..].iter().cloned()).collect();
        }
    }
    if data.len() == 4 {
        let s = data[3].clone();
        let index = s.find('+').or_else(|| s.find('-'));
        match index {
            Some(index) if index > 0 => {
                data.truncate(3);
                data.push(s[..index].to_owned());
                data.push(s[index..].to_owned());
            }
            _ => data.push(String::new()),
        }
    }
    if data.len() < 5 {
        return None;
    }
    let [mut dd, mut mm, mut yy, mut tm, mut tz]: [String; 5] = data[..5].to_vec().try_into().expect("five fields");
    if dd.is_empty() || mm.is_empty() || yy.is_empty() {
        return None;
    }
    mm = mm.to_lowercase();
    if !MONTHS.contains(&mm.as_str()) {
        (dd, mm) = (mm, dd.to_lowercase());
        if !MONTHS.contains(&mm.as_str()) {
            return None;
        }
    }
    let mut month = MONTHS.iter().position(|name| *name == mm).expect("a month") + 1;
    if month > 12 {
        month -= 12;
    }
    if dd.ends_with(',') {
        dd.pop();
    }
    if yy.find(':').is_some_and(|index| index > 0) {
        (yy, tm) = (tm, yy);
    }
    if yy.ends_with(',') {
        yy.pop();
        if yy.is_empty() {
            return None;
        }
    }
    if !yy.chars().next().is_some_and(is_py_digit) {
        (yy, tz) = (tz, yy);
    }
    let _ = tz;
    if tm.ends_with(',') {
        tm.pop();
    }
    let mut parts: Vec<&str> = tm.split(':').collect();
    if parts.len() == 1 && parts[0].contains('.') {
        parts = parts[0].split('.').collect();
        if parts.len() == 2 {
            parts.push("0");
        } else if parts.len() != 3 {
            return None;
        }
    } else if parts.len() == 2 {
        parts.push("0");
    } else if parts.len() != 3 {
        return None;
    }
    let mut year = big_int(&yy)?;
    let day = big_int(&dd)?;
    let hour = big_int(parts[0])?;
    let minute = big_int(parts[1])?;
    let second = big_int(parts[2])?;
    if year < 100 {
        year += if year > 68 { 1900 } else { 2000 };
    }
    Some([
        year,
        i128::try_from(month).expect("a month number"),
        day,
        hour,
        minute,
        second,
    ])
}

/// Python's `int(text)`, for comparisons: past `i128` the value saturates far beyond any
/// size or date; more than 4300 digits is `CPython`'s `ValueError` (the conversion limit).
fn big_int(text: &str) -> Option<i128> {
    const HUGE: i128 = 10_i128.pow(36);
    if text.chars().filter(|ch| is_py_digit(*ch)).count() > 4300 {
        return None;
    }
    match py_int(text) {
        Ok(value) => Some(value),
        Err(message) if message.starts_with("integer out of range") => {
            Some(if py_strip(text).starts_with('-') { -HUGE } else { HUGE })
        }
        Err(_) => None,
    }
}

/// What `FileResponse._parse_range_header` decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeOutcome {
    /// The ranges to send (none: the whole file).
    Ranges(Vec<(i128, i128)>),
    /// `MalformedRangeHeader(content)`: a 400 with this text.
    Malformed(&'static str),
    /// `RangeNotSatisfiable(size)`: a 416.
    NotSatisfiable,
}

/// `FileResponse._parse_range_header(http_range, file_size)`.
pub fn parse_range_header(http_range: &str, file_size: i128) -> RangeOutcome {
    let Some((units, range)) = http_range.split_once('=') else {
        return RangeOutcome::Malformed("Malformed range header.");
    };
    if py_strip(units).to_lowercase() != "bytes" {
        return RangeOutcome::Malformed("Only support bytes range");
    }
    if range.matches(',').count() + 1 > MAX_RANGES {
        return RangeOutcome::Ranges(Vec::new());
    }
    let mut ranges = Vec::new();
    for part in range.split(',') {
        let part = py_strip(part);
        if part.is_empty() || part == "-" || !part.contains('-') {
            continue;
        }
        let (start_text, end_text) = part.split_once('-').expect("a dash");
        let (start_text, end_text) = (py_strip(start_text), py_strip(end_text));
        let parsed = (|| {
            let start = if start_text.is_empty() {
                (file_size - big_int(end_text)?).max(0)
            } else {
                big_int(start_text)?
            };
            let end = if !start_text.is_empty() && !end_text.is_empty() && big_int(end_text)? < file_size {
                big_int(end_text)? + 1
            } else {
                file_size
            };
            Some((start, end))
        })();
        if let Some(pair) = parsed {
            ranges.push(pair);
        }
    }
    if ranges.is_empty() {
        return RangeOutcome::Malformed("Range header: range must be requested");
    }
    if ranges.iter().any(|(start, _)| !(0 <= *start && *start < file_size)) {
        return RangeOutcome::NotSatisfiable;
    }
    if ranges.iter().any(|(start, end)| start >= end) {
        return RangeOutcome::Malformed("Range header: start must be less than end");
    }
    if ranges.len() == 1 {
        return RangeOutcome::Ranges(ranges);
    }
    ranges.sort_unstable();
    let mut merged: Vec<(i128, i128)> = vec![ranges[0]];
    for (start, end) in ranges.into_iter().skip(1) {
        let last = merged.last_mut().expect("one range at least");
        if start <= last.1 {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    RangeOutcome::Ranges(merged)
}

/// `StaticFiles.is_not_modified`.
fn is_not_modified(request: &RequestHead, etag: &str, last_modified: &str) -> bool {
    if let Some(if_none_match) = request.header("if-none-match")
        && !if_none_match.is_empty()
    {
        return if_none_match
            .split(',')
            .any(|tag| py_strip(tag).strip_prefix("W/").unwrap_or(py_strip(tag)) == etag);
    }
    let Some(if_modified_since) = request.header("if-modified-since") else {
        return false;
    };
    match (parsedate(&if_modified_since), parsedate(last_modified)) {
        (Some(since), Some(modified)) => since >= modified,
        _ => false,
    }
}

/// Read `length` bytes of a file from `start`.
async fn read_range(path: &Path, start: u64, length: u64) -> Result<Vec<u8>, Internal> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| Internal(format!("OSError: {error}")))?;
    file.seek(std::io::SeekFrom::Start(start))
        .await
        .map_err(|error| Internal(format!("OSError: {error}")))?;
    let mut buffer = Vec::new();
    file.take(length)
        .read_to_end(&mut buffer)
        .await
        .map_err(|error| Internal(format!("OSError: {error}")))?;
    Ok(buffer)
}

/// `secrets.token_hex(13)`.
fn token_hex() -> String {
    let mut bytes = [0_u8; 13];
    getrandom::fill(&mut bytes).expect("the system's random source");
    hex::encode(bytes)
}

/// `StaticFiles.file_response` (`check_modified`) or a bare `FileResponse`: the file, a
/// 304, or the ranges asked for.
#[allow(
    clippy::too_many_lines,
    reason = "Starlette's response, kept in one piece to follow it"
)]
async fn file_response(
    path: &Path,
    stat: Stat,
    request: &RequestHead,
    status: StatusCode,
    check_modified: bool,
) -> Result<Response, Internal> {
    let (content_length, last_modified, etag) = stat_headers(stat);
    if check_modified && is_not_modified(request, &etag, &last_modified) {
        // `NotModifiedResponse`: of the file's headers only `etag` is one it keeps.
        let mut response = with_body(StatusCode::NOT_MODIFIED, None, Bytes::new());
        response.headers_mut().insert(ETAG, header(&etag));
        return Ok(response);
    }
    let media_type = content_type(path);
    let file_headers: [(HeaderName, String); 5] = [
        (CONTENT_TYPE, media_type.clone()),
        (ACCEPT_RANGES, "bytes".to_owned()),
        (CONTENT_LENGTH, content_length),
        (LAST_MODIFIED, last_modified.clone()),
        (ETAG, etag.clone()),
    ];
    let build = |status: StatusCode, body: Body, replace: &[(HeaderName, String)]| {
        let mut response = http::Response::new(body);
        *response.status_mut() = status;
        for (name, value) in file_headers.iter().chain(replace) {
            response.headers_mut().insert(name.clone(), header(value));
        }
        response
    };
    let head = request.method == Method::HEAD;
    let size = i128::from(stat.size);
    let http_range = request.header("range");
    let use_range = http_range.is_some()
        && request
            .header("if-range")
            .is_none_or(|if_range| if_range == last_modified || if_range == etag);
    let ranges = match http_range.filter(|_| use_range) {
        None => Vec::new(),
        Some(http_range) => match parse_range_header(&http_range, size) {
            RangeOutcome::Ranges(ranges) => ranges,
            RangeOutcome::Malformed(content) => return Ok(plain_text(StatusCode::BAD_REQUEST, content)),
            RangeOutcome::NotSatisfiable => {
                let mut response = plain_text(StatusCode::RANGE_NOT_SATISFIABLE, "");
                response
                    .headers_mut()
                    .insert(CONTENT_RANGE, header(&format!("bytes */{size}")));
                return Ok(response);
            }
        },
    };
    let to_u64 = |value: i128| u64::try_from(value).expect("a range inside the file");
    match ranges.as_slice() {
        [] => {
            let body = if head {
                Body::empty()
            } else {
                Body::from(read_range(path, 0, stat.size).await?)
            };
            Ok(build(status, body, &[]))
        }
        [(first, end)] => {
            let body = if head {
                Body::empty()
            } else {
                Body::from(read_range(path, to_u64(*first), to_u64(end - *first)).await?)
            };
            Ok(build(
                StatusCode::PARTIAL_CONTENT,
                body,
                &[
                    (CONTENT_RANGE, format!("bytes {first}-{}/{size}", end - 1)),
                    (CONTENT_LENGTH, (end - *first).to_string()),
                ],
            ))
        }
        ranges => {
            let boundary = token_hex();
            let static_part = 49 + boundary.len() + media_type.len() + size.to_string().len();
            let content_length: i128 = ranges
                .iter()
                .map(|(start, end)| {
                    let digits = start.to_string().len() + (end - 1).to_string().len();
                    i128::try_from(digits + static_part).expect("small") + (end - start)
                })
                .sum::<i128>()
                + i128::try_from(4 + boundary.len()).expect("small");
            let body = if head {
                Body::empty()
            } else {
                let mut body = Vec::new();
                for (start, end) in ranges {
                    body.extend_from_slice(
                        format!(
                            "--{boundary}\r\nContent-Type: {media_type}\r\nContent-Range: bytes {start}-{}/{size}\r\n\r\n",
                            end - 1
                        )
                        .as_bytes(),
                    );
                    body.extend(read_range(path, to_u64(*start), to_u64(end - start)).await?);
                    body.extend_from_slice(b"\r\n");
                }
                body.extend_from_slice(format!("--{boundary}--").as_bytes());
                Body::from(body)
            };
            Ok(build(
                StatusCode::PARTIAL_CONTENT,
                body,
                &[
                    (CONTENT_TYPE, format!("multipart/byteranges; boundary={boundary}")),
                    (CONTENT_LENGTH, content_length.to_string()),
                ],
            ))
        }
    }
}

#[cfg(test)]
mod tests;
