//! jinjier.art's ranking database: discovery, download, and the lists kept as playlists.
//!
//! Python origin: `src/embyx_manager/clients/jinjier.py`. The site publishes every ranking as
//! one SQLite database inside a zip served under a `.gif` name (`/<yyyymmdd>.gif`); the
//! current name is only written into the inline script of its SQL query page. The database
//! is one table, `ranks(id, kind, number, name, date, note, icon_url)`.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{Cursor, Read};
use std::sync::LazyLock;
use std::time::Duration;

use embyx_core::pytext::py_strip;
use embyx_core::pyurl::{SplitResult, UrlError, urlsplit, urlunsplit};
use regex::Regex;
use rusqlite::types::Value as SqlValue;

use crate::http::{HttpxError, decode_text};
use crate::net::{BuildError, ClientOptions, HttpClient, Request};

pub const DEFAULT_SOURCE_URL: &str = "https://jinjier.art/sql";

/// Ranking families that become playlists. Year lists are keyed by the year itself.
pub const KIND_JAVDB_CENSORED: i64 = 7;
pub const KIND_JAVLIBRARY: i64 = 5;
pub const KIND_AWARDS: i64 = 4;
pub const FIRST_YEAR_KIND: i64 = 2008;
pub const LAST_YEAR_KIND: i64 = 2100;
/// The uncensored family: not a playlist, but its codes mark uncensored entries elsewhere.
pub const KIND_UNCENSORED: i64 = 8;

/// Codes that are never regular censored JAV.
pub const EXCLUDED_BRANDS: [&str; 17] = [
    "HEYZO", "CARIB", "1PONDO", "10MU", "PACOPACO", "MKBD", "SMBD", "LAFBD", "CWPBD", "LAF", "SKYHD", "SKY", "KIRARI",
    "MCB3DBD", "MCDV", "BT", "HEYDOUGA",
];
const FC2_PREFIX: &str = "FC2-";
const UNCENSORED_TAG: &str = " 無碼 ";
const ZIP_MAGIC: &[u8] = b"PK\x03\x04";
const DATABASE_MEMBER_SUFFIX: &str = ".sqlite3";

/// `^\d{6}[_-]\d{3}$` (Python's `\d` is any Unicode decimal digit, as Rust's).
static DATE_CODE_RE: LazyLock<Regex> =
    LazyLock::new(|| embyx_core::pyre::compile(r"^\d{6}[_-]\d{3}\n?$", false, false));
/// `dbFile\s*=\s*"(\d{8})\.gif"` (`pyre` gives `\s` Python's meaning, D2).
static DB_FILE_RE: LazyLock<Regex> =
    LazyLock::new(|| embyx_core::pyre::compile(r#"dbFile\s*=\s*"(\d{8})\.gif""#, false, false));

/// `JinjierError`: the ranking source could not be read. `str(exc)` is the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JinjierError(pub String);

impl fmt::Display for JinjierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for JinjierError {}

/// One entry: its 1-based position after exclusions and de-duplication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedEntry {
    pub rank: usize,
    pub avid: String,
    pub title: String,
}

/// One list: `key` is stable across downloads (`k7`, `k2024`, `k4:<note>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedList {
    pub key: String,
    pub kind: i64,
    pub note: String,
    pub entries: Vec<RankedEntry>,
}

impl RankedList {
    /// The playlist name: the source's own list name.
    pub fn name(&self) -> &str {
        &self.note
    }
}

/// The awards family holds many small lists, so only there the note joins the key.
pub fn list_key(kind: i64, note: &str) -> String {
    if kind == KIND_AWARDS {
        format!("k{kind}:{note}")
    } else {
        format!("k{kind}")
    }
}

pub fn is_playlist_kind(kind: i64) -> bool {
    [KIND_JAVDB_CENSORED, KIND_JAVLIBRARY, KIND_AWARDS].contains(&kind)
        || (FIRST_YEAR_KIND..=LAST_YEAR_KIND).contains(&kind)
}

/// True for codes the library never holds, whatever list they appear in.
pub fn is_excluded_code(avid: &str) -> bool {
    if avid.starts_with(FC2_PREFIX) || DATE_CODE_RE.is_match(avid) {
        return true;
    }
    let brand = avid.split_once('-').map_or(avid, |(brand, _)| brand);
    EXCLUDED_BRANDS.contains(&brand)
}

/// A `ranks` row as the query returns it.
struct Row {
    kind: i64,
    /// As stored: Python reads a row's note and name only when it uses the row (D30).
    note: SqlValue,
    name: SqlValue,
}

/// A list being built: kind, the note of its first row, entries, the codes seen.
type ListBuild<'a> = (i64, &'a SqlValue, Vec<RankedEntry>, HashSet<String>);

/// A note or name that is read: Python's `str` methods fail on anything but text (D30).
fn text(value: &SqlValue) -> Result<&str, JinjierError> {
    match value {
        SqlValue::Text(text) => Ok(text),
        _ => Err(JinjierError(UNEXPECTED_TYPES.to_owned())),
    }
}

/// `f'{note}'` in the awards key: Python formats a note of any type.
fn note_key(value: &SqlValue) -> Result<String, JinjierError> {
    match value {
        SqlValue::Text(text) => Ok(text.clone()),
        SqlValue::Null => Ok("None".to_owned()),
        SqlValue::Integer(number) => Ok(number.to_string()),
        SqlValue::Real(number) => Ok(embyx_core::pyjson::float_repr(*number)),
        SqlValue::Blob(_) => Err(JinjierError(UNEXPECTED_TYPES.to_owned())),
    }
}

const UNEXPECTED_TYPES: &str = "not a readable ranking database: unexpected column types in ranks";

/// `sqlite3.Error` text: SQLite's own message.
fn sqlite_message(error: &rusqlite::Error) -> String {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message)) => message.clone(),
        other => other.to_string(),
    }
}

fn read_rows(database: &[u8]) -> Result<Vec<Row>, String> {
    let mut connection = rusqlite::Connection::open_in_memory().map_err(|error| sqlite_message(&error))?;
    // Zero bytes fail here (SQLite cannot allocate an empty image); Python's `deserialize`
    // raises `MemoryError` for them, which escapes `parse_lists` (D30).
    connection
        .deserialize_read_exact(rusqlite::MAIN_DB, database, database.len(), false)
        .map_err(|error| sqlite_message(&error))?;
    let mut statement = connection
        .prepare("SELECT kind, note, number, name FROM ranks ORDER BY kind, note, number, id")
        .map_err(|error| sqlite_message(&error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, SqlValue>(0)?,
                row.get::<_, SqlValue>(1)?,
                row.get::<_, SqlValue>(3)?,
            ))
        })
        .map_err(|error| sqlite_message(&error))?;
    let mut out = Vec::new();
    for row in rows {
        let (kind, note, name) = row.map_err(|error| sqlite_message(&error))?;
        // The site's table holds integer kinds; Python's `2007 <= kind` fails on any other
        // (D30). Notes and names are checked where they are read.
        let SqlValue::Integer(kind) = kind else {
            return Err("unexpected column types in ranks".to_owned());
        };
        out.push(Row { kind, note, name });
    }
    Ok(out)
}

/// Every list that becomes a playlist, entries in ranking order.
///
/// `avid_of` reads a code out of a title (the AVID parser), which also normalizes spellings.
/// Entries whose code cannot be read are skipped; so are uncensored ones (the source's tag,
/// membership of its uncensored list, or the code). A code seen twice keeps its first place.
pub fn parse_lists(database: &[u8], avid_of: &dyn Fn(&str) -> String) -> Result<Vec<RankedList>, JinjierError> {
    let rows =
        read_rows(database).map_err(|message| JinjierError(format!("not a readable ranking database: {message}")))?;
    let mut uncensored: HashSet<String> = HashSet::new();
    for row in rows.iter().filter(|row| row.kind == KIND_UNCENSORED) {
        let avid = code_of(text(&row.name)?, avid_of);
        if !avid.is_empty() {
            uncensored.insert(avid);
        }
    }
    let mut order: Vec<String> = Vec::new();
    let mut lists: HashMap<String, ListBuild<'_>> = HashMap::new();
    for row in &rows {
        if !is_playlist_kind(row.kind) {
            continue;
        }
        let key = list_key(row.kind, &note_key(&row.note)?);
        let (_, _, entries, seen) = lists.entry(key.clone()).or_insert_with(|| {
            order.push(key.clone());
            (row.kind, &row.note, Vec::new(), HashSet::new())
        });
        let name = text(&row.name)?;
        let avid = code_of(name, avid_of);
        if avid.is_empty() || seen.contains(&avid) {
            continue;
        }
        if name.contains(UNCENSORED_TAG) || uncensored.contains(&avid) || is_excluded_code(&avid) {
            continue;
        }
        seen.insert(avid.clone());
        entries.push(RankedEntry {
            rank: entries.len() + 1,
            avid,
            title: title_of(name),
        });
    }
    // A list that is kept needs a text note: Python would carry any other value to the
    // `playlists` table, whose `note` is `TEXT NOT NULL`, and fail there (D30).
    let mut out = Vec::new();
    for key in order {
        let Some((kind, note, entries, _)) = lists.remove(&key) else {
            continue;
        };
        if entries.is_empty() {
            continue;
        }
        out.push(RankedList {
            key,
            kind,
            note: text(note)?.to_owned(),
            entries,
        });
    }
    Ok(out)
}

/// `str.partition(' ')` on the stripped name.
fn split_name(name: &str) -> (&str, &str) {
    let stripped = py_strip(name);
    stripped.split_once(' ').unwrap_or((stripped, ""))
}

fn code_of(name: &str, avid_of: &dyn Fn(&str) -> String) -> String {
    let (head, _) = split_name(name);
    let avid = avid_of(head);
    if avid.is_empty() { avid_of(name) } else { avid }
}

fn title_of(name: &str) -> String {
    let (_, rest) = split_name(name);
    py_strip(rest).to_owned()
}

/// `JinjierClient`: reads the current database name from the query page and fetches it.
#[derive(Debug)]
pub struct JinjierClient {
    source_url: String,
    origin: String,
    client: HttpClient,
}

/// What building the client fails with.
#[derive(Debug)]
pub enum JinjierBuildError {
    /// `urlsplit(source_url)` raised `ValueError`.
    Url(UrlError),
    Client(BuildError),
}

impl fmt::Display for JinjierBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Url(error) => write!(f, "{}", error.0),
            Self::Client(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for JinjierBuildError {}

impl JinjierClient {
    /// The httpx client `JinjierClient.__init__` builds: `timeout=120.0`, redirects followed.
    pub fn client_options(timeout: Duration) -> ClientOptions {
        ClientOptions {
            timeout,
            follow_redirects: true,
            ..ClientOptions::default()
        }
    }

    /// `JinjierClient(source_url)` with its defaults.
    pub fn new(source_url: &str) -> Result<Self, JinjierBuildError> {
        Self::with_client(source_url, &Self::client_options(Duration::from_mins(2)))
    }

    pub fn with_client(source_url: &str, options: &ClientOptions) -> Result<Self, JinjierBuildError> {
        let parts = urlsplit(source_url, "").map_err(JinjierBuildError::Url)?;
        let origin = urlunsplit(&SplitResult {
            path: String::new(),
            query: String::new(),
            fragment: String::new(),
            ..parts
        });
        Ok(Self {
            source_url: source_url.to_owned(),
            origin,
            client: HttpClient::new(options).map_err(JinjierBuildError::Client)?,
        })
    }

    /// The `yyyymmdd` stamp of the database the site currently serves.
    pub async fn discover_database_name(&self) -> Result<String, JinjierError> {
        let (content_type, body) = self.get(&self.source_url).await?;
        let page = decode_text(&body, content_type.as_deref());
        DB_FILE_RE
            .captures(&page)
            .and_then(|captures| captures.get(1))
            .map(|stamp| stamp.as_str().to_owned())
            .ok_or_else(|| JinjierError(format!("{}: no database name in the page", self.source_url)))
    }

    pub fn database_url(&self, name: &str) -> String {
        format!("{}/{name}.gif", self.origin)
    }

    /// The SQLite file inside the archive published under `name`.
    pub async fn download_database(&self, name: &str) -> Result<Vec<u8>, JinjierError> {
        let url = self.database_url(name);
        let (_, body) = self.get(&url).await?;
        if !body.starts_with(ZIP_MAGIC) {
            return Err(JinjierError(format!("{url}: not a zip archive")));
        }
        let corrupt = || JinjierError(format!("{url}: corrupt zip archive"));
        let mut archive = zip::ZipArchive::new(Cursor::new(body.as_slice())).map_err(|_| corrupt())?;
        let mut member = None;
        for name in archive.file_names() {
            let name = name.map_err(|_| corrupt())?;
            if name.ends_with(DATABASE_MEMBER_SUFFIX) {
                member = Some(name.into_owned());
                break;
            }
        }
        let Some(member) = member else {
            return Err(JinjierError(format!(
                "{url}: no {DATABASE_MEMBER_SUFFIX} member in the archive"
            )));
        };
        let mut file = archive.by_name(&member).map_err(|_| corrupt())?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(|_| corrupt())?;
        Ok(data)
    }

    /// `_get`: the body (and its `Content-Type`), any `httpx.HTTPError` or error status a
    /// `JinjierError` naming the URL.
    async fn get(&self, url: &str) -> Result<(Option<String>, Vec<u8>), JinjierError> {
        let transport = |error: HttpxError| JinjierError(format!("{url}: {}: {error}", error.class_name()));
        let response = self
            .client
            .send(Request::get(url))
            .await
            .map_err(|error| transport(HttpxError::from(error)))?;
        // httpx reads the whole body inside `client.get`, so a body cut short is caught too.
        let status = response.status();
        let is_error = response.is_error();
        let content_type = response.header("content-type");
        let body = response
            .bytes()
            .await
            .map_err(|error| transport(HttpxError::from(error)))?;
        if is_error {
            return Err(JinjierError(format!("{url}: HTTP {status}")));
        }
        Ok((content_type, body.to_vec()))
    }
}

#[cfg(any(test, feature = "test-support"))]
pub mod fixtures;
#[cfg(test)]
mod tests;
