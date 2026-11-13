//! The `urllib.parse` functions sources use on scraped links and URL-quoted fields:
//! `quote`, `unquote`, `urlsplit` (with `SplitResult.hostname`), `urlunsplit`, `urljoin`.
//!
//! Python origin: `urllib/parse.py` of `CPython` 3.12. These work on text the way Python does
//! (no normalisation, no percent-encoding, no lowercasing of the host), which the `url`
//! crate would not reproduce: a joined URL or a decoded wiki title ends up in file names
//! and stored rows. The `pyurl` goldens replay the same inputs through Python.
//!
//! Copied from fav (`fav-core/src/pyurl.rs`); see docs/CONVENTIONS.md, "Code copied from fav".

use std::fmt;
use std::fmt::Write as _;

use unicode_normalization::UnicodeNormalization;

/// `ValueError` from `urlsplit`: a malformed bracketed host or a netloc that NFKC
/// normalisation would change into URL syntax.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlError(pub String);

impl fmt::Display for UrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UrlError {}

/// `urllib.parse.unquote(text)`: `%xx` escapes decoded as UTF-8, invalid sequences
/// replaced with U+FFFD. Runs of non-ASCII text are kept as they are.
pub fn unquote(text: &str) -> String {
    if !text.contains('%') {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while !rest.is_empty() {
        let ascii_len = rest.bytes().take_while(u8::is_ascii).count();
        if ascii_len == 0 {
            let non_ascii = rest.find(|ch: char| ch.is_ascii()).unwrap_or(rest.len());
            out.push_str(&rest[..non_ascii]);
            rest = &rest[non_ascii..];
            continue;
        }
        let bytes = unquote_to_bytes(&rest.as_bytes()[..ascii_len]);
        out.push_str(&String::from_utf8_lossy(&bytes));
        rest = &rest[ascii_len..];
    }
    out
}

/// `_unquote_impl` on an ASCII run.
fn unquote_to_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && let (Some(high), Some(low)) = (bytes.get(index + 1), bytes.get(index + 2))
            && let (Some(high), Some(low)) = (hex_value(*high), hex_value(*low))
        {
            out.push(high * 16 + low);
            index += 3;
            continue;
        }
        out.push(bytes[index]);
        index += 1;
    }
    out
}

fn hex_value(byte: u8) -> Option<u8> {
    char::from(byte).to_digit(16).and_then(|value| u8::try_from(value).ok())
}

/// `urlsplit(...)`'s five parts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SplitResult {
    pub scheme: String,
    pub netloc: String,
    pub path: String,
    pub query: String,
    pub fragment: String,
}

const USES_RELATIVE: [&str; 20] = [
    "", "ftp", "http", "gopher", "nntp", "imap", "wais", "file", "https", "shttp", "mms", "prospero", "rtsp", "rtsps",
    "rtspu", "sftp", "svn", "svn+ssh", "ws", "wss",
];

const USES_NETLOC: [&str; 27] = [
    "",
    "ftp",
    "http",
    "gopher",
    "nntp",
    "telnet",
    "imap",
    "wais",
    "file",
    "mms",
    "https",
    "shttp",
    "snews",
    "prospero",
    "rtsp",
    "rtsps",
    "rtspu",
    "rsync",
    "svn",
    "svn+ssh",
    "sftp",
    "nfs",
    "git",
    "git+ssh",
    "ws",
    "wss",
    "itms-services",
];

const USES_PARAMS: [&str; 16] = [
    "", "ftp", "hdl", "prospero", "http", "imap", "https", "shttp", "rtsp", "rtsps", "rtspu", "sip", "sips", "mms",
    "sftp", "tel",
];

fn is_scheme_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.')
}

/// `urlsplit(url, scheme)` with `allow_fragments=True`.
pub fn urlsplit(url: &str, default_scheme: &str) -> Result<SplitResult, UrlError> {
    let c0_or_space = |ch: char| ch <= ' ';
    let mut url: String = url.trim_start_matches(c0_or_space).replace(['\t', '\r', '\n'], "");
    let mut scheme: String = default_scheme.trim_matches(c0_or_space).replace(['\t', '\r', '\n'], "");
    if let Some(colon) = url.find(':')
        && colon > 0
        && url.starts_with(|ch: char| ch.is_ascii_alphabetic())
        && url[..colon].chars().all(is_scheme_char)
    {
        scheme = url[..colon].to_lowercase();
        url = url[colon + 1..].to_owned();
    }
    let mut netloc = String::new();
    if url.starts_with("//") {
        let delim = url[2..].find(['/', '?', '#']).map_or(url.len(), |offset| offset + 2);
        url[2..delim].clone_into(&mut netloc);
        url = url[delim..].to_owned();
        let open = netloc.contains('[');
        let close = netloc.contains(']');
        if open != close {
            return Err(UrlError("Invalid IPv6 URL".to_owned()));
        }
        if open && close {
            check_bracketed_netloc(&netloc)?;
        }
    }
    let mut fragment = String::new();
    if let Some((before, after)) = url.split_once('#') {
        after.clone_into(&mut fragment);
        url = before.to_owned();
    }
    let mut query = String::new();
    if let Some((before, after)) = url.split_once('?') {
        after.clone_into(&mut query);
        url = before.to_owned();
    }
    check_netloc(&netloc)?;
    Ok(SplitResult {
        scheme,
        netloc,
        path: url,
        query,
        fragment,
    })
}

/// `_check_bracketed_netloc`.
fn check_bracketed_netloc(netloc: &str) -> Result<(), UrlError> {
    let host_and_port = netloc.rsplit_once('@').map_or(netloc, |(_, host)| host);
    let hostname = match host_and_port.split_once('[') {
        Some((before, bracketed)) => {
            if !before.is_empty() {
                return Err(UrlError("Invalid IPv6 URL".to_owned()));
            }
            let (hostname, port) = bracketed.split_once(']').unwrap_or((bracketed, ""));
            if !port.is_empty() && !port.starts_with(':') {
                return Err(UrlError("Invalid IPv6 URL".to_owned()));
            }
            hostname
        }
        None => host_and_port.split_once(':').map_or(host_and_port, |(host, _)| host),
    };
    check_bracketed_host(hostname)
}

/// `_check_bracketed_host`: an `IPvFuture` literal or an IPv6 address.
fn check_bracketed_host(hostname: &str) -> Result<(), UrlError> {
    if let Some(rest) = hostname.strip_prefix('v') {
        // `\Av[a-fA-F0-9]+\..+\Z`
        let hex = rest.bytes().take_while(u8::is_ascii_hexdigit).count();
        let tail = &rest[hex..];
        let valid = hex > 0 && tail.starts_with('.') && tail.len() > 1 && !tail[1..].contains('\n');
        return if valid {
            Ok(())
        } else {
            Err(UrlError("IPvFuture address is invalid".to_owned()))
        };
    }
    if hostname.parse::<std::net::Ipv4Addr>().is_ok() {
        return Err(UrlError("An IPv4 address cannot be in brackets".to_owned()));
    }
    // `ipaddress.ip_address` also takes a `%scope` suffix on IPv6.
    let address = hostname.split_once('%').map_or(
        hostname,
        |(address, scope)| if scope.is_empty() { hostname } else { address },
    );
    if address.parse::<std::net::Ipv6Addr>().is_ok() {
        Ok(())
    } else {
        // `{address!r}`; hosts with quotes or escapes in them are not worth a full `repr`.
        Err(UrlError(format!(
            "'{hostname}' does not appear to be an IPv4 or IPv6 address"
        )))
    }
}

/// `_checknetloc`: reject a netloc whose NFKC form gains `/ ? # @ :`.
fn check_netloc(netloc: &str) -> Result<(), UrlError> {
    if netloc.is_empty() || netloc.is_ascii() {
        return Ok(());
    }
    let stripped: String = netloc
        .chars()
        .filter(|ch| !matches!(ch, '@' | ':' | '#' | '?'))
        .collect();
    let normalized: String = stripped.nfkc().collect();
    if normalized == stripped {
        return Ok(());
    }
    if normalized.contains(['/', '?', '#', '@', ':']) {
        return Err(UrlError(format!(
            "netloc '{netloc}' contains invalid characters under NFKC normalization"
        )));
    }
    Ok(())
}

/// `urlparse`'s split of `;params` off the last path segment.
fn split_params(path: &str) -> (&str, &str) {
    let index = match path.rfind('/') {
        Some(slash) => match path[slash..].find(';') {
            Some(offset) => slash + offset,
            None => return (path, ""),
        },
        None => match path.find(';') {
            Some(index) => index,
            None => return (path, ""),
        },
    };
    (&path[..index], &path[index + 1..])
}

/// `urlparse(url, scheme)`: [`urlsplit`] plus the params of schemes that use them.
pub fn urlparse(url: &str, default_scheme: &str) -> Result<(SplitResult, String), UrlError> {
    let mut split = urlsplit(url, default_scheme)?;
    let mut params = String::new();
    if USES_PARAMS.contains(&split.scheme.as_str()) && split.path.contains(';') {
        let (path, found) = split_params(&split.path);
        let (path, found) = (path.to_owned(), found.to_owned());
        split.path = path;
        params = found;
    }
    Ok((split, params))
}

impl SplitResult {
    /// `_userinfo`: the user and password before the last `@` of the netloc.
    fn userinfo(&self) -> (Option<&str>, Option<&str>) {
        match self.netloc.rsplit_once('@') {
            Some((userinfo, _)) => match userinfo.split_once(':') {
                Some((user, password)) => (Some(user), Some(password)),
                None => (Some(userinfo), None),
            },
            None => (None, None),
        }
    }

    /// `SplitResult.username`.
    pub fn username(&self) -> Option<&str> {
        self.userinfo().0
    }

    /// `SplitResult.password`.
    pub fn password(&self) -> Option<&str> {
        self.userinfo().1
    }

    /// `SplitResult.port`: `None` without one; Python's `ValueError` text for a port that is
    /// not ASCII digits or is beyond 65535.
    pub fn port(&self) -> Result<Option<u16>, UrlError> {
        let host_info = self
            .netloc
            .rsplit_once('@')
            .map_or(self.netloc.as_str(), |(_, host)| host);
        let port = match host_info.split_once('[') {
            Some((_, bracketed)) => bracketed
                .split_once(']')
                .map_or("", |(_, rest)| rest.split_once(':').map_or("", |(_, port)| port)),
            None => host_info.split_once(':').map_or("", |(_, port)| port),
        };
        if port.is_empty() {
            return Ok(None);
        }
        if !port.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(UrlError(format!(
                "Port could not be cast to integer value as {}",
                crate::pytext::py_repr(port)
            )));
        }
        let value = port.trim_start_matches('0');
        match value.parse::<u32>() {
            Ok(number) if number <= 65_535 => Ok(Some(u16::try_from(number).unwrap_or(u16::MAX))),
            _ if value.is_empty() => Ok(Some(0)),
            _ => Err(UrlError("Port out of range 0-65535".to_owned())),
        }
    }

    /// `SplitResult.hostname`: the host without user info, brackets or port, lowercased
    /// (an IPv6 zone keeps its case); `None` when empty.
    pub fn hostname(&self) -> Option<String> {
        let host_info = self
            .netloc
            .rsplit_once('@')
            .map_or(self.netloc.as_str(), |(_, host)| host);
        let hostname = match host_info.split_once('[') {
            Some((_, bracketed)) => bracketed.split_once(']').map_or(bracketed, |(host, _)| host),
            None => host_info.split_once(':').map_or(host_info, |(host, _)| host),
        };
        if hostname.is_empty() {
            return None;
        }
        Some(match hostname.split_once('%') {
            Some((host, zone)) => format!("{}%{zone}", host.to_lowercase()),
            None => hostname.to_lowercase(),
        })
    }
}

/// `quote(text, safe=safe)`: UTF-8 bytes outside letters, digits, `_.-~` and `safe` become
/// `%XX` (upper-case hex).
pub fn quote(text: &str, safe: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-' | '~') || (ch.is_ascii() && safe.contains(ch)) {
            out.push(ch);
        } else {
            let mut buffer = [0_u8; 4];
            for byte in ch.encode_utf8(&mut buffer).bytes() {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

/// `urlunsplit(...)`.
pub fn urlunsplit(parts: &SplitResult) -> String {
    let mut url = parts.path.clone();
    if !parts.netloc.is_empty() {
        if !url.is_empty() && !url.starts_with('/') {
            url.insert(0, '/');
        }
        url = format!("//{}{url}", parts.netloc);
    } else if url.starts_with("//")
        || (!parts.scheme.is_empty()
            && USES_NETLOC.contains(&parts.scheme.as_str())
            && (url.is_empty() || url.starts_with('/')))
    {
        url = format!("//{url}");
    }
    if !parts.scheme.is_empty() {
        url = format!("{}:{url}", parts.scheme);
    }
    if !parts.query.is_empty() {
        url = format!("{url}?{}", parts.query);
    }
    if !parts.fragment.is_empty() {
        url = format!("{url}#{}", parts.fragment);
    }
    url
}

/// `urlunparse(...)`: the path gets its `;params` back first.
fn urlunparse(mut parts: SplitResult, params: &str) -> String {
    if !params.is_empty() {
        parts.path = format!("{};{params}", parts.path);
    }
    urlunsplit(&parts)
}

/// `urljoin(base, url)`: `url` resolved against `base` by RFC 3986, with Python's quirks.
pub fn urljoin(base: &str, url: &str) -> Result<String, UrlError> {
    if base.is_empty() {
        return Ok(url.to_owned());
    }
    if url.is_empty() {
        return Ok(base.to_owned());
    }
    let (base_parts, base_params) = urlparse(base, "")?;
    let (mut parts, mut params) = urlparse(url, &base_parts.scheme)?;
    if parts.scheme != base_parts.scheme || !USES_RELATIVE.contains(&parts.scheme.as_str()) {
        return Ok(url.to_owned());
    }
    if USES_NETLOC.contains(&parts.scheme.as_str()) {
        if !parts.netloc.is_empty() {
            return Ok(urlunparse(parts, &params));
        }
        parts.netloc.clone_from(&base_parts.netloc);
    }
    if parts.path.is_empty() && params.is_empty() {
        parts.path.clone_from(&base_parts.path);
        params = base_params;
        if parts.query.is_empty() {
            parts.query.clone_from(&base_parts.query);
        }
        return Ok(urlunparse(parts, &params));
    }

    let mut base_segments: Vec<&str> = base_parts.path.split('/').collect();
    if base_segments.last() != Some(&"") {
        // The last segment is a file, not a directory.
        base_segments.pop();
    }
    let segments: Vec<&str> = if parts.path.starts_with('/') {
        parts.path.split('/').collect()
    } else {
        let mut segments = base_segments;
        segments.extend(parts.path.split('/'));
        // Empty segments in the middle would double slashes on re-joining.
        if segments.len() > 2 {
            let last = segments.len() - 1;
            let tail = segments[last];
            let mut filtered = vec![segments[0]];
            filtered.extend(segments[1..last].iter().copied().filter(|segment| !segment.is_empty()));
            filtered.push(tail);
            segments = filtered;
        }
        segments
    };
    let mut resolved: Vec<&str> = Vec::new();
    for segment in &segments {
        match *segment {
            ".." => {
                resolved.pop();
            }
            "." => {}
            other => resolved.push(other),
        }
    }
    if matches!(segments.last(), Some(&("." | ".."))) {
        resolved.push("");
    }
    let joined = resolved.join("/");
    parts.path = if joined.is_empty() { "/".to_owned() } else { joined };
    Ok(urlunparse(parts, &params))
}

#[cfg(test)]
mod tests;
