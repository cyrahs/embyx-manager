//! File names as Python holds them: `str` decoded with `surrogateescape`.
//!
//! `os.listdir` gives Python a `str` for every name, with each byte that is not UTF-8 turned
//! into a lone surrogate (`U+DC80`..`U+DCFF`). Rust keeps the bytes (`OsStr`). Code that
//! sorts, compares or reports names goes through here so it decides what Python decides
//! without a lossy decode (two different names never become equal; DIVERGENCES.md D72, the
//! WP6 defect).

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt as _;

/// One code point of a surrogate-escaped `str`: a character, or an undecodable byte
/// (`U+DC80`..`U+DCFF`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PyChar {
    Char(char),
    Escaped(u8),
}

impl PyChar {
    /// The code point Python holds.
    pub(crate) fn code_point(self) -> u32 {
        match self {
            Self::Char(ch) => u32::from(ch),
            Self::Escaped(byte) => 0xdc00 | u32::from(byte),
        }
    }
}

/// `bytes.decode('utf-8', 'surrogateescape')`.
pub(crate) fn surrogateescape(mut bytes: &[u8]) -> Vec<PyChar> {
    let mut chars = Vec::with_capacity(bytes.len());
    while !bytes.is_empty() {
        match std::str::from_utf8(bytes) {
            Ok(text) => {
                chars.extend(text.chars().map(PyChar::Char));
                break;
            }
            Err(error) => {
                let (valid, rest) = bytes.split_at(error.valid_up_to());
                // `valid_up_to` bytes are UTF-8 by the error's own word.
                chars.extend(String::from_utf8_lossy(valid).chars().map(PyChar::Char));
                let bad = error.error_len().unwrap_or(rest.len());
                chars.extend(rest[..bad].iter().map(|&byte| PyChar::Escaped(byte)));
                bytes = &rest[bad..];
            }
        }
    }
    chars
}

/// The key Python sorts a name by: its code points, surrogates included (`sorted(..., key=lambda
/// path: path.name)`).
pub(crate) fn sort_key(name: &OsStr) -> Vec<u32> {
    surrogateescape(name.as_bytes())
        .into_iter()
        .map(PyChar::code_point)
        .collect()
}

/// What the UTF-8 encoder's `UnicodeEncodeError` says about a surrogate-escaped `str`: the
/// first run of consecutive surrogates (`character '\udcff' in position 8`, `characters in
/// position 3-5`). `None` for a name that is UTF-8.
pub(crate) fn encode_error_what(chars: &[PyChar]) -> Option<String> {
    let start = chars.iter().position(|ch| matches!(ch, PyChar::Escaped(_)))?;
    let run = chars[start..]
        .iter()
        .take_while(|ch| matches!(ch, PyChar::Escaped(_)))
        .count();
    Some(match chars[start] {
        PyChar::Escaped(byte) if run == 1 => {
            format!("character '\\u{:04x}' in position {start}", 0xdc00 | u32::from(byte))
        }
        _ => format!("characters in position {start}-{}", start + run - 1),
    })
}

/// The text a name is matched with: the name itself, or for a name that is not UTF-8 its lossy
/// decode. Only for patterns in which an undecodable byte can only ever match `.` (never a
/// literal, a digit or a letter): there `U+FFFD` and Python's surrogate decide alike, and the
/// decision is all that is kept.
pub(crate) fn match_text(name: &OsStr) -> std::borrow::Cow<'_, str> {
    name.to_string_lossy()
}

/// `str(Path(text))` on the bytes: `/` and `.` are ASCII, so pathlib's normalisation does
/// not depend on the decoding.
pub(crate) fn normalized_path_bytes(bytes: &[u8]) -> Vec<u8> {
    let root: &[u8] = if bytes.starts_with(b"//") && !bytes.starts_with(b"///") {
        b"//"
    } else if bytes.starts_with(b"/") {
        b"/"
    } else {
        b""
    };
    let parts: Vec<&[u8]> = bytes
        .split(|&byte| byte == b'/')
        .filter(|part| !part.is_empty() && *part != b".")
        .collect();
    let mut out = root.to_vec();
    out.extend(parts.join(&b'/'));
    if out.is_empty() {
        out.push(b'.');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surrogates_sort_where_python_puts_them() {
        // U+DCFF sorts before U+E000 (private use) and after U+D7FF, as Python sorts them.
        let escaped = OsStr::from_bytes(b"a\xff");
        let private = OsStr::new("a\u{e000}");
        let hangul = OsStr::new("a\u{d7a3}");
        assert!(sort_key(hangul) < sort_key(escaped));
        assert!(sort_key(escaped) < sort_key(private));
        // A lossy decode (U+FFFD) would put it after the private-use character.
        assert!(sort_key(OsStr::new("a\u{fffd}")) > sort_key(private));
    }

    #[test]
    fn encode_error_names_the_first_run() {
        let one = surrogateescape(b"ABC-001.\xff.mp4");
        assert_eq!(
            encode_error_what(&one).as_deref(),
            Some("character '\\udcff' in position 8")
        );
        let run = surrogateescape(b"ab\xfe\xff\xfdc\xff");
        assert_eq!(encode_error_what(&run).as_deref(), Some("characters in position 2-4"));
        assert_eq!(encode_error_what(&surrogateescape(b"plain")), None);
    }
}
