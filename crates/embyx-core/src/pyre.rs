//! Python `re`, `parse_qs` and `int()` details the scraped-page parsers depend on.
//!
//! Patterns are written in Python's syntax and compiled with [`compile`] (the `regex`
//! crate, leftmost-first like Python's backtracking engine) or, where they need look-around
//! or back-references, [`compile_fancy`] (`fancy-regex`, a backtracking engine). Both
//! translate `\s`: Python's class is `str.isspace()`, which also holds the separators
//! U+001C to U+001F that Unicode's `White_Space` (the crates' `\s`) leaves out.
//!
//! Copied from fav (`fav-sources/src/pyre.rs`; fav's D73 is this repository's D2).
//!
//! What is not translated (DIVERGENCES.md D2): `\b`/`\w` and case-insensitive matching
//! use the crates' Unicode tables and simple case folding, Python its own (`str.isalnum`,
//! `lower()` of one code point). They differ only on letters outside the patterns' ASCII
//! literals next to a word boundary or in a case-insensitive literal (U+0130 `İ` matches
//! `i` in Python), which HTML attribute syntax never has. [`python_ignore_case`] closes the
//! `İ`/`ı` gap for a fixed pattern that needs it (D15).

use std::fmt;

/// `int()`'s `ValueError` for a decimal string beyond `sys.get_int_max_str_digits()`.
///
/// fav reported it as its `JobError`; here it carries Python's class name and message so a
/// caller can surface `ValueError: <message>` as Python would.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntError {
    pub message: String,
}

impl IntError {
    /// The Python exception class.
    pub const CLASS: &'static str = "ValueError";
}

impl fmt::Display for IntError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", Self::CLASS, self.message)
    }
}

impl std::error::Error for IntError {}

/// `\s` as Python's `str` patterns read it, for use outside and inside a class.
const PY_SPACE_CLASS: &str = r"[\s\x1c-\x1f]";
const PY_SPACE_IN_CLASS: &str = r"\s\x1c-\x1f";

/// A Python pattern with `\s` rewritten for the Rust engines.
fn translate(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 16);
    let mut chars = pattern.chars();
    let mut in_class = false;
    let mut class_start = false;
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                let Some(next) = chars.next() else {
                    out.push('\\');
                    break;
                };
                if next == 's' {
                    out.push_str(if in_class { PY_SPACE_IN_CLASS } else { PY_SPACE_CLASS });
                } else {
                    out.push('\\');
                    out.push(next);
                }
                class_start = false;
                continue;
            }
            '[' if !in_class => {
                in_class = true;
                class_start = true;
                out.push(ch);
                continue;
            }
            '^' if in_class && class_start => {
                out.push(ch);
                continue;
            }
            ']' if in_class && !class_start => in_class = false,
            _ => {}
        }
        class_start = false;
        out.push(ch);
    }
    out
}

fn flags(ignore_case: bool, dot_all: bool) -> &'static str {
    match (ignore_case, dot_all) {
        (true, true) => "(?is)",
        (true, false) => "(?i)",
        (false, true) => "(?s)",
        (false, false) => "",
    }
}

/// `re.compile(pattern, flags)` on the `regex` crate.
pub fn compile(pattern: &str, ignore_case: bool, dot_all: bool) -> regex::Regex {
    regex::Regex::new(&format!("{}{}", flags(ignore_case, dot_all), translate(pattern))).expect("valid pattern")
}

/// `re.compile(pattern, flags)` for a pattern with look-around or back-references.
pub fn compile_fancy(pattern: &str, ignore_case: bool, dot_all: bool) -> fancy_regex::Regex {
    fancy_regex::RegexBuilder::new(&format!("{}{}", flags(ignore_case, dot_all), translate(pattern)))
        .backtrack_limit(BACKTRACK_LIMIT)
        .build()
        .expect("valid pattern")
}

/// `re.compile(pattern, flags)` for a user-supplied pattern: `Err` holds the engine's
/// message instead of panicking.
pub fn try_compile_fancy(pattern: &str, ignore_case: bool, dot_all: bool) -> Result<fancy_regex::Regex, String> {
    fancy_regex::RegexBuilder::new(&format!("{}{}", flags(ignore_case, dot_all), translate(pattern)))
        .backtrack_limit(BACKTRACK_LIMIT)
        .build()
        .map_err(|error| error.to_string())
}

/// Python's case-insensitive matching lower-cases the subject (`İ` U+0130 becomes `i`) and
/// adds the characters that share an upper case (`ı` U+0131 for `i`), so both match `i`,
/// `I`, `[a-z]` and `[A-Z]`; the Rust engines' simple case folding has neither. This
/// rewrites fixed patterns (no group names or escapes containing an `i`; the AVID and
/// magnet patterns) to list them: a literal `i`/`I` becomes a class, and a class holding `i`, `I` or
/// an `a-z`/`A-Z` range gains both. (`ſ` and the Kelvin sign fold in both engines.)
pub fn python_ignore_case(pattern: &str, ignore_case: bool) -> String {
    const EXTRA: &str = "\u{130}\u{131}";
    if !ignore_case {
        return pattern.to_owned();
    }
    let mut out = String::with_capacity(pattern.len() + 16);
    let mut class = String::new();
    let mut in_class = false;
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        let sink = if in_class { &mut class } else { &mut out };
        match ch {
            '\\' => {
                sink.push(ch);
                if let Some(next) = chars.next() {
                    sink.push(next);
                }
            }
            '[' if !in_class => {
                in_class = true;
                class.clear();
                class.push('[');
                // A `]` first in the class (after an optional `^`) is a literal.
                if chars.peek() == Some(&'^') {
                    class.push(chars.next().unwrap_or('^'));
                }
                if chars.peek() == Some(&']') {
                    class.push(chars.next().unwrap_or(']'));
                }
            }
            ']' if in_class => {
                in_class = false;
                let body = &class[1..];
                if body.contains('i') || body.contains('I') || body.contains("a-z") || body.contains("A-Z") {
                    class.push_str(EXTRA);
                }
                class.push(']');
                out.push_str(&class);
            }
            'i' | 'I' if !in_class => {
                out.push('[');
                out.push(ch);
                out.push_str(EXTRA);
                out.push(']');
            }
            _ => sink.push(ch),
        }
    }
    out
}

/// Python has no limit; this one is far beyond any page the parsers see. A search that
/// reaches it counts as no match.
const BACKTRACK_LIMIT: usize = 50_000_000;

static WHITESPACE_RUN: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| compile(r"\s+", false, false));
static TAG: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| compile(r"<[^>]+>", false, false));

/// `re.sub(r'\s+', ' ', text)`.
pub fn collapse_whitespace(text: &str) -> String {
    WHITESPACE_RUN.replace_all(text, regex::NoExpand(" ")).into_owned()
}

/// `re.sub(r'<[^>]+>', '', text)`.
pub fn strip_tags(text: &str) -> String {
    TAG.replace_all(text, regex::NoExpand("")).into_owned()
}

/// The byte offset `count` code points before `at` (or 0): `max(0, i - count)` on a Python
/// index.
pub fn chars_before(text: &str, at: usize, count: usize) -> usize {
    text[..at]
        .char_indices()
        .rev()
        .nth(count.saturating_sub(1))
        .map_or(0, |(index, _)| index)
}

/// `text[at:at + count]` with `at` a byte offset and `count` code points.
pub fn chars_after(text: &str, at: usize, count: usize) -> &str {
    let rest = &text[at..];
    let end = rest.char_indices().nth(count).map_or(rest.len(), |(index, _)| index);
    &rest[..end]
}

/// `text[:count]` in code points.
pub fn truncate_chars(text: &str, count: usize) -> &str {
    chars_after(text, 0, count)
}

/// `parse_qs(query).get(name, [None])[0]`: the first value of `name`, `+` read as a space
/// and percent escapes decoded as UTF-8 with replacement; pairs without `=` or with an
/// empty value are skipped, as `keep_blank_values=False` does.
pub fn query_first(query: &str, name: &str) -> Option<String> {
    if query.is_empty() {
        return None;
    }
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if value.is_empty() {
            continue;
        }
        if crate::pyurl::unquote(&key.replace('+', " ")) == name {
            return Some(crate::pyurl::unquote(&value.replace('+', " ")));
        }
    }
    None
}

/// `sys.get_int_max_str_digits()`: `int()` refuses longer decimal strings.
const INT_MAX_STR_DIGITS: usize = 4300;

/// `str(int(text))` for a string `str.isdecimal()` accepted: ASCII digits without leading
/// zeros, or `int()`'s `ValueError` beyond 4300 digits.
pub fn int_text(text: &str) -> Result<String, IntError> {
    let digits = text.chars().count();
    if digits > INT_MAX_STR_DIGITS {
        return Err(IntError {
            message: format!(
                "Exceeds the limit ({INT_MAX_STR_DIGITS} digits) for integer string conversion: value has {digits} digits; use sys.set_int_max_str_digits() to increase the limit"
            ),
        });
    }
    let ascii: String = text
        .chars()
        .map(|ch| {
            crate::pytext::py_decimal_value(ch)
                .and_then(|digit| char::from_digit(digit, 10))
                .unwrap_or(ch)
        })
        .collect();
    let trimmed = ascii.trim_start_matches('0');
    Ok(if trimmed.is_empty() {
        "0".to_owned()
    } else {
        trimmed.to_owned()
    })
}

/// `int(text) > 0` for a decimal string.
pub fn is_positive_decimal(text: &str) -> Result<bool, IntError> {
    Ok(int_text(text)? != "0")
}

#[cfg(test)]
mod tests {
    use super::{chars_after, chars_before, query_first, translate};

    #[test]
    fn translates_python_space_class() {
        assert_eq!(translate(r"\s+"), r"[\s\x1c-\x1f]+");
        assert_eq!(translate(r#"[^'"\s<>]"#), r#"[^'"\s\x1c-\x1f<>]"#);
        assert_eq!(translate(r"[]\s]\s"), r"[]\s\x1c-\x1f][\s\x1c-\x1f]");
        assert_eq!(translate(r"\\s"), r"\\s");
        assert_eq!(translate(r"\{id-(?P<id>\d+)\}"), r"\{id-(?P<id>\d+)\}");
    }

    #[test]
    fn char_windows_count_code_points() {
        let text = "ab中文cd";
        let at = text.find('c').unwrap();
        assert_eq!(&text[chars_before(text, at, 2)..at], "中文");
        assert_eq!(chars_before(text, at, 10), 0);
        assert_eq!(chars_after(text, 2, 2), "中文");
        assert_eq!(chars_after(text, at, 10), "cd");
    }

    #[test]
    fn query_first_reads_like_parse_qs() {
        assert_eq!(query_first("v=1&v=2", "v").as_deref(), Some("1"));
        assert_eq!(query_first("v=&v=2", "v").as_deref(), Some("2"));
        assert_eq!(query_first("v&x=1", "v"), None);
        assert_eq!(query_first("%76=a+b%2F", "v").as_deref(), Some("a b/"));
        assert_eq!(query_first("", "v"), None);
    }
}
