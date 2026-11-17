//! Python `re` details the scraped-page parsers depend on.
//!
//! Patterns are written in Python's syntax and compiled with [`compile`] (the `regex`
//! crate, leftmost-first like Python's backtracking engine) or, where they need look-around
//! or back-references, [`compile_fancy`] (`fancy-regex`, a backtracking engine). Both
//! translate `\s`: Python's class is `str.isspace()`, which also holds the separators
//! U+001C to U+001F that Unicode's `White_Space` (the crates' `\s`) leaves out.
//!
//! Copied from fav (`fav-sources/src/pyre.rs`; fav's D73 is this repository's D2). WP16 dropped fav's
//! text helpers nothing here calls (whitespace and tag stripping, char windows, `parse_qs`, `int()`).
//!
//! What is not translated (DIVERGENCES.md D2): `\b`/`\w` and case-insensitive matching
//! use the crates' Unicode tables and simple case folding, Python its own (`str.isalnum`,
//! `lower()` of one code point). They differ only on letters outside the patterns' ASCII
//! literals next to a word boundary or in a case-insensitive literal (U+0130 `İ` matches
//! `i` in Python), which HTML attribute syntax never has. [`python_ignore_case`] closes the
//! `İ`/`ı` gap for a fixed pattern that needs it (D15).

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

#[cfg(test)]
mod tests {
    use super::translate;

    #[test]
    fn translates_python_space_class() {
        assert_eq!(translate(r"\s+"), r"[\s\x1c-\x1f]+");
        assert_eq!(translate(r#"[^'"\s<>]"#), r#"[^'"\s\x1c-\x1f<>]"#);
        assert_eq!(translate(r"[]\s]\s"), r"[]\s\x1c-\x1f][\s\x1c-\x1f]");
        assert_eq!(translate(r"\\s"), r"\\s");
        assert_eq!(translate(r"\{id-(?P<id>\d+)\}"), r"\{id-(?P<id>\d+)\}");
    }
}
