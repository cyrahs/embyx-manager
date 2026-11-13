//! Python `str` semantics that fav's rules depend on.
//!
//! Python's `str.strip()` and `str.split()` cut at `str.isspace()` characters, which
//! include U+001C..U+001F (not Unicode `White_Space`, so not Rust's `trim()`), and
//! `str.isdigit()` accepts every Unicode digit. The tables below are checked against a
//! full code-point scan of Python 3.12 (`goldens/text/`).
//!
//! Copied from fav (`fav-core/src/pytext.rs`); see docs/CONVENTIONS.md, "Code copied from fav".

/// Code-point ranges where Python 3.12's `str.isspace()` is true.
pub const PY_WHITESPACE: &[(u32, u32)] = &[
    (0x0009, 0x000D),
    (0x001C, 0x0020),
    (0x0085, 0x0085),
    (0x00A0, 0x00A0),
    (0x1680, 0x1680),
    (0x2000, 0x200A),
    (0x2028, 0x2029),
    (0x202F, 0x202F),
    (0x205F, 0x205F),
    (0x3000, 0x3000),
];

/// Code-point ranges where Python 3.12's `str.isdigit()` is true (Unicode 15.0).
pub const PY_DIGITS: &[(u32, u32)] = &[
    (0x0030, 0x0039),
    (0x00B2, 0x00B3),
    (0x00B9, 0x00B9),
    (0x0660, 0x0669),
    (0x06F0, 0x06F9),
    (0x07C0, 0x07C9),
    (0x0966, 0x096F),
    (0x09E6, 0x09EF),
    (0x0A66, 0x0A6F),
    (0x0AE6, 0x0AEF),
    (0x0B66, 0x0B6F),
    (0x0BE6, 0x0BEF),
    (0x0C66, 0x0C6F),
    (0x0CE6, 0x0CEF),
    (0x0D66, 0x0D6F),
    (0x0DE6, 0x0DEF),
    (0x0E50, 0x0E59),
    (0x0ED0, 0x0ED9),
    (0x0F20, 0x0F29),
    (0x1040, 0x1049),
    (0x1090, 0x1099),
    (0x1369, 0x1371),
    (0x17E0, 0x17E9),
    (0x1810, 0x1819),
    (0x1946, 0x194F),
    (0x19D0, 0x19DA),
    (0x1A80, 0x1A89),
    (0x1A90, 0x1A99),
    (0x1B50, 0x1B59),
    (0x1BB0, 0x1BB9),
    (0x1C40, 0x1C49),
    (0x1C50, 0x1C59),
    (0x2070, 0x2070),
    (0x2074, 0x2079),
    (0x2080, 0x2089),
    (0x2460, 0x2468),
    (0x2474, 0x247C),
    (0x2488, 0x2490),
    (0x24EA, 0x24EA),
    (0x24F5, 0x24FD),
    (0x24FF, 0x24FF),
    (0x2776, 0x277E),
    (0x2780, 0x2788),
    (0x278A, 0x2792),
    (0xA620, 0xA629),
    (0xA8D0, 0xA8D9),
    (0xA900, 0xA909),
    (0xA9D0, 0xA9D9),
    (0xA9F0, 0xA9F9),
    (0xAA50, 0xAA59),
    (0xABF0, 0xABF9),
    (0xFF10, 0xFF19),
    (0x104A0, 0x104A9),
    (0x10A40, 0x10A43),
    (0x10D30, 0x10D39),
    (0x10E60, 0x10E68),
    (0x11052, 0x1105A),
    (0x11066, 0x1106F),
    (0x110F0, 0x110F9),
    (0x11136, 0x1113F),
    (0x111D0, 0x111D9),
    (0x112F0, 0x112F9),
    (0x11450, 0x11459),
    (0x114D0, 0x114D9),
    (0x11650, 0x11659),
    (0x116C0, 0x116C9),
    (0x11730, 0x11739),
    (0x118E0, 0x118E9),
    (0x11950, 0x11959),
    (0x11C50, 0x11C59),
    (0x11D50, 0x11D59),
    (0x11DA0, 0x11DA9),
    (0x11F50, 0x11F59),
    (0x16A60, 0x16A69),
    (0x16AC0, 0x16AC9),
    (0x16B50, 0x16B59),
    (0x1D7CE, 0x1D7FF),
    (0x1E140, 0x1E149),
    (0x1E2F0, 0x1E2F9),
    (0x1E4F0, 0x1E4F9),
    (0x1E950, 0x1E959),
    (0x1F100, 0x1F10A),
    (0x1FBF0, 0x1FBF9),
];

/// Code-point ranges where Python 3.12's `str.isdecimal()` is true (Unicode 15.0, category
/// `Nd`). Every range is a run of whole `0`..`9` sequences, so a digit's value is its
/// offset from the range start, modulo 10.
pub const PY_DECIMALS: &[(u32, u32)] = &[
    (0x0030, 0x0039),
    (0x0660, 0x0669),
    (0x06F0, 0x06F9),
    (0x07C0, 0x07C9),
    (0x0966, 0x096F),
    (0x09E6, 0x09EF),
    (0x0A66, 0x0A6F),
    (0x0AE6, 0x0AEF),
    (0x0B66, 0x0B6F),
    (0x0BE6, 0x0BEF),
    (0x0C66, 0x0C6F),
    (0x0CE6, 0x0CEF),
    (0x0D66, 0x0D6F),
    (0x0DE6, 0x0DEF),
    (0x0E50, 0x0E59),
    (0x0ED0, 0x0ED9),
    (0x0F20, 0x0F29),
    (0x1040, 0x1049),
    (0x1090, 0x1099),
    (0x17E0, 0x17E9),
    (0x1810, 0x1819),
    (0x1946, 0x194F),
    (0x19D0, 0x19D9),
    (0x1A80, 0x1A89),
    (0x1A90, 0x1A99),
    (0x1B50, 0x1B59),
    (0x1BB0, 0x1BB9),
    (0x1C40, 0x1C49),
    (0x1C50, 0x1C59),
    (0xA620, 0xA629),
    (0xA8D0, 0xA8D9),
    (0xA900, 0xA909),
    (0xA9D0, 0xA9D9),
    (0xA9F0, 0xA9F9),
    (0xAA50, 0xAA59),
    (0xABF0, 0xABF9),
    (0xFF10, 0xFF19),
    (0x104A0, 0x104A9),
    (0x10D30, 0x10D39),
    (0x11066, 0x1106F),
    (0x110F0, 0x110F9),
    (0x11136, 0x1113F),
    (0x111D0, 0x111D9),
    (0x112F0, 0x112F9),
    (0x11450, 0x11459),
    (0x114D0, 0x114D9),
    (0x11650, 0x11659),
    (0x116C0, 0x116C9),
    (0x11730, 0x11739),
    (0x118E0, 0x118E9),
    (0x11950, 0x11959),
    (0x11C50, 0x11C59),
    (0x11D50, 0x11D59),
    (0x11DA0, 0x11DA9),
    (0x11F50, 0x11F59),
    (0x16A60, 0x16A69),
    (0x16AC0, 0x16AC9),
    (0x16B50, 0x16B59),
    (0x1D7CE, 0x1D7FF),
    (0x1E140, 0x1E149),
    (0x1E2F0, 0x1E2F9),
    (0x1E4F0, 0x1E4F9),
    (0x1E950, 0x1E959),
    (0x1FBF0, 0x1FBF9),
];

fn in_ranges(ranges: &[(u32, u32)], ch: char) -> bool {
    let code = u32::from(ch);
    ranges
        .binary_search_by(|&(start, end)| {
            if end < code {
                std::cmp::Ordering::Less
            } else if start > code {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

/// Python `ch.isspace()`.
pub fn is_py_space(ch: char) -> bool {
    in_ranges(PY_WHITESPACE, ch)
}

/// Python `ch.isdigit()`.
pub fn is_py_digit(ch: char) -> bool {
    in_ranges(PY_DIGITS, ch)
}

/// Python `ch.isdecimal()`.
pub fn is_py_decimal(ch: char) -> bool {
    in_ranges(PY_DECIMALS, ch)
}

/// Python `text.isdecimal()`: true for a non-empty string of decimal digits only.
pub fn py_isdecimal(text: &str) -> bool {
    !text.is_empty() && text.chars().all(is_py_decimal)
}

/// The value of a decimal digit (`unicodedata.decimal(ch)`), `None` for anything else.
pub fn py_decimal_value(ch: char) -> Option<u32> {
    let code = u32::from(ch);
    PY_DECIMALS
        .iter()
        .find(|&&(start, end)| start <= code && code <= end)
        .map(|&(start, _)| (code - start) % 10)
}

/// Python `int(text)` for a string of decimal digits only (what callers check with
/// `isdigit()`/`isdecimal()` first): `None` when a character is not a decimal digit, as
/// for superscripts, which `isdigit()` accepts and `int()` refuses. Values beyond `i128`
/// are `None` too.
pub fn py_int_decimal(text: &str) -> Option<i128> {
    if text.is_empty() {
        return None;
    }
    text.chars().try_fold(0_i128, |total, ch| {
        let digit = py_decimal_value(ch)?;
        total.checked_mul(10)?.checked_add(i128::from(digit))
    })
}

/// Python `text.strip()`.
pub fn py_strip(text: &str) -> &str {
    text.trim_matches(is_py_space)
}

/// Python `text.split()` (no separator): runs of whitespace separate, empty parts dropped.
pub fn py_split(text: &str) -> impl Iterator<Item = &str> {
    text.split(is_py_space).filter(|part| !part.is_empty())
}

/// Python `text.isdigit()`: true for a non-empty string of digits only.
pub fn py_isdigit(text: &str) -> bool {
    !text.is_empty() && text.chars().all(is_py_digit)
}

/// Python `text.rstrip(chars)` for a set of characters.
pub fn py_rstrip_chars<'a>(text: &'a str, chars: &[char]) -> &'a str {
    text.trim_end_matches(|ch| chars.contains(&ch))
}

/// Python `text.lstrip(chars)` for a set of characters.
pub fn py_lstrip_chars<'a>(text: &'a str, chars: &[char]) -> &'a str {
    text.trim_start_matches(|ch| chars.contains(&ch))
}

#[cfg(test)]
mod tests {
    use super::{PY_DECIMALS, PY_DIGITS, PY_WHITESPACE, py_int_decimal, py_isdecimal, py_isdigit, py_split, py_strip};
    use crate::golden;

    fn golden_ranges(path: &str) -> Vec<(u32, u32)> {
        let document = golden::load(path);
        let case = &golden::cases(&document)[0];
        golden::list(golden::get(case, "ranges"))
            .iter()
            .map(|pair| {
                let pair = golden::list(pair);
                (golden::uint(&pair[0]), golden::uint(&pair[1]))
            })
            .collect()
    }

    #[test]
    fn whitespace_table_matches_python_isspace() {
        assert_eq!(golden_ranges("text/isspace.json"), PY_WHITESPACE);
    }

    #[test]
    fn digit_table_matches_python_isdigit() {
        assert_eq!(golden_ranges("text/isdigit.json"), PY_DIGITS);
    }

    #[test]
    fn decimal_table_matches_python_isdecimal() {
        assert_eq!(golden_ranges("text/isdecimal.json"), PY_DECIMALS);
        assert!(PY_DECIMALS.iter().all(|(start, end)| (end - start + 1) % 10 == 0));
    }

    #[test]
    fn decimal_strings_convert_like_python_int() {
        assert_eq!(py_int_decimal("0123"), Some(123));
        assert_eq!(py_int_decimal("\u{661}\u{662}"), Some(12));
        assert_eq!(py_int_decimal("\u{1d7d9}\u{1d7ce}"), Some(10));
        assert_eq!(py_int_decimal("\u{b2}"), None);
        assert_eq!(py_int_decimal(""), None);
        assert_eq!(py_int_decimal(" 1"), None);
        assert!(py_isdecimal("\u{ff11}") && !py_isdecimal("\u{b2}") && !py_isdecimal(""));
    }

    #[test]
    fn strip_and_split_use_python_whitespace() {
        assert_eq!(py_strip("\u{1c}\u{3000} a b \u{85}"), "a b");
        assert_eq!(py_strip("\u{200b}x\u{200b}"), "\u{200b}x\u{200b}");
        assert_eq!(
            py_split(" */5\t*\n*  * \u{3000} * ").collect::<Vec<_>>(),
            ["*/5", "*", "*", "*", "*"]
        );
        assert!(py_isdigit("\u{661}\u{662}") && py_isdigit("\u{b2}") && !py_isdigit("\u{bd}") && !py_isdigit(""));
    }
}
