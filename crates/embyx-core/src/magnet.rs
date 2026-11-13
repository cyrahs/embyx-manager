//! Magnet URI parsing, reduced to the one field the pipeline tracks.
//!
//! Python origin: `src/embyx_manager/core/magnet.py`. The `BitTorrent` info hash ties a
//! submitted magnet to the offline task CloudDrive reports, so it is extracted once and used
//! as the join key from then on (`archive_magnet_attempts.info_hash`). Hashes are
//! upper-case hex, because sources advertise the same torrent as hex or base32.

use std::sync::LazyLock;

use crate::pyre;

/// `_BTIH_RE = re.compile(r'urn:btih:([A-Za-z0-9]+)', re.IGNORECASE)`, with Python's
/// case-insensitive `İ`/`ı` (DIVERGENCES.md D15).
static BTIH_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| pyre::compile(&pyre::python_ignore_case(r"urn:btih:([A-Za-z0-9]+)", true), true, false));
const HEX_LENGTH: usize = 40;
const BASE32_LENGTH: usize = 32;

/// The magnet's info hash as upper-case hex, or `None` when it has none.
///
/// Only v1 (SHA-1) hashes are recognized; a v2-only magnet has no 40-hex hash for
/// CloudDrive to report and cannot be tracked.
pub fn extract_info_hash(magnet: &str) -> Option<String> {
    let value = BTIH_RE.captures(magnet)?.get(1)?.as_str();
    // Lengths count characters, as Python's `len` does: case-insensitive `[A-Za-z]` also
    // matches the Kelvin sign and the long s.
    match value.chars().count() {
        // `int(value, 16)` accepts ASCII hex digits after an optional `0x` prefix (the class
        // admits no sign, space or underscore, nor anything else `int` reads as a digit).
        HEX_LENGTH => {
            let digits = value
                .strip_prefix("0x")
                .or_else(|| value.strip_prefix("0X"))
                .unwrap_or(value);
            (!digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_hexdigit())).then(|| value.to_uppercase())
        }
        BASE32_LENGTH => b32decode(&value.to_uppercase()).map(|bytes| upper_hex(&bytes)),
        _ => None,
    }
}

/// `bytes.hex().upper()`.
fn upper_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02X}");
            out
        })
}

/// `base64.b32decode(value)` for an unpadded 32-character value: RFC 4648 alphabet only,
/// `None` where Python raises `binascii.Error`.
fn b32decode(value: &str) -> Option<Vec<u8>> {
    let mut bits: u64 = 0;
    let mut count = 0;
    let mut out = Vec::with_capacity(20);
    for ch in value.chars() {
        let digit = match ch {
            'A'..='Z' => u64::from(u32::from(ch) - u32::from('A')),
            '2'..='7' => u64::from(u32::from(ch) - u32::from('2') + 26),
            _ => return None,
        };
        bits = (bits << 5) | digit;
        count += 5;
        if count >= 8 {
            count -= 8;
            out.push(u8::try_from((bits >> count) & 0xff).ok()?);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::extract_info_hash;
    use crate::golden;

    const HEX: &str = "C12FE1C06BBA254A9DC9F519B335AA7C1367A88A";
    const BASE32: &str = "YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK";

    // py: tests/test_magnet.py::test_info_hash_is_normalized_to_upper_hex cases=7
    #[test]
    fn info_hash_is_normalized_to_upper_hex() {
        let lower_hex = HEX.to_lowercase();
        let lower_base32 = BASE32.to_lowercase();
        for magnet in [
            format!("magnet:?xt=urn:btih:{HEX}"),
            format!("magnet:?xt=urn:btih:{lower_hex}"),
            format!("magnet:?xt=urn:btih:{HEX}&dn=ABC-123"),
            format!("magnet:?dn=ABC-123&xt=urn:btih:{HEX}&tr=udp://tracker"),
            format!("magnet:?xt=urn:btih:{BASE32}"),
            format!("magnet:?xt=urn:btih:{lower_base32}&dn=ABC-123"),
            format!("MAGNET:?XT=URN:BTIH:{HEX}"),
        ] {
            assert_eq!(extract_info_hash(&magnet).as_deref(), Some(HEX), "{magnet}");
        }
    }

    // py: tests/test_magnet.py::test_unusable_magnets_yield_no_hash cases=6
    #[test]
    fn unusable_magnets_yield_no_hash() {
        for magnet in [
            String::new(),
            "not a magnet".to_owned(),
            "magnet:?dn=ABC-123".to_owned(),
            "magnet:?xt=urn:sha1:C12FE1C06BBA254A9DC9F519B335AA7C1367A88A".to_owned(),
            format!("magnet:?xt=urn:btih:{}", &HEX[..HEX.len() - 1]),
            format!("magnet:?xt=urn:btih:{}", "Z".repeat(40)),
        ] {
            assert_eq!(extract_info_hash(&magnet), None, "{magnet}");
        }
    }

    // py: tests/test_magnet.py::test_v2_only_magnet_is_not_tracked
    #[test]
    fn v2_only_magnet_is_not_tracked() {
        let magnet = "magnet:?xt=urn:btmh:1220caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e";
        assert_eq!(extract_info_hash(magnet), None);
    }

    #[test]
    fn info_hashes_match_the_python_baseline() {
        let document = golden::load("magnet/info_hash.json");
        for case in golden::cases(&document) {
            let magnet = golden::string(golden::get(case, "magnet"));
            let expected = golden::get(case, "info_hash").as_str().map(str::to_owned);
            assert_eq!(extract_info_hash(&magnet), expected, "{magnet:?}");
        }
    }
}
