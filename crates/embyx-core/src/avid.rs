//! Extract and normalize video IDs (DVD ID / AVID) from titles and file names.
//!
//! Python origin: `src/embyx_manager/core/avid.py`. This is the single place an AVID is
//! derived, so that RSS, the archiver and the acquisition ledger never disagree about what
//! to call a video; the spelling keys the ledger and names library files (cross-version
//! contract 7), so every branch below follows the Python one in order and the corpora in
//! `tests/data/` plus `goldens/avid/` hold it to the baseline's output.
//!
//! The pattern branches are derived from `JavSP`'s `javsp/avid.py`
//! (<https://github.com/Yuukiy/JavSP>), Copyright (C) `JavSP` contributors, licensed under
//! GPL-3.0; the release-noise vocabulary and the zero-padding rule are ported from
//! metatube-sdk-go (<https://github.com/metatube-community/metatube-sdk-go>), Copyright (C)
//! `MetaTube` contributors, licensed under Apache-2.0. This project is distributed under
//! GPL-3.0 (see LICENSE at the repository root).
//!
//! Regular expressions are Python's, compiled with [`pyre`]. Where Python writes `$`
//! (end of text or before a final newline) the Rust pattern spells it `(?=\n?\z)`, or `\z`
//! where the character before it may be a newline anyway; `^` is the start of the text in
//! both. Lengths and slices count characters, as Python's `len` and slicing do.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use crate::{pypath, pyre, pytext};

pub const MIN_BRAND_LENGTH: usize = 2;
/// Longest suffix still treated as a file extension. A title like
/// "test.xxx@133ARA-030 hello" has a long trailing "suffix" that is part of the name, and
/// cutting it would throw the ID away.
pub const MAX_EXTENSION_LENGTH: usize = 7;

/// Release noise: containers, codecs, resolutions and provenance markers.
const TAG_ALTERNATIVES: &str = concat!(
    r"dvd|iso|mkv|mp4|c?avi|\d*fps|whole|(?:f|hhb)?hd\d*|sd\d*|lt",
    r"|(?:144|240|360|480|720|1080|2160)[pi]|x1080x|uhd|hq|uncensored|leak|[2468]ks?|[xh]26[45]",
);

fn fancy(pattern: &str, ignore_case: bool) -> fancy_regex::Regex {
    pyre::compile_fancy(&pyre::python_ignore_case(pattern, ignore_case), ignore_case, false)
}

fn plain(pattern: &str, ignore_case: bool) -> regex::Regex {
    pyre::compile(&pyre::python_ignore_case(pattern, ignore_case), ignore_case, false)
}

static DOMAIN_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"\w{3,10}\.(COM|NET|APP|XYZ)", true));
/// The tag must stand alone between non-alphanumerics, or `sd` would eat the start of
/// SDMF-016 and leave MF-016 behind. (`(?=$|[^a-z\d])`: a final newline is `[^a-z\d]`.)
static TAG_RE: LazyLock<fancy_regex::Regex> = LazyLock::new(|| {
    fancy(
        &format!(r"(?:^|(?<=[^a-z\d]))(?:{TAG_ALTERNATIVES})+(?=\z|[^a-z\d])"),
        true,
    )
});
static SINGLE_TAG_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(&format!("({TAG_ALTERNATIVES})"), true));
/// Separators left doubled up by stripping; the ID patterns expect exactly one.
static REPEATED_SEPARATOR_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"[-_. ]{2,}", false));
static QUALITY_PREFIX_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"^(?:f?hd|sd)[-_]", true));
/// Trailing disc/subtitle markers, stripped repeatedly. (`\s*$`: `\s*` takes a final
/// newline itself, so `\z` finds the same matches.)
static SUFFIX_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| plain(r"(?:[-_](?:c|uc|ch|cd\d{1,2})|hhb\d*|ch)\s*\z", true));
/// Uncensored studios that prefix their own date-style IDs; the ID is what stays.
static STUDIO_RE: LazyLock<fancy_regex::Regex> = LazyLock::new(|| {
    fancy(
        concat!(
            r"(?:^|(?<=[^a-z\d]))(?:carib(?:b?ean)?(?:com)?(?:pr)?|1?pond?o?|10mu(?:sume)?",
            r"|paco(?:paco)?(?:mama)?|mura(?:mura)?|tokyo[-_\s]?hot)",
            r"(?:[-_\s]+(?P<kept>\d{4,}[-_]\d{2,}|[a-z]{1,4}\d{2,4})|(?=\n?\z))",
        ),
        true,
    )
});

/// Variant markers that mean "this file is the higher-resolution cut".
pub const HIGH_RESOLUTION_TAGS: [&str; 7] = ["2K", "4K", "6K", "8K", "2160P", "2160I", "UHD"];

/// The ordinary BRAND-NUMBER shape; the brand has at least one letter, so purely numeric
/// ids (4102-023, 123456-789) keep their own spelling.
static NUMBER_FORM_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| plain(r"^([A-Z0-9]*[A-Z][A-Z0-9]*)-(\d+)\n?\z", false));
/// Brands whose numbers are catalog serials or two-digit volumes rather than DVD codes, and
/// are left as written: FC2 and friends, and the 3D+2D Blu-ray lines.
pub const NON_CID_BRANDS: [&str; 12] = [
    "FC2", "HEYZO", "HEYDOUGA", "GETCHU", "GYUTTO", "MKBD", "MKD", "S2M", "S2MBD", "MK3D2DBD", "CW3D2BD", "CW3D2DBD",
];

static FC2_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"FC2[^A-Z\d]{0,5}(PPV[^A-Z\d]{0,5})?(\d{5,7})", true));
static HEYDOUGA_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"(HEYDOUGA)[-_]*(\d{4})[-_]0?(\d{3,5})", true));
static GETCHU_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"GETCHU[-_]*(\d+)", true));
static GYUTTO_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"GYUTTO-(\d+)", true));
static LUXU_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"259LUXU-(\d+)", true));
static HEY_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"(?:HEY)[-_]*(\d{4})[-_]0?(\d{3,5})", true));
static MUGEN_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| plain(r"(MKB?D)[-_]*(S\d{2,3})|(S2M|S2MBD)[-_]*(\d{2,3})", true));
static BLU_RAY_3D_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"([A-Z]{2,4}3D2D?BD)[-_]*(\d{2,3})", true));
static IBW_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"(IBW)[-_](\d{2,5}z)", true));
static ID_SERIES_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"(\d{2,3}ID)[-_](\d{2,5})", true));
static ORDINARY_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"([A-Z]{2,10})[-_](\d{2,5})", true));
static TOKYO_HOT_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"(RED[01]\d\d|SKY[0-3]\d\d|EX00[01]\d)", true));
static NO_SEPARATOR_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"([A-Z]{2,})(\d{2,5})", true));
static T_SERIES_RE: LazyLock<fancy_regex::Regex> =
    LazyLock::new(|| fancy(r"(?:^|(?<=[^A-Z\d]))(T[-_]\d{5})(?!\d)|(T[23]8[-_]\d{3})", false));
static TOKYO_HOT_NK_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"([NK])[-_]?(\d{4})", true));
static NUMERIC_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"(\d{6}[-_]\d{2,3})", false));
static LETTER_NUMBER_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"([A-Z]{2,10})[-_]([A-Z]\d{2,5})", false));
static SHORT_NUMERIC_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"(\d{4}[-_]\d{3})", false));
static BRAND_DIGITS_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"^\d+\n?\z", false));
static CD_RE: LazyLock<regex::Regex> = LazyLock::new(|| plain(r"CD(\d+).", false));

fn char_len(text: &str) -> usize {
    text.chars().count()
}

/// The first `count` characters of `text`.
fn char_prefix(text: &str, count: usize) -> &str {
    text.char_indices().nth(count).map_or(text, |(index, _)| &text[..index])
}

/// `re.sub` over a backtracking pattern; `fancy_regex` only fails on its backtrack limit,
/// which these fixed patterns cannot reach on a file name.
fn fancy_sub(
    re: &fancy_regex::Regex,
    text: &str,
    replacement: impl Fn(&fancy_regex::Captures<'_, str>) -> String,
) -> String {
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    for captures in re.captures_iter(text) {
        let Ok(captures) = captures else { break };
        let whole = captures.get(0).expect("group 0");
        out.push_str(&text[last..whole.start()]);
        out.push_str(&replacement(&captures));
        last = whole.end();
    }
    out.push_str(&text[last..]);
    out
}

/// The domain-stripped first try and the safe fallback text.
///
/// A domain followed by more alphanumeric content is unambiguously release noise, so it
/// must stay stripped even when the remainder has no recognizable ID. A domain at the end
/// retains the historical fallback for corpus entries such as `... UUS75.COM`.
fn domain_variants(norm: &str) -> (String, String) {
    let Some(domain) = DOMAIN_RE.find(norm) else {
        return (norm.to_owned(), norm.to_owned());
    };
    let no_domain = DOMAIN_RE.replace_all(norm, "").into_owned();
    let fallback = if norm[domain.end()..].chars().any(pytext::is_py_alnum) {
        no_domain.clone()
    } else {
        norm.to_owned()
    };
    (no_domain, fallback)
}

/// `strip_extension`: drop a trailing file extension, leaving long dotted titles intact.
pub fn strip_extension(name: &str) -> String {
    let suffix = pypath::suffix(name);
    let length = char_len(suffix);
    if 0 < length && length < MAX_EXTENSION_LENGTH {
        // `name[: -len(suffix)]`: counted from the end of the whole string.
        return char_prefix(name, char_len(name) - length).to_owned();
    }
    name.to_owned()
}

/// `strip_variant_tags`: the name without its extension or quality tags; separators are
/// left behind. Two names equal after this are cuts of the same video.
pub fn strip_variant_tags(name: &str) -> String {
    fancy_sub(&TAG_RE, &strip_extension(name), |_| String::new())
}

/// `variant_tags`: quality markers on a release name, upper-cased (`4K`, `1080P`, `H264`).
/// Runs are located first and split afterwards, so tags that pile up without separators
/// (`_4K60FPS`) are read as two tags.
pub fn variant_tags(name: &str) -> BTreeSet<String> {
    let stripped = strip_extension(name);
    let mut tags = BTreeSet::new();
    for run in TAG_RE.find_iter(&stripped) {
        let Ok(run) = run else { break };
        for tag in SINGLE_TAG_RE.captures_iter(run.as_str()) {
            tags.insert(tag[1].to_uppercase());
        }
    }
    tags
}

/// Whether [`variant_tags`] of a name include a [`HIGH_RESOLUTION_TAGS`] marker.
pub fn is_high_resolution(name: &str) -> bool {
    variant_tags(name)
        .iter()
        .any(|tag| HIGH_RESOLUTION_TAGS.contains(&tag.as_str()))
}

/// `_strip_release_noise`: quality tags and studio prefixes removed, the ID and its
/// separators left; only the separators the removal leaves doubled are collapsed.
fn strip_release_noise(stem: &str) -> String {
    let stem = QUALITY_PREFIX_RE.replace(stem, "").into_owned();
    let stem = fancy_sub(&TAG_RE, &stem, |_| String::new());
    let mut stem = fancy_sub(&STUDIO_RE, &stem, |captures| {
        captures
            .name("kept")
            .map(|kept| kept.as_str().to_owned())
            .unwrap_or_default()
    });
    loop {
        let trimmed = SUFFIX_RE.replace(&stem, "").into_owned();
        if trimmed == stem {
            break;
        }
        stem = trimmed;
    }
    pytext::py_strip(&REPEATED_SEPARATOR_RE.replace_all(&stem, "-")).to_owned()
}

/// `_match_odd_numbering`: studios whose numbering does not fit the BRAND-NUMBER shape,
/// tried after the regular patterns of every branch.
fn match_odd_numbering(norm: &str) -> String {
    // T-series oddities: Fanza's T-28621 needs exactly five digits and an identifier
    // boundary; TMA also has irregular ids such as T28-557.
    if let Ok(Some(captures)) = T_SERIES_RE.captures(norm) {
        return match captures.get(1) {
            Some(first) => first.as_str().replace('_', "-"),
            None => captures
                .get(2)
                .map_or_else(String::new, |second| second.as_str().to_owned()),
        };
    }
    // Tokyo-Hot n and k series; some archives write n0893 as N-0893, canonical has no
    // separator.
    if let Some(captures) = TOKYO_HOT_NK_RE.captures(norm) {
        return format!("{}{}", &captures[1], &captures[2]);
    }
    // Purely numeric uncensored ids.
    if let Some(captures) = NUMERIC_RE.captures(norm) {
        return captures[1].to_owned();
    }
    // Numbers carrying a letter prefix, e.g. MARRA-A030; a weak signal, so it runs last.
    if let Some(captures) = LETTER_NUMBER_RE.captures(norm) {
        return format!("{}-{}", &captures[1], &captures[2]);
    }
    // Short numeric ids such as 4102-023, which the six-digit rule above misses.
    if let Some(captures) = SHORT_NUMERIC_RE.captures(norm) {
        return captures[1].to_owned();
    }
    String::new()
}

/// `normalize_number`: the number spelled as the catalogs do, an integer zero-padded to
/// three digits (HTTM-0066 and TBW-19 become HTTM-066 and TBW-019). Numbers with more than
/// three significant digits keep them all; non-CID brands are left as written.
pub fn normalize_number(avid: &str) -> String {
    let Some(captures) = NUMBER_FORM_RE.captures(avid) else {
        return avid.to_owned();
    };
    let brand = &captures[1];
    if NON_CID_BRANDS.contains(&brand) {
        return avid.to_owned();
    }
    // `int(...)` reads any script's decimal digits; `:03d` writes ASCII. Done on the
    // digits, so a number of any length is padded as Python's unbounded int is
    // (DIVERGENCES.md D20 for the 4300-digit limit).
    let Some(digits) = captures[2]
        .chars()
        .map(|ch| pytext::py_decimal_value(ch).and_then(|digit| char::from_digit(digit, 10)))
        .collect::<Option<String>>()
    else {
        return avid.to_owned();
    };
    let significant = digits.trim_start_matches('0');
    let number = if significant.is_empty() { "0" } else { significant };
    format!("{brand}-{number:0>3}")
}

/// Deployment-tunable ID parsing rules (`AvidConfig`).
///
/// `id_exceptions` are returned verbatim when found in a title (upper-cased match);
/// `ignored_id_patterns` are Python regular expressions stripped before matching.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AvidConfig {
    pub id_exceptions: Vec<String>,
    pub ignored_id_patterns: Vec<String>,
}

/// `re.error` from compiling `ignored_id_patterns`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PatternError(pub String);

impl PatternError {
    /// The Python exception class.
    pub const CLASS: &'static str = "PatternError";
}

/// Python refuses inline global flags (`(?i)`) anywhere but at the start of the pattern;
/// the Rust engines accept them, so the joined pattern is checked first.
fn misplaced_global_flags(pattern: &str) -> bool {
    let bytes = pattern.as_bytes();
    let mut index = 0;
    let mut in_class = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 1,
            b'[' if !in_class => in_class = true,
            b']' if in_class => in_class = false,
            b'(' if !in_class && bytes.get(index + 1) == Some(&b'?') => {
                let flags = bytes[index + 2..]
                    .iter()
                    .take_while(|byte| b"aiLmsux-".contains(byte))
                    .count();
                if flags > 0 && bytes.get(index + 2 + flags) == Some(&b')') && index > 0 {
                    return true;
                }
            }
            _ => {}
        }
        index += 1;
    }
    false
}

/// `AvidParser`.
#[derive(Debug, Clone, Default)]
pub struct AvidParser {
    config: AvidConfig,
    ignore_re: Option<fancy_regex::Regex>,
}

impl AvidParser {
    /// `AvidParser(config)`: the non-empty ignore patterns joined with `|` and compiled as
    /// one; a pattern Python cannot compile is refused, as `re.compile` raises.
    pub fn new(config: AvidConfig) -> Result<Self, PatternError> {
        let patterns: Vec<&str> = config
            .ignored_id_patterns
            .iter()
            .map(String::as_str)
            .filter(|pattern| !pattern.is_empty())
            .collect();
        let ignore_re = if patterns.is_empty() {
            None
        } else {
            let joined = patterns.join("|");
            if misplaced_global_flags(&joined) {
                return Err(PatternError(
                    "global flags not at the start of the expression".to_owned(),
                ));
            }
            Some(pyre::try_compile_fancy(&joined, false, false).map_err(PatternError)?)
        };
        Ok(Self { config, ignore_re })
    }

    pub fn config(&self) -> &AvidConfig {
        &self.config
    }

    /// `get_avid`: the canonical ID for a title or file name; empty when none can be read.
    pub fn get_avid(&self, title: &str) -> String {
        let title = title.replace('/', "");
        let upper = title.to_uppercase();
        for exception in &self.config.id_exceptions {
            if upper.contains(exception.as_str()) {
                return exception.clone();
            }
        }
        normalize_number(&self.get_id(&title).to_uppercase())
    }

    /// `get_id`: the DVD ID of a file path, as written (not yet upper-cased or padded).
    #[allow(clippy::too_many_lines, reason = "one branch per Python branch, in Python's order")]
    pub fn get_id(&self, filepath: &str) -> String {
        let name = pypath::name(filepath);
        let mut stem = strip_release_noise(&strip_extension(name));
        if let Some(ignore) = &self.ignore_re {
            stem = fancy_sub(ignore, &stem, |_| String::new());
        }
        let mut norm = stem.to_uppercase();
        if norm.contains("FC2") {
            // FC2 Club ids have 5-7 digits.
            if let Some(captures) = FC2_RE.captures(&norm) {
                return format!("FC2-{}", &captures[2]);
            }
        } else if norm.contains("HEYDOUGA") {
            if let Some(captures) = HEYDOUGA_RE.captures(&norm) {
                return format!("{}-{}-{}", &captures[1], &captures[2], &captures[3]);
            }
        } else if norm.contains("GETCHU") {
            if let Some(captures) = GETCHU_RE.captures(&norm) {
                return format!("GETCHU-{}", &captures[1]);
            }
        } else if norm.contains("GYUTTO") {
            if let Some(captures) = GYUTTO_RE.captures(&norm) {
                return format!("GYUTTO-{}", &captures[1]);
            }
        } else if norm.contains("259LUXU") {
            if let Some(captures) = LUXU_RE.captures(&norm) {
                return format!("259LUXU-{}", &captures[1]);
            }
        } else {
            let (no_domain, fallback_norm) = domain_variants(&norm);
            if no_domain != norm {
                let avid = self.get_avid(&no_domain);
                if !avid.is_empty() {
                    return avid;
                }
                norm = fallback_norm;
            }
            // heydouga abbreviated to hey; three parts, so before the two-part patterns.
            if let Some(captures) = HEY_RE.captures(&norm) {
                return format!("heydouga-{}-{}", &captures[1], &captures[2]);
            }
            // MUGEN's odd ids; the ordinary pattern would read S2MBD-048 as MBD-048.
            if let Some(captures) = MUGEN_RE.captures(&norm) {
                return match (captures.get(1), captures.get(2)) {
                    (Some(first), Some(second)) => format!("{}-{}", first.as_str(), second.as_str()),
                    _ => format!("{}-{}", &captures[3], &captures[4]),
                };
            }
            // 3D+2D Blu-ray lines keep the marketing token (MK3D2DBD, CW3D2BD, CW3D2DBD).
            if let Some(captures) = BLU_RAY_3D_RE.captures(&norm) {
                return format!("{}-{}", &captures[1], &captures[2]);
            }
            // IBW's ids carry a trailing z.
            if let Some(captures) = IBW_RE.captures(&norm) {
                return format!("{}-{}", &captures[1], &captures[2]);
            }
            // The ID series keeps its varying digit prefix (17ID-021, 32ID-020).
            if let Some(captures) = ID_SERIES_RE.captures(&norm) {
                return format!("{}-{}", &captures[1], &captures[2]);
            }
            // Ordinary ids, with a separator first (ABC-123).
            if let Some(captures) = ORDINARY_RE.captures(&norm) {
                return format!("{}-{}", &captures[1], &captures[2]);
            }
            // Tokyo-Hot's red, sky and ex series without a separator (number ranges kept
            // small, since the series have ended).
            if let Some(captures) = TOKYO_HOT_RE.captures(&norm) {
                return captures[1].to_owned();
            }
            // Then as an ordinary id that lost its separator.
            if let Some(captures) = NO_SEPARATOR_RE.captures(&norm) {
                return format!("{}-{}", &captures[1], &captures[2]);
            }
        }
        let fallback = match_odd_numbering(&norm);
        if !fallback.is_empty() {
            return fallback;
        }
        // A few ids are separated by ')(' instead.
        if norm.contains(")(") {
            let avid = self.get_avid(&norm.replace(")(", "-"));
            if !avid.is_empty() {
                return avid;
            }
        }
        // Last, the name of the folder the file is in.
        let parent = parent_name(filepath);
        if !parent.is_empty() {
            return self.get_avid(parent);
        }
        String::new()
    }
}

/// `Path(path).parent.name`.
fn parent_name(path: &str) -> &str {
    let parts: Vec<&str> = path
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect();
    if parts.len() >= 2 { parts[parts.len() - 2] } else { "" }
}

/// `get_brand`: the part before the first `-`; an all-digit brand is cut to its first
/// [`MIN_BRAND_LENGTH`] characters. `None` without a `-`.
pub fn get_brand(avid: &str) -> Option<String> {
    let (brand, _) = avid.split_once('-')?;
    if BRAND_DIGITS_RE.is_match(brand) && char_len(brand) >= MIN_BRAND_LENGTH {
        return Some(char_prefix(brand, MIN_BRAND_LENGTH).to_owned());
    }
    Some(brand.to_owned())
}

/// `get_cd`: the disc number of a `CD<n>` marker followed by any character.
pub fn get_cd(title: &str) -> Option<String> {
    CD_RE.captures(title).map(|captures| captures[1].to_owned())
}

#[cfg(test)]
mod tests;
