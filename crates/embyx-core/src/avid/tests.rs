//! The AVID parser against the Python baseline: both corpora, the hand-written corners and
//! the ported `tests/test_avid.py` / `tests/test_avid_corpus.py`.

use std::collections::BTreeSet;
use std::path::PathBuf;

use super::{
    AvidConfig, AvidParser, HIGH_RESOLUTION_TAGS, PatternError, get_brand, get_cd, is_high_resolution,
    normalize_number, strip_extension, strip_variant_tags, variant_tags,
};
use crate::golden;
use crate::pyjson::Value;

fn parser() -> AvidParser {
    AvidParser::default()
}

fn optional(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        other => Some(golden::string(other)),
    }
}

fn strings(value: &Value) -> Vec<String> {
    golden::list(value).iter().map(golden::string).collect()
}

/// `_rows(name)` of `tests/test_avid_corpus.py`: tab-separated, `#` lines and blanks skipped.
fn corpus_rows(name: &str) -> Vec<Vec<String>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/data")
        .join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    // `str.splitlines()` also splits on \r, \v, \f, \x1c-\x1e, \x85, U+2028 and U+2029.
    text.split([
        '\n', '\r', '\u{b}', '\u{c}', '\u{1c}', '\u{1d}', '\u{1e}', '\u{85}', '\u{2028}', '\u{2029}',
    ])
    .filter(|line| !line.is_empty() && !line.starts_with('#'))
    .map(|line| line.split('\t').map(str::to_owned).collect())
    .collect()
}

/// Every field `goldens/avid/*.json` records for one title.
fn assert_parse(parser: &AvidParser, case: &Value) {
    let title = golden::string(golden::get(case, "title"));
    let avid = parser.get_avid(&title);
    assert_eq!(avid, golden::string(golden::get(case, "avid")), "get_avid({title:?})");
    assert_eq!(
        get_brand(&avid),
        optional(golden::get(case, "brand")),
        "get_brand({avid:?})"
    );
    let tags: Vec<String> = variant_tags(&title).into_iter().collect();
    assert_eq!(
        tags,
        strings(golden::get(case, "variant_tags")),
        "variant_tags({title:?})"
    );
    assert_eq!(
        strip_variant_tags(&title),
        golden::string(golden::get(case, "strip_variant_tags")),
        "strip_variant_tags({title:?})"
    );
    assert_eq!(
        strip_extension(&title),
        golden::string(golden::get(case, "strip_extension")),
        "strip_extension({title:?})"
    );
    assert_eq!(get_cd(&title), optional(golden::get(case, "cd")), "get_cd({title:?})");
}

#[test]
fn corpus_parses_match_the_python_baseline() {
    let parser = parser();
    let document = golden::load("avid/corpus.json");
    let cases = golden::cases(&document);
    assert_eq!(cases.len(), 653 + 171);
    for case in cases {
        assert_parse(&parser, case);
        if let Some(clean) = case.get("clean") {
            let clean = golden::string(clean);
            assert_eq!(
                parser.get_avid(&clean),
                golden::string(golden::get(case, "clean_avid")),
                "{clean:?}"
            );
        }
    }
}

#[test]
fn hand_written_cases_match_the_python_baseline() {
    let parser = parser();
    let document = golden::load("avid/cases.json");
    for case in golden::cases(&document) {
        assert_parse(&parser, case);
    }
}

#[test]
fn get_id_matches_the_python_baseline() {
    let parser = parser();
    let document = golden::load("avid/get_id.json");
    for case in golden::cases(&document) {
        let path = golden::string(golden::get(case, "path"));
        assert_eq!(
            parser.get_id(&path),
            golden::string(golden::get(case, "id")),
            "get_id({path:?})"
        );
    }
}

#[test]
fn normalize_number_matches_the_python_baseline() {
    let document = golden::load("avid/normalize_number.json");
    for case in golden::cases(&document) {
        let avid = golden::string(golden::get(case, "avid"));
        assert_eq!(
            normalize_number(&avid),
            golden::string(golden::get(case, "normalized")),
            "{avid:?}"
        );
    }
}

#[test]
fn brand_and_cd_match_the_python_baseline() {
    let document = golden::load("avid/brand_and_cd.json");
    for case in golden::cases(&document) {
        if let Some(avid) = case.get("avid") {
            let avid = golden::string(avid);
            assert_eq!(
                get_brand(&avid),
                optional(golden::get(case, "brand")),
                "get_brand({avid:?})"
            );
        } else {
            let title = golden::string(golden::get(case, "title"));
            assert_eq!(get_cd(&title), optional(golden::get(case, "cd")), "get_cd({title:?})");
        }
    }
}

#[test]
fn deployment_rules_match_the_python_baseline() {
    let document = golden::load("avid/rules.json");
    for case in golden::cases(&document) {
        let config = AvidConfig {
            id_exceptions: strings(golden::get(case, "id_exceptions")),
            ignored_id_patterns: strings(golden::get(case, "ignored_id_patterns")),
        };
        match AvidParser::new(config.clone()) {
            Err(error) => {
                let expected = case
                    .get("error")
                    .unwrap_or_else(|| panic!("{config:?} compiles in Python: {error}"));
                assert_eq!(golden::string(expected), PatternError::CLASS);
            }
            Ok(parser) => {
                assert!(case.get("error").is_none(), "{config:?} is refused in Python");
                for result in golden::list(golden::get(case, "results")) {
                    let title = golden::string(golden::get(result, "title"));
                    assert_eq!(
                        parser.get_avid(&title),
                        golden::string(golden::get(result, "avid")),
                        "{config:?} {title:?}"
                    );
                }
            }
        }
    }
}

// py: tests/test_avid_corpus.py::test_corpora_are_present
#[test]
fn corpora_are_present() {
    assert!(corpus_rows("avid_corpus_javsp.tsv").len() > 600);
    assert!(corpus_rows("avid_corpus_metatube.tsv").len() > 150);
}

// py: tests/test_avid_corpus.py::test_javsp_corpus cases=653
#[test]
fn javsp_corpus() {
    let parser = parser();
    let rows = corpus_rows("avid_corpus_javsp.tsv");
    assert_eq!(rows.len(), 653);
    for row in rows {
        let (name, expected) = (&row[0], &row[1]);
        let matches = parser.get_avid(name).to_uppercase() == expected.to_uppercase();
        // Rows with a reason are strict xfails in Python: the baseline gets them wrong, and
        // so must the port (the golden holds the wrong value it gives).
        if row.len() > 2 {
            assert!(!matches, "xfail row now passes: {name:?}");
        } else {
            assert!(matches, "{name:?}: {} != {expected}", parser.get_avid(name));
        }
    }
}

// py: tests/test_avid_corpus.py::test_metatube_noise_invariance cases=171
#[test]
fn metatube_noise_invariance() {
    let parser = parser();
    let rows = corpus_rows("avid_corpus_metatube.tsv");
    assert_eq!(rows.len(), 171);
    for row in rows {
        let (noisy, clean) = (&row[0], &row[1]);
        let from_clean = parser.get_avid(clean);
        assert_ne!(from_clean, "", "{clean:?}");
        assert_eq!(parser.get_avid(noisy), from_clean, "{noisy:?}");
    }
}

// py: tests/test_avid.py::test_get_avid_recognizes_common_forms cases=28
#[test]
fn get_avid_recognizes_common_forms() {
    let parser = parser();
    for (title, expected) in [
        ("ABC-123", "ABC-123"),
        ("abc-123.mp4", "ABC-123"),
        ("ABC123", "ABC-123"),
        ("[prestige] ABP-123 something", "ABP-123"),
        ("FC2-PPV-1234567", "FC2-1234567"),
        ("fc2ppv 123456", "FC2-123456"),
        ("HEYDOUGA-4017-233", "HEYDOUGA-4017-233"),
        ("hey-4017-233", "HEYDOUGA-4017-233"),
        ("GETCHU-1234567", "GETCHU-1234567"),
        ("GYUTTO-123456", "GYUTTO-123456"),
        ("259LUXU-1234", "259LUXU-1234"),
        ("32ID-020", "32ID-020"),
        ("17id_021.mp4", "17ID-021"),
        ("CW3D2BD-04 3D キャットウォーク ポイズン 04 : 波多野結衣", "CW3D2BD-04"),
        (
            "[BD-ISO] 3D CATWALK POISON 05 : Megumi Shino (CW3D2BD-05)",
            "CW3D2BD-05",
        ),
        ("CW3D2DBD-24 キャットウォーク ポイズン 24", "CW3D2DBD-24"),
        ("3D-CW3D2DBD-28.mp4", "CW3D2DBD-28"),
        ("MK3D2DBD-01", "MK3D2DBD-01"),
        ("MKBD-S03", "MKBD-S03"),
        ("S2MBD-048", "S2MBD-048"),
        ("S2M-001", "S2M-001"),
        ("IBW-123z", "IBW-123Z"),
        ("T28-557", "T28-557"),
        ("n1234", "N1234"),
        ("N-0893.strm", "N0893"),
        ("k_1234", "K1234"),
        ("123456-789", "123456-789"),
        ("123456_789", "123456_789"),
    ] {
        assert_eq!(parser.get_avid(title), expected, "{title:?}");
    }
}

// py: tests/test_avid.py::test_get_avid_reads_javlibrary_item_titles cases=5
#[test]
fn get_avid_reads_javlibrary_item_titles() {
    let parser = parser();
    for (title, expected) in [
        ("ABP-123中出し温泉旅行", "ABP-123"),
        ("START-123あの子と", "START-123"),
        ("SSIS-001 新人NO.1STYLE", "SSIS-001"),
        ("259LUXU-1234 ラグジュTV", "259LUXU-1234"),
        ("FC2-PPV-1234567 個人撮影", "FC2-1234567"),
    ] {
        assert_eq!(parser.get_avid(title), expected, "{title:?}");
    }
}

// py: tests/test_avid.py::test_get_avid_strips_suspicious_domain_prefix
#[test]
fn get_avid_strips_suspicious_domain_prefix() {
    assert_eq!(parser().get_avid("hjd2048.com-0601meyd524-h264.mp4"), "MEYD-524");
}

// py: tests/test_avid.py::test_get_avid_reads_fanza_t_id_after_domain_prefix
#[test]
fn get_avid_reads_fanza_t_id_after_domain_prefix() {
    assert_eq!(parser().get_avid("hhd800.com@T-28621.mp4"), "T-28621");
}

// py: tests/test_avid.py::test_get_avid_falls_back_to_parent_directory
#[test]
fn get_avid_falls_back_to_parent_directory() {
    assert_eq!(parser().get_avid("ABC-123/whatever!!.mp4"), "ABC-123");
}

// py: tests/test_avid.py::test_get_avid_returns_empty_when_unmatched
#[test]
fn get_avid_returns_empty_when_unmatched() {
    assert_eq!(parser().get_avid("!!!"), "");
}

// py: tests/test_avid.py::test_id_exceptions_win_over_pattern_matching
#[test]
fn id_exceptions_win_over_pattern_matching() {
    let parser = AvidParser::new(AvidConfig {
        id_exceptions: vec!["WEIRD-SPECIAL".to_owned()],
        ignored_id_patterns: Vec::new(),
    })
    .expect("valid rules");
    assert_eq!(parser.get_avid("prefix weird-special suffix 123"), "WEIRD-SPECIAL");
}

// py: tests/test_avid.py::test_ignored_id_patterns_are_stripped_before_matching
#[test]
fn ignored_id_patterns_are_stripped_before_matching() {
    let parser = AvidParser::new(AvidConfig {
        id_exceptions: Vec::new(),
        ignored_id_patterns: vec!["1080P".to_owned(), "X1080X".to_owned()],
    })
    .expect("valid rules");
    assert_eq!(parser.get_avid("ABC-123 1080P"), "ABC-123");
}

// py: tests/test_avid.py::test_get_brand cases=4
#[test]
fn brand() {
    assert_eq!(get_brand("ABC-123").as_deref(), Some("ABC"));
    assert_eq!(get_brand("259LUXU-1234").as_deref(), Some("259LUXU"));
    assert_eq!(get_brand("NOBRAND"), None);
    assert_eq!(get_brand("123456-789").as_deref(), Some("12"));
}

// py: tests/test_avid.py::test_get_cd
#[test]
fn cd() {
    assert_eq!(get_cd("ABC-123 CD2.mp4").as_deref(), Some("2"));
    assert_eq!(get_cd("ABC-123.mp4"), None);
}

// py: tests/test_avid.py::test_content_id_padding_is_removed cases=7
#[test]
fn content_id_padding_is_removed() {
    let parser = parser();
    for (title, expected) in [
        ("ABC-00123", "ABC-123"),
        ("abc00123.mp4", "ABC-123"),
        ("ssis00123", "SSIS-123"),
        ("118abp00077.mp4", "ABP-077"),
        ("ABC-00012", "ABC-012"),
        ("ABP-012", "ABP-012"),
        ("ABC-123", "ABC-123"),
    ] {
        assert_eq!(parser.get_avid(title), expected, "{title:?}");
    }
}

// py: tests/test_avid.py::test_serial_and_volume_brands_keep_their_digits cases=2
#[test]
fn serial_and_volume_brands_keep_their_digits() {
    let parser = parser();
    for title in ["FC2-00123", "MK3D2DBD-01"] {
        assert_eq!(parser.get_avid(title), title);
    }
}

// py: tests/test_avid.py::test_any_leading_zero_beyond_three_digits_is_padding
#[test]
fn any_leading_zero_beyond_three_digits_is_padding() {
    assert_eq!(parser().get_avid("ABC-0123"), "ABC-123");
}

// py: tests/test_avid.py::test_release_noise_does_not_reach_the_id cases=10
#[test]
fn release_noise_does_not_reach_the_id() {
    let parser = parser();
    for (title, expected) in [
        ("heyzo_hd_1380_full.mp4", "HEYZO-1380"),
        ("heyzo_lt_1380_full.mp4", "HEYZO-1380"),
        ("@x@imouser.com_SDMF-016_1.2K.mp4", "SDMF-016"),
        ("[Uncensored] SDDE-618", "SDDE-618"),
        ("062620-001-carib-1080p.mp4", "062620-001"),
        ("1pondo-020317-001.mp4", "020317-001"),
        ("[Bdxav.Club]Tokyo-Hot-n1340 (title)", "N1340"),
        ("[psk.la]Carib-080520-001-FHD", "080520-001"),
        ("HD_GS-333", "GS-333"),
        ("ABP-030-C-c_c-C-Cd1-cd4.mp4", "ABP-030"),
    ] {
        assert_eq!(parser.get_avid(title), expected, "{title:?}");
    }
}

// py: tests/test_avid.py::test_long_dotted_titles_keep_their_id
#[test]
fn long_dotted_titles_keep_their_id() {
    let parser = parser();
    assert_eq!(parser.get_avid("test.xxx@133ARA-030 hello"), "ARA-030");
    assert_eq!(parser.get_avid("hhd800.com@HUNTB-269"), "HUNTB-269");
}

// py: tests/test_avid.py::test_variant_tags cases=7
#[test]
fn variant_tag_sets() {
    for (name, expected) in [
        ("ABC-123-4K.mp4", &["4K"][..]),
        ("vema-181-4k-C.mp4", &["4K"]),
        ("GIGL-677_4K60FPS.mp4", &["4K", "60FPS"]),
        ("ABC-123-2160p.mkv", &["2160P"]),
        ("ABC-123-x264-leak.mp4", &["X264", "LEAK"]),
        ("ABC-123.mp4", &[]),
        ("ABC-1234k.mp4", &[]),
    ] {
        let expected: BTreeSet<String> = expected.iter().map(|tag| (*tag).to_owned()).collect();
        assert_eq!(variant_tags(name), expected, "{name:?}");
    }
}

// py: tests/test_avid.py::test_high_resolution_detection cases=5
#[test]
fn high_resolution_detection() {
    for (name, high_resolution) in [
        ("ABC-123-4K.mp4", true),
        ("ABC-123-2160P.mp4", true),
        ("ABC-123-1080p.mp4", false),
        ("ABC-123.mp4", false),
        ("4k.mp4", true),
    ] {
        let by_tags = variant_tags(name)
            .iter()
            .any(|tag| HIGH_RESOLUTION_TAGS.contains(&tag.as_str()));
        assert_eq!(by_tags, high_resolution, "{name:?}");
        assert_eq!(is_high_resolution(name), high_resolution, "{name:?}");
    }
}

// py: tests/test_avid.py::test_get_avid_normalizes_the_number_to_three_digits cases=11
#[test]
fn get_avid_normalizes_the_number_to_three_digits() {
    let parser = parser();
    for (title, expected) in [
        ("HTTM-0066", "HTTM-066"),
        ("MXDLP-0337.mp4", "MXDLP-337"),
        ("3DSVR-0707", "DSVR-707"),
        ("TBW-19", "TBW-019"),
        ("abp-01", "ABP-001"),
        ("IPZZ-1234", "IPZZ-1234"),
        ("259LUXU-1234", "259LUXU-1234"),
        ("FC2-PPV-1234567", "FC2-1234567"),
        ("T-28621", "T-28621"),
        ("IBW-949z", "IBW-949Z"),
        ("MKD-S03", "MKD-S03"),
    ] {
        assert_eq!(parser.get_avid(title), expected, "{title:?}");
    }
}

#[test]
fn ignore_case_rewrite_adds_the_dotted_and_dotless_i() {
    use crate::pyre::python_ignore_case;
    assert_eq!(python_ignore_case("(IBW)[-_]", true), "([I\u{130}\u{131}]BW)[-_]");
    assert_eq!(python_ignore_case(r"[^a-z\d]", true), "[^a-z\\d\u{130}\u{131}]");
    assert_eq!(python_ignore_case(r"[pi]\d*fps", true), "[pi\u{130}\u{131}]\\d*fps");
    assert_eq!(python_ignore_case(r"[]x]", true), "[]x]");
    assert_eq!(python_ignore_case("ID", false), "ID");
}
