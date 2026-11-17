//! The config sections against the Python baseline (`goldens/config/`).

use super::{AnySection, ErrorDetail, Mode, SECTION_NAMES, ValidationError};
use crate::golden;
use crate::pyjson::{Dict, Value};

fn bool_map(entries: &[(&'static str, bool)]) -> Value {
    let mut dict = Dict::new();
    for (name, flag) in entries {
        dict.insert(*name, Value::Bool(*flag));
    }
    Value::Dict(dict)
}

fn errors_value(errors: &[ErrorDetail]) -> Value {
    Value::List(
        errors
            .iter()
            .map(|error| {
                let mut dict = Dict::new();
                dict.insert("type", Value::Str(error.kind.to_owned()));
                dict.insert("loc", Value::List(error.loc.clone()));
                dict.insert("msg", Value::Str(error.msg.clone()));
                Value::Dict(dict)
            })
            .collect(),
    )
}

fn assert_view(section: &AnySection, case: &Value, context: &str) {
    assert_eq!(
        section.dump_json(),
        golden::string(golden::get(case, "dump_json")),
        "{context}"
    );
    if let Some(masked) = case.get("masked") {
        assert_eq!(&section.masked_values(), masked, "{context}");
        assert_eq!(
            &bool_map(&section.secret_flags()),
            golden::get(case, "secret_flags"),
            "{context}"
        );
        assert_eq!(
            &bool_map(&section.properties()),
            golden::get(case, "properties"),
            "{context}"
        );
    }
}

fn assert_outcome(result: Result<AnySection, ValidationError>, case: &Value, context: &str) {
    match (result, case.get("errors")) {
        (Ok(section), None) => assert_view(&section, case, context),
        (Err(error), Some(expected)) => assert_eq!(&errors_value(&error.errors), expected, "{context}"),
        (Ok(section), Some(expected)) => panic!(
            "{context}: accepted as {}, Python refuses: {expected:?}",
            section.dump_json()
        ),
        (Err(error), None) => panic!("{context}: refused ({error}), Python accepts"),
    }
}

#[test]
fn sections_match_the_python_baseline() {
    let document = golden::load("config/sections.json");
    let cases = golden::cases(&document);
    let names: Vec<String> = cases
        .iter()
        .map(|case| golden::string(golden::get(case, "section")))
        .collect();
    assert_eq!(names, SECTION_NAMES);
    for case in cases {
        let name = golden::string(golden::get(case, "section"));
        let section = AnySection::default_for(&name).expect("registered");
        assert_eq!(section.name(), name);
        let fields: Vec<String> = golden::list(golden::get(case, "fields"))
            .iter()
            .map(golden::string)
            .collect();
        let dumped: Vec<String> = section
            .dump()
            .as_dict()
            .expect("an object")
            .iter()
            .map(|(key, _)| key.as_str().expect("field name").to_owned())
            .collect();
        assert_eq!(dumped, fields, "{name}");
        let secrets: Vec<String> = golden::list(golden::get(case, "secret_fields"))
            .iter()
            .map(golden::string)
            .collect();
        assert_eq!(section.secret_fields(), secrets, "{name}");
        assert_view(&section, case, &name);
    }
    assert!(AnySection::default_for("nope").is_none());
}

#[test]
fn model_validate_matches_the_python_baseline() {
    let document = golden::load("config/validate_python.json");
    let mut big_ints = 0;
    for case in golden::cases(&document) {
        let name = golden::string(golden::get(case, "section"));
        let values = golden::get(case, "values");
        let context = format!("{name} {values:?}");
        let result = AnySection::validate(&name, values, Mode::Python).expect("registered");
        // D10: Python takes integers of any size in an `int` field (and may then refuse them
        // as not positive); the port refuses those beyond i64 with `int_parsing_size`. A
        // float field takes them, as Python does.
        if let Err(error) = &result
            && error.errors.iter().all(|error| error.kind == "int_parsing_size")
            && case.get("errors") != Some(&errors_value(&error.errors))
        {
            big_ints += 1;
            continue;
        }
        assert_outcome(result, case, &context);
    }
    assert_eq!(big_ints, 4, "the golden's integers beyond i64");
}

#[test]
fn model_validate_json_matches_the_python_baseline() {
    let document = golden::load("config/validate_json.json");
    for case in golden::cases(&document) {
        let name = golden::string(golden::get(case, "section"));
        let text = golden::string(golden::get(case, "text"));
        let context = format!("{name} {text:?}");
        let result = AnySection::validate_json(&name, &text).expect("registered");
        if let Some(expected) = case.get("errors") {
            let expected = golden::list(expected);
            let duplicated = expected.len() == 2 && expected[0] == expected[1];
            if duplicated {
                // D12: pydantic reports an extra key given twice once per occurrence; the
                // port reads the object first and reports it once.
                let error = result.expect_err(&context);
                assert_eq!(
                    errors_value(&error.errors),
                    Value::List(vec![expected[0].clone()]),
                    "{context}"
                );
                continue;
            }
        }
        assert_outcome(result, case, &context);
    }
}

#[test]
fn floats_are_written_like_pydantic() {
    let document = golden::load("config/floats.json");
    for case in golden::cases(&document) {
        let bits = u64::try_from(golden::int(golden::get(case, "bits"))).expect("64 bits");
        let value = f64::from_bits(bits);
        let json = crate::pyjson::dumps(&Value::Float(value), &crate::pyjson::DumpOptions::python_default());
        assert_eq!(
            json.expect("a float dumps"),
            golden::string(golden::get(case, "json")),
            "{value:e}"
        );
        let section = super::MappingConfig {
            debounce_seconds: value,
            ..super::MappingConfig::default()
        };
        assert_eq!(
            super::Section::dump_json(&section),
            golden::string(golden::get(case, "dump")),
            "{value:e}"
        );
    }
}

// ---------- ported model tests ----------

use super::{
    ArchiveConfig, CloudDriveConfig, EmbyConfig, FillActorConfig, PlaylistsConfig, RssCategory, RssConfig, Section,
    masked_values, secret_flags,
};

/// `Model(**values)`: `model_validate` over Python values.
fn build<S: Section>(values: &serde_json::Value) -> Result<S, ValidationError> {
    S::validate(&Value::from_serde(values), Mode::Python)
}

fn accept<S: Section>(values: &serde_json::Value) -> S {
    build(values).unwrap_or_else(|error| panic!("{values}: {error}"))
}

/// `pytest.raises(ValueError, match=pattern)`: the pattern is searched in `str(error)`.
fn assert_refused<S: Section>(values: &serde_json::Value, pattern: &str) {
    let error = build::<S>(values).expect_err(&values.to_string());
    let pattern = regex::Regex::new(pattern).expect("pattern");
    assert!(pattern.is_match(&error.to_string()), "{values}: {error}");
}

fn pairs(entries: &[(&str, &str)]) -> Vec<(String, String)> {
    entries
        .iter()
        .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
        .collect()
}

// py: tests/test_archive_config.py::test_defaults_leave_the_pipeline_unconfigured
#[test]
fn archive_defaults_leave_the_pipeline_unconfigured() {
    let config = ArchiveConfig::default();
    assert!(config.mapping.is_empty());
    assert!(config.priority_mapping.is_empty());
    assert!(config.brand_mapping.is_empty());
    assert!(!config.configured());
}

// py: tests/test_archive_config.py::test_scan_cron_normalizes_whitespace
#[test]
fn archive_scan_cron_normalizes_whitespace() {
    assert_eq!(ArchiveConfig::default().scan_cron, "0 4 * * *");
    let config: ArchiveConfig = accept(&serde_json::json!({"scan_cron": "  30  3 * * 1 "}));
    assert_eq!(config.scan_cron, "30 3 * * 1");
}

// py: tests/test_archive_config.py::test_rejects_an_unusable_scan_cron cases=4
#[test]
fn archive_rejects_an_unusable_scan_cron() {
    for expr in ["", "every day at four", "0 4 * *", "99 4 * * *"] {
        assert_refused::<ArchiveConfig>(&serde_json::json!({ "scan_cron": expr }), "scan_cron");
    }
}

// py: tests/test_archive_config.py::test_mapping_normalizes_subdirectories
#[test]
fn archive_mapping_normalizes_subdirectories() {
    let config: ArchiveConfig = accept(&serde_json::json!({
        "src_dir": "/downloads", "dst_dir": "/library", "mapping": {" intake/ ": "./sorted"}
    }));
    assert_eq!(config.mapping, pairs(&[("intake", "sorted")]));
    assert!(config.configured());
}

// py: tests/test_archive_config.py::test_priority_mapping_normalizes_subdirectories
#[test]
fn archive_priority_mapping_normalizes_subdirectories() {
    let config: ArchiveConfig = accept(&serde_json::json!({
        "src_dir": "/downloads", "dst_dir": "/library", "priority_mapping": {" vip/ ": "./starred"}
    }));
    assert_eq!(config.priority_mapping, pairs(&[("vip", "starred")]));
    assert!(config.configured());
}

// py: tests/test_archive_config.py::test_both_tables_may_share_a_destination
#[test]
fn archive_both_tables_may_share_a_destination() {
    let config: ArchiveConfig = accept(&serde_json::json!({
        "src_dir": "/downloads", "dst_dir": "/library",
        "mapping": {"intake": "sorted"}, "priority_mapping": {"vip": "sorted"}
    }));
    assert_eq!(config.mapping, pairs(&[("intake", "sorted")]));
    assert_eq!(config.priority_mapping, pairs(&[("vip", "sorted")]));
}

// py: tests/test_archive_config.py::test_brand_mapping_upper_cases_and_deduplicates_brands
#[test]
fn archive_brand_mapping_upper_cases_and_deduplicates_brands() {
    let config: ArchiveConfig = accept(&serde_json::json!({"brand_mapping": {"special": [" abc ", "ABC", "def"]}}));
    assert_eq!(
        config.brand_mapping,
        vec![("special".to_owned(), vec!["ABC".to_owned(), "DEF".to_owned()])]
    );
}

// py: tests/test_archive_config.py::test_rejects_unusable_routes cases=11
#[test]
fn archive_rejects_unusable_routes() {
    for (values, message) in [
        (
            serde_json::json!({"mapping": {"": "sorted"}}),
            "source must not be empty",
        ),
        (
            serde_json::json!({"mapping": {"intake": ""}}),
            "destination must not be empty",
        ),
        (
            serde_json::json!({"mapping": {"/intake": "sorted"}}),
            "source must be relative",
        ),
        (
            serde_json::json!({"mapping": {"intake": "../escape"}}),
            "destination must not contain",
        ),
        (
            serde_json::json!({"brand_mapping": {"special": []}}),
            "must list at least one brand",
        ),
        (
            serde_json::json!({"brand_mapping": {"special": ["  "]}}),
            "must list at least one brand",
        ),
        (
            serde_json::json!({"brand_mapping": {"/special": ["ABC"]}}),
            "destination must be relative",
        ),
        (
            serde_json::json!({"priority_mapping": {"/vip": "starred"}}),
            r"archive\.priority_mapping source must be relative",
        ),
        (
            serde_json::json!({"priority_mapping": {"vip": "../escape"}}),
            r"archive\.priority_mapping destination must not contain",
        ),
        (
            serde_json::json!({"priority_mapping": {"vip": "starred", " vip/ ": "other"}}),
            r"archive\.priority_mapping must not repeat the source subdirectory 'vip'",
        ),
        (
            serde_json::json!({"mapping": {"intake": "sorted"}, "priority_mapping": {"./intake": "starred"}}),
            "must not share the source subdirectory 'intake'",
        ),
    ] {
        assert_refused::<ArchiveConfig>(&values, message);
    }
}

// py: tests/test_rss_config.py::test_defaults_have_no_categories
#[test]
fn rss_defaults_have_no_categories() {
    assert!(RssConfig::default().categories.is_empty());
}

// py: tests/test_rss_config.py::test_a_category_directory_is_normalized
#[test]
fn rss_category_directory_is_normalized() {
    let config: RssConfig =
        accept(&serde_json::json!({"categories": [{"label": " Rank ", "task_dir_path": "/115/embyx_in/rank/"}]}));
    assert_eq!(config.categories[0].label, "Rank");
    assert_eq!(config.categories[0].task_dir_path, "/115/embyx_in/rank");
}

// py: tests/test_rss_config.py::test_rejects_a_category_without_a_label cases=2
#[test]
fn rss_rejects_a_category_without_a_label() {
    for label in ["", "   "] {
        assert_refused::<RssConfig>(
            &serde_json::json!({"categories": [{"label": label, "task_dir_path": "/115/rank"}]}),
            "label",
        );
    }
}

// py: tests/test_rss_config.py::test_rejects_a_category_without_a_directory cases=3
#[test]
fn rss_rejects_a_category_without_a_directory() {
    for entry in [
        serde_json::json!({"label": "Rank"}),
        serde_json::json!({"label": "Rank", "task_dir_path": ""}),
        serde_json::json!({"label": "Rank", "task_dir_path": "   "}),
    ] {
        assert_refused::<RssConfig>(&serde_json::json!({ "categories": [entry] }), "task_dir_path");
    }
}

// py: tests/test_rss_config.py::test_rejects_a_repeated_label
#[test]
fn rss_rejects_a_repeated_label() {
    assert_refused::<RssConfig>(
        &serde_json::json!({"categories": [
            {"label": "Rank", "task_dir_path": "/115/a"},
            {"label": "Rank", "task_dir_path": "/115/b"},
        ]}),
        "must not repeat the label",
    );
}

// py: tests/test_rss_config.py::test_categories_may_share_one_directory
#[test]
fn rss_categories_may_share_one_directory() {
    let config: RssConfig = accept(&serde_json::json!({"categories": [
        {"label": "Actor", "task_dir_path": "/115/embyx_in"},
        {"label": "Star", "task_dir_path": "/115/embyx_in"},
    ]}));
    assert!(
        config
            .categories
            .iter()
            .all(|category| category.task_dir_path == "/115/embyx_in")
    );
    assert_eq!(config.categories.len(), 2);
}

// py: tests/test_rss_config.py::test_rejects_a_relative_category_directory
#[test]
fn rss_rejects_a_relative_category_directory() {
    assert_refused::<RssConfig>(
        &serde_json::json!({"categories": [{"label": "Rank", "task_dir_path": "embyx_in/rank"}]}),
        "absolute path",
    );
}

// py: tests/test_rss_config.py::test_rejects_an_unknown_key
#[test]
fn rss_rejects_an_unknown_key() {
    assert_refused::<RssConfig>(&serde_json::json!({"rank_label": "Rank"}), "rank_label");
}

// py: tests/test_rss_config.py::test_no_categories_is_valid_but_leaves_nothing_to_ingest
#[test]
fn rss_no_categories_is_valid_but_leaves_nothing_to_ingest() {
    let config: RssConfig = accept(&serde_json::json!({"categories": []}));
    assert!(config.categories.is_empty());
}

// py: tests/test_rss_config.py::test_a_category_round_trips_through_the_stored_json
#[test]
fn rss_category_round_trips_through_the_stored_json() {
    let config = RssConfig {
        categories: vec![RssCategory {
            label: "Rank".to_owned(),
            task_dir_path: "/115/rank".to_owned(),
        }],
        ..RssConfig::default()
    };
    assert_eq!(RssConfig::validate_json(&config.dump_json()).expect("valid"), config);
}

// py: tests/test_fill_actor_config.py::test_defaults_carry_no_deployment_paths
#[test]
fn fill_actor_defaults_carry_no_deployment_paths() {
    let config = FillActorConfig::default();
    assert_eq!(config.actor_root, "");
    assert!(config.additional_roots.is_empty());
    assert_eq!(config.move_in_root, "");
    assert!(!config.apply_enabled);
    assert!(!config.configured());
}

// py: tests/test_fill_actor_config.py::test_rejects_invalid_combinations cases=8
#[test]
fn fill_actor_rejects_invalid_combinations() {
    for (values, message) in [
        (
            serde_json::json!({"actor_root": "media/actor"}),
            "must be an absolute path",
        ),
        (serde_json::json!({"actor_root": "/a/../b"}), "must not contain"),
        (serde_json::json!({"additional_roots": ["/a", "/a"]}), "must not repeat"),
        (
            serde_json::json!({"actor_root": "/a", "additional_roots": ["/a"], "move_in_root": "/m"}),
            "must not also be an additional root",
        ),
        (
            serde_json::json!({"cloud_move_in_root": "/cloud/in"}),
            "must be configured together",
        ),
        (
            serde_json::json!({"apply_enabled": true}),
            "requires the CloudDrive move path configuration",
        ),
        (
            serde_json::json!({"root_sentinel": "nested/file"}),
            "must be a single path segment",
        ),
        (
            serde_json::json!({
                "additional_roots": ["/a", "/b"],
                "cloud_strm_mount_prefix": "/mnt",
                "cloud_source_roots": ["/cloud/a"],
                "cloud_move_in_root": "/cloud/in",
            }),
            "one-for-one",
        ),
    ] {
        assert_refused::<FillActorConfig>(&values, message);
    }
}

// py: tests/test_config.py::test_masked_values_blank_secrets
#[test]
fn masked_values_blank_secrets() {
    let config = CloudDriveConfig {
        address: "cd.internal:19798".to_owned(),
        api_token: "secret-token".to_owned(),
        ..CloudDriveConfig::default()
    };
    let masked = masked_values(&config);
    assert_eq!(masked.get("api_token"), Some(&Value::Str(String::new())));
    assert_eq!(masked.get("address"), Some(&Value::Str("cd.internal:19798".to_owned())));
    assert_eq!(secret_flags(&config), vec![("api_token", true)]);
    assert_eq!(secret_flags(&CloudDriveConfig::default()), vec![("api_token", false)]);
}

// py: tests/test_config.py::test_emby_config_normalizes_the_address_and_reports_configured
#[test]
fn emby_config_normalizes_the_address_and_reports_configured() {
    assert!(!EmbyConfig::default().configured());
    let config: EmbyConfig = accept(&serde_json::json!({"address": "http://embyx.internal:8096/", "api_key": "k"}));
    assert_eq!(config.address, "http://embyx.internal:8096");
    assert!(
        accept::<EmbyConfig>(&serde_json::json!({"address": "http://embyx.internal", "api_key": "k"})).configured()
    );
    assert!(!accept::<EmbyConfig>(&serde_json::json!({"address": "http://embyx.internal"})).configured());
    assert_refused::<EmbyConfig>(&serde_json::json!({"address": "embyx.internal:8096"}), r"emby\.address");
    assert_refused::<EmbyConfig>(
        &serde_json::json!({"address": "http://user:pw@embyx.internal"}),
        r"emby\.address",
    );
}

// py: tests/test_config.py::test_playlists_config_validates_its_fields
#[test]
fn playlists_config_validates_its_fields() {
    let default = PlaylistsConfig::default();
    assert_eq!(
        (default.enabled, default.interval_seconds, default.source_url.as_str()),
        (false, 86_400, "https://jinjier.art/sql")
    );
    let config: PlaylistsConfig = accept(&serde_json::json!({"source_url": "https://mirror.test/sql/"}));
    assert_eq!(config.source_url, "https://mirror.test/sql");
    let config: PlaylistsConfig = accept(&serde_json::json!({"task_dir_path": "/115/embyx_in/rank/"}));
    assert_eq!(config.task_dir_path, "/115/embyx_in/rank");
    assert_refused::<PlaylistsConfig>(&serde_json::json!({"interval_seconds": 0}), "interval_seconds");
    assert_refused::<PlaylistsConfig>(&serde_json::json!({"source_url": ""}), "source_url");
    assert_refused::<PlaylistsConfig>(&serde_json::json!({"task_dir_path": "relative/dir"}), "task_dir_path");
}

#[test]
fn secrets_stay_out_of_debug() {
    let cloud = CloudDriveConfig {
        api_token: "secret-token".to_owned(),
        ..CloudDriveConfig::default()
    };
    let emby = EmbyConfig {
        api_key: "secret-key".to_owned(),
        ..EmbyConfig::default()
    };
    let text = format!("{cloud:?} {emby:?} {:?}", AnySection::Emby(emby.clone()));
    assert!(!text.contains("secret-token") && !text.contains("secret-key"), "{text}");
}
