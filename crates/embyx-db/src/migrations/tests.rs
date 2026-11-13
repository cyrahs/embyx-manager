//! The statements against the baseline, and the ported `tests/test_config_migrations.py`,
//! which runs migration 8's and 9's config rewrites against rows shaped the way deployments
//! could have left them.

use chrono::{TimeZone as _, Utc};
use embyx_core::config::{ArchiveConfig, CloudDriveConfig, RssConfig, Section};
use embyx_core::golden;
use embyx_core::pyjson::{self, DumpOptions, Value};
use sqlx::{AssertSqlSafe, PgPool};

use super::{CURRENT_SCHEMA_VERSION, statements};
use crate::database::{ADVISORY_NAMESPACE, ENQUEUE_LOCK_KEY, MIGRATION_LOCK_KEY, MUTATION_LOCK_KEY};
use crate::testdb::TestDatabase;

#[test]
fn statements_match_the_python_baseline() {
    let document = golden::load("db/migrations.json");
    let cases = golden::cases(&document);
    let constants = &cases[0];
    assert_eq!(
        golden::int(golden::get(constants, "current_schema_version")),
        i128::from(CURRENT_SCHEMA_VERSION)
    );
    assert_eq!(
        golden::int(golden::get(constants, "advisory_namespace")),
        i128::from(ADVISORY_NAMESPACE)
    );
    let keys = golden::get(constants, "lock_keys");
    assert_eq!(
        golden::int(golden::get(keys, "migration")),
        i128::from(MIGRATION_LOCK_KEY)
    );
    assert_eq!(golden::int(golden::get(keys, "enqueue")), i128::from(ENQUEUE_LOCK_KEY));
    assert_eq!(
        golden::int(golden::get(keys, "mutation")),
        i128::from(MUTATION_LOCK_KEY)
    );
    assert_eq!(
        golden::int(golden::get(keys, "locking.mutation")),
        i128::from(MUTATION_LOCK_KEY)
    );
    assert_eq!(
        golden::int(golden::get(keys, "locking.namespace")),
        i128::from(ADVISORY_NAMESPACE)
    );
    assert_eq!(
        crate::database::DatabaseError::UnsupportedSchemaVersion(20).to_string(),
        golden::string(golden::get(constants, "unsupported_version_message"))
    );
    let versions = &cases[1..];
    assert_eq!(
        versions.len(),
        usize::try_from(CURRENT_SCHEMA_VERSION).expect("positive")
    );
    for case in versions {
        let version = i32::try_from(golden::int(golden::get(case, "version"))).expect("small");
        let expected: Vec<String> = golden::list(golden::get(case, "statements"))
            .iter()
            .map(golden::string)
            .collect();
        assert_eq!(statements(version), expected, "migration {version}");
    }
    assert!(statements(0).is_empty() && statements(CURRENT_SCHEMA_VERSION + 1).is_empty());
}

// The statements that rewrite stored configuration, in the order they run.
fn backfill_acquisitions() -> &'static str {
    statements(8)[1]
}
fn reshape_rss_section() -> &'static str {
    statements(8)[2]
}
fn strip_clouddrive_dir() -> &'static str {
    statements(8)[3]
}
fn raise_tracker_interval() -> &'static str {
    statements(9)[0]
}

const INBOX: &str = "/115/embyx_in";

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 14, 0, 0, 0).unwrap()
}

/// `_store_section`: the values as `json.dumps` writes them.
async fn store_section(pool: &PgPool, section: &str, values: &serde_json::Value) {
    let text = pyjson::dumps(&Value::from_serde(values), &DumpOptions::python_default()).expect("JSON");
    sqlx::query(
        "
        INSERT INTO app_config (section, value_json, version, updated_at)
        VALUES ($1, $2, 1, $3)
        ON CONFLICT (section) DO UPDATE SET value_json = excluded.value_json
        ",
    )
    .bind(section)
    .bind(text)
    .bind(now())
    .execute(pool)
    .await
    .expect("store");
}

async fn execute(pool: &PgPool, statement: &'static str) {
    sqlx::raw_sql(AssertSqlSafe(statement))
        .execute(pool)
        .await
        .expect("statement");
}

async fn stored(pool: &PgPool, section: &str) -> Option<String> {
    sqlx::query_scalar("SELECT value_json FROM app_config WHERE section = $1")
        .bind(section)
        .fetch_optional(pool)
        .await
        .expect("read")
}

/// `_migrate`: store a pre-migration pair of sections, rewrite them, and read both back.
async fn migrate(
    rss: serde_json::Value,
    clouddrive: Option<serde_json::Value>,
) -> (RssConfig, Option<CloudDriveConfig>) {
    let test = TestDatabase::create().await;
    let database = test.database();
    let pool = database.get_pool().await.expect("pool");
    store_section(&pool, "rss", &rss).await;
    if let Some(clouddrive) = &clouddrive {
        store_section(&pool, "clouddrive", clouddrive).await;
    }
    for statement in [reshape_rss_section(), strip_clouddrive_dir()] {
        execute(&pool, statement).await;
    }
    let rss = RssConfig::validate_json(&stored(&pool, "rss").await.expect("rss row")).expect("valid rss");
    let clouddrive = stored(&pool, "clouddrive")
        .await
        .map(|text| CloudDriveConfig::validate_json(&text).expect("valid clouddrive"));
    database.close().await;
    test.drop_database().await;
    (rss, clouddrive)
}

fn labels(rss: &RssConfig) -> Vec<&str> {
    rss.categories.iter().map(|category| category.label.as_str()).collect()
}

// py: tests/test_config_migrations.py::test_both_labels_become_categories_on_the_old_shared_directory
#[tokio::test]
async fn both_labels_become_categories_on_the_old_shared_directory() {
    let (rss, clouddrive) = migrate(
        serde_json::json!({
            "enabled": true,
            "interval_seconds": 900,
            "actor_label": "Actor",
            "rank_label": "Rank",
            "failed_avid_cooldown_seconds": 3600,
        }),
        Some(serde_json::json!({"address": "clouddrive:19798", "api_token": "x", "task_dir_path": INBOX})),
    )
    .await;
    let categories: Vec<(&str, &str)> = rss
        .categories
        .iter()
        .map(|category| (category.label.as_str(), category.task_dir_path.as_str()))
        .collect();
    assert_eq!(categories, [("Actor", INBOX), ("Rank", INBOX)]);
    assert!(rss.enabled);
    assert_eq!(rss.interval_seconds, 900);
    assert_eq!(rss.failed_avid_cooldown_seconds, 3600);
    // The key is gone (the model forbids it, so validating proved that) and the rest kept.
    let clouddrive = clouddrive.expect("clouddrive row");
    assert_eq!(clouddrive.address, "clouddrive:19798");
}

// py: tests/test_config_migrations.py::test_renamed_labels_are_carried_over
#[tokio::test]
async fn renamed_labels_are_carried_over() {
    let (rss, _) = migrate(
        serde_json::json!({"enabled": true, "actor_label": "演员", "rank_label": "排行"}),
        Some(serde_json::json!({ "task_dir_path": INBOX })),
    )
    .await;
    assert_eq!(labels(&rss), ["演员", "排行"]);
}

// py: tests/test_config_migrations.py::test_a_blank_label_falls_back_to_the_shipped_default
#[tokio::test]
async fn a_blank_label_falls_back_to_the_shipped_default() {
    let (rss, _) = migrate(
        serde_json::json!({"enabled": true, "actor_label": "  ", "rank_label": "Rank"}),
        Some(serde_json::json!({ "task_dir_path": INBOX })),
    )
    .await;
    assert_eq!(labels(&rss), ["Actor", "Rank"]);
}

// py: tests/test_config_migrations.py::test_a_missing_label_falls_back_to_the_shipped_default
#[tokio::test]
async fn a_missing_label_falls_back_to_the_shipped_default() {
    let (rss, _) = migrate(
        serde_json::json!({"enabled": true, "rank_label": "Rank"}),
        Some(serde_json::json!({ "task_dir_path": INBOX })),
    )
    .await;
    assert_eq!(labels(&rss), ["Actor", "Rank"]);
}

// py: tests/test_config_migrations.py::test_both_labels_pointing_at_one_category_collapse_to_one_entry
#[tokio::test]
async fn both_labels_pointing_at_one_category_collapse_to_one_entry() {
    let (rss, _) = migrate(
        serde_json::json!({"enabled": true, "actor_label": "Feed", "rank_label": "Feed"}),
        Some(serde_json::json!({ "task_dir_path": INBOX })),
    )
    .await;
    assert_eq!(labels(&rss), ["Feed"]);
}

// py: tests/test_config_migrations.py::test_no_stored_directory_leaves_no_categories
#[tokio::test]
async fn no_stored_directory_leaves_no_categories() {
    let (rss, _) = migrate(
        serde_json::json!({"enabled": true, "actor_label": "Actor", "rank_label": "Rank"}),
        Some(serde_json::json!({"address": "clouddrive:19798", "task_dir_path": ""})),
    )
    .await;
    assert!(rss.categories.is_empty());
    assert!(rss.enabled);
}

// py: tests/test_config_migrations.py::test_a_section_that_was_never_written_is_left_alone
#[tokio::test]
async fn a_section_that_was_never_written_is_left_alone() {
    let test = TestDatabase::create().await;
    let database = test.database();
    let pool = database.get_pool().await.expect("pool");
    for statement in [reshape_rss_section(), strip_clouddrive_dir()] {
        execute(&pool, statement).await;
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM app_config")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(count, 0);
    database.close().await;
    test.drop_database().await;
}

// py: tests/test_config_migrations.py::test_acquisitions_keep_the_directory_they_were_submitted_to
#[tokio::test]
async fn acquisitions_keep_the_directory_they_were_submitted_to() {
    let test = TestDatabase::create().await;
    let database = test.database();
    let pool = database.get_pool().await.expect("pool");
    store_section(&pool, "clouddrive", &serde_json::json!({ "task_dir_path": INBOX })).await;
    sqlx::query(
        "
        INSERT INTO archive_acquisitions (avid, state, source, created_at, updated_at)
        VALUES ('ABC-123', 'downloading', 'rss_actor', $1, $1)
        ",
    )
    .bind(now())
    .execute(&pool)
    .await
    .expect("insert");
    execute(&pool, backfill_acquisitions()).await;
    let dir: Option<String> =
        sqlx::query_scalar("SELECT task_dir_path FROM archive_acquisitions WHERE avid = 'ABC-123'")
            .fetch_one(&pool)
            .await
            .expect("read");
    assert_eq!(dir.as_deref(), Some(INBOX));
    database.close().await;
    test.drop_database().await;
}

/// `_migrate_archive`.
async fn migrate_archive(archive: serde_json::Value) -> ArchiveConfig {
    let test = TestDatabase::create().await;
    let database = test.database();
    let pool = database.get_pool().await.expect("pool");
    store_section(&pool, "archive", &archive).await;
    execute(&pool, raise_tracker_interval()).await;
    let config = ArchiveConfig::validate_json(&stored(&pool, "archive").await.expect("row")).expect("valid");
    database.close().await;
    test.drop_database().await;
    config
}

// py: tests/test_config_migrations.py::test_the_old_default_tracker_interval_follows_the_new_default
#[tokio::test]
async fn the_old_default_tracker_interval_follows_the_new_default() {
    let archive = migrate_archive(serde_json::json!({"enabled": true, "tracker_interval_seconds": 300})).await;
    assert_eq!(archive.tracker_interval_seconds, 1800);
    assert!(archive.enabled);
}

// py: tests/test_config_migrations.py::test_a_deliberately_chosen_tracker_interval_stays
#[tokio::test]
async fn a_deliberately_chosen_tracker_interval_stays() {
    let archive = migrate_archive(serde_json::json!({"enabled": true, "tracker_interval_seconds": 600})).await;
    assert_eq!(archive.tracker_interval_seconds, 600);
}
