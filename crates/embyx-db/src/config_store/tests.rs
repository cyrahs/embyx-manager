use embyx_core::config::{CloudDriveConfig, MappingConfig};
use embyx_core::pyjson::{Dict, Value};

use super::{ConfigStore, ConfigUpdateError, UnknownConfigSectionError};
use crate::testdb::TestDatabase;

fn values(entries: &[(&str, &str)]) -> Dict {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(*key, Value::Str((*value).to_owned()));
    }
    dict
}

// py: tests/test_config.py::test_store_defaults_and_update_round_trip
#[tokio::test]
async fn store_defaults_and_update_round_trip() {
    let test = TestDatabase::create().await;
    let store = ConfigStore::new(test.database());
    store.load().await.expect("load");
    assert_eq!(store.get::<CloudDriveConfig>().address, "");
    assert_eq!(store.get_with_version("clouddrive").expect("known").version, 0);
    let mut submitted = values(&[("address", "cd.internal:19798"), ("api_token", "secret-token")]);
    submitted.insert("secure", Value::Bool(false));
    let updated = store.update("clouddrive", &submitted, None).await.expect("update");
    assert_eq!(updated.version, 1);
    assert_eq!(
        updated.section.dump().get("address"),
        Some(&Value::Str("cd.internal:19798".to_owned()))
    );
    assert_eq!(store.get::<CloudDriveConfig>().api_token, "secret-token");
    // The stored bytes are the section's `model_dump_json()`.
    let stored: String = sqlx::query_scalar("SELECT value_json FROM app_config WHERE section = 'clouddrive'")
        .fetch_one(test.pool())
        .await
        .expect("row");
    assert_eq!(
        stored,
        r#"{"address":"cd.internal:19798","api_token":"secret-token","secure":false}"#
    );
    test.drop_database().await;
}

// py: tests/test_config.py::test_store_keeps_secret_when_empty_value_submitted
#[tokio::test]
async fn store_keeps_secret_when_empty_value_submitted() {
    let test = TestDatabase::create().await;
    let store = ConfigStore::new(test.database());
    store.load().await.expect("load");
    store
        .update(
            "clouddrive",
            &values(&[("address", "cd.internal:19798"), ("api_token", "secret-token")]),
            None,
        )
        .await
        .expect("first");
    let updated = store
        .update(
            "clouddrive",
            &values(&[("address", "cd2.internal:19798"), ("api_token", "")]),
            None,
        )
        .await
        .expect("second");
    assert_eq!(updated.version, 2);
    let embyx_core::config::AnySection::CloudDrive(section) = updated.section else {
        panic!("a clouddrive section");
    };
    assert_eq!(section.address, "cd2.internal:19798");
    assert_eq!(section.api_token, "secret-token");
    // `None` keeps it too.
    let mut cleared = Dict::new();
    cleared.insert("api_token", Value::Null);
    store.update("clouddrive", &cleared, None).await.expect("third");
    assert_eq!(store.get::<CloudDriveConfig>().api_token, "secret-token");
    test.drop_database().await;
}

// py: tests/test_config.py::test_store_version_conflict
#[tokio::test]
async fn store_version_conflict() {
    let test = TestDatabase::create().await;
    let store = ConfigStore::new(test.database());
    store.load().await.expect("load");
    store
        .update("clouddrive", &values(&[("address", "cd.internal:19798")]), None)
        .await
        .expect("first");
    let error = store
        .update("clouddrive", &values(&[("address", "cd2.internal:19798")]), Some(0))
        .await
        .expect_err("conflict");
    let ConfigUpdateError::VersionConflict(conflict) = error else {
        panic!("a version conflict: {error}");
    };
    assert_eq!(
        conflict.to_string(),
        "config section clouddrive version conflict: expected 0, stored 1"
    );
    test.drop_database().await;
}

// py: tests/test_config.py::test_store_refresh_converges_second_replica
#[tokio::test]
async fn store_refresh_converges_second_replica() {
    let test = TestDatabase::create().await;
    let writer = ConfigStore::new(test.database());
    writer.load().await.expect("load");
    writer
        .update(
            "mapping",
            &values(&[("src_dir", "/mnt/strm"), ("dst_dir", "/mnt/library")]),
            None,
        )
        .await
        .expect("update");
    let replica = ConfigStore::new(test.database());
    replica.load().await.expect("load");
    assert_eq!(replica.get::<MappingConfig>().src_dir, "/mnt/strm");
    test.drop_database().await;
}

// py: tests/test_config.py::test_store_rejects_invalid_values
#[tokio::test]
async fn store_rejects_invalid_values() {
    let test = TestDatabase::create().await;
    let store = ConfigStore::new(test.database());
    store.load().await.expect("load");
    let error = store
        .update("fill_actor", &values(&[("actor_root", "relative/actors")]), None)
        .await
        .expect_err("refused");
    assert!(matches!(error, ConfigUpdateError::Invalid(_)), "{error}");
    assert!(error.to_string().contains("absolute path"), "{error}");
    test.drop_database().await;
}

#[tokio::test]
async fn an_invalid_stored_section_keeps_the_previous_value() {
    let test = TestDatabase::create().await;
    let store = ConfigStore::new(test.database());
    store.load().await.expect("load");
    store
        .update("mapping", &values(&[("src_dir", "/mnt/strm")]), None)
        .await
        .expect("update");
    sqlx::query("UPDATE app_config SET value_json = '{\"src_dir\": 1}', version = 5 WHERE section = 'mapping'")
        .execute(test.pool())
        .await
        .expect("corrupt");
    store.refresh().await.expect("refresh");
    let kept = store.get_with_version("mapping").expect("known");
    assert_eq!(kept.version, 1);
    assert_eq!(store.get::<MappingConfig>().src_dir, "/mnt/strm");
    // A section with no row reads as its defaults at version 0.
    sqlx::query("DELETE FROM app_config")
        .execute(test.pool())
        .await
        .expect("delete");
    store.refresh().await.expect("refresh");
    assert_eq!(store.get_with_version("mapping").expect("known").version, 0);
    assert_eq!(store.get::<MappingConfig>(), MappingConfig::default());
    test.drop_database().await;
}

#[tokio::test]
async fn unknown_sections_are_refused() {
    let test = TestDatabase::create().await;
    let store = ConfigStore::new(test.database());
    let error = store.get_with_version("nope").expect_err("unknown");
    assert_eq!(
        error,
        UnknownConfigSectionError {
            section: "nope".to_owned()
        }
    );
    assert_eq!(error.to_string(), "'nope'");
    let error = store.update("nope", &Dict::new(), None).await.expect_err("unknown");
    assert!(matches!(error, ConfigUpdateError::UnknownSection(_)), "{error}");
    assert_eq!(store.sections().len(), 9);
    test.drop_database().await;
}

#[tokio::test]
async fn a_version_past_int4_is_refused_not_wrapped() {
    let test = TestDatabase::create().await;
    let store = ConfigStore::new(test.database());
    store.load().await.expect("load");
    store
        .update("mapping", &values(&[("src_dir", "/a")]), None)
        .await
        .expect("update");
    sqlx::query("UPDATE app_config SET version = 2147483647 WHERE section = 'mapping'")
        .execute(test.pool())
        .await
        .expect("set version");
    store.refresh().await.expect("refresh");
    let error = store
        .update("mapping", &values(&[("src_dir", "/b")]), None)
        .await
        .expect_err("past int4");
    assert!(matches!(error, ConfigUpdateError::Database(_)), "{error}");
    let version: i32 = sqlx::query_scalar("SELECT version FROM app_config WHERE section = 'mapping'")
        .fetch_one(test.pool())
        .await
        .expect("version");
    assert_eq!(version, i32::MAX);
    assert_eq!(store.get::<MappingConfig>().src_dir, "/a");
    test.drop_database().await;
}
