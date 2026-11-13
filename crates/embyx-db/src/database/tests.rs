use std::str::FromStr as _;

use sqlx::postgres::PgConnectOptions;

use super::{Database, DatabaseError};
use crate::migrations::CURRENT_SCHEMA_VERSION;
use crate::testdb::TestDatabase;

async fn versions(test: &TestDatabase) -> Vec<i32> {
    sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
        .fetch_all(test.pool())
        .await
        .expect("versions")
}

#[tokio::test]
async fn get_pool_migrates_once_to_the_current_version() {
    let test = TestDatabase::create().await;
    let database = test.database();
    let pool = database.get_pool().await.expect("pool");
    assert_eq!(versions(&test).await, (1..=CURRENT_SCHEMA_VERSION).collect::<Vec<_>>());
    // The same pool comes back; a second database on the same server finds nothing to do.
    let again = database.get_pool().await.expect("pool");
    assert!(pool.size() >= 1 && again.size() >= 1);
    let other = test.database();
    other.get_pool().await.expect("pool");
    assert_eq!(
        versions(&test).await.len(),
        usize::try_from(CURRENT_SCHEMA_VERSION).expect("positive")
    );
    other.close().await;
    database.close().await;
    // A closed database opens a fresh pool on the next call.
    database.get_pool().await.expect("reopened");
    database.close().await;
    test.drop_database().await;
}

#[tokio::test]
async fn concurrent_start_up_applies_each_migration_once() {
    let test = TestDatabase::create().await;
    let databases: Vec<Database> = (0..4).map(|_| test.database()).collect();
    let handles: Vec<_> = databases
        .iter()
        .cloned()
        .map(|database| tokio::spawn(async move { database.get_pool().await.map(|_| ()) }))
        .collect();
    for handle in handles {
        handle.await.expect("joined").expect("migrated");
    }
    assert_eq!(versions(&test).await, (1..=CURRENT_SCHEMA_VERSION).collect::<Vec<_>>());
    for database in databases {
        database.close().await;
    }
    test.drop_database().await;
}

#[tokio::test]
async fn a_newer_schema_is_refused_and_left_alone() {
    let test = TestDatabase::create().await;
    sqlx::raw_sql(
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL);
         INSERT INTO schema_migrations VALUES (20, now());",
    )
    .execute(test.pool())
    .await
    .expect("seed");
    let database = test.database();
    let error = database.get_pool().await.expect_err("refused");
    assert!(matches!(error, DatabaseError::UnsupportedSchemaVersion(20)), "{error}");
    assert_eq!(
        error.to_string(),
        "unsupported embyx-manager database schema version: 20"
    );
    // Nothing was created, and the next call refuses again.
    let tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'schema_migrations'",
    )
    .fetch_one(test.pool())
    .await
    .expect("count");
    assert_eq!(tables, 0);
    assert!(database.get_pool().await.is_err());
    test.drop_database().await;
}

#[test]
fn debug_leaves_the_password_out() {
    let database = Database::new("postgresql://embyx:hunter2@db.internal:5432/embyx").expect("valid");
    let text = format!("{database:?}");
    assert!(!text.contains("hunter2"), "{text}");
    assert!(text.contains("db.internal"), "{text}");
    let options = PgConnectOptions::from_str("postgresql://a:b@h/d").expect("valid");
    let text = format!("{:?}", Database::from_options(options));
    assert!(!text.contains("\"b\""), "{text}");
}

#[test]
fn an_unparseable_url_is_refused() {
    let error = Database::new("postgresql://embyx:hunter2@h:notaport/db").expect_err("refused");
    assert!(matches!(error, DatabaseError::InvalidUrl(_)), "{error}");
    assert!(!error.to_string().contains("hunter2"), "{error}");
}

#[test]
fn only_postgresql_schemes_are_accepted() {
    for dsn in [
        "postgresql://h/d",
        "postgres://h/d",
        "POSTGRES://h/d",
        " postgresql:///d",
    ] {
        Database::new(dsn).unwrap_or_else(|error| panic!("{dsn}: {error}"));
    }
    for (dsn, scheme) in [
        ("mysql://u:hunter2@h/db", "'mysql'"),
        ("embyx:hunter2@h", "'embyx'"),
        ("hunter2@h/db", "''"),
        ("", "''"),
    ] {
        let error = Database::new(dsn).expect_err("refused");
        assert_eq!(
            error.to_string(),
            format!(
                "invalid database URL: invalid DSN: scheme is expected to be either \"postgresql\" or \"postgres\", got {scheme}"
            )
        );
        assert!(!error.to_string().contains("hunter2"), "{error}");
    }
}
