//! API-coverage probe for sqlx (docs/rewrite/API-COVERAGE.md, "sqlx").
//!
//! What the Python layer (asyncpg, `db.py`, `locking.py`) relies on, checked against a real
//! PostgreSQL server: the session time zone, the two-key advisory locks and what happens to
//! a waiter that is cancelled, `*_json` TEXT columns cast to `jsonb` in SQL, every column type
//! the 19 tables use (TEXT, BIGINT, INTEGER, BOOLEAN, DOUBLE PRECISION, TIMESTAMPTZ, DATE,
//! BIGSERIAL), array binds for `unnest`, and the unique-violation code asyncpg raises as
//! `UniqueViolationError`. No application code is ported here.

use std::time::Duration;

use chrono::{DateTime, NaiveDate, TimeZone as _, Utc};
use sqlx::Connection as _;
use sqlx::Row as _;

use crate::testdb::TestDatabase;

/// `db.py`: `ADVISORY_NAMESPACE = 0x454D4258` ('EMBX'), which fits PostgreSQL's `int4` key.
const ADVISORY_NAMESPACE: i32 = 0x454D_4258;
/// `db.py`: `MUTATION_LOCK_KEY`.
const MUTATION_LOCK_KEY: i32 = 2;

#[tokio::test]
async fn sessions_run_in_utc_whatever_the_server_default() {
    let database = TestDatabase::create().await;
    // asyncpg leaves the server's TimeZone alone; sqlx sets UTC on every connection. Aware
    // timestamps are equal either way (binary protocol); only text renderings differ.
    let row =
        sqlx::query("SELECT current_setting('TimeZone') AS zone, '2026-01-02 03:04:05+00'::timestamptz::text AS text")
            .fetch_one(database.pool())
            .await
            .expect("query");
    assert_eq!(row.get::<String, _>("zone"), "UTC");
    assert_eq!(row.get::<String, _>("text"), "2026-01-02 03:04:05+00");
    database.drop_database().await;
}

#[tokio::test]
async fn every_column_type_of_the_schema_round_trips() {
    let database = TestDatabase::create().await;
    let pool = database.pool();
    sqlx::raw_sql(
        "CREATE TABLE probe (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            size BIGINT,
            attempts INTEGER NOT NULL DEFAULT 0,
            paused BOOLEAN NOT NULL DEFAULT FALSE,
            progress DOUBLE PRECISION,
            created_at TIMESTAMPTZ NOT NULL,
            release_date DATE,
            value_json TEXT NOT NULL DEFAULT '[]'
        )",
    )
    .execute(pool)
    .await
    .expect("create");
    let at = Utc.with_ymd_and_hms(2026, 11, 13, 18, 22, 0).unwrap();
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO probe (name, size, progress, created_at, release_date, value_json)
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind("UNVRSP-003")
    .bind(115_000_000_000_i64)
    .bind(0.5_f64)
    .bind(at)
    .bind(NaiveDate::from_ymd_opt(2026, 1, 2))
    .bind(r#"{"cloud_name": "115", "keep": [1, 1.0]}"#)
    .fetch_one(pool)
    .await
    .expect("insert");
    assert_eq!(id, 1);
    let row = sqlx::query("SELECT * FROM probe WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("select");
    assert_eq!(row.get::<String, _>("name"), "UNVRSP-003");
    assert_eq!(row.get::<Option<i64>, _>("size"), Some(115_000_000_000));
    assert_eq!(row.get::<i32, _>("attempts"), 0);
    assert!(!row.get::<bool, _>("paused"));
    assert_eq!(row.get::<Option<f64>, _>("progress"), Some(0.5));
    assert_eq!(row.get::<DateTime<Utc>, _>("created_at"), at);
    assert_eq!(
        row.get::<Option<NaiveDate>, _>("release_date"),
        NaiveDate::from_ymd_opt(2026, 1, 2)
    );
    // The JSON columns are TEXT: the bytes the writer stored come back unchanged
    // (contract 2, stored JSON), and `::jsonb` edits work as the migrations use them.
    assert_eq!(
        row.get::<String, _>("value_json"),
        r#"{"cloud_name": "115", "keep": [1, 1.0]}"#
    );
    let edited: String = sqlx::query_scalar(
        "UPDATE probe SET value_json = (value_json::jsonb - 'cloud_name')::text RETURNING value_json",
    )
    .fetch_one(pool)
    .await
    .expect("jsonb edit");
    assert_eq!(edited, r#"{"keep": [1, 1.0]}"#);
    // A parsed jsonb value comes back as serde_json.
    let parsed: sqlx::types::Json<serde_json::Value> = sqlx::query_scalar("SELECT value_json::jsonb FROM probe")
        .fetch_one(pool)
        .await
        .expect("jsonb");
    assert_eq!(parsed.0, serde_json::json!({"keep": [1, 1.0]}));
    // Arrays bind for `unnest` and `= ANY($1)`.
    let names: Vec<String> =
        sqlx::query_scalar("SELECT x FROM unnest($1::text[]) WITH ORDINALITY AS t(x, n) ORDER BY n")
            .bind(vec!["b".to_owned(), "a".to_owned()])
            .fetch_all(pool)
            .await
            .expect("unnest");
    assert_eq!(names, ["b", "a"]);
    let nulls = sqlx::query("SELECT * FROM probe WHERE size = ANY($1)")
        .bind(vec![115_000_000_000_i64])
        .fetch_all(pool)
        .await
        .expect("any");
    assert_eq!(nulls.len(), 1);
    database.drop_database().await;
}

#[tokio::test]
async fn a_unique_violation_is_known_by_its_sqlstate() {
    let database = TestDatabase::create().await;
    let pool = database.pool();
    sqlx::raw_sql(
        "CREATE TABLE tasks (id BIGSERIAL PRIMARY KEY, avid TEXT NOT NULL, state TEXT NOT NULL);
         CREATE UNIQUE INDEX tasks_one_open_per_avid ON tasks (avid) WHERE state NOT IN ('done', 'cancelled');
         INSERT INTO tasks (avid, state) VALUES ('ABC-123', 'queued');",
    )
    .execute(pool)
    .await
    .expect("setup");
    let error = sqlx::query("INSERT INTO tasks (avid, state) VALUES ('ABC-123', 'queued')")
        .execute(pool)
        .await
        .expect_err("second open task");
    let database_error = error.as_database_error().expect("a server error");
    // asyncpg.UniqueViolationError is SQLSTATE 23505.
    assert_eq!(database_error.code().as_deref(), Some("23505"));
    assert!(database_error.is_unique_violation());
    assert_eq!(database_error.constraint(), Some("tasks_one_open_per_avid"));
    database.drop_database().await;
}

#[tokio::test]
async fn two_key_advisory_locks_exclude_other_sessions() {
    let database = TestDatabase::create().await;
    let mut holder = sqlx::PgConnection::connect_with(database.options())
        .await
        .expect("holder");
    let mut other = sqlx::PgConnection::connect_with(database.options())
        .await
        .expect("other");
    sqlx::query("SELECT pg_advisory_lock($1, $2)")
        .bind(ADVISORY_NAMESPACE)
        .bind(MUTATION_LOCK_KEY)
        .execute(&mut holder)
        .await
        .expect("lock");
    let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1, $2)")
        .bind(ADVISORY_NAMESPACE)
        .bind(MUTATION_LOCK_KEY)
        .fetch_one(&mut other)
        .await
        .expect("try");
    assert!(!taken, "a second session must not get the lock");
    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1, $2)")
        .bind(ADVISORY_NAMESPACE)
        .bind(MUTATION_LOCK_KEY)
        .fetch_one(&mut holder)
        .await
        .expect("unlock");
    assert!(released);
    // The migration lock is the transaction-scoped form; it ends with the transaction.
    let mut transaction = holder.begin().await.expect("begin");
    sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind(ADVISORY_NAMESPACE)
        .bind(0_i32)
        .execute(&mut *transaction)
        .await
        .expect("xact lock");
    let during: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1, 0)")
        .bind(ADVISORY_NAMESPACE)
        .fetch_one(&mut other)
        .await
        .expect("try during");
    assert!(!during);
    transaction.commit().await.expect("commit");
    let after: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1, 0)")
        .bind(ADVISORY_NAMESPACE)
        .fetch_one(&mut other)
        .await
        .expect("try after");
    assert!(after);
    drop(holder);
    drop(other);
    database.drop_database().await;
}

/// Hold the mutation lock on a dedicated connection; returns it.
async fn hold_lock(database: &TestDatabase) -> sqlx::PgConnection {
    let mut holder = sqlx::PgConnection::connect_with(database.options())
        .await
        .expect("holder");
    sqlx::query("SELECT pg_advisory_lock($1, $2)")
        .bind(ADVISORY_NAMESPACE)
        .bind(MUTATION_LOCK_KEY)
        .execute(&mut holder)
        .await
        .expect("lock");
    holder
}

async fn release_lock(holder: &mut sqlx::PgConnection) {
    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1, $2)")
        .bind(ADVISORY_NAMESPACE)
        .bind(MUTATION_LOCK_KEY)
        .fetch_one(holder)
        .await
        .expect("unlock");
    assert!(released);
}

/// Whether a fresh session can take the lock within `patience`.
async fn lock_is_free(database: &TestDatabase, patience: Duration) -> bool {
    let mut third = sqlx::PgConnection::connect_with(database.options())
        .await
        .expect("third");
    let deadline = tokio::time::Instant::now() + patience;
    loop {
        let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1, $2)")
            .bind(ADVISORY_NAMESPACE)
            .bind(MUTATION_LOCK_KEY)
            .fetch_one(&mut third)
            .await
            .expect("try");
        if taken || tokio::time::Instant::now() >= deadline {
            return taken;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// asyncpg cancels the server-side wait when the awaiting task is cancelled (`locking.py`,
/// `PostgresAdvisoryLock`: "cancelling the awaiting task cancels the server-side wait").
/// sqlx does not: dropping the future that waits in `pg_advisory_lock` on a pooled
/// connection leaves the backend waiting, it takes the lock once the holder lets go, and the
/// connection goes back to the pool holding it, so nothing else ever gets the lock. The
/// port must not wait this way (next test).
#[tokio::test]
async fn a_dropped_wait_on_a_pooled_connection_takes_the_lock_later() {
    let database = TestDatabase::create().await;
    let mut holder = hold_lock(&database).await;
    let pool = database.pool().clone();
    let waiter = async move {
        let mut connection = pool.acquire().await.expect("acquire");
        sqlx::query("SELECT pg_advisory_lock($1, $2)")
            .bind(ADVISORY_NAMESPACE)
            .bind(MUTATION_LOCK_KEY)
            .execute(&mut *connection)
            .await
            .expect("lock");
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(300), waiter).await.is_err(),
        "the waiter must still be waiting when it is cancelled"
    );
    release_lock(&mut holder).await;
    assert!(
        !lock_is_free(&database, Duration::from_secs(1)).await,
        "sqlx now ends a dropped wait; revisit API-COVERAGE.md, \"sqlx\""
    );
    drop(holder);
    database.drop_database().await;
}

/// The port's way: note the waiting session's backend pid, and on cancellation cancel the
/// wait with `pg_cancel_backend` from another connection (what asyncpg's cancel request
/// does). The wait fails with `query_canceled` (57014) and never takes the lock.
#[tokio::test]
async fn a_wait_cancelled_with_pg_cancel_backend_never_takes_the_lock() {
    let database = TestDatabase::create().await;
    let mut holder = hold_lock(&database).await;
    let pool = database.pool().clone();
    let mut waiting = pool.acquire().await.expect("acquire");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *waiting)
        .await
        .expect("pid");
    let wait = tokio::spawn(async move {
        sqlx::query("SELECT pg_advisory_lock($1, $2)")
            .bind(ADVISORY_NAMESPACE)
            .bind(MUTATION_LOCK_KEY)
            .execute(&mut *waiting)
            .await
            .map(|_| ())
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !wait.is_finished(),
        "the waiter must still be waiting when it is cancelled"
    );

    let cancelled: bool = sqlx::query_scalar("SELECT pg_cancel_backend($1)")
        .bind(pid)
        .fetch_one(database.pool())
        .await
        .expect("cancel");
    assert!(cancelled);
    let error = wait.await.expect("joined").expect_err("the wait was cancelled");
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("57014")
    );

    release_lock(&mut holder).await;
    assert!(
        lock_is_free(&database, Duration::from_secs(1)).await,
        "a cancelled wait took the lock"
    );
    drop(holder);
    database.drop_database().await;
}
