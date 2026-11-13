use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlx::{ConnectOptions as _, Connection as _};

use super::{AsyncFileLock, PostgresAdvisoryLock, RetryIntervalError};
use crate::database::{ADVISORY_NAMESPACE, MUTATION_LOCK_KEY};
use crate::testdb::TestDatabase;

// py: tests/test_locking.py::test_file_lock_serializes_independent_instances
#[tokio::test]
async fn file_lock_serializes_independent_instances() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = AsyncFileLock::new(dir.path().join("mutation.lock"));
    let second = AsyncFileLock::new(dir.path().join("mutation.lock"));
    let acquired = Arc::new(AtomicBool::new(false));
    let held = first.acquire().await.expect("first");
    let flag = Arc::clone(&acquired);
    let task = tokio::spawn(async move {
        let _guard = second.acquire().await.expect("second");
        flag.store(true, Ordering::SeqCst);
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!acquired.load(Ordering::SeqCst));
    drop(held);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("in time")
        .expect("joined");
    assert!(acquired.load(Ordering::SeqCst));
}

// py: tests/test_locking.py::test_cancelled_file_lock_waiter_stops_before_holder_releases
#[tokio::test]
async fn cancelled_file_lock_waiter_stops_before_holder_releases() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("nested").join("mutation.lock");
    let first = AsyncFileLock::with_retry_interval(&path, Duration::from_millis(5)).expect("interval");
    let second = AsyncFileLock::with_retry_interval(&path, Duration::from_millis(5)).expect("interval");
    let held = first.acquire().await.expect("first");
    let contender = second.clone();
    let waiter = tokio::spawn(async move {
        let _guard = contender.acquire().await.expect("lock");
        panic!("cancelled waiter acquired the lock");
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    waiter.abort();
    let joined = tokio::time::timeout(Duration::from_millis(200), waiter)
        .await
        .expect("in time");
    assert!(joined.expect_err("cancelled").is_cancelled());
    drop(held);
    let guard = tokio::time::timeout(Duration::from_millis(200), second.acquire())
        .await
        .expect("in time")
        .expect("lock");
    drop(guard);
}

#[test]
fn file_lock_retry_interval_must_be_positive() {
    let error = AsyncFileLock::with_retry_interval("/nonexistent/x.lock", Duration::ZERO).expect_err("refused");
    assert_eq!(error, RetryIntervalError);
    assert_eq!(error.to_string(), "lock retry interval must be positive");
}

/// Whether a fresh session can take the mutation lock right now (and gives it back).
async fn lock_is_free(database: &TestDatabase) -> bool {
    let mut connection = database.options().connect().await.expect("connect");
    let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1, $2)")
        .bind(ADVISORY_NAMESPACE)
        .bind(MUTATION_LOCK_KEY)
        .fetch_one(&mut connection)
        .await
        .expect("try");
    let _ = connection.close().await;
    taken
}

/// Poll [`lock_is_free`] for up to `patience` (a closed session's lock is freed by the
/// server shortly after the socket closes).
async fn becomes_free(database: &TestDatabase, patience: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + patience;
    loop {
        if lock_is_free(database).await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn advisory_lock_excludes_other_sessions_until_released() {
    let test = TestDatabase::create().await;
    let lock = PostgresAdvisoryLock::new(test.database());
    let guard = lock.acquire().await.expect("lock");
    assert!(!lock_is_free(&test).await);
    let contender = lock.clone();
    let waiting = tokio::spawn(async move { contender.acquire().await.expect("second").release().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiting.is_finished(), "the second holder must wait");
    guard.release().await.expect("release");
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("in time")
        .expect("joined")
        .expect("released");
    assert!(lock_is_free(&test).await);
    test.database().close().await;
    test.drop_database().await;
}

#[tokio::test]
async fn a_dropped_advisory_wait_never_takes_the_lock() {
    let test = TestDatabase::create().await;
    let database = test.database();
    let lock = PostgresAdvisoryLock::new(database.clone());
    let holder = lock.acquire().await.expect("lock");
    let contender = lock.clone();
    let waiter = tokio::spawn(async move {
        let _guard = contender.acquire().await;
        panic!("cancelled waiter acquired the lock");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    waiter.abort();
    assert!(waiter.await.expect_err("cancelled").is_cancelled());
    // Give the cancellation its round trip, then let go: nobody may be holding it after.
    tokio::time::sleep(Duration::from_millis(100)).await;
    holder.release().await.expect("release");
    assert!(
        becomes_free(&test, Duration::from_secs(5)).await,
        "a cancelled wait took the lock"
    );
    // The pool still works and the lock can be taken again.
    lock.acquire().await.expect("again").release().await.expect("release");
    database.close().await;
    test.drop_database().await;
}

#[tokio::test]
async fn an_unreleased_advisory_guard_frees_the_lock_when_dropped() {
    let test = TestDatabase::create().await;
    let database = test.database();
    let lock = PostgresAdvisoryLock::new(database.clone());
    let guard = lock.acquire().await.expect("lock");
    assert!(!lock_is_free(&test).await);
    drop(guard);
    assert!(becomes_free(&test, Duration::from_secs(5)).await);
    database.close().await;
    test.drop_database().await;
}
