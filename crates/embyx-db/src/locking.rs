//! Cross-process locks: a file lock and a PostgreSQL session advisory lock.
//!
//! Python origin: `src/embyx_manager/locking.py`. Both are cancellation-responsive: a
//! waiter whose future is dropped stops waiting and never takes the lock afterwards.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rustix::fs::{FlockOperation, flock};
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection as _, PgConnection, Postgres};

use crate::database::{ADVISORY_NAMESPACE, Database, DatabaseError, MUTATION_LOCK_KEY};

/// `AsyncFileLock`: an exclusive `flock` on a file shared by all application processes,
/// polled every `retry_interval` (the wait is an async sleep, so dropping the future stops
/// it at once).
#[derive(Debug, Clone)]
pub struct AsyncFileLock {
    path: PathBuf,
    retry_interval: Duration,
}

/// `ValueError('lock retry interval must be positive')`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("lock retry interval must be positive")]
pub struct RetryIntervalError;

/// A held [`AsyncFileLock`]; dropping it unlocks and closes the file.
#[derive(Debug)]
pub struct FileLockGuard {
    file: File,
}

impl Drop for FileLockGuard {
    fn drop(&mut self) {
        // Closing the descriptor releases the lock too; unlocking first is what Python does.
        let _ = flock(&self.file, FlockOperation::Unlock);
    }
}

impl AsyncFileLock {
    /// Python's default retry interval, 0.05 s.
    pub const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_millis(50);

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            retry_interval: Self::DEFAULT_RETRY_INTERVAL,
        }
    }

    /// `AsyncFileLock(path, retry_interval=...)`.
    pub fn with_retry_interval(path: impl Into<PathBuf>, retry_interval: Duration) -> Result<Self, RetryIntervalError> {
        if retry_interval.is_zero() {
            return Err(RetryIntervalError);
        }
        Ok(Self {
            path: path.into(),
            retry_interval,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// `acquire()`: create the parent directories and the file (`open('a+b')`), then poll a
    /// non-blocking exclusive lock until it is granted.
    pub async fn acquire(&self) -> std::io::Result<FileLockGuard> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&self.path)?;
        loop {
            match flock(&file, FlockOperation::NonBlockingLockExclusive) {
                Ok(()) => return Ok(FileLockGuard { file }),
                Err(rustix::io::Errno::WOULDBLOCK) => tokio::time::sleep(self.retry_interval).await,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

/// `PostgresAdvisoryLock`: a session advisory lock `(ADVISORY_NAMESPACE, key)` held on one
/// pooled connection for the critical section. The server queues waiters, so acquisition
/// is fair.
///
/// asyncpg cancels the server-side wait when the awaiting task is cancelled; sqlx would
/// leave the backend waiting, take the lock later and return the connection to the pool
/// holding it (`probes.rs`). So the wait runs in its own task, and when the awaiting future
/// is dropped the wait is cancelled with `pg_cancel_backend` and that connection is closed
/// rather than reused, which ends its session and any lock it may have been granted in the
/// meantime (DIVERGENCES.md D17).
#[derive(Debug, Clone)]
pub struct PostgresAdvisoryLock {
    database: Database,
    key: i32,
}

/// Why the advisory lock could not be taken or released.
#[derive(Debug, thiserror::Error)]
pub enum AdvisoryLockError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error("the advisory lock wait ended unexpectedly")]
    WaitAborted,
}

/// A held [`PostgresAdvisoryLock`]. [`release`](Self::release) unlocks and returns the
/// connection to the pool; dropping it without releasing closes the connection instead,
/// which ends the session and with it the lock.
#[derive(Debug)]
pub struct AdvisoryLockGuard {
    connection: Option<PoolConnection<Postgres>>,
    key: i32,
}

impl AdvisoryLockGuard {
    /// `pg_advisory_unlock`, then the connection goes back to the pool.
    pub async fn release(mut self) -> Result<(), AdvisoryLockError> {
        let Some(mut connection) = self.connection.take() else {
            return Ok(());
        };
        let result = sqlx::query("SELECT pg_advisory_unlock($1, $2)")
            .bind(ADVISORY_NAMESPACE)
            .bind(self.key)
            .execute(&mut *connection)
            .await;
        if result.is_err() {
            // The session state is unknown: do not hand it to the next user.
            connection.close_on_drop();
        }
        result.map(|_| ()).map_err(AdvisoryLockError::from)
    }
}

impl Drop for AdvisoryLockGuard {
    fn drop(&mut self) {
        if let Some(mut connection) = self.connection.take() {
            connection.close_on_drop();
        }
    }
}

type Wait = tokio::task::JoinHandle<(PoolConnection<Postgres>, Result<(), sqlx::Error>)>;

/// Cancels a still-running wait when the acquiring future is dropped.
///
/// The cancel goes over a connection of its own, as asyncpg's cancel request does: waiters
/// may hold every pooled connection, and a cancel queued behind them would never run.
struct PendingWait {
    wait: Option<Wait>,
    options: Arc<PgConnectOptions>,
    pid: i32,
    abandoned: Arc<AtomicBool>,
}

impl Drop for PendingWait {
    fn drop(&mut self) {
        let Some(wait) = self.wait.take() else {
            return;
        };
        self.abandoned.store(true, Ordering::SeqCst);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            // No runtime left: the process is ending and its connections with it.
            wait.abort();
            return;
        };
        let options = Arc::clone(&self.options);
        let pid = self.pid;
        runtime.spawn(async move {
            if let Ok(mut cancel) = PgConnection::connect_with(&options).await {
                let _ = sqlx::query("SELECT pg_cancel_backend($1)")
                    .bind(pid)
                    .execute(&mut cancel)
                    .await;
                let _ = cancel.close().await;
            }
            if let Ok((mut connection, _)) = wait.await {
                // Granted or cancelled, this session never goes back to the pool.
                connection.close_on_drop();
            }
        });
    }
}

impl PostgresAdvisoryLock {
    /// The mutation lock.
    pub fn new(database: Database) -> Self {
        Self::with_key(database, MUTATION_LOCK_KEY)
    }

    pub fn with_key(database: Database, key: i32) -> Self {
        Self { database, key }
    }

    /// `acquire()`: wait for the lock on a pooled connection.
    pub async fn acquire(&self) -> Result<AdvisoryLockGuard, AdvisoryLockError> {
        let pool = self.database.get_pool().await?;
        let mut connection = pool.acquire().await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await?;
        let abandoned = Arc::new(AtomicBool::new(false));
        let key = self.key;
        let flag = Arc::clone(&abandoned);
        let wait: Wait = tokio::spawn(async move {
            let result = sqlx::query("SELECT pg_advisory_lock($1, $2)")
                .bind(ADVISORY_NAMESPACE)
                .bind(key)
                .execute(&mut *connection)
                .await
                .map(|_| ());
            if flag.load(Ordering::SeqCst) {
                connection.close_on_drop();
            }
            (connection, result)
        });
        let mut pending = PendingWait {
            wait: Some(wait),
            options: pool.connect_options(),
            pid,
            abandoned,
        };
        let outcome = pending.wait.as_mut().expect("the wait was just started").await;
        pending.wait = None;
        let (mut connection, result) = outcome.map_err(|_| AdvisoryLockError::WaitAborted)?;
        if let Err(error) = result {
            connection.close_on_drop();
            return Err(error.into());
        }
        Ok(AdvisoryLockGuard {
            connection: Some(connection),
            key: self.key,
        })
    }
}

#[cfg(test)]
mod tests;
