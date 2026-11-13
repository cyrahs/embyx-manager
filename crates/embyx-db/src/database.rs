//! The application database: pool ownership and the schema migrations.
//!
//! Python origin: `Database` and `_migrate` in `src/embyx_manager/db.py`. All tables share
//! one migration sequence recorded in `schema_migrations`; concurrent start-up is serialized
//! with a transaction-scoped advisory lock, so exactly one process applies each pending
//! migration (cross-version contract 1). A database already at a higher version than this
//! build knows is refused, never touched.

use std::fmt;
use std::str::FromStr as _;
use std::sync::Arc;

use chrono::Utc;
use embyx_core::pytext::py_repr;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};

use crate::migrations::{self, CURRENT_SCHEMA_VERSION};

/// `ADVISORY_NAMESPACE = 0x454D4258` ('EMBX'): the first key of every advisory lock.
pub const ADVISORY_NAMESPACE: i32 = 0x454D_4258;
/// `MIGRATION_LOCK_KEY`: serializes start-up migrations.
pub const MIGRATION_LOCK_KEY: i32 = 0;
/// `ENQUEUE_LOCK_KEY`: serializes job enqueueing.
pub const ENQUEUE_LOCK_KEY: i32 = 1;
/// `MUTATION_LOCK_KEY`: the cross-process mutation lock.
pub const MUTATION_LOCK_KEY: i32 = 2;

/// Why the database could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    /// `UnsupportedSchemaVersionError` (a `RuntimeError`): the database was migrated by a
    /// newer build.
    #[error("unsupported embyx-manager database schema version: {0}")]
    UnsupportedSchemaVersion(i32),
    /// The URL is not a PostgreSQL connection URL. The text is sqlx's, which never repeats
    /// the URL.
    #[error("invalid database URL: {0}")]
    InvalidUrl(String),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

struct Inner {
    options: PgConnectOptions,
    min_pool_size: u32,
    max_pool_size: u32,
    pool: tokio::sync::Mutex<Option<PgPool>>,
}

/// `Database`: owns the pool, created and migrated on first use. Clones share it.
#[derive(Clone)]
pub struct Database {
    inner: Arc<Inner>,
}

/// The connect options carry the password: only the host and database name are shown.
impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("host", &self.inner.options.get_host())
            .field("database", &self.inner.options.get_database())
            .field("min_pool_size", &self.inner.min_pool_size)
            .field("max_pool_size", &self.inner.max_pool_size)
            .finish_non_exhaustive()
    }
}

/// `urllib.parse.urlparse(url).scheme`: the lower-cased text before the first `:` when it
/// is a letter followed by letters, digits, `+`, `-` or `.`; empty otherwise.
fn url_scheme(url: &str) -> String {
    let url: String = url
        .trim_start_matches(|ch: char| ch <= ' ')
        .chars()
        .filter(|ch| !matches!(ch, '\t' | '\r' | '\n'))
        .collect();
    let Some((scheme, _)) = url.split_once(':') else {
        return String::new();
    };
    let mut chars = scheme.chars();
    let valid = chars.next().is_some_and(|ch| ch.is_ascii_alphabetic())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'));
    if valid {
        scheme.to_ascii_lowercase()
    } else {
        String::new()
    }
}

impl Database {
    /// `Database(dsn)`, with Python's pool sizes (1 to 10).
    ///
    /// The scheme is checked first, as asyncpg does (`urlparse(dsn).scheme`): sqlx parses
    /// any scheme as PostgreSQL, and a URL without one would leave its password in the
    /// database name.
    pub fn new(dsn: &str) -> Result<Self, DatabaseError> {
        let scheme = url_scheme(dsn);
        if scheme != "postgresql" && scheme != "postgres" {
            return Err(DatabaseError::InvalidUrl(format!(
                "invalid DSN: scheme is expected to be either \"postgresql\" or \"postgres\", got {}",
                py_repr(&scheme)
            )));
        }
        let options = PgConnectOptions::from_str(dsn).map_err(|error| DatabaseError::InvalidUrl(error.to_string()))?;
        Ok(Self::from_options(options))
    }

    /// A database from parsed connect options (tests build them for a scratch database).
    pub fn from_options(options: PgConnectOptions) -> Self {
        Self::with_pool_sizes(options, 1, 10)
    }

    /// `Database(dsn, min_pool_size=..., max_pool_size=...)`.
    pub fn with_pool_sizes(options: PgConnectOptions, min_pool_size: u32, max_pool_size: u32) -> Self {
        Self {
            inner: Arc::new(Inner {
                options,
                min_pool_size,
                max_pool_size,
                pool: tokio::sync::Mutex::new(None),
            }),
        }
    }

    /// `get_pool()`: the pool, created and migrated on the first call. A failed migration
    /// closes the new pool, and the next call tries again.
    pub async fn get_pool(&self) -> Result<PgPool, DatabaseError> {
        let mut slot = self.inner.pool.lock().await;
        if let Some(pool) = slot.as_ref() {
            return Ok(pool.clone());
        }
        let pool = PgPoolOptions::new()
            .min_connections(self.inner.min_pool_size)
            .max_connections(self.inner.max_pool_size)
            .connect_with(self.inner.options.clone())
            .await?;
        if let Err(error) = migrate(&pool).await {
            pool.close().await;
            return Err(error);
        }
        *slot = Some(pool.clone());
        Ok(pool)
    }

    /// `aclose()`: close the pool; a later [`get_pool`](Self::get_pool) opens a new one.
    pub async fn close(&self) {
        if let Some(pool) = self.inner.pool.lock().await.take() {
            pool.close().await;
        }
    }
}

/// `_migrate(pool)`: apply every pending migration in one transaction.
pub async fn migrate(pool: &PgPool) -> Result<(), DatabaseError> {
    let mut transaction = pool.begin().await?;
    // Serialize concurrent process start-up against one another.
    sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind(ADVISORY_NAMESPACE)
        .bind(MIGRATION_LOCK_KEY)
        .execute(&mut *transaction)
        .await?;
    sqlx::raw_sql(
        "
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL
                )
                ",
    )
    .execute(&mut *transaction)
    .await?;
    let version: Option<i32> = sqlx::query_scalar("SELECT MAX(version) AS version FROM schema_migrations")
        .fetch_one(&mut *transaction)
        .await?;
    let version = version.unwrap_or(0);
    if version > CURRENT_SCHEMA_VERSION {
        return Err(DatabaseError::UnsupportedSchemaVersion(version));
    }
    for pending in version + 1..=CURRENT_SCHEMA_VERSION {
        for statement in migrations::statements(pending) {
            // The migrations are fixed SQL of this build, never built from data.
            sqlx::raw_sql(AssertSqlSafe(*statement))
                .execute(&mut *transaction)
                .await?;
        }
        sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES ($1, $2)")
            .bind(pending)
            .bind(Utc::now())
            .execute(&mut *transaction)
            .await?;
    }
    transaction.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests;
