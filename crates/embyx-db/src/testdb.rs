//! Temporary databases for tests (docs/CONVENTIONS.md, "Tests").
//!
//! Each test that needs PostgreSQL creates its own database through the server named by
//! `EMBYX_MANAGER_TEST_DATABASE_URL` (the variable the Python tests read) and drops it at
//! the end. A test that needs one and does not find the variable fails; it never skips.
//! Nothing here may point at a real embyx database: the server must be a disposable one.
//!
//! Adapted from fav (`fav-db/src/testdb.rs`); see docs/CONVENTIONS.md, "Code copied from
//! fav". WP1 adds the application's `Database` type; until then the helper hands out a
//! plain pool.

use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, ConnectOptions as _, Connection as _, PgConnection, PgPool};

pub const URL_VARIABLE: &str = "EMBYX_MANAGER_TEST_DATABASE_URL";

/// Every test database's name starts with this; after a crashed run, drop the leftovers by
/// this prefix (the command is in docs/CONVENTIONS.md, "Tests").
pub const NAME_PREFIX: &str = "embyx_test_";

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A database created for one test.
#[derive(Debug)]
pub struct TestDatabase {
    name: String,
    admin: PgConnectOptions,
    options: PgConnectOptions,
    pool: PgPool,
}

/// The server's connect options from `EMBYX_MANAGER_TEST_DATABASE_URL`.
pub fn admin_options() -> PgConnectOptions {
    let url = std::env::var(URL_VARIABLE).unwrap_or_else(|_| {
        panic!("{URL_VARIABLE} is not set; database tests need a disposable PostgreSQL server (docs/CONVENTIONS.md)")
    });
    PgConnectOptions::from_str(&url).unwrap_or_else(|error| panic!("{URL_VARIABLE} is not a PostgreSQL URL: {error}"))
}

impl TestDatabase {
    /// Create an empty database named `embyx_test_<pid>_<n>_<nanos>`.
    pub async fn create() -> Self {
        let admin = admin_options();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.subsec_nanos());
        let name = format!(
            "{NAME_PREFIX}{}_{}_{nanos}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let mut connection = admin
            .connect()
            .await
            .unwrap_or_else(|error| panic!("cannot reach {URL_VARIABLE}: {error}"));
        // The name is generated above from a fixed prefix and digits only.
        sqlx::raw_sql(AssertSqlSafe(format!("CREATE DATABASE \"{name}\";")))
            .execute(&mut connection)
            .await
            .expect("create the test database");
        let _ = connection.close().await;
        let options = admin.clone().database(&name);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_lazy_with(options.clone());
        Self {
            name,
            admin,
            options,
            pool,
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Connect options for this database, for a test that needs its own connections.
    pub fn options(&self) -> &PgConnectOptions {
        &self.options
    }

    /// Close the pool and drop the database.
    pub async fn drop_database(self) {
        self.pool.close().await;
        let mut connection: PgConnection = self.admin.connect().await.expect("reconnect to drop the test database");
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE);",
            self.name
        )))
        .execute(&mut connection)
        .await
        .expect("drop the test database");
        let _ = connection.close().await;
    }
}
