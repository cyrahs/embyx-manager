//! The database readiness probe behind `/api/health`.
//!
//! Python origin: `PostgresFillActorRepository.health_check` (`fill_actor/postgres_repository.py`),
//! which `bootstrap.py` hands to `create_app` as `app_ready`. It needs nothing of the fill
//! actor, so it lives with the database here; WP10 and WP12 call this rather than porting it
//! again with the repository.

use chrono::Utc;

use crate::database::{Database, DatabaseError};
use crate::migrations::CURRENT_SCHEMA_VERSION;

/// `health_check()`: a write the database must accept (an upsert into `health_probe`,
/// rolled back) and the schema version this build expects. A connection or PostgreSQL
/// failure is `false`; a database migrated by a newer build is the error Python lets
/// through (`UnsupportedSchemaVersionError`).
pub async fn health_check(database: &Database) -> Result<bool, DatabaseError> {
    let probe = async {
        let pool = database.get_pool().await?;
        let mut transaction = pool.begin().await?;
        let version: Option<i32> = sqlx::query_scalar("SELECT MAX(version) FROM schema_migrations")
            .fetch_one(&mut *transaction)
            .await?;
        sqlx::query(
            "
                        INSERT INTO health_probe (id, checked_at) VALUES (1, $1)
                        ON CONFLICT (id) DO UPDATE SET checked_at = excluded.checked_at
                        ",
        )
        .bind(Utc::now())
        .execute(&mut *transaction)
        .await?;
        transaction.rollback().await?;
        Ok::<_, DatabaseError>(version)
    };
    match probe.await {
        Ok(version) => Ok(version == Some(CURRENT_SCHEMA_VERSION)),
        Err(error @ DatabaseError::UnsupportedSchemaVersion(_)) => Err(error),
        Err(DatabaseError::Sqlx(_) | DatabaseError::InvalidUrl(_)) => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use crate::testdb::TestDatabase;

    use super::health_check;

    #[tokio::test]
    async fn a_migrated_database_is_ready_and_the_probe_leaves_nothing_behind() {
        let database = TestDatabase::create().await;
        assert!(health_check(&database.database()).await.expect("no newer schema"));
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM health_probe")
            .fetch_one(database.pool())
            .await
            .expect("count");
        assert_eq!(rows, 0, "the upsert is rolled back");
        // A database a newer build migrated is refused, not reported as not ready.
        sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES (99, now())")
            .execute(database.pool())
            .await
            .expect("insert");
        let fresh = crate::database::Database::from_options(database.options().clone());
        assert!(health_check(&fresh).await.is_err());
        fresh.close().await;
        database.drop_database().await;
    }

    #[tokio::test]
    async fn an_unreachable_database_is_not_ready() {
        let options = sqlx::postgres::PgConnectOptions::new()
            .host("127.0.0.1")
            .port(9)
            .username("nobody")
            .database("nothing");
        let database = crate::database::Database::from_options(options);
        assert!(!health_check(&database).await.expect("a connection failure is false"));
    }
}
