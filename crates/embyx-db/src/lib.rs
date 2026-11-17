//! PostgreSQL access of embyx-manager.
//!
//! Python origin: `db.py`, `locking.py` and `config/store.py`: the pool and the 19
//! migrations ([`database`], [`migrations`]), the file and advisory locks ([`locking`]), the
//! versioned config store ([`config_store`]) and the readiness probe of `/api/health`
//! ([`health`]). Behind the `test-support` feature: the test database helper ([`testdb`])
//! and the database half of the effect-transcript harness ([`scenario`]); the sqlx
//! API-coverage probe (`probes`) is tests only.

pub mod config_store;
pub mod database;
pub mod health;
pub mod locking;
pub mod migrations;
#[cfg(test)]
mod probes;
#[cfg(any(test, feature = "test-support"))]
pub mod scenario;
#[cfg(any(test, feature = "test-support"))]
pub mod testdb;
