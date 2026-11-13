//! PostgreSQL access of embyx-manager.
//!
//! Python origin: `db.py` and `locking.py`. WP1 ports the pool, the 19 migrations and the
//! advisory locks. WP0 provides the test database helper ([`testdb`], feature
//! `test-support`), the database half of the effect-transcript harness ([`scenario`], same
//! feature) and the sqlx API-coverage probe (`probes`, tests only).

#[cfg(test)]
mod probes;
#[cfg(any(test, feature = "test-support"))]
pub mod scenario;
#[cfg(any(test, feature = "test-support"))]
pub mod testdb;
