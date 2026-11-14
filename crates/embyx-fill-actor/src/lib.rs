//! The fill-actor pipeline.
//!
//! Python origin: `src/embyx_manager/fill_actor/` (without the in-memory repository
//! `persistence.py`, which is not ported: D110) and `src/embyx_manager/adapters.py`. WP10
//! ports the models, records, the PostgreSQL repository, the cloud-move paths and the port
//! adapters; WP11 and WP12 port the service and the API on top of them.

pub mod adapters;
pub mod cloud_moves;
pub mod errors;
pub mod models;
pub mod ports;
pub mod postgres_repository;
pub mod pydatetime;
pub mod records;

#[cfg(all(test, target_os = "linux"))]
mod probes;
