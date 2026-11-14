//! The fill-actor pipeline.
//!
//! Python origin: `src/embyx_manager/fill_actor/` (without the in-memory repository
//! `persistence.py`, which is not ported: D110) and `src/embyx_manager/adapters.py`. WP10
//! ports the models, records, the PostgreSQL repository, the cloud-move paths and the port
//! adapters; WP11 the service (scan, plan, journaled local moves, CloudDrive moves and their
//! recovery: [`service`], over the file system of [`fs`]); WP12 the jobs and the API on top.

pub mod adapters;
pub mod cloud_moves;
pub mod errors;
pub mod fs;
pub mod models;
pub mod ports;
pub mod postgres_repository;
pub mod pydatetime;
pub mod records;
pub mod service;

mod pystr;

#[cfg(all(test, target_os = "linux"))]
mod probes;
