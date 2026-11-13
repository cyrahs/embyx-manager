//! The fill-actor pipeline.
//!
//! Python origin: `src/embyx_manager/fill_actor/` (without the in-memory repository
//! `persistence.py`, which is not ported). Empty in WP0 apart from the API-coverage probe for
//! the filesystem calls the journaled moves need (`probes`, tests only); WP10 to WP12 port it.

#[cfg(all(test, target_os = "linux"))]
mod probes;
