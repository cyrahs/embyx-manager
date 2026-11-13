//! The monitor pipelines.
//!
//! Python origin: `src/embyx_manager/monitor/`. WP2 added [`feeds`] (the feed parser); the
//! rest is the API-coverage probes for the cron evaluator and the `.strm` watcher (`probes`,
//! tests only) until WP5 to WP9 port the ledger, intake, tracker, archive, mapping, RSS,
//! playlists and the scheduler.

pub mod feeds;

#[cfg(test)]
mod probes;
