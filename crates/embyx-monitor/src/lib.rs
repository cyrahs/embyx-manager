//! The monitor pipelines.
//!
//! Python origin: `src/embyx_manager/monitor/`. Empty in WP0 apart from the API-coverage
//! probes for the cron evaluator and the `.strm` watcher (`probes`, tests only); WP5 to WP9
//! port the ledger, intake, tracker, archive, mapping, RSS, playlists and the scheduler.

#[cfg(test)]
mod probes;
