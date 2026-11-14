//! The monitor pipelines.
//!
//! Python origin: `src/embyx_manager/monitor/`. WP2 added [`feeds`] (the feed parser). WP5
//! added the acquisition ledger ([`acquisitions`]), pipeline run history ([`runs`]) and run
//! reporting ([`reports`]), the resolve schedule ([`cadence`]), release dates
//! ([`release_dates`]), the shared intake ([`intake`]) and the subscription registry
//! ([`subscriptions`]); behind the `test-support` feature, the in-memory ledger of the Python
//! tests ([`testing`]). The API-coverage probes for the cron evaluator and the `.strm` watcher
//! (`probes`) are tests only. WP6 added the archive pipeline ([`archive`]) over a
//! `pathlib`-shaped file system ([`fs`]), the reconcile scan ([`reconcile`]) and the move-in
//! sweep ([`move_in`]). WP7 added the tracker ([`tracker`]), replacements ([`replace`], 换版)
//! and the manual source ([`manual`]), with their CloudDrive calls as traits ([`cloud`]). WP8
//! and WP9 port RSS, mapping, playlists and the scheduler.

pub mod acquisitions;
pub mod archive;
pub mod cadence;
pub mod cloud;
pub mod feeds;
pub mod fs;
pub mod intake;
pub mod manual;
pub mod mapping;
pub mod move_in;
pub mod playlist_sync;
pub mod playlists;
pub mod reconcile;
pub mod release_dates;
pub mod replace;
pub mod reports;
pub mod rss;
pub mod runs;
pub mod subscriptions;
pub mod support;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
pub mod tracker;

#[cfg(test)]
mod probes;
