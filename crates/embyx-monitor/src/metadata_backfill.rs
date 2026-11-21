//! Metadata backfill: movies Emby filed without `MetaTube` metadata get their refresh again.
//!
//! No Python origin: added after the rewrite (D179). Emby asks its metadata providers once,
//! when a `.strm` appears. When that one `MetaTube` call fails (a connection reset on the way to
//! the server, the server briefly down), Emby still records the item as refreshed and never
//! asks again, so the title stays a bare AVID without a poster, title or cast. On 2026-11-20,
//! 164 of 33,293 movies were in that state and 134 of them resolved at once on a new search.
//!
//! A pass reads the movie list once, takes the movies without a `MetaTube` provider id that are
//! older than [`BackfillSettings::min_age`] (Emby may still be working on a new one), newest
//! first, and queues a full metadata refresh for each, one every [`BackfillSettings::pacing`]
//! seconds and at most [`BackfillSettings::per_pass`] per pass. A movie is tried at most
//! [`BackfillSettings::max_attempts`] times per process: titles no provider knows (FC2,
//! uncensored date numbers) are not refreshed every day forever. The first pass runs
//! [`BackfillSettings::first_pass_after`] seconds after startup, then one every
//! [`BackfillSettings::interval`] seconds. The Emby address and key are the `emby` section's,
//! read at every pass; without them the pass is skipped. Nothing is stored.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, TimeDelta};
use embyx_clients::emby::{EmbyClient, EmbyError, EmbyMovie};
use embyx_clients::net::retry::Sleeper;
use tokio_util::sync::CancellationToken;

use crate::support::{BoxFuture, Clock};

/// The logger name the backfill reports under.
pub const LOGGER: &str = "embyx-manager.metadata-backfill";
/// The provider id a movie with metadata carries (the `MetaTube` plugin's key).
pub const PROVIDER: &str = "MetaTube";

/// The Emby calls a pass makes.
pub trait BackfillServer: Send + Sync {
    fn movies(&self) -> BoxFuture<'_, Result<Vec<EmbyMovie>, EmbyError>>;
    fn refresh_metadata<'a>(&'a self, item_id: &'a str) -> BoxFuture<'a, Result<(), EmbyError>>;
}

impl BackfillServer for EmbyClient {
    fn movies(&self) -> BoxFuture<'_, Result<Vec<EmbyMovie>, EmbyError>> {
        Box::pin(Self::movies(self))
    }

    fn refresh_metadata<'a>(&'a self, item_id: &'a str) -> BoxFuture<'a, Result<(), EmbyError>> {
        Box::pin(Self::refresh_metadata(self, item_id))
    }
}

/// How often and how hard the backfill works.
#[derive(Debug, Clone, PartialEq)]
pub struct BackfillSettings {
    /// Seconds from startup to the first pass.
    pub first_pass_after: f64,
    /// Seconds from one pass's end to the next pass.
    pub interval: f64,
    /// Refreshes queued per pass at most.
    pub per_pass: usize,
    /// Seconds between two refreshes, so Emby and `MetaTube` take one title at a time.
    pub pacing: f64,
    /// A movie created less than this long ago is left to Emby's own first refresh.
    pub min_age: TimeDelta,
    /// Refreshes of one movie per process at most.
    pub max_attempts: u32,
}

impl Default for BackfillSettings {
    fn default() -> Self {
        Self {
            first_pass_after: 600.0,
            interval: 86_400.0,
            per_pass: 200,
            pacing: 15.0,
            min_age: TimeDelta::hours(1),
            max_attempts: 3,
        }
    }
}

/// What one pass found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassReport {
    /// Movies in the library.
    pub movies: usize,
    /// Movies without a `MetaTube` id.
    pub missing: usize,
    /// Of those, created within `min_age`.
    pub too_new: usize,
    /// Of those, already refreshed `max_attempts` times by this process.
    pub given_up: usize,
    /// Names of the movies whose refresh was queued, in order.
    pub refreshed: Vec<String>,
    /// Refreshes the server refused or that did not reach it.
    pub failed: usize,
    /// The pass stopped early (shutdown).
    pub stopped: bool,
}

impl PassReport {
    /// The one log line a pass ends with.
    pub fn summary(&self) -> String {
        let mut text = format!(
            "metadata backfill: {} movies, {} without {PROVIDER} metadata ({} too new, {} given up); \
             queued {} refreshes, {} failed",
            self.movies,
            self.missing,
            self.too_new,
            self.given_up,
            self.refreshed.len(),
            self.failed,
        );
        if self.stopped {
            text.push_str("; stopped early");
        }
        text
    }
}

/// Builds the server for a pass from the live settings; `None` when Emby is not configured.
pub type ServerFor = Arc<dyn Fn() -> Option<Arc<dyn BackfillServer>> + Send + Sync>;

/// The backfill loop and its per-process attempt counts.
pub struct MetadataBackfill {
    settings: BackfillSettings,
    clock: Clock,
    sleeper: Sleeper,
    attempts: Mutex<HashMap<String, u32>>,
}

impl std::fmt::Debug for MetadataBackfill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataBackfill")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl MetadataBackfill {
    pub fn new(settings: BackfillSettings, clock: Clock, sleeper: Sleeper) -> Self {
        Self {
            settings,
            clock,
            sleeper,
            attempts: Mutex::new(HashMap::new()),
        }
    }

    /// One pass. An error reading the movie list, or a rejected key, ends it with that error;
    /// a refresh that fails otherwise is counted and the pass goes on.
    pub async fn pass(&self, server: &dyn BackfillServer, stop: &CancellationToken) -> Result<PassReport, EmbyError> {
        let movies = server.movies().await?;
        let now = (self.clock)();
        let mut report = PassReport {
            movies: movies.len(),
            ..PassReport::default()
        };
        let mut due: Vec<&EmbyMovie> = Vec::new();
        {
            let attempts = self.attempts.lock().unwrap_or_else(PoisonError::into_inner);
            for movie in movies.iter().filter(|movie| !movie.has_provider(PROVIDER)) {
                report.missing += 1;
                let created = DateTime::parse_from_rfc3339(&movie.date_created).ok();
                if created.is_some_and(|created| now.signed_duration_since(created) < self.settings.min_age) {
                    report.too_new += 1;
                } else if attempts.get(&movie.item_id).copied().unwrap_or(0) >= self.settings.max_attempts {
                    report.given_up += 1;
                } else {
                    due.push(movie);
                }
            }
        }
        // Newest first: a title that just failed is the one someone is looking for. ISO 8601
        // in UTC sorts as text; a movie without a date goes last.
        due.sort_by(|a, b| b.date_created.cmp(&a.date_created));
        for (index, movie) in due.into_iter().take(self.settings.per_pass).enumerate() {
            let go_on = if index == 0 {
                !stop.is_cancelled()
            } else {
                self.pause(stop).await
            };
            if !go_on {
                report.stopped = true;
                break;
            }
            *self
                .attempts
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(movie.item_id.clone())
                .or_insert(0) += 1;
            match server.refresh_metadata(&movie.item_id).await {
                Ok(()) => report.refreshed.push(movie.name.clone()),
                Err(error @ EmbyError::Auth(_)) => return Err(error),
                Err(error) => {
                    tracing::warn!(logger = LOGGER, error = %error, "refresh of {} failed", movie.name);
                    report.failed += 1;
                }
            }
        }
        Ok(report)
    }

    /// The loop: the first pass after `first_pass_after`, then one every `interval`, until
    /// `stop` fires.
    pub async fn run(self: Arc<Self>, server_for: ServerFor, stop: CancellationToken) {
        if !self.sleep(self.settings.first_pass_after, &stop).await {
            return;
        }
        loop {
            let Some(server) = server_for() else {
                tracing::info!(logger = LOGGER, "metadata backfill skipped: Emby is not configured");
                if !self.sleep(self.settings.interval, &stop).await {
                    return;
                }
                continue;
            };
            match self.pass(server.as_ref(), &stop).await {
                // The app's own loggers print WARNING and above only (as Python's root logger
                // does), so a pass that did something says so at that level; a pass that found
                // nothing to do stays quiet.
                Ok(report) if report.refreshed.is_empty() && report.failed == 0 => {
                    tracing::info!(logger = LOGGER, "{}", report.summary());
                }
                Ok(report) => {
                    tracing::warn!(logger = LOGGER, "{}: {}", report.summary(), report.refreshed.join(", "));
                }
                Err(error) => tracing::warn!(logger = LOGGER, error = %error, "metadata backfill pass failed"),
            }
            if !self.sleep(self.settings.interval, &stop).await {
                return;
            }
        }
    }

    async fn pause(&self, stop: &CancellationToken) -> bool {
        self.sleep(self.settings.pacing, stop).await
    }

    /// Sleep `seconds`; `false` when `stop` fired first.
    async fn sleep(&self, seconds: f64, stop: &CancellationToken) -> bool {
        if stop.is_cancelled() {
            return false;
        }
        tokio::select! {
            biased;
            () = stop.cancelled() => false,
            () = (self.sleeper)(seconds) => !stop.is_cancelled(),
        }
    }
}

#[cfg(test)]
mod tests;
