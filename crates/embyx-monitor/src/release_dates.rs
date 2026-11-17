//! Filling in the release date an acquisition was recorded without.
//!
//! Python origin: `src/embyx_manager/monitor/release_dates.py`. Feed items carry no release
//! date, so an AVID sighted in a feed enters the ledger without one and the resolve schedule
//! falls back to its fixed cooldown. The date is looked up the first time the row has to be
//! parked without a magnet: only rows that actually wait pay for the lookup.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use embyx_clients::avbase::{AvbaseClient, AvbaseWork};

use crate::acquisitions::{AcquisitionRecord, Ledger, LedgerError};
use crate::support::{BoxFuture, Clock, system_clock};

/// `ReleaseDateLookup`: `lookup(avid)` answers the release date the catalog knows, or `None`.
pub trait ReleaseDateLookup: Send + Sync {
    fn lookup<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Option<NaiveDate>>;
}

/// `_CatalogWork`: what the finder reads of a catalog search result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogWork {
    pub work_id: String,
    pub release_date: Option<NaiveDate>,
}

impl From<AvbaseWork> for CatalogWork {
    fn from(work: AvbaseWork) -> Self {
        Self {
            work_id: work.work_id,
            release_date: work.release_date,
        }
    }
}

/// A failed catalog search: only its text reaches the process log.
pub type SearchError = Box<dyn std::error::Error + Send + Sync>;

/// The catalog search the finder asks (`avbase.search_works` in the application).
pub trait CatalogSearch: Send + Sync {
    fn search<'a>(&'a self, query: &'a str) -> BoxFuture<'a, Result<Vec<CatalogWork>, SearchError>>;
}

impl CatalogSearch for AvbaseClient {
    fn search<'a>(&'a self, query: &'a str) -> BoxFuture<'a, Result<Vec<CatalogWork>, SearchError>> {
        Box::pin(async move {
            match self.search_works(query).await {
                Ok(works) => Ok(works.into_iter().map(CatalogWork::from).collect()),
                Err(error) => Err(Box::new(error) as SearchError),
            }
        })
    }
}

/// `ReleaseDateFinder`: release dates from a catalog search, remembering what it did not know.
///
/// A miss is cached for `miss_ttl` so a work the catalog has not listed is not asked about on
/// every retry; a failed request is not a miss and is not cached. Errors are logged and
/// answered with `None`: magnet resolution is never held up by the catalog.
pub struct ReleaseDateFinder {
    search: Arc<dyn CatalogSearch>,
    miss_ttl: TimeDelta,
    clock: Clock,
    misses: Mutex<HashMap<String, DateTime<Utc>>>,
}

impl std::fmt::Debug for ReleaseDateFinder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReleaseDateFinder")
            .field("miss_ttl", &self.miss_ttl)
            .finish_non_exhaustive()
    }
}

impl ReleaseDateFinder {
    /// `ReleaseDateFinder(search)`: a week's miss cache, the wall clock.
    pub fn new(search: Arc<dyn CatalogSearch>) -> Self {
        Self::with_options(search, TimeDelta::days(7), system_clock())
    }

    /// `ReleaseDateFinder(search, miss_ttl=, clock=)`.
    pub fn with_options(search: Arc<dyn CatalogSearch>, miss_ttl: TimeDelta, clock: Clock) -> Self {
        Self {
            search,
            miss_ttl,
            clock,
            misses: Mutex::new(HashMap::new()),
        }
    }

    fn misses(&self) -> std::sync::MutexGuard<'_, HashMap<String, DateTime<Utc>>> {
        self.misses.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `await finder(avid)`.
    pub async fn find(&self, avid: &str) -> Option<NaiveDate> {
        let now = (self.clock)();
        let missed_at = self.misses().get(avid).copied();
        if missed_at.is_some_and(|missed_at| now - missed_at < self.miss_ttl) {
            return None;
        }
        let works = match self.search.search(avid).await {
            Ok(works) => works,
            Err(error) => {
                tracing::warn!(logger = "embyx-manager.release-dates", error = %error, "Release date lookup failed for {avid}");
                return None;
            }
        };
        let wanted = caseless::default_case_fold_str(avid);
        for work in works {
            if caseless::default_case_fold_str(&work.work_id) == wanted
                && let Some(release_date) = work.release_date
            {
                self.misses().remove(avid);
                return Some(release_date);
            }
        }
        self.misses().insert(avid.to_owned(), now);
        None
    }
}

impl ReleaseDateLookup for ReleaseDateFinder {
    fn lookup<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Option<NaiveDate>> {
        Box::pin(self.find(avid))
    }
}

/// `ensure_release_date(ledger, record, lookup)`: the record's release date, looked up and
/// written back when it has none.
pub async fn ensure_release_date(
    ledger: &dyn Ledger,
    record: &AcquisitionRecord,
    lookup: Option<&dyn ReleaseDateLookup>,
) -> Result<Option<NaiveDate>, LedgerError> {
    let Some(lookup) = lookup else {
        return Ok(record.release_date);
    };
    if record.release_date.is_some() {
        return Ok(record.release_date);
    }
    let found = lookup.lookup(&record.avid).await;
    if let Some(found) = found {
        ledger.set_release_date(&record.avid, found).await?;
    }
    Ok(found)
}

#[cfg(test)]
mod tests;
