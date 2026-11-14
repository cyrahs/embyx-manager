//! The playlists pipeline: ranking lists from jinjier.art mirrored as Emby playlists.
//!
//! Python origin: `src/embyx_manager/monitor/playlist_sync.py`. One run refreshes the stored
//! lists when the source published a new database, reads the Emby movie index once, and then
//! brings every enabled list's playlist to its ranked, library-present titles. Order is what
//! the playlist is for, so a playlist whose entries differ from the target in any position is
//! emptied and refilled in one go rather than patched. A disabled list has its playlist
//! removed; enabling it again recreates it on the next run.
//!
//! The source failing is a warning, not a failure: the stored entries still sync. One list
//! failing on the Emby side (an `EmbyError`) is recorded on that list and the run goes on.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use embyx_clients::emby::{EmbyClient, EmbyError, EmbyPlaylist, EmbyPlaylistEntry};
use embyx_clients::jinjier::{JinjierClient, JinjierError, parse_lists};

use crate::playlists::{PlaylistRecord, PlaylistStore};
use crate::reports::{PyException, RunCancelledError, RunContext};
use crate::support::{BoxFuture, Clock, StoreError, system_clock};

/// `avid_of`: reads an AVID out of a title or directory name.
pub type AvidOf = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// The ranking source (`JinjierClient`).
pub trait RankingSource: Send + Sync {
    fn discover_database_name(&self) -> BoxFuture<'_, Result<String, JinjierError>>;
    fn download_database<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<u8>, JinjierError>>;
}

impl RankingSource for JinjierClient {
    fn discover_database_name(&self) -> BoxFuture<'_, Result<String, JinjierError>> {
        Box::pin(Self::discover_database_name(self))
    }

    fn download_database<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<u8>, JinjierError>> {
        Box::pin(Self::download_database(self, name))
    }
}

/// The Emby calls the pipeline makes (`EmbyClient`).
pub trait PlaylistServer: Send + Sync {
    /// Every movie's AVID with its item id, each AVID once, in the server's order.
    fn movie_index<'a>(
        &'a self,
        avid_of: &'a (dyn Fn(&str) -> String + Sync),
    ) -> BoxFuture<'a, Result<Vec<(String, String)>, EmbyError>>;
    fn list_playlists(&self) -> BoxFuture<'_, Result<Vec<EmbyPlaylist>, EmbyError>>;
    fn create_playlist<'a>(&'a self, name: &'a str, item_ids: &'a [String])
    -> BoxFuture<'a, Result<String, EmbyError>>;
    fn playlist_entries<'a>(&'a self, playlist_id: &'a str)
    -> BoxFuture<'a, Result<Vec<EmbyPlaylistEntry>, EmbyError>>;
    fn add_entries<'a>(&'a self, playlist_id: &'a str, item_ids: &'a [String]) -> BoxFuture<'a, Result<(), EmbyError>>;
    fn remove_entries<'a>(
        &'a self,
        playlist_id: &'a str,
        entry_ids: &'a [String],
    ) -> BoxFuture<'a, Result<(), EmbyError>>;
    fn delete_playlist<'a>(&'a self, playlist_id: &'a str) -> BoxFuture<'a, Result<(), EmbyError>>;
}

impl PlaylistServer for EmbyClient {
    fn movie_index<'a>(
        &'a self,
        avid_of: &'a (dyn Fn(&str) -> String + Sync),
    ) -> BoxFuture<'a, Result<Vec<(String, String)>, EmbyError>> {
        Box::pin(Self::movie_index(self, avid_of))
    }

    fn list_playlists(&self) -> BoxFuture<'_, Result<Vec<EmbyPlaylist>, EmbyError>> {
        Box::pin(Self::list_playlists(self))
    }

    fn create_playlist<'a>(
        &'a self,
        name: &'a str,
        item_ids: &'a [String],
    ) -> BoxFuture<'a, Result<String, EmbyError>> {
        Box::pin(Self::create_playlist(self, name, item_ids))
    }

    fn playlist_entries<'a>(
        &'a self,
        playlist_id: &'a str,
    ) -> BoxFuture<'a, Result<Vec<EmbyPlaylistEntry>, EmbyError>> {
        Box::pin(Self::playlist_entries(self, playlist_id))
    }

    fn add_entries<'a>(&'a self, playlist_id: &'a str, item_ids: &'a [String]) -> BoxFuture<'a, Result<(), EmbyError>> {
        Box::pin(Self::add_entries(self, playlist_id, item_ids))
    }

    fn remove_entries<'a>(
        &'a self,
        playlist_id: &'a str,
        entry_ids: &'a [String],
    ) -> BoxFuture<'a, Result<(), EmbyError>> {
        Box::pin(Self::remove_entries(self, playlist_id, entry_ids))
    }

    fn delete_playlist<'a>(&'a self, playlist_id: &'a str) -> BoxFuture<'a, Result<(), EmbyError>> {
        Box::pin(Self::delete_playlist(self, playlist_id))
    }
}

/// `RuntimeError`'s message when there is nothing to sync.
pub const NOTHING_STORED: &str = "no ranking lists are stored and the source could not be read";

/// Why a run ended early.
#[derive(Debug, thiserror::Error)]
pub enum PlaylistSyncError {
    #[error(transparent)]
    Cancelled(#[from] RunCancelledError),
    /// `RuntimeError`: no lists are stored and none could be downloaded.
    #[error("{NOTHING_STORED}")]
    NothingStored,
    /// The movie index or the playlist listing failed, or an error that is not an
    /// `EmbyError` came out of one list's calls.
    #[error(transparent)]
    Emby(#[from] EmbyError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl PyException for PlaylistSyncError {
    fn class_name(&self) -> &str {
        match self {
            Self::Cancelled(error) => error.class_name(),
            Self::NothingStored => "RuntimeError",
            Self::Emby(error) => error.class_name(),
            Self::Store(error) => error.class_name(),
        }
    }
}

/// One list's failure inside the loop: an `EmbyError` is contained, anything else ends the run.
enum ListError {
    Emby(EmbyError),
    Store(StoreError),
}

impl From<EmbyError> for ListError {
    fn from(error: EmbyError) -> Self {
        Self::Emby(error)
    }
}

impl From<StoreError> for ListError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

/// `dict[str, str]` from the movie index: AVID to item id.
type Index = HashMap<String, String>;

/// `PlaylistSyncPipeline`.
pub struct PlaylistSyncPipeline {
    repository: Arc<dyn PlaylistStore>,
    source: Arc<dyn RankingSource>,
    emby: Arc<dyn PlaylistServer>,
    avid_of: AvidOf,
    now: Clock,
}

impl std::fmt::Debug for PlaylistSyncPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaylistSyncPipeline").finish_non_exhaustive()
    }
}

impl PlaylistSyncPipeline {
    pub fn new(
        repository: Arc<dyn PlaylistStore>,
        source: Arc<dyn RankingSource>,
        emby: Arc<dyn PlaylistServer>,
        avid_of: AvidOf,
    ) -> Self {
        Self {
            repository,
            source,
            emby,
            avid_of,
            now: system_clock(),
        }
    }

    #[must_use]
    pub fn with_clock(mut self, now: Clock) -> Self {
        self.now = now;
        self
    }

    fn now(&self) -> DateTime<Utc> {
        (self.now)()
    }

    pub async fn run(&self, ctx: &RunContext) -> Result<(), PlaylistSyncError> {
        let records = self.refresh_lists(ctx).await?;
        ctx.check_cancelled()?;
        let index: Index = self.emby.movie_index(&*self.avid_of).await?.into_iter().collect();
        ctx.set("library_titles", i64::try_from(index.len()).unwrap_or(i64::MAX));
        let mut existing: HashSet<String> = self
            .emby
            .list_playlists()
            .await?
            .into_iter()
            .map(|playlist| playlist.playlist_id)
            .collect();
        for record in &records {
            ctx.check_cancelled()?;
            let outcome = if record.enabled {
                self.sync_one(record, &index, &mut existing, ctx).await
            } else {
                self.retire(record, &index, &mut existing, ctx).await
            };
            match outcome {
                Ok(()) => {}
                Err(ListError::Emby(error)) if error.is_emby_error() => {
                    ctx.error(&format!("{}: {error}", record.name));
                    ctx.add("lists_failed", 1);
                    self.repository
                        .record_error(&record.key, self.now(), &error.to_string())
                        .await?;
                }
                Err(ListError::Emby(error)) => return Err(error.into()),
                Err(ListError::Store(error)) => return Err(error.into()),
            }
        }
        Ok(())
    }

    // -- source ---------------------------------------------------------------

    /// Download and store the lists when the source moved on; else keep the stored ones.
    async fn refresh_lists(&self, ctx: &RunContext) -> Result<Vec<PlaylistRecord>, PlaylistSyncError> {
        let mut records = self.repository.list().await?;
        let stored = self.repository.source().await?;
        let name = match self.source.discover_database_name().await {
            Ok(name) => Some(name),
            Err(error) => {
                ctx.warning(&format!(
                    "ranking source unavailable, keeping the stored lists: {error}"
                ));
                None
            }
        };
        if let Some(name) = name
            && (stored.as_ref().is_none_or(|stored| stored.database_name != name) || records.is_empty())
        {
            let downloaded = self.source.download_database(&name).await;
            match downloaded.and_then(|database| parse_lists(&database, &*self.avid_of)) {
                Err(error) => ctx.warning(&format!(
                    "ranking database {name} could not be read, keeping the stored lists: {error}"
                )),
                Ok(lists) => {
                    let now = self.now();
                    self.repository.replace_lists(&lists, now).await?;
                    self.repository.record_source(&name, now).await?;
                    ctx.set("source_updated", 1);
                    ctx.info(&format!("stored {} lists from ranking database {name}", lists.len()));
                    records = self.repository.list().await?;
                }
            }
        }
        if records.is_empty() {
            return Err(PlaylistSyncError::NothingStored);
        }
        ctx.set("lists", i64::try_from(records.len()).unwrap_or(i64::MAX));
        Ok(records)
    }

    // -- one list ----------------------------------------------------------------

    /// `[entry.avid for entry in record.entries if (entry.avid in index) == held]`.
    fn split(record: &PlaylistRecord, index: &Index) -> (Vec<String>, Vec<String>) {
        record
            .entries
            .iter()
            .map(|entry| entry.avid.clone())
            .partition(|avid| index.contains_key(avid))
    }

    async fn sync_one(
        &self,
        record: &PlaylistRecord,
        index: &Index,
        existing: &mut HashSet<String>,
        ctx: &RunContext,
    ) -> Result<(), ListError> {
        let (present, missing) = Self::split(record, index);
        let target: Vec<String> = present.iter().map(|avid| index[avid].clone()).collect();
        let mut playlist_id = record
            .emby_playlist_id
            .clone()
            .filter(|playlist_id| existing.contains(playlist_id));
        match &playlist_id {
            None => {
                if !target.is_empty() {
                    // An empty playlist would only say the library holds nothing of this list;
                    // the missing count says that already.
                    let created = self.emby.create_playlist(&record.name, &target).await?;
                    existing.insert(created.clone());
                    playlist_id = Some(created);
                    ctx.add("lists_created", 1);
                    ctx.info(&format!(
                        "created playlist {} with {} titles",
                        record.name,
                        target.len()
                    ));
                }
            }
            Some(current_id) => {
                let current = self.emby.playlist_entries(current_id).await?;
                if current.iter().map(|entry| &entry.item_id).ne(target.iter()) {
                    let entry_ids: Vec<String> = current.into_iter().map(|entry| entry.entry_id).collect();
                    self.emby.remove_entries(current_id, &entry_ids).await?;
                    self.emby.add_entries(current_id, &target).await?;
                    ctx.add("lists_rebuilt", 1);
                    ctx.info(&format!("rebuilt playlist {}: {} titles", record.name, target.len()));
                } else {
                    ctx.add("lists_unchanged", 1);
                }
            }
        }
        self.repository
            .record_sync(&record.key, self.now(), &present, &missing, playlist_id.as_deref())
            .await?;
        ctx.add("lists_synced", 1);
        ctx.add("missing_total", i64::try_from(missing.len()).unwrap_or(i64::MAX));
        Ok(())
    }

    /// A disabled list: no playlist on the server, but its gap stays known.
    async fn retire(
        &self,
        record: &PlaylistRecord,
        index: &Index,
        existing: &mut HashSet<String>,
        ctx: &RunContext,
    ) -> Result<(), ListError> {
        if let Some(playlist_id) = &record.emby_playlist_id
            && existing.contains(playlist_id)
        {
            self.emby.delete_playlist(playlist_id).await?;
            existing.remove(playlist_id);
            ctx.add("lists_removed", 1);
            ctx.info(&format!("removed playlist {}: the list is disabled", record.name));
        }
        let (present, missing) = Self::split(record, index);
        self.repository
            .record_sync(&record.key, self.now(), &present, &missing, None)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod scenarios;
#[cfg(test)]
mod tests;
