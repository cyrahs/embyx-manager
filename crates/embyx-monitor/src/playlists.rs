//! Ranking lists mirrored as Emby playlists: the stored lists and their sync state.
//!
//! Python origin: `src/embyx_manager/monitor/playlists.py`. One row per list the ranking
//! source publishes. The entries are the source's ranked codes after the library's exclusions;
//! `present` and `missing` are what the last sync found in the Emby index, in ranking order.
//! `enabled` is the operator's choice: a disabled list keeps its entries and statistics but
//! has no playlist on the server.
//!
//! The JSON columns hold the bytes Python writes: `entries_json` is
//! `json.dumps([[rank, avid, title], ...], ensure_ascii=False)`, `present_json` and
//! `missing_json` are `json.dumps(list)` (ASCII escapes).

use chrono::{DateTime, Utc};
use embyx_clients::jinjier::{RankedEntry, RankedList};
use embyx_core::pyjson::{self, DumpOptions, Value};
use embyx_db::database::Database;
use sqlx::Row as _;
use sqlx::postgres::PgRow;

use crate::acquisitions::load_string_list;
use crate::support::{BoxFuture, StoreError, out_of_range};

/// What the last download left behind in the source table.
const DEPARTED: &str = "the source no longer publishes this list";

/// `PlaylistRecord`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaylistRecord {
    pub key: String,
    pub kind: i64,
    pub note: String,
    pub name: String,
    pub enabled: bool,
    pub entries: Vec<RankedEntry>,
    /// Codes the last sync found in the library, in ranking order.
    pub present: Vec<String>,
    /// Codes the last sync did not find, in ranking order.
    pub missing: Vec<String>,
    pub emby_playlist_id: Option<String>,
    pub last_synced_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// `PlaylistSourceState`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaylistSourceState {
    /// The `yyyymmdd` stamp of the database the stored entries came from.
    pub database_name: String,
    pub fetched_at: DateTime<Utc>,
}

/// The repository surface the playlists pipeline writes through; the Python tests run the
/// pipeline against an in-memory stand-in (`FakeRepository`).
pub trait PlaylistStore: Send + Sync {
    fn list(&self) -> BoxFuture<'_, Result<Vec<PlaylistRecord>, StoreError>>;
    fn source(&self) -> BoxFuture<'_, Result<Option<PlaylistSourceState>, StoreError>>;
    fn record_source<'a>(&'a self, database_name: &'a str, now: DateTime<Utc>)
    -> BoxFuture<'a, Result<(), StoreError>>;
    fn replace_lists<'a>(
        &'a self,
        lists: &'a [RankedList],
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<(), StoreError>>;
    fn record_sync<'a>(
        &'a self,
        key: &'a str,
        now: DateTime<Utc>,
        present: &'a [String],
        missing: &'a [String],
        emby_playlist_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), StoreError>>;
    fn record_error<'a>(
        &'a self,
        key: &'a str,
        now: DateTime<Utc>,
        error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>>;
}

/// `json.dumps([[entry.rank, entry.avid, entry.title] for entry in entries], ensure_ascii=False)`.
pub fn dump_entries(entries: &[RankedEntry]) -> String {
    let value = Value::List(
        entries
            .iter()
            .map(|entry| {
                Value::List(vec![
                    Value::Int(i128::try_from(entry.rank).unwrap_or(i128::MAX)),
                    Value::Str(entry.avid.clone()),
                    Value::Str(entry.title.clone()),
                ])
            })
            .collect(),
    );
    let options = DumpOptions {
        ensure_ascii: false,
        ..DumpOptions::python_default()
    };
    pyjson::dumps(&value, &options).expect("ranked entries always dump")
}

/// `json.dumps(list(codes))`.
fn dump_codes(codes: &[String]) -> String {
    let value = Value::List(codes.iter().map(|code| Value::Str(code.clone())).collect());
    pyjson::dumps(&value, &DumpOptions::python_default()).expect("a list of strings always dumps")
}

/// `RankedEntry(rank=int(rank), avid=str(avid), title=str(title)) for rank, avid, title in
/// json.loads(text)`. Only the shape `replace_lists` writes reads back; any other is a decode
/// error (Python would coerce what `int()` and `str()` accept).
fn load_entries(text: &str) -> Result<Vec<RankedEntry>, StoreError> {
    let shape = || StoreError::Decode {
        class: "TypeError",
        message: "entries_json does not hold [rank, avid, title] lists".to_owned(),
    };
    let value = pyjson::loads(text).map_err(|error| StoreError::Decode {
        class: "JSONDecodeError",
        message: error.python_message(text),
    })?;
    let Value::List(items) = value else {
        return Err(shape());
    };
    items
        .into_iter()
        .map(|item| match item {
            Value::List(fields) => match fields.as_slice() {
                [Value::Int(rank), Value::Str(avid), Value::Str(title)] => Ok(RankedEntry {
                    rank: usize::try_from(*rank).map_err(|_| shape())?,
                    avid: avid.clone(),
                    title: title.clone(),
                }),
                _ => Err(shape()),
            },
            _ => Err(shape()),
        })
        .collect()
}

/// A list kind for the `INTEGER` column; asyncpg's `DataError` beyond 32 bits.
fn kind_column(kind: i64, position: usize) -> Result<i32, StoreError> {
    i32::try_from(kind).map_err(|_| out_of_range(position, i128::from(kind), 32))
}

/// `_from_row(row)`.
fn from_row(row: &PgRow) -> Result<PlaylistRecord, StoreError> {
    let kind: i32 = row.try_get("kind")?;
    Ok(PlaylistRecord {
        key: row.try_get("key")?,
        kind: i64::from(kind),
        note: row.try_get("note")?,
        name: row.try_get("name")?,
        enabled: row.try_get("enabled")?,
        entries: load_entries(row.try_get("entries_json")?)?,
        present: load_string_list(row.try_get("present_json")?, "present_json")?,
        missing: load_string_list(row.try_get("missing_json")?, "missing_json")?,
        emby_playlist_id: row.try_get("emby_playlist_id")?,
        last_synced_at: row.try_get("last_synced_at")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// `PlaylistRepository`.
#[derive(Debug, Clone)]
pub struct PlaylistRepository {
    database: Database,
}

impl PlaylistRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub async fn list(&self) -> Result<Vec<PlaylistRecord>, StoreError> {
        let pool = self.database.get_pool().await?;
        let rows = sqlx::query("SELECT * FROM playlists ORDER BY kind, note, key")
            .fetch_all(&pool)
            .await?;
        rows.iter().map(from_row).collect()
    }

    pub async fn get(&self, key: &str) -> Result<Option<PlaylistRecord>, StoreError> {
        let pool = self.database.get_pool().await?;
        let row = sqlx::query("SELECT * FROM playlists WHERE key = $1")
            .bind(key)
            .fetch_optional(&pool)
            .await?;
        row.as_ref().map(from_row).transpose()
    }

    /// Store a fresh download: new lists start enabled, known ones keep the operator's flag.
    ///
    /// A list the source no longer publishes keeps its row and playlist but is marked so; a
    /// renamed ranking should not silently take the operator's playlists away.
    pub async fn replace_lists(&self, lists: &[RankedList], now: DateTime<Utc>) -> Result<(), StoreError> {
        let pool = self.database.get_pool().await?;
        let mut transaction = pool.begin().await?;
        for ranked in lists {
            sqlx::query(
                "
                INSERT INTO playlists (key, kind, note, name, entries_json, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $6)
                ON CONFLICT (key) DO UPDATE SET
                    kind = excluded.kind, note = excluded.note, name = excluded.name,
                    entries_json = excluded.entries_json, updated_at = excluded.updated_at
                ",
            )
            .bind(&ranked.key)
            .bind(kind_column(ranked.kind, 2)?)
            .bind(&ranked.note)
            .bind(ranked.name())
            .bind(dump_entries(&ranked.entries))
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        let keys: Vec<&str> = lists.iter().map(|ranked| ranked.key.as_str()).collect();
        sqlx::query(
            "
            UPDATE playlists SET last_error = $2, updated_at = $3
            WHERE key <> ALL($1::text[])
            ",
        )
        .bind(keys)
        .bind(DEPARTED)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn set_enabled(
        &self,
        key: &str,
        enabled: bool,
        now: DateTime<Utc>,
    ) -> Result<Option<PlaylistRecord>, StoreError> {
        let pool = self.database.get_pool().await?;
        let row = sqlx::query("UPDATE playlists SET enabled = $2, updated_at = $3 WHERE key = $1 RETURNING *")
            .bind(key)
            .bind(enabled)
            .bind(now)
            .fetch_optional(&pool)
            .await?;
        row.as_ref().map(from_row).transpose()
    }

    pub async fn record_sync(
        &self,
        key: &str,
        now: DateTime<Utc>,
        present: &[String],
        missing: &[String],
        emby_playlist_id: Option<&str>,
    ) -> Result<(), StoreError> {
        let pool = self.database.get_pool().await?;
        sqlx::query(
            "
            UPDATE playlists
            SET present_json = $2, missing_json = $3, emby_playlist_id = $4,
                last_synced_at = $5, last_error = NULL, updated_at = $5
            WHERE key = $1
            ",
        )
        .bind(key)
        .bind(dump_codes(present))
        .bind(dump_codes(missing))
        .bind(emby_playlist_id)
        .bind(now)
        .execute(&pool)
        .await?;
        Ok(())
    }

    pub async fn record_error(&self, key: &str, now: DateTime<Utc>, error: &str) -> Result<(), StoreError> {
        let pool = self.database.get_pool().await?;
        sqlx::query("UPDATE playlists SET last_error = $2, updated_at = $3 WHERE key = $1")
            .bind(key)
            .bind(error)
            .bind(now)
            .execute(&pool)
            .await?;
        Ok(())
    }

    pub async fn source(&self) -> Result<Option<PlaylistSourceState>, StoreError> {
        let pool = self.database.get_pool().await?;
        let row = sqlx::query("SELECT database_name, fetched_at FROM playlist_source WHERE id")
            .fetch_optional(&pool)
            .await?;
        row.map(|row| {
            Ok(PlaylistSourceState {
                database_name: row.try_get("database_name")?,
                fetched_at: row.try_get("fetched_at")?,
            })
        })
        .transpose()
    }

    pub async fn record_source(&self, database_name: &str, now: DateTime<Utc>) -> Result<(), StoreError> {
        let pool = self.database.get_pool().await?;
        sqlx::query(
            "
            INSERT INTO playlist_source (id, database_name, fetched_at) VALUES (TRUE, $1, $2)
            ON CONFLICT (id) DO UPDATE SET database_name = excluded.database_name, fetched_at = excluded.fetched_at
            ",
        )
        .bind(database_name)
        .bind(now)
        .execute(&pool)
        .await?;
        Ok(())
    }
}

impl PlaylistStore for PlaylistRepository {
    fn list(&self) -> BoxFuture<'_, Result<Vec<PlaylistRecord>, StoreError>> {
        Box::pin(Self::list(self))
    }

    fn source(&self) -> BoxFuture<'_, Result<Option<PlaylistSourceState>, StoreError>> {
        Box::pin(Self::source(self))
    }

    fn record_source<'a>(
        &'a self,
        database_name: &'a str,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(Self::record_source(self, database_name, now))
    }

    fn replace_lists<'a>(
        &'a self,
        lists: &'a [RankedList],
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(Self::replace_lists(self, lists, now))
    }

    fn record_sync<'a>(
        &'a self,
        key: &'a str,
        now: DateTime<Utc>,
        present: &'a [String],
        missing: &'a [String],
        emby_playlist_id: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(Self::record_sync(self, key, now, present, missing, emby_playlist_id))
    }

    fn record_error<'a>(
        &'a self,
        key: &'a str,
        now: DateTime<Utc>,
        error: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(Self::record_error(self, key, now, error))
    }
}

#[cfg(test)]
mod tests;
