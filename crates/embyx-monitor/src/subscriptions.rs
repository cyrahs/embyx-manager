//! The subscription registry: which feeds the poller reads, and where they file.
//!
//! Python origin: `src/embyx_manager/monitor/subscriptions.py`. A subscription is a plain feed
//! URL or an AVBase talent whose feed URL is derived from its id; each belongs to one of the
//! configured RSS categories. The poller remembers, per subscription, a bounded list of the
//! item keys it has seen (`cursor_json`), only so an item is not re-read on every poll.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use embyx_core::pyjson::{self, DumpOptions, Value};
use embyx_core::pytext::py_strip;
use embyx_core::pyurl::urlsplit;
use embyx_db::database::Database;
use sqlx::Row as _;
use sqlx::postgres::PgRow;

use crate::acquisitions::load_string_list;
use crate::reports::{PyException, str_enum};
use crate::support::{StoreError, out_of_range};

/// `AVBASE_FEED_URL`.
pub const AVBASE_FEED_URL: &str = "https://www.avbase.net/talents/{talent_id}/feed";
/// `CURSOR_LIMIT`: item keys remembered per subscription.
pub const CURSOR_LIMIT: usize = 500;

str_enum! {
    /// `SubscriptionKind`.
    SubscriptionKind { Rss = "rss", AvbaseTalent = "avbase_talent" }
}

/// `SubscriptionExistsError`: the same feed (or talent) is already subscribed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("already subscribed: {what}")]
pub struct SubscriptionExistsError {
    pub what: String,
}

/// What a subscription call fails with.
#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    #[error(transparent)]
    Exists(#[from] SubscriptionExistsError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl From<sqlx::Error> for SubscriptionError {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(error.into())
    }
}

impl From<embyx_db::database::DatabaseError> for SubscriptionError {
    fn from(error: embyx_db::database::DatabaseError) -> Self {
        Self::Store(StoreError::Database(error))
    }
}

impl PyException for SubscriptionError {
    fn class_name(&self) -> &str {
        match self {
            Self::Exists(_) => "SubscriptionExistsError",
            Self::Store(error) => error.class_name(),
        }
    }
}

/// `SubscriptionRecord`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionRecord {
    pub id: i64,
    pub kind: SubscriptionKind,
    pub category: String,
    pub enabled: bool,
    pub url: Option<String>,
    pub talent_id: Option<i32>,
    pub name: Option<String>,
    pub aliases: Vec<String>,
    pub cursor: Vec<String>,
    /// True until the first poll, which then records the feed's current items as seen without
    /// ingesting them.
    pub seed_pending: bool,
    pub last_polled_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl SubscriptionRecord {
    /// `feed_url`.
    pub fn feed_url(&self) -> String {
        match self.kind {
            SubscriptionKind::AvbaseTalent => {
                let talent = self.talent_id.map_or_else(|| "None".to_owned(), |id| id.to_string());
                AVBASE_FEED_URL.replace("{talent_id}", &talent)
            }
            SubscriptionKind::Rss => self.url.clone().unwrap_or_default(),
        }
    }

    /// `display_name`: the name, or the feed URL when it has none (or an empty one).
    pub fn display_name(&self) -> String {
        match &self.name {
            Some(name) if !name.is_empty() => name.clone(),
            _ => self.feed_url(),
        }
    }
}

/// `validate_feed_url(value)`: an absolute HTTP(S) URL without credentials; a query string is
/// fine. The stripped URL, or `ValueError`'s message.
pub fn validate_feed_url(value: &str) -> Result<String, String> {
    const NOT_ABSOLUTE: &str = "feed url must be an absolute HTTP(S) URL";
    let url = py_strip(value);
    let parsed = urlsplit(url, "").map_err(|_| NOT_ABSOLUTE.to_owned())?;
    let port = parsed.port().map_err(|_| NOT_ABSOLUTE.to_owned())?;
    if !matches!(parsed.scheme.as_str(), "http" | "https") || parsed.hostname().is_none() || port == Some(0) {
        return Err(NOT_ABSOLUTE.to_owned());
    }
    if parsed.username().is_some() || parsed.password().is_some() {
        return Err("feed url must not include credentials".to_owned());
    }
    Ok(url.to_owned())
}

/// `trim_cursor(keys)`: newest-last, de-duplicated (first occurrence kept), capped at
/// [`CURSOR_LIMIT`].
pub fn trim_cursor(keys: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let unique: Vec<&String> = keys.iter().filter(|key| seen.insert(key.as_str())).collect();
    let start = unique.len().saturating_sub(CURSOR_LIMIT);
    unique[start..].iter().map(|key| (*key).clone()).collect()
}

fn dump_list(items: &[String]) -> String {
    let value = Value::List(items.iter().map(|item| Value::Str(item.clone())).collect());
    pyjson::dumps(&value, &DumpOptions::python_default()).expect("a list of strings always dumps")
}

/// A talent id for the `INTEGER` column; asyncpg's `DataError` beyond 32 bits.
fn talent_column(talent_id: i64, position: usize) -> Result<i32, StoreError> {
    i32::try_from(talent_id).map_err(|_| out_of_range(position, i128::from(talent_id), 32))
}

/// `SubscriptionRepository`.
#[derive(Debug, Clone)]
pub struct SubscriptionRepository {
    database: Database,
}

impl SubscriptionRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub async fn list(&self) -> Result<Vec<SubscriptionRecord>, SubscriptionError> {
        let pool = self.database.get_pool().await?;
        let rows = sqlx::query("SELECT * FROM feed_subscriptions ORDER BY category, kind, COALESCE(name, url), id")
            .fetch_all(&pool)
            .await?;
        Ok(rows.iter().map(from_row).collect::<Result<_, _>>()?)
    }

    pub async fn get(&self, subscription_id: i64) -> Result<Option<SubscriptionRecord>, SubscriptionError> {
        let pool = self.database.get_pool().await?;
        let row = sqlx::query("SELECT * FROM feed_subscriptions WHERE id = $1")
            .bind(subscription_id)
            .fetch_optional(&pool)
            .await?;
        Ok(row.as_ref().map(from_row).transpose()?)
    }

    /// `add_rss(url=, category=, now=, name=None, seed_pending=False)`.
    pub async fn add_rss(
        &self,
        url: &str,
        category: &str,
        now: DateTime<Utc>,
        name: Option<&str>,
        seed_pending: bool,
    ) -> Result<SubscriptionRecord, SubscriptionError> {
        let pool = self.database.get_pool().await?;
        let row = sqlx::query(
            "
            INSERT INTO feed_subscriptions
                (kind, category, enabled, url, name, seed_pending, created_at, updated_at)
            VALUES ('rss', $1, TRUE, $2, $3, $4, $5, $5)
            RETURNING *
            ",
        )
        .bind(category)
        .bind(url)
        .bind(name)
        .bind(seed_pending)
        .bind(now)
        .fetch_one(&pool)
        .await;
        match row {
            Err(error) if StoreError::is_unique_violation(&error) => {
                Err(SubscriptionExistsError { what: url.to_owned() }.into())
            }
            Err(error) => Err(error.into()),
            Ok(row) => Ok(from_row(&row)?),
        }
    }

    /// `add_talent(talent_id=, name=, aliases=, category=, now=, seed_pending=False)`.
    pub async fn add_talent(
        &self,
        talent_id: i64,
        name: &str,
        aliases: &[String],
        category: &str,
        now: DateTime<Utc>,
        seed_pending: bool,
    ) -> Result<SubscriptionRecord, SubscriptionError> {
        let column = talent_column(talent_id, 2)?;
        let pool = self.database.get_pool().await?;
        let row = sqlx::query(
            "
            INSERT INTO feed_subscriptions
                (kind, category, enabled, talent_id, name, aliases_json, seed_pending, created_at, updated_at)
            VALUES ('avbase_talent', $1, TRUE, $2, $3, $4, $5, $6, $6)
            RETURNING *
            ",
        )
        .bind(category)
        .bind(column)
        .bind(name)
        .bind(dump_list(aliases))
        .bind(seed_pending)
        .bind(now)
        .fetch_one(&pool)
        .await;
        match row {
            Err(error) if StoreError::is_unique_violation(&error) => Err(SubscriptionExistsError {
                what: format!("talent {talent_id}"),
            }
            .into()),
            Err(error) => Err(error.into()),
            Ok(row) => Ok(from_row(&row)?),
        }
    }

    /// `update(subscription_id, now=, enabled=None, category=None, url=None)`: `None` keeps a
    /// field; `Ok(None)` when there is no such subscription.
    pub async fn update(
        &self,
        subscription_id: i64,
        now: DateTime<Utc>,
        enabled: Option<bool>,
        category: Option<&str>,
        url: Option<&str>,
    ) -> Result<Option<SubscriptionRecord>, SubscriptionError> {
        let pool = self.database.get_pool().await?;
        let row = sqlx::query(
            "
            UPDATE feed_subscriptions
            SET enabled = COALESCE($2, enabled), category = COALESCE($3, category),
                url = COALESCE($5, url), updated_at = $4
            WHERE id = $1
            RETURNING *
            ",
        )
        .bind(subscription_id)
        .bind(enabled)
        .bind(category)
        .bind(now)
        .bind(url)
        .fetch_optional(&pool)
        .await;
        match row {
            Err(error) if StoreError::is_unique_violation(&error) => Err(SubscriptionExistsError {
                what: url.unwrap_or_default().to_owned(),
            }
            .into()),
            Err(error) => Err(error.into()),
            Ok(row) => Ok(row.as_ref().map(from_row).transpose()?),
        }
    }

    /// Point a talent subscription at the id AVBase now lists the talent under. The cursor
    /// stays: feed items are keyed by the work URL, which does not change with the talent.
    pub async fn relink_talent(
        &self,
        subscription_id: i64,
        talent_id: i64,
        aliases: &[String],
        now: DateTime<Utc>,
    ) -> Result<Option<SubscriptionRecord>, SubscriptionError> {
        let column = talent_column(talent_id, 2)?;
        let pool = self.database.get_pool().await?;
        let row = sqlx::query(
            "
            UPDATE feed_subscriptions
            SET talent_id = $2, aliases_json = $3, updated_at = $4
            WHERE id = $1 AND kind = 'avbase_talent'
            RETURNING *
            ",
        )
        .bind(subscription_id)
        .bind(column)
        .bind(dump_list(aliases))
        .bind(now)
        .fetch_optional(&pool)
        .await;
        match row {
            Err(error) if StoreError::is_unique_violation(&error) => Err(SubscriptionExistsError {
                what: format!("talent {talent_id}"),
            }
            .into()),
            Err(error) => Err(error.into()),
            Ok(row) => Ok(row.as_ref().map(from_row).transpose()?),
        }
    }

    pub async fn delete(&self, subscription_id: i64) -> Result<bool, SubscriptionError> {
        let pool = self.database.get_pool().await?;
        let done = sqlx::query("DELETE FROM feed_subscriptions WHERE id = $1")
            .bind(subscription_id)
            .execute(&pool)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Note the outcome of one poll; `cursor` `None` keeps the stored one.
    ///
    /// A successful poll hands over the new cursor and clears the error; a failed one leaves
    /// the cursor alone so the items it did not get to are read next time. Storing a cursor
    /// also settles a pending seed.
    pub async fn record_poll(
        &self,
        subscription_id: i64,
        now: DateTime<Utc>,
        cursor: Option<&[String]>,
        error: Option<&str>,
    ) -> Result<(), SubscriptionError> {
        let pool = self.database.get_pool().await?;
        sqlx::query(
            "
            UPDATE feed_subscriptions
            SET last_polled_at = $2, last_error = $3,
                cursor_json = COALESCE($4, cursor_json),
                seed_pending = CASE WHEN $4 IS NULL THEN seed_pending ELSE FALSE END,
                updated_at = $2
            WHERE id = $1
            ",
        )
        .bind(subscription_id)
        .bind(now)
        .bind(error)
        .bind(cursor.map(|keys| dump_list(&trim_cursor(keys))))
        .execute(&pool)
        .await?;
        Ok(())
    }
}

/// `_from_row(row)`.
fn from_row(row: &PgRow) -> Result<SubscriptionRecord, StoreError> {
    let kind: &str = row.try_get("kind")?;
    Ok(SubscriptionRecord {
        id: row.try_get("id")?,
        kind: SubscriptionKind::parse(kind).ok_or_else(|| StoreError::invalid_enum(kind, "SubscriptionKind"))?,
        category: row.try_get("category")?,
        enabled: row.try_get("enabled")?,
        url: row.try_get("url")?,
        talent_id: row.try_get("talent_id")?,
        name: row.try_get("name")?,
        aliases: load_string_list(row.try_get("aliases_json")?, "aliases_json")?,
        cursor: load_string_list(row.try_get("cursor_json")?, "cursor_json")?,
        seed_pending: row.try_get("seed_pending")?,
        last_polled_at: row.try_get("last_polled_at")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

#[cfg(test)]
mod tests;
