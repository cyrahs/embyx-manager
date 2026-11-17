//! The acquisition ledger: one row per AVID, from discovery to archived.
//!
//! Python origin: `src/embyx_manager/monitor/acquisitions.py`. An AVID is discovered (RSS,
//! manual, or reconcile), gets a list of magnet candidates, and submits them one at a time as
//! CloudDrive offline tasks; the tracker joins the offline task list to the attempts on
//! `info_hash` and drives each attempt to a conclusion, falling through to the next candidate
//! whenever one fails, stalls, or turns out to hold no usable video.
//!
//! Every state change is a compare-and-set: the expected state is part of the `WHERE` clause,
//! so two replicas racing over the same row cannot both win, and no in-process lock is needed.
//! A CAS that matches nothing returns `false` (someone else moved the row); an edge that is not
//! in the state machine at all is an error, because that is a programming error rather than a
//! race. The statements are Python's, copied (cross-version contract 3): either build resumes
//! the rows the other wrote.
//!
//! [`Ledger`] is the repository's interface, so the pipelines can run against the in-memory
//! ledger of the Python tests (`testing::FakeLedger`).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use chrono::{DateTime, NaiveDate, Utc};
use embyx_core::pyjson::{self, DumpOptions, Value};
use embyx_db::database::Database;
use sqlx::Row as _;
use sqlx::postgres::PgRow;

use crate::reports::{PyException, str_enum};
use crate::support::{BoxFuture, StoreError, out_of_range_in_element};

str_enum! {
    /// `AcquisitionState`.
    AcquisitionState {
        Discovered = "discovered",
        Downloading = "downloading",
        Archived = "archived",
        ResolveFailed = "resolve_failed",
        Exhausted = "exhausted",
        NeedsAttention = "needs_attention",
        Ignored = "ignored",
    }
}

str_enum! {
    /// `AcquisitionSource`: the fixed sources. RSS and playlist sources are open-ended strings
    /// ([`rss_source`], [`playlist_source`]).
    AcquisitionSource { Manual = "manual", Reconcile = "reconcile", FillActor = "fill_actor" }
}

str_enum! {
    /// `AttemptState`.
    AttemptState {
        Pending = "pending",
        Submitted = "submitted",
        Downloading = "downloading",
        Finished = "finished",
        Archiving = "archiving",
        Archived = "archived",
        Junk = "junk",
        Error = "error",
        Stalled = "stalled",
        Lost = "lost",
    }
}

/// `RSS_SOURCE_PREFIX`. RSS sources carry the category they came from, so the set is
/// open-ended. Rows written before categories existed hold `rss_actor` and `rss_rank`, which
/// stay readable as plain strings.
pub const RSS_SOURCE_PREFIX: &str = "rss:";

/// `rss_source(label)`: the ledger source for items ingested from one subscription category.
pub fn rss_source(label: &str) -> String {
    format!("{RSS_SOURCE_PREFIX}{label}")
}

/// `PLAYLIST_SOURCE_PREFIX`: a playlist's fill carries the list it came from.
pub const PLAYLIST_SOURCE_PREFIX: &str = "playlist:";

/// `playlist_source(key)`.
pub fn playlist_source(key: &str) -> String {
    format!("{PLAYLIST_SOURCE_PREFIX}{key}")
}

use AcquisitionState as A;
use AttemptState as T;

/// `ACTIVE_STATES`: the tracker owns the AVID and nothing else may touch its folder.
pub const ACTIVE_STATES: &[AcquisitionState] = &[A::Discovered, A::Downloading];
/// `REPLACEABLE_STATES`: states a title the library holds can be fetched again from, to
/// replace the copy it holds (换版).
pub const REPLACEABLE_STATES: &[AcquisitionState] = &[A::Archived, A::Ignored, A::ResolveFailed, A::Exhausted];
/// `RETRYABLE_STATES`: states a retry timer can lift.
pub const RETRYABLE_STATES: &[AcquisitionState] = &[A::ResolveFailed, A::Exhausted];
/// `DUE_STATES`: what the background resolve pass picks up when `next_action_at` arrives.
pub const DUE_STATES: &[AcquisitionState] = &[A::ResolveFailed, A::Exhausted, A::Discovered];
/// `IN_FLIGHT_ATTEMPT_STATES`: still in flight at CloudDrive.
pub const IN_FLIGHT_ATTEMPT_STATES: &[AttemptState] = &[T::Submitted, T::Downloading];
/// `LIVE_ATTEMPT_STATES`: an offline task can still be acted on; a hash is unique across these.
pub const LIVE_ATTEMPT_STATES: &[AttemptState] = &[T::Submitted, T::Downloading, T::Finished, T::Archiving];
/// `_CLAIM_RETRIES`: how often a claim that lost a race for a shared hash is re-run.
const CLAIM_RETRIES: usize = 3;

/// `_ACQUISITION_EDGES[source]`.
pub fn acquisition_edges(source: AcquisitionState) -> &'static [AcquisitionState] {
    match source {
        A::Discovered | A::Exhausted => &[
            A::Downloading,
            A::ResolveFailed,
            A::NeedsAttention,
            A::Archived,
            A::Ignored,
        ],
        // Downloading -> downloading: the next candidate submitted for the same AVID.
        A::Downloading => &[A::Downloading, A::Archived, A::Exhausted, A::NeedsAttention, A::Ignored],
        // Resolve failed -> resolve failed: another resolve pass came up empty.
        A::ResolveFailed => &[
            A::Downloading,
            A::ResolveFailed,
            A::Exhausted,
            A::NeedsAttention,
            A::Archived,
            A::Ignored,
        ],
        // Needs attention -> downloading: an operator resolved it; the tracker resumes.
        A::NeedsAttention => &[A::Downloading, A::Archived, A::Exhausted, A::Ignored],
        // Archived is final: the video is in the library and the folder is gone.
        A::Archived => &[],
        // Ignored is an operator decision, reversible only by re-discovering the AVID.
        A::Ignored => &[A::Discovered],
    }
}

/// `_ATTEMPT_EDGES[source]`.
pub fn attempt_edges(source: AttemptState) -> &'static [AttemptState] {
    match source {
        T::Pending => &[T::Submitted, T::Error],
        // Downloading -> downloading: a progress refresh.
        T::Submitted | T::Downloading => &[T::Downloading, T::Finished, T::Error, T::Stalled, T::Lost],
        T::Finished => &[T::Archiving, T::Lost],
        // Archiving is a claim: released back to finished when the archive run cannot conclude.
        T::Archiving => &[T::Archived, T::Junk, T::Finished],
        T::Archived | T::Junk | T::Error | T::Stalled | T::Lost => &[],
    }
}

/// `IllegalTransitionError`: a transition that is not an edge of the state machine.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind}: {from} -> {to} is not a legal transition")]
pub struct IllegalTransitionError {
    /// `acquisition` or `attempt` (the enum's name without `State`, lower-cased).
    pub kind: &'static str,
    pub from: &'static str,
    pub to: &'static str,
}

impl IllegalTransitionError {
    fn acquisition(source: AcquisitionState, target: AcquisitionState) -> Self {
        Self {
            kind: "acquisition",
            from: source.as_str(),
            to: target.as_str(),
        }
    }

    fn attempt(source: AttemptState, target: AttemptState) -> Self {
        Self {
            kind: "attempt",
            from: source.as_str(),
            to: target.as_str(),
        }
    }
}

/// `MagnetInUseError`: the chosen magnet is live on another attempt, so it cannot be
/// submitted now.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{avid} attempt {attempt_no}: the magnet is already live on another attempt")]
pub struct MagnetInUseError {
    pub avid: String,
    pub attempt_no: i32,
}

/// What a ledger call fails with.
#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error(transparent)]
    IllegalTransition(#[from] IllegalTransitionError),
    #[error(transparent)]
    MagnetInUse(#[from] MagnetInUseError),
    /// `ValueError` for an argument the call refuses (`record_progress` with a concluded state).
    #[error("{0}")]
    Value(String),
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl From<sqlx::Error> for LedgerError {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(StoreError::from(error))
    }
}

impl From<embyx_db::database::DatabaseError> for LedgerError {
    fn from(error: embyx_db::database::DatabaseError) -> Self {
        Self::Store(StoreError::Database(error))
    }
}

impl PyException for LedgerError {
    fn class_name(&self) -> &str {
        match self {
            Self::IllegalTransition(_) => "IllegalTransitionError",
            Self::MagnetInUse(_) => "MagnetInUseError",
            Self::Value(_) => "ValueError",
            Self::Store(error) => error.class_name(),
        }
    }
}

/// `MagnetCandidate`: one magnet worth trying for an AVID, before it becomes an attempt row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MagnetCandidate {
    pub magnet: String,
    pub info_hash: Option<String>,
    pub source: String,
    /// Python's `int` (JavBus sizes are parsed without a bound); the column is `BIGINT`.
    pub size_hint: Option<i128>,
}

/// `AcquisitionRecord`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquisitionRecord {
    pub avid: String,
    pub state: AcquisitionState,
    pub source: String,
    pub note: Option<String>,
    pub archived_paths: Vec<String>,
    pub next_action_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// The offline directory this AVID's magnets go to; `None` means the default.
    pub task_dir_path: Option<String>,
    /// The earliest release date a source knew; anchors the resolve schedule.
    pub release_date: Option<NaiveDate>,
    /// Library paths of the copy this acquisition replaces, deleted just before the new
    /// download is filed; empty for an ordinary acquisition.
    pub replace_paths: Vec<String>,
    /// The highest attempt number when the replacement began: only later attempts count
    /// against the attempt cap.
    pub replace_after_attempt: Option<i32>,
}

/// `MagnetAttemptRecord`.
#[derive(Debug, Clone, PartialEq)]
pub struct MagnetAttemptRecord {
    pub avid: String,
    pub attempt_no: i32,
    pub magnet: String,
    pub info_hash: Option<String>,
    pub magnet_source: String,
    pub size_hint: Option<i64>,
    pub state: AttemptState,
    pub progress: Option<f64>,
    pub error: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

/// The optional keyword arguments of `discover()`: what one sighting knows besides its source.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sighting {
    /// Pins the offline directory this AVID's magnets go to; `None` leaves the default.
    pub task_dir_path: Option<String>,
    /// Hands the resolve to the background pass at that time instead of the caller.
    pub next_action_at: Option<DateTime<Utc>>,
    /// Kept from the first source that knew it.
    pub release_date: Option<NaiveDate>,
    /// The sighting carries a magnet: a row still cooling down is accepted.
    pub wake: bool,
    /// Sources this sighting must not take a row from.
    pub yields_to: Vec<String>,
}

impl Sighting {
    pub fn in_dir(task_dir_path: impl Into<String>) -> Self {
        Self {
            task_dir_path: Some(task_dir_path.into()),
            ..Self::default()
        }
    }
}

/// The optional keyword arguments of `transition()`, written together with the state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransitionFields {
    pub note: Option<String>,
    pub next_action_at: Option<DateTime<Utc>>,
    /// `None` keeps the stored paths.
    pub archived_paths: Option<Vec<String>>,
}

impl TransitionFields {
    pub fn note(note: impl Into<String>) -> Self {
        Self {
            note: Some(note.into()),
            ..Self::default()
        }
    }

    pub fn next_action_at(at: DateTime<Utc>) -> Self {
        Self {
            next_action_at: Some(at),
            ..Self::default()
        }
    }

    pub fn archived_paths(paths: Vec<String>) -> Self {
        Self {
            archived_paths: Some(paths),
            ..Self::default()
        }
    }
}

/// The ledger's interface: what [`AcquisitionRepository`] offers, so a pipeline can run
/// against the in-memory ledger of the tests. Dyn-compatible (`Arc<dyn Ledger>`).
pub trait Ledger: Send + Sync + fmt::Debug {
    /// `discover(avid, source=, now=, ...)`: record a sighting; whether the AVID still needs
    /// magnets resolved.
    fn discover<'a>(
        &'a self,
        avid: &'a str,
        source: &'a str,
        now: DateTime<Utc>,
        sighting: Sighting,
    ) -> BoxFuture<'a, Result<bool, LedgerError>>;
    fn get<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<AcquisitionRecord>, LedgerError>>;
    fn states_for<'a>(
        &'a self,
        avids: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeMap<String, AcquisitionState>, LedgerError>>;
    fn task_dirs_for<'a>(&'a self, avids: &'a [String])
    -> BoxFuture<'a, Result<BTreeMap<String, String>, LedgerError>>;
    fn set_release_date<'a>(
        &'a self,
        avid: &'a str,
        release_date: NaiveDate,
    ) -> BoxFuture<'a, Result<bool, LedgerError>>;
    fn begin_replacement<'a>(
        &'a self,
        avid: &'a str,
        source: &'a str,
        task_dir_path: &'a str,
        replace_paths: &'a [String],
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<AcquisitionRecord>, LedgerError>>;
    fn transition<'a>(
        &'a self,
        avid: &'a str,
        expected: AcquisitionState,
        target: AcquisitionState,
        now: DateTime<Utc>,
        fields: TransitionFields,
    ) -> BoxFuture<'a, Result<bool, LedgerError>>;
    fn due_for_retry(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> BoxFuture<'_, Result<Vec<AcquisitionRecord>, LedgerError>>;
    fn active_avids(&self) -> BoxFuture<'_, Result<BTreeSet<String>, LedgerError>>;
    fn active_task_dirs(&self) -> BoxFuture<'_, Result<Vec<String>, LedgerError>>;
    fn latest_task_dir<'a>(&'a self, source: &'a str) -> BoxFuture<'a, Result<Option<String>, LedgerError>>;
    fn add_attempts<'a>(
        &'a self,
        avid: &'a str,
        candidates: &'a [MagnetCandidate],
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<usize, LedgerError>>;
    fn claim_next_pending<'a>(
        &'a self,
        avid: &'a str,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<MagnetAttemptRecord>, LedgerError>>;
    fn claim_attempt<'a>(
        &'a self,
        avid: &'a str,
        attempt_no: i32,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<MagnetAttemptRecord>, LedgerError>>;
    fn transition_attempt<'a>(
        &'a self,
        avid: &'a str,
        attempt_no: i32,
        expected: AttemptState,
        target: AttemptState,
        now: DateTime<Utc>,
        error: Option<String>,
    ) -> BoxFuture<'a, Result<bool, LedgerError>>;
    fn record_progress<'a>(
        &'a self,
        avid: &'a str,
        attempt_no: i32,
        state: AttemptState,
        progress: Option<f64>,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<bool, LedgerError>>;
    fn attempts_by_info_hash<'a>(
        &'a self,
        info_hashes: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeMap<String, MagnetAttemptRecord>, LedgerError>>;
    fn archived_info_hashes<'a>(
        &'a self,
        info_hashes: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeSet<String>, LedgerError>>;
    fn in_flight_attempts(&self) -> BoxFuture<'_, Result<Vec<MagnetAttemptRecord>, LedgerError>>;
    fn attempts_for<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Vec<MagnetAttemptRecord>, LedgerError>>;
    fn list_acquisitions<'a>(
        &'a self,
        states: Option<&'a [AcquisitionState]>,
        limit: i64,
        offset: i64,
    ) -> BoxFuture<'a, Result<Vec<AcquisitionRecord>, LedgerError>>;
    /// `count_by_state()`, in the order PostgreSQL returns the groups (Python's dict order).
    fn count_by_state(&self) -> BoxFuture<'_, Result<Vec<(AcquisitionState, i64)>, LedgerError>>;
}

/// `sorted(state.value for state in states)`.
fn sorted_values<S: Copy, F: Fn(S) -> &'static str>(states: &[S], value: F) -> Vec<String> {
    let set: BTreeSet<&'static str> = states.iter().map(|state| value(*state)).collect();
    set.into_iter().map(str::to_owned).collect()
}

fn acquisition_values(states: &[AcquisitionState]) -> Vec<String> {
    sorted_values(states, AcquisitionState::as_str)
}

fn attempt_values(states: &[AttemptState]) -> Vec<String> {
    sorted_values(states, AttemptState::as_str)
}

/// `json.dumps(list(paths))`.
fn dump_paths(paths: &[String]) -> String {
    let value = Value::List(paths.iter().map(|path| Value::Str(path.clone())).collect());
    pyjson::dumps(&value, &DumpOptions::python_default()).expect("a list of strings always dumps")
}

/// `tuple(json.loads(text))` for a stored list of paths.
pub(crate) fn load_string_list(text: &str, column: &str) -> Result<Vec<String>, StoreError> {
    let value = pyjson::loads(text).map_err(|error| StoreError::Decode {
        class: "JSONDecodeError",
        message: error.python_message(text),
    })?;
    match value {
        Value::List(items) => items
            .into_iter()
            .map(|item| match item {
                Value::Str(text) => Ok(text),
                _ => Err(StoreError::Decode {
                    class: "TypeError",
                    message: format!("{column} holds a list item that is not a string"),
                }),
            })
            .collect(),
        _ => Err(StoreError::Decode {
            class: "TypeError",
            message: format!("{column} does not hold a JSON list"),
        }),
    }
}

fn parse_acquisition_state(value: &str) -> Result<AcquisitionState, StoreError> {
    AcquisitionState::parse(value).ok_or_else(|| StoreError::invalid_enum(value, "AcquisitionState"))
}

fn parse_attempt_state(value: &str) -> Result<AttemptState, StoreError> {
    AttemptState::parse(value).ok_or_else(|| StoreError::invalid_enum(value, "AttemptState"))
}

/// `_acquisition_from_row(row)`.
fn acquisition_from_row(row: &PgRow) -> Result<AcquisitionRecord, StoreError> {
    let replace_paths: Option<String> = row.try_get("replace_paths_json")?;
    Ok(AcquisitionRecord {
        avid: row.try_get("avid")?,
        state: parse_acquisition_state(row.try_get("state")?)?,
        source: row.try_get("source")?,
        note: row.try_get("note")?,
        archived_paths: load_string_list(row.try_get("archived_paths_json")?, "archived_paths_json")?,
        next_action_at: row.try_get("next_action_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        task_dir_path: row.try_get("task_dir_path")?,
        release_date: row.try_get("release_date")?,
        // `if row['replace_paths_json']`: NULL and '' both mean no replacement.
        replace_paths: match replace_paths.as_deref() {
            None | Some("") => Vec::new(),
            Some(text) => load_string_list(text, "replace_paths_json")?,
        },
        replace_after_attempt: row.try_get("replace_after_attempt")?,
    })
}

/// `_attempt_from_row(row)`.
fn attempt_from_row(row: &PgRow) -> Result<MagnetAttemptRecord, StoreError> {
    Ok(MagnetAttemptRecord {
        avid: row.try_get("avid")?,
        attempt_no: row.try_get("attempt_no")?,
        magnet: row.try_get("magnet")?,
        info_hash: row.try_get("info_hash")?,
        magnet_source: row.try_get("magnet_source")?,
        size_hint: row.try_get("size_hint")?,
        state: parse_attempt_state(row.try_get("state")?)?,
        progress: row.try_get("progress")?,
        error: row.try_get("error")?,
        submitted_at: row.try_get("submitted_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn acquisitions_from_rows(rows: &[PgRow]) -> Result<Vec<AcquisitionRecord>, LedgerError> {
    Ok(rows.iter().map(acquisition_from_row).collect::<Result<_, _>>()?)
}

fn attempts_from_rows(rows: &[PgRow]) -> Result<Vec<MagnetAttemptRecord>, LedgerError> {
    Ok(rows.iter().map(attempt_from_row).collect::<Result<_, _>>()?)
}

/// `sorted({h for h in info_hashes if h})`.
fn wanted_hashes(info_hashes: &[String]) -> Vec<String> {
    let set: BTreeSet<&String> = info_hashes.iter().filter(|hash| !hash.is_empty()).collect();
    set.into_iter().cloned().collect()
}

/// `AcquisitionRepository`: the ledger on PostgreSQL.
#[derive(Debug, Clone)]
pub struct AcquisitionRepository {
    database: Database,
}

impl AcquisitionRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    // -- discovery ---------------------------------------------------------

    /// `discover()`: record an AVID sighting; return whether it still needs magnets resolved.
    ///
    /// `false` means someone already owns this AVID: it is downloading, archived, ignored,
    /// waiting on an operator, or cooling down after a failed resolve. An accepted
    /// re-discovery normally takes the row over: its directory is repointed at
    /// `task_dir_path` and, when that moves it to another source's directory, the source
    /// follows, unless the row's source is one this sighting `yields_to`.
    pub async fn discover(
        &self,
        avid: &str,
        source: &str,
        now: DateTime<Utc>,
        sighting: Sighting,
    ) -> Result<bool, LedgerError> {
        let pool = self.database.get_pool().await?;
        let inserted: Option<String> = sqlx::query_scalar(
            "
            INSERT INTO archive_acquisitions
                (avid, state, source, task_dir_path, next_action_at, release_date, created_at, updated_at)
            VALUES ($1, 'discovered', $2, $3, $4, $5, $6, $6)
            ON CONFLICT (avid) DO NOTHING
            RETURNING avid
            ",
        )
        .bind(avid)
        .bind(source)
        .bind(&sighting.task_dir_path)
        .bind(sighting.next_action_at)
        .bind(sighting.release_date)
        .bind(now)
        .fetch_optional(&pool)
        .await?;
        if inserted.is_some() {
            return Ok(true);
        }
        let Some(existing) = self.get(avid).await? else {
            // Deleted between the insert and the read.
            return Ok(true);
        };
        let accepted = if existing.state == A::Discovered {
            true
        } else if RETRYABLE_STATES.contains(&existing.state) {
            sighting.wake || existing.next_action_at.is_none_or(|due| due <= now)
        } else {
            return Ok(false);
        };
        if accepted
            && let Some(release_date) = sighting.release_date
            && existing.release_date.is_none()
        {
            sqlx::query("UPDATE archive_acquisitions SET release_date = $2 WHERE avid = $1")
                .bind(avid)
                .bind(release_date)
                .execute(&pool)
                .await?;
        }
        let owned_elsewhere = existing.source != source && sighting.yields_to.contains(&existing.source);
        if accepted
            && let Some(task_dir_path) = &sighting.task_dir_path
            && existing.task_dir_path.as_ref() != Some(task_dir_path)
            && !owned_elsewhere
        {
            // The retry follows whichever category just re-discovered it, so repointing a
            // category takes effect from its next pass. A directory that belongs to another
            // source carries the source along: the row is now that source's to file.
            sqlx::query("UPDATE archive_acquisitions SET task_dir_path = $2, source = $3 WHERE avid = $1")
                .bind(avid)
                .bind(task_dir_path)
                .bind(source)
                .execute(&pool)
                .await?;
        }
        if accepted && let Some(next_action_at) = sighting.next_action_at {
            // A queueing source accepted an orphaned or cooled-down row: give it a timer or
            // the background pass would never look at it again.
            sqlx::query("UPDATE archive_acquisitions SET next_action_at = $2 WHERE avid = $1")
                .bind(avid)
                .bind(next_action_at)
                .execute(&pool)
                .await?;
        }
        Ok(accepted)
    }

    pub async fn get(&self, avid: &str) -> Result<Option<AcquisitionRecord>, LedgerError> {
        let pool = self.database.get_pool().await?;
        let row = sqlx::query("SELECT * FROM archive_acquisitions WHERE avid = $1")
            .bind(avid)
            .fetch_optional(&pool)
            .await?;
        Ok(row.as_ref().map(acquisition_from_row).transpose()?)
    }

    /// The ledger state of every listed AVID that has a row; absent ones are not tracked.
    pub async fn states_for(&self, avids: &[String]) -> Result<BTreeMap<String, AcquisitionState>, LedgerError> {
        if avids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let pool = self.database.get_pool().await?;
        let rows = sqlx::query("SELECT avid, state FROM archive_acquisitions WHERE avid = ANY($1::text[])")
            .bind(avids)
            .fetch_all(&pool)
            .await?;
        let mut states = BTreeMap::new();
        for row in rows {
            let state = parse_acquisition_state(row.try_get("state")?)?;
            states.insert(row.try_get("avid")?, state);
        }
        Ok(states)
    }

    /// The offline directory each listed AVID was fetched into, for the rows that recorded one.
    pub async fn task_dirs_for(&self, avids: &[String]) -> Result<BTreeMap<String, String>, LedgerError> {
        if avids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let pool = self.database.get_pool().await?;
        let rows = sqlx::query(
            "SELECT avid, task_dir_path FROM archive_acquisitions WHERE avid = ANY($1::text[]) AND task_dir_path IS NOT NULL",
        )
        .bind(avids)
        .fetch_all(&pool)
        .await?;
        let mut dirs = BTreeMap::new();
        for row in rows {
            dirs.insert(row.try_get("avid")?, row.try_get("task_dir_path")?);
        }
        Ok(dirs)
    }

    /// Fill in a release date the row was recorded without; never overwrites.
    pub async fn set_release_date(&self, avid: &str, release_date: NaiveDate) -> Result<bool, LedgerError> {
        let pool = self.database.get_pool().await?;
        let done =
            sqlx::query("UPDATE archive_acquisitions SET release_date = $2 WHERE avid = $1 AND release_date IS NULL")
                .bind(avid)
                .bind(release_date)
                .execute(&pool)
                .await?;
        Ok(done.rows_affected() == 1)
    }

    /// `begin_replacement()`: hand a title the library already holds back to the tracker for
    /// a new copy (换版).
    ///
    /// The row returns to `discovered` carrying the library paths of the copy it replaces; a
    /// title with no row yet gets one. Attempts already on the row stay, so their hashes are
    /// never tried again. `None` when the row is not in a [`REPLACEABLE_STATES`] state.
    pub async fn begin_replacement(
        &self,
        avid: &str,
        source: &str,
        task_dir_path: &str,
        replace_paths: &[String],
        now: DateTime<Utc>,
    ) -> Result<Option<AcquisitionRecord>, LedgerError> {
        let pool = self.database.get_pool().await?;
        let paths_json = dump_paths(replace_paths);
        let mut transaction = pool.begin().await?;
        let row = sqlx::query("SELECT state FROM archive_acquisitions WHERE avid = $1 FOR UPDATE")
            .bind(avid)
            .fetch_optional(&mut *transaction)
            .await?;
        match row {
            None => {
                sqlx::query(
                    "
                    INSERT INTO archive_acquisitions (
                        avid, state, source, task_dir_path, replace_paths_json, replace_after_attempt,
                        created_at, updated_at
                    )
                    VALUES ($1, 'discovered', $2, $3, $4, 0, $5, $5)
                    ",
                )
                .bind(avid)
                .bind(source)
                .bind(task_dir_path)
                .bind(&paths_json)
                .bind(now)
                .execute(&mut *transaction)
                .await?;
            }
            Some(row) => {
                let state = parse_acquisition_state(row.try_get("state")?)?;
                if !REPLACEABLE_STATES.contains(&state) {
                    // Python returns from inside `async with`, which commits (nothing was written).
                    transaction.commit().await?;
                    return Ok(None);
                }
                sqlx::query(
                    "
                    UPDATE archive_acquisitions
                    SET state = 'discovered', note = NULL, next_action_at = NULL, task_dir_path = $2,
                        replace_paths_json = $3, updated_at = $4,
                        replace_after_attempt = (
                            SELECT COALESCE(MAX(attempt_no), 0) FROM archive_magnet_attempts WHERE avid = $1
                        )
                    WHERE avid = $1
                    ",
                )
                .bind(avid)
                .bind(task_dir_path)
                .bind(&paths_json)
                .bind(now)
                .execute(&mut *transaction)
                .await?;
            }
        }
        transaction.commit().await?;
        self.get(avid).await
    }

    // -- acquisition transitions -------------------------------------------

    /// `transition()`: compare-and-set one acquisition's state; `false` when the row moved on.
    ///
    /// The fields are written together with the state, so a row is never observed in the new
    /// state without them. Reaching `archived` concludes a replacement as well.
    pub async fn transition(
        &self,
        avid: &str,
        expected: AcquisitionState,
        target: AcquisitionState,
        now: DateTime<Utc>,
        fields: TransitionFields,
    ) -> Result<bool, LedgerError> {
        if !acquisition_edges(expected).contains(&target) {
            return Err(IllegalTransitionError::acquisition(expected, target).into());
        }
        let pool = self.database.get_pool().await?;
        let done = sqlx::query(
            "
            UPDATE archive_acquisitions
            SET state = $3, note = $4, next_action_at = $5,
                archived_paths_json = COALESCE($6, archived_paths_json), updated_at = $7,
                replace_paths_json = CASE WHEN $3 = 'archived' THEN NULL ELSE replace_paths_json END,
                replace_after_attempt = CASE WHEN $3 = 'archived' THEN NULL ELSE replace_after_attempt END
            WHERE avid = $1 AND state = $2
            ",
        )
        .bind(avid)
        .bind(expected.as_str())
        .bind(target.as_str())
        .bind(fields.note)
        .bind(fields.next_action_at)
        .bind(fields.archived_paths.as_deref().map(dump_paths))
        .bind(now)
        .execute(&pool)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Acquisitions whose action timer has expired, oldest deadline first: cooled-down retries
    /// and queued discoveries alike.
    pub async fn due_for_retry(&self, now: DateTime<Utc>, limit: i64) -> Result<Vec<AcquisitionRecord>, LedgerError> {
        let pool = self.database.get_pool().await?;
        let rows = sqlx::query(
            "
            SELECT * FROM archive_acquisitions
            WHERE state = ANY($1::text[]) AND next_action_at IS NOT NULL AND next_action_at <= $2
            ORDER BY next_action_at, avid
            LIMIT $3
            ",
        )
        .bind(acquisition_values(DUE_STATES))
        .bind(now)
        .bind(limit)
        .fetch_all(&pool)
        .await?;
        acquisitions_from_rows(&rows)
    }

    /// AVIDs the tracker currently owns; reconcile must leave their folders alone.
    pub async fn active_avids(&self) -> Result<BTreeSet<String>, LedgerError> {
        let pool = self.database.get_pool().await?;
        let avids: Vec<String> =
            sqlx::query_scalar("SELECT avid FROM archive_acquisitions WHERE state = ANY($1::text[])")
                .bind(acquisition_values(ACTIVE_STATES))
                .fetch_all(&pool)
                .await?;
        Ok(avids.into_iter().collect())
    }

    /// Offline directories holding an acquisition that is still in flight, so the tracker polls
    /// a directory a source pinned outside the configured ones.
    pub async fn active_task_dirs(&self) -> Result<Vec<String>, LedgerError> {
        let pool = self.database.get_pool().await?;
        Ok(sqlx::query_scalar(
            "
            SELECT DISTINCT task_dir_path FROM archive_acquisitions
            WHERE state = ANY($1::text[]) AND task_dir_path IS NOT NULL
            ORDER BY task_dir_path
            ",
        )
        .bind(acquisition_values(ACTIVE_STATES))
        .fetch_all(&pool)
        .await?)
    }

    /// The offline directory this source's newest acquisition was queued under (the manual
    /// picker's memory).
    pub async fn latest_task_dir(&self, source: &str) -> Result<Option<String>, LedgerError> {
        let pool = self.database.get_pool().await?;
        Ok(sqlx::query_scalar(
            "
            SELECT task_dir_path FROM archive_acquisitions
            WHERE source = $1 AND task_dir_path IS NOT NULL
            ORDER BY created_at DESC, avid DESC
            LIMIT 1
            ",
        )
        .bind(source)
        .fetch_optional(&pool)
        .await?)
    }

    // -- attempts ------------------------------------------------------------

    /// Append candidates as pending attempts, skipping hashes already tried.
    ///
    /// Numbering continues from the highest existing attempt. The parent acquisition row is
    /// locked first: locking the attempts would protect nothing on the first call.
    pub async fn add_attempts(
        &self,
        avid: &str,
        candidates: &[MagnetCandidate],
        now: DateTime<Utc>,
    ) -> Result<usize, LedgerError> {
        if candidates.is_empty() {
            return Ok(0);
        }
        let pool = self.database.get_pool().await?;
        let mut transaction = pool.begin().await?;
        sqlx::query("SELECT 1 FROM archive_acquisitions WHERE avid = $1 FOR UPDATE")
            .bind(avid)
            .execute(&mut *transaction)
            .await?;
        let rows = sqlx::query("SELECT attempt_no, info_hash FROM archive_magnet_attempts WHERE avid = $1")
            .bind(avid)
            .fetch_all(&mut *transaction)
            .await?;
        let mut next_no = 1;
        let mut seen = BTreeSet::new();
        for row in &rows {
            let attempt_no: i32 = row.try_get("attempt_no")?;
            next_no = next_no.max(attempt_no + 1);
            if let Some(info_hash) = row.try_get::<Option<String>, _>("info_hash")? {
                seen.insert(info_hash);
            }
        }
        let mut pending: Vec<&MagnetCandidate> = Vec::new();
        for candidate in candidates {
            if let Some(info_hash) = &candidate.info_hash {
                if seen.contains(info_hash) {
                    continue;
                }
                seen.insert(info_hash.clone());
            }
            pending.push(candidate);
        }
        if pending.is_empty() {
            // As Python's `return 0` inside `async with`: the transaction commits, unchanged.
            transaction.commit().await?;
            return Ok(0);
        }
        for (offset, candidate) in pending.iter().enumerate() {
            let attempt_no = next_no + i32::try_from(offset).expect("a handful of candidates");
            let size_hint = candidate
                .size_hint
                .map(|size| i64::try_from(size).map_err(|_| out_of_range_in_element(6, offset, size, 64)))
                .transpose()?;
            sqlx::query(
                "
                INSERT INTO archive_magnet_attempts (
                    avid, attempt_no, magnet, info_hash, magnet_source, size_hint, state, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7)
                ",
            )
            .bind(avid)
            .bind(attempt_no)
            .bind(&candidate.magnet)
            .bind(&candidate.info_hash)
            .bind(&candidate.source)
            .bind(size_hint)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(pending.len())
    }

    /// Atomically take the lowest pending attempt and mark it submitted.
    ///
    /// The claim happens before the CloudDrive call, so a crash mid-submit leaves the attempt
    /// submitted and the tracker reconciles it rather than double-submitting it. A pending
    /// attempt whose hash is live on another attempt is passed over and stays pending.
    pub async fn claim_next_pending(
        &self,
        avid: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<MagnetAttemptRecord>, LedgerError> {
        let pool = self.database.get_pool().await?;
        for _ in 0..CLAIM_RETRIES {
            let claimed = sqlx::query(
                "
                UPDATE archive_magnet_attempts SET state = 'submitted', submitted_at = $2, updated_at = $2
                WHERE avid = $1 AND state = 'pending' AND attempt_no = (
                    SELECT attempt_no FROM archive_magnet_attempts AS attempt
                    WHERE avid = $1 AND state = 'pending'
                      AND NOT EXISTS (
                          SELECT 1 FROM archive_magnet_attempts AS live
                          WHERE live.info_hash = attempt.info_hash AND live.state = ANY($3::text[])
                      )
                    ORDER BY attempt_no
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING *
                ",
            )
            .bind(avid)
            .bind(now)
            .bind(attempt_values(LIVE_ATTEMPT_STATES))
            .fetch_optional(&pool)
            .await;
            match claimed {
                // Another AVID took the same hash live between our check and our write; the
                // next pass sees it and passes this attempt over.
                Err(error) if StoreError::is_unique_violation(&error) => {}
                Err(error) => return Err(error.into()),
                Ok(row) => return Ok(row.as_ref().map(attempt_from_row).transpose()?),
            }
        }
        Ok(None)
    }

    /// Take one specific pending attempt, for a magnet an operator chose; [`MagnetInUseError`]
    /// when the magnet is live on another attempt (the attempt stays pending).
    pub async fn claim_attempt(
        &self,
        avid: &str,
        attempt_no: i32,
        now: DateTime<Utc>,
    ) -> Result<Option<MagnetAttemptRecord>, LedgerError> {
        let pool = self.database.get_pool().await?;
        let claimed = sqlx::query(
            "
            UPDATE archive_magnet_attempts SET state = 'submitted', submitted_at = $3, updated_at = $3
            WHERE avid = $1 AND attempt_no = $2 AND state = 'pending'
            RETURNING *
            ",
        )
        .bind(avid)
        .bind(attempt_no)
        .bind(now)
        .fetch_optional(&pool)
        .await;
        match claimed {
            Err(error) if StoreError::is_unique_violation(&error) => Err(MagnetInUseError {
                avid: avid.to_owned(),
                attempt_no,
            }
            .into()),
            Err(error) => Err(error.into()),
            Ok(row) => Ok(row.as_ref().map(attempt_from_row).transpose()?),
        }
    }

    /// `transition_attempt()`: the attempt's compare-and-set.
    pub async fn transition_attempt(
        &self,
        avid: &str,
        attempt_no: i32,
        expected: AttemptState,
        target: AttemptState,
        now: DateTime<Utc>,
        error: Option<String>,
    ) -> Result<bool, LedgerError> {
        if !attempt_edges(expected).contains(&target) {
            return Err(IllegalTransitionError::attempt(expected, target).into());
        }
        let pool = self.database.get_pool().await?;
        let done = sqlx::query(
            "
            UPDATE archive_magnet_attempts SET state = $4, error = $5, updated_at = $6
            WHERE avid = $1 AND attempt_no = $2 AND state = $3
            ",
        )
        .bind(avid)
        .bind(attempt_no)
        .bind(expected.as_str())
        .bind(target.as_str())
        .bind(error)
        .bind(now)
        .execute(&pool)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Refresh an in-flight attempt, writing only when something actually moved.
    ///
    /// Leaving `updated_at` alone on an unchanged poll is what makes stall detection possible.
    /// Conclusions go through [`transition_attempt`](Self::transition_attempt).
    pub async fn record_progress(
        &self,
        avid: &str,
        attempt_no: i32,
        state: AttemptState,
        progress: Option<f64>,
        now: DateTime<Utc>,
    ) -> Result<bool, LedgerError> {
        if !IN_FLIGHT_ATTEMPT_STATES.contains(&state) {
            return Err(LedgerError::Value(format!(
                "record_progress only refreshes in-flight attempts, got {state}"
            )));
        }
        let pool = self.database.get_pool().await?;
        let done = sqlx::query(
            "
            UPDATE archive_magnet_attempts SET state = $3, progress = $4, updated_at = $5
            WHERE avid = $1 AND attempt_no = $2
              AND state IN ('submitted', 'downloading')
              AND (state IS DISTINCT FROM $3 OR progress IS DISTINCT FROM $4)
            ",
        )
        .bind(avid)
        .bind(attempt_no)
        .bind(state.as_str())
        .bind(progress)
        .bind(now)
        .execute(&pool)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Look up live attempts by CloudDrive infoHash; the tracker's join key. A task whose
    /// attempt already concluded drops out of the mapping.
    pub async fn attempts_by_info_hash(
        &self,
        info_hashes: &[String],
    ) -> Result<BTreeMap<String, MagnetAttemptRecord>, LedgerError> {
        let hashes = wanted_hashes(info_hashes);
        if hashes.is_empty() {
            return Ok(BTreeMap::new());
        }
        let pool = self.database.get_pool().await?;
        let rows = sqlx::query(
            "
            SELECT * FROM archive_magnet_attempts
            WHERE info_hash = ANY($1::text[]) AND state = ANY($2::text[])
            ",
        )
        .bind(hashes)
        .bind(attempt_values(LIVE_ATTEMPT_STATES))
        .fetch_all(&pool)
        .await?;
        let mut found = BTreeMap::new();
        for attempt in attempts_from_rows(&rows)? {
            // The partial unique index keeps one live attempt per hash, which is never NULL here.
            let key = attempt.info_hash.clone().unwrap_or_default();
            found.insert(key, attempt);
        }
        Ok(found)
    }

    /// The hashes among these whose attempt was archived and that no live attempt holds; their
    /// offline tasks are history only.
    pub async fn archived_info_hashes(&self, info_hashes: &[String]) -> Result<BTreeSet<String>, LedgerError> {
        let hashes = wanted_hashes(info_hashes);
        if hashes.is_empty() {
            return Ok(BTreeSet::new());
        }
        let pool = self.database.get_pool().await?;
        let found: Vec<String> = sqlx::query_scalar(
            "
            SELECT DISTINCT archived.info_hash FROM archive_magnet_attempts AS archived
            WHERE archived.info_hash = ANY($1::text[]) AND archived.state = $2
              AND NOT EXISTS (
                SELECT 1 FROM archive_magnet_attempts AS live
                WHERE live.info_hash = archived.info_hash AND live.state = ANY($3::text[])
              )
            ",
        )
        .bind(hashes)
        .bind(T::Archived.as_str())
        .bind(attempt_values(LIVE_ATTEMPT_STATES))
        .fetch_all(&pool)
        .await?;
        Ok(found.into_iter().collect())
    }

    /// Attempts CloudDrive should still be working on, oldest update first.
    pub async fn in_flight_attempts(&self) -> Result<Vec<MagnetAttemptRecord>, LedgerError> {
        let pool = self.database.get_pool().await?;
        let rows = sqlx::query(
            "
            SELECT * FROM archive_magnet_attempts
            WHERE state = ANY($1::text[])
            ORDER BY updated_at, avid, attempt_no
            ",
        )
        .bind(attempt_values(IN_FLIGHT_ATTEMPT_STATES))
        .fetch_all(&pool)
        .await?;
        attempts_from_rows(&rows)
    }

    pub async fn attempts_for(&self, avid: &str) -> Result<Vec<MagnetAttemptRecord>, LedgerError> {
        let pool = self.database.get_pool().await?;
        let rows = sqlx::query("SELECT * FROM archive_magnet_attempts WHERE avid = $1 ORDER BY attempt_no")
            .bind(avid)
            .fetch_all(&pool)
            .await?;
        attempts_from_rows(&rows)
    }

    // -- listing -------------------------------------------------------------

    pub async fn list_acquisitions(
        &self,
        states: Option<&[AcquisitionState]>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<AcquisitionRecord>, LedgerError> {
        let pool = self.database.get_pool().await?;
        let rows = match states {
            None => {
                sqlx::query("SELECT * FROM archive_acquisitions ORDER BY updated_at DESC, avid LIMIT $1 OFFSET $2")
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(&pool)
                    .await?
            }
            Some(states) => {
                sqlx::query(
                    "
                    SELECT * FROM archive_acquisitions WHERE state = ANY($1::text[])
                    ORDER BY updated_at DESC, avid LIMIT $2 OFFSET $3
                    ",
                )
                .bind(acquisition_values(states))
                .bind(limit)
                .bind(offset)
                .fetch_all(&pool)
                .await?
            }
        };
        acquisitions_from_rows(&rows)
    }

    /// `count_by_state()`, in the order PostgreSQL returns the groups.
    pub async fn count_by_state(&self) -> Result<Vec<(AcquisitionState, i64)>, LedgerError> {
        let pool = self.database.get_pool().await?;
        let rows = sqlx::query("SELECT state, COUNT(*) AS total FROM archive_acquisitions GROUP BY state")
            .fetch_all(&pool)
            .await?;
        let mut counts = Vec::with_capacity(rows.len());
        for row in rows {
            counts.push((parse_acquisition_state(row.try_get("state")?)?, row.try_get("total")?));
        }
        Ok(counts)
    }
}

impl Ledger for AcquisitionRepository {
    fn discover<'a>(
        &'a self,
        avid: &'a str,
        source: &'a str,
        now: DateTime<Utc>,
        sighting: Sighting,
    ) -> BoxFuture<'a, Result<bool, LedgerError>> {
        Box::pin(Self::discover(self, avid, source, now, sighting))
    }

    fn get<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<AcquisitionRecord>, LedgerError>> {
        Box::pin(Self::get(self, avid))
    }

    fn states_for<'a>(
        &'a self,
        avids: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeMap<String, AcquisitionState>, LedgerError>> {
        Box::pin(Self::states_for(self, avids))
    }

    fn task_dirs_for<'a>(
        &'a self,
        avids: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeMap<String, String>, LedgerError>> {
        Box::pin(Self::task_dirs_for(self, avids))
    }

    fn set_release_date<'a>(
        &'a self,
        avid: &'a str,
        release_date: NaiveDate,
    ) -> BoxFuture<'a, Result<bool, LedgerError>> {
        Box::pin(Self::set_release_date(self, avid, release_date))
    }

    fn begin_replacement<'a>(
        &'a self,
        avid: &'a str,
        source: &'a str,
        task_dir_path: &'a str,
        replace_paths: &'a [String],
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<AcquisitionRecord>, LedgerError>> {
        Box::pin(Self::begin_replacement(
            self,
            avid,
            source,
            task_dir_path,
            replace_paths,
            now,
        ))
    }

    fn transition<'a>(
        &'a self,
        avid: &'a str,
        expected: AcquisitionState,
        target: AcquisitionState,
        now: DateTime<Utc>,
        fields: TransitionFields,
    ) -> BoxFuture<'a, Result<bool, LedgerError>> {
        Box::pin(Self::transition(self, avid, expected, target, now, fields))
    }

    fn due_for_retry(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> BoxFuture<'_, Result<Vec<AcquisitionRecord>, LedgerError>> {
        Box::pin(Self::due_for_retry(self, now, limit))
    }

    fn active_avids(&self) -> BoxFuture<'_, Result<BTreeSet<String>, LedgerError>> {
        Box::pin(Self::active_avids(self))
    }

    fn active_task_dirs(&self) -> BoxFuture<'_, Result<Vec<String>, LedgerError>> {
        Box::pin(Self::active_task_dirs(self))
    }

    fn latest_task_dir<'a>(&'a self, source: &'a str) -> BoxFuture<'a, Result<Option<String>, LedgerError>> {
        Box::pin(Self::latest_task_dir(self, source))
    }

    fn add_attempts<'a>(
        &'a self,
        avid: &'a str,
        candidates: &'a [MagnetCandidate],
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<usize, LedgerError>> {
        Box::pin(Self::add_attempts(self, avid, candidates, now))
    }

    fn claim_next_pending<'a>(
        &'a self,
        avid: &'a str,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<MagnetAttemptRecord>, LedgerError>> {
        Box::pin(Self::claim_next_pending(self, avid, now))
    }

    fn claim_attempt<'a>(
        &'a self,
        avid: &'a str,
        attempt_no: i32,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<MagnetAttemptRecord>, LedgerError>> {
        Box::pin(Self::claim_attempt(self, avid, attempt_no, now))
    }

    fn transition_attempt<'a>(
        &'a self,
        avid: &'a str,
        attempt_no: i32,
        expected: AttemptState,
        target: AttemptState,
        now: DateTime<Utc>,
        error: Option<String>,
    ) -> BoxFuture<'a, Result<bool, LedgerError>> {
        Box::pin(Self::transition_attempt(
            self, avid, attempt_no, expected, target, now, error,
        ))
    }

    fn record_progress<'a>(
        &'a self,
        avid: &'a str,
        attempt_no: i32,
        state: AttemptState,
        progress: Option<f64>,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<bool, LedgerError>> {
        Box::pin(Self::record_progress(self, avid, attempt_no, state, progress, now))
    }

    fn attempts_by_info_hash<'a>(
        &'a self,
        info_hashes: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeMap<String, MagnetAttemptRecord>, LedgerError>> {
        Box::pin(Self::attempts_by_info_hash(self, info_hashes))
    }

    fn archived_info_hashes<'a>(
        &'a self,
        info_hashes: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeSet<String>, LedgerError>> {
        Box::pin(Self::archived_info_hashes(self, info_hashes))
    }

    fn in_flight_attempts(&self) -> BoxFuture<'_, Result<Vec<MagnetAttemptRecord>, LedgerError>> {
        Box::pin(Self::in_flight_attempts(self))
    }

    fn attempts_for<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Vec<MagnetAttemptRecord>, LedgerError>> {
        Box::pin(Self::attempts_for(self, avid))
    }

    fn list_acquisitions<'a>(
        &'a self,
        states: Option<&'a [AcquisitionState]>,
        limit: i64,
        offset: i64,
    ) -> BoxFuture<'a, Result<Vec<AcquisitionRecord>, LedgerError>> {
        Box::pin(Self::list_acquisitions(self, states, limit, offset))
    }

    fn count_by_state(&self) -> BoxFuture<'_, Result<Vec<(AcquisitionState, i64)>, LedgerError>> {
        Box::pin(Self::count_by_state(self))
    }
}

#[cfg(test)]
mod tests;
