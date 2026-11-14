//! The in-memory ledger of the Python tests (`FakeLedger` in `tests/test_monitor_rss.py`).
//!
//! The Python intake, RSS, tracker, manual, replace and API tests run their pipelines against
//! this stand-in for `AcquisitionRepository`; the real repository is exercised against
//! PostgreSQL. Its rules are the fake's own (for one, `due_for_retry` skips queued
//! discoveries, which the real query includes), copied rule for rule so ported tests assert
//! what the Python tests assert. Behind the `test-support` feature for other crates' tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};

use chrono::{DateTime, NaiveDate, TimeZone as _, Utc};
use indexmap::IndexMap;

use crate::acquisitions::{
    AcquisitionRecord, AcquisitionState, AttemptState, Ledger, LedgerError, MagnetAttemptRecord, MagnetCandidate,
    Sighting, TransitionFields,
};
use crate::support::BoxFuture;

/// `now_stub()`: the fake's `created_at` / `updated_at`.
pub fn now_stub() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 13, 0, 0, 0).single().expect("valid")
}

const LIVE: &[AttemptState] = &[
    AttemptState::Submitted,
    AttemptState::Downloading,
    AttemptState::Finished,
    AttemptState::Archiving,
];
const IN_FLIGHT: &[AttemptState] = &[AttemptState::Submitted, AttemptState::Downloading];

/// The fake's dictionaries, insertion-ordered like Python's. Tests read and seed them through
/// [`FakeLedger::state`].
#[derive(Debug, Default)]
pub struct FakeLedgerState {
    pub states: IndexMap<String, AcquisitionState>,
    pub sources: IndexMap<String, String>,
    pub notes: IndexMap<String, Option<String>>,
    pub next_action_at: IndexMap<String, Option<DateTime<Utc>>>,
    pub attempts: IndexMap<String, Vec<MagnetAttemptRecord>>,
    pub task_dirs: IndexMap<String, Option<String>>,
    pub archived_paths: IndexMap<String, Vec<String>>,
    pub release_dates: IndexMap<String, Option<NaiveDate>>,
    pub replace_paths: IndexMap<String, Vec<String>>,
    pub replace_after: IndexMap<String, i32>,
}

impl FakeLedgerState {
    fn record(&self, avid: &str) -> Option<AcquisitionRecord> {
        let state = *self.states.get(avid)?;
        Some(AcquisitionRecord {
            avid: avid.to_owned(),
            state,
            source: self
                .sources
                .get(avid)
                .cloned()
                .unwrap_or_else(|| "rss:Actor".to_owned()),
            note: self.notes.get(avid).cloned().flatten(),
            archived_paths: Vec::new(),
            next_action_at: self.next_action_at.get(avid).copied().flatten(),
            created_at: now_stub(),
            updated_at: now_stub(),
            task_dir_path: self.task_dirs.get(avid).cloned().flatten(),
            release_date: self.release_dates.get(avid).copied().flatten(),
            replace_paths: self.replace_paths.get(avid).cloned().unwrap_or_default(),
            replace_after_attempt: self.replace_after.get(avid).copied(),
        })
    }

    fn active_avids(&self) -> BTreeSet<String> {
        self.states
            .iter()
            .filter(|(_, state)| matches!(state, AcquisitionState::Discovered | AcquisitionState::Downloading))
            .map(|(avid, _)| avid.clone())
            .collect()
    }

    fn attempt_mut(&mut self, avid: &str, attempt_no: i32) -> Option<&mut MagnetAttemptRecord> {
        self.attempts
            .get_mut(avid)?
            .iter_mut()
            .find(|attempt| attempt.attempt_no == attempt_no)
    }
}

/// The range Python's `items[start:stop]` takes from a list of `len` items (negative bounds count
/// from the end, out-of-range bounds clamp).
fn py_slice(len: usize, start: i64, stop: i64) -> std::ops::Range<usize> {
    let len_i = i64::try_from(len).unwrap_or(i64::MAX);
    let bound = |index: i64| {
        let index = if index < 0 {
            (index + len_i).max(0)
        } else {
            index.min(len_i)
        };
        usize::try_from(index).unwrap_or(0)
    };
    let (start, stop) = (bound(start), bound(stop));
    start..stop.max(start)
}

/// `FakeLedger(known=...)`.
#[derive(Debug, Default)]
pub struct FakeLedger {
    state: Mutex<FakeLedgerState>,
}

impl FakeLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// `FakeLedger(known={avid: state, ...})`.
    pub fn with_known(known: &[(&str, AcquisitionState)]) -> Self {
        let ledger = Self::new();
        {
            let mut state = ledger.state();
            for (avid, known_state) in known {
                state.states.insert((*avid).to_owned(), *known_state);
            }
        }
        ledger
    }

    /// The fake's dictionaries, for assertions and seeding.
    pub fn state(&self) -> MutexGuard<'_, FakeLedgerState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `attempt_states(avid)`.
    pub fn attempt_states(&self, avid: &str) -> Vec<AttemptState> {
        self.state()
            .attempts
            .get(avid)
            .map(|attempts| attempts.iter().map(|attempt| attempt.state).collect())
            .unwrap_or_default()
    }

    /// `magnets(avid)`.
    pub fn magnets(&self, avid: &str) -> Vec<String> {
        self.state()
            .attempts
            .get(avid)
            .map(|attempts| attempts.iter().map(|attempt| attempt.magnet.clone()).collect())
            .unwrap_or_default()
    }

    fn discover_sync(&self, avid: &str, source: &str, now: DateTime<Utc>, sighting: Sighting) -> bool {
        let mut state = self.state();
        let Some(current) = state.states.get(avid).copied() else {
            state.states.insert(avid.to_owned(), AcquisitionState::Discovered);
            state.sources.insert(avid.to_owned(), source.to_owned());
            state.task_dirs.insert(avid.to_owned(), sighting.task_dir_path);
            state.next_action_at.insert(avid.to_owned(), sighting.next_action_at);
            state.release_dates.insert(avid.to_owned(), sighting.release_date);
            return true;
        };
        let accepted = match current {
            AcquisitionState::Discovered => true,
            AcquisitionState::ResolveFailed | AcquisitionState::Exhausted => {
                let due = state.next_action_at.get(avid).copied().flatten();
                sighting.wake || due.is_none_or(|due| due <= now)
            }
            _ => return false,
        };
        if accepted
            && let Some(release_date) = sighting.release_date
            && state.release_dates.get(avid).copied().flatten().is_none()
        {
            state.release_dates.insert(avid.to_owned(), Some(release_date));
        }
        let owner = state.sources.get(avid).cloned().unwrap_or_else(|| source.to_owned());
        let owned_elsewhere = owner != source && sighting.yields_to.contains(&owner);
        if accepted
            && let Some(task_dir_path) = &sighting.task_dir_path
            && state.task_dirs.get(avid).cloned().flatten().as_ref() != Some(task_dir_path)
            && !owned_elsewhere
        {
            state.task_dirs.insert(avid.to_owned(), Some(task_dir_path.clone()));
            state.sources.insert(avid.to_owned(), source.to_owned());
        }
        if accepted && let Some(next_action_at) = sighting.next_action_at {
            state.next_action_at.insert(avid.to_owned(), Some(next_action_at));
        }
        accepted
    }

    fn begin_replacement_sync(
        &self,
        avid: &str,
        source: &str,
        task_dir_path: &str,
        replace_paths: &[String],
    ) -> Option<AcquisitionRecord> {
        let mut state = self.state();
        if let Some(current) = state.states.get(avid)
            && !matches!(
                current,
                AcquisitionState::Archived
                    | AcquisitionState::Ignored
                    | AcquisitionState::ResolveFailed
                    | AcquisitionState::Exhausted
            )
        {
            return None;
        }
        state
            .sources
            .entry(avid.to_owned())
            .or_insert_with(|| source.to_owned());
        state.states.insert(avid.to_owned(), AcquisitionState::Discovered);
        state.notes.insert(avid.to_owned(), None);
        state.next_action_at.insert(avid.to_owned(), None);
        state.task_dirs.insert(avid.to_owned(), Some(task_dir_path.to_owned()));
        state.replace_paths.insert(avid.to_owned(), replace_paths.to_vec());
        let highest = state
            .attempts
            .get(avid)
            .and_then(|attempts| attempts.iter().map(|attempt| attempt.attempt_no).max())
            .unwrap_or(0);
        state.replace_after.insert(avid.to_owned(), highest);
        state.record(avid)
    }

    fn transition_sync(
        &self,
        avid: &str,
        expected: AcquisitionState,
        target: AcquisitionState,
        fields: TransitionFields,
    ) -> bool {
        let mut state = self.state();
        if state.states.get(avid) != Some(&expected) {
            return false;
        }
        state.states.insert(avid.to_owned(), target);
        state.notes.insert(avid.to_owned(), fields.note);
        state.next_action_at.insert(avid.to_owned(), fields.next_action_at);
        if target == AcquisitionState::Archived {
            state.replace_paths.shift_remove(avid);
            state.replace_after.shift_remove(avid);
        }
        if let Some(paths) = fields.archived_paths {
            state.archived_paths.insert(avid.to_owned(), paths);
        }
        true
    }

    fn add_attempts_sync(&self, avid: &str, candidates: &[MagnetCandidate], now: DateTime<Utc>) -> usize {
        let mut state = self.state();
        let existing = state.attempts.entry(avid.to_owned()).or_default();
        let mut seen: BTreeSet<String> = existing
            .iter()
            .filter_map(|attempt| attempt.info_hash.clone().filter(|hash| !hash.is_empty()))
            .collect();
        let mut added = 0;
        for candidate in candidates {
            let info_hash = candidate.info_hash.clone().filter(|hash| !hash.is_empty());
            if let Some(hash) = &info_hash {
                if seen.contains(hash) {
                    continue;
                }
                seen.insert(hash.clone());
            }
            existing.push(MagnetAttemptRecord {
                avid: avid.to_owned(),
                attempt_no: i32::try_from(existing.len() + 1).expect("a handful of attempts"),
                magnet: candidate.magnet.clone(),
                info_hash: candidate.info_hash.clone(),
                magnet_source: candidate.source.clone(),
                // Python's fake keeps any int; this one holds what the record type can.
                size_hint: candidate
                    .size_hint
                    .map(|size| i64::try_from(size).expect("the fake holds sizes within i64")),
                state: AttemptState::Pending,
                progress: None,
                error: None,
                submitted_at: None,
                updated_at: now,
            });
            added += 1;
        }
        added
    }

    fn claim_sync(&self, avid: &str, attempt_no: Option<i32>, now: DateTime<Utc>) -> Option<MagnetAttemptRecord> {
        let mut state = self.state();
        let attempt = state.attempts.get_mut(avid)?.iter_mut().find(|attempt| {
            attempt.state == AttemptState::Pending && attempt_no.is_none_or(|number| attempt.attempt_no == number)
        })?;
        attempt.state = AttemptState::Submitted;
        attempt.submitted_at = Some(now);
        Some(attempt.clone())
    }

    fn transition_attempt_sync(
        &self,
        avid: &str,
        attempt_no: i32,
        expected: AttemptState,
        target: AttemptState,
        error: Option<String>,
    ) -> bool {
        let mut state = self.state();
        let Some(attempt) = state.attempts.get_mut(avid).and_then(|attempts| {
            attempts
                .iter_mut()
                .find(|attempt| attempt.attempt_no == attempt_no && attempt.state == expected)
        }) else {
            return false;
        };
        attempt.state = target;
        attempt.error = error;
        true
    }

    fn record_progress_sync(
        &self,
        avid: &str,
        attempt_no: i32,
        target: AttemptState,
        progress: Option<f64>,
        now: DateTime<Utc>,
    ) -> bool {
        let mut state = self.state();
        let Some(attempt) = state.attempt_mut(avid, attempt_no) else {
            return false;
        };
        if !IN_FLIGHT.contains(&attempt.state) {
            return false;
        }
        if attempt.state == target && attempt.progress == progress {
            return false;
        }
        attempt.state = target;
        attempt.progress = progress;
        attempt.updated_at = now;
        true
    }

    fn all_attempts(&self) -> Vec<MagnetAttemptRecord> {
        self.state().attempts.values().flatten().cloned().collect()
    }
}

fn ready<'a, T: Send + 'a>(value: T) -> BoxFuture<'a, Result<T, LedgerError>> {
    Box::pin(std::future::ready(Ok(value)))
}

impl Ledger for FakeLedger {
    fn discover<'a>(
        &'a self,
        avid: &'a str,
        source: &'a str,
        now: DateTime<Utc>,
        sighting: Sighting,
    ) -> BoxFuture<'a, Result<bool, LedgerError>> {
        ready(self.discover_sync(avid, source, now, sighting))
    }

    fn get<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<AcquisitionRecord>, LedgerError>> {
        ready(self.state().record(avid))
    }

    /// Not in the Python fake (its tests never call it); answered from the states.
    fn states_for<'a>(
        &'a self,
        avids: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeMap<String, AcquisitionState>, LedgerError>> {
        let state = self.state();
        let found = avids
            .iter()
            .filter_map(|avid| state.states.get(avid).map(|known| (avid.clone(), *known)))
            .collect();
        ready(found)
    }

    /// Not in the Python fake (its tests never call it); answered from the directories.
    fn task_dirs_for<'a>(
        &'a self,
        avids: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeMap<String, String>, LedgerError>> {
        let state = self.state();
        let found = avids
            .iter()
            .filter_map(|avid| {
                state
                    .task_dirs
                    .get(avid)
                    .cloned()
                    .flatten()
                    .map(|dir| (avid.clone(), dir))
            })
            .collect();
        ready(found)
    }

    fn set_release_date<'a>(
        &'a self,
        avid: &'a str,
        release_date: NaiveDate,
    ) -> BoxFuture<'a, Result<bool, LedgerError>> {
        let mut state = self.state();
        if !state.states.contains_key(avid) || state.release_dates.get(avid).copied().flatten().is_some() {
            return ready(false);
        }
        state.release_dates.insert(avid.to_owned(), Some(release_date));
        ready(true)
    }

    fn begin_replacement<'a>(
        &'a self,
        avid: &'a str,
        source: &'a str,
        task_dir_path: &'a str,
        replace_paths: &'a [String],
        _now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<AcquisitionRecord>, LedgerError>> {
        ready(self.begin_replacement_sync(avid, source, task_dir_path, replace_paths))
    }

    fn transition<'a>(
        &'a self,
        avid: &'a str,
        expected: AcquisitionState,
        target: AcquisitionState,
        _now: DateTime<Utc>,
        fields: TransitionFields,
    ) -> BoxFuture<'a, Result<bool, LedgerError>> {
        ready(self.transition_sync(avid, expected, target, fields))
    }

    fn due_for_retry(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> BoxFuture<'_, Result<Vec<AcquisitionRecord>, LedgerError>> {
        let state = self.state();
        let mut due: Vec<&String> = state
            .states
            .iter()
            .filter(|(avid, known)| {
                matches!(known, AcquisitionState::ResolveFailed | AcquisitionState::Exhausted)
                    && state
                        .next_action_at
                        .get(*avid)
                        .copied()
                        .flatten()
                        .is_some_and(|at| at <= now)
            })
            .map(|(avid, _)| avid)
            .collect();
        due.sort();
        // `due[:limit]`.
        let records = due[py_slice(due.len(), 0, limit)]
            .iter()
            .filter_map(|avid| state.record(avid))
            .collect();
        ready(records)
    }

    fn active_avids(&self) -> BoxFuture<'_, Result<BTreeSet<String>, LedgerError>> {
        ready(self.state().active_avids())
    }

    fn active_task_dirs(&self) -> BoxFuture<'_, Result<Vec<String>, LedgerError>> {
        let state = self.state();
        let active = state.active_avids();
        let dirs: BTreeSet<String> = state
            .task_dirs
            .iter()
            .filter(|(avid, _)| active.contains(*avid))
            .filter_map(|(_, dir)| dir.clone().filter(|dir| !dir.is_empty()))
            .collect();
        ready(dirs.into_iter().collect())
    }

    fn latest_task_dir<'a>(&'a self, source: &'a str) -> BoxFuture<'a, Result<Option<String>, LedgerError>> {
        // Insertion order stands in for created_at: the fake records one sighting per AVID.
        let state = self.state();
        let found = state
            .sources
            .iter()
            .rev()
            .filter(|(_, owner)| owner.as_str() == source)
            .find_map(|(avid, _)| {
                state
                    .task_dirs
                    .get(avid)
                    .cloned()
                    .flatten()
                    .filter(|dir| !dir.is_empty())
            });
        ready(found)
    }

    fn add_attempts<'a>(
        &'a self,
        avid: &'a str,
        candidates: &'a [MagnetCandidate],
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<usize, LedgerError>> {
        ready(self.add_attempts_sync(avid, candidates, now))
    }

    fn claim_next_pending<'a>(
        &'a self,
        avid: &'a str,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<MagnetAttemptRecord>, LedgerError>> {
        ready(self.claim_sync(avid, None, now))
    }

    fn claim_attempt<'a>(
        &'a self,
        avid: &'a str,
        attempt_no: i32,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<MagnetAttemptRecord>, LedgerError>> {
        ready(self.claim_sync(avid, Some(attempt_no), now))
    }

    fn transition_attempt<'a>(
        &'a self,
        avid: &'a str,
        attempt_no: i32,
        expected: AttemptState,
        target: AttemptState,
        _now: DateTime<Utc>,
        error: Option<String>,
    ) -> BoxFuture<'a, Result<bool, LedgerError>> {
        ready(self.transition_attempt_sync(avid, attempt_no, expected, target, error))
    }

    fn record_progress<'a>(
        &'a self,
        avid: &'a str,
        attempt_no: i32,
        state: AttemptState,
        progress: Option<f64>,
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<bool, LedgerError>> {
        ready(self.record_progress_sync(avid, attempt_no, state, progress, now))
    }

    fn attempts_by_info_hash<'a>(
        &'a self,
        info_hashes: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeMap<String, MagnetAttemptRecord>, LedgerError>> {
        let wanted: BTreeSet<&String> = info_hashes.iter().filter(|hash| !hash.is_empty()).collect();
        let found = self
            .all_attempts()
            .into_iter()
            .filter(|attempt| {
                attempt.info_hash.as_ref().is_some_and(|hash| wanted.contains(hash)) && LIVE.contains(&attempt.state)
            })
            .map(|attempt| (attempt.info_hash.clone().unwrap_or_default(), attempt))
            .collect();
        ready(found)
    }

    fn archived_info_hashes<'a>(
        &'a self,
        info_hashes: &'a [String],
    ) -> BoxFuture<'a, Result<BTreeSet<String>, LedgerError>> {
        let wanted: BTreeSet<&String> = info_hashes.iter().filter(|hash| !hash.is_empty()).collect();
        let attempts: Vec<MagnetAttemptRecord> = self
            .all_attempts()
            .into_iter()
            .filter(|attempt| attempt.info_hash.as_ref().is_some_and(|hash| wanted.contains(hash)))
            .collect();
        let held: BTreeSet<String> = attempts
            .iter()
            .filter(|attempt| LIVE.contains(&attempt.state))
            .filter_map(|attempt| attempt.info_hash.clone())
            .collect();
        let archived = attempts
            .iter()
            .filter(|attempt| attempt.state == AttemptState::Archived)
            .filter_map(|attempt| attempt.info_hash.clone())
            .filter(|hash| !held.contains(hash))
            .collect();
        ready(archived)
    }

    fn in_flight_attempts(&self) -> BoxFuture<'_, Result<Vec<MagnetAttemptRecord>, LedgerError>> {
        let attempts = self
            .all_attempts()
            .into_iter()
            .filter(|attempt| IN_FLIGHT.contains(&attempt.state))
            .collect();
        ready(attempts)
    }

    fn attempts_for<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Vec<MagnetAttemptRecord>, LedgerError>> {
        ready(self.state().attempts.get(avid).cloned().unwrap_or_default())
    }

    fn list_acquisitions<'a>(
        &'a self,
        states: Option<&'a [AcquisitionState]>,
        limit: i64,
        offset: i64,
    ) -> BoxFuture<'a, Result<Vec<AcquisitionRecord>, LedgerError>> {
        let state = self.state();
        let mut avids: Vec<&String> = state
            .states
            .iter()
            .filter(|(_, known)| states.is_none_or(|wanted| wanted.contains(known)))
            .map(|(avid, _)| avid)
            .collect();
        avids.sort();
        // `[...][offset : offset + limit]`.
        let records = avids[py_slice(avids.len(), offset, offset.saturating_add(limit))]
            .iter()
            .filter_map(|avid| state.record(avid))
            .collect();
        ready(records)
    }

    fn count_by_state(&self) -> BoxFuture<'_, Result<Vec<(AcquisitionState, i64)>, LedgerError>> {
        let state = self.state();
        let mut counts: Vec<(AcquisitionState, i64)> = Vec::new();
        for known in state.states.values() {
            match counts.iter_mut().find(|(seen, _)| seen == known) {
                Some(entry) => entry.1 += 1,
                None => counts.push((*known, 1)),
            }
        }
        ready(counts)
    }
}

#[cfg(test)]
mod tests {
    use super::py_slice;

    /// `list(range(5))[start:stop]` for the bounds the fake's queries pass.
    #[test]
    fn slices_follow_python() {
        assert_eq!(py_slice(5, 0, 50), 0..5);
        assert_eq!(py_slice(5, 0, -1), 0..4);
        assert_eq!(py_slice(5, 0, -9), 0..0);
        assert_eq!(py_slice(5, 3, 1), 3..3);
        assert_eq!(py_slice(5, -2, 5), 3..5);
        assert_eq!(py_slice(5, 9, 12), 5..5);
        assert_eq!(py_slice(0, 0, 50), 0..0);
    }
}
