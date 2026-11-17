//! The in-memory subscription registry of the Python tests (`FakeSubscriptions` and
//! `make_subscription` in `tests/test_monitor_rss.py`), which the RSS, monitor API and
//! category routing tests run against; the real repository is exercised against PostgreSQL.

use std::sync::{Mutex, MutexGuard};

use chrono::{DateTime, Utc};

use super::now_stub;
use crate::rss::SubscriptionStore;
use crate::subscriptions::{
    SubscriptionError, SubscriptionExistsError, SubscriptionKind, SubscriptionRecord, trim_cursor,
};
use crate::support::BoxFuture;

/// `feed_url(label)`: the feed a test category's subscription reads.
pub fn feed_url(label: &str) -> String {
    format!("https://feeds.test/{label}")
}

/// `make_subscription(id, ...)`'s keyword arguments, defaulted as in Python.
#[derive(Debug, Clone)]
pub struct SubscriptionSpec {
    pub category: String,
    pub url: Option<String>,
    pub name: Option<String>,
    pub enabled: bool,
    pub cursor: Vec<String>,
    pub seed_pending: bool,
    pub talent_id: Option<i32>,
    pub aliases: Vec<String>,
}

impl Default for SubscriptionSpec {
    fn default() -> Self {
        Self {
            category: "Actor".to_owned(),
            url: None,
            name: None,
            enabled: true,
            cursor: Vec::new(),
            seed_pending: false,
            talent_id: None,
            aliases: Vec::new(),
        }
    }
}

/// `make_subscription(subscription_id, ...)`: a talent subscription when `talent_id` is
/// given, else a feed URL (the category's test feed unless `url` says otherwise).
pub fn make_subscription(subscription_id: i64, spec: SubscriptionSpec) -> SubscriptionRecord {
    let talent = spec.talent_id.is_some();
    SubscriptionRecord {
        id: subscription_id,
        kind: if talent {
            SubscriptionKind::AvbaseTalent
        } else {
            SubscriptionKind::Rss
        },
        url: if talent {
            None
        } else {
            Some(spec.url.unwrap_or_else(|| feed_url(&spec.category)))
        },
        category: spec.category,
        enabled: spec.enabled,
        talent_id: spec.talent_id,
        name: spec.name,
        aliases: spec.aliases,
        cursor: spec.cursor,
        seed_pending: spec.seed_pending,
        last_polled_at: None,
        last_error: None,
        created_at: now_stub(),
        updated_at: now_stub(),
    }
}

/// One `record_poll` call: the subscription, the cursor handed over, the error.
pub type Poll = (i64, Option<Vec<String>>, Option<String>);

/// The fake's dictionaries, insertion-ordered like Python's.
#[derive(Debug, Default)]
pub struct FakeSubscriptionsState {
    pub records: indexmap::IndexMap<i64, SubscriptionRecord>,
    pub polls: Vec<Poll>,
}

/// `FakeSubscriptions`.
#[derive(Debug, Default)]
pub struct FakeSubscriptions {
    state: Mutex<FakeSubscriptionsState>,
}

impl FakeSubscriptions {
    pub fn new(records: Vec<SubscriptionRecord>) -> Self {
        Self {
            state: Mutex::new(FakeSubscriptionsState {
                records: records.into_iter().map(|record| (record.id, record)).collect(),
                polls: Vec::new(),
            }),
        }
    }

    pub fn state(&self) -> MutexGuard<'_, FakeSubscriptionsState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `records[subscription_id]`.
    pub fn record(&self, subscription_id: i64) -> SubscriptionRecord {
        self.state().records[&subscription_id].clone()
    }

    fn next_id(&self) -> i64 {
        self.state().records.keys().max().copied().unwrap_or(0) + 1
    }

    /// `list()`: sorted by category, kind, display name, id.
    pub fn list_now(&self) -> Vec<SubscriptionRecord> {
        let mut records: Vec<SubscriptionRecord> = self.state().records.values().cloned().collect();
        records.sort_by(|a, b| {
            (&a.category, a.kind.as_str(), a.display_name(), a.id).cmp(&(
                &b.category,
                b.kind.as_str(),
                b.display_name(),
                b.id,
            ))
        });
        records
    }

    pub fn get(&self, subscription_id: i64) -> Option<SubscriptionRecord> {
        self.state().records.get(&subscription_id).cloned()
    }

    pub fn add_rss(
        &self,
        url: &str,
        category: &str,
        now: DateTime<Utc>,
        name: Option<&str>,
        seed_pending: bool,
    ) -> Result<SubscriptionRecord, SubscriptionError> {
        if self
            .state()
            .records
            .values()
            .any(|record| record.url.as_deref() == Some(url))
        {
            return Err(SubscriptionExistsError { what: url.to_owned() }.into());
        }
        let mut record = make_subscription(
            self.next_id(),
            SubscriptionSpec {
                category: category.to_owned(),
                url: Some(url.to_owned()),
                name: name.map(str::to_owned),
                seed_pending,
                ..SubscriptionSpec::default()
            },
        );
        record.created_at = now;
        record.updated_at = now;
        self.state().records.insert(record.id, record.clone());
        Ok(record)
    }

    pub fn add_talent(
        &self,
        talent_id: i32,
        name: &str,
        aliases: &[String],
        category: &str,
        now: DateTime<Utc>,
        seed_pending: bool,
    ) -> Result<SubscriptionRecord, SubscriptionError> {
        if self
            .state()
            .records
            .values()
            .any(|record| record.talent_id == Some(talent_id))
        {
            return Err(SubscriptionExistsError {
                what: format!("talent {talent_id}"),
            }
            .into());
        }
        let mut record = make_subscription(
            self.next_id(),
            SubscriptionSpec {
                category: category.to_owned(),
                name: Some(name.to_owned()),
                talent_id: Some(talent_id),
                aliases: aliases.to_vec(),
                seed_pending,
                ..SubscriptionSpec::default()
            },
        );
        record.created_at = now;
        record.updated_at = now;
        self.state().records.insert(record.id, record.clone());
        Ok(record)
    }

    pub fn update(
        &self,
        subscription_id: i64,
        now: DateTime<Utc>,
        enabled: Option<bool>,
        category: Option<&str>,
        url: Option<&str>,
    ) -> Result<Option<SubscriptionRecord>, SubscriptionError> {
        let mut state = self.state();
        if !state.records.contains_key(&subscription_id) {
            return Ok(None);
        }
        if let Some(url) = url
            && state
                .records
                .values()
                .any(|other| other.id != subscription_id && other.url.as_deref() == Some(url))
        {
            return Err(SubscriptionExistsError { what: url.to_owned() }.into());
        }
        let record = state.records.get_mut(&subscription_id).expect("checked above");
        record.updated_at = now;
        if let Some(enabled) = enabled {
            record.enabled = enabled;
        }
        if let Some(category) = category {
            category.clone_into(&mut record.category);
        }
        if let Some(url) = url {
            record.url = Some(url.to_owned());
        }
        Ok(Some(record.clone()))
    }

    pub fn relink(
        &self,
        subscription_id: i64,
        talent_id: i128,
        aliases: &[String],
        now: DateTime<Utc>,
    ) -> Result<Option<SubscriptionRecord>, SubscriptionError> {
        let mut state = self.state();
        match state.records.get(&subscription_id) {
            Some(record) if record.kind == SubscriptionKind::AvbaseTalent => {}
            _ => return Ok(None),
        }
        if state
            .records
            .values()
            .any(|other| other.id != subscription_id && other.talent_id.map(i128::from) == Some(talent_id))
        {
            return Err(SubscriptionExistsError {
                what: format!("talent {talent_id}"),
            }
            .into());
        }
        let record = state.records.get_mut(&subscription_id).expect("checked above");
        record.talent_id = Some(i32::try_from(talent_id).expect("a test talent id fits the column"));
        record.aliases = aliases.to_vec();
        record.updated_at = now;
        Ok(Some(record.clone()))
    }

    pub fn delete(&self, subscription_id: i64) -> bool {
        self.state().records.shift_remove(&subscription_id).is_some()
    }

    pub fn poll(&self, subscription_id: i64, now: DateTime<Utc>, cursor: Option<&[String]>, error: Option<&str>) {
        let mut state = self.state();
        state.polls.push((
            subscription_id,
            cursor.map(<[String]>::to_vec),
            error.map(str::to_owned),
        ));
        let record = state.records.get_mut(&subscription_id).expect("a known subscription");
        record.last_polled_at = Some(now);
        record.last_error = error.map(str::to_owned);
        record.updated_at = now;
        if let Some(cursor) = cursor {
            record.cursor = trim_cursor(cursor);
            record.seed_pending = false;
        }
    }
}

impl SubscriptionStore for FakeSubscriptions {
    fn list(&self) -> BoxFuture<'_, Result<Vec<SubscriptionRecord>, SubscriptionError>> {
        let records = self.list_now();
        Box::pin(async move { Ok(records) })
    }

    fn relink_talent<'a>(
        &'a self,
        subscription_id: i64,
        talent_id: i128,
        aliases: &'a [String],
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<SubscriptionRecord>, SubscriptionError>> {
        let result = self.relink(subscription_id, talent_id, aliases, now);
        Box::pin(async move { result })
    }

    fn record_poll<'a>(
        &'a self,
        subscription_id: i64,
        now: DateTime<Utc>,
        cursor: Option<&'a [String]>,
        error: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), SubscriptionError>> {
        self.poll(subscription_id, now, cursor, error);
        Box::pin(async { Ok(()) })
    }
}
