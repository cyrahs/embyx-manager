//! Subscription polling: feed items -> AVIDs -> magnet candidates -> offline tasks.
//!
//! Python origin: `src/embyx_manager/monitor/rss.py`. This pipeline only discovers. It polls
//! every enabled subscription, records each AVID in the acquisition ledger, and hands
//! resolution and submission to the shared intake; what happens to that download afterwards
//! belongs to the tracker.
//!
//! Each subscription remembers a bounded cursor of the item keys it has seen, only so an item
//! is not re-read on every poll; retries are driven by the ledger's schedule. Magnets are
//! resolved as a ranked list, so a magnet that fails has a successor waiting.
//!
//! One run covers every configured category in turn, and every enabled subscription in it; one
//! subscription failing does not cost the others their pass. The order categories are
//! configured in is their precedence: an AVID that more than one category sights belongs to
//! the earliest of them, and before a lower category files a new row the catalog is asked who
//! is credited on the work, so a talent subscribed in a category ahead claims it from the start.
//! AVBase now and then re-creates a talent under a new id; a talent subscription whose feed
//! answers 404 follows the talent's name to the new id.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDate, Utc};
use embyx_clients::avbase::AvbaseTalent;
use embyx_clients::feeds::{FeedFetchError, HttpFeedFetcher};
use embyx_clients::rss_magnet::{FeedMagnetError, get_magnet_from_html};
use embyx_core::avid::AvidParser;
use embyx_core::config::models::{RssCategory, RssConfig};
use embyx_core::pyurl::UrlError;
use indexmap::IndexMap;

use crate::acquisitions::{AcquisitionState, LedgerError, MagnetCandidate, Sighting, TransitionFields, rss_source};
use crate::archive::{ArchivePipeline, blocking};
use crate::feeds::{FeedItem, FeedParseError, avid_candidate_from_link, parse_feed};
use crate::intake::{AcquisitionIntake, IntakeError, IntakeParts, JavBusMagnets, OfflineFiles, SukebeiMagnets};
use crate::reports::{PyException, Raised, RunCancelledError, RunContext};
use crate::subscriptions::{
    SubscriptionError, SubscriptionKind, SubscriptionRecord, SubscriptionRepository, trim_cursor,
};
use crate::support::{BoxFuture, Clock, OverflowError, out_of_range, system_clock};

/// `RECHECKABLE_STATES`: states discovery hands back for a fresh look; a library hit settles
/// them.
pub const RECHECKABLE_STATES: &[AcquisitionState] = &[
    AcquisitionState::Discovered,
    AcquisitionState::ResolveFailed,
    AcquisitionState::Exhausted,
];

const HTTP_NOT_FOUND: u16 = 404;
/// `_ERROR_LIMIT`: characters of an error kept on the subscription.
const ERROR_LIMIT: usize = 500;

/// What a feed fetch fails with: the fetcher's own errors, or any other exception (the
/// Python tests' fetchers raise `RuntimeError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    Feed(FeedFetchError),
    Raised(Raised),
}

impl FetchError {
    /// The status of an `httpx.HTTPStatusError`.
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Feed(error) => error.status(),
            Self::Raised(_) => None,
        }
    }
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Feed(error) => error.fmt(f),
            Self::Raised(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for FetchError {}

impl PyException for FetchError {
    fn class_name(&self) -> &str {
        match self {
            Self::Feed(error) => error.class_name(),
            Self::Raised(error) => error.class_name(),
        }
    }
}

impl From<FeedFetchError> for FetchError {
    fn from(error: FeedFetchError) -> Self {
        Self::Feed(error)
    }
}

/// `fetch(url)`: a feed body.
pub trait FeedFetcher: Send + Sync {
    fn fetch<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Vec<u8>, FetchError>>;
}

impl FeedFetcher for HttpFeedFetcher {
    fn fetch<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Vec<u8>, FetchError>> {
        Box::pin(async move { Self::fetch(self, url).await.map_err(FetchError::from) })
    }
}

/// What the catalog knows about a work that bears on filing it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreditedWork {
    /// The talents credited on the work, by catalog id.
    pub talent_ids: BTreeSet<i128>,
    pub release_date: Option<NaiveDate>,
}

/// Any exception a catalog lookup raises; the pipeline only logs it.
pub type LookupError = Box<dyn PyException + Send + Sync>;

/// `cast_lookup(avid)`: the catalog's credits for the work, `None` when it is not listed.
pub trait CastLookup: Send + Sync {
    fn lookup<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<CreditedWork>, LookupError>>;
}

/// `talent_lookup(name)`: the talent the catalog lists under that name (or alias).
pub trait TalentLookup: Send + Sync {
    fn lookup<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Option<AvbaseTalent>, LookupError>>;
}

/// The subscription registry calls the poller makes (`SubscriptionRepository`).
pub trait SubscriptionStore: Send + Sync {
    fn list(&self) -> BoxFuture<'_, Result<Vec<SubscriptionRecord>, SubscriptionError>>;
    fn relink_talent<'a>(
        &'a self,
        subscription_id: i64,
        talent_id: i128,
        aliases: &'a [String],
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<SubscriptionRecord>, SubscriptionError>>;
    fn record_poll<'a>(
        &'a self,
        subscription_id: i64,
        now: DateTime<Utc>,
        cursor: Option<&'a [String]>,
        error: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), SubscriptionError>>;
}

impl SubscriptionStore for SubscriptionRepository {
    fn list(&self) -> BoxFuture<'_, Result<Vec<SubscriptionRecord>, SubscriptionError>> {
        Box::pin(Self::list(self))
    }

    fn relink_talent<'a>(
        &'a self,
        subscription_id: i64,
        talent_id: i128,
        aliases: &'a [String],
        now: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Option<SubscriptionRecord>, SubscriptionError>> {
        Box::pin(async move {
            // asyncpg refuses an id past the INTEGER column as the repository does.
            let talent_id =
                i64::try_from(talent_id).map_err(|_| SubscriptionError::Store(out_of_range(2, talent_id, 32)))?;
            Self::relink_talent(self, subscription_id, talent_id, aliases, now).await
        })
    }

    fn record_poll<'a>(
        &'a self,
        subscription_id: i64,
        now: DateTime<Utc>,
        cursor: Option<&'a [String]>,
        error: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), SubscriptionError>> {
        Box::pin(Self::record_poll(self, subscription_id, now, cursor, error))
    }
}

/// Why one subscription's poll (or the run) stopped.
#[derive(Debug, thiserror::Error)]
pub enum RssError {
    #[error(transparent)]
    Cancelled(#[from] RunCancelledError),
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error(transparent)]
    Parse(#[from] FeedParseError),
    /// `urlsplit`'s `ValueError` for an item link that is not a URL.
    #[error(transparent)]
    Url(#[from] UrlError),
    #[error(transparent)]
    Magnet(#[from] FeedMagnetError),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    Subscription(#[from] SubscriptionError),
    #[error(transparent)]
    Intake(#[from] IntakeError),
    /// `TalentMovedError`: the talent's feed is gone and the id AVBase lists it under now
    /// cannot be taken over.
    #[error("{0}")]
    TalentMoved(String),
}

impl PyException for RssError {
    fn class_name(&self) -> &str {
        match self {
            Self::Cancelled(error) => error.class_name(),
            Self::Fetch(error) => error.class_name(),
            Self::Parse(_) => "FeedParseError",
            Self::Url(_) => "ValueError",
            Self::Magnet(error) => error.class_name(),
            Self::Ledger(error) => error.class_name(),
            Self::Subscription(error) => error.class_name(),
            Self::Intake(error) => error.class_name(),
            Self::TalentMoved(_) => "TalentMovedError",
        }
    }
}

/// `_describe(exc)`: `Class: message` (the class alone for an empty message), at most
/// [`ERROR_LIMIT`] characters.
fn describe(error: &dyn PyException) -> String {
    let message = error.to_string();
    let text = if message.is_empty() {
        error.class_name().to_owned()
    } else {
        format!("{}: {message}", error.class_name())
    };
    text.chars().take(ERROR_LIMIT).collect()
}

/// `_Filing`: the category a sighting files under: normally its own, else one ahead that
/// credits a subscribed talent.
#[derive(Debug, Clone)]
struct Filing {
    source: String,
    task_dir: String,
    yields_to: Vec<String>,
}

/// The constructor's arguments (`RssPipeline(config=, avid_parser=, subscriptions=, ...)`).
pub struct RssParts {
    pub config: RssConfig,
    pub avid_parser: AvidParser,
    pub subscriptions: Arc<dyn SubscriptionStore>,
    pub fetch: Arc<dyn FeedFetcher>,
    pub cloud: Arc<dyn OfflineFiles>,
    pub sukebei: Arc<dyn SukebeiMagnets>,
    pub javbus: Arc<dyn JavBusMagnets>,
    pub ledger: Arc<dyn crate::acquisitions::Ledger>,
    pub archiver: ArchivePipeline,
    pub on_submitted: Option<crate::intake::OnSubmitted>,
    pub cast_lookup: Option<Arc<dyn CastLookup>>,
    pub talent_lookup: Option<Arc<dyn TalentLookup>>,
}

impl fmt::Debug for RssParts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RssParts")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The per-run state Python keeps on the pipeline object.
#[derive(Debug, Default)]
struct RunState {
    /// Category label per subscribed talent id, from the enabled talent subscriptions of this
    /// run.
    talent_categories: HashMap<i128, String>,
    /// Catalog answers: the same work is often on more than one chart. Kept across runs, as
    /// Python's dict on the pipeline is.
    credits: HashMap<String, Option<CreditedWork>>,
}

/// `RssPipeline`.
pub struct RssPipeline {
    config: RssConfig,
    avid: AvidParser,
    subscriptions: Arc<dyn SubscriptionStore>,
    fetch: Arc<dyn FeedFetcher>,
    ledger: Arc<dyn crate::acquisitions::Ledger>,
    archiver: ArchivePipeline,
    cast_lookup: Option<Arc<dyn CastLookup>>,
    talent_lookup: Option<Arc<dyn TalentLookup>>,
    intake: AcquisitionIntake,
    state: Mutex<RunState>,
    clock: Clock,
}

impl fmt::Debug for RssPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RssPipeline")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl RssPipeline {
    /// `RssPipeline(...)`: `OverflowError` for a cooldown beyond `timedelta`'s range, as the
    /// intake's constructor raises.
    pub fn new(parts: RssParts) -> Result<Self, OverflowError> {
        let intake = AcquisitionIntake::new(IntakeParts {
            ledger: Arc::clone(&parts.ledger),
            sukebei: parts.sukebei,
            javbus: parts.javbus,
            cloud: parts.cloud,
            failed_cooldown_seconds: parts.config.failed_avid_cooldown_seconds,
            on_submitted: parts.on_submitted,
            release_date_lookup: None,
        })?;
        Ok(Self {
            config: parts.config,
            avid: parts.avid_parser,
            subscriptions: parts.subscriptions,
            fetch: parts.fetch,
            ledger: parts.ledger,
            archiver: parts.archiver,
            cast_lookup: parts.cast_lookup,
            talent_lookup: parts.talent_lookup,
            intake,
            state: Mutex::default(),
            clock: system_clock(),
        })
    }

    /// The clock `datetime.now(UTC)` stands for, here and in the intake.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.intake = self.intake.with_clock(Arc::clone(&clock));
        self.clock = clock;
        self
    }

    /// The sleeper the intake's `CLOUD_RETRY` waits go through.
    #[must_use]
    pub fn with_sleeper(mut self, sleeper: embyx_clients::net::retry::Sleeper) -> Self {
        self.intake = self.intake.with_sleeper(sleeper);
        self
    }

    fn now(&self) -> DateTime<Utc> {
        (self.clock)()
    }

    fn run_state(&self) -> std::sync::MutexGuard<'_, RunState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Poll every enabled subscription, category by category.
    pub async fn run(&self, ctx: &RunContext) -> Result<(), RssError> {
        let mut by_category: IndexMap<String, Vec<SubscriptionRecord>> = IndexMap::new();
        let mut talent_categories = HashMap::new();
        for subscription in self.subscriptions.list().await? {
            if subscription.enabled {
                if let Some(talent_id) = subscription.talent_id {
                    talent_categories
                        .entry(i128::from(talent_id))
                        .or_insert_with(|| subscription.category.clone());
                }
                by_category
                    .entry(subscription.category.clone())
                    .or_default()
                    .push(subscription);
            }
        }
        self.run_state().talent_categories = talent_categories;
        for category in &self.config.categories {
            for subscription in by_category.shift_remove(&category.label).unwrap_or_default() {
                ctx.check_cancelled()?;
                self.poll(ctx, category, subscription).await?;
            }
        }
        // A subscription whose category was removed from the configuration has no offline
        // directory to file into; it is reported, not guessed at.
        for (label, orphans) in by_category {
            for subscription in orphans {
                ctx.warning(&format!(
                    "{} belongs to the unconfigured category {label}",
                    subscription.display_name()
                ));
                ctx.add("subscriptions_failed", 1);
                let error = format!("category {label} is not configured");
                self.subscriptions
                    .record_poll(subscription.id, self.now(), None, Some(&error))
                    .await?;
            }
        }
        Ok(())
    }

    async fn poll(
        &self,
        ctx: &RunContext,
        category: &RssCategory,
        subscription: SubscriptionRecord,
    ) -> Result<(), RssError> {
        let (id, name) = (subscription.id, subscription.display_name());
        match self.poll_subscription(ctx, category, subscription).await {
            Ok(()) => Ok(()),
            Err(RssError::Cancelled(error)) => Err(error.into()),
            Err(error) => {
                ctx.exception(&format!("Failed to poll {name}"), Some(&error));
                ctx.add("subscriptions_failed", 1);
                let text = describe(&error);
                self.subscriptions
                    .record_poll(id, self.now(), None, Some(&text))
                    .await?;
                Ok(())
            }
        }
    }

    async fn poll_subscription(
        &self,
        ctx: &RunContext,
        category: &RssCategory,
        subscription: SubscriptionRecord,
    ) -> Result<(), RssError> {
        let (subscription, body) = self.fetch_feed(ctx, subscription).await?;
        let feed = parse_feed(&body)?;
        let count = |items: usize| i64::try_from(items).unwrap_or(i64::MAX);
        ctx.add("subscriptions_polled", 1);
        if subscription.seed_pending {
            // The backlog was covered elsewhere: remember what the feed holds today and
            // ingest nothing.
            ctx.info(&format!(
                "Seeded {} with {} items",
                subscription.display_name(),
                feed.items.len()
            ));
            ctx.add("subscriptions_seeded", 1);
            let keys: Vec<String> = feed.items.iter().map(|item| item.key.clone()).collect();
            self.subscriptions
                .record_poll(subscription.id, self.now(), Some(&keys), None)
                .await?;
            return Ok(());
        }
        ctx.add("items", count(feed.items.len()));
        let seen: HashSet<&str> = subscription.cursor.iter().map(String::as_str).collect();
        let fresh: Vec<FeedItem> = feed
            .items
            .iter()
            .filter(|item| !seen.contains(item.key.as_str()))
            .cloned()
            .collect();
        ctx.add("new_items", count(fresh.len()));
        ctx.info(&format!(
            "Find {} items ({} new) in {}",
            feed.items.len(),
            fresh.len(),
            subscription.display_name()
        ));
        if !fresh.is_empty() {
            self.ingest(ctx, category, &fresh).await?;
        }
        let mut keys = subscription.cursor.clone();
        keys.extend(fresh.iter().map(|item| item.key.clone()));
        self.subscriptions
            .record_poll(subscription.id, self.now(), Some(&trim_cursor(&keys)), None)
            .await?;
        Ok(())
    }

    /// The feed body, and the subscription it was read for (relinked if the talent moved).
    async fn fetch_feed(
        &self,
        ctx: &RunContext,
        subscription: SubscriptionRecord,
    ) -> Result<(SubscriptionRecord, Vec<u8>), RssError> {
        let error = match self.fetch.fetch(&subscription.feed_url()).await {
            Ok(body) => return Ok((subscription, body)),
            Err(error) => error,
        };
        if error.status() != Some(HTTP_NOT_FOUND) {
            return Err(error.into());
        }
        let Some(relinked) = self.relink_talent(ctx, &subscription).await? else {
            return Err(error.into());
        };
        let body = self.fetch.fetch(&relinked.feed_url()).await?;
        Ok((relinked, body))
    }

    /// Follow a talent whose feed is gone to the id AVBase lists the name under now; `None`
    /// to keep the 404.
    async fn relink_talent(
        &self,
        ctx: &RunContext,
        subscription: &SubscriptionRecord,
    ) -> Result<Option<SubscriptionRecord>, RssError> {
        let Some(talent) = self.moved_talent(ctx, subscription).await else {
            return Ok(None);
        };
        let old_id = subscription.talent_id.map(i128::from);
        let name = subscription.name.clone().unwrap_or_default();
        // A freshly re-created talent can come back without its aliases; the ones already
        // known are better than none.
        let aliases = if talent.aliases.is_empty() {
            &subscription.aliases
        } else {
            &talent.aliases
        };
        let relinked = match self
            .subscriptions
            .relink_talent(subscription.id, talent.talent_id, aliases, self.now())
            .await
        {
            Ok(relinked) => relinked,
            Err(SubscriptionError::Exists(_)) => {
                return Err(RssError::TalentMoved(format!(
                    "AVBase now lists {name} as talent {}, which another subscription already follows",
                    talent.talent_id
                )));
            }
            Err(error) => return Err(error.into()),
        };
        let Some(relinked) = relinked else {
            return Ok(None);
        };
        let old_text = old_id.map_or_else(|| "None".to_owned(), |id| id.to_string());
        ctx.info(&format!(
            "AVBase moved {name} from talent {old_text} to {}; the subscription follows",
            talent.talent_id
        ));
        ctx.add("subscriptions_relinked", 1);
        let mut state = self.run_state();
        if let Some(category) = old_id.and_then(|id| state.talent_categories.remove(&id)) {
            state.talent_categories.entry(talent.talent_id).or_insert(category);
        }
        Ok(Some(relinked))
    }

    /// The talent AVBase lists the subscription's name under, if that is a different id of
    /// the same talent.
    async fn moved_talent(&self, ctx: &RunContext, subscription: &SubscriptionRecord) -> Option<AvbaseTalent> {
        let name = subscription.name.as_deref().filter(|name| !name.is_empty())?;
        if subscription.kind != SubscriptionKind::AvbaseTalent {
            return None;
        }
        let lookup = self.talent_lookup.as_ref()?;
        let Ok(talent) = lookup.lookup(name).await else {
            // The catalog being down leaves the 404 to report; the next run asks again.
            ctx.warning(&format!(
                "AVBase could not be asked where {} moved",
                subscription.display_name()
            ));
            return None;
        };
        let talent = talent?;
        if Some(talent.talent_id) == subscription.talent_id.map(i128::from) {
            return None;
        }
        let wanted = caseless::default_case_fold_str(name);
        if !talent
            .names()
            .iter()
            .any(|listed| caseless::default_case_fold_str(listed) == wanted)
        {
            // A name search that lands on someone not credited under this name is not the
            // same talent; leave the subscription for a person to look at.
            ctx.warning(&format!(
                "AVBase answered {name} with {} (talent {}); not relinking",
                talent.name, talent.talent_id
            ));
            return None;
        }
        Some(talent)
    }

    async fn ingest(&self, ctx: &RunContext, category: &RssCategory, items: &[FeedItem]) -> Result<(), RssError> {
        let source = rss_source(&category.label);
        // Pinned onto each acquisition so its retries keep landing here even if the category
        // is later repointed or removed.
        let task_dir = category.task_dir_path.clone();
        let yields_to = self.outranking_sources(category);

        let mut avid_item: IndexMap<String, Vec<&FeedItem>> = IndexMap::new();
        for item in items {
            let avid = self.avid_of(item)?;
            if avid.is_empty() {
                let shown = [item.title.as_str(), item.link.as_deref().unwrap_or_default()]
                    .into_iter()
                    .find(|text| !text.is_empty())
                    .unwrap_or(&item.key);
                ctx.warning(&format!("Failed to get avid for {shown}"));
                continue;
            }
            avid_item.entry(avid).or_default().push(item);
        }
        ctx.add("unique_avids", i64::try_from(avid_item.len()).unwrap_or(i64::MAX));

        let now = self.now();
        let mut wanted: IndexMap<String, Vec<&FeedItem>> = IndexMap::new();
        let mut item_magnets: HashMap<String, Option<String>> = HashMap::new();
        let mut dirs: HashMap<String, String> = HashMap::new();
        for (avid, avid_items) in &avid_item {
            // An item carrying a magnet is evidence the wait is over: it wakes a row still
            // cooling down from an earlier empty pass.
            let item_magnet = get_magnet_from_html(&avid_items[0].content, avid)?;
            let own = Filing {
                source: source.clone(),
                task_dir: task_dir.clone(),
                yields_to: yields_to.clone(),
            };
            let (filing, cast) = self.filing_for(avid, category, own, ctx).await?;
            let accepted = self
                .ledger
                .discover(
                    avid,
                    &filing.source,
                    now,
                    Sighting {
                        task_dir_path: Some(filing.task_dir.clone()),
                        next_action_at: None,
                        release_date: cast.and_then(|cast| cast.release_date),
                        wake: item_magnet.is_some(),
                        yields_to: filing.yields_to.clone(),
                    },
                )
                .await?;
            if !accepted {
                ctx.add("skipped_known", 1);
                continue;
            }
            wanted.insert(avid.clone(), avid_items.clone());
            item_magnets.insert(avid.clone(), item_magnet);
            let dir = self.filing_dir(avid, &filing.task_dir, &filing.yields_to, ctx).await?;
            dirs.insert(avid.clone(), dir);
        }
        if wanted.len() != avid_item.len() {
            ctx.info(&format!(
                "Skipping {} avids already tracked",
                avid_item.len() - wanted.len()
            ));
        }
        if wanted.is_empty() {
            return Ok(());
        }

        ctx.check_cancelled()?;
        let wanted = self.skip_library_held(wanted, &dirs, ctx).await?;
        if wanted.is_empty() {
            return Ok(());
        }

        ctx.check_cancelled()?;
        let resolved = self.resolve_all(&wanted, &item_magnets, ctx).await?;
        ctx.check_cancelled()?;
        for (avid, candidates) in &resolved {
            ctx.check_cancelled()?;
            self.intake
                .record_and_submit(avid, candidates, &dirs[avid], ctx)
                .await?;
        }
        Ok(())
    }

    /// The ledger sources of the categories configured ahead of this one.
    fn outranking_sources(&self, category: &RssCategory) -> Vec<String> {
        self.config
            .categories
            .iter()
            .take_while(|configured| configured.label != category.label)
            .map(|configured| rss_source(&configured.label))
            .collect()
    }

    /// Where this sighting files, and what the catalog said about the work if it was asked.
    ///
    /// A category with none ahead of it, or a row a category ahead already owns or that is
    /// past discovery, files as the sighting itself. Otherwise the catalog's credits decide: a
    /// talent subscribed in a category ahead claims the work for that category, the earliest
    /// configured one when several qualify.
    async fn filing_for(
        &self,
        avid: &str,
        category: &RssCategory,
        own: Filing,
        ctx: &RunContext,
    ) -> Result<(Filing, Option<CreditedWork>), RssError> {
        if own.yields_to.is_empty() || self.cast_lookup.is_none() {
            return Ok((own, None));
        }
        if let Some(record) = self.ledger.get(avid).await?
            && (own.yields_to.contains(&record.source) || !RECHECKABLE_STATES.contains(&record.state))
        {
            return Ok((own, None));
        }
        let Some(cast) = self.cast_of(avid, ctx).await else {
            return Ok((own, None));
        };
        let subscribed: HashSet<String> = {
            let state = self.run_state();
            cast.talent_ids
                .iter()
                .filter_map(|talent| state.talent_categories.get(talent).cloned())
                .collect()
        };
        for owner in self.config.categories.iter().take(own.yields_to.len()) {
            if !subscribed.contains(&owner.label) {
                continue;
            }
            ctx.add("filed_by_cast", 1);
            ctx.info(&format!(
                "{avid} credits a talent subscribed in {}; filing it there instead of {}",
                owner.label, category.label
            ));
            let owner_source = rss_source(&owner.label);
            let position = own
                .yields_to
                .iter()
                .position(|source| *source == owner_source)
                .expect("a category ahead is among the sources yielded to");
            let owner_filing = Filing {
                source: owner_source,
                task_dir: owner.task_dir_path.clone(),
                yields_to: own.yields_to[..position].to_vec(),
            };
            return Ok((owner_filing, Some(cast)));
        }
        Ok((own, Some(cast)))
    }

    async fn cast_of(&self, avid: &str, ctx: &RunContext) -> Option<CreditedWork> {
        if let Some(known) = self.run_state().credits.get(avid) {
            return known.clone();
        }
        let lookup = self.cast_lookup.as_ref()?;
        let cast = match lookup.lookup(avid).await {
            Ok(cast) => cast,
            Err(error) => {
                // An unreadable catalog files the sighting as itself.
                ctx.exception(&format!("Failed to look up the cast of {avid}"), Some(&*error));
                None
            }
        };
        self.run_state().credits.insert(avid.to_owned(), cast.clone());
        cast
    }

    /// The directory this sighting's download goes to: the row's, when a category ahead owns
    /// it.
    async fn filing_dir(
        &self,
        avid: &str,
        task_dir: &str,
        yields_to: &[String],
        ctx: &RunContext,
    ) -> Result<String, RssError> {
        if yields_to.is_empty() {
            return Ok(task_dir.to_owned());
        }
        let Some(record) = self.ledger.get(avid).await? else {
            return Ok(task_dir.to_owned());
        };
        match record.task_dir_path {
            Some(path) if !path.is_empty() && path != task_dir => {
                ctx.add("filed_by_owner", 1);
                ctx.info(&format!("{avid} belongs to {}; filing into {path}", record.source));
                Ok(path)
            }
            _ => Ok(task_dir.to_owned()),
        }
    }

    /// The AVID an item is about: from its title, else from its link's last path segment
    /// (AVBase titles are the work's title alone).
    fn avid_of(&self, item: &FeedItem) -> Result<String, UrlError> {
        if !item.title.is_empty() {
            let avid = self.avid.get_avid(&item.title);
            if !avid.is_empty() {
                return Ok(avid);
            }
        }
        let candidate = avid_candidate_from_link(item.link.as_deref())?;
        Ok(if candidate.is_empty() {
            String::new()
        } else {
            self.avid.get_avid(&candidate)
        })
    }

    // -- the library check ----------------------------------------------------

    /// Drop AVIDs the library already holds, settling their ledger rows as archived. An
    /// unreadable library is not a held one: the download proceeds and the archive-time
    /// check gets the final word.
    async fn skip_library_held<'i>(
        &self,
        wanted: IndexMap<String, Vec<&'i FeedItem>>,
        dirs: &HashMap<String, String>,
        ctx: &RunContext,
    ) -> Result<IndexMap<String, Vec<&'i FeedItem>>, RssError> {
        let mut kept = IndexMap::new();
        for (avid, items) in wanted {
            ctx.check_cancelled()?;
            let held = match blocking(|| self.archiver.library_holdings(&avid, ctx, Some(&dirs[&avid]))) {
                Ok(held) => held,
                Err(error) => {
                    ctx.exception(&format!("Failed to check the library for {avid}"), Some(&error));
                    Vec::new()
                }
            };
            if held.is_empty() {
                kept.insert(avid, items);
                continue;
            }
            ctx.add("already_in_library", 1);
            ctx.info(&format!("{avid} is already in the library at {}", held[0]));
            if let Some(record) = self.ledger.get(&avid).await?
                && RECHECKABLE_STATES.contains(&record.state)
            {
                self.ledger
                    .transition(
                        &avid,
                        record.state,
                        AcquisitionState::Archived,
                        self.now(),
                        TransitionFields {
                            note: Some("already in library".to_owned()),
                            next_action_at: None,
                            archived_paths: Some(held),
                        },
                    )
                    .await?;
            }
        }
        Ok(kept)
    }

    // -- magnet resolution ---------------------------------------------------

    /// Every AVID's candidates, resolved concurrently (`asyncio.gather`); the map is in the
    /// order the resolves finished, as Python's dict is filled.
    async fn resolve_all(
        &self,
        avid_item: &IndexMap<String, Vec<&FeedItem>>,
        item_magnets: &HashMap<String, Option<String>>,
        ctx: &RunContext,
    ) -> Result<IndexMap<String, Vec<MagnetCandidate>>, RssError> {
        let resolved: Mutex<IndexMap<String, Vec<MagnetCandidate>>> = Mutex::new(IndexMap::new());
        let lookups = avid_item.keys().map(|avid| {
            let item_magnet = item_magnets.get(avid).cloned().flatten();
            let resolved = &resolved;
            async move {
                match self.intake.resolve(avid, ctx, item_magnet.as_deref()).await {
                    Err(error) => ctx.exception(&format!("Failed to get magnets for {avid}"), Some(&error)),
                    Ok(candidates) if !candidates.is_empty() => {
                        resolved
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .insert(avid.clone(), candidates);
                    }
                    Ok(_) => {}
                }
            }
        });
        futures_util::future::join_all(lookups).await;
        let resolved = resolved.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
        let found: usize = resolved.values().map(Vec::len).sum();
        ctx.add("magnets_found", i64::try_from(found).unwrap_or(i64::MAX));
        ctx.info(&format!(
            "Found magnets for {} of {} avids",
            resolved.len(),
            avid_item.len()
        ));
        for avid in avid_item.keys() {
            if resolved.contains_key(avid) {
                continue;
            }
            ctx.add("magnets_failed", 1);
            ctx.warning(&format!("Failed to get any magnet for {avid}"));
            self.intake.park_unresolved(avid).await?;
        }
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests;
