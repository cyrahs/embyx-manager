//! The monitor's part of the assembly root: the pipelines' runners and readiness checks,
//! the scheduler, the shared magnet submit, the manual and replacement sources, and the
//! monitor and playlists routers.
//!
//! Python origin: `build_app` in `bootstrap.py` (`rss_runner`, `archive_runner`,
//! `tracker_poll`, `submit_magnet`, `mapping_factory`, `playlists_runner`, the `*_ready`
//! checks, `_offline_task_dirs`, `_polled_task_dirs`, `AvidParserHandle`). Each run that
//! talks to CloudDrive does so through one scoped handle (`AsyncCloudDrive::scoped`), and a
//! run the scheduler stops for shutdown awaits that handle's `settle()` before it ends, so
//! a mutation it sent is never cut off (cross-version contract 6).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use embyx_clients::avbase::{AvbaseClient, AvbaseOptions};
use embyx_clients::clouddrive::{AsyncCloudDrive, CloudDriveError};
use embyx_clients::emby::EmbyClient;
use embyx_clients::feeds::HttpFeedFetcher;
use embyx_clients::javbus::{JavBusClient, JavBusOptions};
use embyx_clients::jinjier::JinjierClient;
use embyx_clients::sukebei::{SukebeiClient, SukebeiOptions};
use embyx_core::avid::{AvidConfig, AvidParser};
use embyx_core::config::AnySection;
use embyx_core::config::models::{
    ArchiveConfig, AvidRulesConfig, CloudDriveConfig, EmbyConfig, FillActorConfig, MappingConfig, MergeConfig,
    PlaylistsConfig, RssConfig,
};
use embyx_db::config_store::ConfigStore;
use embyx_db::database::Database;
use embyx_monitor::acquisitions::{AcquisitionRepository, Ledger, LedgerError};
use embyx_monitor::archive::ArchivePipeline;
use embyx_monitor::cloud::{CloudListing, TrackerCloud};
use embyx_monitor::intake::{AcquisitionIntake, IntakeParts, JavBusMagnets, OfflineFiles, OnSubmitted, SukebeiMagnets};
use embyx_monitor::manual::{CloudFactory, ManualIntakeSource};
use embyx_monitor::mapping::MappingPipeline;
use embyx_monitor::move_in::MoveInSweeper;
use embyx_monitor::playlist_sync::PlaylistSyncPipeline;
use embyx_monitor::playlists::PlaylistRepository;
use embyx_monitor::reconcile::ReconcileScanner;
use embyx_monitor::release_dates::{ReleaseDateFinder, ReleaseDateLookup};
use embyx_monitor::replace::{ArchiverFactory, ConfiguredDirs, IntakeFactory, ParserFactory, ReplacementSource};
use embyx_monitor::reports::{PyException, Raised, RunContext};
use embyx_monitor::rss::{CastLookup, CreditedWork, LookupError, RssParts, RssPipeline, TalentLookup};
use embyx_monitor::runs::PipelineRunRepository;
use embyx_monitor::scheduler::{
    MappingFactory, MappingRun, MonitorScheduler, Ready, RunFailure, RunScope, Runner, SchedulerParts,
    TrackerSubmissions, until_shutdown,
};
use embyx_monitor::subscriptions::SubscriptionRepository;
use embyx_monitor::support::BoxFuture;
use embyx_monitor::tracker::{AcquisitionTracker, SubmitMagnet, TrackerParts, TrackerSettings};

use super::CloudDriveHandle;
use crate::auth::MutationAuth;
use crate::monitor_api::{
    AcquisitionApi, ManualIntake, Replacement, SubscriptionsApi, TalentResolver, create_monitor_router,
};
use crate::playlists_api::{PlaylistFillApi, TrackedStates, create_playlists_router, resolve_fill_dir};
use crate::routing::ApiRouter;

/// `LOGGER`: `bootstrap.py`'s module logger.
const LOGGER: &str = "embyx_manager.bootstrap";

/// What the monitor adds to the app.
pub(crate) struct MonitorParts {
    pub scheduler: MonitorScheduler,
    pub monitor: ApiRouter,
    pub playlists: ApiRouter,
}

/// A client the assembly could not build: Python's constructor would have raised.
#[derive(Debug)]
pub struct ClientBuildError(pub String);

/// `AvidParserHandle`: rebuilds the parser when the `avid` section's version changes.
///
/// A stored ignore pattern Python cannot compile makes Python's `current()` raise on every
/// use; here the rules in force before stay so, and the refusal is logged once per version
/// (D108).
#[derive(Debug)]
struct AvidHandle {
    store: Arc<ConfigStore>,
    state: Mutex<(Option<i32>, AvidParser)>,
}

impl AvidHandle {
    fn current(&self) -> AvidParser {
        let versioned = self
            .store
            .get_with_version("avid")
            .expect("avid is a registered section");
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.0 != Some(versioned.version) {
            let AnySection::Avid(rules) = versioned.section else {
                unreachable!("the registry keeps types aligned");
            };
            let AvidRulesConfig {
                id_exceptions,
                ignored_id_patterns,
            } = rules;
            match AvidParser::new(AvidConfig {
                id_exceptions,
                ignored_id_patterns,
            }) {
                Ok(parser) => state.1 = parser,
                Err(error) => tracing::error!(
                    target: LOGGER,
                    "the avid rules of version {} do not compile, keeping the previous ones: {error}",
                    versioned.version
                ),
            }
            state.0 = Some(versioned.version);
        }
        state.1.clone()
    }
}

/// `_offline_task_dirs(store)`: every directory a source queues offline tasks under, the RSS
/// categories' and the fill actor's, without repeats.
fn offline_task_dirs(store: &ConfigStore) -> Vec<String> {
    let mut dirs: Vec<String> = store
        .get::<RssConfig>()
        .categories
        .into_iter()
        .map(|category| category.task_dir_path)
        .collect();
    let fill_actor_dir = store.get::<FillActorConfig>().task_dir_path;
    if !fill_actor_dir.is_empty() {
        dirs.push(fill_actor_dir);
    }
    dedupe(dirs)
}

/// `tuple(dict.fromkeys(dirs))`.
fn dedupe(dirs: Vec<String>) -> Vec<String> {
    let mut seen = Vec::with_capacity(dirs.len());
    for dir in dirs {
        if !seen.contains(&dir) {
            seen.push(dir);
        }
    }
    seen
}

/// `_polled_task_dirs(store, ledger)`: the configured directories plus any the ledger still
/// has work in.
async fn polled_task_dirs(store: &ConfigStore, ledger: &dyn Ledger) -> Result<Vec<String>, LedgerError> {
    let mut dirs = offline_task_dirs(store);
    dirs.extend(ledger.active_task_dirs().await?);
    Ok(dedupe(dirs))
}

/// `_rss_configuration_gap(store)`.
fn rss_configuration_gap(store: &ConfigStore) -> Option<String> {
    if !store.get::<CloudDriveConfig>().configured() {
        return Some("CloudDrive is not configured".to_owned());
    }
    if store.get::<RssConfig>().categories.is_empty() {
        return Some("at least one RSS category must be configured".to_owned());
    }
    None
}

/// A pipeline's error as the run's failure: a stop it saw is a cancel, the rest raised.
fn failure<E: PyException + Send + Sync + 'static>(error: E) -> RunFailure {
    if error.class_name() == "RunCancelledError" {
        RunFailure::Cancelled
    } else {
        RunFailure::raised(error)
    }
}

/// `RuntimeError(message)`.
fn runtime_error(message: &str) -> RunFailure {
    RunFailure::raised(Raised::new("RuntimeError", message))
}

/// The settle step of a run's scoped CloudDrive handle (immediate without one).
async fn settle(cloud: Option<&AsyncCloudDrive>) {
    if let Some(cloud) = cloud {
        cloud.settle().await;
    }
}

/// `credited_cast(avid)`: the talents AVBase credits on a work.
struct CreditedCast(Arc<AvbaseClient>);

impl CastLookup for CreditedCast {
    fn lookup<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<CreditedWork>, LookupError>> {
        Box::pin(async move {
            let work = match self.0.work(avid).await {
                Ok(work) => work,
                Err(error) if error.is_avbase_error() => {
                    tracing::warn!(
                        target: LOGGER,
                        "AVBase could not be read for {avid}; filing it by sighting alone"
                    );
                    return Ok(None);
                }
                Err(error) => return Err(Box::new(Raised::new(error.class_name(), error.to_string())) as LookupError),
            };
            Ok(work.map(|work| CreditedWork {
                talent_ids: work.cast.iter().filter_map(|member| member.talent_id).collect(),
                release_date: work.release_date,
            }))
        })
    }
}

/// `avbase.talent(name)`.
struct TalentByName(Arc<AvbaseClient>);

impl TalentLookup for TalentByName {
    fn lookup<'a>(
        &'a self,
        name: &'a str,
    ) -> BoxFuture<'a, Result<Option<embyx_clients::avbase::AvbaseTalent>, LookupError>> {
        Box::pin(async move {
            self.0
                .talent(name)
                .await
                .map_err(|error| Box::new(Raised::new(error.class_name(), error.to_string())) as LookupError)
        })
    }
}

/// Everything the closures share.
struct Shared {
    store: Arc<ConfigStore>,
    cloud: Arc<CloudDriveHandle>,
    ledger: Arc<AcquisitionRepository>,
    avid: AvidHandle,
    sukebei: Arc<SukebeiClient>,
    javbus: Arc<JavBusClient>,
    avbase: Arc<AvbaseClient>,
    feeds: Arc<HttpFeedFetcher>,
    subscriptions: Arc<SubscriptionRepository>,
    release_dates: Arc<ReleaseDateFinder>,
    playlists: Arc<PlaylistRepository>,
    submissions: Arc<TrackerSubmissions>,
    /// Held across runs: settling a folder takes two passes to observe.
    reconcile: tokio::sync::Mutex<Option<ReconcileScanner>>,
    /// Held across runs so a staged file is only reported as unreadable once.
    move_in: tokio::sync::Mutex<Option<MoveInSweeper>>,
}

impl Shared {
    fn ledger(&self) -> Arc<dyn Ledger> {
        Arc::clone(&self.ledger) as Arc<dyn Ledger>
    }

    fn archiver(&self) -> ArchivePipeline {
        ArchivePipeline::new(self.store.get::<ArchiveConfig>(), self.avid.current())
    }

    fn on_submitted(&self) -> OnSubmitted {
        let submissions = Arc::clone(&self.submissions);
        Arc::new(move || submissions.notify())
    }

    /// `intake_factory()` over a given CloudDrive client.
    fn intake(&self, cloud: AsyncCloudDrive) -> Option<Arc<AcquisitionIntake>> {
        let built = AcquisitionIntake::new(IntakeParts {
            ledger: self.ledger(),
            sukebei: Arc::clone(&self.sukebei) as Arc<dyn SukebeiMagnets>,
            javbus: Arc::clone(&self.javbus) as Arc<dyn JavBusMagnets>,
            cloud: Arc::new(cloud) as Arc<dyn OfflineFiles>,
            failed_cooldown_seconds: self.store.get::<RssConfig>().failed_avid_cooldown_seconds,
            on_submitted: Some(self.on_submitted()),
            release_date_lookup: Some(Arc::clone(&self.release_dates) as Arc<dyn ReleaseDateLookup>),
        });
        match built {
            Ok(intake) => Some(Arc::new(intake)),
            Err(error) => {
                tracing::error!(target: LOGGER, "the acquisition intake cannot be built: {error}");
                None
            }
        }
    }

    /// `_submit_magnet_at_cloud(avid, magnet)`: queue one magnet at CloudDrive, under the
    /// acquisition's own directory (else the first configured one).
    async fn submit_at_cloud(
        &self,
        cloud: Option<AsyncCloudDrive>,
        avid: &str,
        magnet: &str,
    ) -> Result<bool, LedgerError> {
        let Some(cloud) = cloud else {
            return Ok(false);
        };
        let record = self.ledger.get(avid).await?;
        let task_dir = if let Some(task_dir) = record.and_then(|record| record.task_dir_path) {
            task_dir
        } else {
            // An acquisition the reconcile scan created has no directory of its own. Any
            // polled one will do: the tracker finds a finished download by searching
            // every archive route, not by where it was queued.
            let dirs = offline_task_dirs(&self.store);
            let Some(first) = dirs.into_iter().next() else {
                tracing::error!(target: LOGGER, "no offline directory is configured to submit {avid} to");
                return Ok(false);
            };
            first
        };
        match cloud.add_offline_files(&[magnet.to_owned()], &task_dir).await {
            Ok(result) => Ok(result.success),
            // Already queued at CloudDrive: the hash is what is tracked, so this counts as
            // submitted either way.
            Err(CloudDriveError::Rpc(error)) if error.details.contains("任务已存在") => Ok(true),
            Err(error) => {
                tracing::error!(target: LOGGER, "failed to add a magnet for {avid}\n{error}");
                Ok(false)
            }
        }
    }

    /// `submit_magnet(avid, magnet)`, the tracker's and the dashboard's: a magnet accepted
    /// wakes the tracker. `cloud` is the run's scoped client, or `None` to take the current
    /// one per call (the dashboard's).
    fn submit_magnet(self: &Arc<Self>, cloud: Option<AsyncCloudDrive>) -> SubmitMagnet {
        let shared = Arc::clone(self);
        Arc::new(move |avid: String, magnet: String| {
            let shared = Arc::clone(&shared);
            let cloud = cloud.clone();
            Box::pin(async move {
                let cloud = cloud.or_else(|| shared.cloud.current());
                let accepted = shared.submit_at_cloud(cloud, &avid, &magnet).await?;
                if accepted {
                    shared.submissions.notify();
                }
                Ok(accepted)
            })
        })
    }

    // -- readiness ---------------------------------------------------------------------

    fn tracker_ready(&self) -> Option<String> {
        let archive = self.store.get::<ArchiveConfig>();
        if !archive.enabled {
            return Some("archive is disabled".to_owned());
        }
        if !archive.configured() {
            return Some("archive source, destination, and routes must be configured".to_owned());
        }
        if self.cloud.current().is_none() {
            return Some("CloudDrive must be configured".to_owned());
        }
        if offline_task_dirs(&self.store).is_empty() {
            return Some(
                "at least one offline directory (an RSS category or fill actor) must be configured".to_owned(),
            );
        }
        None
    }

    // -- runners -------------------------------------------------------------------------

    async fn rss_run(self: Arc<Self>, scope: RunScope) -> Result<(), RunFailure> {
        let Some(cloud) = self.cloud.current().map(|cloud| cloud.scoped()) else {
            return Err(runtime_error("CloudDrive is not configured"));
        };
        let work = async {
            let pipeline = RssPipeline::new(RssParts {
                config: self.store.get::<RssConfig>(),
                avid_parser: self.avid.current(),
                subscriptions: Arc::clone(&self.subscriptions) as Arc<dyn embyx_monitor::rss::SubscriptionStore>,
                fetch: Arc::clone(&self.feeds) as Arc<dyn embyx_monitor::rss::FeedFetcher>,
                cloud: Arc::new(cloud.clone()) as Arc<dyn OfflineFiles>,
                sukebei: Arc::clone(&self.sukebei) as Arc<dyn SukebeiMagnets>,
                javbus: Arc::clone(&self.javbus) as Arc<dyn JavBusMagnets>,
                ledger: self.ledger(),
                archiver: self.archiver(),
                on_submitted: Some(self.on_submitted()),
                cast_lookup: Some(Arc::new(CreditedCast(Arc::clone(&self.avbase)))),
                talent_lookup: Some(Arc::new(TalentByName(Arc::clone(&self.avbase)))),
            })
            .map_err(|error| RunFailure::raised(Raised::new("OverflowError", error.0)))?;
            pipeline.run(&scope.ctx).await.map_err(failure)
        };
        until_shutdown(&scope.shutdown, work, settle(Some(&cloud))).await
    }

    /// `_refresh_move_in_listing(cloud, api_root, ctx)`: ask CloudDrive to re-check the
    /// staging tree so the mount sees fresh moves.
    async fn refresh_move_in_listing(cloud: &AsyncCloudDrive, api_root: &str, ctx: &RunContext) {
        let refreshed = async {
            let entries = CloudListing::list_directory(cloud, api_root).await?;
            for entry in entries.iter().filter(|entry| entry.is_directory) {
                CloudListing::list_directory(cloud, &entry.full_path).await?;
            }
            Ok::<(), embyx_monitor::intake::SourceError>(())
        };
        if let Err(error) = refreshed.await {
            ctx.exception(
                &format!("failed to refresh the CloudDrive move-in directory {api_root}"),
                Some(&error),
            );
        }
    }

    async fn archive_run(self: Arc<Self>, scope: RunScope) -> Result<(), RunFailure> {
        let cloud = self.cloud.current().map(|cloud| cloud.scoped());
        let work = async {
            let ctx = &scope.ctx;
            let fill_config = self.store.get::<FillActorConfig>();
            if let Some(cloud) = &cloud {
                for task_dir in polled_task_dirs(&self.store, self.ledger.as_ref())
                    .await
                    .map_err(failure)?
                {
                    if let Err(error) = CloudListing::list_directory(cloud, &task_dir).await {
                        ctx.exception(
                            &format!("failed to refresh the CloudDrive task directory {task_dir}"),
                            Some(&error),
                        );
                    }
                }
                if !fill_config.cloud_move_in_root.is_empty() {
                    Self::refresh_move_in_listing(cloud, &fill_config.cloud_move_in_root, ctx).await;
                }
            }
            let archive_config = self.store.get::<ArchiveConfig>();
            let archiver = ArchivePipeline::new(archive_config.clone(), self.avid.current());
            {
                let mut scanner = self.reconcile.lock().await;
                match scanner.as_mut() {
                    Some(scanner) => scanner.rebind(archiver.clone(), archive_config),
                    None => *scanner = Some(ReconcileScanner::new(self.ledger(), archiver.clone(), archive_config)),
                }
                scanner.as_mut().expect("set above").run(ctx).await.map_err(failure)?;
            }
            if fill_config.move_in_root.is_empty() {
                return Ok(());
            }
            let mut sweeper = self.move_in.lock().await;
            let root = PathBuf::from(&fill_config.move_in_root);
            match sweeper.as_mut() {
                Some(sweeper) => sweeper.rebind(archiver, self.avid.current(), root, fill_config.task_dir_path.clone()),
                None => {
                    *sweeper = Some(MoveInSweeper::new(
                        self.ledger(),
                        archiver,
                        self.avid.current(),
                        root,
                        fill_config.task_dir_path.clone(),
                    ));
                }
            }
            sweeper.as_mut().expect("set above").run(ctx).await.map_err(failure)
        };
        until_shutdown(&scope.shutdown, work, settle(cloud.as_ref())).await
    }

    async fn tracker_run(self: Arc<Self>, scope: RunScope) -> Result<(), RunFailure> {
        let Some(cloud) = self.cloud.current().map(|cloud| cloud.scoped()) else {
            return Ok(());
        };
        let work = async {
            let ctx = &scope.ctx;
            let archive_config = self.store.get::<ArchiveConfig>();
            let task_dirs = polled_task_dirs(&self.store, self.ledger.as_ref())
                .await
                .map_err(failure)?;
            let tracker = AcquisitionTracker::new(TrackerParts {
                ledger: self.ledger(),
                cloud: Arc::new(cloud.clone()) as Arc<dyn TrackerCloud>,
                settings: TrackerSettings::from_config(
                    &archive_config,
                    task_dirs,
                    // The library sits on the same CloudDrive mount the merge tab strips to
                    // reach API paths; a replacement deletes through it.
                    &self.store.get::<MergeConfig>().cloud_mount_prefix,
                ),
                archiver: ArchivePipeline::new(archive_config, self.avid.current()),
                submit_magnet: self.submit_magnet(Some(cloud.clone())),
                release_date_lookup: Some(Arc::clone(&self.release_dates) as Arc<dyn ReleaseDateLookup>),
            });
            let polled = tracker.poll(ctx).await;
            // The resolver leg of the pass: parked acquisitions whose cooldown expired get a
            // fresh resolve-and-submit, whether or not the listing above worked. Records
            // without a pinned directory fall back to the first configured one, as
            // `submit_magnet` does.
            let retried = match self.intake(cloud.clone()) {
                Some(intake) => {
                    let dirs = offline_task_dirs(&self.store);
                    intake
                        .retry_due(ctx, dirs.first().map(String::as_str), 50)
                        .await
                        .map(|_| ())
                        .map_err(failure)
                }
                None => Ok(()),
            };
            // `finally`: an error of the retry pass replaces the poll's.
            retried?;
            polled.map_err(failure)
        };
        until_shutdown(&scope.shutdown, work, settle(Some(&cloud))).await
    }

    async fn playlists_run(self: Arc<Self>, scope: RunScope) -> Result<(), RunFailure> {
        let work = async {
            let emby_config = self.store.get::<EmbyConfig>();
            let emby = EmbyClient::new(&emby_config.address, &emby_config.api_key)
                .map_err(|error| runtime_error(&error.to_string()))?;
            let source = JinjierClient::new(&self.store.get::<PlaylistsConfig>().source_url)
                .map_err(|error| runtime_error(&error.to_string()))?;
            let parser = self.avid.current();
            let pipeline = PlaylistSyncPipeline::new(
                Arc::clone(&self.playlists) as Arc<dyn embyx_monitor::playlists::PlaylistStore>,
                Arc::new(source),
                Arc::new(emby),
                Arc::new(move |name: &str| parser.get_avid(name)),
            );
            pipeline.run(&scope.ctx).await.map_err(failure)
        };
        until_shutdown(&scope.shutdown, work, settle(None)).await
    }
}

fn runner<F, Fut>(shared: &Arc<Shared>, run: F) -> Runner
where
    F: Fn(Arc<Shared>, RunScope) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), RunFailure>> + Send + 'static,
{
    let shared = Arc::clone(shared);
    Arc::new(move |scope| Box::pin(run(Arc::clone(&shared), scope)))
}

fn ready<F>(shared: &Arc<Shared>, check: F) -> Ready
where
    F: Fn(&Shared) -> Option<String> + Send + Sync + 'static,
{
    let shared = Arc::clone(shared);
    Arc::new(move || check(&shared))
}

/// Build the monitor: clients, runners, scheduler, sources and the two routers.
pub(crate) fn assemble(
    store: &Arc<ConfigStore>,
    database: &Database,
    cloud: &Arc<CloudDriveHandle>,
    auth: &MutationAuth,
) -> Result<MonitorParts, ClientBuildError> {
    let shared = Arc::new(build_shared(store, database, cloud)?);
    let scheduler = build_scheduler(&shared, database);
    let (monitor, playlists) = build_routers(&shared, &scheduler, database, auth);
    Ok(MonitorParts {
        scheduler,
        monitor,
        playlists,
    })
}

/// The clients and repositories every runner and source shares.
fn build_shared(
    store: &Arc<ConfigStore>,
    database: &Database,
    cloud: &Arc<CloudDriveHandle>,
) -> Result<Shared, ClientBuildError> {
    let built = |what: &str, error: &dyn std::fmt::Display| ClientBuildError(format!("{what}: {error}"));
    let avbase = Arc::new(AvbaseClient::new(&AvbaseOptions::default()).map_err(|error| built("AVBase", &error))?);
    Ok(Shared {
        store: Arc::clone(store),
        cloud: Arc::clone(cloud),
        ledger: Arc::new(AcquisitionRepository::new(database.clone())),
        avid: AvidHandle {
            store: Arc::clone(store),
            state: Mutex::new((None, AvidParser::default())),
        },
        sukebei: Arc::new(SukebeiClient::new(&SukebeiOptions::default()).map_err(|error| built("sukebei", &error))?),
        javbus: Arc::new(JavBusClient::new(&JavBusOptions::default()).map_err(|error| built("JavBus", &error))?),
        // Feed sightings carry no release date; the catalog fills it in when a row first
        // has to wait for a magnet, so the resolve schedule can anchor on it.
        release_dates: Arc::new(ReleaseDateFinder::new(Arc::clone(&avbase) as Arc<_>)),
        avbase,
        feeds: Arc::new(HttpFeedFetcher::new().map_err(|error| built("feeds", &error))?),
        subscriptions: Arc::new(SubscriptionRepository::new(database.clone())),
        playlists: Arc::new(PlaylistRepository::new(database.clone())),
        submissions: Arc::new(TrackerSubmissions::new()),
        reconcile: tokio::sync::Mutex::new(None),
        move_in: tokio::sync::Mutex::new(None),
    })
}

/// The scheduler, with a runner and a readiness check per pipeline.
fn build_scheduler(shared: &Arc<Shared>, database: &Database) -> MonitorScheduler {
    let store = &shared.store;
    let mapping_shared = Arc::clone(shared);
    let mapping_factory: MappingFactory = Arc::new(move || {
        Box::new(MappingPipeline::new(
            mapping_shared.store.get::<MappingConfig>(),
            mapping_shared.avid.current(),
        )) as Box<dyn MappingRun>
    });
    let mut parts = SchedulerParts::new(
        Arc::clone(store) as Arc<dyn embyx_monitor::scheduler::SchedulerConfig>,
        Arc::new(PipelineRunRepository::new(database.clone())),
        (
            runner(shared, Shared::rss_run),
            runner(shared, Shared::archive_run),
            mapping_factory,
        ),
        (
            ready(shared, |shared| rss_configuration_gap(&shared.store)),
            ready(shared, |shared| {
                (!shared.store.get::<ArchiveConfig>().configured())
                    .then(|| "archive source, destination, and mapping must be configured".to_owned())
            }),
            ready(shared, |shared| {
                (!shared.store.get::<MappingConfig>().configured())
                    .then(|| "mapping source and destination directories must be configured".to_owned())
            }),
        ),
    );
    parts.tracker_poll = Some(runner(shared, Shared::tracker_run));
    parts.tracker_ready = Some(ready(shared, Shared::tracker_ready));
    parts.playlists_runner = Some(runner(shared, Shared::playlists_run));
    parts.playlists_ready = Some(ready(shared, |shared| {
        (!shared.store.get::<EmbyConfig>().configured())
            .then(|| "Emby address and API key must be configured".to_owned())
    }));
    parts.submissions = Arc::clone(&shared.submissions);
    MonitorScheduler::new(parts)
}

/// The monitor and playlists routers, over the manual and replacement sources.
fn build_routers(
    shared: &Arc<Shared>,
    scheduler: &MonitorScheduler,
    database: &Database,
    auth: &MutationAuth,
) -> (ApiRouter, ApiRouter) {
    let store = &shared.store;
    let cloud = &shared.cloud;
    // The factories the manual and replacement sources resolve live configuration through.
    let intake_shared = Arc::clone(shared);
    let intake_factory: IntakeFactory = Arc::new(move || {
        intake_shared
            .cloud
            .current()
            .and_then(|cloud| intake_shared.intake(cloud))
    });
    let cloud_handle = Arc::clone(cloud);
    let cloud_factory: CloudFactory = Arc::new(move || {
        cloud_handle
            .current()
            .map(|cloud| Arc::new(cloud) as Arc<dyn CloudListing>)
    });
    let archiver_shared = Arc::clone(shared);
    let archiver_factory: ArchiverFactory = Arc::new(move || archiver_shared.archiver());
    let parser_shared = Arc::clone(shared);
    let parser_factory: ParserFactory = Arc::new(move || parser_shared.avid.current());
    let dirs_store = Arc::clone(store);
    let configured_dirs: ConfiguredDirs = Arc::new(move || offline_task_dirs(&dirs_store));
    let dashboard_submit = shared.submit_magnet(None);

    let manual: Arc<dyn ManualIntake> = Arc::new(ManualIntakeSource {
        ledger: shared.ledger(),
        intake_factory: Arc::clone(&intake_factory),
        cloud_factory,
        archiver_factory: Arc::clone(&archiver_factory),
        parser_factory: Arc::clone(&parser_factory),
        configured_dirs: Arc::clone(&configured_dirs),
        // A playlist fill only queues: wake the tracker so the first resolves start now.
        on_queued: Some(shared.on_submitted()),
        logger: embyx_monitor::manual::LOGGER.to_owned(),
    });
    let replacement: Arc<dyn Replacement> = Arc::new(ReplacementSource::new(
        shared.ledger(),
        intake_factory,
        archiver_factory,
        parser_factory,
        configured_dirs,
        Arc::clone(&dashboard_submit),
    ));

    let fill_store = Arc::clone(store);
    let playlists_router = create_playlists_router(
        Arc::clone(&shared.playlists) as Arc<dyn crate::playlists_api::PlaylistCatalog>,
        auth.clone(),
        Some(Arc::clone(&shared.ledger) as Arc<dyn TrackedStates>),
        Some(PlaylistFillApi {
            manual: Arc::clone(&manual),
            task_dir: Arc::new(move || {
                resolve_fill_dir(&fill_store.get::<PlaylistsConfig>(), &fill_store.get::<RssConfig>())
            }),
        }),
    );

    let tracker_shared = Arc::clone(shared);
    let categories_store = Arc::clone(store);
    let talent_client = Arc::clone(&shared.avbase);
    let resolve_talent: TalentResolver = Arc::new(move |query: String| {
        let client = Arc::clone(&talent_client);
        Box::pin(async move { client.find_talent(&query).await })
    });
    let monitor_router = create_monitor_router(
        Arc::new(scheduler.clone()),
        Arc::new(PipelineRunRepository::new(database.clone())),
        auth.clone(),
        Some(AcquisitionApi {
            ledger: shared.ledger(),
            submit_magnet: Some(dashboard_submit),
            tracker_ready: Some(Arc::new(move || tracker_shared.tracker_ready())),
            manual: Some(manual),
            replacement: Some(replacement),
        }),
        Some(SubscriptionsApi {
            repository: Arc::clone(&shared.subscriptions) as Arc<dyn crate::monitor_api::SubscriptionRegistry>,
            categories: Arc::new(move || {
                categories_store
                    .get::<RssConfig>()
                    .categories
                    .into_iter()
                    .map(|category| category.label)
                    .collect()
            }),
            resolve_talent: Some(resolve_talent),
        }),
    );
    (monitor_router, playlists_router)
}

#[cfg(test)]
mod tests {
    use super::dedupe;

    #[test]
    fn directories_keep_their_first_place() {
        let dirs = |items: &[&str]| items.iter().map(|item| (*item).to_owned()).collect::<Vec<_>>();
        assert_eq!(dedupe(dirs(&["/a", "/b", "/a", "/c", "/b"])), dirs(&["/a", "/b", "/c"]));
    }
}
