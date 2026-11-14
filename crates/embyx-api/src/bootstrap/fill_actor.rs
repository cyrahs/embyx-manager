//! The fill actor's part of the assembly root: the runtime snapshot of the stored section,
//! the service over the shared clients, the job manager, the AVID cast lookup, the archive
//! trigger after a move, and the router, health probe and lifespan.
//!
//! Python origin: `build_app` in `bootstrap.py` (`FillActorRuntimeHandle`,
//! `_build_fill_actor_runtime`, `notify_files_moved`, `actors_for_avid`, the service and
//! `FillActorJobManager`). The service takes the PostgreSQL advisory lock as its mutation
//! lock, as Python's does, so a Rust and a Python process never move files at once.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use embyx_clients::avbase::AvbaseClient;
use embyx_clients::javbus::JavBusClient;
use embyx_core::config::AnySection;
use embyx_core::config::models::FillActorConfig;
use embyx_core::settings::Settings;
use embyx_db::config_store::ConfigStore;
use embyx_db::database::Database;
use embyx_db::locking::PostgresAdvisoryLock;
use embyx_fill_actor::adapters::{
    AvidBrandResolver, CloudDriveFileMover, IntakeFactory, LedgerAcquisitionGateway, UnionActorCatalog,
};
use embyx_fill_actor::cloud_moves::CloudMovePaths;
use embyx_fill_actor::jobs::{FillActorJobManager, JobManagerOptions};
use embyx_fill_actor::postgres_repository::PostgresFillActorRepository;
use embyx_fill_actor::service::{
    FillActorPaths, FillActorRuntime, FillActorService, OnMoved, RuntimeProvider, ServiceOptions, ServiceParts,
};
use embyx_monitor::reports::PipelineName;
use embyx_monitor::scheduler::{MonitorScheduler, TriggerError};
use tokio_util::task::TaskTracker;

use super::CloudDriveHandle;
use crate::app::{FeatureHealthProbe, Lifespan};
use crate::auth::MutationAuth;
use crate::fill_actor_api::{
    AvidActorLookup, FillActorApi, FillActorLifespan, create_fill_actor_router, fill_actor_health,
};
use crate::routing::ApiRouter;

/// `LOGGER`: `bootstrap.py`'s module logger.
const LOGGER: &str = "embyx_manager.bootstrap";

/// What the monitor's assembly shares with the fill actor: Python's `build_app` locals.
pub(crate) struct FillActorInputs {
    pub avbase: Arc<AvbaseClient>,
    pub javbus: Arc<JavBusClient>,
    /// `intake_factory()`.
    pub intake: IntakeFactory,
    pub scheduler: MonitorScheduler,
}

/// What the fill actor adds to the app.
pub(crate) struct FillActorParts {
    pub router: ApiRouter,
    pub health: FeatureHealthProbe,
    pub lifespan: Arc<dyn Lifespan>,
}

/// `FillActorRuntimeHandle`: the stored section as the snapshot the service reads, rebuilt
/// only when the section's version changes. An invalid stored value leaves the feature
/// unconfigured rather than taking the process down; the Settings page is how it gets fixed.
#[derive(Debug)]
pub struct FillActorRuntimeHandle {
    store: Arc<ConfigStore>,
    state: Mutex<(Option<i32>, Arc<FillActorRuntime>)>,
}

impl FillActorRuntimeHandle {
    pub fn new(store: Arc<ConfigStore>) -> Self {
        Self {
            store,
            state: Mutex::new((None, Arc::new(FillActorRuntime::default()))),
        }
    }

    /// `current()`.
    pub fn current(&self) -> Arc<FillActorRuntime> {
        let versioned = self
            .store
            .get_with_version("fill_actor")
            .expect("fill_actor is a registered section");
        let AnySection::FillActor(config) = versioned.section else {
            unreachable!("the registry keeps types aligned");
        };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.0 != Some(versioned.version) {
            *state = (
                Some(versioned.version),
                Arc::new(build_fill_actor_runtime(&config, versioned.version)),
            );
        }
        Arc::clone(&state.1)
    }
}

/// `_build_fill_actor_runtime(config, version)`.
pub fn build_fill_actor_runtime(config: &FillActorConfig, version: i32) -> FillActorRuntime {
    let unconfigured = FillActorRuntime {
        version,
        ..FillActorRuntime::default()
    };
    if !config.configured() {
        return unconfigured;
    }
    let cloud_move_paths = if config.cloud_configured() {
        match CloudMovePaths::from_values(
            &config.cloud_strm_mount_prefix,
            &config.cloud_source_roots,
            &config.cloud_move_in_root,
        ) {
            Ok(paths) => Some(paths),
            Err(error) => {
                tracing::error!(
                    target: LOGGER,
                    error = %error,
                    "stored fill_actor CloudDrive paths are invalid; moves stay disabled"
                );
                return unconfigured;
            }
        }
    } else {
        None
    };
    FillActorRuntime {
        version,
        paths: Some(FillActorPaths::from_iterable(
            &config.actor_root,
            config.additional_roots.iter().map(PathBuf::from),
            &config.move_in_root,
        )),
        root_sentinel: Some(config.root_sentinel.clone()),
        move_in_by_brand: config.move_in_by_brand,
        apply_enabled: config.apply_enabled,
        cloud_move_paths,
    }
}

/// `notify_files_moved()`: the fill actor staged new files, so archive them now rather than
/// on the next cron run. The trigger runs in a task of its own; busy or not configured, the
/// cron sweep files them instead.
fn notify_files_moved(scheduler: MonitorScheduler, tasks: TaskTracker) -> OnMoved {
    Arc::new(move || {
        let scheduler = scheduler.clone();
        tasks.spawn(async move {
            match scheduler.trigger(PipelineName::Archive).await {
                Ok(_) => {}
                Err(error @ (TriggerError::Busy(_) | TriggerError::NotConfigured { .. })) => tracing::info!(
                    target: LOGGER,
                    "not starting an archive run for freshly moved files: {error}"
                ),
                Err(error) => tracing::error!(
                    target: LOGGER,
                    error = %error,
                    "failed to start an archive run for freshly moved files"
                ),
            }
        });
        Ok(())
    })
}

/// `actors_for_avid(avid)`: who is credited on a video, AVBase's cast (a name resolves every
/// alias), else JavBus's stars.
fn actors_for_avid(avbase: Arc<AvbaseClient>, javbus: Arc<JavBusClient>) -> AvidActorLookup {
    Arc::new(move |avid: String| {
        let (avbase, javbus) = (Arc::clone(&avbase), Arc::clone(&javbus));
        Box::pin(async move {
            let work = avbase.work(&avid).await.unwrap_or_else(|_| {
                tracing::warn!(target: LOGGER, "AVBase could not be read for {avid}; asking JavBus instead");
                None
            });
            if let Some(work) = work.filter(|work| !work.cast.is_empty()) {
                let mut seen = std::collections::HashSet::new();
                return Ok(work
                    .cast
                    .into_iter()
                    .filter(|member| seen.insert(member.name.clone()))
                    .map(|member| (member.name.clone(), member.name))
                    .collect());
            }
            let actors = javbus
                .get_video_actors(&avid)
                .await
                .map_err(|error| error.to_string())?;
            Ok(actors.into_iter().map(|actor| (actor.actor_id, actor.name)).collect())
        })
    })
}

/// Build the fill actor: the service, its job manager, and what the app mounts.
pub(crate) fn assemble(
    settings: &Settings,
    store: &Arc<ConfigStore>,
    database: &Database,
    cloud: &Arc<CloudDriveHandle>,
    auth: &MutationAuth,
    inputs: FillActorInputs,
) -> FillActorParts {
    let repository = Arc::new(PostgresFillActorRepository::new(database.clone()));
    let runtime_handle = Arc::new(FillActorRuntimeHandle::new(Arc::clone(store)));
    let runtime: RuntimeProvider = Arc::new(move || runtime_handle.current());
    let task_store = Arc::clone(store);
    let queued_scheduler = inputs.scheduler.clone();
    let gateway = LedgerAcquisitionGateway {
        intake_factory: inputs.intake,
        task_dir: Arc::new(move || task_store.get::<FillActorConfig>().task_dir_path),
        // Wakes the tracker so queued rows get their first resolve without waiting out the
        // poll interval.
        on_queued: Some(Arc::new(move || queued_scheduler.notify_submission())),
    };
    let mover_cloud = Arc::clone(cloud);
    let limit = |value: u64| usize::try_from(value).unwrap_or(usize::MAX);
    let service = FillActorService::new(
        ServiceParts {
            runtime,
            actor_catalog: Arc::new(UnionActorCatalog::new(
                Arc::clone(&inputs.avbase),
                Arc::clone(&inputs.javbus),
            )),
            acquisition_gateway: Arc::new(gateway),
            brand_resolver: Arc::new(AvidBrandResolver),
            repository: Arc::clone(&repository) as Arc<_>,
        },
        ServiceOptions {
            max_actors: limit(settings.max_actors),
            max_videos: limit(settings.max_videos),
            magnet_concurrency: limit(settings.magnet_concurrency),
            // The mover is always there; whether moves may run is the runtime's call, since
            // the CloudDrive paths it needs live in the database.
            cloud_file_mover: Some(Arc::new(CloudDriveFileMover::new(Arc::new(move || {
                mover_cloud.current()
            })))),
            mutation_lock: Some(PostgresAdvisoryLock::new(database.clone()).into()),
            on_moved: Some(notify_files_moved(inputs.scheduler.clone(), TaskTracker::new())),
            ..ServiceOptions::default()
        },
    )
    .expect("the settings' limits are positive");
    let jobs = FillActorJobManager::new(
        Arc::new(service.clone()),
        Arc::clone(&repository) as Arc<_>,
        JobManagerOptions::default(),
    )
    .expect("the default job options are valid");
    let router = create_fill_actor_router(FillActorApi {
        service: service.clone(),
        repository: Arc::clone(&repository),
        jobs: jobs.clone(),
        auth: auth.clone(),
        avid_actor_lookup: Some(actors_for_avid(inputs.avbase, inputs.javbus)),
    });
    FillActorParts {
        router,
        health: fill_actor_health(service.clone(), Arc::clone(&repository)),
        lifespan: Arc::new(FillActorLifespan::new(service, repository, jobs)),
    }
}
