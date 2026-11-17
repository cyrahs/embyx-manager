//! The assembly root: the database, the config store, the shared CloudDrive client and the
//! two apps.
//!
//! Python origin: the shell of `bootstrap.py` (`build_app`, `CloudDriveHandle`, the runtime
//! lifespan's store load and client close). WP4 assembles what the shell needs: the store,
//! the config router, health and the transcode listener's empty app; WP9 the monitor
//! ([`monitor`]: its scheduler, started after the store loads and closed before the clients,
//! and the monitor and playlists routers); WP12 the fill actor ([`fill_actor`]: the service on
//! the shared clients, its job manager, router, health block and lifespan); WP14 the merge
//! ([`merge`]: the service, auto-merge and the title scan warming up, started after the
//! scheduler and closed before it, the merge router and the transcode listener's routes).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use embyx_clients::clouddrive::{AsyncCloudDrive, ClientOptions};
use embyx_core::config::AnySection;
use embyx_core::settings::{Settings, SettingsError};
use embyx_db::config_store::ConfigStore;
use embyx_db::database::{Database, DatabaseError};
use futures_util::future::BoxFuture;
use tokio_util::task::TaskTracker;

use crate::app::{AppParts, Lifespan, ReadyProbe, create_app, create_transcode_app};
use crate::auth::MutationAuth;
use crate::config_api::{ConfigApiOptions, create_config_router};
use crate::http::Internal;
use crate::routing::App;

mod fill_actor;
mod merge;
mod monitor;

pub use fill_actor::{FillActorRuntimeHandle, build_fill_actor_runtime};

pub use monitor::ClientBuildError;

/// How long shutdown waits for CloudDrive calls still running, after the requests in flight
/// drained: under the pod's 60 s termination grace period, so the database usually still
/// closes in time (D41, D56).
pub const CLOUD_CLOSE_LIMIT: Duration = Duration::from_secs(45);

#[derive(Debug, Default)]
struct HandleState {
    version: Option<i32>,
    cloud: Option<AsyncCloudDrive>,
}

/// `CloudDriveHandle`: owns the CloudDrive client and swaps it when the `clouddrive`
/// section's version changes. A replaced client is closed in the background, which waits
/// for its calls still running (D41); shutdown waits for those closes too.
#[derive(Debug)]
pub struct CloudDriveHandle {
    store: Arc<ConfigStore>,
    state: Mutex<HandleState>,
    closing: TaskTracker,
}

impl CloudDriveHandle {
    pub fn new(store: Arc<ConfigStore>) -> Self {
        Self {
            store,
            state: Mutex::new(HandleState::default()),
            closing: TaskTracker::new(),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, HandleState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `current()`: the client for the stored section, `None` while it is not configured.
    /// Must be called inside the runtime (a new client spawns its channel there).
    pub fn current(&self) -> Option<AsyncCloudDrive> {
        let versioned = self
            .store
            .get_with_version("clouddrive")
            .expect("clouddrive is a registered section");
        let AnySection::CloudDrive(config) = versioned.section else {
            unreachable!("the registry keeps types aligned");
        };
        let mut state = self.state();
        if state.version != Some(versioned.version) {
            if let Some(old) = state.cloud.take() {
                self.closing.spawn(async move { old.close().await });
            }
            state.version = Some(versioned.version);
            if config.configured() {
                state.cloud = Some(AsyncCloudDrive::connect(&ClientOptions::new(
                    config.address.clone(),
                    config.api_token.clone(),
                    config.secure,
                )));
            }
        }
        state.cloud.clone()
    }

    /// `close()`: close the current client and wait, up to `limit`, for it and every
    /// replaced client to finish the calls they had running.
    pub async fn close(&self, limit: Duration) {
        let current = self.state().cloud.take();
        if let Some(cloud) = current {
            self.closing.spawn(async move { cloud.close().await });
        }
        self.closing.close();
        if tokio::time::timeout(limit, self.closing.wait()).await.is_err() {
            tracing::warn!(
                target: "embyx_manager.bootstrap",
                "CloudDrive calls still running after {} s; shutting down without them",
                limit.as_secs()
            );
        }
    }
}

/// Why the app cannot be assembled.
#[derive(Debug)]
pub enum BuildError {
    /// `settings.validate_exposure()` refused the settings: Python's traceback, exit 1.
    Settings(SettingsError),
    /// The database URL is not one; Python fails at startup, when it first connects.
    Database(DatabaseError),
    /// An HTTP client could not be built (its TLS backend or proxy settings refused).
    Client(ClientBuildError),
}

/// The assembled server: both apps and the lifespans that run around them.
pub struct Assembled {
    pub app: App,
    pub transcode_app: App,
    pub lifespans: Vec<Arc<dyn Lifespan>>,
    pub store: Arc<ConfigStore>,
    pub database: Database,
    pub cloud: Arc<CloudDriveHandle>,
    pub scheduler: embyx_monitor::scheduler::MonitorScheduler,
    /// The names the app's `feature_health` reports under, in order.
    pub feature_health: Vec<String>,
    /// Whether the fill-actor router got its `avid_actor_lookup`.
    pub fill_actor_avid_lookup: bool,
}

impl std::fmt::Debug for Assembled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Assembled")
            .field("app", &self.app)
            .field("transcode_app", &self.transcode_app)
            .field("lifespans", &self.lifespans.len())
            .finish_non_exhaustive()
    }
}

/// `runtime_lifespan`: the config store loaded first, then the scheduler, the merge service,
/// the title scan warming up in the background and auto-merge started; at the end the warm-up
/// cancelled, auto-merge, the merge service and the scheduler closed, then the clients and the
/// database last.
struct RuntimeLifespan {
    store: Arc<ConfigStore>,
    database: Database,
    cloud: Arc<CloudDriveHandle>,
    scheduler: embyx_monitor::scheduler::MonitorScheduler,
    merge: merge::MergeParts,
    warming: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Lifespan for RuntimeLifespan {
    fn start(&self) -> BoxFuture<'_, Result<(), Internal>> {
        Box::pin(async move {
            self.store.load().await.map_err(Internal::new)?;
            self.scheduler.start().await.map_err(Internal::new)?;
            self.merge.service.start();
            let (catalog, cache) = (self.merge.catalog.clone(), self.merge.cache.clone());
            let warming = tokio::spawn(async move { embyx_merge::catalog::warm_title_cache(&catalog, &cache).await });
            *self.warming.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(warming);
            self.merge.auto.start();
            Ok(())
        })
    }

    fn stop(&self) -> BoxFuture<'_, Result<(), Internal>> {
        Box::pin(async move {
            let warming = self
                .warming
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(warming) = warming {
                warming.abort();
            }
            self.merge.auto.aclose().await;
            self.merge.service.aclose().await;
            self.scheduler.aclose().await;
            self.cloud.close(CLOUD_CLOSE_LIMIT).await;
            self.database.close().await;
            Ok(())
        })
    }
}

/// The front end's build, next to the executable (`static/`), as Python finds it next to
/// its package (`Path(__file__).parent / 'static'`).
pub fn frontend_dist() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("static")))
}

/// `build_app(settings)`.
pub fn build_app(settings: &Settings, frontend_dist: Option<PathBuf>) -> Result<Assembled, BuildError> {
    settings.validate_exposure().map_err(BuildError::Settings)?;
    let database = Database::new(&settings.database_url).map_err(BuildError::Database)?;
    let store = Arc::new(ConfigStore::new(database.clone()));
    let cloud = Arc::new(CloudDriveHandle::new(Arc::clone(&store)));

    let ready_database = database.clone();
    let app_ready: ReadyProbe = Arc::new(move || {
        let database = ready_database.clone();
        Box::pin(async move { embyx_db::health::health_check(&database).await.map_err(Internal::new) })
    });
    let auth = MutationAuth::new(settings.api_token.as_deref());
    let config_router = create_config_router(Arc::clone(&store), auth.clone(), ConfigApiOptions::default());
    let monitor = monitor::assemble(&store, &database, &cloud, &auth).map_err(BuildError::Client)?;
    let fill_actor = fill_actor::assemble(settings, &store, &database, &cloud, &auth, monitor.fill_actor);
    let merge = merge::assemble(
        &store,
        &database,
        &cloud,
        &auth,
        &monitor.scheduler,
        settings.transcode_token.as_deref(),
    )
    .map_err(BuildError::Client)?;
    let feature_health = vec![("fill_actor".to_owned(), fill_actor.health)];
    let feature_health_names = feature_health.iter().map(|(name, _)| name.clone()).collect();
    let app = create_app(AppParts {
        app_ready,
        routers: vec![
            fill_actor.router,
            config_router,
            monitor.monitor,
            monitor.playlists,
            merge.router.clone(),
        ],
        feature_health,
        api_token: settings.api_token.clone(),
        max_request_bytes: usize::try_from(settings.max_request_bytes).unwrap_or(usize::MAX),
        frontend_dist,
    })
    .expect("no feature reports under a reserved health key");
    // Served on a port of its own when EMBYX_MANAGER_TRANSCODE_PORT is set.
    let transcode_app = create_transcode_app(merge.transcode.clone());
    let runtime: Arc<dyn Lifespan> = Arc::new(RuntimeLifespan {
        store: Arc::clone(&store),
        database: database.clone(),
        cloud: Arc::clone(&cloud),
        scheduler: monitor.scheduler.clone(),
        merge,
        warming: Mutex::new(None),
    });
    Ok(Assembled {
        app,
        transcode_app,
        // The runtime comes up first and goes down last; features stack on top of it.
        lifespans: vec![runtime, fill_actor.lifespan],
        store,
        database,
        cloud,
        scheduler: monitor.scheduler,
        feature_health: feature_health_names,
        fill_actor_avid_lookup: fill_actor.avid_actor_lookup,
    })
}

#[cfg(test)]
mod tests;
