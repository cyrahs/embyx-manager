//! The merge's part of the assembly root: the service, auto-merge, the title scan cache, the
//! merge router and the transcode listener's router.
//!
//! Python origin: `build_app` in `bootstrap.py` (`_trigger_after_merge`, `MergeService(...)`,
//! `MergeCatalog(...)`, `TitleScanCache()`, `AutoMerger(...)`, `create_merge_router(...)`,
//! `create_transcode_router(merge_service, token=settings.transcode_token)`) and the merge
//! steps of `runtime_lifespan`.

use std::collections::HashMap;
use std::sync::Arc;

use embyx_clients::pyvalue::Raised;
use embyx_core::config::models::{ArchiveConfig, CloudDriveConfig, MappingConfig, MergeConfig};
use embyx_db::config_store::ConfigStore;
use embyx_db::database::Database;
use embyx_merge::auto::{AutoMerger, AutoParts};
use embyx_merge::catalog::{MergeCatalog, TaskDirsFor, TitleScanCache};
use embyx_merge::ports::{MergeCloud, MergeKube, Pipeline, Trigger};
use embyx_merge::service::{CloudSource, MergeService, ServiceParts, StepError};
use embyx_merge::tasks::MergeTaskRepository;
use embyx_merge::transcode::TranscodeRepository;
use embyx_monitor::acquisitions::{AcquisitionRepository, LedgerError};
use embyx_monitor::reports::PipelineName;
use embyx_monitor::scheduler::{MonitorScheduler, TriggerError};

use super::{ClientBuildError, CloudDriveHandle};
use crate::auth::MutationAuth;
use crate::merge_api::{MergeTasksApi, create_merge_router};
use crate::routing::ApiRouter;
use crate::transcode_api::create_transcode_router;

/// `LOGGER`: `bootstrap.py`'s module logger.
const LOGGER: &str = "embyx_manager.bootstrap";

/// What the merge adds to the app and to the runtime lifespan.
pub(crate) struct MergeParts {
    pub service: MergeService,
    pub auto: AutoMerger,
    pub catalog: MergeCatalog,
    pub cache: TitleScanCache,
    pub router: ApiRouter,
    pub transcode: ApiRouter,
}

/// `_trigger_after_merge(pipeline)`: a merged title is staged or filed, so archive or remap
/// it now rather than on the next schedule. A busy or unconfigured pipeline is logged and
/// let be, as Python does; any other failure (the run's start could not be recorded) raises
/// into the merge step.
fn trigger_after_merge(scheduler: &MonitorScheduler) -> Trigger {
    let scheduler = scheduler.clone();
    Arc::new(move |pipeline: Pipeline| {
        let scheduler = scheduler.clone();
        Box::pin(async move {
            let name = match pipeline {
                Pipeline::Archive => PipelineName::Archive,
                Pipeline::Mapping => PipelineName::Mapping,
            };
            match scheduler.trigger(name).await {
                Ok(_) => Ok(()),
                Err(error @ (TriggerError::Busy(_) | TriggerError::NotConfigured { .. })) => {
                    tracing::info!(
                        target: LOGGER,
                        "not starting a {} run after a merge step: {error}",
                        pipeline.as_str()
                    );
                    Ok(())
                }
                Err(error) => {
                    let class = match &error {
                        TriggerError::Start(store) => store.class_name(),
                        _ => "RuntimeError",
                    };
                    Err(StepError::from(Raised::new(class, error.to_string())))
                }
            }
        })
    })
}

/// `ledger.task_dirs_for`: the acquisition ledger's offline directory per title.
fn task_dirs_for(ledger: Arc<AcquisitionRepository>) -> TaskDirsFor {
    Arc::new(move |avids: Vec<String>| {
        let ledger = Arc::clone(&ledger);
        Box::pin(async move {
            match ledger.task_dirs_for(&avids).await {
                Ok(found) => Ok(found.into_iter().collect::<HashMap<_, _>>()),
                Err(LedgerError::Store(error)) => {
                    Err(StepError::from(Raised::new(error.class_name(), error.to_string())))
                }
                Err(error) => Err(StepError::from(Raised::new("Exception", error.to_string()))),
            }
        })
    })
}

/// Build the merge: service, catalog, cache, auto-merge and both routers.
pub(crate) fn assemble(
    store: &Arc<ConfigStore>,
    database: &Database,
    cloud: &Arc<CloudDriveHandle>,
    auth: &MutationAuth,
    scheduler: &MonitorScheduler,
    transcode_token: Option<&str>,
) -> Result<MergeParts, ClientBuildError> {
    let section = |store: &Arc<ConfigStore>| Arc::clone(store);
    let kube = embyx_clients::kube::KubeJobs::from_environment()
        .map_err(|error| ClientBuildError(format!("Kubernetes Jobs: {error}")))?
        .map(|kube| Arc::new(kube) as Arc<dyn MergeKube>);
    let cloud_handle = Arc::clone(cloud);
    let cloud_source: CloudSource = Arc::new(move || {
        cloud_handle
            .current()
            .map(|cloud| Arc::new(cloud) as Arc<dyn MergeCloud>)
    });
    let repository = MergeTaskRepository::new(database.clone());
    let (merge_store, archive_store, clouddrive_store) = (section(store), section(store), section(store));
    let mut parts = ServiceParts::new(
        repository.clone(),
        Arc::clone(&cloud_source),
        kube,
        Arc::new(move || merge_store.get::<MergeConfig>()),
        Arc::new(move || archive_store.get::<ArchiveConfig>()),
        Arc::new(move || clouddrive_store.get::<CloudDriveConfig>()),
        trigger_after_merge(scheduler),
    );
    parts.transcodes = Some(TranscodeRepository::new(database.clone()));
    let service = MergeService::new(parts);

    let (archive_store, mapping_store) = (section(store), section(store));
    let catalog = MergeCatalog {
        archive: Arc::new(move || archive_store.get::<ArchiveConfig>()),
        mapping: Arc::new(move || mapping_store.get::<MappingConfig>()),
        task_dirs_for: task_dirs_for(Arc::new(AcquisitionRepository::new(database.clone()))),
    };
    let cache = TitleScanCache::default();
    let auto_store = section(store);
    let auto_merger = AutoMerger::new(AutoParts::new(
        repository,
        catalog.clone(),
        cache.clone(),
        service.clone(),
        Arc::new(move || auto_store.get::<MergeConfig>()),
        cloud_source,
    ));
    let status_auto = auto_merger.clone();
    let router = create_merge_router(
        catalog.clone(),
        Some(MergeTasksApi {
            service: service.clone(),
            auto_status: Some(Arc::new(move || status_auto.status())),
        }),
        auth.clone(),
        Some(cache.clone()),
    );
    let transcode = create_transcode_router(service.clone(), transcode_token);
    Ok(MergeParts {
        service,
        auto: auto_merger,
        catalog,
        cache,
        router,
        transcode,
    })
}
