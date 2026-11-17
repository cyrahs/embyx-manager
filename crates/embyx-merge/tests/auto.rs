//! Automatic merging over a fake mapping tree, CloudDrive listing and disk.
//!
//! Ported from `tests/test_merge_auto.py`. Python hands the merger duck-typed fakes of the
//! repository and the service; here the merger takes the real ones, so the tasks live in a
//! disposable PostgreSQL database and the service is one that can run (a Job API, CloudDrive
//! configured, no home worker). What the Python fakes answered is what these answer: no task
//! is parked and nothing is reserved. Python's count of `wake()` calls is read from the real
//! service (`MergeService::woken`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use chrono::Utc;
use embyx_clients::clouddrive::aio::{EnsureResult, MoveResult, OperationResult};
use embyx_clients::clouddrive::{CloudDriveError, CloudFile, CopyTask, UploadState};
use embyx_core::config::models::{ArchiveConfig, CloudDriveConfig, MappingConfig, MergeConfig};
use embyx_db::testdb::TestDatabase;
use embyx_merge::auto::{AutoMerger, AutoParts, GIB};
use embyx_merge::catalog::{AutoState, MergeCatalog, TitleScanCache};
use embyx_merge::fakes::FakeKube;
use embyx_merge::ports::{BoxFuture, MergeCloud, MergeKube};
use embyx_merge::service::{MergeService, ServiceParts};
use embyx_merge::tasks::{MergeState, MergeTask, MergeTaskRepository, NewTask, TaskField};

const LIBRARY: &str = "/mnt/cd2/115/embyx";
const PART: i64 = 1 << 30;

fn routes(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(source, destination)| ((*source).to_owned(), (*destination).to_owned()))
        .collect()
}

/// `tests/test_merge_detect.py`'s `ARCHIVE`.
fn archive() -> ArchiveConfig {
    ArchiveConfig {
        src_dir: "/mnt/cd2/115/embyx_in".to_owned(),
        dst_dir: LIBRARY.to_owned(),
        mapping: routes(&[
            ("hh", "type/hh"),
            ("rank", "rank"),
            ("rest", "rest"),
            ("special", "type/special"),
        ]),
        priority_mapping: routes(&[("vr", "type/vr"), ("clt", "actor/clt")]),
        ..ArchiveConfig::default()
    }
}

/// `tests/test_merge_detect.py`'s `parts`.
fn parts(root: &Path, directory: &str, avid: &str, count: i32, library_dir: &str) {
    let brand = avid.split('-').next().expect("brand");
    for index in 1..=count {
        let path = root.join(directory).join(format!("{avid}-cd{index}.strm"));
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(path, format!("{LIBRARY}/{library_dir}/{brand}/{avid}-cd{index}.mp4\n")).expect("write strm");
    }
}

fn strm_files(directory: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            strm_files(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "strm") {
            found.push(path);
        }
    }
}

#[derive(Default)]
struct Listing {
    listed: Vec<String>,
    /// Parts deleted from 115 whose strm files are still in the mapping tree.
    gone: BTreeSet<String>,
    /// Files in 115 the mapping tree does not show yet, by directory.
    extra: BTreeMap<String, BTreeSet<String>>,
}

/// Lists every part of the mapping tree's titles at `PART` bytes each.
#[derive(Clone)]
struct StrmCloud {
    root: PathBuf,
    listing: Arc<Mutex<Listing>>,
}

impl StrmCloud {
    fn listing(&self) -> std::sync::MutexGuard<'_, Listing> {
        self.listing.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn unused<T>() -> BoxFuture<'static, T> {
    Box::pin(async { unreachable!("auto-merge only lists directories") })
}

impl MergeCloud for StrmCloud {
    fn list_directory<'a>(
        &'a self,
        api_dir: &'a str,
        _force_refresh: bool,
    ) -> BoxFuture<'a, Result<Vec<CloudFile>, CloudDriveError>> {
        Box::pin(async move {
            let mut files = Vec::new();
            strm_files(&self.root, &mut files);
            let mut names: BTreeSet<String> = files
                .iter()
                .map(|path| std::fs::read_to_string(path).expect("read strm").trim().to_owned())
                .filter(|target| embyx_merge::service::pure_parent(target) == format!("/mnt/cd2{api_dir}"))
                .map(|target| embyx_core::pypath::name(&target).to_owned())
                .collect();
            let mut listing = self.listing();
            listing.listed.push(api_dir.to_owned());
            names.extend(listing.extra.get(api_dir).cloned().unwrap_or_default());
            Ok(names
                .into_iter()
                .filter(|name| !listing.gone.contains(name))
                .map(|name| CloudFile {
                    id: String::new(),
                    full_path: format!("{api_dir}/{name}"),
                    name,
                    size: PART,
                    is_directory: false,
                    write_time: None,
                    hashes: BTreeMap::new(),
                })
                .collect())
        })
    }

    fn stat_file<'a>(&'a self, _api_path: &'a str) -> BoxFuture<'a, Result<Option<CloudFile>, CloudDriveError>> {
        unused()
    }

    fn copy_tasks(&self) -> BoxFuture<'_, Result<Vec<CopyTask>, CloudDriveError>> {
        unused()
    }

    fn copy_file<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<OperationResult, CloudDriveError>> {
        unused()
    }

    fn upload_state<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Option<UploadState>, CloudDriveError>> {
        unused()
    }

    fn restart_copy_task<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<(), CloudDriveError>> {
        unused()
    }

    fn cancel_copy_task<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<(), CloudDriveError>> {
        unused()
    }

    fn delete_files<'a>(&'a self, _: &'a [String]) -> BoxFuture<'a, Result<OperationResult, CloudDriveError>> {
        unused()
    }

    fn ensure_directory<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<EnsureResult, CloudDriveError>> {
        unused()
    }

    fn move_file<'a>(&'a self, _: &'a str, _: &'a str) -> BoxFuture<'a, Result<MoveResult, CloudDriveError>> {
        unused()
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn scoped(&self) -> Arc<dyn MergeCloud> {
        Arc::new(self.clone())
    }
}

struct Harness {
    test: Option<TestDatabase>,
    tmp: tempfile::TempDir,
    repository: MergeTaskRepository,
    cloud: StrmCloud,
    config: Arc<Mutex<MergeConfig>>,
    free_gib: Arc<Mutex<i128>>,
    merger: AutoMerger,
    service: MergeService,
}

impl Harness {
    async fn new(free_gib: i128, configure: impl FnOnce(&mut MergeConfig)) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let test = TestDatabase::create().await;
        let repository = MergeTaskRepository::new(test.database());
        let mut merge = MergeConfig {
            auto_enabled: true,
            ..MergeConfig::default()
        };
        configure(&mut merge);
        let config = Arc::new(Mutex::new(merge));
        let cloud = StrmCloud {
            root: tmp.path().to_path_buf(),
            listing: Arc::new(Mutex::new(Listing::default())),
        };
        let source = cloud.clone();
        let cloud_source: embyx_merge::service::CloudSource =
            Arc::new(move || Some(Arc::new(source.clone()) as Arc<dyn MergeCloud>));
        let shared = Arc::clone(&config);
        let merge_config: embyx_merge::service::Section<MergeConfig> =
            Arc::new(move || shared.lock().unwrap_or_else(PoisonError::into_inner).clone());
        let service = MergeService::new(ServiceParts::new(
            repository.clone(),
            Arc::clone(&cloud_source),
            Some(Arc::new(FakeKube::default()) as Arc<dyn MergeKube>),
            Arc::clone(&merge_config),
            Arc::new(archive),
            Arc::new(|| CloudDriveConfig {
                address: "cd:19798".to_owned(),
                api_token: "t".to_owned(),
                ..CloudDriveConfig::default()
            }),
            Arc::new(|_| Box::pin(async { Ok(()) })),
        ));
        let root = tmp.path().to_string_lossy().into_owned();
        let catalog = MergeCatalog {
            archive: Arc::new(archive),
            mapping: Arc::new(move || MappingConfig {
                src_dir: "/remote".to_owned(),
                dst_dir: root.clone(),
                ..MappingConfig::default()
            }),
            task_dirs_for: Arc::new(|_| Box::pin(async { Ok(std::collections::HashMap::new()) })),
        };
        let free = Arc::new(Mutex::new(free_gib));
        let space = Arc::clone(&free);
        let mut auto = AutoParts::new(
            repository.clone(),
            catalog,
            TitleScanCache::default(),
            service.clone(),
            merge_config,
            cloud_source,
        );
        auto.free_space = Arc::new(move |_path| Ok(*space.lock().unwrap_or_else(PoisonError::into_inner) * GIB));
        Self {
            test: Some(test),
            tmp,
            repository,
            cloud,
            config,
            free_gib: free,
            merger: AutoMerger::new(auto),
            service,
        }
    }

    fn root(&self) -> &Path {
        self.tmp.path()
    }

    async fn close(mut self) {
        self.repository.database().close().await;
        if let Some(test) = self.test.take() {
            test.drop_database().await;
        }
    }

    async fn step(&self) {
        self.merger.step().await.expect("step");
    }

    async fn tasks(&self) -> Vec<MergeTask> {
        let mut tasks = self.repository.listing().await.expect("listing");
        tasks.sort_by_key(|task| task.id);
        tasks
    }

    async fn avids(&self) -> Vec<String> {
        self.tasks().await.into_iter().map(|task| task.avid).collect()
    }

    /// `task(id, avid, state, ...)` appended to the fake repository: a row in that state.
    async fn add(&self, avid: &str, state: MergeState, failed_state: Option<MergeState>) -> MergeTask {
        let task = self
            .repository
            .create(&NewTask {
                avid: avid.to_owned(),
                source: "vr".to_owned(),
                library_dir: "type/vr".to_owned(),
                brand: avid.split('-').next().expect("brand").to_owned(),
                parts: vec!["/a-cd1.mp4".to_owned(), "/a-cd2.mp4".to_owned()],
                merged_name: format!("{avid}.mp4"),
            })
            .await
            .expect("create");
        self.set_state(&task, state, failed_state).await
    }

    /// `repository.tasks[i] = task(..., state, ...)`.
    async fn set_state(&self, task: &MergeTask, state: MergeState, failed_state: Option<MergeState>) -> MergeTask {
        let mut fields = vec![TaskField::FailedState(failed_state)];
        if state == MergeState::Done {
            fields.push(TaskField::FinishedAt(Some(Utc::now())));
        }
        self.repository
            .transition(task.id, task.state, state, fields)
            .await
            .expect("transition")
            .expect("moved")
    }
}

// py: tests/test_merge_auto.py::test_queues_the_title_with_most_parts_and_leaves_stacked_ones
#[tokio::test]
async fn queues_the_title_with_most_parts_and_leaves_stacked_ones() {
    let harness = Harness::new(100, |_| {}).await;
    parts(harness.root(), "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 20, "type/vr");
    parts(harness.root(), "type/vr/VRKM/VRKM-385", "VRKM-385", 12, "type/vr");
    parts(harness.root(), "rank/ABP/ABP-123", "ABP-123", 3, "rank");

    harness.step().await;

    let tasks = harness.tasks().await;
    assert_eq!(
        tasks.iter().map(|task| task.avid.as_str()).collect::<Vec<_>>(),
        vec!["SQTEVR-009"]
    );
    assert_eq!(
        tasks[0].parts[0],
        format!("{LIBRARY}/type/vr/SQTEVR/SQTEVR-009-cd1.mp4")
    );
    assert_eq!(harness.merger.status().state, AutoState::Busy);
    assert_eq!(harness.service.woken(), 1);
    harness.close().await;
}

// py: tests/test_merge_auto.py::test_waits_for_the_open_task_to_be_filed
#[tokio::test]
async fn waits_for_the_open_task_to_be_filed() {
    let harness = Harness::new(100, |_| {}).await;
    parts(harness.root(), "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 20, "type/vr");
    parts(harness.root(), "type/vr/VRKM/VRKM-385", "VRKM-385", 12, "type/vr");
    harness.step().await;

    let first = harness.tasks().await.remove(0);
    let first = harness.set_state(&first, MergeState::Archiving, None).await;
    harness.step().await;
    assert_eq!(harness.tasks().await.len(), 1);
    assert_eq!(harness.merger.status().state, AutoState::Busy);

    // Filed: its parts left the library, and the next title goes.
    let directory = harness.root().join("type/vr/SQTEVR/SQTEVR-009");
    for entry in std::fs::read_dir(&directory).expect("read_dir").flatten() {
        std::fs::remove_file(entry.path()).expect("unlink");
    }
    harness.set_state(&first, MergeState::Done, None).await;
    harness.step().await;
    assert_eq!(harness.avids().await, vec!["SQTEVR-009", "VRKM-385"]);
    harness.close().await;
}

// py: tests/test_merge_auto.py::test_skips_a_title_that_does_not_fit_and_takes_one_that_does
#[tokio::test]
async fn skips_a_title_that_does_not_fit_and_takes_one_that_does() {
    // 35 GiB free less the 20 GiB reserve leaves room for 15 parts.
    let harness = Harness::new(35, |_| {}).await;
    parts(harness.root(), "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 20, "type/vr");
    parts(harness.root(), "type/vr/VRKM/VRKM-385", "VRKM-385", 12, "type/vr");

    harness.step().await;

    assert_eq!(harness.avids().await, vec!["VRKM-385"]);
    let status = harness.merger.status();
    assert_eq!(
        (status.state, status.room, status.skipped_count),
        (AutoState::Busy, Some(15 * GIB), 1)
    );
    assert_eq!(
        (
            status.skipped[0].avid.as_str(),
            status.skipped[0].size,
            status.skipped[0].reason.as_str()
        ),
        ("SQTEVR-009", Some(20 * i128::from(PART)), "too_big")
    );
    harness.close().await;
}

// py: tests/test_merge_auto.py::test_stays_idle_when_nothing_fits
#[tokio::test]
async fn stays_idle_when_nothing_fits() {
    let harness = Harness::new(30, |_| {}).await;
    parts(harness.root(), "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 20, "type/vr");

    harness.step().await;

    assert!(harness.tasks().await.is_empty());
    let status = harness.merger.status();
    assert_eq!((status.state, status.skipped_count), (AutoState::Idle, 1));
    harness.close().await;
}

// py: tests/test_merge_auto.py::test_takes_stacked_titles_only_when_asked_and_never_a_removed_one
#[tokio::test]
async fn takes_stacked_titles_only_when_asked_and_never_a_removed_one() {
    let harness = Harness::new(100, |config| config.auto_include_stackable = true).await;
    parts(harness.root(), "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 20, "type/vr");
    parts(harness.root(), "rank/ABP/ABP-123", "ABP-123", 3, "rank");
    harness.add("SQTEVR-009", MergeState::Cancelled, None).await;

    harness.step().await;

    assert_eq!(harness.avids().await, vec!["SQTEVR-009", "ABP-123"]);
    harness.close().await;
}

// py: tests/test_merge_auto.py::test_remembers_titles_that_need_a_source_picked
#[tokio::test]
async fn remembers_titles_that_need_a_source_picked() {
    let harness = Harness::new(100, |_| {}).await;
    parts(harness.root(), "type/x/DEF/DEF-001", "DEF-001", 12, "type/x");

    harness.step().await;
    harness.step().await;

    assert!(harness.tasks().await.is_empty());
    let status = harness.merger.status();
    assert_eq!(
        (status.state, status.skipped[0].reason.as_str()),
        (AutoState::Idle, "merge_source_required")
    );
    harness.close().await;
}

// py: tests/test_merge_auto.py::test_does_nothing_while_switched_off
#[tokio::test]
async fn does_nothing_while_switched_off() {
    let harness = Harness::new(100, |_| {}).await;
    parts(harness.root(), "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 20, "type/vr");
    // The default config has it off.
    *harness.config.lock().unwrap_or_else(PoisonError::into_inner) = MergeConfig::default();

    harness.step().await;

    assert!(harness.tasks().await.is_empty());
    assert_eq!(harness.merger.status().state, AutoState::Off);
    harness.close().await;
}

// py: tests/test_merge_auto.py::test_skips_a_failed_title_and_takes_the_next
#[tokio::test]
async fn skips_a_failed_title_and_takes_the_next() {
    let harness = Harness::new(100, |_| {}).await;
    parts(harness.root(), "type/vr/VRKM/VRKM-01468", "VRKM-01468", 40, "type/vr");
    parts(harness.root(), "type/vr/VRKM/VRKM-385", "VRKM-385", 12, "type/vr");
    harness
        .add("VRKM-01468", MergeState::Failed, Some(MergeState::Merging))
        .await;

    harness.step().await;

    assert_eq!(harness.avids().await, vec!["VRKM-01468", "VRKM-385"]);
    let status = harness.merger.status();
    assert_eq!(status.state, AutoState::Busy);
    assert_eq!(
        status
            .skipped
            .iter()
            .map(|item| (item.avid.as_str(), item.reason.as_str()))
            .collect::<Vec<_>>(),
        vec![("VRKM-01468", "failed")]
    );
    harness.close().await;
}

// py: tests/test_merge_auto.py::test_pauses_on_a_failed_task_when_asked
#[tokio::test]
async fn pauses_on_a_failed_task_when_asked() {
    let harness = Harness::new(100, |config| config.auto_pause_on_failure = true).await;
    parts(harness.root(), "type/vr/VRKM/VRKM-385", "VRKM-385", 12, "type/vr");
    harness
        .add("SQTEVR-009", MergeState::Failed, Some(MergeState::Uploading))
        .await;

    harness.step().await;

    assert_eq!(harness.tasks().await.len(), 1);
    assert_eq!(harness.merger.status().state, AutoState::Paused);
    harness.close().await;
}

// py: tests/test_merge_auto.py::test_never_merges_a_merged_title_again_while_its_strm_files_linger
#[tokio::test]
async fn never_merges_a_merged_title_again_while_its_strm_files_linger() {
    let harness = Harness::new(100, |_| {}).await;
    parts(harness.root(), "type/vr/EBVR/EBVR-997", "EBVR-997", 22, "type/vr");
    parts(harness.root(), "type/vr/VRKM/VRKM-385", "VRKM-385", 12, "type/vr");
    harness.step().await;
    assert_eq!(harness.avids().await, vec!["EBVR-997"]);

    // Filed, but mapping has not removed the old -cdN strm files yet.
    let first = harness.tasks().await.remove(0);
    harness.set_state(&first, MergeState::Done, None).await;
    harness.step().await;

    assert_eq!(harness.avids().await, vec!["EBVR-997", "VRKM-385"]);
    harness.close().await;
}

// py: tests/test_merge_auto.py::test_skips_a_title_whose_parts_left_115_since_they_were_sized
#[tokio::test]
async fn skips_a_title_whose_parts_left_115_since_they_were_sized() {
    // 15 GiB of room: sized, but too big for now.
    let harness = Harness::new(35, |_| {}).await;
    parts(harness.root(), "type/vr/SQTEVR/SQTEVR-009", "SQTEVR-009", 20, "type/vr");
    harness.step().await;
    assert!(harness.tasks().await.is_empty());

    harness.cloud.listing().gone.insert("SQTEVR-009-cd3.mp4".to_owned());
    *harness.free_gib.lock().unwrap_or_else(PoisonError::into_inner) = 100;
    harness.step().await;

    assert!(harness.tasks().await.is_empty());
    assert_eq!(
        harness
            .merger
            .status()
            .skipped
            .iter()
            .map(|item| (item.avid.clone(), item.reason.clone()))
            .collect::<Vec<_>>(),
        vec![("SQTEVR-009".to_owned(), "parts_missing".to_owned())]
    );
    harness.close().await;
}

// py: tests/test_merge_auto.py::test_skips_a_title_the_library_already_holds_whole
#[tokio::test]
async fn skips_a_title_the_library_already_holds_whole() {
    let harness = Harness::new(100, |_| {}).await;
    parts(harness.root(), "type/vr/SAVR/SAVR-1135", "SAVR-1135", 14, "type/vr");
    parts(harness.root(), "type/vr/VRKM/VRKM-385", "VRKM-385", 12, "type/vr");
    // Filed by a merge of the same title under its padded ID; mapping has not caught up.
    harness.cloud.listing().extra.insert(
        "/115/embyx/type/vr/SAVR".to_owned(),
        ["SAVR-01135.mp4".to_owned()].into_iter().collect(),
    );

    harness.step().await;

    assert_eq!(harness.avids().await, vec!["VRKM-385"]);
    assert_eq!(
        harness
            .merger
            .status()
            .skipped
            .iter()
            .map(|item| (item.avid.clone(), item.reason.clone()))
            .collect::<Vec<_>>(),
        vec![("SAVR-1135".to_owned(), "whole_copy".to_owned())]
    );
    assert!(!harness.cloud.listing().listed.is_empty());
    harness.close().await;
}
