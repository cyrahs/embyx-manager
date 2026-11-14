//! Merge tasks walked through their states against an in-memory CloudDrive and Job API.
//!
//! Ported from `tests/test_merge_service.py` (the `merge.kube` tests of that file are WP3's,
//! in `embyx-clients`). The tasks live in a disposable PostgreSQL database, as Python's
//! `make_database()` gives them.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use chrono::{Duration, TimeZone as _, Utc};
use embyx_clients::clouddrive::CopyStatus;
use embyx_core::config::models::{ArchiveConfig, CloudDriveConfig, MergeConfig};
use embyx_core::pyjson::Dict;
use embyx_db::testdb::TestDatabase;
use embyx_merge::fakes::{FakeCloud, FakeKube};
use embyx_merge::ports::{MergeCloud, MergeKube, Pipeline, Trigger};
use embyx_merge::service::{ActionError, MergeService, ServiceParts, api_path};
use embyx_merge::tasks::{MergeState, MergeTask, MergeTaskRepository, NewTask, TaskField};

const LIBRARY: &str = "/mnt/cd2/115/embyx";
const SHA1: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const WHOLE: &str = "/115/embyx/type/vr/SQTEVR/SQTEVR-0009.mp4";

fn parts() -> Vec<String> {
    (1..=3)
        .map(|index| format!("{LIBRARY}/type/vr/SQTEVR/SQTEVR-009-cd{index}.mp4"))
        .collect()
}

fn archive() -> ArchiveConfig {
    ArchiveConfig {
        src_dir: "/mnt/cd2/115/embyx_in".to_owned(),
        dst_dir: LIBRARY.to_owned(),
        mapping: vec![("rank".to_owned(), "rank".to_owned())],
        priority_mapping: vec![("vr".to_owned(), "type/vr".to_owned())],
        ..ArchiveConfig::default()
    }
}

fn clouddrive() -> CloudDriveConfig {
    CloudDriveConfig {
        address: "cd:19798".to_owned(),
        api_token: "t".to_owned(),
        ..CloudDriveConfig::default()
    }
}

fn mount(part: &str) -> String {
    api_path(part, "/mnt/cd2").expect("under the mount")
}

struct Harness {
    test: Option<TestDatabase>,
    _tmp: tempfile::TempDir,
    work: PathBuf,
    cloud: FakeCloud,
    kube: FakeKube,
    triggered: Arc<Mutex<Vec<Pipeline>>>,
    archive: Arc<Mutex<ArchiveConfig>>,
    repository: MergeTaskRepository,
    service: MergeService,
}

impl Harness {
    async fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let work = tmp.path().join("upload");
        std::fs::create_dir(&work).expect("mkdir");
        let cloud = FakeCloud::new(&["/", "/115", "/115/upload", "/115/embyx_in", "/115/embyx/type/vr/SQTEVR"]);
        for part in parts() {
            cloud.add_file(&mount(&part), 100, "");
        }
        let kube = FakeKube::default();
        let triggered = Arc::new(Mutex::new(Vec::new()));
        let test = TestDatabase::create().await;
        let repository = MergeTaskRepository::new(test.database());
        let config = MergeConfig {
            work_dir: work.to_string_lossy().into_owned(),
            ..MergeConfig::default()
        };
        let archive_config = Arc::new(Mutex::new(archive()));
        let seen = Arc::clone(&triggered);
        let trigger: Trigger = Arc::new(move |pipeline| {
            seen.lock().unwrap_or_else(PoisonError::into_inner).push(pipeline);
            Box::pin(async {})
        });
        let source = cloud.clone();
        let shared_archive = Arc::clone(&archive_config);
        let service = MergeService::new(ServiceParts::new(
            repository.clone(),
            Arc::new(move || Some(Arc::new(source.clone()) as Arc<dyn MergeCloud>)),
            Some(Arc::new(kube.clone()) as Arc<dyn MergeKube>),
            Arc::new(move || config.clone()),
            Arc::new(move || shared_archive.lock().unwrap_or_else(PoisonError::into_inner).clone()),
            Arc::new(clouddrive),
            trigger,
        ));
        Self {
            test: Some(test),
            _tmp: tmp,
            work,
            cloud,
            kube,
            triggered,
            archive: archive_config,
            repository,
            service,
        }
    }

    async fn close(mut self) {
        self.repository.database().close().await;
        if let Some(test) = self.test.take() {
            test.drop_database().await;
        }
    }

    fn pool(&self) -> &sqlx::PgPool {
        self.test.as_ref().expect("open").pool()
    }

    async fn create(&self) -> MergeTask {
        self.create_title("SQTEVR-009", "vr", "type/vr").await
    }

    async fn create_title(&self, avid: &str, source: &str, library_dir: &str) -> MergeTask {
        self.repository
            .create(&NewTask {
                avid: avid.to_owned(),
                source: source.to_owned(),
                library_dir: library_dir.to_owned(),
                brand: avid.split('-').next().expect("brand").to_owned(),
                parts: parts(),
                merged_name: format!("{avid}.mp4"),
            })
            .await
            .expect("create")
    }

    async fn state(&self, task: &MergeTask) -> MergeTask {
        self.repository.get(task.id).await.expect("get").expect("the task")
    }

    async fn tick(&self) {
        self.service.tick().await.expect("tick");
    }

    async fn ticks(&self, count: usize) {
        for _ in 0..count {
            self.tick().await;
        }
    }

    fn output(&self) -> PathBuf {
        self.work.join("SQTEVR-009.mp4")
    }

    fn status_path(&self, task: &MergeTask) -> PathBuf {
        self.work.join(".merge").join(format!("{}.json", task.id))
    }

    fn write_status(&self, task: &MergeTask, text: &str) {
        let status = self.status_path(task);
        std::fs::create_dir_all(status.parent().expect("parent")).expect("mkdir");
        std::fs::write(status, text).expect("write status");
    }

    fn finish_merge(&self, task: &MergeTask) {
        std::fs::write(self.output(), vec![b'x'; 300]).expect("write merged file");
        self.write_status(
            task,
            &format!(r#"{{"state": "done", "size": 300, "sha1": "{}"}}"#, SHA1.to_lowercase()),
        );
    }

    /// Date the task's last update `by` earlier than now.
    async fn age(&self, task: &MergeTask, by: Duration) {
        sqlx::query("UPDATE merge_tasks SET updated_at = $2 WHERE id = $1")
            .bind(task.id)
            .bind(Utc::now() - by)
            .execute(self.pool())
            .await
            .expect("age");
    }

    /// Date both the last update and the last state change `by` earlier than now.
    async fn age_state(&self, task: &MergeTask, by: Duration) {
        sqlx::query("UPDATE merge_tasks SET state_changed_at = $2, updated_at = $2 WHERE id = $1")
            .bind(task.id)
            .bind(Utc::now() - by)
            .execute(self.pool())
            .await
            .expect("age");
    }

    fn staging(task: &MergeTask) -> String {
        format!("/115/upload/merge-{}", task.id)
    }

    fn complete_copy(&self, task: &MergeTask, sha1: &str) {
        for copy in &mut self.cloud.state().copies {
            copy.status = CopyStatus::Completed;
        }
        self.cloud
            .add_file(&format!("{}/SQTEVR-009.mp4", Self::staging(task)), 300, sha1);
    }

    fn triggered(&self) -> Vec<Pipeline> {
        self.triggered.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    fn job_name(task: &MergeTask) -> String {
        task.job_name.clone().unwrap_or_default()
    }

    /// Walk the task through the states up to `last` without the service.
    async fn walk(&self, task: MergeTask, states: &[MergeState]) -> MergeTask {
        let mut task = task;
        for state in states {
            task = self
                .repository
                .transition(task.id, task.state, *state, Vec::new())
                .await
                .expect("transition")
                .expect("moved");
        }
        task
    }

    /// Merge the task and start its upload: a tick to start the Job, its report, two ticks.
    async fn merged_and_copying(&self) -> MergeTask {
        let task = self.create().await;
        self.tick().await;
        self.finish_merge(&self.state(&task).await);
        self.ticks(2).await;
        self.state(&task).await
    }
}

fn refused(outcome: Result<MergeTask, ActionError>) -> &'static str {
    match outcome {
        Err(ActionError::Refused(code)) => code,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

// py: tests/test_merge_service.py::test_a_title_walks_from_the_queue_into_the_intake_route
#[tokio::test]
async fn a_title_walks_from_the_queue_into_the_intake_route() {
    let harness = Harness::new().await;
    let task = harness.create().await;

    harness.tick().await;
    let task = harness.state(&task).await;
    assert_eq!(task.state, MergeState::Merging);
    let created = harness.kube.state().created.clone();
    assert_eq!(created.len(), 1);
    let (name, args) = &created[0];
    assert_eq!(Some(name.as_str()), task.job_name.as_deref());
    assert_eq!(name, &format!("embyx-merge-{}-sqtevr-009", task.id));
    let mut expected = vec![
        "--output".to_owned(),
        harness.output().to_string_lossy().into_owned(),
        "--status".to_owned(),
        harness.status_path(&task).to_string_lossy().into_owned(),
        "--reserve-gib".to_owned(),
        "20".to_owned(),
        "--".to_owned(),
    ];
    expected.extend(parts());
    assert_eq!(args, &expected);

    harness.finish_merge(&task);
    harness.tick().await;
    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.merged_bytes, task.merged_sha1.as_deref()),
        (MergeState::Uploading, Some(300), Some(SHA1))
    );
    assert_eq!(harness.kube.state().deleted, vec![name.clone()]);

    harness.tick().await;
    let copies: Vec<(String, String)> = harness
        .cloud
        .state()
        .copies
        .iter()
        .map(|copy| (copy.source_path.clone(), copy.dest_path.clone()))
        .collect();
    assert_eq!(
        copies,
        vec![("/downloads/upload/SQTEVR-009.mp4".to_owned(), Harness::staging(&task))]
    );
    assert_eq!(harness.state(&task).await.upload_attempts, 1);

    harness.complete_copy(&task, SHA1);
    harness.tick().await;
    assert_eq!(harness.state(&task).await.state, MergeState::Verifying);
    harness.tick().await;
    assert_eq!(harness.state(&task).await.state, MergeState::Replacing);

    harness.tick().await;
    let task = harness.state(&task).await;
    assert_eq!(task.state, MergeState::Archiving);
    assert!(
        !harness
            .cloud
            .state()
            .files
            .keys()
            .any(|path| path.starts_with("/115/embyx/"))
    );
    assert!(harness.cloud.has_file("/115/embyx_in/vr/SQTEVR-009/SQTEVR-009.mp4"));
    assert!(harness.cloud.state().deleted.contains(&Harness::staging(&task)));
    assert!(!harness.output().exists());
    assert_eq!(harness.triggered(), vec![Pipeline::Archive]);

    harness.tick().await;
    assert_eq!(harness.state(&task).await.state, MergeState::Archiving);
    {
        let mut cloud = harness.cloud.state();
        cloud.files.remove("/115/embyx_in/vr/SQTEVR-009/SQTEVR-009.mp4");
        cloud.directories.remove("/115/embyx_in/vr/SQTEVR-009");
    }
    harness.tick().await;
    let task = harness.state(&task).await;
    assert_eq!(task.state, MergeState::Done);
    assert!(task.finished_at.is_some());
    assert_eq!(harness.triggered(), vec![Pipeline::Archive, Pipeline::Mapping]);
    harness.close().await;
}

// py: tests/test_merge_service.py::test_only_one_title_merges_at_a_time
#[tokio::test]
async fn only_one_title_merges_at_a_time() {
    let harness = Harness::new().await;
    let first = harness.create().await;
    let second = harness.create_title("ABP-123", "rank", "rank").await;

    harness.ticks(2).await;

    assert_eq!(harness.state(&first).await.state, MergeState::Merging);
    assert_eq!(harness.state(&second).await.state, MergeState::Queued);
    assert_eq!(harness.kube.state().created.len(), 1);
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_failed_job_fails_the_task_with_its_log
#[tokio::test]
async fn a_failed_job_fails_the_task_with_its_log() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.tick().await;
    let task = harness.state(&task).await;
    harness.kube.set_condition(&Harness::job_name(&task), "Failed", None);

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Merging))
    );
    assert_eq!(task.error.as_deref(), Some("the merge Job failed: ffmpeg: boom"));
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_refused_merge_fails_the_task_though_its_job_completed
#[tokio::test]
async fn a_refused_merge_fails_the_task_though_its_job_completed() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.tick().await;
    let task = harness.state(&task).await;
    harness.write_status(
        &task,
        r#"{"state": "failed", "refused": true, "error": "cd2 carries different streams"}"#,
    );
    harness.kube.set_condition(&Harness::job_name(&task), "Complete", None);

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Merging))
    );
    assert_eq!(task.error.as_deref(), Some("cd2 carries different streams"));
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_job_that_completed_without_a_report_fails_the_task
#[tokio::test]
async fn a_job_that_completed_without_a_report_fails_the_task() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.tick().await;
    let task = harness.state(&task).await;
    harness.kube.set_condition(&Harness::job_name(&task), "Complete", None);

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Merging))
    );
    assert_eq!(
        task.error.as_deref(),
        Some("the merge Job ended without reporting a result")
    );
    harness.close().await;
}

fn names(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .expect("read_dir")
        .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

// py: tests/test_merge_service.py::test_a_killed_job_leaves_no_half_written_merge_behind
#[tokio::test]
async fn a_killed_job_leaves_no_half_written_merge_behind() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.tick().await;
    let task = harness.state(&task).await;
    // An OOM kill gives the worker no chance to remove these itself.
    std::fs::write(harness.work.join(".SQTEVR-009.mp4.part"), b"x").expect("write");
    std::fs::create_dir(harness.work.join(".SQTEVR-009.mp4.parts")).expect("mkdir");
    std::fs::write(
        harness.work.join(".SQTEVR-009.mp4.parts").join("SQTEVR-009-cd1.mp4"),
        b"x",
    )
    .expect("write");
    harness.kube.set_condition(&Harness::job_name(&task), "Failed", None);

    harness.tick().await;

    assert_eq!(harness.state(&task).await.state, MergeState::Failed);
    assert_eq!(names(&harness.work), Vec::<String>::new());
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_retried_merge_starts_a_fresh_job_under_the_same_name
#[tokio::test]
async fn a_retried_merge_starts_a_fresh_job_under_the_same_name() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.tick().await;
    let task = harness.state(&task).await;
    let name = Harness::job_name(&task);
    harness.kube.set_condition(&name, "Failed", None);
    harness.tick().await;

    harness.service.retry(task.id).await.expect("retry");
    harness.ticks(2).await;

    let task = harness.state(&task).await;
    assert_eq!(task.state, MergeState::Merging);
    assert_eq!(harness.kube.state().deleted, vec![name.clone()]);
    assert_eq!(harness.kube.state().jobs.get(&name), Some(&Dict::new()));
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_job_past_its_deadline_fails_the_task_with_the_reason
#[tokio::test]
async fn a_job_past_its_deadline_fails_the_task_with_the_reason() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.tick().await;
    let task = harness.state(&task).await;
    harness.kube.set_condition(
        &Harness::job_name(&task),
        "Failed",
        Some("Job was active longer than specified deadline"),
    );

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(
        task.error.as_deref(),
        Some("the merge Job failed: Job was active longer than specified deadline | ffmpeg: boom")
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_merged_file_lost_before_the_upload_finished_is_merged_again
#[tokio::test]
async fn a_merged_file_lost_before_the_upload_finished_is_merged_again() {
    let harness = Harness::new().await;
    let task = harness.merged_and_copying().await;

    std::fs::remove_file(harness.output()).expect("unlink");
    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(task.state, MergeState::Queued);
    assert_eq!(
        task.notice.as_deref(),
        Some("the merged file was lost before the upload finished; merging again")
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_copy_cloud_drive_stopped_listing_is_not_uploaded_again
#[tokio::test]
async fn a_copy_cloud_drive_stopped_listing_is_not_uploaded_again() {
    let harness = Harness::new().await;
    let task = harness.merged_and_copying().await;

    harness.cloud.state().copies.clear();
    harness.tick().await;
    assert!(harness.cloud.state().copies.is_empty());
    assert_eq!(harness.state(&task).await.state, MergeState::Uploading);

    harness
        .cloud
        .add_file(&format!("{}/SQTEVR-009.mp4", Harness::staging(&task)), 300, SHA1);
    harness.tick().await;
    assert_eq!(harness.state(&task).await.state, MergeState::Verifying);
    assert!(harness.cloud.state().copies.is_empty());
    harness.close().await;
}

// py: tests/test_merge_service.py::test_upload_progress_comes_from_the_upload_queue_while_the_copy_task_counts_nothing
#[tokio::test]
async fn upload_progress_comes_from_the_upload_queue_while_the_copy_task_counts_nothing() {
    let harness = Harness::new().await;
    let task = harness.merged_and_copying().await;
    // CloudDrive keeps the copy task at "Scanned" with 0 bytes until the file is up.
    harness.cloud.state().copies[0].status = CopyStatus::Scanned;
    harness
        .cloud
        .state()
        .sent
        .insert(format!("{}/SQTEVR-009.mp4", Harness::staging(&task)), 240);

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.uploaded_bytes, task.notice),
        (MergeState::Uploading, Some(240), None)
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_failed_copy_is_restarted_then_given_up_on
#[tokio::test]
async fn a_failed_copy_is_restarted_then_given_up_on() {
    let harness = Harness::new().await;
    let task = harness.merged_and_copying().await;
    {
        let mut cloud = harness.cloud.state();
        cloud.copies[0].status = CopyStatus::Failed;
        cloud.copies[0].errors = vec!["115 refused".to_owned()];
    }

    // The first failure rests five minutes before the copy is restarted.
    harness.ticks(2).await;
    assert!(harness.cloud.state().restarted.is_empty());
    assert_eq!(
        harness.state(&task).await.notice.as_deref(),
        Some("upload attempt 2 of 5 starts in 5 min: 115 refused")
    );

    harness.age(&task, Duration::minutes(5)).await;
    harness.tick().await;
    assert_eq!(
        harness.cloud.state().restarted,
        vec![("/downloads/upload/SQTEVR-009.mp4".to_owned(), Harness::staging(&task))]
    );
    let task = harness.state(&task).await;
    assert_eq!((task.upload_attempts, task.notice.as_deref()), (2, Some("115 refused")));

    harness
        .repository
        .update(task.id, vec![TaskField::UploadAttempts(5)])
        .await
        .expect("update");
    harness.tick().await;
    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Uploading))
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_an_upload_clouddrive_gave_up_on_fails_the_task
#[tokio::test]
async fn an_upload_clouddrive_gave_up_on_fails_the_task() {
    let harness = Harness::new().await;
    let task = harness.merged_and_copying().await;
    // The copy task itself still reads "Scanned"; only the upload queue knows.
    harness.cloud.state().copies[0].status = CopyStatus::Scanned;
    harness.cloud.state().fatal.insert(
        format!("{}/SQTEVR-009.mp4", Harness::staging(&task)),
        "code: 10025, message: 单文件不能大于115GB".to_owned(),
    );

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Uploading))
    );
    assert_eq!(
        task.error.as_deref(),
        Some("CloudDrive gave up uploading SQTEVR-009.mp4: code: 10025, message: 单文件不能大于115GB")
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_an_upload_lost_to_a_network_error_is_restarted_then_given_up_on
#[tokio::test]
async fn an_upload_lost_to_a_network_error_is_restarted_then_given_up_on() {
    const ERROR: &str = "Reqwest error error sending request";
    let harness = Harness::new().await;
    let task = harness.merged_and_copying().await;
    harness.cloud.state().copies[0].status = CopyStatus::Scanned;
    harness
        .cloud
        .state()
        .fatal
        .insert(format!("{}/SQTEVR-009.mp4", Harness::staging(&task)), ERROR.to_owned());

    harness.ticks(2).await;

    // Each failure rests longer before the next attempt: 5 min, 15 min, 1 h, 3 h.
    assert!(harness.cloud.state().restarted.is_empty());
    let task = harness.state(&task).await;
    assert_eq!(task.state, MergeState::Uploading);
    assert_eq!(
        task.notice,
        Some(format!("upload attempt 2 of 5 starts in 5 min: {ERROR}"))
    );
    harness.age(&task, Duration::minutes(4)).await;
    harness.tick().await;
    assert!(harness.cloud.state().restarted.is_empty());

    harness.age(&task, Duration::minutes(5)).await;
    harness.ticks(2).await;
    // Restarted once, then left alone while CloudDrive queues it again.
    assert_eq!(
        harness.cloud.state().restarted,
        vec![("/downloads/upload/SQTEVR-009.mp4".to_owned(), Harness::staging(&task))]
    );
    let task = harness.state(&task).await;
    assert_eq!((task.upload_attempts, task.notice.as_deref()), (2, Some(ERROR)));

    // Still failing once CloudDrive had its moment: the next attempt rests 15 minutes.
    harness.age(&task, Duration::minutes(2)).await;
    harness.tick().await;
    let task = harness.state(&task).await;
    assert_eq!(
        task.notice,
        Some(format!("upload attempt 3 of 5 starts in 15 min: {ERROR}"))
    );
    harness.age(&task, Duration::minutes(14)).await;
    harness.tick().await;
    assert_eq!(harness.cloud.state().restarted.len(), 1);
    harness.age(&task, Duration::minutes(15)).await;
    harness.tick().await;
    assert_eq!(harness.cloud.state().restarted.len(), 2);

    harness
        .repository
        .update(task.id, vec![TaskField::UploadAttempts(4), TaskField::Notice(None)])
        .await
        .expect("update");
    harness.tick().await;
    assert_eq!(
        harness.state(&task).await.notice,
        Some(format!("upload attempt 5 of 5 starts in 3 h: {ERROR}"))
    );

    harness
        .repository
        .update(task.id, vec![TaskField::UploadAttempts(5)])
        .await
        .expect("update");
    harness.tick().await;
    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Uploading))
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_sha1_mismatch_fails_before_any_original_is_touched
#[tokio::test]
async fn a_sha1_mismatch_fails_before_any_original_is_touched() {
    let harness = Harness::new().await;
    let task = harness.merged_and_copying().await;
    harness.complete_copy(&task, &"B".repeat(40));
    harness.ticks(2).await;

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Verifying))
    );
    assert!(task.error.as_deref().unwrap_or_default().contains("SHA-1"));
    assert!(parts().iter().all(|part| harness.cloud.has_file(&mount(part))));

    let retried = harness.service.retry(task.id).await.expect("retry");
    assert_eq!(retried.state, MergeState::Uploading);
    harness.close().await;
}

// py: tests/test_merge_service.py::test_verifying_waits_for_115_to_report_the_hash
#[tokio::test]
async fn verifying_waits_for_115_to_report_the_hash() {
    let harness = Harness::new().await;
    let task = harness.merged_and_copying().await;
    harness.complete_copy(&task, "");
    harness.tick().await;

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(task.state, MergeState::Verifying);
    assert_eq!(task.notice.as_deref(), Some("waiting for 115 to report the SHA-1"));
    harness.close().await;
}

// py: tests/test_merge_service.py::test_cancelling_a_merge_removes_its_job_and_files
#[tokio::test]
async fn cancelling_a_merge_removes_its_job_and_files() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.tick().await;
    let task = harness.state(&task).await;
    std::fs::write(harness.work.join(".SQTEVR-009.mp4.part"), b"partial").expect("write");

    let cancelled = harness.service.cancel(task.id).await.expect("cancel");

    assert_eq!(cancelled.state, MergeState::Cancelled);
    assert_eq!(harness.kube.state().deleted, vec![Harness::job_name(&task)]);
    assert!(!harness.work.join(".SQTEVR-009.mp4.part").exists());
    assert_eq!(
        refused(harness.service.cancel(task.id).await),
        "merge_task_not_cancellable"
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_task_that_failed_after_the_originals_went_cannot_be_dismissed
#[tokio::test]
async fn a_task_that_failed_after_the_originals_went_cannot_be_dismissed() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    let task = harness
        .walk(
            task,
            &[
                MergeState::Merging,
                MergeState::Uploading,
                MergeState::Verifying,
                MergeState::Replacing,
            ],
        )
        .await;
    harness
        .repository
        .transition(
            task.id,
            MergeState::Replacing,
            MergeState::Failed,
            vec![TaskField::FailedState(Some(MergeState::Replacing))],
        )
        .await
        .expect("fail");

    assert_eq!(
        refused(harness.service.cancel(task.id).await),
        "merge_task_not_cancellable"
    );
    assert_eq!(
        harness.service.retry(task.id).await.expect("retry").state,
        MergeState::Replacing
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_title_the_library_holds_whole_is_not_merged
#[tokio::test]
async fn a_title_the_library_holds_whole_is_not_merged() {
    let harness = Harness::new().await;
    harness.cloud.add_file(WHOLE, 900, "");
    let task = harness.create().await;

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Queued))
    );
    assert_eq!(
        task.error.as_deref(),
        Some(
            "the library already holds SQTEVR-0009.mp4, a whole copy of SQTEVR-009; \
             merging its parts would only duplicate it"
        )
    );
    assert!(harness.kube.state().created.is_empty());
    assert_eq!(
        harness.service.cancel(task.id).await.expect("cancel").state,
        MergeState::Cancelled
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_a_whole_copy_filed_during_the_upload_keeps_the_parts
#[tokio::test]
async fn a_whole_copy_filed_during_the_upload_keeps_the_parts() {
    let harness = Harness::new().await;
    let task = harness.merged_and_copying().await;
    harness.complete_copy(&task, SHA1);
    harness.tick().await;
    harness.cloud.add_file(WHOLE, 900, "");

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Verifying))
    );
    assert!(parts().iter().all(|part| harness.cloud.has_file(&mount(part))));
    assert_eq!(
        harness.service.cancel(task.id).await.expect("cancel").state,
        MergeState::Cancelled
    );
    assert!(harness.cloud.state().deleted.contains(&Harness::staging(&task)));
    harness.close().await;
}

const ALL_THE_WAY: [MergeState; 5] = [
    MergeState::Merging,
    MergeState::Uploading,
    MergeState::Verifying,
    MergeState::Replacing,
    MergeState::Archiving,
];

/// `archiving_too_long`: a task whose merged file has sat unfiled in the intake route past
/// the wait.
async fn archiving_too_long(harness: &Harness) -> MergeTask {
    let task = harness.create().await;
    let task = harness.walk(task, &ALL_THE_WAY).await;
    {
        let mut cloud = harness.cloud.state();
        for part in parts() {
            cloud.files.remove(&mount(&part));
        }
        cloud.directories.insert("/115/embyx_in/vr".to_owned());
        cloud.directories.insert("/115/embyx_in/vr/SQTEVR-009".to_owned());
    }
    harness
        .cloud
        .add_file("/115/embyx_in/vr/SQTEVR-009/SQTEVR-009.mp4", 300, SHA1);
    harness.age_state(&task, Duration::hours(3)).await;
    task
}

// py: tests/test_merge_service.py::test_a_merged_copy_the_archive_refuses_for_a_whole_copy_can_be_removed
#[tokio::test]
async fn a_merged_copy_the_archive_refuses_for_a_whole_copy_can_be_removed() {
    let harness = Harness::new().await;
    let task = archiving_too_long(&harness).await;
    harness.cloud.add_file(WHOLE, 900, "");

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Archiving))
    );
    assert_eq!(
        task.error.as_deref(),
        Some(
            "the library already holds SQTEVR-0009.mp4, so the archive leaves /115/embyx_in/vr/SQTEVR-009 alone; \
             remove the task to delete the merged copy"
        )
    );
    let cancelled = harness.service.cancel(task.id).await.expect("cancel");
    assert_eq!(cancelled.state, MergeState::Cancelled);
    assert_eq!(
        harness.cloud.state().deleted,
        vec!["/115/embyx_in/vr/SQTEVR-009".to_owned()]
    );
    assert!(harness.cloud.has_file(WHOLE));
    harness.close().await;
}

// py: tests/test_merge_service.py::test_the_only_merged_copy_cannot_be_removed
#[tokio::test]
async fn the_only_merged_copy_cannot_be_removed() {
    let harness = Harness::new().await;
    let task = archiving_too_long(&harness).await;

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(
        task.error.as_deref(),
        Some("the archive has not filed /115/embyx_in/vr/SQTEVR-009; check the archive run log")
    );
    assert_eq!(
        refused(harness.service.cancel(task.id).await),
        "merge_task_not_cancellable"
    );
    assert!(harness.cloud.state().deleted.is_empty());
    assert_eq!(
        harness.service.retry(task.id).await.expect("retry").state,
        MergeState::Archiving
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_unavailable_without_a_job_template
#[tokio::test]
async fn unavailable_without_a_job_template() {
    let harness = Harness::new().await;
    let service = MergeService::new(ServiceParts::new(
        harness.repository.clone(),
        Arc::new(|| None),
        None,
        Arc::new(MergeConfig::default),
        Arc::new(archive),
        Arc::new(clouddrive),
        Arc::new(|_| Box::pin(async {})),
    ));
    assert_eq!(
        service.unavailable().as_deref(),
        Some("the merge Job template is not mounted (EMBYX_MANAGER_MERGE_JOB_TEMPLATE)")
    );
    harness.archive.lock().unwrap_or_else(PoisonError::into_inner).src_dir = "/elsewhere/embyx_in".to_owned();
    assert_eq!(
        harness.service.unavailable().as_deref(),
        Some("archive.src_dir must lie under merge.cloud_mount_prefix")
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_archiving_gives_up_after_the_archive_never_files_the_title
#[tokio::test]
async fn archiving_gives_up_after_the_archive_never_files_the_title() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    let task = harness.walk(task, &ALL_THE_WAY).await;
    {
        let mut cloud = harness.cloud.state();
        cloud.directories.insert("/115/embyx_in/vr".to_owned());
        cloud.directories.insert("/115/embyx_in/vr/SQTEVR-009".to_owned());
    }
    harness.age_state(&task, Duration::hours(3)).await;

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(
        (task.state, task.failed_state),
        (MergeState::Failed, Some(MergeState::Archiving))
    );
    harness.close().await;
}

// py: tests/test_merge_service.py::test_the_repository_remembers_removed_titles_and_the_last_filed_one
#[tokio::test]
async fn the_repository_remembers_removed_titles_and_the_last_filed_one() {
    let harness = Harness::new().await;
    let removed = harness.create().await;
    assert_eq!(harness.repository.last_done_at().await.expect("read"), None);
    harness.service.cancel(removed.id).await.expect("cancel");
    let filed = harness.create().await;
    let finished_at = Utc.with_ymd_and_hms(2026, 10, 5, 19, 0, 0).single().expect("a time");
    harness
        .repository
        .transition(
            filed.id,
            MergeState::Queued,
            MergeState::Done,
            vec![TaskField::FinishedAt(Some(finished_at))],
        )
        .await
        .expect("done");

    assert_eq!(
        harness.repository.cancelled_avids().await.expect("read"),
        ["SQTEVR-009".to_owned()].into_iter().collect()
    );
    assert_eq!(
        harness.repository.last_done_at().await.expect("read"),
        Some(finished_at)
    );
    harness.close().await;
}

// A status file whose phase is not text is refused as asyncpg refuses the argument; the task
// keeps the notice and the step is tried again on the next tick.
#[tokio::test]
async fn a_status_file_with_a_non_text_phase_leaves_a_notice() {
    let harness = Harness::new().await;
    let task = harness.create().await;
    harness.tick().await;
    let task = harness.state(&task).await;
    harness.write_status(&task, r#"{"state": "running", "phase": 5, "progress": 0.5}"#);

    harness.tick().await;

    let task = harness.state(&task).await;
    assert_eq!(task.state, MergeState::Merging);
    assert_eq!(
        task.notice.as_deref(),
        Some("invalid input for query argument $3: 5 (expected str, got int)")
    );
    harness.close().await;
}
