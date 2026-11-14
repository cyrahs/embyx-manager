//! Cross-version contract 6 on the merge service: a CloudDrive mutation that was sent is
//! awaited to its end, also when the caller goes away. Python shields each RPC, so a
//! cancelled tick or action waits for the call before it re-raises; the Rust service runs
//! its calls through a scoped [`AsyncCloudDrive`] handle whose calls live in tasks of their
//! own, and `aclose()` settles that handle before it returns.
//!
//! Against the CloudDrive fake over gRPC, with a call held at the server.

use std::sync::Arc;
use std::time::Duration;

use embyx_clients::clouddrive::fake::FakeCloudDrive;
use embyx_clients::clouddrive::{AsyncCloudDrive, ClientOptions};
use embyx_core::config::models::{ArchiveConfig, CloudDriveConfig, MergeConfig};
use embyx_core::pyjson::{self, Value};
use embyx_core::scenario::Recorder;
use embyx_db::testdb::TestDatabase;
use embyx_merge::fakes::FakeKube;
use embyx_merge::ports::{MergeCloud, MergeKube, Trigger};
use embyx_merge::service::{MergeService, ServiceParts};
use embyx_merge::tasks::{MergeState, MergeTask, MergeTaskRepository, NewTask, TaskField};

const LIBRARY: &str = "/mnt/cd2/115/embyx";

fn parts() -> Vec<String> {
    (1..=2)
        .map(|index| format!("{LIBRARY}/type/vr/SQTEVR/SQTEVR-009-cd{index}.mp4"))
        .collect()
}

fn seed() -> Value {
    pyjson::loads(
        r#"{"directories": ["/115/upload", "/115/embyx_in"],
            "files": [{"path": "/115/embyx/type/vr/SQTEVR/SQTEVR-009-cd1.mp4", "size": 10},
                      {"path": "/115/embyx/type/vr/SQTEVR/SQTEVR-009-cd2.mp4", "size": 10}]}"#,
    )
    .expect("the seed")
}

struct Harness {
    test: TestDatabase,
    _work: tempfile::TempDir,
    fake: FakeCloudDrive,
    _served: embyx_clients::clouddrive::fake::Served,
    cloud: AsyncCloudDrive,
    repository: MergeTaskRepository,
    service: MergeService,
}

impl Harness {
    async fn new(seed: &Value) -> Self {
        let served = FakeCloudDrive::new(seed, Recorder::new()).serve().await;
        let cloud = AsyncCloudDrive::connect(&ClientOptions::new(served.address(), "test-token", false));
        let work = tempfile::tempdir().expect("tempdir");
        let test = TestDatabase::create().await;
        let repository = MergeTaskRepository::new(test.database());
        let merge = MergeConfig {
            work_dir: work.path().to_string_lossy().into_owned(),
            ..MergeConfig::default()
        };
        let archive = ArchiveConfig {
            src_dir: "/mnt/cd2/115/embyx_in".to_owned(),
            dst_dir: LIBRARY.to_owned(),
            priority_mapping: vec![("vr".to_owned(), "type/vr".to_owned())],
            ..ArchiveConfig::default()
        };
        let clouddrive = CloudDriveConfig {
            address: served.address(),
            api_token: "test-token".to_owned(),
            ..CloudDriveConfig::default()
        };
        let source = cloud.clone();
        let trigger: Trigger = Arc::new(|_| Box::pin(async {}));
        let service = MergeService::new(ServiceParts::new(
            repository.clone(),
            Arc::new(move || Some(Arc::new(source.clone()) as Arc<dyn MergeCloud>)),
            Some(Arc::new(FakeKube::default()) as Arc<dyn MergeKube>),
            Arc::new(move || merge.clone()),
            Arc::new(move || archive.clone()),
            Arc::new(move || clouddrive.clone()),
            trigger,
        ));
        Self {
            test,
            _work: work,
            fake: served.fake.clone(),
            _served: served,
            cloud,
            repository,
            service,
        }
    }

    /// A task moved straight to `state` with `fields`.
    async fn task_in(&self, state: MergeState, fields: Vec<TaskField>) -> MergeTask {
        let task = self
            .repository
            .create(&NewTask {
                avid: "SQTEVR-009".to_owned(),
                source: "vr".to_owned(),
                library_dir: "type/vr".to_owned(),
                brand: "SQTEVR".to_owned(),
                parts: parts(),
                merged_name: "SQTEVR-009.mp4".to_owned(),
            })
            .await
            .expect("create");
        self.repository
            .transition(task.id, MergeState::Queued, state, fields)
            .await
            .expect("transition")
            .expect("moved")
    }

    fn has(&self, path: &str) -> bool {
        let dumped = self.fake.dump();
        dumped
            .get("files")
            .and_then(Value::as_list)
            .expect("files")
            .iter()
            .any(|file| file.get("path").and_then(Value::as_str) == Some(path))
    }

    async fn close(self) {
        self.cloud.close().await;
        self.repository.database().close().await;
        self.test.drop_database().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_a_delete_the_tick_sent() {
    let harness = Harness::new(&seed()).await;
    let task = harness.task_in(MergeState::Replacing, Vec::new()).await;
    let hold = harness.fake.hold("DeleteFiles");

    harness.service.start();
    tokio::time::timeout(Duration::from_secs(10), hold.arrived())
        .await
        .expect("the tick deletes the parts");
    let closing = tokio::spawn({
        let service = harness.service.clone();
        async move { service.aclose().await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !closing.is_finished(),
        "aclose() returned while the delete was in flight"
    );

    hold.release();
    tokio::time::timeout(Duration::from_secs(10), closing)
        .await
        .expect("aclose() returns once the delete is done")
        .expect("aclose");
    assert_eq!(harness.cloud.in_flight(), 0);
    assert!(!harness.has("/115/embyx/type/vr/SQTEVR/SQTEVR-009-cd1.mp4"));
    // The tick was dropped after the delete: the task waits for the next one.
    let after = harness.repository.get(task.id).await.expect("get").expect("task");
    assert_eq!(after.state, MergeState::Replacing);
    harness.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_whose_caller_went_away_still_finishes_the_copy_cancel() {
    let mut seed = seed();
    if let Value::Dict(fields) = &mut seed {
        fields.insert(
            "copy_tasks",
            pyjson::loads(
                r#"[{"source_path": "/downloads/upload/SQTEVR-009.mp4", "dest_path": "/115/upload/merge-1",
                     "status": "Scanned", "total_bytes": 0, "uploaded_bytes": 0, "failed_files": 0,
                     "paused": false, "errors": []}]"#,
            )
            .expect("copy tasks"),
        );
    }
    let harness = Harness::new(&seed).await;
    let task = harness
        .task_in(
            MergeState::Uploading,
            vec![
                TaskField::MergedBytes(Some(25)),
                TaskField::MergedSha1(Some("AA".to_owned())),
            ],
        )
        .await;
    let hold = harness.fake.hold("CancelCopyTask");

    let cancelling = tokio::spawn({
        let service = harness.service.clone();
        async move { service.cancel(task.id).await }
    });
    tokio::time::timeout(Duration::from_secs(10), hold.arrived())
        .await
        .expect("the cancel reaches CloudDrive");
    cancelling.abort();
    assert!(cancelling.await.expect_err("aborted").is_cancelled());

    hold.release();
    tokio::time::timeout(Duration::from_secs(10), async {
        while harness.cloud.in_flight() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the copy cancel finishes on its own");
    let copies = harness.fake.dump();
    assert_eq!(
        copies.get("copy_tasks").and_then(Value::as_list).map(<[Value]>::len),
        Some(0),
        "the copy was cancelled"
    );
    // The action itself was dropped before it recorded anything.
    let after = harness.repository.get(task.id).await.expect("get").expect("task");
    assert_eq!(after.state, MergeState::Uploading);
    harness.close().await;
}
