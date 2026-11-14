//! Ported from `tests/test_adapters.py`, with fakes standing in for the clients as Python's
//! `SimpleNamespace` and `AsyncMock` do, plus the scoped mover against the fake CloudDrive
//! server (D41: a scoped mover's `settle()` waits for a call whose caller was dropped).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::NaiveDate;
use embyx_clients::avbase::{AvbaseError, AvbaseTalent, AvbaseWork};
use embyx_clients::clouddrive::aio::{EnsureResult, MoveResult, WriteTime};
use embyx_clients::clouddrive::fake::FakeCloudDrive;
use embyx_clients::clouddrive::{AsyncCloudDrive, ClientOptions, CloudDriveError, CloudFile};
use embyx_clients::javbus::{JavBusActor, JavBusActorPage, JavBusError, PageProgress};
use embyx_core::pyjson;
use embyx_core::scenario::Recorder;
use embyx_fill_actor::adapters::{
    ACTOR_NOT_FOUND, AvbaseCatalog, AvidBrandResolver, CloudDriveApi, CloudDriveFileMover, Intake, JavBusCatalog,
    LedgerAcquisitionGateway, TalentPage, TalentPageWalk, UnionActorCatalog,
};
use embyx_fill_actor::cloud_moves::{BoxFuture, CloudFileMover, MoverError};
use embyx_fill_actor::ports::{AcquisitionGateway, AcquisitionOutcome, ActorCatalog, BrandResolver};
use embyx_monitor::acquisitions::AcquisitionSource;
use embyx_monitor::intake::{IntakeError, IntakeOutcome};
use embyx_monitor::reports::RunContext;
use indexmap::IndexMap;

// ---------- the catalog fakes ----------

struct FakeAvbase {
    talents: HashMap<String, AvbaseTalent>,
    pages: Vec<TalentPage>,
    looked_up: Mutex<Vec<String>>,
}

impl FakeAvbase {
    fn new(talents: &[(&str, &AvbaseTalent)], pages: Vec<TalentPage>) -> Self {
        Self {
            talents: talents
                .iter()
                .map(|(name, talent)| ((*name).to_owned(), (*talent).clone()))
                .collect(),
            pages,
            looked_up: Mutex::new(Vec::new()),
        }
    }
}

struct FakePages(std::vec::IntoIter<TalentPage>);

impl TalentPageWalk for FakePages {
    fn next_page(&mut self) -> BoxFuture<'_, Result<Option<TalentPage>, AvbaseError>> {
        let page = self.0.next();
        Box::pin(async move { Ok(page) })
    }
}

impl AvbaseCatalog for FakeAvbase {
    fn talent<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Option<AvbaseTalent>, AvbaseError>> {
        self.looked_up.lock().unwrap().push(name.to_owned());
        let talent = self.talents.get(name).cloned();
        Box::pin(async move { Ok(talent) })
    }

    fn talent_pages<'a>(&'a self, _name: &'a str) -> Box<dyn TalentPageWalk + 'a> {
        Box::new(FakePages(self.pages.clone().into_iter()))
    }
}

#[derive(Default)]
struct FakeJavBus {
    stars: HashMap<String, Vec<JavBusActor>>,
    pages: HashMap<String, JavBusActorPage>,
    catalogs: HashMap<String, Vec<String>>,
    scraped: Mutex<Vec<String>>,
}

impl JavBusCatalog for FakeJavBus {
    fn search_stars<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<JavBusActor>, JavBusError>> {
        let stars = self.stars.get(name).cloned().unwrap_or_default();
        Box::pin(async move { Ok(stars) })
    }

    fn get_actor<'a>(&'a self, actor_id: &'a str) -> BoxFuture<'a, Result<Option<JavBusActorPage>, JavBusError>> {
        let page = self.pages.get(actor_id).cloned();
        Box::pin(async move { Ok(page) })
    }

    fn scrape<'a>(
        &'a self,
        actor_id: &'a str,
        _progress: Option<&'a PageProgress>,
    ) -> BoxFuture<'a, Result<Vec<String>, JavBusError>> {
        self.scraped.lock().unwrap().push(actor_id.to_owned());
        let catalog = self.catalogs[actor_id].clone();
        Box::pin(async move { Ok(catalog) })
    }
}

fn saika() -> AvbaseTalent {
    AvbaseTalent {
        talent_id: 5022,
        name: "河北彩花".to_owned(),
        aliases: vec!["河北彩伽".to_owned()],
        total_works: 2,
    }
}

fn work(work_id: &str, day: Option<NaiveDate>) -> AvbaseWork {
    AvbaseWork {
        work_id: work_id.to_owned(),
        prefix: String::new(),
        title: work_id.to_owned(),
        release_date: day,
        cast: Vec::new(),
    }
}

fn star(actor_id: &str, name: &str) -> JavBusActor {
    JavBusActor {
        actor_id: actor_id.to_owned(),
        name: name.to_owned(),
    }
}

fn ids(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn day(year: i32, month: u32, date: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, date).unwrap()
}

/// One awaited progress callback: `(completed, total, current)`.
type Progress = (u64, Option<u64>, Option<u64>);

// py: tests/test_adapters.py::test_the_union_catalog_joins_avbase_works_with_every_javbus_star_of_the_actor
#[tokio::test]
async fn the_union_catalog_joins_avbase_works_with_every_javbus_star_of_the_actor() {
    let talent = saika();
    let avbase = FakeAvbase::new(
        &[("河北彩伽", &talent), ("河北彩花", &talent)],
        vec![
            (1, 2, vec![work("SONE-100", Some(day(2026, 1, 1)))]),
            (2, 2, vec![work("SSIS-900", None)]),
        ],
    );
    let javbus = FakeJavBus {
        stars: HashMap::from([
            (
                "河北彩花".to_owned(),
                vec![star("sl1", "河北彩花"), star("zzz", "河北彩花子")],
            ),
            ("河北彩伽".to_owned(), vec![star("new1", "河北彩伽")]),
        ]),
        catalogs: HashMap::from([
            ("sl1".to_owned(), ids(&["SSIS-900", "OLD-001"])),
            ("new1".to_owned(), ids(&["SONE-100", "SONE-101"])),
        ]),
        ..FakeJavBus::default()
    };
    let catalog = UnionActorCatalog::new(avbase, javbus);
    let awaited: Arc<Mutex<Vec<Progress>>> = Arc::default();
    let progress: Box<PageProgress> = Box::new({
        let awaited = Arc::clone(&awaited);
        move |completed, total, current| {
            awaited.lock().unwrap().push((completed, total, current));
            Box::pin(async {})
        }
    });

    let listing = catalog.list_videos("河北彩伽", Some(&*progress)).await.unwrap();

    assert_eq!(
        (
            listing.actor_name.as_deref(),
            listing.talent_id,
            listing.aliases.clone()
        ),
        (Some("河北彩花"), Some(5022), ids(&["河北彩伽"]))
    );
    // AVBase first, then each JavBus star page; the fuzzy search hit under another name is dropped.
    assert_eq!(listing.video_ids, ids(&["SONE-100", "SSIS-900", "OLD-001", "SONE-101"]));
    assert_eq!(
        listing.release_dates,
        IndexMap::from([("SONE-100".to_owned(), day(2026, 1, 1))])
    );
    assert_eq!(
        listing.source_counts,
        IndexMap::from([
            ("avbase".to_owned(), 2),
            ("javbus:sl1".to_owned(), 2),
            ("javbus:new1".to_owned(), 2),
        ])
    );
    let mut scraped = catalog.javbus.scraped.lock().unwrap().clone();
    scraped.sort();
    assert_eq!(scraped, ids(&["new1", "sl1"]));
    assert!(awaited.lock().unwrap().contains(&(1, Some(2), Some(1))));
}

// py: tests/test_adapters.py::test_a_javbus_star_id_names_the_actor_through_its_star_page
#[tokio::test]
async fn a_javbus_star_id_names_the_actor_through_its_star_page() {
    let talent = saika();
    let avbase = FakeAvbase::new(&[("河北彩花", &talent)], vec![(1, 1, vec![work("SONE-100", None)])]);
    let javbus = FakeJavBus {
        stars: HashMap::from([("河北彩花".to_owned(), vec![star("sl1", "河北彩花")])]),
        pages: HashMap::from([(
            "sl1".to_owned(),
            JavBusActorPage {
                actor_id: "sl1".to_owned(),
                name: "河北彩花".to_owned(),
                video_ids: ids(&["SSIS-900"]),
            },
        )]),
        catalogs: HashMap::from([("sl1".to_owned(), ids(&["SSIS-900"]))]),
        ..FakeJavBus::default()
    };
    let catalog = UnionActorCatalog::new(avbase, javbus);

    let listing = catalog.list_videos("sl1", None).await.unwrap();

    assert_eq!(listing.talent_id, Some(5022));
    assert_eq!(*catalog.avbase.looked_up.lock().unwrap(), ids(&["sl1", "河北彩花"]));
    assert_eq!(listing.video_ids, ids(&["SONE-100", "SSIS-900"]));
}

// py: tests/test_adapters.py::test_an_actor_neither_catalog_knows_is_an_error
#[tokio::test]
async fn an_actor_neither_catalog_knows_is_an_error() {
    let catalog = UnionActorCatalog::new(FakeAvbase::new(&[], Vec::new()), FakeJavBus::default());

    let error = catalog.list_videos("nobody", None).await.unwrap_err();

    assert_eq!(error.class, ACTOR_NOT_FOUND);
    assert_eq!(
        error.message,
        "neither AVBase nor JavBus knows an actor called 'nobody'"
    );
}

// ---------- the acquisition gateway ----------

#[derive(Debug, Clone, PartialEq)]
struct Queued {
    avid: String,
    source: String,
    task_dir_path: String,
    release_date: Option<NaiveDate>,
}

struct FakeIntake {
    outcome: IntakeOutcome,
    calls: Mutex<Vec<Queued>>,
}

impl FakeIntake {
    fn returning(outcome: IntakeOutcome) -> Arc<Self> {
        Arc::new(Self {
            outcome,
            calls: Mutex::new(Vec::new()),
        })
    }
}

impl Intake for FakeIntake {
    fn queue<'a>(
        &'a self,
        avid: &'a str,
        source: &'a str,
        task_dir_path: &'a str,
        _ctx: &'a RunContext,
        release_date: Option<NaiveDate>,
    ) -> BoxFuture<'a, Result<IntakeOutcome, IntakeError>> {
        self.calls.lock().unwrap().push(Queued {
            avid: avid.to_owned(),
            source: source.to_owned(),
            task_dir_path: task_dir_path.to_owned(),
            release_date,
        });
        let outcome = self.outcome;
        Box::pin(async move { Ok(outcome) })
    }
}

fn gateway(
    intake: Option<Arc<FakeIntake>>,
    task_dir: &str,
    nudges: Option<Arc<Mutex<Vec<bool>>>>,
) -> LedgerAcquisitionGateway {
    let task_dir = task_dir.to_owned();
    LedgerAcquisitionGateway {
        intake_factory: Arc::new(move || intake.clone().map(|intake| intake as Arc<dyn Intake>)),
        task_dir: Arc::new(move || task_dir.clone()),
        on_queued: nudges.map(|nudges| Arc::new(move || nudges.lock().unwrap().push(true)) as _),
    }
}

// py: tests/test_adapters.py::test_acquisition_gateway_queues_and_wakes_the_tracker
#[tokio::test]
async fn acquisition_gateway_queues_and_wakes_the_tracker() {
    let intake = FakeIntake::returning(IntakeOutcome::Queued);
    let nudges: Arc<Mutex<Vec<bool>>> = Arc::default();
    let gateway = gateway(Some(Arc::clone(&intake)), "/115/fill", Some(Arc::clone(&nudges)));

    assert_eq!(
        gateway.queue_missing("ABC-001", None).await.unwrap(),
        AcquisitionOutcome::Queued
    );
    let calls = intake.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        vec![Queued {
            avid: "ABC-001".to_owned(),
            source: AcquisitionSource::FillActor.as_str().to_owned(),
            task_dir_path: "/115/fill".to_owned(),
            release_date: None,
        }]
    );
    assert_eq!(*nudges.lock().unwrap(), vec![true]);
}

// py: tests/test_adapters.py::test_acquisition_gateway_does_not_wake_the_tracker_for_tracked_avids
#[tokio::test]
async fn acquisition_gateway_does_not_wake_the_tracker_for_tracked_avids() {
    let intake = FakeIntake::returning(IntakeOutcome::AlreadyTracked);
    let nudges: Arc<Mutex<Vec<bool>>> = Arc::default();
    let gateway = gateway(Some(intake), "/115/fill", Some(Arc::clone(&nudges)));

    assert_eq!(
        gateway.queue_missing("ABC-001", None).await.unwrap(),
        AcquisitionOutcome::AlreadyTracked
    );
    assert!(nudges.lock().unwrap().is_empty());
}

// py: tests/test_adapters.py::test_acquisition_gateway_reports_unconfigured_clouddrive
#[tokio::test]
async fn acquisition_gateway_reports_unconfigured_clouddrive() {
    let gateway = gateway(None, "/115/fill", None);

    assert_eq!(
        gateway.queue_missing("ABC-001", None).await.unwrap(),
        AcquisitionOutcome::CloudNotConfigured
    );
}

// py: tests/test_adapters.py::test_acquisition_gateway_reports_missing_offline_directory
#[tokio::test]
async fn acquisition_gateway_reports_missing_offline_directory() {
    let intake = FakeIntake::returning(IntakeOutcome::Queued);
    let gateway = gateway(Some(Arc::clone(&intake)), "", None);

    assert_eq!(
        gateway.queue_missing("ABC-001", None).await.unwrap(),
        AcquisitionOutcome::TaskDirNotConfigured
    );
    assert!(intake.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn acquisition_gateway_passes_the_release_date_and_maps_every_outcome_by_value() {
    for outcome in IntakeOutcome::ALL {
        let intake = FakeIntake::returning(*outcome);
        let gateway = gateway(Some(Arc::clone(&intake)), "/115/fill", None);
        let queued = gateway.queue_missing("ABC-001", Some(day(2026, 1, 2))).await.unwrap();
        assert_eq!(queued.as_str(), outcome.as_str());
        assert_eq!(intake.calls.lock().unwrap()[0].release_date, Some(day(2026, 1, 2)));
    }
}

// py: tests/test_adapters.py::test_brand_resolver_uses_avid_rules
#[test]
fn brand_resolver_uses_avid_rules() {
    let resolver = AvidBrandResolver;

    assert_eq!(resolver.resolve_brand("ABC-001").as_deref(), Some("ABC"));
    assert_eq!(resolver.resolve_brand("NOBRAND"), None);
}

// ---------- the cloud mover ----------

/// A recorded call: the method and its arguments.
type Call = (String, Vec<String>);

/// The CloudDrive handle's canned answers and the calls it got.
#[derive(Clone, Default)]
struct FakeCloud {
    listing: Vec<CloudFile>,
    ensured: Option<EnsureResult>,
    moved: Option<MoveResult>,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl FakeCloud {
    fn record(&self, method: &str, args: &[&str]) {
        self.calls
            .lock()
            .unwrap()
            .push((method.to_owned(), args.iter().map(|arg| (*arg).to_owned()).collect()));
    }
}

impl CloudDriveApi for FakeCloud {
    fn list_directory<'a>(&'a self, api_dir: &'a str) -> BoxFuture<'a, Result<Vec<CloudFile>, CloudDriveError>> {
        self.record("list_directory", &[api_dir]);
        Box::pin(async move { Ok(self.listing.clone()) })
    }

    fn ensure_directory<'a>(
        &'a self,
        parent_api_dir: &'a str,
        folder_name: &'a str,
    ) -> BoxFuture<'a, Result<EnsureResult, CloudDriveError>> {
        self.record("ensure_directory", &[parent_api_dir, folder_name]);
        Box::pin(async move { Ok(self.ensured.clone().expect("an ensure_directory answer")) })
    }

    fn move_file<'a>(
        &'a self,
        source_api_path: &'a str,
        destination_api_dir: &'a str,
    ) -> BoxFuture<'a, Result<MoveResult, CloudDriveError>> {
        self.record("move_file", &[source_api_path, destination_api_dir]);
        Box::pin(async move { Ok(self.moved.clone().expect("a move_file answer")) })
    }

    fn scoped(&self) -> Self {
        self.clone()
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

fn mover(cloud: Option<FakeCloud>) -> CloudDriveFileMover<FakeCloud> {
    CloudDriveFileMover::new(Arc::new(move || cloud.clone()))
}

fn cloud_file(id: &str, full_path: &str, size: i64, is_directory: bool, write_time: Option<WriteTime>) -> CloudFile {
    CloudFile {
        id: id.to_owned(),
        name: full_path.rsplit('/').next().unwrap().to_owned(),
        full_path: full_path.to_owned(),
        size,
        is_directory,
        write_time,
        hashes: BTreeMap::new(),
    }
}

// py: tests/test_adapters.py::test_cloud_mover_converts_listing_and_skips_directories
#[tokio::test]
async fn cloud_mover_converts_listing_and_skips_directories() {
    let mut file = cloud_file(
        "file-id",
        "/cloud/ABC/ABC-001.mp4",
        123,
        false,
        Some(WriteTime {
            seconds: 456,
            nanos: 789,
        }),
    );
    file.hashes.insert("2".to_owned(), "sha1-value".to_owned());
    let cloud = FakeCloud {
        listing: vec![
            cloud_file(
                "dir-id",
                "/cloud/ABC",
                0,
                true,
                Some(WriteTime { seconds: 1, nanos: 0 }),
            ),
            file,
        ],
        ..FakeCloud::default()
    };

    let files = mover(Some(cloud)).list_directory("/cloud/ABC").await.unwrap();

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "/cloud/ABC/ABC-001.mp4");
    assert_eq!(files[0].write_time, 456 * 1_000_000_000 + 789);
}

// py: tests/test_adapters.py::test_cloud_mover_rejects_missing_write_time
#[tokio::test]
async fn cloud_mover_rejects_missing_write_time() {
    let cloud = FakeCloud {
        listing: vec![cloud_file("file-id", "/cloud/ABC/ABC-001.mp4", 123, false, None)],
        ..FakeCloud::default()
    };

    let error = mover(Some(cloud)).list_directory("/cloud/ABC").await.unwrap_err();

    assert_eq!(error.class_name(), "TypeError");
    assert!(error.to_string().contains("invalid write time"), "{error}");
}

#[tokio::test]
async fn cloud_mover_rejects_write_times_out_of_range_and_bad_metadata() {
    for nanos in [-1, 1_000_000_000] {
        let cloud = FakeCloud {
            listing: vec![cloud_file(
                "file-id",
                "/cloud/ABC/ABC-001.mp4",
                1,
                false,
                Some(WriteTime { seconds: 1, nanos }),
            )],
            ..FakeCloud::default()
        };
        let error = mover(Some(cloud)).list_directory("/cloud/ABC").await.unwrap_err();
        assert_eq!(
            (error.class_name(), error.to_string().as_str()),
            ("TypeError", "CloudDrive listing returned invalid write time")
        );
    }
    let cloud = FakeCloud {
        listing: vec![cloud_file(
            "file-id",
            "/cloud/ABC/ABC-001.mp4",
            -1,
            false,
            Some(WriteTime { seconds: 1, nanos: 0 }),
        )],
        ..FakeCloud::default()
    };
    let error = mover(Some(cloud)).list_directory("/cloud/ABC").await.unwrap_err();
    assert_eq!(
        (error.class_name(), error.to_string().as_str()),
        ("ValueError", "invalid CloudDrive file metadata")
    );
}

// py: tests/test_adapters.py::test_cloud_mover_ensure_directory_returns_success_flag
#[tokio::test]
async fn cloud_mover_ensure_directory_returns_success_flag() {
    let cloud = FakeCloud {
        ensured: Some(EnsureResult {
            success: true,
            created: false,
            path: "/cloud/ABC".to_owned(),
        }),
        ..FakeCloud::default()
    };
    let calls = Arc::clone(&cloud.calls);

    assert!(mover(Some(cloud)).ensure_directory("/cloud", "ABC").await.unwrap());
    assert_eq!(
        *calls.lock().unwrap(),
        vec![("ensure_directory".to_owned(), ids(&["/cloud", "ABC"]))]
    );
}

// py: tests/test_adapters.py::test_cloud_mover_move_file_maps_response
#[tokio::test]
async fn cloud_mover_move_file_maps_response() {
    let cloud = FakeCloud {
        moved: Some(MoveResult {
            success: true,
            error_message: String::new(),
            result_file_paths: ids(&["/cloud/dst/ABC-001.mp4"]),
        }),
        ..FakeCloud::default()
    };

    let response = mover(Some(cloud))
        .move_file("/cloud/src/ABC-001.mp4", "/cloud/dst")
        .await
        .unwrap();

    assert!(response.success);
    assert_eq!(response.error_message, None);
    assert_eq!(response.result_paths, ids(&["/cloud/dst/ABC-001.mp4"]));
}

// py: tests/test_adapters.py::test_cloud_mover_raises_when_unconfigured
#[tokio::test]
async fn cloud_mover_raises_when_unconfigured() {
    let error = mover(None).list_directory("/cloud/ABC").await.unwrap_err();

    assert_eq!(error, MoverError::Unconfigured);
    assert_eq!(error.class_name(), "CloudDriveUnconfiguredError");
    assert_eq!(error.to_string(), "CloudDrive connection is not configured");
}

#[tokio::test]
async fn a_scoped_mover_settles_only_after_a_dropped_move_has_its_answer() {
    let seed = pyjson::loads(r#"{"directories": ["/in", "/out"], "files": [{"path": "/in/a.mp4"}]}"#).unwrap();
    let served = FakeCloudDrive::new(&seed, Recorder::new()).serve().await;
    let hold = served.fake.hold("MoveFile");
    let cloud = AsyncCloudDrive::connect(&ClientOptions::new(served.address(), "test-token", false));
    let resolved = cloud.clone();
    let mover = CloudDriveFileMover::new(Arc::new(move || Some(resolved.clone())));
    let scoped = mover.scoped();

    let caller = Arc::clone(&scoped);
    let task = tokio::spawn(async move { caller.move_file("/in/a.mp4", "/out").await });
    tokio::time::timeout(Duration::from_secs(5), hold.arrived())
        .await
        .expect("the move reached the server");
    task.abort();
    assert!(task.await.expect_err("aborted").is_cancelled());

    // The unscoped mover has nothing to wait for; the scoped one waits for the move.
    tokio::time::timeout(Duration::from_secs(1), mover.settle())
        .await
        .expect("an unscoped mover settles at once");
    let settled = tokio::spawn({
        let scoped = Arc::clone(&scoped);
        async move { scoped.settle().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!settled.is_finished(), "settle() returned while the move was in flight");

    hold.release();
    tokio::time::timeout(Duration::from_secs(5), settled)
        .await
        .expect("settle() returns once the move is done")
        .expect("settle");
    assert_eq!(cloud.in_flight(), 0);
    let listing = scoped.list_directory("/out").await.unwrap_err();
    // The fake CloudDrive has no write times: the moved file is there, and refused as Python
    // refuses it.
    assert_eq!(listing.class_name(), "TypeError");
    cloud.close().await;
}
