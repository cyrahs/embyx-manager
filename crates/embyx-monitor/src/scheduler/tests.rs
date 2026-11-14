//! `tests/test_monitor_scheduler.py`: the scheduler over an in-memory store and run
//! history, in real time with short sleeps as the Python tests run.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, TimeZone as _, Timelike as _, Utc};
use embyx_core::config::{ArchiveConfig, MappingConfig, PlaylistsConfig, RssConfig};

use super::*;
use crate::reports::Raised;
use crate::runs::PipelineRunRecord;

/// `FakeStore`: every section at its defaults unless given.
#[derive(Default)]
struct FakeStore {
    rss: Mutex<RssConfig>,
    archive: Mutex<ArchiveConfig>,
    mapping: Mutex<MappingConfig>,
    playlists: Mutex<PlaylistsConfig>,
}

impl SchedulerConfig for FakeStore {
    fn rss(&self) -> RssConfig {
        locked(&self.rss).clone()
    }

    fn archive(&self) -> ArchiveConfig {
        locked(&self.archive).clone()
    }

    fn mapping(&self) -> MappingConfig {
        locked(&self.mapping).clone()
    }

    fn playlists(&self) -> PlaylistsConfig {
        locked(&self.playlists).clone()
    }

    fn refresh(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

/// `FakeRuns`: records by id, in start order.
#[derive(Default)]
struct FakeRuns {
    records: Mutex<HashMap<String, PipelineRunRecord>>,
    order: Mutex<Vec<String>>,
}

impl FakeRuns {
    fn record(&self, run_id: &str) -> PipelineRunRecord {
        locked(&self.records)[run_id].clone()
    }

    fn finished(&self, pipeline: PipelineName) -> Vec<PipelineRunRecord> {
        let records = locked(&self.records);
        locked(&self.order)
            .iter()
            .map(|run_id| records[run_id].clone())
            .filter(|record| record.pipeline == pipeline && record.state != RunState::Running)
            .collect()
    }
}

impl RunStore for FakeRuns {
    fn start_run(&self, pipeline: PipelineName, trigger: RunTrigger) -> BoxFuture<'_, Result<String, StoreError>> {
        Box::pin(async move {
            let run_id = crate::support::uuid4_hex();
            locked(&self.records).insert(
                run_id.clone(),
                PipelineRunRecord {
                    run_id: run_id.clone(),
                    pipeline,
                    trigger,
                    state: RunState::Running,
                    started_at: Utc::now(),
                    finished_at: None,
                    stats: Counters::new(),
                    errors: Vec::new(),
                    log_tail: Vec::new(),
                },
            );
            locked(&self.order).push(run_id.clone());
            Ok(run_id)
        })
    }

    fn finish_run<'a>(
        &'a self,
        run_id: &'a str,
        state: RunState,
        counters: &'a Counters,
        errors: &'a [String],
        log_tail: &'a [String],
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let mut records = locked(&self.records);
            let record = records.get_mut(run_id).expect("a started run");
            record.state = state;
            record.finished_at = Some(Utc::now());
            record.stats = counters.clone();
            record.errors = errors.to_vec();
            record.log_tail = log_tail.to_vec();
            Ok(())
        })
    }

    fn fail_stale_running<'a>(&'a self, _error: &'a str) -> BoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(async { Ok(0) })
    }
}

fn ready() -> Ready {
    Arc::new(|| None)
}

fn not_ready(reason: &'static str) -> Ready {
    Arc::new(move || Some(reason.to_owned()))
}

/// A runner from a closure over the scope.
fn runner<F, Fut>(function: F) -> Runner
where
    F: Fn(RunScope) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), RunFailure>> + Send + 'static,
{
    Arc::new(move |scope| Box::pin(function(scope)))
}

/// `make_scheduler(store, runs, ...)`'s defaults: runners counting their runs, every
/// pipeline ready, no tracker.
fn parts(store: &Arc<FakeStore>, runs: &Arc<FakeRuns>) -> SchedulerParts {
    let mapping_factory: MappingFactory = Arc::new(|| {
        Box::new(MappingPipeline::new(
            MappingConfig::default(),
            embyx_core::avid::AvidParser::default(),
        ))
    });
    SchedulerParts::new(
        Arc::clone(store) as Arc<dyn SchedulerConfig>,
        Arc::clone(runs) as Arc<dyn RunStore>,
        (
            runner(|scope: RunScope| async move {
                scope.ctx.add("rss_runs", 1);
                Ok(())
            }),
            runner(|scope: RunScope| async move {
                scope.ctx.add("archive_runs", 1);
                Ok(())
            }),
            mapping_factory,
        ),
        (ready(), ready(), ready()),
    )
}

async fn sleep_ms(millis: u64) {
    tokio::time::sleep(Duration::from_millis(millis)).await;
}

fn archive_configured() -> ArchiveConfig {
    ArchiveConfig {
        enabled: true,
        src_dir: "/x".to_owned(),
        dst_dir: "/y".to_owned(),
        mapping: vec![("a".to_owned(), "b".to_owned())],
        ..ArchiveConfig::default()
    }
}

// py: tests/test_monitor_scheduler.py::test_manual_trigger_persists_completed_run
#[tokio::test(flavor = "multi_thread")]
async fn manual_trigger_persists_completed_run() {
    let (store, runs) = (Arc::new(FakeStore::default()), Arc::new(FakeRuns::default()));
    let scheduler = MonitorScheduler::new(parts(&store, &runs));

    let run_id = scheduler.trigger(PipelineName::Rss).await.expect("started");
    sleep_ms(50).await;

    let record = runs.record(&run_id);
    assert_eq!(record.state, RunState::Completed);
    assert_eq!(record.trigger, RunTrigger::Manual);
    assert_eq!(record.stats, Counters::from([("rss_runs", 1)]));
    scheduler.aclose().await;
}

// py: tests/test_monitor_scheduler.py::test_trigger_rejects_concurrent_run
#[tokio::test(flavor = "multi_thread")]
async fn trigger_rejects_concurrent_run() {
    let (store, runs) = (Arc::new(FakeStore::default()), Arc::new(FakeRuns::default()));
    let release = Arc::new(Notify::new());
    let mut parts = parts(&store, &runs);
    let waiting = Arc::clone(&release);
    parts.rss_runner = runner(move |_scope| {
        let release = Arc::clone(&waiting);
        async move {
            release.notified().await;
            Ok(())
        }
    });
    let scheduler = MonitorScheduler::new(parts);
    scheduler.trigger(PipelineName::Rss).await.expect("started");
    sleep_ms(10).await;

    assert!(matches!(
        scheduler.trigger(PipelineName::Rss).await,
        Err(TriggerError::Busy(PipelineName::Rss))
    ));

    release.notify_one();
    sleep_ms(50).await;
    scheduler.aclose().await;
}

// py: tests/test_monitor_scheduler.py::test_trigger_rejects_unconfigured_pipeline
#[tokio::test(flavor = "multi_thread")]
async fn trigger_rejects_unconfigured_pipeline() {
    let (store, runs) = (Arc::new(FakeStore::default()), Arc::new(FakeRuns::default()));
    let mut parts = parts(&store, &runs);
    parts.rss_ready = not_ready("FreshRSS is not configured");
    let scheduler = MonitorScheduler::new(parts);

    let refused = scheduler.trigger(PipelineName::Rss).await;
    assert!(
        matches!(&refused, Err(TriggerError::NotConfigured { pipeline: PipelineName::Rss, reason }) if reason == "FreshRSS is not configured"),
        "{refused:?}"
    );
    assert_eq!(
        refused.expect_err("refused").to_string(),
        "pipeline rss is not runnable: FreshRSS is not configured"
    );
    scheduler.aclose().await;
}

// py: tests/test_monitor_scheduler.py::test_failed_run_is_recorded_with_errors
#[tokio::test(flavor = "multi_thread")]
async fn failed_run_is_recorded_with_errors() {
    let (store, runs) = (Arc::new(FakeStore::default()), Arc::new(FakeRuns::default()));
    let mut parts = parts(&store, &runs);
    parts.rss_runner = runner(|_scope| async { Err(RunFailure::raised(Raised::new("RuntimeError", "feed exploded"))) });
    let scheduler = MonitorScheduler::new(parts);

    let run_id = scheduler.trigger(PipelineName::Rss).await.expect("started");
    sleep_ms(50).await;

    let record = runs.record(&run_id);
    assert_eq!(record.state, RunState::Failed);
    assert!(record.errors.iter().any(|error| error.contains("rss run failed")));
    // The run log keeps Python's line, `repr(exc)` included.
    assert_eq!(
        record.errors,
        vec!["ERROR: rss run failed: RuntimeError('feed exploded')".to_owned()]
    );
    scheduler.aclose().await;
}

// py: tests/test_monitor_scheduler.py::test_cancel_running_marks_run_cancelled
#[tokio::test(flavor = "multi_thread")]
async fn cancel_running_marks_run_cancelled() {
    let (store, runs) = (Arc::new(FakeStore::default()), Arc::new(FakeRuns::default()));
    let started = Arc::new(Notify::new());
    let mut parts = parts(&store, &runs);
    let signal = Arc::clone(&started);
    parts.rss_runner = runner(move |scope: RunScope| {
        let started = Arc::clone(&signal);
        async move {
            started.notify_one();
            loop {
                scope.ctx.check_cancelled()?;
                sleep_ms(10).await;
            }
        }
    });
    let scheduler = MonitorScheduler::new(parts);
    let run_id = scheduler.trigger(PipelineName::Rss).await.expect("started");
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("the run started");

    assert!(scheduler.cancel_running(PipelineName::Rss));
    sleep_ms(100).await;

    let record = runs.record(&run_id);
    assert_eq!(record.state, RunState::Cancelled);
    assert_eq!(record.log_tail, vec!["WARNING: run cancelled by operator".to_owned()]);
    assert!(!scheduler.cancel_running(PipelineName::Rss));
    scheduler.aclose().await;
}

// py: tests/test_monitor_scheduler.py::test_rss_loop_runs_on_its_interval_and_archive_waits_for_its_cron
#[tokio::test(flavor = "multi_thread")]
async fn rss_loop_runs_on_its_interval_and_archive_waits_for_its_cron() {
    let store = Arc::new(FakeStore {
        rss: Mutex::new(RssConfig {
            enabled: true,
            interval_seconds: 3600,
            ..RssConfig::default()
        }),
        archive: Mutex::new(archive_configured()),
        ..FakeStore::default()
    });
    let runs = Arc::new(FakeRuns::default());
    let scheduler = MonitorScheduler::new(parts(&store, &runs));

    scheduler.start().await.expect("start");
    for _ in 0..100 {
        if !runs.finished(PipelineName::Rss).is_empty() {
            break;
        }
        sleep_ms(20).await;
    }
    scheduler.aclose().await;

    let rss_runs = runs.finished(PipelineName::Rss);
    assert_eq!(rss_runs.len(), 1);
    assert_eq!(rss_runs[0].trigger, RunTrigger::Scheduled);
    // The archive scan no longer piggybacks on the rss interval.
    assert!(runs.finished(PipelineName::Archive).is_empty());
}

// py: tests/test_monitor_scheduler.py::test_archive_scan_fires_when_its_cron_time_arrives
#[tokio::test(flavor = "multi_thread")]
async fn archive_scan_fires_when_its_cron_time_arrives() {
    let store = Arc::new(FakeStore {
        archive: Mutex::new(archive_configured()),
        ..FakeStore::default()
    });
    let runs = Arc::new(FakeRuns::default());
    let mut parts = parts(&store, &runs);
    parts.timings.cron_poll = 0.01;
    let scheduler = MonitorScheduler::new(parts);

    scheduler.start().await.expect("start");
    for _ in 0..100 {
        if scheduler.next().archive.is_some() {
            break;
        }
        sleep_ms(10).await;
    }
    // Pull the next fire into the past instead of waiting out a real minute.
    scheduler.next().archive = Some(Utc::now() - TimeDelta::seconds(1));
    for _ in 0..200 {
        if !runs.finished(PipelineName::Archive).is_empty() {
            break;
        }
        sleep_ms(20).await;
    }
    let next_after_fire = scheduler.next().archive;
    scheduler.aclose().await;

    let archive_runs = runs.finished(PipelineName::Archive);
    assert_eq!(archive_runs.len(), 1);
    assert_eq!(archive_runs[0].trigger, RunTrigger::Scheduled);
    let next_after_fire = next_after_fire.expect("a next fire");
    assert!(next_after_fire > Utc::now());
}

/// A tracker poll counting its calls.
fn counting_poll() -> (Runner, Arc<Mutex<usize>>) {
    let polls = Arc::new(Mutex::new(0_usize));
    let counter = Arc::clone(&polls);
    let poll = runner(move |_scope| {
        let counter = Arc::clone(&counter);
        async move {
            *locked(&counter) += 1;
            Ok(())
        }
    });
    (poll, polls)
}

fn tracker_scheduler(store: &Arc<FakeStore>, runs: &Arc<FakeRuns>, poll: Runner) -> MonitorScheduler {
    let mut parts = parts(store, runs);
    parts.timings.tracker_fast_checks = vec![0.05, 0.1, 0.15];
    parts.tracker_poll = Some(poll);
    MonitorScheduler::new(parts)
}

fn hourly_tracker() -> Arc<FakeStore> {
    Arc::new(FakeStore {
        archive: Mutex::new(ArchiveConfig {
            tracker_interval_seconds: 3600,
            ..ArchiveConfig::default()
        }),
        ..FakeStore::default()
    })
}

// py: tests/test_monitor_scheduler.py::test_tracker_fast_checks_follow_a_submission
#[tokio::test(flavor = "multi_thread")]
async fn tracker_fast_checks_follow_a_submission() {
    let (store, runs) = (hourly_tracker(), Arc::new(FakeRuns::default()));
    let (poll, polls) = counting_poll();
    let scheduler = tracker_scheduler(&store, &runs, poll);
    scheduler.start().await.expect("start");
    for _ in 0..100 {
        if *locked(&polls) > 0 {
            break;
        }
        sleep_ms(10).await;
    }
    // The startup poll; the next regular one is an hour out.
    assert_eq!(*locked(&polls), 1);

    scheduler.notify_submission();
    for _ in 0..200 {
        if *locked(&polls) >= 4 {
            break;
        }
        sleep_ms(10).await;
    }
    scheduler.aclose().await;

    // One extra poll per fast-check offset, pulled in ahead of the hour-long interval.
    assert_eq!(*locked(&polls), 4);
    // The poll's counters are what the dashboard shows.
    assert!(scheduler.tracker_state().last_polled_at.is_some());
    assert_eq!(scheduler.tracker_state().last_error, None);
}

// py: tests/test_monitor_scheduler.py::test_tracker_merges_a_burst_of_submissions
#[tokio::test(flavor = "multi_thread")]
async fn tracker_merges_a_burst_of_submissions() {
    let (store, runs) = (hourly_tracker(), Arc::new(FakeRuns::default()));
    let (poll, polls) = counting_poll();
    let scheduler = tracker_scheduler(&store, &runs, poll);
    scheduler.start().await.expect("start");
    for _ in 0..100 {
        if *locked(&polls) > 0 {
            break;
        }
        sleep_ms(10).await;
    }

    // A batch of submissions within the merge window shares one burst of checks.
    scheduler.notify_submission();
    scheduler.notify_submission();
    scheduler.notify_submission();
    sleep_ms(500).await;
    scheduler.aclose().await;

    assert_eq!(*locked(&polls), 4);
}

/// A failing poll is recorded with its text and the loop goes on; a poll answering the
/// shutdown ends the loop.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_tracker_poll_is_recorded_for_the_dashboard() {
    let (store, runs) = (hourly_tracker(), Arc::new(FakeRuns::default()));
    let poll = runner(|_scope| async { Err(RunFailure::raised(Raised::new("RuntimeError", "listing failed"))) });
    let scheduler = tracker_scheduler(&store, &runs, poll);
    scheduler.start().await.expect("start");
    for _ in 0..100 {
        if scheduler.tracker_state().last_polled_at.is_some() {
            break;
        }
        sleep_ms(10).await;
    }
    scheduler.aclose().await;
    let state = scheduler.tracker_state();
    assert_eq!(state.last_error.as_deref(), Some("listing failed"));
    assert!(state.last_stats.is_empty());
    assert_eq!(state.last_polled_at.map(|at| at.nanosecond()), Some(0));
}

// py: tests/test_monitor_scheduler.py::test_next_cron_fire_returns_the_next_local_occurrence_in_utc
#[test]
fn next_cron_fire_returns_the_next_local_occurrence_in_utc() {
    let base = Utc.with_ymd_and_hms(2026, 8, 14, 3, 59, 30).single().expect("valid");
    let fire = next_cron_fire("0 4 * * *", base).expect("a fire");

    assert!(fire > base);
    let local = fire.with_timezone(&chrono::Local);
    assert_eq!((local.hour(), local.minute()), (4, 0));

    // In a zone with an offset, the fire is the local 04:00 of that zone, in UTC.
    let shanghai = next_cron_fire_in("0 4 * * *", base, &chrono_tz::Asia::Shanghai).expect("a fire");
    assert_eq!(
        shanghai,
        Utc.with_ymd_and_hms(2026, 8, 14, 20, 0, 0).single().expect("valid")
    );
}

/// Across a DST change the fire keeps the offset the zone had at `now`, as
/// `now.astimezone()` gives cronsim a fixed-offset datetime.
#[test]
fn next_cron_fire_keeps_the_offset_of_now_across_a_dst_change() {
    // Berlin leaves summer time on 2026-10-25 at 03:00 local (01:00 UTC).
    let before = Utc.with_ymd_and_hms(2026, 10, 24, 12, 0, 0).single().expect("valid");
    let fire = next_cron_fire_in("0 4 * * *", before, &chrono_tz::Europe::Berlin).expect("a fire");
    // 04:00 at +02:00, although Berlin is at +01:00 by then.
    assert_eq!(
        fire,
        Utc.with_ymd_and_hms(2026, 10, 25, 2, 0, 0).single().expect("valid")
    );
}

// py: tests/test_monitor_scheduler.py::test_next_cron_fire_schedules_nothing_for_an_unparsable_expression
#[test]
fn next_cron_fire_schedules_nothing_for_an_unparsable_expression() {
    assert_eq!(next_cron_fire("every day at four", Utc::now()), None);
    // What cronsim refuses but croner would take schedules nothing either.
    for refused in ["@daily", "0 4 * * ?", "0 0 15W * *", "0 4 30 2 *"] {
        assert_eq!(next_cron_fire(refused, Utc::now()), None, "{refused}");
    }
}

/// `goldens/cron/next_fires.json`: every chain cronsim computes, through the port.
#[test]
fn next_cron_fire_follows_every_golden_chain() {
    use std::str::FromStr as _;

    use embyx_core::golden;
    let document = golden::load("cron/next_fires.json");
    let mut compared = 0;
    for case in golden::cases(&document) {
        let expr = golden::string(golden::get(case, "expr"));
        let zone = chrono_tz::Tz::from_str(&golden::string(golden::get(case, "zone"))).expect("zone");
        let mut now: DateTime<Utc> = DateTime::parse_from_rfc3339(&golden::string(golden::get(case, "start")))
            .expect("start")
            .to_utc();
        for fire in golden::list(golden::get(case, "fires")) {
            let want = fire
                .as_str()
                .map(|text| DateTime::parse_from_rfc3339(text).expect("fire").to_utc());
            let have = next_cron_fire_in(&expr, now, &zone);
            assert_eq!(have, want, "{zone} {expr:?} from {now}");
            match have {
                Some(at) => now = at + TimeDelta::seconds(1),
                None => break,
            }
        }
        compared += 1;
    }
    assert_eq!(compared, 1148);
}

// py: tests/test_monitor_scheduler.py::test_update_loop_skips_disabled_pipelines
#[tokio::test(flavor = "multi_thread")]
async fn update_loop_skips_disabled_pipelines() {
    let store = Arc::new(FakeStore {
        rss: Mutex::new(RssConfig {
            enabled: false,
            interval_seconds: 3600,
            ..RssConfig::default()
        }),
        ..FakeStore::default()
    });
    let runs = Arc::new(FakeRuns::default());
    let scheduler = MonitorScheduler::new(parts(&store, &runs));

    scheduler.start().await.expect("start");
    sleep_ms(200).await;
    scheduler.aclose().await;

    assert!(runs.finished(PipelineName::Rss).is_empty());
}

// py: tests/test_monitor_scheduler.py::test_mapping_loop_runs_startup_full_sync_and_watchdog_batch
#[tokio::test(flavor = "multi_thread")]
async fn mapping_loop_runs_startup_full_sync_and_watchdog_batch() {
    let root = tempfile::tempdir().expect("tempdir");
    let src = root.path().join("flat");
    std::fs::create_dir(&src).expect("mkdir");
    let mapping_config = MappingConfig {
        enabled: true,
        src_dir: src.to_str().expect("UTF-8").to_owned(),
        dst_dir: root.path().join("emby").to_str().expect("UTF-8").to_owned(),
        debounce_seconds: 0.01,
        full_sync_interval_seconds: 86_400,
    };
    let store = Arc::new(FakeStore {
        mapping: Mutex::new(mapping_config.clone()),
        ..FakeStore::default()
    });
    let runs = Arc::new(FakeRuns::default());
    let mut parts = parts(&store, &runs);
    parts.mapping_factory = Arc::new(move || {
        Box::new(MappingPipeline::new(
            mapping_config.clone(),
            embyx_core::avid::AvidParser::default(),
        ))
    });
    let scheduler = MonitorScheduler::new(parts);
    scheduler.start().await.expect("start");

    for _ in 0..100 {
        if !runs.finished(PipelineName::Mapping).is_empty() {
            break;
        }
        sleep_ms(20).await;
    }
    let startup_runs = runs.finished(PipelineName::Mapping);
    assert_eq!(startup_runs.len(), 1);
    assert_eq!(startup_runs[0].trigger, RunTrigger::Startup);

    let strm = src.join("ABC-123.strm");
    std::fs::write(&strm, "/cloud/abc.mp4").expect("write");
    scheduler.on_fs_event(&strm, false);

    for _ in 0..200 {
        if runs.finished(PipelineName::Mapping).len() >= 2 {
            break;
        }
        sleep_ms(20).await;
    }
    scheduler.aclose().await;

    let watchdog_runs: Vec<_> = runs
        .finished(PipelineName::Mapping)
        .into_iter()
        .filter(|run| run.trigger == RunTrigger::Watchdog)
        .collect();
    assert_eq!(watchdog_runs.len(), 1);
    assert_eq!(watchdog_runs[0].stats.get("files_updated"), Some(1));
    assert!(root.path().join("emby").join("ABC-123").join("ABC-123.strm").exists());
}

/// The watcher itself: a `.strm` written under the source after startup is mapped by an
/// incremental run without anyone calling `on_fs_event`, and a directory moved in brings
/// its files.
#[tokio::test(flavor = "multi_thread")]
async fn the_watcher_feeds_incremental_runs() {
    let root = tempfile::tempdir().expect("tempdir");
    let src = root.path().join("flat");
    std::fs::create_dir(&src).expect("mkdir");
    let staged = root.path().join("staged");
    std::fs::create_dir_all(staged.join("batch")).expect("mkdir");
    std::fs::write(staged.join("batch").join("DEF-456.strm"), "/cloud/def.mp4").expect("write");
    let mapping_config = MappingConfig {
        enabled: true,
        src_dir: src.to_str().expect("UTF-8").to_owned(),
        dst_dir: root.path().join("emby").to_str().expect("UTF-8").to_owned(),
        debounce_seconds: 0.05,
        full_sync_interval_seconds: 86_400,
    };
    let store = Arc::new(FakeStore {
        mapping: Mutex::new(mapping_config.clone()),
        ..FakeStore::default()
    });
    let runs = Arc::new(FakeRuns::default());
    let mut parts = parts(&store, &runs);
    parts.mapping_factory = Arc::new(move || {
        Box::new(MappingPipeline::new(
            mapping_config.clone(),
            embyx_core::avid::AvidParser::default(),
        ))
    });
    let scheduler = MonitorScheduler::new(parts);
    scheduler.start().await.expect("start");
    for _ in 0..100 {
        if !runs.finished(PipelineName::Mapping).is_empty() && scheduler.observed_dir().is_some() {
            break;
        }
        sleep_ms(20).await;
    }
    assert_eq!(scheduler.observed_dir().as_deref(), src.to_str());

    std::fs::write(src.join("ABC-123.strm"), "/cloud/abc.mp4").expect("write");
    // A directory moved in from outside the tree: notify reports only the directory.
    std::fs::rename(staged.join("batch"), src.join("batch")).expect("move in");
    let emby = root.path().join("emby");
    for _ in 0..200 {
        if emby.join("ABC-123").join("ABC-123.strm").exists()
            && emby.join("batch").join("DEF-456").join("DEF-456.strm").exists()
        {
            break;
        }
        sleep_ms(20).await;
    }
    scheduler.aclose().await;

    assert!(emby.join("ABC-123").join("ABC-123.strm").exists());
    assert!(emby.join("batch").join("DEF-456").join("DEF-456.strm").exists());
    assert!(
        runs.finished(PipelineName::Mapping)
            .iter()
            .any(|run| run.trigger == RunTrigger::Watchdog)
    );
    assert_eq!(scheduler.observed_dir(), None);
}

// py: tests/test_monitor_scheduler.py::test_playlists_pipeline_is_unavailable_until_a_runner_is_wired
#[tokio::test(flavor = "multi_thread")]
async fn playlists_pipeline_is_unavailable_until_a_runner_is_wired() {
    let (store, runs) = (Arc::new(FakeStore::default()), Arc::new(FakeRuns::default()));
    let scheduler = MonitorScheduler::new(parts(&store, &runs));

    let status = scheduler
        .status()
        .into_iter()
        .find(|entry| entry.pipeline == PipelineName::Playlists)
        .expect("a playlists entry");
    assert!(!status.configured);
    assert_eq!(
        status.reason.as_deref(),
        Some("playlist sync is not available in this deployment")
    );
    assert!(matches!(
        scheduler.trigger(PipelineName::Playlists).await,
        Err(TriggerError::NotConfigured { .. })
    ));
    scheduler.aclose().await;
}

// py: tests/test_monitor_scheduler.py::test_playlists_runner_is_triggered_and_scheduled_by_its_own_config
#[tokio::test(flavor = "multi_thread")]
async fn playlists_runner_is_triggered_and_scheduled_by_its_own_config() {
    let store = Arc::new(FakeStore {
        playlists: Mutex::new(PlaylistsConfig {
            enabled: true,
            interval_seconds: 3600,
            ..PlaylistsConfig::default()
        }),
        ..FakeStore::default()
    });
    let runs = Arc::new(FakeRuns::default());
    let calls = Arc::new(Mutex::new(Vec::<&str>::new()));
    let mut parts = parts(&store, &runs);
    parts.rss_runner = runner(|_scope| async { Ok(()) });
    parts.archive_runner = runner(|_scope| async { Ok(()) });
    parts.rss_ready = not_ready("off");
    parts.archive_ready = not_ready("off");
    parts.mapping_ready = not_ready("off");
    let recorded = Arc::clone(&calls);
    parts.playlists_runner = Some(runner(move |scope: RunScope| {
        let recorded = Arc::clone(&recorded);
        async move {
            locked(&recorded).push("run");
            scope.ctx.add("lists_synced", 3);
            Ok(())
        }
    }));
    parts.playlists_ready = Some(ready());
    let scheduler = MonitorScheduler::new(parts);
    scheduler.start().await.expect("start");
    sleep_ms(50).await;
    // The loop ran once at start-up and now waits out the interval.
    assert_eq!(*locked(&calls), vec!["run"]);
    let status = scheduler
        .status()
        .into_iter()
        .find(|entry| entry.pipeline == PipelineName::Playlists)
        .expect("a playlists entry");
    assert!(status.enabled);
    let next = status.next_scheduled_at.expect("scheduled");
    assert!(next > Utc::now() + TimeDelta::minutes(50));

    let run_id = scheduler.trigger(PipelineName::Playlists).await.expect("started");
    sleep_ms(50).await;
    assert_eq!(runs.record(&run_id).state, RunState::Completed);
    assert_eq!(runs.record(&run_id).stats, Counters::from([("lists_synced", 3)]));
    assert_eq!(*locked(&calls), vec!["run", "run"]);
    scheduler.aclose().await;
}

/// Shutdown during a run: the runner answers its token, the run is recorded as cancelled
/// by shutdown, and `aclose` returns only after the runner's settle step.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_cancels_a_run_after_its_settle_step() {
    let (store, runs) = (Arc::new(FakeStore::default()), Arc::new(FakeRuns::default()));
    let settled = Arc::new(AtomicBool::new(false));
    let mut parts = parts(&store, &runs);
    let flag = Arc::clone(&settled);
    parts.rss_runner = runner(move |scope: RunScope| {
        let flag = Arc::clone(&flag);
        async move {
            until_shutdown(&scope.shutdown, std::future::pending(), async move {
                sleep_ms(50).await;
                flag.store(true, Ordering::SeqCst);
            })
            .await
        }
    });
    let scheduler = MonitorScheduler::new(parts);
    let run_id = scheduler.trigger(PipelineName::Rss).await.expect("started");
    let running = scheduler
        .status()
        .into_iter()
        .find(|entry| entry.pipeline == PipelineName::Rss)
        .expect("rss");
    assert_eq!(running.running_run_id.as_deref(), Some(run_id.as_str()));

    scheduler.aclose().await;

    assert!(settled.load(Ordering::SeqCst));
    let record = runs.record(&run_id);
    assert_eq!(record.state, RunState::Cancelled);
    assert_eq!(record.log_tail, vec!["WARNING: run cancelled by shutdown".to_owned()]);
}

/// A status reports the next time only for an enabled, runnable pipeline.
#[tokio::test(flavor = "multi_thread")]
async fn status_hides_the_next_time_of_a_pipeline_that_cannot_run() {
    let store = Arc::new(FakeStore {
        rss: Mutex::new(RssConfig {
            enabled: true,
            interval_seconds: 3600,
            ..RssConfig::default()
        }),
        ..FakeStore::default()
    });
    let runs = Arc::new(FakeRuns::default());
    let mut parts = parts(&store, &runs);
    parts.archive_ready = not_ready("archive source, destination, and mapping must be configured");
    let scheduler = MonitorScheduler::new(parts);
    scheduler.start().await.expect("start");
    for _ in 0..100 {
        if scheduler.next().rss.is_some() {
            break;
        }
        sleep_ms(10).await;
    }
    let status: HashMap<PipelineName, PipelineStatus> = scheduler
        .status()
        .into_iter()
        .map(|entry| (entry.pipeline, entry))
        .collect();
    scheduler.aclose().await;

    let rss = &status[&PipelineName::Rss];
    assert!(rss.enabled && rss.configured);
    let next = rss.next_scheduled_at.expect("scheduled");
    assert!(next > Utc::now() + TimeDelta::minutes(50));
    let archive = &status[&PipelineName::Archive];
    assert!(!archive.enabled && !archive.configured);
    assert_eq!(archive.next_scheduled_at, None);
    assert_eq!(
        archive.reason.as_deref(),
        Some("archive source, destination, and mapping must be configured")
    );
    assert_eq!(
        status.keys().copied().collect::<std::collections::BTreeSet<_>>().len(),
        4
    );
}
