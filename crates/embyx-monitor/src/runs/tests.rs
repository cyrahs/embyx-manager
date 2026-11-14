//! Ported from `tests/test_monitor_runs.py`, against a temporary PostgreSQL database.

use chrono::TimeZone as _;
use embyx_db::testdb::TestDatabase;

use super::*;
use crate::support::fixed_clock;

struct Fixture {
    test: TestDatabase,
    database: Database,
    runs: PipelineRunRepository,
}

impl Fixture {
    async fn new() -> Self {
        let test = TestDatabase::create().await;
        let database = test.database();
        let runs = PipelineRunRepository::new(database.clone());
        Self { test, database, runs }
    }

    async fn finish(self) {
        self.database.close().await;
        self.test.drop_database().await;
    }
}

async fn finish_empty(runs: &PipelineRunRepository, run_id: &str) {
    runs.finish_run(run_id, RunState::Completed, &Counters::new(), &[], &[])
        .await
        .expect("finish");
}

// py: tests/test_monitor_runs.py::test_run_lifecycle_round_trip
#[tokio::test]
async fn run_lifecycle_round_trip() {
    let fixture = Fixture::new().await;
    let runs = &fixture.runs;

    let run_id = runs
        .start_run(PipelineName::Rss, RunTrigger::Scheduled)
        .await
        .expect("start");
    let started = runs.get_run(&run_id).await.expect("get").expect("a run");
    assert_eq!(started.state, RunState::Running);
    assert_eq!(started.finished_at, None);

    runs.finish_run(
        &run_id,
        RunState::Completed,
        &Counters::from([("items", 3), ("magnets_found", 2)]),
        &[],
        &["line-1".to_owned(), "line-2".to_owned()],
    )
    .await
    .expect("finish");

    let finished = runs.get_run(&run_id).await.expect("get").expect("a run");
    assert_eq!(finished.state, RunState::Completed);
    assert_eq!(finished.stats, Counters::from([("items", 3), ("magnets_found", 2)]));
    assert_eq!(finished.log_tail, ["line-1", "line-2"]);
    assert!(finished.finished_at.is_some());
    fixture.finish().await;
}

// py: tests/test_monitor_runs.py::test_list_and_latest_runs_are_ordered
#[tokio::test]
async fn list_and_latest_runs_are_ordered() {
    let fixture = Fixture::new().await;
    let runs = &fixture.runs;
    let first = runs
        .start_run(PipelineName::Mapping, RunTrigger::Startup)
        .await
        .expect("start");
    finish_empty(runs, &first).await;
    let second = runs
        .start_run(PipelineName::Mapping, RunTrigger::Watchdog)
        .await
        .expect("start");
    let other = runs
        .start_run(PipelineName::Rss, RunTrigger::Scheduled)
        .await
        .expect("start");

    let mapping_runs = runs.list_runs(Some(PipelineName::Mapping), 20, 0).await.expect("list");
    assert_eq!(
        mapping_runs.iter().map(|run| run.run_id.as_str()).collect::<Vec<_>>(),
        [second.as_str(), first.as_str()]
    );

    let latest = runs
        .latest_run(PipelineName::Mapping)
        .await
        .expect("latest")
        .expect("a run");
    assert_eq!(latest.run_id, second);

    let mut everything: Vec<String> = runs
        .list_runs(None, 20, 0)
        .await
        .expect("list")
        .into_iter()
        .map(|run| run.run_id)
        .collect();
    everything.sort();
    let mut expected = vec![first, second, other];
    expected.sort();
    assert_eq!(everything, expected);
    fixture.finish().await;
}

// py: tests/test_monitor_runs.py::test_fail_stale_running_marks_orphans
#[tokio::test]
async fn fail_stale_running_marks_orphans() {
    let fixture = Fixture::new().await;
    let runs = &fixture.runs;
    let orphan = runs
        .start_run(PipelineName::Archive, RunTrigger::Scheduled)
        .await
        .expect("start");

    let count = runs.fail_stale_running("process restarted").await.expect("fail");

    assert_eq!(count, 1);
    let found = runs.get_run(&orphan).await.expect("get").expect("a run");
    assert_eq!(found.state, RunState::Failed);
    assert_eq!(found.errors, ["process restarted"]);
    fixture.finish().await;
}

// py: tests/test_monitor_runs.py::test_prune_keeps_recent_runs_per_pipeline
#[tokio::test]
async fn prune_keeps_recent_runs_per_pipeline() {
    let fixture = Fixture::new().await;
    let runs = &fixture.runs;
    let mut kept = Vec::new();
    for _ in 0..4 {
        let run_id = runs
            .start_run(PipelineName::Rss, RunTrigger::Scheduled)
            .await
            .expect("start");
        finish_empty(runs, &run_id).await;
        kept.push(run_id);
    }

    let deleted = runs.prune(2).await.expect("prune");

    assert_eq!(deleted, 2);
    let remaining = runs.list_runs(Some(PipelineName::Rss), 10, 0).await.expect("list");
    assert_eq!(
        remaining.iter().map(|run| run.run_id.as_str()).collect::<Vec<_>>(),
        [kept[3].as_str(), kept[2].as_str()]
    );
    fixture.finish().await;
}

// py: tests/test_monitor_runs.py::test_every_pipeline_can_start_a_run
#[tokio::test]
async fn every_pipeline_can_start_a_run() {
    // The table's check constraint spells out the pipeline names; a new PipelineName must be
    // added there too.
    let fixture = Fixture::new().await;
    let runs = &fixture.runs;

    for pipeline in PipelineName::ALL {
        let run_id = runs.start_run(*pipeline, RunTrigger::Manual).await.expect("start");
        let found = runs.get_run(&run_id).await.expect("get").expect("a run");
        assert_eq!(found.pipeline, *pipeline);
    }
    fixture.finish().await;
}

/// The stored columns are `json.dumps(...)` in Python's default form: ASCII-only, `", "` and
/// `": "`, counters in insertion order (the dashboard and either build read these bytes).
#[tokio::test]
async fn finished_runs_store_python_s_json_bytes() {
    let fixture = Fixture::new().await;
    let at = Utc.with_ymd_and_hms(2026, 9, 1, 4, 0, 0).single().expect("valid");
    let runs = PipelineRunRepository::with_clock(fixture.database.clone(), fixed_clock(at));
    let run_id = runs
        .start_run(PipelineName::Rss, RunTrigger::Manual)
        .await
        .expect("start");
    runs.finish_run(
        &run_id,
        RunState::Failed,
        &Counters::from([("zeta", 1), ("alpha", 22)]),
        &["ERROR: 失败 \"x\"".to_owned()],
        &[
            "Added magnet for ABC-123".to_owned(),
            "WARNING: 演员\u{1f600}".to_owned(),
        ],
    )
    .await
    .expect("finish");

    let (stats, errors, log, finished_at): (String, String, String, DateTime<Utc>) =
        sqlx::query_as("SELECT stats_json, errors_json, log_json, finished_at FROM pipeline_runs WHERE run_id = $1")
            .bind(&run_id)
            .fetch_one(fixture.test.pool())
            .await
            .expect("row");
    assert_eq!(stats, r#"{"zeta": 1, "alpha": 22}"#);
    assert_eq!(errors, r#"["ERROR: \u5931\u8d25 \"x\""]"#);
    assert_eq!(
        log,
        r#"["Added magnet for ABC-123", "WARNING: \u6f14\u5458\ud83d\ude00"]"#
    );
    assert_eq!(finished_at, at);
    let read = runs.get_run(&run_id).await.expect("get").expect("a run");
    assert_eq!(read.errors, ["ERROR: 失败 \"x\""]);
    assert_eq!(read.log_tail[1], "WARNING: 演员\u{1f600}");
    assert_eq!(run_id.len(), 32);
    fixture.finish().await;
}
