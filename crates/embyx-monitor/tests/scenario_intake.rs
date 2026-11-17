//! The intake scenario (`tools/scenarios/specs/intake/`): the Rust port of
//! `tools/scenarios/operations/intake.py` runs the spec's steps through `AcquisitionIntake`
//! over the ledger in PostgreSQL and `AsyncCloudDrive` against the Rust fake, stores the run
//! with `PipelineRunRepository`, and must render the bytes the Python baseline committed under
//! `goldens/scenarios/intake/`.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use embyx_clients::clouddrive::{AsyncCloudDrive, ClientOptions};
use embyx_clients::http::HttpxError;
use embyx_clients::javbus::{JavBusError, JavBusMagnet};
use embyx_clients::net::StatusError;
use embyx_clients::net::retry::RecordingSleeper;
use embyx_clients::scenario::ScenarioFakes;
use embyx_core::pyjson::{Dict, Value};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::testdb::TestDatabase;
use embyx_monitor::acquisitions::{AcquisitionRepository, Ledger};
use embyx_monitor::intake::{AcquisitionIntake, IntakeParts, JavBusMagnets, OfflineFiles, SourceError, SukebeiMagnets};
use embyx_monitor::reports::{PipelineName, RunContext, RunState, RunTrigger};
use embyx_monitor::runs::PipelineRunRepository;
use embyx_monitor::support::BoxFuture;

/// `operations/intake.py` `TOKEN`.
const TOKEN: &str = "scenario-token";

fn text<'a>(args: &'a Value, key: &str) -> &'a str {
    args.get(key)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("step argument {key:?} is a string"))
}

fn int(value: &Value) -> i128 {
    match value {
        Value::Int(int) => *int,
        other => panic!("expected an int, got {other:?}"),
    }
}

/// `_Sukebei`: `args.sukebei[avid]`, or no magnet.
struct StubSukebei(Value);

impl SukebeiMagnets for StubSukebei {
    fn get_magnet<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<String>, SourceError>> {
        let answer = self.0.get(avid).and_then(Value::as_str).map(str::to_owned);
        Box::pin(std::future::ready(Ok(answer)))
    }
}

/// `_JavBus`: `args.javbus[avid]` as `[magnet, size]` pairs, or the HTTP status it raises.
struct StubJavBus(Value);

impl JavBusMagnets for StubJavBus {
    fn get_magnets<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Vec<JavBusMagnet>, SourceError>> {
        let answer = match self.0.get(avid) {
            None => Ok(Vec::new()),
            Some(Value::Int(status)) => {
                let status = u16::try_from(*status).expect("a status");
                assert_eq!(status, 404, "the stub knows the reason phrase of 404 only");
                Err(SourceError::from(JavBusError::Http(HttpxError::Status(StatusError {
                    status,
                    reason: "Not Found".to_owned(),
                    url: format!("https://www.javbus.com/{avid}"),
                    location: None,
                }))))
            }
            Some(Value::List(pairs)) => Ok(pairs
                .iter()
                .map(|pair| {
                    let pair = pair.as_list().expect("[magnet, size]");
                    let size = int(&pair[1]);
                    JavBusMagnet {
                        magnet: pair[0].as_str().expect("a magnet").to_owned(),
                        size: String::new(),
                        size_int: size,
                        tags: Vec::new(),
                        score: size,
                    }
                })
                .collect()),
            Some(other) => panic!("javbus stub answer {other:?}"),
        };
        Box::pin(std::future::ready(answer))
    }
}

/// `steps`: the intake over the scenario's ledger and CloudDrive; the run stored at the end.
async fn steps(spec: &Spec, database: &TestDatabase, cloud: Arc<AsyncCloudDrive>, sleeper: &RecordingSleeper) -> Value {
    let args = spec.section("args");
    let start = DateTime::parse_from_rfc3339(spec.clock())
        .expect("an RFC 3339 clock")
        .with_timezone(&Utc);
    let current = Arc::new(Mutex::new(start));
    let reading = Arc::clone(&current);
    let clock: embyx_monitor::support::Clock = Arc::new(move || *reading.lock().expect("clock"));
    let ledger = AcquisitionRepository::new(database.database());
    let intake = AcquisitionIntake::new(IntakeParts {
        ledger: Arc::new(ledger) as Arc<dyn Ledger>,
        sukebei: Arc::new(StubSukebei(
            args.get("sukebei").cloned().unwrap_or(Value::Dict(Dict::new())),
        )),
        javbus: Arc::new(StubJavBus(
            args.get("javbus").cloned().unwrap_or(Value::Dict(Dict::new())),
        )),
        cloud: cloud as Arc<dyn OfflineFiles>,
        failed_cooldown_seconds: i64::try_from(int(args.get("failed_cooldown_seconds").expect("a cooldown")))
            .expect("small"),
        on_submitted: None,
        release_date_lookup: None,
    })
    .expect("a valid cooldown")
    .with_clock(Arc::clone(&clock))
    .with_sleeper(sleeper.sleeper());
    let runs = PipelineRunRepository::with_clock(database.database(), clock);
    let run_id = runs
        .start_run(PipelineName::Rss, RunTrigger::Manual)
        .await
        .expect("start_run");
    let run = RunContext::new("scenario-intake");
    let mut reported = Vec::new();
    for step in args.get("steps").and_then(Value::as_list).expect("steps") {
        let op = text(step, "op");
        let step_args = step.get("args").cloned().unwrap_or_else(|| Value::Dict(Dict::new()));
        if let Some(seconds) = step_args.get("advance_seconds") {
            let seconds = i64::try_from(int(seconds)).expect("small");
            *current.lock().expect("clock") += TimeDelta::seconds(seconds);
        }
        let release_date = step_args
            .get("release_date")
            .and_then(Value::as_str)
            .map(|text| NaiveDate::parse_from_str(text, "%Y-%m-%d").expect("an ISO date"));
        let result = match op {
            "enqueue" => Value::from(
                intake
                    .enqueue(
                        text(&step_args, "avid"),
                        text(&step_args, "source"),
                        text(&step_args, "task_dir"),
                        &run,
                        step_args.get("item_magnet").and_then(Value::as_str),
                        release_date,
                    )
                    .await
                    .expect("enqueue")
                    .as_str(),
            ),
            "queue" => Value::from(
                intake
                    .queue(
                        text(&step_args, "avid"),
                        text(&step_args, "source"),
                        text(&step_args, "task_dir"),
                        &run,
                        release_date,
                    )
                    .await
                    .expect("queue")
                    .as_str(),
            ),
            "retry_due" => {
                let processed = intake
                    .retry_due(&run, step_args.get("fallback_task_dir").and_then(Value::as_str), 50)
                    .await
                    .expect("retry_due");
                Value::Int(i128::try_from(processed).expect("small"))
            }
            other => panic!("unknown step {other:?}"),
        };
        let mut entry = Dict::new();
        entry.insert("op", Value::from(op));
        entry.insert("result", result);
        reported.push(Value::Dict(entry));
    }
    runs.finish_run(
        &run_id,
        RunState::Completed,
        &run.stats(),
        &run.errors(),
        &run.log_tail(),
    )
    .await
    .expect("finish_run");
    let mut result = Dict::new();
    result.insert("steps", Value::List(reported));
    Value::Dict(result)
}

// One retried RPC failure: CLOUD_RETRY waited once, for a second.
#[tokio::test]
async fn intake_steps_matches_the_python_transcript() {
    let spec = Spec::load("intake/steps");
    let seeded = Seeded::new(&spec);
    let database = TestDatabase::create().await;
    embyx_db::scenario::seed(database.pool(), &spec).await;
    let recorder = Recorder::new();
    let fakes = ScenarioFakes::start(&spec, &recorder).await;
    let cloud = Arc::new(AsyncCloudDrive::connect(&ClientOptions::new(
        fakes.clouddrive.address(),
        TOKEN,
        false,
    )));
    let sleeper = RecordingSleeper::new();
    let result = steps(&spec, &database, Arc::clone(&cloud), &sleeper).await;
    cloud.close().await;
    assert_eq!(sleeper.delays(), [1.0]);
    let rows = embyx_db::scenario::dump_rows(database.pool(), &spec).await;
    let fakes_dump = fakes.dump();
    drop(fakes);
    scenario::assert_transcript(Parts {
        spec: &spec,
        seeded: &seeded,
        outcome: scenario::result(result),
        calls: recorder.calls(),
        rows,
        fakes: fakes_dump,
    });
    database.drop_database().await;
}
