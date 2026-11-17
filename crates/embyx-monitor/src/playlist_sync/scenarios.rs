//! The playlist scenario (`tools/scenarios/specs/playlists/`): the Rust port of
//! `tools/scenarios/operations/playlists.py` runs [`PlaylistSyncPipeline`] over the real
//! repository on the scenario database, the real [`EmbyClient`] against the scripted Emby fake
//! and the recording ranking-source stub, and must render the bytes the Python baseline
//! committed under `goldens/scenarios/playlists/`.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeDelta, Utc};
use embyx_clients::emby::EmbyClient;
use embyx_clients::jinjier::JinjierError;
use embyx_clients::jinjier::fixtures::make_database;
use embyx_clients::scenario::ScenarioFakes;
use embyx_core::avid::AvidParser;
use embyx_core::pyjson::{Dict, Value};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::testdb::TestDatabase;

use super::{PlaylistServer, PlaylistSyncPipeline, RankingSource};
use crate::archive::scenarios::{entry, error_value, flag, int, steps_of, text};
use crate::playlists::{PlaylistRepository, PlaylistStore};
use crate::reports::RunContext;
use crate::support::BoxFuture;

const API_KEY: &str = "scenario-key";

/// `_Source`: the ranking source stub; every call is recorded as a `jinjier` call.
struct StubSource {
    recorder: Recorder,
    answers: Mutex<Value>,
}

impl StubSource {
    fn answers(&self) -> Value {
        self.answers.lock().expect("answers").clone()
    }

    fn call(&self, method: &str, request: Dict, outcome: Result<Value, &JinjierError>) {
        let mut call = Dict::new();
        call.insert("fake", Value::from("jinjier"));
        call.insert("method", Value::from(method));
        call.insert("request", Value::Dict(request));
        match outcome {
            Ok(response) => call.insert("response", response),
            Err(error) => {
                let mut value = Dict::new();
                value.insert("type", Value::from("JinjierError"));
                value.insert("message", Value::Str(error.to_string()));
                call.insert("error", Value::Dict(value));
            }
        }
        self.recorder.record(Value::Dict(call));
    }
}

impl RankingSource for StubSource {
    fn discover_database_name(&self) -> BoxFuture<'_, Result<String, JinjierError>> {
        let answers = self.answers();
        let result = if let Some(name) = answers.get("name").and_then(Value::as_str) {
            self.call("discover_database_name", Dict::new(), Ok(Value::from(name)));
            Ok(name.to_owned())
        } else {
            let error = JinjierError("page changed".to_owned());
            self.call("discover_database_name", Dict::new(), Err(&error));
            Err(error)
        };
        Box::pin(async move { result })
    }

    fn download_database<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Vec<u8>, JinjierError>> {
        let answers = self.answers();
        let mut request = Dict::new();
        request.insert("name", Value::from(name));
        let result = if flag(&answers, "broken_download") {
            let error = JinjierError(format!("{name}: HTTP 503"));
            self.call("download_database", request, Err(&error));
            Err(error)
        } else {
            let rows: Vec<(i64, i64, String, String)> = answers
                .get("rows")
                .and_then(Value::as_list)
                .unwrap_or_default()
                .iter()
                .map(|row| {
                    let fields = row.as_list().expect("a [kind, number, name, note] row");
                    (
                        int(&fields[0]),
                        int(&fields[1]),
                        fields[2].as_str().expect("a name").to_owned(),
                        fields[3].as_str().expect("a note").to_owned(),
                    )
                })
                .collect();
            let mut response = Dict::new();
            response.insert("rows", Value::Int(i128::try_from(rows.len()).expect("small")));
            self.call("download_database", request, Ok(Value::Dict(response)));
            let borrowed: Vec<(i64, i64, &str, &str)> = rows
                .iter()
                .map(|(kind, number, name, note)| (*kind, *number, name.as_str(), note.as_str()))
                .collect();
            Ok(make_database(&borrowed))
        };
        Box::pin(async move { result })
    }
}

async fn steps(spec: &Spec, database: &TestDatabase, recorder: &Recorder, emby_url: &str) -> Value {
    let start = DateTime::parse_from_rfc3339(spec.clock())
        .expect("an RFC 3339 clock")
        .with_timezone(&Utc);
    let current = Arc::new(Mutex::new(start));
    let now = || *current.lock().expect("clock");
    let args = spec.section("args");
    let app = database.database();
    let repository = Arc::new(PlaylistRepository::new(app.clone()));
    let source = Arc::new(StubSource {
        recorder: recorder.clone(),
        answers: Mutex::new(args.get("source").cloned().unwrap_or_else(|| Value::Dict(Dict::new()))),
    });
    let emby = EmbyClient::new(emby_url, API_KEY).expect("an Emby client");
    let parser = AvidParser::default();
    let reading = Arc::clone(&current);
    let pipeline = PlaylistSyncPipeline::new(
        Arc::clone(&repository) as Arc<dyn PlaylistStore>,
        Arc::clone(&source) as Arc<dyn RankingSource>,
        Arc::new(emby) as Arc<dyn PlaylistServer>,
        Arc::new(move |title: &str| parser.get_avid(title)),
    )
    .with_clock(Arc::new(move || *reading.lock().expect("clock")));
    let mut reported = Vec::new();
    for (op, step) in steps_of(spec) {
        if let Some(seconds) = step.get("advance_seconds") {
            *current.lock().expect("clock") += TimeDelta::seconds(int(seconds));
        }
        let run = RunContext::new("scenario-playlists");
        let outcome = match op.as_str() {
            "run" => {
                if let Some(answers) = step.get("source") {
                    *source.answers.lock().expect("answers") = answers.clone();
                }
                if flag(&step, "stop_before") {
                    run.request_stop();
                }
                pipeline
                    .run(&run)
                    .await
                    .map(|()| Value::Null)
                    .map_err(|error| error_value(&error))
            }
            "set_enabled" => repository
                .set_enabled(text(&step, "key"), flag(&step, "enabled"), now())
                .await
                .map(|record| {
                    record.map_or(Value::Null, |record| {
                        let mut value = Dict::new();
                        value.insert("key", Value::Str(record.key));
                        value.insert("enabled", Value::Bool(record.enabled));
                        Value::Dict(value)
                    })
                })
                .map_err(|error| {
                    let mut value = Dict::new();
                    value.insert("type", Value::from(error.class_name()));
                    value.insert("message", Value::Str(error.to_string()));
                    Value::Dict(value)
                }),
            other => panic!("unknown step {other:?}"),
        };
        reported.push(entry(&op, outcome, &run));
    }
    app.close().await;
    let mut result = Dict::new();
    result.insert("steps", Value::List(reported));
    Value::Dict(result)
}

/// `playlists.steps`.
async fn playlist_steps(name: &str) {
    let spec = Spec::load(name);
    let seeded = Seeded::new(&spec);
    let database = TestDatabase::create().await;
    embyx_db::scenario::seed(database.pool(), &spec).await;
    let recorder = Recorder::new();
    let fakes = ScenarioFakes::start(&spec, &recorder).await;
    let result = steps(&spec, &database, &recorder, &fakes.emby.url).await;
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

#[tokio::test]
async fn playlist_sync_matches_the_python_transcript() {
    playlist_steps("playlists/sync").await;
}
