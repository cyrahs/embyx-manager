//! The RSS scenario (`tools/scenarios/specs/rss/`): the Rust port of
//! `tools/scenarios/operations/rss.py` runs [`RssPipeline`] over the real subscription and
//! acquisition repositories on the scenario database, `AsyncCloudDrive` against the Rust
//! fake, an [`ArchivePipeline`] over the spec's library seen through [`RecordingFs`], and the
//! recording feed and AVBase stubs, and must render the bytes the Python baseline committed
//! under `goldens/scenarios/rss/`.
//!
//! [`RecordingFs`]: crate::fs::recording::RecordingFs

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use chrono::NaiveDate;
use embyx_clients::avbase::AvbaseTalent;
use embyx_clients::clouddrive::{AsyncCloudDrive, ClientOptions};
use embyx_clients::feeds::FeedFetchError;
use embyx_clients::http::HttpxError;
use embyx_clients::javbus::{JavBusError, JavBusMagnet};
use embyx_clients::net::StatusError;
use embyx_clients::net::retry::RecordingSleeper;
use embyx_clients::scenario::ScenarioFakes;
use embyx_core::avid::AvidParser;
use embyx_core::config::Section as _;
use embyx_core::config::lax::Mode;
use embyx_core::config::models::RssConfig;
use embyx_core::pyjson::{Dict, Value};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::testdb::TestDatabase;

use super::{CastLookup, CreditedWork, FeedFetcher, FetchError, LookupError, RssParts, RssPipeline, TalentLookup};
use crate::acquisitions::{AcquisitionRepository, Ledger};
use crate::archive::ArchivePipeline;
use crate::archive::scenarios::{
    ScenarioClock, Step, begin_step, config, error_value, int, only_op, recording_fs, steps_of, strings,
};
use crate::fs::Fs;
use crate::intake::{JavBusMagnets, OfflineFiles, SourceError, SukebeiMagnets};
use crate::reports::{PyException, Raised, RunContext};
use crate::subscriptions::SubscriptionRepository;
use crate::support::BoxFuture;

/// `operations/intake.py` `TOKEN`.
const TOKEN: &str = "scenario-token";

fn error_dict(class: &str, message: &str) -> Value {
    let mut error = Dict::new();
    error.insert("type", Value::from(class));
    error.insert("message", Value::from(message));
    Value::Dict(error)
}

fn call(fake: &str, method: &str, request: Dict) -> Dict {
    let mut call = Dict::new();
    call.insert("fake", Value::from(fake));
    call.insert("method", Value::from(method));
    call.insert("request", Value::Dict(request));
    call
}

/// The reason phrase httpx gives the statuses the spec answers with.
fn reason(status: u16) -> &'static str {
    match status {
        404 => "Not Found",
        503 => "Service Unavailable",
        other => panic!("the stub knows no reason phrase for {other}"),
    }
}

/// `_Feeds`: the spec's bodies and statuses; every fetch is recorded.
struct StubFeeds {
    recorder: Recorder,
    feeds: Mutex<Value>,
    stop_after: Mutex<Option<String>>,
    run: Mutex<Option<Arc<RunContext>>>,
}

impl StubFeeds {
    fn answer(&self, url: &str) -> Result<Vec<u8>, FetchError> {
        match self.feeds.lock().expect("feeds").get(url) {
            None => Err(FetchError::Raised(Raised::new("KeyError", format!("'{url}'")))),
            Some(Value::Int(status)) => {
                let status = u16::try_from(*status).expect("a status");
                Err(FetchError::Feed(FeedFetchError::Http(HttpxError::Status(
                    StatusError {
                        status,
                        reason: reason(status).to_owned(),
                        url: url.to_owned(),
                        location: None,
                    },
                ))))
            }
            Some(Value::Str(body)) => Ok(body.as_bytes().to_vec()),
            Some(other) => panic!("feed answer {other:?}"),
        }
    }
}

impl FeedFetcher for StubFeeds {
    fn fetch<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Vec<u8>, FetchError>> {
        let mut request = Dict::new();
        request.insert("url", Value::from(url));
        let mut recorded = call("feed", "fetch", request);
        let result = self.answer(url);
        match &result {
            Ok(body) => {
                let mut response = Dict::new();
                response.insert("bytes", Value::Int(i128::try_from(body.len()).expect("small")));
                recorded.insert("response", Value::Dict(response));
            }
            Err(error) => recorded.insert("error", error_dict(error.class_name(), &error.to_string())),
        }
        if self.stop_after.lock().expect("stop").as_deref() == Some(url)
            && let Some(run) = &*self.run.lock().expect("run")
        {
            run.request_stop();
        }
        self.recorder.record(Value::Dict(recorded));
        Box::pin(std::future::ready(result))
    }
}

/// `_Avbase`: the cast and talent lookups; every call is recorded.
struct StubAvbase {
    recorder: Recorder,
    cast: Value,
    talents: Value,
}

impl StubAvbase {
    fn answer(&self, method: &str, key: &str, value: &str, answers: &Value) -> Result<Option<Value>, LookupError> {
        let mut request = Dict::new();
        request.insert(key, Value::from(value));
        let mut recorded = call("avbase", method, request);
        let answer = answers.get(value).cloned();
        if answer.as_ref().and_then(Value::as_str) == Some("error") {
            recorded.insert("error", error_dict("RuntimeError", "catalog down"));
            self.recorder.record(Value::Dict(recorded));
            return Err(Box::new(Raised::new("RuntimeError", "catalog down")));
        }
        recorded.insert("response", answer.clone().unwrap_or(Value::Null));
        self.recorder.record(Value::Dict(recorded));
        Ok(answer)
    }
}

impl CastLookup for StubAvbase {
    fn lookup<'a>(&'a self, avid: &'a str) -> BoxFuture<'a, Result<Option<CreditedWork>, LookupError>> {
        let result = self.answer("cast", "avid", avid, &self.cast).map(|answer| {
            answer.map(|ids| CreditedWork {
                talent_ids: ids
                    .as_list()
                    .expect("talent ids")
                    .iter()
                    .map(|id| i128::from(int(id)))
                    .collect::<BTreeSet<_>>(),
                release_date: NaiveDate::from_ymd_opt(2026, 9, 1),
            })
        });
        Box::pin(std::future::ready(result))
    }
}

impl TalentLookup for StubAvbase {
    fn lookup<'a>(&'a self, name: &'a str) -> BoxFuture<'a, Result<Option<AvbaseTalent>, LookupError>> {
        let result = self.answer("talent", "name", name, &self.talents).map(|answer| {
            answer.map(|talent| AvbaseTalent {
                talent_id: i128::from(int(talent.get("talent_id").expect("a talent id"))),
                name: talent.get("name").and_then(Value::as_str).expect("a name").to_owned(),
                aliases: talent
                    .get("aliases")
                    .and_then(Value::as_list)
                    .unwrap_or_default()
                    .iter()
                    .map(|alias| alias.as_str().expect("an alias").to_owned())
                    .collect(),
                total_works: 0,
            })
        });
        Box::pin(std::future::ready(result))
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
                Err(SourceError::from(JavBusError::Http(HttpxError::Status(StatusError {
                    status,
                    reason: reason(status).to_owned(),
                    url: format!("https://www.javbus.com/{avid}"),
                    location: None,
                }))))
            }
            Some(Value::List(pairs)) => Ok(pairs
                .iter()
                .map(|pair| {
                    let pair = pair.as_list().expect("[magnet, size]");
                    let size = i128::from(int(&pair[1]));
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

/// `rss.steps`.
async fn rss_steps(name: &str) {
    let spec = Spec::load(name);
    let seeded = Seeded::new(&spec);
    let database = TestDatabase::create().await;
    embyx_db::scenario::seed(database.pool(), &spec).await;
    let recorder = Recorder::new();
    let fakes = ScenarioFakes::start(&spec, &recorder).await;
    let args = spec.section("args");
    let empty = || Value::Dict(Dict::new());
    let fs = recording_fs(&seeded, &args, &recorder);
    let clock = ScenarioClock::new(&spec);
    let cloud = Arc::new(AsyncCloudDrive::connect(&ClientOptions::new(
        fakes.clouddrive.address(),
        TOKEN,
        false,
    )));
    let feeds = Arc::new(StubFeeds {
        recorder: recorder.clone(),
        feeds: Mutex::new(args.get("feeds").cloned().unwrap_or_else(empty)),
        stop_after: Mutex::new(None),
        run: Mutex::new(None),
    });
    let avbase = Arc::new(StubAvbase {
        recorder: recorder.clone(),
        cast: args.get("cast").cloned().unwrap_or_else(empty),
        talents: args.get("talents").cloned().unwrap_or_else(empty),
    });
    let submitted = Arc::new(Mutex::new(0_i128));
    let counting = Arc::clone(&submitted);
    let sleeper = RecordingSleeper::new();
    let pipeline = RssPipeline::new(RssParts {
        config: RssConfig::validate(args.get("rss").expect("an rss section"), Mode::Python)
            .expect("a valid rss section"),
        avid_parser: AvidParser::default(),
        subscriptions: Arc::new(SubscriptionRepository::new(database.database())),
        fetch: Arc::clone(&feeds) as Arc<dyn FeedFetcher>,
        cloud: Arc::clone(&cloud) as Arc<dyn OfflineFiles>,
        sukebei: Arc::new(StubSukebei(args.get("sukebei").cloned().unwrap_or_else(empty))),
        javbus: Arc::new(StubJavBus(args.get("javbus").cloned().unwrap_or_else(empty))),
        ledger: Arc::new(AcquisitionRepository::new(database.database())) as Arc<dyn Ledger>,
        archiver: ArchivePipeline::with_fs(
            config(&seeded, args.get("config").expect("an archive config")),
            AvidParser::default(),
            Arc::clone(&fs) as Arc<dyn Fs>,
        ),
        on_submitted: Some(Arc::new(move || *counting.lock().expect("count") += 1)),
        cast_lookup: Some(Arc::clone(&avbase) as Arc<dyn CastLookup>),
        talent_lookup: Some(avbase as Arc<dyn TalentLookup>),
    })
    .expect("a valid cooldown")
    .with_clock(clock.clock())
    .with_sleeper(sleeper.sleeper());

    let mut reported = Vec::new();
    for (op, step) in steps_of(&spec) {
        let run = match begin_step(&seeded, &fs, &clock, &op, &step) {
            Step::Shaped => {
                reported.push(only_op(&op));
                continue;
            }
            Step::Run(run) => run,
        };
        assert_eq!(op, "run", "unknown step {op:?}");
        *feeds.run.lock().expect("run") = Some(Arc::clone(&run));
        *feeds.stop_after.lock().expect("stop") =
            step.get("stop_after_fetch").and_then(Value::as_str).map(str::to_owned);
        if let Some(replaced) = step.get("feeds") {
            *feeds.feeds.lock().expect("feeds") = replaced.clone();
        }
        *submitted.lock().expect("count") = 0;
        let mut entry = Dict::new();
        entry.insert("op", Value::from(op.as_str()));
        match pipeline.run(&run).await {
            Ok(()) => entry.insert("result", Value::Null),
            Err(error) => entry.insert("error", error_value(&error)),
        }
        entry.insert("submitted", Value::Int(*submitted.lock().expect("count")));
        entry.insert("stats", run.stats().to_value());
        entry.insert("log", strings(run.log_tail()));
        entry.insert("errors", strings(run.errors()));
        reported.push(Value::Dict(entry));
    }
    // The one dropped `AddOfflineFiles` waited out `CLOUD_RETRY`'s first step.
    assert_eq!(sleeper.delays(), [1.0]);
    cloud.close().await;
    let rows = embyx_db::scenario::dump_rows(database.pool(), &spec).await;
    let fakes_dump = fakes.dump();
    drop(fakes);
    let mut result = Dict::new();
    result.insert("steps", Value::List(reported));
    scenario::assert_transcript(Parts {
        spec: &spec,
        seeded: &seeded,
        outcome: scenario::result(Value::Dict(result)),
        calls: recorder.calls(),
        rows,
        fakes: fakes_dump,
    });
    database.drop_database().await;
}

#[tokio::test]
async fn rss_run_matches_the_python_transcript() {
    rss_steps("rss/run").await;
}
