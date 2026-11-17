//! `home_worker_requests.json`: the bytes of every request the worker sends and how it reads
//! the answer (contract 5). Python's goldens come from httpx's `MockTransport`; here the
//! client talks to a local HTTP/1.1 server, so the server part of each URL differs.

use std::fs;
use std::sync::Arc;

use embyx_core::golden;
use embyx_core::pyjson::{self, Dict, Value};
use sha2::{Digest as _, Sha256};

use super::{cases, object};
use crate::chunks::{LocalChunk, RateLimiter, RateMeter, SystemClock};
use crate::config::WorkerConfig;
use crate::run;
use crate::server::{CallError, Cluster as _, HttpServer};
use crate::testing::{FakeHttp, Recorded, Reply};

const SHOWN_HEADERS: [&str; 8] = [
    "authorization",
    "content-type",
    "content-length",
    "transfer-encoding",
    "x-chunk-start",
    "x-chunk-duration",
    "x-chunk-frames",
    "x-chunk-sha256",
];

/// The golden's server with its host swapped for the local one.
fn local(server: &str, base: &str) -> String {
    let rest = server
        .strip_prefix("http://")
        .and_then(|rest| rest.find('/').map(|at| &rest[at..]))
        .unwrap_or("");
    format!("{base}{rest}")
}

/// A request as the golden shows it, headers sorted (their order is the client's own).
fn view(request: &Recorded, base: &str) -> Value {
    let mut headers: Vec<(String, String)> = request
        .headers
        .iter()
        .filter(|(name, _)| SHOWN_HEADERS.contains(&name.as_str()))
        .cloned()
        .collect();
    headers.sort();
    let body = if request.body.len() < 4096 {
        Value::Str(String::from_utf8(request.body.clone()).expect("UTF-8 body"))
    } else {
        Value::Null
    };
    let headers: Dict = headers
        .into_iter()
        .map(|(name, value)| (name, Value::Str(value)))
        .collect();
    object([
        ("method", Value::Str(request.method.clone())),
        ("url", Value::Str(format!("{base}{}", request.path))),
        ("headers", Value::Dict(headers)),
        ("body", body),
        (
            "body_bytes",
            Value::Int(i128::try_from(request.body.len()).expect("size")),
        ),
        ("body_sha256", Value::Str(hex::encode(Sha256::digest(&request.body)))),
    ])
}

/// The golden's request with its headers sorted and its URL on the local server.
fn wanted(request: &Value, server: &str, base: &str) -> Value {
    let mut request = super::dict(request);
    let url = golden::string(request.get("url").expect("url"));
    request.insert("url", Value::Str(url.replacen(server, &local(server, base), 1)));
    let mut headers: Vec<(String, Value)> = super::dict(request.get("headers").expect("headers"))
        .iter()
        .map(|(key, value)| (key.as_str().expect("name").to_owned(), value.clone()))
        .collect();
    headers.sort_by(|a, b| a.0.cmp(&b.0));
    request.insert("headers", Value::Dict(headers.into_iter().collect()));
    Value::Dict(request)
}

fn outcome(result: Result<Dict, CallError>) -> Value {
    match result {
        Ok(dict) => object([("value", Value::Dict(dict))]),
        Err(CallError::Server(error)) => object([(
            "server_error",
            object([
                ("status", Value::Int(i128::from(error.status))),
                ("code", Value::Str(error.code.clone())),
                ("details", Value::Dict(error.details.clone())),
                ("str", Value::Str(error.to_string())),
                ("details_repr", Value::Str(error.details_repr())),
            ]),
        )]),
        Err(other) => object([("error", object([("type", Value::Str(format!("{other:?}")))]))]),
    }
}

fn expected_outcome(case: &Value) -> Value {
    for key in ["value", "server_error", "error"] {
        if let Some(value) = case.get(key) {
            return object([(key, value.clone())]);
        }
    }
    panic!("the case has no outcome: {case:?}")
}

#[test]
fn requests_and_answers_match_python() {
    let root = tempfile::tempdir().expect("temporary directory");
    let chunk_path = root.path().join("chunk-00003.mkv");
    let mut body: Vec<u8> = (0..1100).flat_map(|_| 0..=255_u8).collect();
    body.extend_from_slice(b"tail");
    fs::write(&chunk_path, &body).expect("chunk");
    let chunk = LocalChunk {
        part_index: 1,
        index: 3,
        start: 6.006,
        duration: 59.993_333_333,
        frames: 3596,
        bytes: i128::try_from(body.len()).expect("size"),
        sha256: hex::encode(Sha256::digest(&body)),
        path: chunk_path,
    };
    let report = pyjson::loads(
        r#"{"name": "pc-名前", "version": "1", "task_id": 7, "state": "encoding",
            "parts": [{"index": 0, "fps": 29.5, "position": 12.000001}, {"index": 2, "fps": 0.0, "position": 0.0}],
            "upload_bps": 1234567.891, "queue_chunks": 2, "queue_bytes": 3000000000}"#,
    )
    .expect("report");
    let mut idle = WorkerConfig::new("x");
    idle.name = "pc".to_owned();
    let claim_report = run::idle_report(&idle);
    let mut checked = 0;
    for case in cases("requests") {
        let call = golden::string(golden::get(&case, "call"));
        if call == "upload_chunk" {
            assert_eq!(golden::int(golden::get(&case, "size")), chunk.bytes);
            continue;
        }
        let answer = golden::get(&case, "answer");
        let status = u16::try_from(golden::int(golden::get(answer, "status"))).expect("status");
        let reply = Reply::Answer(
            status,
            golden::string(golden::get(answer, "type")),
            golden::string(golden::get(answer, "body")).into_bytes(),
        );
        let fake = FakeHttp::start(move |_| reply.clone());
        let server_url = golden::string(golden::get(&case, "server"));
        let mut config = WorkerConfig::new(local(&server_url, &fake.base()));
        config.token = golden::string(golden::get(&case, "token"));
        let server = HttpServer::new(&config);
        let clock = Arc::new(SystemClock::default());
        let result = match call.as_str() {
            "claim" => server.claim(&claim_report),
            "heartbeat" => server.heartbeat(&report),
            "upload" => server.upload(7, &chunk, &RateLimiter::new(0.0, clock.clone()), &RateMeter::new(clock)),
            "finish" => server.finish(7, 2),
            other => panic!("unknown call {other}"),
        };
        assert_eq!(outcome(result), expected_outcome(&case), "case {case:?}");
        let seen: Vec<Value> = fake
            .requests()
            .iter()
            .map(|request| view(request, &fake.base()))
            .collect();
        let want: Vec<Value> = golden::list(golden::get(&case, "requests"))
            .iter()
            .map(|request| wanted(request, &server_url, &fake.base()))
            .collect();
        assert_eq!(seen, want, "case {case:?}");
        assert_eq!(
            server.part_url(7, 2),
            golden::string(golden::get(&case, "part_url")).replacen(&server_url, &local(&server_url, &fake.base()), 1)
        );
        checked += 1;
    }
    assert_eq!(checked, 64);
}
