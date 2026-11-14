//! `home_worker_loop.json`: the claim loop, its waits and retries, and a title whose parts
//! are all done already.

use std::fs;
use std::sync::{Arc, Mutex};

use embyx_core::golden;
use embyx_core::pyjson::Value;

use super::{cases, dict, object};
use crate::config::WorkerConfig;
use crate::console::MemoryLog;
use crate::run::Worker;
use crate::server::{CallError, ServerError};
use crate::task::Timing;
use crate::testing::{FakeCluster, ScriptedRunner};

#[test]
fn the_claim_loop_matches_python() {
    let root = tempfile::tempdir().expect("temporary directory");
    let root = root.path();
    let mut checked = 0;
    for (number, case) in cases("loop").into_iter().enumerate() {
        let cache = root.join(format!("loop{number}"));
        fs::create_dir(&cache).expect("cache");
        for name in case.get("stale").map(golden::list).unwrap_or_default() {
            let name = golden::string(name);
            if name.contains('.') {
                fs::write(cache.join(name), "x").expect("file");
            } else {
                fs::create_dir_all(cache.join(name).join("cd01")).expect("folder");
            }
        }
        let cluster = Arc::new(FakeCluster::default());
        for answer in golden::list(golden::get(&case, "answers")) {
            let answer = match answer.get("error").and_then(Value::as_str) {
                Some("transport") => Err(CallError::Transport("connection refused".to_owned())),
                Some("server") => {
                    let code = golden::string(golden::get(answer, "code"));
                    let mut details = embyx_core::pyjson::Dict::new();
                    details.insert("code", Value::Str(code.clone()));
                    Err(CallError::Server(ServerError {
                        status: u16::try_from(golden::int(golden::get(answer, "status"))).expect("status"),
                        code,
                        details,
                    }))
                }
                _ => Ok(dict(golden::get(answer, "value"))),
            };
            cluster.claims.lock().expect("claims").push_back(answer);
        }
        let log = Arc::new(MemoryLog::default());
        let sleeps = Arc::new(Mutex::new(Vec::new()));
        let mut config = WorkerConfig::new("http://cluster");
        config.cache_dir = cache.clone();
        config.upload_mbps = 25.5;
        config.name = "<host>".to_owned();
        let left = Arc::clone(&cluster);
        let recorded = Arc::clone(&sleeps);
        let worker = Worker {
            config,
            cluster: cluster.clone(),
            runner: Arc::new(ScriptedRunner::new(|argv| panic!("no ffprobe expected: {argv:?}"))),
            log: log.clone(),
            timing: Timing::default(),
            stop: Arc::new(move || usize::from(left.claims.lock().expect("claims").is_empty())),
            sleep: Some(Arc::new(move |duration| {
                recorded.lock().expect("sleeps").push(duration);
            })),
        };
        let code = i128::from(worker.serve().is_err());
        assert_eq!(Value::Int(code), *golden::get(&case, "code"), "case {case:?}");
        let root_text = root.to_string_lossy();
        let lines = Value::List(
            log.lines()
                .into_iter()
                .map(|(status, line)| {
                    let line = super::slashes(&line.replace(&*root_text, "<root>"));
                    object([("status", Value::Bool(status)), ("line", Value::Str(line))])
                })
                .collect(),
        );
        assert_eq!(lines, *golden::get(&case, "log"), "case {case:?}");
        let sleeps = Value::List(
            sleeps
                .lock()
                .expect("sleeps")
                .iter()
                .map(|duration| Value::Float(duration.as_secs_f64()))
                .collect(),
        );
        assert_eq!(sleeps, *golden::get(&case, "sleeps"), "case {case:?}");
        let claims = Value::List(cluster.claimed.lock().expect("claimed").clone());
        assert_eq!(claims, *golden::get(&case, "claims"), "case {case:?}");
        let mut left: Vec<Value> = fs::read_dir(&cache)
            .expect("cache")
            .map(|entry| Value::Str(entry.expect("entry").file_name().to_string_lossy().into_owned()))
            .collect();
        left.sort_by_key(|name| name.as_str().unwrap_or_default().to_owned());
        assert_eq!(Value::List(left), *golden::get(&case, "left"), "case {case:?}");
        checked += 1;
    }
    assert_eq!(checked, 3);
}
