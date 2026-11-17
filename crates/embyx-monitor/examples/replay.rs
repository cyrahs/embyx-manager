//! Level 2 decision replay, the Rust side (`tools/level2/replay.py` holds the Python side and
//! the comparison). Reads a request file `{"avid": <section JSON text>, "archive": <section JSON
//! text>, "inputs": [name, ...]}` and prints one JSON line per name: the AVID the stored rules
//! read from it, its brand, the folder `find_dst_dir` files it under for every normal route's
//! destination (in the order `dict.fromkeys(mapping.values())` gives) and the run lines that
//! logged. Pure decisions only: nothing is read from or written to a file system.
//!
//!     cargo build -q -p embyx-monitor --example replay
//!     target/debug/examples/replay request.json

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use embyx_core::avid::{AvidConfig, AvidParser, get_brand};
use embyx_core::config::{ArchiveConfig, AvidRulesConfig, Section};
use embyx_core::pypath;
use embyx_monitor::archive::ArchivePipeline;
use embyx_monitor::reports::RunContext;
use serde_json::{Value, json};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            #[allow(clippy::print_stderr, reason = "a command-line tool reports its failure")]
            {
                eprintln!("replay: {error}");
            }
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let path = std::env::args().nth(1).ok_or("usage: replay REQUEST.json")?;
    let request: Value = serde_json::from_slice(&std::fs::read(&path).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())?;
    let text = |name: &str| {
        request[name]
            .as_str()
            .map(str::to_owned)
            .ok_or(format!("no {name} section text"))
    };
    let rules = AvidRulesConfig::validate_json(&text("avid")?).map_err(|error| format!("avid: {error:?}"))?;
    let archive = ArchiveConfig::validate_json(&text("archive")?).map_err(|error| format!("archive: {error:?}"))?;
    let parser = AvidParser::new(AvidConfig {
        id_exceptions: rules.id_exceptions,
        ignored_id_patterns: rules.ignored_id_patterns,
    })
    .map_err(|error| format!("avid rules: {error}"))?;
    let mut destinations: Vec<String> = Vec::new();
    for (_, destination) in &archive.mapping {
        if !destinations.contains(destination) {
            destinations.push(destination.clone());
        }
    }
    let pipeline = ArchivePipeline::new(archive, parser);
    let dst_dir = pipeline.dst_dir.to_string_lossy().into_owned();
    let mut out = std::io::stdout().lock();
    for input in request["inputs"].as_array().ok_or("no inputs")? {
        let input = input.as_str().ok_or("an input that is not text")?;
        let avid = pipeline.avid_of(input);
        let ctx = RunContext::new("archive");
        let routes: Vec<Value> = if avid.is_empty() {
            Vec::new()
        } else {
            destinations
                .iter()
                .map(|destination| {
                    let route = PathBuf::from(pypath::join(&dst_dir, destination));
                    pipeline
                        .find_dst_dir(&avid, &route, &ctx)
                        .map_or(Value::Null, |found| Value::from(found.to_string_lossy().into_owned()))
                })
                .collect()
        };
        let brand = if avid.is_empty() { None } else { get_brand(&avid) };
        let line = json!({
            "input": input,
            "avid": avid,
            "brand": brand,
            "routes": routes,
            "log": ctx.log_tail(),
        });
        writeln!(out, "{line}").map_err(|error| error.to_string())?;
    }
    Ok(())
}
