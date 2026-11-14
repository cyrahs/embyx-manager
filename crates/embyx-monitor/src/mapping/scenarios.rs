//! The mapping scenarios (`tools/scenarios/specs/mapping/`): the Rust port of
//! `tools/scenarios/operations/mapping.py` runs each spec's steps through [`MappingPipeline`]
//! over the seeded tree, through [`RecordingFs`] with every directory listed in name order
//! (the Python side patches `os.scandir` the same way), and must render the bytes the Python
//! baseline committed under `goldens/scenarios/mapping/`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::DateTime;
use embyx_core::avid::AvidParser;
use embyx_core::config::models::MappingConfig;
use embyx_core::pyjson::{Dict, Value};
use embyx_core::scenario::{Recorder, Seeded, Spec};

use super::MappingPipeline;
use crate::archive::scenarios::{entry, error_value, finish, flag, int, only_op, recording_fs, steps_of, strings, text};
use crate::fs::{Fs, path_text};
use crate::reports::RunContext;

/// `os.utime(path, (seconds, seconds))`.
fn set_times(path: &Path, seconds: i64) {
    let stamp = rustix::fs::Timespec {
        tv_sec: seconds,
        tv_nsec: 0,
    };
    let times = rustix::fs::Timestamps {
        last_access: stamp,
        last_modification: stamp,
    };
    rustix::fs::utimensat(rustix::fs::CWD, path, &times, rustix::fs::AtFlags::empty()).expect("utime");
}

/// `_pin_times`: every seeded file (not a symbolic link) gets the scenario clock as its
/// access and modification time.
fn pin_times(spec: &Spec, seeded: &Seeded) {
    fn walk(directory: &Path, stamp: i64) {
        for item in std::fs::read_dir(directory).expect("a seeded directory") {
            let item = item.expect("an entry");
            let kind = item.file_type().expect("a file type");
            if kind.is_dir() {
                walk(&item.path(), stamp);
            } else if !kind.is_symlink() {
                set_times(&item.path(), stamp);
            }
        }
    }
    let stamp = DateTime::parse_from_rfc3339(spec.clock())
        .expect("an RFC 3339 clock")
        .timestamp();
    let names = spec
        .section("roots")
        .get("names")
        .and_then(Value::as_list)
        .map(|names| names.iter().map(|name| name.as_str().expect("a root name").to_owned()).collect())
        .unwrap_or_else(Vec::new);
    for name in names {
        walk(seeded.root(&name), stamp);
    }
}

/// `_shape_tree`: the unrecorded `write`, `remove` and `mkdir` steps; false for any other.
fn shape_tree(seeded: &Seeded, op: &str, args: &Value) -> bool {
    match op {
        "write" => {
            let path = seeded.resolve(text(args, "path"));
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("parents");
            std::fs::write(&path, text(args, "text")).expect("write");
            set_times(&path, int(args.get("mtime").expect("an mtime")));
        }
        "remove" => std::fs::remove_file(seeded.resolve(text(args, "path"))).expect("remove"),
        "mkdir" => std::fs::create_dir_all(seeded.resolve(text(args, "path"))).expect("mkdir"),
        _ => return false,
    }
    true
}

/// `_pipeline(ctx, config)`.
fn pipeline(seeded: &Seeded, config: &Value, fs: &Arc<dyn Fs>) -> MappingPipeline {
    let root = |key: &str| path_text(&seeded.resolve(text(config, key)));
    MappingPipeline::with_fs(
        MappingConfig {
            enabled: true,
            src_dir: root("src_dir"),
            dst_dir: root("dst_dir"),
            ..MappingConfig::default()
        },
        AvidParser::default(),
        Arc::clone(fs),
    )
}

fn paths(seeded: &Seeded, args: &Value, key: &str) -> Vec<PathBuf> {
    args.get(key)
        .and_then(Value::as_list)
        .unwrap_or_default()
        .iter()
        .map(|path| seeded.resolve(path.as_str().expect("a spec path")))
        .collect()
}

/// `sorted(str(path) for path in failed)`.
fn sorted_texts(failed: &[PathBuf]) -> Value {
    let mut texts: Vec<String> = failed.iter().map(|path| path_text(path)).collect();
    texts.sort();
    strings(texts)
}

/// `mapping.steps`.
fn mapping_steps(name: &str) {
    let spec = Spec::load(name);
    assert!(spec.get("database").is_none(), "mapping.steps runs without a database");
    let seeded = Seeded::new(&spec);
    let recorder = Recorder::new();
    let args = spec.section("args");
    let fs = recording_fs(&seeded, &args, &recorder);
    fs.sort_listings();
    let dyn_fs: Arc<dyn Fs> = Arc::clone(&fs) as Arc<dyn Fs>;
    let mut mapping = pipeline(&seeded, args.get("config").expect("a config"), &dyn_fs);
    pin_times(&spec, &seeded);
    let mut reported = Vec::new();
    for (op, step) in steps_of(&spec) {
        if shape_tree(&seeded, &op, &step) {
            reported.push(only_op(&op));
            continue;
        }
        let run = Arc::new(RunContext::new("scenario-mapping"));
        if flag(&step, "stop_before") {
            run.request_stop();
        }
        fs.set_run(Some(Arc::clone(&run)));
        let outcome: Result<Value, Value> = match op.as_str() {
            "configure" => {
                mapping = pipeline(&seeded, step.get("config").expect("a config"), &dyn_fs);
                Ok(Value::Null)
            }
            "full" => mapping
                .run_full(&run)
                .map(|()| Value::Null)
                .map_err(|error| error_value(&error)),
            "incremental" => mapping
                .run_incremental(&run, &paths(&seeded, &step, "changed"), &paths(&seeded, &step, "deleted"))
                .map(|(failed_changed, failed_deleted)| {
                    let mut result = Dict::new();
                    result.insert("failed_changed", sorted_texts(&failed_changed));
                    result.insert("failed_deleted", sorted_texts(&failed_deleted));
                    Value::Dict(result)
                })
                .map_err(|error| error_value(&error)),
            other => panic!("unknown step {other:?}"),
        };
        reported.push(entry(&op, outcome, &run));
    }
    finish(&spec, &seeded, reported, &recorder, Value::Dict(Dict::new()));
}

#[test]
fn mapping_full_matches_the_python_transcript() {
    mapping_steps("mapping/full");
}

#[test]
fn mapping_incremental_matches_the_python_transcript() {
    mapping_steps("mapping/incremental");
}

#[test]
fn mapping_failures_match_the_python_transcript() {
    mapping_steps("mapping/failures");
}
