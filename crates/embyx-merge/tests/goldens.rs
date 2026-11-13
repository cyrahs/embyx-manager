//! The merge worker against Python's runs: `goldens/merge/` (made by
//! `tools/goldens/areas/merge_worker.py` and `merge_detect.py` from the baseline).
//!
//! Each worker scenario lays the same files out in a temporary directory, answers ffprobe and
//! ffmpeg as Python's fakes did, and must ask for the same argument lists and concat scripts,
//! write the same status texts in the same order, exit the same way, print the same lines and
//! leave the same files behind.

use std::collections::VecDeque;
use std::fs;
use std::io::{self, Cursor};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, FixedOffset};
use embyx_core::config::models::ArchiveConfig;
use embyx_core::golden;
use embyx_core::pyjson::{Dict, Value};
use embyx_merge::detect;
use embyx_merge::transcode;
use embyx_merge::worker::{
    self, Captured, Command, Environment, Exec, Process, Settings, Spawned, StatusFile, Worker, WorkerError,
};

const ROOT: &str = "<root>";
const SPARSE_ABOVE: u64 = 64 * 1024 * 1024;

fn now() -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339("2026-11-13T22:30:26.123456+00:00").expect("fixed time")
}

fn text(value: &Value) -> String {
    golden::string(value)
}

/// What the fake ffmpeg writes: `bytes(range(256))` repeated, or zeros past 64 MiB.
fn write_output(path: &Path, size: u64) {
    if size > SPARSE_ABOVE {
        let file = fs::File::create(path).expect("create");
        file.set_len(size).expect("truncate");
    } else {
        #[allow(clippy::cast_possible_truncation, reason = "the pattern repeats every 256 bytes")]
        let bytes: Vec<u8> = (0..size).map(|index| (index % 256) as u8).collect();
        fs::write(path, bytes).expect("write");
    }
}

struct Answered {
    returncode: i64,
}

impl Process for Answered {
    fn kill(&self) {}

    fn try_wait(&self) -> io::Result<Option<i64>> {
        Ok(Some(self.returncode))
    }
}

/// Python's `_Recorder`: answers from the scenario, keeps every call.
struct Recorder {
    root: String,
    probes: Dict,
    ffmpeg: Mutex<VecDeque<Value>>,
    calls: Mutex<Vec<Value>>,
}

impl Recorder {
    fn mask(&self, value: &str) -> String {
        value.replace(&self.root, ROOT)
    }

    fn argv(&self, argv: &[String]) -> Value {
        Value::List(argv.iter().map(|arg| Value::Str(self.mask(arg))).collect())
    }
}

impl Exec for Recorder {
    fn run(&self, argv: &[String]) -> io::Result<Captured> {
        let mut call = Dict::new();
        call.insert("argv", self.argv(argv));
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Value::Dict(call));
        let key = self.mask(argv.last().expect("a path"));
        let answer = self.probes.get(&key);
        if let Some(code) = answer.and_then(|answer| match answer {
            Value::Dict(fields) => fields.get("oserror"),
            _ => None,
        }) {
            return Err(io::Error::from_raw_os_error(
                i32::try_from(golden::int(code)).expect("errno"),
            ));
        }
        Ok(match answer {
            Some(answer) => Captured {
                returncode: i64::try_from(golden::int(golden::get(answer, "returncode"))).expect("small"),
                stdout: text(golden::get(answer, "stdout")).into_bytes(),
                stderr: text(golden::get(answer, "stderr")).into_bytes(),
            },
            None => Captured {
                returncode: 1,
                stdout: Vec::new(),
                stderr: b"No such file or directory\n".to_vec(),
            },
        })
    }

    fn spawn(&self, argv: &[String]) -> io::Result<Spawned> {
        let mut scripts = Dict::new();
        for pair in argv.windows(2) {
            if pair[0] == "-i" && Path::new(&pair[1]).extension().is_some_and(|suffix| suffix == "txt") {
                let content = fs::read_to_string(&pair[1]).expect("the script was written");
                scripts.insert(self.mask(&pair[1]), Value::Str(self.mask(&content)));
            }
        }
        let mut call = Dict::new();
        call.insert("argv", self.argv(argv));
        call.insert("scripts", Value::Dict(scripts));
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Value::Dict(call));
        let answer = self
            .ffmpeg
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
            .expect("an ffmpeg answer is left");
        if let Some(code) = answer.get("oserror") {
            return Err(io::Error::from_raw_os_error(
                i32::try_from(golden::int(code)).expect("errno"),
            ));
        }
        if let Value::Int(size) = golden::get(&answer, "write") {
            write_output(
                Path::new(argv.last().expect("a target")),
                u64::try_from(*size).expect("size"),
            );
        }
        Ok(Spawned {
            stdout: Box::new(Cursor::new(text(golden::get(&answer, "stdout")).into_bytes())),
            stderr: Box::new(Cursor::new(text(golden::get(&answer, "stderr")).into_bytes())),
            process: Arc::new(Answered {
                returncode: i64::try_from(golden::int(golden::get(&answer, "returncode"))).expect("small"),
            }),
        })
    }
}

fn lay_out(root: &Path, files: &Value) {
    let Value::Dict(files) = files else {
        panic!("files is a dict");
    };
    for (relative, entry) in files.iter() {
        let relative = relative.as_str().expect("a path");
        let path = root.join(relative);
        if relative.ends_with('/') {
            fs::create_dir_all(&path).expect("mkdir");
            continue;
        }
        fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        if let Some(content) = entry.get("text") {
            fs::write(&path, text(content)).expect("write");
        } else {
            let file = fs::File::create(&path).expect("create");
            file.set_len(u64::try_from(golden::int(golden::get(entry, "size"))).expect("size"))
                .expect("truncate");
        }
    }
}

/// Every file with its size and every empty directory, sorted, as Python's `tree`.
/// Size of a file with the temporary directory written as `<root>` (small files only).
fn masked_size(root: &Path, path: &Path) -> u64 {
    let size = fs::metadata(path).expect("stat").len();
    if size > 1 << 20 {
        return size;
    }
    let text = fs::read(path).expect("read");
    let needle = root.to_string_lossy().into_owned().into_bytes();
    let mut count = 0u64;
    let mut index = 0;
    while index + needle.len() <= text.len() {
        if text[index..].starts_with(&needle) {
            count += 1;
            index += needle.len();
        } else {
            index += 1;
        }
    }
    let shrink = needle.len() as u64 - "<root>".len() as u64;
    size - count * shrink
}

fn tree(root: &Path) -> Vec<Value> {
    fn walk(root: &Path, directory: &Path, found: &mut Vec<(String, Option<u64>)>) {
        for entry in fs::read_dir(directory).expect("list") {
            let path = entry.expect("entry").path();
            let relative = path
                .strip_prefix(root)
                .expect("under root")
                .to_string_lossy()
                .into_owned();
            if path.is_dir() {
                if fs::read_dir(&path).expect("list").next().is_none() {
                    found.push((format!("{relative}/"), None));
                }
                walk(root, &path, found);
            } else {
                found.push((relative, Some(masked_size(root, &path))));
            }
        }
    }
    let mut found = Vec::new();
    walk(root, root, &mut found);
    found.sort();
    found
        .into_iter()
        .map(|(path, size)| {
            Value::List(vec![
                Value::Str(path),
                size.map_or(Value::Null, |size| Value::Int(i128::from(size))),
            ])
        })
        .collect()
}

fn optional(value: &Value, root: &str) -> Option<String> {
    match value {
        Value::Null => None,
        other => Some(text(other).replace(ROOT, root)),
    }
}

fn command_of(spec: &Value, root: &str) -> Command {
    Command {
        parts: golden::list(golden::get(spec, "parts"))
            .iter()
            .map(|part| text(part).replace(ROOT, root))
            .collect(),
        output: text(golden::get(spec, "output")).replace(ROOT, root),
        status: text(golden::get(spec, "status")).replace(ROOT, root),
        reserve_gib: match golden::get(spec, "reserve_gib") {
            Value::Float(value) => *value,
            other => panic!("reserve_gib {other:?}"),
        },
        home_prepare: optional(golden::get(spec, "home_prepare"), root),
        home_assemble: optional(golden::get(spec, "home_assemble"), root),
    }
}

/// The exit code and stderr, or for an exception Python let escape, the last traceback line.
fn check_outcome(name: &str, case: &Value, code: i32, stderr: &str) {
    if golden::get(case, "uncaught") == &Value::Null {
        assert_eq!(
            i128::from(code),
            golden::int(golden::get(case, "code")),
            "{name}: exit code"
        );
        assert_eq!(stderr, text(golden::get(case, "stderr")), "{name}: stderr");
    } else {
        assert_eq!(code, 1, "{name}: an uncaught exception exits 1");
        assert_eq!(
            stderr.lines().last().unwrap_or_default(),
            text(golden::get(case, "traceback_tail")),
            "{name}: last traceback line"
        );
    }
}

fn replay(case: &Value) {
    let spec = golden::get(case, "scenario");
    let name = text(golden::get(spec, "name"));
    let tmp = tempfile::tempdir().expect("tempdir");
    let root_path = tmp.path().to_path_buf();
    let root = root_path.to_string_lossy().into_owned();
    lay_out(&root_path, golden::get(spec, "files"));
    fs::create_dir(root_path.join("cgroup")).expect("mkdir");
    fs::write(
        root_path.join("cgroup/memory.max"),
        text(golden::get(spec, "memory_max")),
    )
    .expect("write");
    fs::create_dir(root_path.join("tmp")).expect("mkdir");

    let Value::Dict(probes) = golden::get(spec, "probes").clone() else {
        panic!("probes is a dict");
    };
    let recorder = Arc::new(Recorder {
        root: root.clone(),
        probes,
        ffmpeg: Mutex::new(golden::list(golden::get(spec, "ffmpeg")).iter().cloned().collect()),
        calls: Mutex::new(Vec::new()),
    });
    let free = u64::try_from(golden::int(golden::get(spec, "free"))).expect("free");
    let worker = Worker::with(
        Settings::default(),
        Arc::clone(&recorder) as Arc<dyn Exec>,
        Arc::new(move |path: &Path| fs::metadata(path).map(|_| free)),
    );

    let command = command_of(golden::get(spec, "command"), &root);
    let writes = Arc::new(Mutex::new(Vec::new()));
    let mut tick = 0.0;
    let mut status = StatusFile::with_clocks(
        worker::py_path(&command.status),
        Box::new(move || {
            tick += 1.0;
            tick
        }),
        Box::new(now),
    )
    .observed({
        let writes = Arc::clone(&writes);
        let root = root.clone();
        Box::new(move |written: &str| {
            writes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(Value::Str(written.replace(&root, ROOT)));
        })
    });
    let environment = Environment {
        tmpdir: root_path.join("tmp").to_string_lossy().into_owned(),
        cgroup_memory_max: root_path.join("cgroup/memory.max"),
    };
    let mut stderr = Vec::new();

    let code = worker::run(&worker, &command, &mut status, &environment, &mut stderr);

    let stderr = String::from_utf8(stderr).expect("utf-8").replace(&root, ROOT);
    let calls = recorder.calls.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(Value::List(calls), golden::get(case, "calls").clone(), "{name}: calls");
    let writes = writes.lock().unwrap_or_else(PoisonError::into_inner).clone();
    let expected_writes = golden::list(golden::get(case, "writes"));
    assert_eq!(
        writes.len(),
        expected_writes.len(),
        "{name}: number of status writes: {writes:#?}"
    );
    for (index, (found, wanted)) in writes.iter().zip(expected_writes).enumerate() {
        assert_eq!(found, wanted, "{name}: status write {index}");
    }
    check_outcome(&name, case, code, &stderr);
    assert!(
        recorder
            .ffmpeg
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty(),
        "{name}: ffmpeg answers left over"
    );
    assert_eq!(
        Value::List(tree(&root_path)),
        golden::get(case, "tree").clone(),
        "{name}: files left"
    );
}

#[test]
fn merges_run_as_python_ran_them() {
    let document = golden::load("merge/worker_merge.json");
    let cases = golden::cases(&document);
    assert!(cases.len() >= 40, "{}", cases.len());
    for case in cases {
        replay(case);
    }
}

#[test]
fn home_prepare_and_assemble_run_as_python_ran_them() {
    let document = golden::load("merge/worker_home.json");
    for case in golden::cases(&document) {
        replay(case);
    }
}

fn outcome(result: Result<Value, WorkerError>) -> Value {
    let mut out = Dict::new();
    match result {
        Ok(value) => out.insert("value", value),
        Err(error) => {
            let (class, message) = match error {
                WorkerError::Refused(message) => ("MergeRefusedError".to_owned(), message),
                WorkerError::Failed(message) => ("MergeWorkerError".to_owned(), message),
                WorkerError::Uncaught(exception) => (exception.class.to_owned(), exception.message),
            };
            let mut error = Dict::new();
            error.insert("type", Value::Str(class));
            error.insert("message", Value::Str(message));
            out.insert("error", Value::Dict(error));
        }
    }
    Value::Dict(out)
}

fn float(value: &Value) -> f64 {
    match value {
        Value::Float(number) => *number,
        Value::Int(number) => {
            #[allow(clippy::cast_precision_loss, reason = "golden inputs are small")]
            let number = *number as f64;
            number
        }
        other => panic!("not a number: {other:?}"),
    }
}

#[test]
fn worker_helpers_give_pythons_values() {
    let document = golden::load("merge/worker_functions.json");
    let case = &golden::cases(&document)[0];
    for entry in golden::list(golden::get(case, "transcode_bitrate")) {
        let args = golden::list(golden::get(entry, "args"));
        let found = outcome(
            worker::transcode_bitrate(
                golden::int(&args[0]),
                float(&args[1]),
                golden::int(&args[2]),
                float(&args[3]),
                float(&args[4]),
                worker::MAX_UPLOAD_BYTES,
            )
            .map(Value::Int),
        );
        let mut wanted = Dict::new();
        for key in ["value", "error"] {
            if let Some(value) = entry.get(key) {
                wanted.insert(key, value.clone());
            }
        }
        assert_eq!(found, Value::Dict(wanted), "transcode_bitrate{args:?}");
    }
    for entry in golden::list(golden::get(case, "transcode_memory")) {
        let probe = worker::Probe {
            duration: 1.0,
            signature: vec![vec![
                Value::Str("video".to_owned()),
                Value::Str("h264".to_owned()),
                golden::get(entry, "width").clone(),
                golden::get(entry, "height").clone(),
                Value::Null,
                Value::Null,
            ]],
            layout: Vec::new(),
            packets: None,
            audio_bits: 0,
        };
        assert_eq!(
            worker::transcode_memory(&probe),
            golden::int(golden::get(entry, "value")),
            "{entry:?}"
        );
    }
    for entry in golden::list(golden::get(case, "paths")) {
        let part = golden::int(golden::get(entry, "part"));
        let chunk = golden::int(golden::get(entry, "chunk"));
        assert_eq!(
            transcode::chunk_path(Path::new("/w"), part, chunk).to_string_lossy(),
            text(golden::get(entry, "chunk_path"))
        );
        assert_eq!(
            transcode::audio_path(Path::new("/w"), part).to_string_lossy(),
            text(golden::get(entry, "audio_path"))
        );
    }
    for entry in golden::list(golden::get(case, "scripts")) {
        let entries: Vec<(PathBuf, f64)> = golden::list(golden::get(entry, "entries"))
            .iter()
            .map(|pair| {
                let pair = golden::list(pair);
                (PathBuf::from(text(&pair[0])), float(&pair[1]))
            })
            .collect();
        assert_eq!(
            transcode::concat_script(&entries),
            text(golden::get(entry, "concat_script"))
        );
        let paths: Vec<PathBuf> = entries.into_iter().map(|(path, _)| path).collect();
        assert_eq!(worker::concat_list(&paths), text(golden::get(entry, "concat_list")));
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    for entry in golden::list(golden::get(case, "memory_limit")) {
        let path = match golden::get(entry, "content") {
            Value::Null => tmp.path().join("missing"),
            content => {
                let path = tmp.path().join("memory.max");
                fs::write(&path, text(content)).expect("write");
                path
            }
        };
        let found = outcome(worker::memory_limit(&path).map(|limit| limit.map_or(Value::Null, Value::Int)));
        let mut wanted = Dict::new();
        for key in ["value", "error"] {
            if let Some(value) = entry.get(key) {
                wanted.insert(key, value.clone());
            }
        }
        assert_eq!(found, Value::Dict(wanted), "{entry:?}");
    }
}

fn archive(spec: &Value) -> ArchiveConfig {
    let routes = |key: &str| -> Vec<(String, String)> {
        let Value::Dict(routes) = golden::get(spec, key) else {
            panic!("{key} is a dict");
        };
        routes
            .iter()
            .map(|(source, destination)| (source.as_str().expect("str").to_owned(), text(destination)))
            .collect()
    };
    ArchiveConfig {
        src_dir: text(golden::get(spec, "src_dir")),
        dst_dir: text(golden::get(spec, "dst_dir")),
        mapping: routes("mapping"),
        priority_mapping: routes("priority_mapping"),
        ..ArchiveConfig::default()
    }
}

fn optional_text(value: Option<String>) -> Value {
    value.map_or(Value::Null, Value::Str)
}

#[test]
fn routes_resolve_as_python_resolves_them() {
    let document = golden::load("merge/detect_routes.json");
    let case = &golden::cases(&document)[0];
    let spec = golden::get(case, "archive");
    let archive = archive(spec);
    let bare = ArchiveConfig {
        src_dir: String::new(),
        ..archive.clone()
    };
    let sources: Vec<Value> = detect::route_sources(&archive).into_iter().map(Value::Str).collect();
    assert_eq!(Value::List(sources), golden::get(case, "route_sources").clone());
    for entry in golden::list(golden::get(case, "library_dirs")) {
        let entry = golden::list(entry);
        let value = entry[0].as_str();
        assert_eq!(
            optional_text(detect::source_for_library_dir(&archive, value)),
            entry[1],
            "{value:?}"
        );
    }
    for entry in golden::list(golden::get(case, "task_dirs")) {
        let entry = golden::list(entry);
        let value = entry[0].as_str();
        assert_eq!(
            optional_text(detect::source_for_task_dir(&archive, value)),
            entry[1],
            "{value:?}"
        );
        assert_eq!(
            optional_text(detect::source_for_task_dir(&bare, value)),
            entry[2],
            "{value:?}"
        );
    }
    for entry in golden::list(golden::get(case, "is_whole_copy")) {
        let entry = golden::list(entry);
        let (stem, avid) = (text(&entry[0]), text(&entry[1]));
        assert_eq!(
            Value::Bool(detect::is_whole_copy(&stem, &avid)),
            entry[2],
            "{stem:?} {avid:?}"
        );
    }
    for entry in golden::list(golden::get(case, "whole_copies")) {
        let entry = golden::list(entry);
        let names: Vec<String> = golden::list(&entry[1]).iter().map(text).collect();
        let found = detect::whole_copies(&text(&entry[0]), names.iter().map(String::as_str));
        assert_eq!(Value::List(found.into_iter().map(Value::Str).collect()), entry[2]);
    }
}

fn title(title: &detect::MultipartTitle) -> Value {
    let mut out = Dict::new();
    out.insert("avid", Value::Str(title.avid.clone()));
    out.insert("directory", Value::Str(title.directory.clone()));
    out.insert(
        "parts",
        Value::List(
            title
                .parts
                .iter()
                .map(|part| {
                    Value::List(vec![
                        Value::Int(part.index),
                        Value::Str(part.strm.clone()),
                        Value::Str(part.target.clone()),
                    ])
                })
                .collect(),
        ),
    );
    out.insert(
        "missing",
        Value::List(title.missing.iter().copied().map(Value::Int).collect()),
    );
    out.insert("library_dir", optional_text(title.library_dir.clone()));
    out.insert("brand", optional_text(title.brand.clone()));
    out.insert(
        "problem",
        title
            .problem
            .map_or(Value::Null, |problem| Value::Str(problem.as_str().to_owned())),
    );
    #[allow(clippy::cast_possible_wrap, reason = "a handful of parts")]
    out.insert("part_count", Value::Int(title.part_count() as i128));
    out.insert("stackable", Value::Bool(title.stackable()));
    out.insert("complete", Value::Bool(title.complete()));
    Value::Dict(out)
}

#[test]
fn scans_find_what_python_finds() {
    let document = golden::load("merge/detect_scan.json");
    for case in golden::cases(&document) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let Value::Dict(files) = golden::get(case, "files") else {
            panic!("files is a dict");
        };
        for (relative, content) in files.iter() {
            let path = tmp.path().join(relative.as_str().expect("a path"));
            fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            let bytes = match content {
                Value::Dict(link) => {
                    let target = text(link.get("symlink").expect("a symlink target"));
                    std::os::unix::fs::symlink(target, &path).expect("symlink");
                    continue;
                }
                Value::Str(content) => content.clone().into_bytes(),
                Value::List(values) => values
                    .iter()
                    .map(|value| u8::try_from(golden::int(value)).expect("a byte"))
                    .collect(),
                other => panic!("content {other:?}"),
            };
            fs::write(path, bytes).expect("write");
        }
        let library_root = text(golden::get(case, "library_root"));
        let titles = detect::scan_multipart(tmp.path(), &library_root);
        let found = Value::List(titles.iter().map(title).collect());
        assert_eq!(
            found,
            golden::get(case, "titles").clone(),
            "{} under {library_root}",
            text(golden::get(case, "tree"))
        );
    }
}
