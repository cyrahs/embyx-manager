//! The Python and the Rust merge worker against real ffmpeg on the same tiny parts.
//!
//! Each scenario lays the same files out under two temporary roots, runs the baseline's
//! `worker.run` on one (`tools/merge_ffmpeg/python_worker.py`, through `uv`) and the Rust
//! `worker::run` on the other, and compares the exit code, stderr and every status write
//! with the root written as `<root>` and without `updated_at`. Both status clocks stand
//! still, so only the forced writes happen: the phase sequence, the final status with its
//! size, sha1 and duration, and the refusal texts must all agree.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as Process;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use chrono::Utc;
use embyx_core::pyjson::{self, Value};
use embyx_merge::worker::{self, Command, Environment, Settings, StatusFile, Worker};

const ROOT: &str = "<root>";

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn ffmpeg(arguments: &[&str]) {
    let status = Process::new("ffmpeg")
        .args(["-hide_banner", "-loglevel", "error", "-y"])
        .args(arguments)
        .status()
        .expect("ffmpeg runs (it must be installed)");
    assert!(status.success(), "ffmpeg {arguments:?}");
}

/// A two-second part, as `tests/test_merge_worker.py::make_part` makes it.
fn make_part(path: &Path, size: &str, audio_first: bool) {
    let video = format!("testsrc=size={size}:rate=10:duration=2");
    let mut arguments = vec![
        "-f",
        "lavfi",
        "-i",
        video.as_str(),
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:duration=2",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-shortest",
    ];
    if audio_first {
        arguments.extend(["-map", "1:a", "-map", "0:v"]);
    }
    let path = path.to_string_lossy();
    arguments.push(&path);
    ffmpeg(&arguments);
}

/// The source files every scenario copies from, made once: three plain parts, one with the
/// audio first, one of another frame size, and an AV1 chunk of each plain part's video as
/// the home worker would upload it.
fn sources() -> &'static Path {
    static SOURCES: OnceLock<tempfile::TempDir> = OnceLock::new();
    SOURCES
        .get_or_init(|| {
            let directory = tempfile::tempdir().expect("tempdir");
            let root = directory.path();
            for index in 1..=3 {
                make_part(&root.join(format!("plain-{index}.mp4")), "160x120", false);
                let chunk = root.join(format!("chunk-{index}.mkv"));
                let part = root.join(format!("plain-{index}.mp4"));
                ffmpeg(&[
                    "-i",
                    &part.to_string_lossy(),
                    "-map",
                    "0:v",
                    "-c:v",
                    "libsvtav1",
                    "-preset",
                    "12",
                    "-f",
                    "matroska",
                    &chunk.to_string_lossy(),
                ]);
            }
            make_part(&root.join("audio-first.mp4"), "160x120", true);
            make_part(&root.join("larger.mp4"), "320x240", false);
            directory
        })
        .path()
}

/// One run's outcome: exit code, stderr, and the status writes as JSON without `updated_at`.
#[derive(Debug, PartialEq)]
struct Outcome {
    code: i64,
    stderr: String,
    writes: Vec<Value>,
}

fn written(text: &str, root: &str) -> Value {
    let Value::Dict(mut status) = pyjson::loads(&text.replace(root, ROOT)).expect("a status is JSON") else {
        panic!("a status is an object: {text}");
    };
    assert!(status.remove("updated_at").is_some(), "no updated_at in {text}");
    Value::Dict(status)
}

struct Run {
    parts: Vec<String>,
    output: String,
    status: String,
    home_prepare: Option<String>,
    home_assemble: Option<String>,
    max_upload_bytes: Option<i128>,
    transcode_preset: Option<&'static str>,
}

impl Run {
    fn merge(root: &Path, parts: &[&str]) -> Self {
        Self {
            parts: parts
                .iter()
                .map(|name| root.join("library").join(name).to_string_lossy().into_owned())
                .collect(),
            output: root.join("upload/ABC-123.mp4").to_string_lossy().into_owned(),
            status: root.join("upload/.merge/1.json").to_string_lossy().into_owned(),
            home_prepare: None,
            home_assemble: None,
            max_upload_bytes: None,
            transcode_preset: None,
        }
    }

    fn python(&self, root: &Path) -> Outcome {
        let mut command = pyjson::Dict::new();
        command.insert(
            "parts",
            Value::List(self.parts.iter().cloned().map(Value::Str).collect()),
        );
        command.insert("output", Value::Str(self.output.clone()));
        command.insert("status", Value::Str(self.status.clone()));
        command.insert("reserve_gib", Value::Float(0.0));
        command.insert(
            "home_prepare",
            self.home_prepare.clone().map_or(Value::Null, Value::Str),
        );
        command.insert(
            "home_assemble",
            self.home_assemble.clone().map_or(Value::Null, Value::Str),
        );
        if let Some(bytes) = self.max_upload_bytes {
            command.insert("max_upload_bytes", Value::Int(bytes));
        }
        if let Some(preset) = self.transcode_preset {
            command.insert("transcode_preset", Value::Str(preset.to_owned()));
        }
        let repository = repository();
        let output = Process::new("uv")
            .args(["run", "--locked", "python"])
            .arg(repository.join("tools/merge_ffmpeg/python_worker.py"))
            .arg(pyjson::dumps(&Value::Dict(command), &pyjson::DumpOptions::plain()).expect("JSON"))
            .current_dir(&repository)
            .env("TMPDIR", root.join("tmp"))
            .output()
            .expect("uv runs (it must be installed)");
        let stdout = String::from_utf8(output.stdout).expect("utf-8");
        assert!(
            output.status.success(),
            "the Python worker failed: {}{stdout}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result = pyjson::loads(stdout.trim_end()).expect("the helper prints JSON");
        let root = root.to_string_lossy();
        let Value::Int(code) = result_field(&result, "code") else {
            panic!("no exit code in {stdout}");
        };
        let Value::Str(stderr) = result_field(&result, "stderr") else {
            panic!("no stderr in {stdout}");
        };
        let Value::List(writes) = result_field(&result, "writes") else {
            panic!("no writes in {stdout}");
        };
        Outcome {
            code: i64::try_from(*code).expect("exit code"),
            stderr: stderr.replace(root.as_ref(), ROOT),
            writes: writes
                .iter()
                .map(|text| match text {
                    Value::Str(text) => written(text, &root),
                    other => panic!("a write is text: {other:?}"),
                })
                .collect(),
        }
    }

    fn rust(&self, root: &Path) -> Outcome {
        let mut settings = Settings::default();
        if let Some(bytes) = self.max_upload_bytes {
            settings.max_upload_bytes = bytes;
        }
        if let Some(preset) = self.transcode_preset {
            preset.clone_into(&mut settings.transcode_preset);
        }
        let worker = Worker::new(settings);
        let command = Command {
            parts: self.parts.clone(),
            output: self.output.clone(),
            status: self.status.clone(),
            reserve_gib: 0.0,
            home_prepare: self.home_prepare.clone(),
            home_assemble: self.home_assemble.clone(),
        };
        let writes = Arc::new(Mutex::new(Vec::new()));
        let mut status = StatusFile::with_clocks(
            worker::py_path(&command.status),
            Box::new(|| 0.0),
            Box::new(|| Utc::now().fixed_offset()),
        )
        .observed({
            let writes = Arc::clone(&writes);
            Box::new(move |text: &str| {
                writes
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(text.to_owned());
            })
        });
        let environment = Environment {
            tmpdir: root.join("tmp").to_string_lossy().into_owned(),
            ..Environment::from_process()
        };
        let mut stderr = Vec::new();
        let code = worker::run(&worker, &command, &mut status, &environment, &mut stderr);
        let root = root.to_string_lossy();
        let writes = writes.lock().unwrap_or_else(PoisonError::into_inner).clone();
        Outcome {
            code: i64::from(code),
            stderr: String::from_utf8(stderr).expect("utf-8").replace(root.as_ref(), ROOT),
            writes: writes.iter().map(|text| written(text, &root)).collect(),
        }
    }
}

fn result_field<'a>(result: &'a Value, key: &str) -> &'a Value {
    match result {
        Value::Dict(fields) => fields.get(key).unwrap_or_else(|| panic!("no {key}")),
        other => panic!("not an object: {other:?}"),
    }
}

/// A root with `library/` holding the named sources (as `ABC-123-cdN.mp4`), `upload/` and `tmp/`.
fn lay_out(root: &Path, sources_by_part: &[&str]) -> Vec<String> {
    for directory in ["library", "upload", "tmp"] {
        fs::create_dir_all(root.join(directory)).expect("mkdir");
    }
    sources_by_part
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let name = format!("ABC-123-cd{}.mp4", index + 1);
            fs::copy(sources().join(source), root.join("library").join(&name)).expect("copy");
            name
        })
        .collect()
}

fn phases(outcome: &Outcome) -> Vec<String> {
    outcome
        .writes
        .iter()
        .map(|status| match status {
            Value::Dict(fields) => match (fields.get("state"), fields.get("phase")) {
                (Some(Value::Str(state)), Some(Value::Str(phase))) => format!("{state}/{phase}"),
                (Some(Value::Str(state)), _) => state.clone(),
                _ => String::new(),
            },
            _ => String::new(),
        })
        .collect()
}

/// Runs `run` (built for a root) under a Python and a Rust root and compares them.
fn compare(name: &str, prepare: impl Fn(&Path) -> Run) -> Outcome {
    compare_except(name, &[], prepare)
}

/// As `compare`, with the `volatile` status fields only checked to be in the same writes:
/// SVT-AV1 does not give the same bytes twice, so an encode's size and sha1 vary.
fn compare_except(name: &str, volatile: &[&str], prepare: impl Fn(&Path) -> Run) -> Outcome {
    let python_root = tempfile::tempdir().expect("tempdir");
    let rust_root = tempfile::tempdir().expect("tempdir");
    let python = prepare(python_root.path()).python(python_root.path());
    let rust = prepare(rust_root.path()).rust(rust_root.path());
    assert_eq!(phases(&rust), phases(&python), "{name}: phase sequence");
    let blank = |outcome: &Outcome| Outcome {
        code: outcome.code,
        stderr: outcome.stderr.clone(),
        writes: outcome
            .writes
            .iter()
            .map(|status| match status {
                Value::Dict(fields) => {
                    let mut fields = fields.clone();
                    for key in volatile {
                        if fields.get(key).is_some() {
                            fields.insert(*key, Value::Str("<varies>".to_owned()));
                        }
                    }
                    Value::Dict(fields)
                }
                other => other.clone(),
            })
            .collect(),
    };
    assert_eq!(blank(&rust), blank(&python), "{name}: outcome");
    rust
}

fn last(outcome: &Outcome) -> &pyjson::Dict {
    match outcome.writes.last() {
        Some(Value::Dict(fields)) => fields,
        other => panic!("no last status: {other:?}"),
    }
}

#[test]
fn a_plain_merge_writes_what_python_writes() {
    let outcome = compare("concat", |root| {
        let names = lay_out(root, &["plain-1.mp4", "plain-2.mp4", "plain-3.mp4"]);
        Run::merge(root, &names.iter().map(String::as_str).collect::<Vec<_>>())
    });
    assert_eq!(outcome.code, 0);
    assert_eq!(last(&outcome).get("state"), Some(&Value::Str("done".to_owned())));
    assert!(matches!(last(&outcome).get("sha1"), Some(Value::Str(sha1)) if sha1.len() == 40));
}

#[test]
fn a_merge_that_puts_streams_in_order_writes_what_python_writes() {
    let outcome = compare("reorder", |root| {
        let names = lay_out(root, &["plain-1.mp4", "audio-first.mp4"]);
        Run::merge(root, &names.iter().map(String::as_str).collect::<Vec<_>>())
    });
    assert_eq!(outcome.code, 0);
    assert!(
        phases(&outcome).contains(&"running/reordering".to_owned()),
        "{outcome:?}"
    );
}

#[test]
fn a_refusal_writes_what_python_writes() {
    let outcome = compare("refused", |root| {
        let names = lay_out(root, &["plain-1.mp4", "larger.mp4"]);
        Run::merge(root, &names.iter().map(String::as_str).collect::<Vec<_>>())
    });
    assert_eq!(outcome.code, 0);
    assert_eq!(last(&outcome).get("refused"), Some(&Value::Bool(true)));
}

#[test]
fn an_av1_re_encode_writes_what_python_writes() {
    let outcome = compare_except("transcode", &["written_bytes", "size", "sha1"], |root| {
        let names = lay_out(root, &["plain-1.mp4", "plain-2.mp4", "plain-3.mp4"]);
        let total: u64 = names
            .iter()
            .map(|name| fs::metadata(root.join("library").join(name)).expect("stat").len())
            .sum();
        let mut run = Run::merge(root, &names.iter().map(String::as_str).collect::<Vec<_>>());
        run.max_upload_bytes = Some(i128::from(total) - 1);
        run.transcode_preset = Some("12");
        run
    });
    assert_eq!(outcome.code, 0);
    assert!(
        phases(&outcome).contains(&"running/transcoding".to_owned()),
        "{outcome:?}"
    );
    assert_eq!(last(&outcome).get("state"), Some(&Value::Str("done".to_owned())));
}

#[test]
fn home_prepare_and_assemble_write_what_python_writes() {
    let transcode = |root: &Path| root.join("upload/.transcode/7");
    let prepared = compare("home prepare", |root| {
        let names = lay_out(root, &["plain-1.mp4", "plain-2.mp4", "plain-3.mp4"]);
        let mut run = Run::merge(root, &names.iter().map(String::as_str).collect::<Vec<_>>());
        run.status = transcode(root).join("audio.json").to_string_lossy().into_owned();
        run.home_prepare = Some(transcode(root).to_string_lossy().into_owned());
        run
    });
    assert_eq!(prepared.code, 0);
    assert_eq!(last(&prepared).get("audio_done"), Some(&Value::Int(3)));

    let assembled = compare("home assemble", |root| {
        // The prepare step's own output, and the chunks and manifest the home worker and
        // the app leave beside it.
        let names = lay_out(root, &["plain-1.mp4", "plain-2.mp4", "plain-3.mp4"]);
        let mut prepare = Run::merge(root, &names.iter().map(String::as_str).collect::<Vec<_>>());
        prepare.status = transcode(root).join("audio.json").to_string_lossy().into_owned();
        prepare.home_prepare = Some(transcode(root).to_string_lossy().into_owned());
        assert_eq!(prepare.rust(root).code, 0);
        let mut parts = Vec::new();
        for index in 0..3 {
            let chunk = embyx_merge::transcode::chunk_path(&transcode(root), index, 0);
            fs::create_dir_all(chunk.parent().expect("a chunk has a directory")).expect("mkdir");
            fs::copy(sources().join(format!("chunk-{}.mkv", index + 1)), &chunk).expect("copy");
            parts.push(format!(
                r#"{{"index": {index}, "chunks": [{{"index": 0, "duration": 2.0}}]}}"#
            ));
        }
        fs::write(
            transcode(root).join("manifest.json"),
            format!(r#"{{"parts": [{}]}}"#, parts.join(", ")),
        )
        .expect("write");
        let mut run = Run::merge(root, &[]);
        run.parts = Vec::new();
        run.home_assemble = Some(transcode(root).to_string_lossy().into_owned());
        run
    });
    assert_eq!(assembled.code, 0);
    assert_eq!(last(&assembled).get("state"), Some(&Value::Str("done".to_owned())));
}
