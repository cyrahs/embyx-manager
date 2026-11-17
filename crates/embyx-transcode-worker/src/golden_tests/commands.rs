//! `home_worker_commands.json` and `home_worker_probes.json`: every ffmpeg and ffprobe
//! argument list, NVENC ones included, and what the worker reads from ffprobe's answers.

use std::path::Path;

use embyx_core::golden;
use embyx_core::pyjson::Value;

use super::{cases, dict, expected, float, object, py_error, raised, strings, value};
use crate::config::WorkerConfig;
use crate::ffmpeg::{self, EncodeRun, ProbeError};
use crate::process::{Captured, RunError};
use crate::testing::ScriptedRunner;

#[test]
fn ffmpeg_argument_lists_match_python() {
    let mut checked = 0;
    for case in cases("commands") {
        let encoder = dict(golden::get(&case, "encoder"));
        let got = match golden::string(golden::get(&case, "kind")).as_str() {
            "decoder" => value(strings(&ffmpeg::decoder_args(&encoder))),
            "encoder" => {
                let bits = golden::int(golden::get(&case, "video_bits"));
                let fps = float(golden::get(&case, "fps"));
                match ffmpeg::encoder_args(&encoder, bits, fps) {
                    Ok(argv) => value(strings(&argv)),
                    Err(error) => raised(py_error(&error)),
                }
            }
            "encode" => {
                let mut config = WorkerConfig::new("http://cluster");
                config.token = golden::string(golden::get(&case, "token"));
                config.ffmpeg = golden::string(golden::get(&case, "ffmpeg"));
                let directory = golden::string(golden::get(&case, "directory"));
                let listing = golden::string(golden::get(&case, "listing"));
                let url = golden::string(golden::get(&case, "url"));
                let run = EncodeRun {
                    url: &url,
                    encoder: &encoder,
                    video_bits: golden::int(golden::get(&case, "video_bits")),
                    fps: float(golden::get(&case, "fps")),
                    seek: float(golden::get(&case, "seek")),
                    chunk_seconds: golden::int(golden::get(&case, "chunk_seconds")),
                    first_index: golden::int(golden::get(&case, "first_index")),
                    directory: Path::new(&directory),
                    listing: Path::new(&listing),
                };
                match ffmpeg::encode_command(&config, &run) {
                    Ok(argv) => value(strings(&argv)),
                    Err(error) => raised(py_error(&error)),
                }
            }
            "uses_nvenc" => value(Value::Bool(ffmpeg::uses_nvenc(&encoder))),
            other => panic!("unknown kind {other}"),
        };
        assert_eq!(got, expected(&case), "case {case:?}");
        checked += 1;
    }
    assert_eq!(checked, 108);
}

fn answer(case: &Value) -> Captured {
    let answer = golden::get(case, "answer");
    Captured {
        returncode: i64::try_from(golden::int(golden::get(answer, "returncode"))).expect("small"),
        stdout: golden::string(golden::get(answer, "stdout")),
        stderr: golden::string(golden::get(answer, "stderr")),
    }
}

fn probe_error(error: &ProbeError) -> Value {
    let class = match error {
        ProbeError::Runtime(_) => "RuntimeError",
        ProbeError::Value(_) => "ValueError",
        ProbeError::Timeout(_) => "TimeoutExpired",
        ProbeError::Os(_) => "OSError",
    };
    super::error(class, &error.to_string())
}

/// `JSONDecodeError` is a `ValueError`; the worker only ever catches the latter.
fn widen(expected: Value) -> Value {
    let Some(error) = expected.get("error") else {
        return expected;
    };
    if golden::string(golden::get(error, "type")) == "JSONDecodeError" {
        return raised(super::error(
            "ValueError",
            &golden::string(golden::get(error, "message")),
        ));
    }
    expected
}

fn calls(runner: &ScriptedRunner) -> Value {
    Value::List(
        runner
            .calls()
            .into_iter()
            .map(|(argv, timeout)| {
                object([
                    (
                        "argv",
                        Value::List(argv.iter().map(|item| Value::Str(super::slashes(item))).collect()),
                    ),
                    (
                        "timeout",
                        timeout.map_or(Value::Null, |after| Value::Int(i128::from(after.as_secs()))),
                    ),
                ])
            })
            .collect(),
    )
}

#[test]
fn ffprobe_readings_match_python() {
    let path = Path::new("/cache/7/cd01/chunk-00003.mkv");
    let mut checked = 0;
    for case in cases("probes") {
        let captured = answer(&case);
        let runner = ScriptedRunner::new(move |_| Ok::<_, RunError>(captured.clone()));
        let got = match golden::string(golden::get(&case, "kind")).as_str() {
            "first_pts" => match ffmpeg::first_pts(&runner, path, "ffprobe") {
                Ok(pts) => value(Value::Float(pts)),
                Err(error) => raised(probe_error(&error)),
            },
            "count_frames" => match ffmpeg::count_frames(&runner, path, "ffprobe") {
                Ok(frames) => value(Value::Int(frames)),
                Err(error) => raised(probe_error(&error)),
            },
            "probe_fps" => {
                let mut config = WorkerConfig::new("http://cluster");
                config.token = golden::string(golden::get(&case, "token"));
                config.ffprobe = "/usr/bin/ffprobe".to_owned();
                match ffmpeg::probe_fps(&runner, "http://cluster/api/transcode/tasks/7/parts/0", &config) {
                    Ok(rate) => value(Value::Float(rate)),
                    Err(error) => raised(probe_error(&error)),
                }
            }
            other => panic!("unknown kind {other}"),
        };
        assert_eq!(got, widen(expected(&case)), "case {case:?}");
        assert_eq!(&calls(&runner), golden::get(&case, "calls"), "case {case:?}");
        checked += 1;
    }
    assert_eq!(checked, 44);
}

#[test]
fn a_probe_that_times_out_names_the_command_without_the_token() {
    // Rust only: subprocess.TimeoutExpired's text in Python prints the token, this worker does not (D168).
    let runner = ScriptedRunner::new(|_| Err(RunError::Timeout(ffmpeg::PROBE_TIMEOUT)));
    let mut config = WorkerConfig::new("http://cluster");
    config.token = "secret-token".to_owned();
    let error = ffmpeg::probe_fps(&runner, "http://cluster/x", &config).expect_err("timed out");
    let text = error.to_string();
    assert!(matches!(error, ProbeError::Timeout(_)));
    assert!(
        text.starts_with("Command '['ffprobe', '-v', 'error', '-headers', 'Authorization: Bearer <redacted>\\r\\n'"),
        "{text}"
    );
    assert!(text.ends_with("' timed out after 600 seconds"), "{text}");
    assert!(!text.contains("secret-token"));
}
