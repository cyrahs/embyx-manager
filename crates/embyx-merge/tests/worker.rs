//! The merge Job's worker, run against real ffmpeg on tiny generated parts.
//!
//! Ported from `tests/test_merge_worker.py`. Python skips the ffmpeg tests when ffmpeg is
//! missing; here they need it (`rust.yml` installs it), as a test that cannot run fails.

use std::path::{Path, PathBuf};
use std::process::Command as Process;
use std::sync::mpsc;
use std::time::Duration;

use embyx_core::pyjson::Value;
use embyx_merge::worker::{
    self, Command, Environment, MAX_UPLOAD_BYTES, Settings, StatusFile, Worker, WorkerError, concat_list, partial_path,
    read_status,
};
use sha1::{Digest as _, Sha1};

fn make_part_sized(path: &Path, size: &str, seconds: u32, audio_first: bool) -> PathBuf {
    let mut command = Process::new("ffmpeg");
    command.args(["-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i"]);
    command.arg(format!("testsrc=size={size}:rate=10:duration={seconds}"));
    command.args(["-f", "lavfi", "-i"]);
    command.arg(format!("sine=frequency=440:duration={seconds}"));
    command.args(["-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest"]);
    if audio_first {
        command.args(["-map", "1:a", "-map", "0:v"]);
    }
    let status = command.arg(path).status().expect("ffmpeg runs (it must be installed)");
    assert!(status.success(), "ffmpeg made {}", path.display());
    path.to_path_buf()
}

fn make_part(path: &Path) -> PathBuf {
    make_part_sized(path, "160x120", 2, false)
}

fn stream_types(path: &Path) -> Vec<String> {
    let output = Process::new("ffprobe")
        .args(["-v", "error", "-show_entries", "stream=codec_type", "-of", "csv=p=0"])
        .arg(path)
        .output()
        .expect("ffprobe runs");
    assert!(output.status.success());
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// The `parts` fixture: three two-second parts under `library/`.
fn three_parts(tmp: &Path) -> Vec<PathBuf> {
    let library = tmp.join("library");
    std::fs::create_dir(&library).expect("mkdir");
    (1..=3)
        .map(|index| make_part(&library.join(format!("ABC-123-cd{index}.mp4"))))
        .collect()
}

fn command(parts: &[PathBuf], output: &Path, status: &Path, reserve_gib: f64) -> Command {
    Command {
        parts: parts.iter().map(|part| part.to_string_lossy().into_owned()).collect(),
        output: output.to_string_lossy().into_owned(),
        status: status.to_string_lossy().into_owned(),
        reserve_gib,
        home_prepare: None,
        home_assemble: None,
    }
}

/// `worker.run(...)` as `__main__.py` calls it.
fn run(worker: &Worker, command: &Command) -> i32 {
    let mut status = StatusFile::new(worker::py_path(&command.status));
    let mut stderr = Vec::new();
    worker::run(worker, command, &mut status, &Environment::from_process(), &mut stderr)
}

fn field<'a>(status: &'a embyx_core::pyjson::Dict, key: &str) -> &'a Value {
    status
        .get(key)
        .unwrap_or_else(|| panic!("status has no {key}: {status:?}"))
}

fn approx(value: &Value, expected: f64, tolerance: f64) -> bool {
    matches!(value, Value::Float(found) if (found - expected).abs() <= tolerance)
}

// py: tests/test_merge_worker.py::test_merge_concatenates_parts_and_reports_size_and_sha1
#[test]
fn merge_concatenates_parts_and_reports_size_and_sha1() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let parts = three_parts(tmp.path());
    let out = tmp.path().join("upload");
    std::fs::create_dir(&out).expect("mkdir");
    let status_path = out.join(".merge").join("1.json");

    let code = run(
        &Worker::default(),
        &command(&parts, &out.join("ABC-123.mp4"), &status_path, 0.0),
    );

    assert_eq!(code, 0);
    let merged = out.join("ABC-123.mp4");
    let status = read_status(&status_path).expect("a status was written");
    assert_eq!(field(&status, "state").as_str(), Some("done"));
    let size = std::fs::metadata(&merged).expect("merged").len();
    assert!(matches!(field(&status, "size"), Value::Int(found) if *found == i128::from(size)));
    let digest = Sha1::digest(std::fs::read(&merged).expect("read"))
        .iter()
        .fold(String::new(), |mut text, byte| {
            use std::fmt::Write as _;
            write!(text, "{byte:02X}").expect("formatting into a string");
            text
        });
    assert_eq!(field(&status, "sha1").as_str(), Some(digest.as_str()));
    assert!(approx(field(&status, "duration"), 6.0, 0.5), "{status:?}");
    assert!(!partial_path(&merged).exists());
}

// py: tests/test_merge_worker.py::test_merge_refuses_parts_with_different_streams
#[test]
fn merge_refuses_parts_with_different_streams() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut parts = three_parts(tmp.path());
    let odd = make_part_sized(&parts[0].with_file_name("ABC-123-cd4.mp4"), "320x240", 2, false);
    parts.push(odd);
    let status_path = tmp.path().join("status.json");

    let code = run(
        &Worker::default(),
        &command(&parts, &tmp.path().join("ABC-123.mp4"), &status_path, 0.0),
    );

    // A refusal is the check's answer, not a broken Job: it ends cleanly.
    assert_eq!(code, 0);
    let status = read_status(&status_path).expect("a status was written");
    assert_eq!(field(&status, "state").as_str(), Some("failed"));
    assert!(matches!(field(&status, "refused"), Value::Bool(true)));
    let error = field(&status, "error").as_str().expect("an error");
    assert!(error.contains("ABC-123-cd4.mp4 carries different streams"), "{error}");
    assert!(!tmp.path().join("ABC-123.mp4").exists());
}

fn streams_in_another_order(suffix: &str) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let library = tmp.path().join("library");
    std::fs::create_dir(&library).expect("mkdir");
    let parts: Vec<PathBuf> = (1..=3)
        .map(|index| {
            make_part_sized(
                &library.join(format!("ABC-123-cd{index}{suffix}")),
                "160x120",
                2,
                index == 1 || index == 3,
            )
        })
        .collect();
    assert_eq!(stream_types(&parts[0]), ["audio", "video"]);
    let out = tmp.path().join("upload");
    std::fs::create_dir(&out).expect("mkdir");
    let mut status = StatusFile::new(out.join("status.json"));
    let merged = out.join(format!("ABC-123{suffix}"));

    let result = Worker::default()
        .merge(&parts, &merged, &mut status, 0, tmp.path(), None)
        .expect("merged");

    assert!(
        approx(result.get("duration").expect("duration"), 6.0, 0.5),
        "{result:?}"
    );
    assert_eq!(stream_types(&merged), ["video", "audio"]);
    let mut names: Vec<String> = std::fs::read_dir(&out)
        .expect("list")
        .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, [format!("ABC-123{suffix}"), "status.json".to_owned()]);
}

// py: tests/test_merge_worker.py::test_merge_puts_parts_with_the_streams_in_another_order_video_first cases=1
#[test]
fn merge_puts_mp4_parts_with_the_streams_in_another_order_video_first() {
    streams_in_another_order(".mp4");
}

// py: tests/test_merge_worker.py::test_merge_puts_parts_with_the_streams_in_another_order_video_first cases=1
#[test]
fn merge_puts_mkv_parts_with_the_streams_in_another_order_video_first() {
    streams_in_another_order(".mkv");
}

// py: tests/test_merge_worker.py::test_merge_refuses_an_mp4_whose_index_would_outgrow_the_memory_limit
#[test]
fn merge_refuses_an_mp4_whose_index_would_outgrow_the_memory_limit() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let parts = three_parts(tmp.path());
    let mut status = StatusFile::new(tmp.path().join("status.json"));
    let output = tmp.path().join("ABC-123.mp4");

    let error = Worker::default()
        .merge(&parts, &output, &mut status, 0, tmp.path(), Some(1 << 20))
        .expect_err("refused");

    let WorkerError::Refused(message) = error else {
        panic!("not a refusal: {error:?}");
    };
    let pattern = regex::Regex::new(r"hold [\d,]+ packets, whose mp4 index needs about").expect("pattern");
    assert!(pattern.is_match(&message), "{message}");
    assert!(!partial_path(&output).exists());
}

// py: tests/test_merge_worker.py::test_merge_re_encodes_parts_bigger_than_115_accepts_to_av1
#[test]
fn merge_re_encodes_parts_bigger_than_115_accepts_to_av1() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let parts = three_parts(tmp.path());
    let total: u64 = parts
        .iter()
        .map(|part| std::fs::metadata(part).expect("part").len())
        .sum();
    let worker = Worker::new(Settings {
        max_upload_bytes: i128::from(total) - 1,
        transcode_preset: "12".to_owned(),
        ..Settings::default()
    });
    let mut status = StatusFile::new(tmp.path().join("status.json"));
    let merged = tmp.path().join("ABC-123.mp4");

    let result = worker
        .merge(&parts, &merged, &mut status, 0, tmp.path(), None)
        .expect("merged");

    let size = std::fs::metadata(&merged).expect("merged").len();
    assert!(matches!(result.get("size"), Some(Value::Int(found)) if *found == i128::from(size)));
    assert!(size < total);
    assert!(
        approx(result.get("duration").expect("duration"), 6.0, 0.5),
        "{result:?}"
    );
    let output = Process::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_name,codec_tag_string",
            "-of",
            "csv=p=0",
        ])
        .arg(&merged)
        .output()
        .expect("ffprobe runs");
    assert!(output.status.success());
    let streams: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    assert_eq!(streams, ["av1,av01", "aac,mp4a"]);
    let written = read_status(&tmp.path().join("status.json")).expect("a status was written");
    // The status keeps the bitrate the transcoding phase chose.
    assert!(matches!(field(&written, "video_bitrate"), Value::Int(bits) if *bits > 0));
}

fn refuses_a_container_it_cannot_re_encode(suffix: &str) {
    // MOV cannot carry AV1, so it is refused like AVI.
    let tmp = tempfile::tempdir().expect("tempdir");
    let parts: Vec<PathBuf> = (1..=2)
        .map(|index| make_part(&tmp.path().join(format!("ABC-123-cd{index}{suffix}"))))
        .collect();
    let worker = Worker::new(Settings {
        max_upload_bytes: 1000,
        ..Settings::default()
    });
    let mut status = StatusFile::new(tmp.path().join("status.json"));

    let error = worker
        .merge(
            &parts,
            &tmp.path().join(format!("ABC-123{suffix}")),
            &mut status,
            0,
            tmp.path(),
            None,
        )
        .expect_err("refused");

    let WorkerError::Refused(message) = error else {
        panic!("not a refusal: {error:?}");
    };
    let wanted = format!("115 accepts in one file, and a {suffix} merge cannot be re-encoded");
    assert!(message.contains(&wanted), "{message}");
}

// py: tests/test_merge_worker.py::test_merge_refuses_parts_bigger_than_115_accepts_in_a_container_it_cannot_re_encode cases=1
#[test]
fn merge_refuses_avi_parts_bigger_than_115_accepts() {
    refuses_a_container_it_cannot_re_encode(".avi");
}

// py: tests/test_merge_worker.py::test_merge_refuses_parts_bigger_than_115_accepts_in_a_container_it_cannot_re_encode cases=1
#[test]
fn merge_refuses_mov_parts_bigger_than_115_accepts() {
    refuses_a_container_it_cannot_re_encode(".mov");
}

// py: tests/test_merge_worker.py::test_merge_refuses_a_re_encode_that_still_comes_out_too_big
#[test]
fn merge_refuses_a_re_encode_that_still_comes_out_too_big() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let parts = three_parts(tmp.path());
    let total: u64 = parts
        .iter()
        .map(|part| std::fs::metadata(part).expect("part").len())
        .sum();
    // A budget the encoder cannot hold to: the size check after the encode has the last word.
    let worker = Worker::new(Settings {
        max_upload_bytes: i128::from(total) - 1,
        transcode_preset: "12".to_owned(),
        video_bits: Some(50_000_000),
        ..Settings::default()
    });
    let mut status = StatusFile::new(tmp.path().join("status.json"));

    let error = worker
        .merge(
            &parts,
            &tmp.path().join("ABC-123.mp4"),
            &mut status,
            0,
            tmp.path(),
            None,
        )
        .expect_err("refused");

    assert!(
        matches!(&error, WorkerError::Refused(message) if message.contains("re-encoded file came to")),
        "{error:?}"
    );
    assert!(!tmp.path().join("ABC-123.mp4").exists());
}

// py: tests/test_merge_worker.py::test_transcode_bitrate_fits_the_budget_or_a_share_of_the_source
#[test]
fn transcode_bitrate_fits_the_budget_or_a_share_of_the_source() {
    use worker::{
        HOME_BUDGET_SHARE, HOME_SOURCE_SHARE, TRANSCODE_BUDGET_SHARE, TRANSCODE_SOURCE_SHARE, transcode_bitrate,
    };
    let hours = 48.0 * 3600.0;
    let cpu = |total, audio| {
        transcode_bitrate(
            total,
            hours,
            audio,
            TRANSCODE_SOURCE_SHARE,
            TRANSCODE_BUDGET_SHARE,
            MAX_UPLOAD_BYTES,
        )
    };
    let approx_rel = |found: f64, expected: f64, rel: f64| (found - expected).abs() <= expected.abs() * rel;
    // IDBD-905: 130 GB over 48 h, 256 kb/s AAC: the source share (0.4 x 5.76 Mb/s) is lower.
    let bits = cpu(129_600_000_000, 256_000).expect("fits");
    #[allow(clippy::cast_precision_loss, reason = "test arithmetic")]
    let source = 0.4 * (129_600_000_000.0 * 8.0 / hours - 256_000.0);
    #[allow(clippy::cast_precision_loss, reason = "test arithmetic")]
    {
        assert!(approx_rel(bits as f64, source, 1e-6), "{bits}");
        // A source far above the budget gets the budget, audio included, under 115 GB.
        let bits = cpu(400_000_000_000, 256_000).expect("fits");
        let budget = MAX_UPLOAD_BYTES as f64 * TRANSCODE_BUDGET_SHARE;
        assert!(
            approx_rel((bits as f64 + 256_000.0) * hours / 8.0, budget, 1e-6),
            "{bits}"
        );
        // The home worker's NVENC overshoots its target, so it aims lower under the limit.
        let home = transcode_bitrate(
            400_000_000_000,
            hours,
            256_000,
            HOME_SOURCE_SHARE,
            HOME_BUDGET_SHARE,
            MAX_UPLOAD_BYTES,
        )
        .expect("fits");
        assert!((home as f64 + 256_000.0) * hours / 8.0 * 1.15 < MAX_UPLOAD_BYTES as f64);
    }
    let error = cpu(400_000_000_000, 1_000_000_000).expect_err("no room");
    assert!(
        matches!(&error, WorkerError::Refused(message) if message.contains("audio alone")),
        "{error:?}"
    );
}

// py: tests/test_merge_worker.py::test_transcode_memory_scales_with_the_frame_and_keeps_a_floor
#[test]
fn transcode_memory_scales_with_the_frame_and_keeps_a_floor() {
    use worker::{Probe, TRANSCODE_MEMORY_BYTES, TRANSCODE_MEMORY_FLOOR_BYTES, transcode_memory};
    let video = |width: Value, height: Value| Probe {
        duration: 1.0,
        signature: vec![vec![
            Value::Str("video".to_owned()),
            Value::Str("h264".to_owned()),
            width,
            height,
            Value::Null,
            Value::Null,
        ]],
        layout: Vec::new(),
        packets: None,
        audio_bits: 0,
    };
    // UNVRSP-003's 4096x2048 VR frames are the reference.
    assert_eq!(
        transcode_memory(&video(Value::Int(4096), Value::Int(2048))),
        TRANSCODE_MEMORY_BYTES
    );
    assert_eq!(
        transcode_memory(&video(Value::Int(1920), Value::Int(1080))),
        TRANSCODE_MEMORY_FLOOR_BYTES
    );
    assert_eq!(
        transcode_memory(&video(Value::Int(8192), Value::Int(4096))),
        4 * TRANSCODE_MEMORY_BYTES
    );
    // No size to go by: assume the reference.
    assert_eq!(
        transcode_memory(&video(Value::Null, Value::Null)),
        TRANSCODE_MEMORY_BYTES
    );
}

// py: tests/test_merge_worker.py::test_memory_limit_reads_the_cgroup
#[test]
fn memory_limit_reads_the_cgroup() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let limit = tmp.path().join("memory.max");
    std::fs::write(&limit, "3221225472\n").expect("write");
    assert_eq!(worker::memory_limit(&limit), Ok(Some(3 * 1024 * 1024 * 1024)));
    std::fs::write(&limit, "max\n").expect("write");
    assert_eq!(worker::memory_limit(&limit), Ok(None));
    assert_eq!(worker::memory_limit(&tmp.path().join("missing")), Ok(None));
}

// py: tests/test_merge_worker.py::test_merge_refuses_to_start_without_room_for_the_reserve
#[test]
fn merge_refuses_to_start_without_room_for_the_reserve() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let parts = three_parts(tmp.path());
    let mut status = StatusFile::new(tmp.path().join("status.json"));

    let error = Worker::default()
        .merge(
            &parts,
            &tmp.path().join("ABC-123.mp4"),
            &mut status,
            1 << 60,
            tmp.path(),
            None,
        )
        .expect_err("no room");

    assert!(
        matches!(&error, WorkerError::Failed(message) if message.contains("not enough space")),
        "{error:?}"
    );
}

// py: tests/test_merge_worker.py::test_a_merge_short_of_space_fails_the_job
#[test]
fn a_merge_short_of_space_fails_the_job() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let parts = three_parts(tmp.path());
    let status_path = tmp.path().join("status.json");

    #[allow(clippy::cast_precision_loss, reason = "1 << 30 is exact")]
    let code = run(
        &Worker::default(),
        &command(
            &parts,
            &tmp.path().join("ABC-123.mp4"),
            &status_path,
            (1_u64 << 30) as f64,
        ),
    );

    // Room can be made, so this is a failure to retry rather than a refusal.
    assert_eq!(code, 1);
    let status = read_status(&status_path).expect("a status was written");
    assert_eq!(field(&status, "state").as_str(), Some("failed"));
    assert!(status.get("refused").is_none());
}

// py: tests/test_merge_worker.py::test_merge_keeps_the_parts_container_and_refuses_a_mix
#[test]
fn merge_keeps_the_parts_container_and_refuses_a_mix() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let parts = three_parts(tmp.path());
    let mut status = StatusFile::new(tmp.path().join("status.json"));

    let error = Worker::default()
        .merge(
            &parts,
            &tmp.path().join("ABC-123.mkv"),
            &mut status,
            0,
            tmp.path(),
            None,
        )
        .expect_err("refused");

    assert!(
        matches!(&error, WorkerError::Refused(message) if message.contains("share one container")),
        "{error:?}"
    );
}

// py: tests/test_merge_worker.py::test_concat_list_escapes_quotes
#[test]
fn concat_list_escapes_quotes() {
    assert_eq!(
        concat_list(&[PathBuf::from("/a/it's.mp4"), PathBuf::from("/b/c.mp4")]),
        "file '/a/it'\\''s.mp4'\nfile '/b/c.mp4'\n"
    );
}

/// Run `script` with `sh` in ffmpeg's place on a thread, so a deadlock fails the test instead
/// of hanging it.
fn run_fake_ffmpeg(tmp: &Path, script: &str, stall_seconds: f64) -> Result<(), WorkerError> {
    let status_path = tmp.join("status.json");
    let script = script.to_owned();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let worker = Worker::new(Settings {
            stall_seconds,
            ..Settings::default()
        });
        let mut status = StatusFile::new(status_path);
        let command = vec!["sh".to_owned(), "-c".to_owned(), script];
        let _ = sender.send(worker.run_ffmpeg(&command, 10.0, &mut status));
    });
    receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("the worker hung on ffmpeg")
}

// py: tests/test_merge_worker.py::test_a_flood_on_stderr_does_not_stall_the_merge
#[test]
fn a_flood_on_stderr_does_not_stall_the_merge() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // Far more than a pipe holds is written to stderr before any progress, as a damaged part
    // makes ffmpeg do; then a failure, whose last lines are reported.
    let script = "i=0; while [ $i -lt 20000 ]; do echo \"damaged element $i\" >&2; i=$((i+1)); done; \
                  echo out_time_us=10000000; echo total_size=4096; exit 1";

    let error = run_fake_ffmpeg(tmp.path(), script, 60.0).expect_err("ffmpeg failed");

    let WorkerError::Failed(message) = error else {
        panic!("not a worker error: {error:?}");
    };
    assert!(
        message.starts_with("ffmpeg failed: damaged element 19980 |"),
        "{message}"
    );
    assert!(message.ends_with("damaged element 19999"), "{message}");
    let status = read_status(&tmp.path().join("status.json")).expect("a status was written");
    assert!(matches!(field(&status, "progress"), Value::Float(progress) if (*progress - 1.0).abs() < f64::EPSILON));
}

// py: tests/test_merge_worker.py::test_ffmpeg_that_stops_moving_is_stopped
#[test]
fn ffmpeg_that_stops_moving_is_stopped() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // Progress blocks keep coming, as they do from a stuck ffmpeg, but never move.
    let script = "echo stuck >&2; while true; do echo out_time_us=5000000; echo total_size=4096; sleep 0.05; done";

    let error = run_fake_ffmpeg(tmp.path(), script, 1.0).expect_err("stopped");

    assert_eq!(
        error,
        WorkerError::Failed("ffmpeg made no progress for 1 seconds and was stopped: stuck".to_owned())
    );
}
