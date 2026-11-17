//! The chunks waiting for their upload, and the upload's pace.
//!
//! Python origin: `sha256_of`, `LocalChunk`, `kept_chunks`, `RateLimiter` and `RateMeter` of
//! `src/embyx_manager/merge/home_worker.py`. The cache layout (`<cache>/<task>/cdNN/`, a
//! `chunk-NNNNN.mkv` with a `chunk-NNNNN.json` sidecar written through a `.name.tmp` file) is
//! part of cross-version contract 5: either build carries on from chunks the other left.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use embyx_core::pyjson::{self, DumpOptions, Value};
use sha2::{Digest as _, Sha256};

/// `sha256_of(path)`: read in pieces of 4 MiB.
pub fn sha256_of(path: &Path) -> io::Result<String> {
    let mut digest = Sha256::new();
    let mut handle = fs::File::open(path)?;
    let mut buffer = vec![0_u8; 4 * 1024 * 1024];
    loop {
        let read = handle.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

/// `LocalChunk`: a finished chunk in the cache, described by the JSON file next to it.
#[derive(Debug, Clone, PartialEq)]
pub struct LocalChunk {
    pub part_index: i128,
    pub index: i128,
    pub start: f64,
    pub duration: f64,
    pub frames: i128,
    pub bytes: i128,
    pub sha256: String,
    pub path: PathBuf,
}

impl LocalChunk {
    /// `chunk.end`.
    pub fn end(&self) -> f64 {
        self.start + self.duration
    }

    /// `chunk.sidecar()`: the chunk's path with `.json` for its suffix.
    pub fn sidecar(&self) -> PathBuf {
        self.path.with_extension("json")
    }

    /// The sidecar's bytes: `json.dumps(payload)`.
    pub fn sidecar_text(&self) -> String {
        let mut payload = pyjson::Dict::new();
        payload.insert("part_index", Value::Int(self.part_index));
        payload.insert("index", Value::Int(self.index));
        payload.insert("start", Value::Float(self.start));
        payload.insert("duration", Value::Float(self.duration));
        payload.insert("frames", Value::Int(self.frames));
        payload.insert("bytes", Value::Int(self.bytes));
        payload.insert("sha256", Value::Str(self.sha256.clone()));
        pyjson::dumps(&Value::Dict(payload), &DumpOptions::python_default()).unwrap_or_default()
    }

    /// `chunk.save()`: the sidecar written through a temporary file and replaced in one step.
    pub fn save(&self) -> io::Result<()> {
        let sidecar = self.sidecar();
        let name = sidecar
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let temporary = sidecar.with_file_name(format!(".{name}.tmp"));
        fs::write(&temporary, self.sidecar_text())?;
        fs::rename(&temporary, &sidecar)
    }

    /// `chunk.remove()`: the chunk and its sidecar, either already gone or not.
    pub fn remove(&self) -> io::Result<()> {
        remove_missing_ok(&self.path)?;
        remove_missing_ok(&self.sidecar())
    }

    /// `LocalChunk.load(sidecar)`: `None` for a sidecar that cannot be read, is not the
    /// worker's JSON, or whose chunk is missing or of another size.
    pub fn load(sidecar: &Path) -> Option<Self> {
        // `cls(path=..., **payload)`: exactly the seven fields, no other key.
        const FIELDS: [&str; 7] = ["part_index", "index", "start", "duration", "frames", "bytes", "sha256"];
        let bytes = fs::read(sidecar).ok()?;
        let text = String::from_utf8(bytes).ok()?;
        let Value::Dict(payload) = pyjson::loads(&text).ok()? else {
            return None;
        };
        if payload.len() != FIELDS.len() || FIELDS.iter().any(|field| !payload.contains_key(field)) {
            return None;
        }
        let int = |key: &str| match payload.get(key) {
            Some(Value::Int(number)) => Some(*number),
            _ => None,
        };
        let float = |key: &str| match payload.get(key) {
            Some(Value::Float(number)) => Some(*number),
            #[allow(clippy::cast_precision_loss, reason = "Python adds an int to a float the same way")]
            Some(Value::Int(number)) => Some(*number as f64),
            _ => None,
        };
        let chunk = Self {
            part_index: int("part_index")?,
            index: int("index")?,
            start: float("start")?,
            duration: float("duration")?,
            frames: int("frames")?,
            bytes: int("bytes")?,
            sha256: payload.get("sha256")?.as_str()?.to_owned(),
            path: sidecar.with_extension("mkv"),
        };
        let metadata = fs::metadata(&chunk.path).ok()?;
        if !metadata.is_file() || i128::from(metadata.len()) != chunk.bytes {
            return None;
        }
        Some(chunk)
    }
}

/// `path.unlink(missing_ok=True)`.
pub(crate) fn remove_missing_ok(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

fn names(directory: &Path) -> Vec<(String, PathBuf)> {
    let Ok(entries) = fs::read_dir(directory) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| (entry.file_name().to_string_lossy().into_owned(), entry.path()))
        .collect()
}

/// `kept_chunks(directory, next_index, resume_at)`: the chunks in `directory` that carry on
/// from what the cluster has; everything else there is deleted.
pub fn kept_chunks(directory: &Path, next_index: i128, resume_at: f64) -> Vec<LocalChunk> {
    let mut found: HashMap<i128, LocalChunk> = HashMap::new();
    if directory.is_dir() {
        for (name, path) in names(directory) {
            if name.starts_with("chunk-")
                && Path::new(&name).extension().is_some_and(|suffix| suffix == "json")
                && let Some(chunk) = LocalChunk::load(&path)
            {
                found.insert(chunk.index, chunk);
            }
        }
    }
    let mut kept = Vec::new();
    let (mut expected_index, mut expected_start) = (next_index, resume_at);
    while let Some(chunk) = found
        .get(&expected_index)
        .filter(|chunk| (chunk.start - expected_start).abs() <= 0.25)
    {
        let chunk = chunk.clone();
        found.remove(&expected_index);
        expected_index = chunk.index + 1;
        expected_start = chunk.end();
        kept.push(chunk);
    }
    let keep: Vec<std::ffi::OsString> = kept
        .iter()
        .flat_map(|chunk| {
            [
                chunk.path.file_name().map(ToOwned::to_owned),
                chunk.sidecar().file_name().map(ToOwned::to_owned),
            ]
        })
        .flatten()
        .collect();
    if let Ok(entries) = fs::read_dir(directory) {
        for entry in entries.filter_map(Result::ok) {
            if !keep.contains(&entry.file_name()) {
                // `leftover.unlink()` with OSError suppressed: a directory stays.
                let _ = fs::remove_file(entry.path());
            }
        }
    }
    kept
}

/// A monotonic clock and a sleep, injected so tests need not wait.
pub trait Clock: Send + Sync {
    /// `time.monotonic()`, in seconds.
    fn monotonic(&self) -> f64;
    /// `time.sleep(seconds)`.
    fn sleep(&self, seconds: f64);
}

/// The real clock.
#[derive(Debug, Clone, Copy)]
pub struct SystemClock {
    origin: Instant,
}

impl Default for SystemClock {
    fn default() -> Self {
        Self { origin: Instant::now() }
    }
}

impl Clock for SystemClock {
    fn monotonic(&self) -> f64 {
        self.origin.elapsed().as_secs_f64()
    }

    fn sleep(&self, seconds: f64) {
        if seconds > 0.0 && seconds.is_finite() {
            std::thread::sleep(Duration::from_secs_f64(seconds));
        }
    }
}

/// `RateLimiter`: a token bucket shared by the uploads; `bits_per_second <= 0` means no cap.
pub struct RateLimiter {
    rate: f64,
    clock: Arc<dyn Clock>,
    state: Mutex<(f64, f64)>,
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("rate", &self.rate)
            .finish_non_exhaustive()
    }
}

impl RateLimiter {
    pub fn new(bits_per_second: f64, clock: Arc<dyn Clock>) -> Self {
        let last = clock.monotonic();
        Self {
            rate: bits_per_second / 8.0,
            clock,
            state: Mutex::new((0.0, last)),
        }
    }

    /// `take(count)`: sleeps as long as `count` more bytes need to keep under the rate.
    pub fn take(&self, count: usize) {
        if self.rate <= 0.0 {
            return;
        }
        let wait = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let (allowance, last) = &mut *state;
            let now = self.clock.monotonic();
            *allowance = crate::py::min(*allowance + (now - *last) * self.rate, self.rate);
            *last = now;
            #[allow(clippy::cast_precision_loss, reason = "a piece of the upload, far below 2**53")]
            {
                *allowance -= count as f64;
            }
            if *allowance < 0.0 { -*allowance / self.rate } else { 0.0 }
        };
        if wait > 0.0 {
            self.clock.sleep(wait);
        }
    }
}

/// `RateMeter`: bytes per second over the last few seconds.
pub struct RateMeter {
    window: f64,
    clock: Arc<dyn Clock>,
    samples: Mutex<VecDeque<(f64, usize)>>,
}

impl std::fmt::Debug for RateMeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateMeter")
            .field("window", &self.window)
            .finish_non_exhaustive()
    }
}

impl RateMeter {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            window: 10.0,
            clock,
            samples: Mutex::new(VecDeque::new()),
        }
    }

    /// `add(count)`.
    pub fn add(&self, count: usize) {
        let now = self.clock.monotonic();
        self.samples
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back((now, count));
    }

    /// `rate()`.
    pub fn rate(&self) -> f64 {
        let now = self.clock.monotonic();
        let mut samples = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        while samples.front().is_some_and(|(at, _)| now - at > self.window) {
            samples.pop_front();
        }
        let total: usize = samples.iter().map(|(_, count)| count).sum();
        #[allow(clippy::cast_precision_loss, reason = "bytes over ten seconds, far below 2**53")]
        let total = total as f64;
        total / self.window
    }
}
