//! The worker's settings: a TOML file and the command line, which wins.
//!
//! Python origin: `default_config_path`, `default_cache_dir`, `WorkerConfig`, `add_arguments`
//! and `load_config` in `src/embyx_manager/merge/home_worker.py`. The file's keys and the
//! conversions Python applies to them (`str()`, `float()`, `int()`, `dict()`) are part of
//! the worker contract (PLAN.md, cross-version contract 5): a config file that works with
//! one build works with the other.

use std::fmt;
use std::path::{Path, PathBuf};

use embyx_core::pyjson::{Dict, Value};

use crate::py::{self, PyError};

/// The command line's options (`add_arguments`); `None` where the option was not given.
#[derive(Clone, Default)]
pub struct Args {
    pub config: Option<PathBuf>,
    pub server: Option<String>,
    pub token: Option<String>,
    pub cache_dir: Option<PathBuf>,
    pub upload_mbps: Option<f64>,
    pub jobs: Option<i128>,
    pub max_queue_gb: Option<f64>,
    pub ffmpeg: Option<String>,
    pub ffprobe: Option<String>,
}

impl fmt::Debug for Args {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Args")
            .field("config", &self.config)
            .field("server", &self.server)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("cache_dir", &self.cache_dir)
            .field("upload_mbps", &self.upload_mbps)
            .field("jobs", &self.jobs)
            .field("max_queue_gb", &self.max_queue_gb)
            .field("ffmpeg", &self.ffmpeg)
            .field("ffprobe", &self.ffprobe)
            .finish()
    }
}

/// `WorkerConfig`.
#[derive(Clone)]
pub struct WorkerConfig {
    pub server: String,
    pub token: String,
    pub cache_dir: PathBuf,
    pub upload_mbps: f64,
    pub jobs: i128,
    pub max_queue_gb: f64,
    pub ffmpeg: String,
    pub ffprobe: String,
    pub name: String,
    /// Replaces keys of the encoder settings the cluster sends (for a test without NVENC, say).
    pub encoder: Dict,
}

impl fmt::Debug for WorkerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerConfig")
            .field("server", &self.server)
            .field("token", &if self.token.is_empty() { "" } else { "<redacted>" })
            .field("cache_dir", &self.cache_dir)
            .field("upload_mbps", &self.upload_mbps)
            .field("jobs", &self.jobs)
            .field("max_queue_gb", &self.max_queue_gb)
            .field("ffmpeg", &self.ffmpeg)
            .field("ffprobe", &self.ffprobe)
            .field("name", &self.name)
            .field("encoder", &self.encoder)
            .finish()
    }
}

impl WorkerConfig {
    /// `WorkerConfig(server=...)` with every other field at its default.
    pub fn new(server: impl Into<String>) -> Self {
        Self::with_environment(server, &Environment::from_process())
    }

    fn with_environment(server: impl Into<String>, environment: &Environment) -> Self {
        Self {
            server: server.into(),
            token: String::new(),
            cache_dir: default_cache_dir(environment),
            upload_mbps: 30.0,
            jobs: 3,
            max_queue_gb: 8.0,
            ffmpeg: "ffmpeg".to_owned(),
            ffprobe: "ffprobe".to_owned(),
            name: environment.hostname.clone(),
            encoder: Dict::new(),
        }
    }
}

/// What the defaults read from the process: `APPDATA`, `LOCALAPPDATA`, the home directory
/// and `socket.gethostname()`. Injected so the goldens can pin them.
#[derive(Debug, Clone)]
pub struct Environment {
    pub appdata: Option<String>,
    pub local_appdata: Option<String>,
    pub home: PathBuf,
    pub hostname: String,
}

impl Environment {
    pub fn from_process() -> Self {
        Self {
            appdata: std::env::var("APPDATA").ok(),
            local_appdata: std::env::var("LOCALAPPDATA").ok(),
            home: std::env::home_dir().unwrap_or_default(),
            hostname: crate::process::hostname(),
        }
    }
}

/// `default_config_path()`: `%APPDATA%\embyx-transcode\config.toml`, else
/// `~/.config/embyx-transcode/config.toml`.
pub fn default_config_path(environment: &Environment) -> PathBuf {
    let root = match environment.appdata.as_deref() {
        Some(base) if !base.is_empty() => PathBuf::from(base),
        _ => environment.home.join(".config"),
    };
    root.join("embyx-transcode").join("config.toml")
}

/// `default_cache_dir()`: `%LOCALAPPDATA%\embyx-transcode`, else `~/.cache/embyx-transcode`.
pub fn default_cache_dir(environment: &Environment) -> PathBuf {
    let root = match environment.local_appdata.as_deref() {
        Some(base) if !base.is_empty() => PathBuf::from(base),
        _ => environment.home.join(".cache"),
    };
    root.join("embyx-transcode")
}

/// Why `load_config` stopped the worker.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// `raise SystemExit(msg)`: the message alone on stderr, exit status 1.
    #[error("{0}")]
    Exit(String),
    /// An exception Python does not catch there (a TOML syntax error, `float('x')`, ...):
    /// its traceback ends the worker with status 1.
    #[error("{}: {}", .0.class, .0.message)]
    Raised(PyError),
}

/// `load_config(args)`.
pub fn load_config(args: &Args) -> Result<WorkerConfig, ConfigError> {
    load_config_with(args, &Environment::from_process())
}

/// `load_config(args)` with the environment given.
pub fn load_config_with(args: &Args, environment: &Environment) -> Result<WorkerConfig, ConfigError> {
    let path = args.config.clone().unwrap_or_else(|| default_config_path(environment));
    let mut values = Dict::new();
    if path.is_file() {
        values = read_toml(&path)?;
    } else if args.config.is_some() {
        return Err(ConfigError::Exit(format!("config file not found: {}", path.display())));
    }
    let overrides = [
        ("server", args.server.clone().map(Value::Str)),
        ("token", args.token.clone().map(Value::Str)),
        // A path from the command line is always taken (a `Path` is never false); it is
        // kept apart below so a name that is not UTF-8 survives.
        (
            "cache_dir",
            args.cache_dir
                .as_ref()
                .map(|path| Value::Str(path.to_string_lossy().into_owned())),
        ),
        ("upload_mbps", args.upload_mbps.map(Value::Float)),
        ("jobs", args.jobs.map(Value::Int)),
        ("max_queue_gb", args.max_queue_gb.map(Value::Float)),
        ("ffmpeg", args.ffmpeg.clone().map(Value::Str)),
        ("ffprobe", args.ffprobe.clone().map(Value::Str)),
    ];
    for (key, value) in overrides {
        if let Some(value) = value {
            values.insert(key, value);
        }
    }
    if !values.get("server").is_some_and(py::truthy) {
        return Err(ConfigError::Exit(format!(
            "no server given: pass --server or set server in {}",
            path.display()
        )));
    }
    let server = values.get("server").map(py::str_of).unwrap_or_default();
    let mut config = WorkerConfig::with_environment(server.trim_end_matches('/'), environment);
    config.token = values.get("token").map(py::str_of).unwrap_or_default();
    if let Some(path) = &args.cache_dir {
        config.cache_dir.clone_from(path);
    } else if let Some(value) = values.get("cache_dir").filter(|value| py::truthy(value)) {
        config.cache_dir = path_of(value).map_err(ConfigError::Raised)?;
    }
    let raised = ConfigError::Raised;
    if let Some(value) = values.get("upload_mbps") {
        config.upload_mbps = py::float(value).map_err(raised)?;
    }
    if let Some(value) = values.get("jobs") {
        config.jobs = py::int(value).map_err(raised)?.max(1);
    }
    if let Some(value) = values.get("max_queue_gb") {
        config.max_queue_gb = py::float(value).map_err(raised)?;
    }
    if let Some(value) = values.get("ffmpeg") {
        config.ffmpeg = py::str_of(value);
    }
    if let Some(value) = values.get("ffprobe") {
        config.ffprobe = py::str_of(value);
    }
    if let Some(value) = values.get("name") {
        config.name = py::str_of(value);
    }
    if let Some(value) = values.get("encoder") {
        config.encoder = dict_of(value).map_err(raised)?;
    }
    Ok(config)
}

/// `tomllib.loads(path.read_text(encoding='utf-8'))`.
fn read_toml(path: &Path) -> Result<Dict, ConfigError> {
    let bytes = std::fs::read(path).map_err(|error| {
        ConfigError::Raised(PyError::new(
            "OSError",
            format!("{error}: {}", embyx_core::pytext::py_repr(&path.to_string_lossy())),
        ))
    })?;
    let text = String::from_utf8(bytes).map_err(|error| {
        ConfigError::Raised(PyError::new(
            "UnicodeDecodeError",
            format!("'utf-8' codec can't decode the file: {error}"),
        ))
    })?;
    let table: toml::Table = text
        .parse()
        .map_err(|error: toml::de::Error| ConfigError::Raised(PyError::new("TOMLDecodeError", error.message())))?;
    Ok(table_to_dict(&table))
}

/// A TOML table as `tomllib` gives it, as a JSON-shaped value. A date or time becomes its
/// TOML text (`tomllib` gives a `datetime`, whose `str()` puts a space for the `T`; no key
/// the worker reads holds one).
pub(crate) fn table_to_dict(table: &toml::Table) -> Dict {
    let mut dict = Dict::new();
    for (key, value) in table {
        dict.insert(key.clone(), toml_to_value(value));
    }
    dict
}

fn toml_to_value(value: &toml::Value) -> Value {
    match value {
        toml::Value::String(text) => Value::Str(text.clone()),
        toml::Value::Integer(number) => Value::Int(i128::from(*number)),
        toml::Value::Float(number) => Value::Float(*number),
        toml::Value::Boolean(flag) => Value::Bool(*flag),
        toml::Value::Datetime(when) => Value::Str(when.to_string()),
        toml::Value::Array(items) => Value::List(items.iter().map(toml_to_value).collect()),
        toml::Value::Table(table) => Value::Dict(table_to_dict(table)),
    }
}

/// `Path(value)`.
fn path_of(value: &Value) -> Result<PathBuf, PyError> {
    match value {
        Value::Str(text) => Ok(PathBuf::from(text)),
        other => Err(PyError::new(
            "TypeError",
            format!(
                "argument should be a str or an os.PathLike object where __fspath__ returns a str, not '{}'",
                py::type_name(other)
            ),
        )),
    }
}

/// `dict(value)`: a table, or a list of key-value pairs.
fn dict_of(value: &Value) -> Result<Dict, PyError> {
    match value {
        Value::Dict(dict) => Ok(dict.clone()),
        Value::List(items) => {
            let mut dict = Dict::new();
            for (at, item) in items.iter().enumerate() {
                let pair = match item {
                    Value::List(pair) => pair.clone(),
                    Value::Str(text) => text.chars().map(|ch| Value::Str(ch.to_string())).collect(),
                    other => {
                        return Err(PyError::new(
                            "TypeError",
                            format!(
                                "cannot convert dictionary update sequence element #{at} to a sequence (got '{}')",
                                py::type_name(other)
                            ),
                        ));
                    }
                };
                let [key, value] = <[Value; 2]>::try_from(pair).map_err(|pair| {
                    PyError::new(
                        "ValueError",
                        format!(
                            "dictionary update sequence element #{at} has length {}; 2 is required",
                            pair.len()
                        ),
                    )
                })?;
                let Value::Str(key) = key else {
                    return Err(PyError::new(
                        "TypeError",
                        format!("unhashable or non-string key {}", py::repr(&key)),
                    ));
                };
                dict.insert(key, value);
            }
            Ok(dict)
        }
        Value::Str(text) if text.is_empty() => Ok(Dict::new()),
        Value::Str(text) => Err(PyError::new(
            "ValueError",
            format!(
                "dictionary update sequence element #0 has length {}; 2 is required",
                usize::from(!text.is_empty())
            ),
        )),
        other => Err(PyError::new(
            "TypeError",
            format!("'{}' object is not iterable", py::type_name(other)),
        )),
    }
}
