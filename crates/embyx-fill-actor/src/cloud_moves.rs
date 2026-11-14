//! CloudDrive moves of `.strm`-mapped videos: the metadata snapshot a plan keeps of the
//! remote file, the mover port, and the mapping of a `.strm` file to its CloudDrive path.
//!
//! Python origin: `src/embyx_manager/fill_actor/cloud_moves.py`. Every check and message is
//! Python's (`goldens/fill_actor/cloud_files.json`, `cloud_paths.json`): the plan's stored
//! `cloud_file_json` is built from [`CloudFileMetadata`], and the move journal depends on
//! [`CloudMovePaths::parse_mapping`] giving the same API path and digest in both builds.

use std::fmt;
use std::future::Future;
use std::io::Read as _;
use std::path::Path;
use std::pin::Pin;

use embyx_clients::clouddrive::CloudDriveError;
use embyx_core::pyjson::{Dict, Value};
use embyx_core::pytext::{casefold, py_strip};
use sha2::{Digest as _, Sha256};

/// A boxed future, for the object-safe ports.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// `MAX_STRM_BYTES`.
pub const MAX_STRM_BYTES: usize = 4096;
/// `VIDEO_SUFFIXES`.
pub const VIDEO_SUFFIXES: [&str; 9] = [".mp4", ".mkv", ".avi", ".wmv", ".mov", ".flv", ".m4v", ".ts", ".rmvb"];

/// A Python `ValueError` or `TypeError` raised by a check here, with its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckError {
    /// `ValueError(message)`.
    Value(String),
    /// `TypeError(message)`.
    Type(String),
    /// `InvalidStrmTargetError(message)`, a `ValueError` subclass.
    InvalidStrmTarget(String),
}

impl CheckError {
    fn value(message: impl Into<String>) -> Self {
        Self::Value(message.into())
    }

    fn strm(message: &str) -> Self {
        Self::InvalidStrmTarget(message.to_owned())
    }

    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Value(_) => "ValueError",
            Self::Type(_) => "TypeError",
            Self::InvalidStrmTarget(_) => "InvalidStrmTargetError",
        }
    }

    /// `isinstance(exc, ValueError)` (an `InvalidStrmTargetError` is one).
    pub fn is_value_error(&self) -> bool {
        !matches!(self, Self::Type(_))
    }
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(message) | Self::Type(message) | Self::InvalidStrmTarget(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for CheckError {}

/// `CloudFileMetadata`: what a plan remembers of the remote file it will move, compared
/// again before the move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudFileMetadata {
    pub path: String,
    pub file_id: String,
    pub name: String,
    pub size: i128,
    /// Nanoseconds since the epoch.
    pub write_time: i128,
    /// `(kind, hash)` pairs, sorted.
    pub hashes: Vec<(String, String)>,
}

fn non_negative_int(value: Option<&Value>) -> Option<i128> {
    match value {
        Some(Value::Int(number)) if *number >= 0 => Some(*number),
        _ => None,
    }
}

impl CloudFileMetadata {
    /// `CloudFileMetadata.from_mapping(value)`: the snapshot from a listing entry or from
    /// the stored `cloud_file_json`, refused unless every field has its type and the path
    /// is normalized and names the file.
    pub fn from_mapping(value: &Dict) -> Result<Self, CheckError> {
        let invalid = || CheckError::value("invalid CloudDrive file metadata");
        let empty = Value::Dict(Dict::new());
        let path = value.get("path").and_then(Value::as_str);
        let file_id = value.get("id").and_then(Value::as_str);
        let name = value.get("name").and_then(Value::as_str);
        let size = non_negative_int(value.get("size"));
        let write_time = non_negative_int(value.get("write_time"));
        let hashes = match value.get("hashes").unwrap_or(&empty) {
            Value::Dict(hashes) => Some(hashes),
            _ => None,
        };
        let (Some(path), Some(file_id), Some(name), Some(size), Some(write_time), Some(hashes_value)) =
            (path, file_id, name, size, write_time, hashes)
        else {
            return Err(invalid());
        };
        let normalized = absolute_cloud_path(path, "CloudDrive metadata path")?;
        if normalized.name() != name || file_id.is_empty() {
            return Err(CheckError::value("invalid CloudDrive file identity"));
        }
        let mut hashes = Vec::with_capacity(hashes_value.len());
        for (key, item) in hashes_value.iter() {
            let (Some(key), Value::Str(item)) = (key.as_str(), item) else {
                return Err(CheckError::Type("invalid CloudDrive file hashes".to_owned()));
            };
            hashes.push((key.to_owned(), item.clone()));
        }
        hashes.sort();
        Ok(Self {
            path: normalized.text,
            file_id: file_id.to_owned(),
            name: name.to_owned(),
            size,
            write_time,
            hashes,
        })
    }

    /// `matches_identity(other)`: the same remote file, unchanged (the path may differ).
    pub fn matches_identity(&self, other: &Self) -> bool {
        self.file_id == other.file_id
            && self.name == other.name
            && self.size == other.size
            && self.write_time == other.write_time
            && self.hashes == other.hashes
    }

    /// The mapping `from_mapping` reads, as `_cloud_file_to_json` builds it.
    pub fn to_mapping(&self) -> Dict {
        let mut dict = Dict::new();
        dict.insert("path", Value::Str(self.path.clone()));
        dict.insert("id", Value::Str(self.file_id.clone()));
        dict.insert("name", Value::Str(self.name.clone()));
        dict.insert("size", Value::Int(self.size));
        dict.insert("write_time", Value::Int(self.write_time));
        let mut hashes = Dict::new();
        for (kind, value) in &self.hashes {
            hashes.insert(kind.as_str(), Value::Str(value.clone()));
        }
        dict.insert("hashes", Value::Dict(hashes));
        dict
    }
}

/// `CloudMoveResponse`: CloudDrive's answer to a move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudMoveResponse {
    pub success: bool,
    pub error_message: Option<String>,
    pub result_paths: Vec<String>,
}

impl CloudMoveResponse {
    /// `CloudMoveResponse.from_mapping(value)`: an empty error message becomes `None`.
    pub fn from_mapping(value: &Dict) -> Result<Self, CheckError> {
        let invalid = || CheckError::value("invalid CloudDrive move response");
        let Some(Value::Bool(success)) = value.get("success") else {
            return Err(invalid());
        };
        let error_message = match value.get("error_message") {
            None | Some(Value::Null) => None,
            Some(Value::Str(text)) => Some(text.clone()),
            Some(_) => return Err(invalid()),
        };
        let result_paths = match value.get("result_paths") {
            None => Vec::new(),
            Some(Value::List(items)) => items
                .iter()
                .map(|item| item.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(invalid)?,
            Some(_) => return Err(invalid()),
        };
        Ok(Self {
            success: *success,
            error_message: error_message.filter(|text| !text.is_empty()),
            result_paths,
        })
    }
}

/// Why a mover call failed: what the service's `except Exception` sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoverError {
    /// `CloudDriveUnconfiguredError` (a `RuntimeError`): CloudDrive has no configuration.
    Unconfigured,
    /// The CloudDrive client's own error (`grpc.RpcError`, `FileNotFoundError`, ...).
    CloudDrive(CloudDriveError),
    /// A listing or answer of the wrong shape (`TypeError` / `ValueError`).
    Check(CheckError),
}

/// `CloudDriveUnconfiguredError`'s text.
pub const CLOUDDRIVE_UNCONFIGURED: &str = "CloudDrive connection is not configured";

impl MoverError {
    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Unconfigured => "CloudDriveUnconfiguredError",
            Self::CloudDrive(error) => error.class_name(),
            Self::Check(error) => error.class_name(),
        }
    }
}

impl fmt::Display for MoverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unconfigured => f.write_str(CLOUDDRIVE_UNCONFIGURED),
            Self::CloudDrive(error) => error.fmt(f),
            Self::Check(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for MoverError {}

impl From<CloudDriveError> for MoverError {
    fn from(error: CloudDriveError) -> Self {
        Self::CloudDrive(error)
    }
}

impl From<CheckError> for MoverError {
    fn from(error: CheckError) -> Self {
        Self::Check(error)
    }
}

/// `CloudFileMover`: the CloudDrive calls the cloud moves make.
pub trait CloudFileMover: Send + Sync {
    /// Fresh metadata of the files (not directories) in one directory.
    fn list_directory<'a>(
        &'a self,
        api_directory: &'a str,
    ) -> BoxFuture<'a, Result<Vec<CloudFileMetadata>, MoverError>>;

    /// Make sure `folder_name` exists under the parent; whether it is a directory now.
    fn ensure_directory<'a>(
        &'a self,
        parent_api_directory: &'a str,
        folder_name: &'a str,
    ) -> BoxFuture<'a, Result<bool, MoverError>>;

    /// Move one file into a directory without overwriting.
    fn move_file<'a>(
        &'a self,
        source_api_path: &'a str,
        destination_api_directory: &'a str,
    ) -> BoxFuture<'a, Result<CloudMoveResponse, MoverError>>;

    /// A mover on the same CloudDrive whose calls [`Self::settle`] waits for: what code that
    /// cancels work around a mutation and then acts on the outcome runs that work on
    /// (`AsyncCloudDrive::scoped`, D41).
    fn scoped(&self) -> std::sync::Arc<dyn CloudFileMover>;

    /// Wait until no call started through this scoped mover is running, also those whose
    /// caller was dropped. Returns at once on a mover that is not scoped.
    fn settle(&self) -> BoxFuture<'_, ()>;
}

/// `ParsedStrmTarget`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedStrmTarget {
    /// SHA-256 of the mapping file's bytes, in hex.
    pub mapping_sha256: String,
    pub mounted_path: String,
    pub api_path: String,
}

/// A path that passed `_absolute_cloud_path`: `str(PurePosixPath(value))`, with its anchor
/// (`/` or `//`, which pathlib keeps apart) and parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudPath {
    text: String,
}

impl CloudPath {
    pub fn as_str(&self) -> &str {
        &self.text
    }

    fn anchor(&self) -> &str {
        if self.text.starts_with("//") { "//" } else { "/" }
    }

    fn parts(&self) -> Vec<&str> {
        self.text.split('/').filter(|part| !part.is_empty()).collect()
    }

    /// `.name`.
    fn name(&self) -> &str {
        self.parts().last().copied().unwrap_or("")
    }

    /// `self.relative_to(other)`'s parts, or `None` where pathlib raises `ValueError`.
    fn relative_to(&self, other: &Self) -> Option<Vec<&str>> {
        if self.anchor() != other.anchor() {
            return None;
        }
        let (mine, theirs) = (self.parts(), other.parts());
        mine.starts_with(&theirs).then(|| mine[theirs.len()..].to_vec())
    }
}

impl fmt::Display for CloudPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

/// `_absolute_cloud_path(value, name=...)`.
pub fn absolute_cloud_path(value: &str, name: &str) -> Result<CloudPath, CheckError> {
    if value.is_empty() || value.contains('\0') || value.contains("://") || value.contains('\\') {
        return Err(CheckError::value(format!("{name} must be an absolute POSIX path")));
    }
    let normalized = embyx_core::pypath::PyPath::new(value).as_str().to_owned();
    let absolute = normalized.starts_with('/');
    let dotted = normalized.split('/').any(|part| part == ".." || part == ".");
    if !absolute || dotted || normalized != value.trim_end_matches('/') {
        return Err(CheckError::value(format!(
            "{name} must be a normalized absolute POSIX path"
        )));
    }
    Ok(CloudPath { text: normalized })
}

/// `CloudMovePaths`: where `.strm` targets are mounted, which CloudDrive roots each source
/// maps to, and where moved files go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudMovePaths {
    pub strm_mount_prefix: CloudPath,
    pub source_api_roots: Vec<CloudPath>,
    pub move_in_api_root: CloudPath,
}

/// Python `str.splitlines()`: lines end at `\n`, `\r`, `\r\n`, `\v`, `\f`, `\x1c`-`\x1e`,
/// `\x85`, U+2028 and U+2029. (A copy of `embyx_merge::py::splitlines`; PLAN.md, "Cleanup
/// notes".)
fn splitlines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((at, ch)) = chars.next() {
        let ends = matches!(
            ch,
            '\n' | '\r' | '\u{0b}' | '\u{0c}' | '\u{1c}' | '\u{1d}' | '\u{1e}' | '\u{85}' | '\u{2028}' | '\u{2029}'
        );
        if !ends {
            continue;
        }
        lines.push(&text[start..at]);
        start = at + ch.len_utf8();
        if ch == '\r' && chars.peek().is_some_and(|&(_, next)| next == '\n') {
            chars.next();
            start += 1;
        }
    }
    if start < text.len() {
        lines.push(&text[start..]);
    }
    lines
}

impl CloudMovePaths {
    /// `CloudMovePaths.from_values(...)`.
    pub fn from_values(
        strm_mount_prefix: &str,
        source_api_roots: &[String],
        move_in_api_root: &str,
    ) -> Result<Self, CheckError> {
        if source_api_roots.is_empty() {
            return Err(CheckError::value("CloudDrive source roots must not be empty"));
        }
        let strm_mount_prefix = absolute_cloud_path(strm_mount_prefix, "STRM mount prefix")?;
        let source_api_roots = source_api_roots
            .iter()
            .map(|root| absolute_cloud_path(root, "CloudDrive source root"))
            .collect::<Result<Vec<_>, _>>()?;
        let move_in_api_root = absolute_cloud_path(move_in_api_root, "CloudDrive move-in root")?;
        Ok(Self {
            strm_mount_prefix,
            source_api_roots,
            move_in_api_root,
        })
    }

    /// `parse_mapping(mapping_path, source_index=...)`: the CloudDrive file one `.strm`
    /// file points at, which must lie under the source's own root. A negative index counts
    /// from the end, as a Python index does.
    pub fn parse_mapping(&self, mapping_path: &Path, source_index: isize) -> Result<ParsedStrmTarget, CheckError> {
        let index = if source_index < 0 {
            source_index.checked_add_unsigned(self.source_api_roots.len())
        } else {
            Some(source_index)
        };
        let expected_root = index
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| self.source_api_roots.get(index))
            .ok_or_else(|| CheckError::strm("CloudDrive source root mapping is incomplete"))?;
        let payload = read_mapping(mapping_path).map_err(|_| CheckError::strm("STRM mapping is unavailable"))?;
        if payload.is_empty() || payload.len() > MAX_STRM_BYTES || payload.contains(&0) {
            return Err(CheckError::strm("STRM mapping size is invalid"));
        }
        let value = std::str::from_utf8(&payload).map_err(|_| CheckError::strm("STRM mapping must be UTF-8"))?;
        let lines = splitlines(value);
        if lines.len() != 1 || lines[0].is_empty() || lines[0] != py_strip(lines[0]) {
            return Err(CheckError::strm("STRM mapping must contain one absolute path"));
        }
        let mounted = absolute_cloud_path(lines[0], "STRM target")?;
        let suffix = casefold(embyx_core::pypath::suffix(mounted.as_str()));
        if !VIDEO_SUFFIXES.contains(&suffix.as_str()) {
            return Err(CheckError::strm("STRM target is not a supported video file"));
        }
        let relative = mounted
            .relative_to(&self.strm_mount_prefix)
            .ok_or_else(|| CheckError::strm("STRM target is outside the configured mount prefix"))?;
        let api_path = CloudPath {
            text: format!("/{}", relative.join("/")),
        };
        if api_path.relative_to(expected_root).is_none() {
            return Err(CheckError::strm(
                "STRM target is outside the configured CloudDrive source root",
            ));
        }
        Ok(ParsedStrmTarget {
            mapping_sha256: hex::encode(Sha256::digest(&payload)),
            mounted_path: mounted.text,
            api_path: api_path.text,
        })
    }

    /// `destination_directory(brand)`: the move-in directory of one brand.
    pub fn destination_directory(&self, brand: &str) -> Result<String, CheckError> {
        if brand.is_empty() || brand == "." || brand == ".." || brand.contains(['/', '\\', '\0']) {
            return Err(CheckError::value("CloudDrive destination brand is invalid"));
        }
        Ok(format!("{}/{brand}", self.move_in_api_root.text))
    }
}

/// `mapping_path.read_bytes()`, read no further than one byte past the size limit (a longer
/// file is refused for its size either way).
fn read_mapping(path: &Path) -> std::io::Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    let mut payload = Vec::new();
    file.take(MAX_STRM_BYTES as u64 + 1).read_to_end(&mut payload)?;
    Ok(payload)
}
