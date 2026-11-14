//! The `.strm` mapping pipeline: mirror flat `xx/yy/zz.strm` into `xx/yy/zz/zz.strm`.
//!
//! Python origin: `src/embyx_manager/monitor/mapping.py`. Remote storage keeps a flat
//! structure to reduce CloudDrive access; the local mirror adds one directory per title so
//! Emby classifies entries correctly. The mirror is derived output: a full sync copies what
//! changed, deletes `.strm` files whose source is gone (with the metadata Emby wrote beside
//! them) and removes directories left without a `.strm`.
//!
//! Every file-system call goes through [`Fs`] (the 3.13 `pathlib` / `shutil` / `filecmp`
//! algorithms, [`crate::fs`]), so scenarios record the mutations and inject failures. File
//! names are handled as bytes: which sidecar belongs to which `.strm`, and which `.strm` is
//! live, is never decided through a lossy decode (DIVERGENCES.md D91). Log lines show paths
//! with `str()` (lossy for a name that is not UTF-8, D72).

use std::cmp::Ordering;
use std::collections::HashSet;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use embyx_core::avid::AvidParser;
use embyx_core::config::MappingConfig;
use embyx_core::pypath::PyPath;
use embyx_core::pytext::is_py_decimal;

use crate::fs::{self, Fs, LocalFs, PyOsError, path_text};
use crate::reports::{PyException, RunCancelledError, RunContext};

/// `errno.ENOTEMPTY` on Linux.
pub const ENOTEMPTY: i32 = 39;

/// Why a mapping call stopped.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MappingError {
    #[error(transparent)]
    Cancelled(#[from] RunCancelledError),
    /// `ValueError(message)`: a root that is not a directory.
    #[error("{0}")]
    Value(String),
    /// An `OSError` (`FileExistsError` for a destination that is not a file).
    #[error(transparent)]
    Os(#[from] PyOsError),
}

impl PyException for MappingError {
    fn class_name(&self) -> &str {
        match self {
            Self::Cancelled(error) => error.class_name(),
            Self::Value(_) => "ValueError",
            Self::Os(error) => error.class_name(),
        }
    }

    fn py_repr(&self) -> String {
        match self {
            Self::Cancelled(error) => error.py_repr(),
            Self::Value(message) => format!("ValueError({})", embyx_core::pytext::py_repr(message)),
            Self::Os(error) => error.py_repr(),
        }
    }
}

/// `MappingPipeline`.
#[derive(Debug, Clone)]
pub struct MappingPipeline {
    config: MappingConfig,
    avid_parser: AvidParser,
    /// `Path(config.src_dir)`.
    pub src_dir: PathBuf,
    /// `Path(config.dst_dir)`.
    pub dst_dir: PathBuf,
    fs: Arc<dyn Fs>,
}

impl MappingPipeline {
    /// `MappingPipeline(config=, avid_parser=)` on the real file system.
    pub fn new(config: MappingConfig, avid_parser: AvidParser) -> Self {
        Self::with_fs(config, avid_parser, Arc::new(LocalFs))
    }

    pub fn with_fs(config: MappingConfig, avid_parser: AvidParser, fs: Arc<dyn Fs>) -> Self {
        Self {
            src_dir: PyPath::new(&config.src_dir).as_path().to_path_buf(),
            dst_dir: PyPath::new(&config.dst_dir).as_path().to_path_buf(),
            config,
            avid_parser,
            fs,
        }
    }

    pub fn config(&self) -> &MappingConfig {
        &self.config
    }

    pub fn fs(&self) -> &Arc<dyn Fs> {
        &self.fs
    }

    // -- full sync --------------------------------------------------------

    /// `run_full(ctx)`: update everything, delete strays, remove empty directories.
    pub fn run_full(&self, ctx: &RunContext) -> Result<(), MappingError> {
        let fs = &*self.fs;
        if !fs::is_dir(fs, &self.src_dir)? {
            return Err(MappingError::Value(format!(
                "{} is not a directory",
                path_text(&self.src_dir)
            )));
        }
        if !fs::exists(fs, &self.dst_dir)? {
            fs.mkdir_parents_new(&self.dst_dir)?;
        } else if !fs::is_dir(fs, &self.dst_dir)? {
            return Err(MappingError::Value(format!(
                "{} is not a directory",
                path_text(&self.dst_dir)
            )));
        }
        ctx.info(&format!(
            "starting mapping from {} to {}",
            path_text(&self.src_dir),
            path_text(&self.dst_dir)
        ));
        self.update_all(ctx)?;
        self.delete_strays(ctx)?;
        self.delete_empty_dirs(ctx)
    }

    // -- incremental ------------------------------------------------------

    /// `run_incremental(ctx, changed=, deleted=)`: one debounced batch of watcher events,
    /// deletions first, each in `sorted()` order; the paths that failed come back (each once,
    /// in that order). One path failing is logged and the batch goes on; only a stop request
    /// ends it early.
    pub fn run_incremental(
        &self,
        ctx: &RunContext,
        changed: &[PathBuf],
        deleted: &[PathBuf],
    ) -> Result<(Vec<PathBuf>, Vec<PathBuf>), RunCancelledError> {
        let mut failed_changed = Vec::new();
        let mut failed_deleted = Vec::new();
        for path in py_sorted_set(deleted) {
            ctx.check_cancelled()?;
            if let Err(error) = self.delete_one(&path, ctx) {
                ctx.exception(
                    &format!("Incremental delete failed for {}", path_text(&path)),
                    Some(&error),
                );
                failed_deleted.push(path);
            }
        }
        for path in py_sorted_set(changed) {
            ctx.check_cancelled()?;
            if let Err(error) = self.update_one(&path, ctx) {
                ctx.exception(
                    &format!("Incremental update failed for {}", path_text(&path)),
                    Some(&error),
                );
                failed_changed.push(path);
            }
        }
        Ok((failed_changed, failed_deleted))
    }

    // -- single-path operations -------------------------------------------

    /// `map_strm_path(src)`: where a source `.strm` goes in the mirror, `None` when it is not
    /// a `.strm`, not under the source root, or its name holds no AVID.
    pub fn map_strm_path(&self, src: &Path) -> Option<PathBuf> {
        if !is_strm(src) {
            return None;
        }
        let relative = relative_or_absolute(src, &self.src_dir)?;
        let name = src.file_name()?;
        let avid = self.avid_parser.get_avid(&name.to_string_lossy());
        if avid.is_empty() {
            return None;
        }
        Some(py_join(&py_join(&self.dst_dir, &py_parent(&relative)), Path::new(&avid)).join(name))
    }

    /// `update_one(src, ctx)`: copy one source `.strm` into its title directory unless the
    /// copy there is current.
    pub fn update_one(&self, src: &Path, ctx: &RunContext) -> Result<(), MappingError> {
        let fs = &*self.fs;
        let Some(dst) = self.map_strm_path(src) else {
            if is_strm(src) {
                ctx.warning(&format!("failed to get avid for {}, skipping", path_text(src)));
            }
            return Ok(());
        };
        if !fs::exists(fs, src)? {
            ctx.warning(&format!("source file missing, skipping {}", path_text(src)));
            return Ok(());
        }
        fs.mkdir_parents(&py_parent(&dst))?;
        if fs::exists(fs, &dst)? && !fs::is_file(fs, &dst)? {
            return Err(PyOsError::classed(
                "FileExistsError",
                format!("{} exists and is not a file", path_text(&dst)),
            )
            .into());
        }
        if fs::exists(fs, &dst)?
            && fs.stat(src, true)?.mtime <= fs.stat(&dst, true)?.mtime
            && fs.cmp_files(src, &dst)?
        {
            ctx.add("files_skipped", 1);
            return Ok(());
        }
        fs.copy2(src, &dst)?;
        ctx.add("files_updated", 1);
        ctx.info(&format!(
            "updated {} -> {}",
            path_text(&display(src, &self.src_dir)),
            path_text(&display(&dst, &self.dst_dir))
        ));
        Ok(())
    }

    /// `delete_one(src, ctx)`: remove the mirror of a source `.strm`, its own metadata, and
    /// the directories that leaves empty.
    pub fn delete_one(&self, src: &Path, ctx: &RunContext) -> Result<(), MappingError> {
        let fs = &*self.fs;
        let Some(dst) = self.map_strm_path(src) else {
            return Ok(());
        };
        if !fs::exists(fs, &dst)? {
            return Ok(());
        }
        fs.unlink(&dst)?;
        ctx.add("files_deleted", 1);
        ctx.info(&format!("deleted {}", path_text(&display(&dst, &self.dst_dir))));
        let parent = py_parent(&dst);
        self.delete_sidecars(&parent, &[stem(&dst).to_owned()], ctx, false)?;
        self.delete_empty_dirs_for_path(&parent, ctx)
    }

    // -- internals ----------------------------------------------------------

    fn update_all(&self, ctx: &RunContext) -> Result<(), MappingError> {
        for src in self.fs.glob_recursive(&self.src_dir, &strm_glob) {
            ctx.check_cancelled()?;
            self.update_one(&src, ctx)?;
        }
        Ok(())
    }

    /// `_delete_strays`: delete `xx/yy/zz/zz.strm` in the mirror whose `xx/yy/zz.strm` no
    /// longer exists, with its metadata.
    ///
    /// A `.strm` left in a title directory the ID no longer maps to (the ID rules changed,
    /// e.g. ABC-0123 became ABC-123) goes too once its copy exists in the directory it maps to
    /// now; otherwise Emby lists the title twice. Every title directory also loses the
    /// metadata of parts whose `.strm` is gone, which an earlier run may have left behind.
    fn delete_strays(&self, ctx: &RunContext) -> Result<(), MappingError> {
        let fs = &*self.fs;
        // `dict[Path, set[str]]` in insertion order.
        let mut deleted: Vec<(PathBuf, Vec<OsString>)> = Vec::new();
        for dst in fs.glob_recursive(&self.dst_dir, &strm_glob) {
            ctx.check_cancelled()?;
            let relative = relative_to(&dst, &self.dst_dir).unwrap_or_else(|| dst.clone());
            let src_rel_dir = py_parent(&py_parent(&relative));
            let src = py_join(&self.src_dir, &src_rel_dir).join(dst.file_name().unwrap_or_default());
            let parent = py_parent(&dst);
            let index = match deleted.iter().position(|(directory, _)| *directory == parent) {
                Some(index) => index,
                None => {
                    deleted.push((parent, Vec::new()));
                    deleted.len() - 1
                }
            };
            if !fs::exists(fs, &src)? || self.moved(&src, &dst)? {
                fs.unlink(&dst)?;
                let stems = &mut deleted[index].1;
                let gone = stem(&dst).to_owned();
                if !stems.contains(&gone) {
                    stems.push(gone);
                }
                ctx.add("files_deleted", 1);
                ctx.info(&format!("deleted {}", path_text(&relative)));
            }
        }
        for (directory, stems) in deleted {
            ctx.check_cancelled()?;
            self.delete_sidecars(&directory, &stems, ctx, true)?;
        }
        Ok(())
    }

    /// `_moved(src, dst)`: whether `src` now maps to another directory and its `.strm` is
    /// already there.
    fn moved(&self, src: &Path, dst: &Path) -> Result<bool, PyOsError> {
        match self.map_strm_path(src) {
            Some(mapped) if mapped != dst => fs::is_file(&*self.fs, &mapped),
            _ => Ok(false),
        }
    }

    /// `_delete_sidecars(directory, stems, ctx, orphaned_parts=)`: delete the metadata Emby
    /// wrote beside `.strm` files that are gone.
    ///
    /// A file belongs to the `.strm` whose name, plus `.` or `-`, starts it; the longest such
    /// name wins, so `ABC-123-cd1-poster.jpg` stays with a live `ABC-123-cd1.strm` even when
    /// `ABC-123.strm` goes. Files no `.strm` claims (`poster.jpg`, `fanart.jpg`) stay. With
    /// `orphaned_parts`, part metadata whose `-cdN` `.strm` no longer exists goes too.
    fn delete_sidecars(
        &self,
        directory: &Path,
        stems: &[OsString],
        ctx: &RunContext,
        orphaned_parts: bool,
    ) -> Result<(), MappingError> {
        let fs = &*self.fs;
        let entries = match fs.iterdir(directory) {
            Ok(entries) => entries,
            Err(error) if error.errno == Some(fs::ENOENT) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let mut live: Vec<OsString> = Vec::new();
        for entry in &entries {
            let entry_stem = stem(entry).to_owned();
            if is_strm(entry) && !live.contains(&entry_stem) {
                live.push(entry_stem);
            }
        }
        let gone: Vec<OsString> = stems.iter().filter(|stem| !live.contains(stem)).cloned().collect();
        let candidates: Vec<&OsString> = live.iter().chain(&gone).collect();
        for entry in &entries {
            if is_strm(entry) || !fs::is_file(fs, entry)? {
                continue;
            }
            let name = entry.file_name().unwrap_or_default().as_bytes();
            let mut owner = owner(name, &candidates);
            let part = if orphaned_parts && !live.is_empty() {
                part_stem(name)
            } else {
                None
            };
            if let Some(part) = part
                && !live.iter().any(|stem| stem.as_bytes() == part)
                && part.len() > owner.map_or(0, <[u8]>::len)
            {
                owner = Some(part);
            }
            let Some(owner) = owner else {
                continue;
            };
            if live.iter().any(|stem| stem.as_bytes() == owner) {
                continue;
            }
            fs.unlink(entry)?;
            ctx.add("sidecars_deleted", 1);
            ctx.info(&format!("deleted {}", path_text(&display(entry, &self.dst_dir))));
        }
        Ok(())
    }

    /// `_delete_empty_dirs`: directories without a descendant `.strm` are removed as derived
    /// mapping output, even if non-`.strm` leftovers are still present.
    fn delete_empty_dirs(&self, ctx: &RunContext) -> Result<(), MappingError> {
        let fs = &*self.fs;
        let mut empty_dirs = Vec::new();
        for path in fs.glob_recursive(&self.dst_dir, &|_| true) {
            if fs::is_dir(fs, &path)? && fs::glob_recursive(fs, &path, &strm_glob, 1).is_empty() {
                empty_dirs.push(path);
            }
        }
        if empty_dirs.is_empty() {
            return Ok(());
        }
        empty_dirs.sort_by(|a, b| py_path_cmp(b, a));
        let mut kept: HashSet<PathBuf> = HashSet::new();
        for empty_dir in empty_dirs {
            ctx.check_cancelled()?;
            if kept.contains(&empty_dir) || !fs::exists(fs, &empty_dir)? {
                continue;
            }
            let relative = path_text(&relative_to(&empty_dir, &self.dst_dir).unwrap_or_else(|| empty_dir.clone()));
            if let Err(error) = fs.rmtree(&empty_dir) {
                // One undeletable directory must not abort the whole run: the remaining empty
                // directories still need cleaning. ENOTEMPTY in particular means the file
                // system disagrees with the listing just seen (a stale NFS directory cache
                // does exactly this), so the directory is not derived-output garbage and stays
                // put, and so do its ancestors: they are not empty either.
                kept.extend(py_parents(&empty_dir));
                ctx.add("dirs_skipped", 1);
                if error.errno == Some(ENOTEMPTY) {
                    ctx.warning(&format!("skipped directory that is not empty on disk: {relative}"));
                } else {
                    ctx.exception(&format!("failed to delete empty directory {relative}"), Some(&error));
                }
                continue;
            }
            ctx.add("dirs_deleted", 1);
            ctx.info(&format!("deleted empty directory: {relative}"));
        }
        Ok(())
    }

    /// `_delete_empty_dirs_for_path(path, ctx)`: walk up from `path`, removing each empty
    /// directory below the mirror's root, and stop at the first that is not.
    fn delete_empty_dirs_for_path(&self, path: &Path, ctx: &RunContext) -> Result<(), MappingError> {
        let fs = &*self.fs;
        if !fs::exists(fs, &self.dst_dir)? {
            return Ok(());
        }
        let mut current = path.to_path_buf();
        while current != self.dst_dir && py_parents(&current).contains(&self.dst_dir) {
            match fs.iterdir(&current) {
                Ok(entries) if !entries.is_empty() => break,
                Ok(_) => {}
                Err(error) if error.errno == Some(fs::ENOENT) => {
                    current = py_parent(&current);
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
            let relative = path_text(&relative_to(&current, &self.dst_dir).unwrap_or_else(|| current.clone()));
            match fs.rmdir(&current) {
                Ok(()) => {}
                // The listing said empty but the file system says otherwise (stale NFS
                // directory cache); treat it as the non-empty stop.
                Err(error) if error.errno == Some(ENOTEMPTY) => {
                    ctx.warning(&format!("skipped directory that is not empty on disk: {relative}"));
                    break;
                }
                Err(error) => return Err(error.into()),
            }
            ctx.add("dirs_deleted", 1);
            ctx.info(&format!("deleted empty directory: {relative}"));
            current = py_parent(&current);
        }
        Ok(())
    }
}

/// The name filter of `glob('**/*.strm')`: `*.strm`, case-sensitive.
fn strm_glob(name: &OsStr) -> bool {
    name.as_bytes().ends_with(b".strm")
}

/// `path.suffix.lower() == '.strm'`. No character outside ASCII lowers to one of `.strm`'s
/// letters, so an ASCII case-insensitive comparison of the bytes is exact.
fn is_strm(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| suffix(name.as_bytes()).eq_ignore_ascii_case(b".strm"))
}

/// `PurePath.suffix` of a name: from its last dot, unless the dot starts or ends it.
fn suffix(name: &[u8]) -> &[u8] {
    match name.iter().rposition(|byte| *byte == b'.') {
        Some(dot) if dot > 0 && dot < name.len() - 1 => &name[dot..],
        _ => &[],
    }
}

/// `PurePath.stem`.
fn stem(path: &Path) -> &OsStr {
    let name = path.file_name().unwrap_or_default().as_bytes();
    OsStr::from_bytes(&name[..name.len() - suffix(name).len()])
}

/// `_owner(name, stems)`: the longest stem that, followed by `.` or `-`, starts the name.
/// Every candidate is a prefix of the one name, so comparing byte lengths orders them as
/// Python's character lengths do.
fn owner<'a>(name: &[u8], stems: &[&'a OsString]) -> Option<&'a [u8]> {
    let mut best: Option<&'a [u8]> = None;
    for stem in stems {
        let stem = stem.as_bytes();
        let claims = name.len() > stem.len() && name.starts_with(stem) && matches!(name[stem.len()], b'.' | b'-');
        if claims && best.is_none_or(|best| stem.len() > best.len()) {
            best = Some(stem);
        }
    }
    best
}

/// One character of a name as Python's file-system decoding reads it: a UTF-8 character,
/// or an undecodable byte (a lone surrogate under `surrogateescape`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    Char(char),
    Byte(u8),
}

impl Unit {
    /// The code point Python's `str` holds for it.
    fn code_point(self) -> u32 {
        match self {
            Self::Char(ch) => u32::from(ch),
            Self::Byte(byte) => 0xDC00 + u32::from(byte),
        }
    }
}

/// The characters of a name with the byte offset each one ends at.
fn units(bytes: &[u8]) -> Vec<(Unit, usize)> {
    let mut units = Vec::new();
    let mut offset = 0;
    for chunk in bytes.utf8_chunks() {
        for ch in chunk.valid().chars() {
            offset += ch.len_utf8();
            units.push((Unit::Char(ch), offset));
        }
        for byte in chunk.invalid() {
            offset += 1;
            units.push((Unit::Byte(*byte), offset));
        }
    }
    units
}

/// `PART_SIDECAR_RE.match(name)['stem']`: `^(?P<stem>.+-cd\d+)[.-]` with `IGNORECASE`, the
/// bytes of the stem. `.+` is greedy and stops at a newline, so the stem ends at the last
/// `-cd<digits>` followed by `.` or `-` whose prefix holds no newline; `\d` is any Unicode
/// decimal digit.
fn part_stem(name: &[u8]) -> Option<&[u8]> {
    let units = units(name);
    let first_newline = units
        .iter()
        .position(|(unit, _)| *unit == Unit::Char('\n'))
        .unwrap_or(units.len());
    let char_at = |index: usize| match units.get(index) {
        Some((Unit::Char(ch), _)) => Some(*ch),
        _ => None,
    };
    // `.+` takes units 0..start (at least one, no newline); the rest must start at `start`.
    for start in (1..=first_newline).rev() {
        if char_at(start) != Some('-')
            || !matches!(char_at(start + 1), Some('c' | 'C'))
            || !matches!(char_at(start + 2), Some('d' | 'D'))
        {
            continue;
        }
        let digits = units[start + 3..]
            .iter()
            .take_while(|(unit, _)| matches!(unit, Unit::Char(ch) if is_py_decimal(*ch)))
            .count();
        if digits == 0 || !matches!(char_at(start + 3 + digits), Some('.' | '-')) {
            continue;
        }
        return Some(&name[..units[start + 2 + digits].1]);
    }
    None
}

/// The parts of `str(path).split('/')` as Python compares them: code points, an undecodable
/// byte as its surrogate.
fn py_parts(path: &Path) -> Vec<Vec<u32>> {
    path.as_os_str()
        .as_bytes()
        .split(|byte| *byte == b'/')
        .map(|part| units(part).into_iter().map(|(unit, _)| unit.code_point()).collect())
        .collect()
}

/// `PurePath` ordering (`_parts_normcase` on POSIX).
fn py_path_cmp(a: &Path, b: &Path) -> Ordering {
    py_parts(a).cmp(&py_parts(b))
}

/// `sorted(set(paths))`.
fn py_sorted_set(paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut sorted: Vec<PathBuf> = paths.iter().map(|path| normalize(path)).collect();
    sorted.sort_by(|a, b| py_path_cmp(a, b));
    sorted.dedup();
    sorted
}

/// `Path(path)`: `.` parts and repeated or trailing slashes dropped.
fn normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            other => normalized.push(other),
        }
    }
    if normalized.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        normalized
    }
}

/// `path.parent`: `.` for a single relative part, the root for the root.
fn py_parent(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        Some(_) => PathBuf::from("."),
        None => path.to_path_buf(),
    }
}

/// `path.parents`: every lexical ancestor, nearest first.
fn py_parents(path: &Path) -> Vec<PathBuf> {
    let mut parents = Vec::new();
    let mut current = path.to_path_buf();
    loop {
        let parent = py_parent(&current);
        if parent == current || (current == Path::new(".")) {
            return parents;
        }
        parents.push(parent.clone());
        current = parent;
    }
}

/// `base / part`, where a `.` part adds nothing (`Path('a') / '.'` is `a`).
fn py_join(base: &Path, part: &Path) -> PathBuf {
    if part == Path::new(".") {
        return base.to_path_buf();
    }
    if base == Path::new(".") && part.is_relative() {
        return part.to_path_buf();
    }
    base.join(part)
}

/// `path.relative_to(base)` (`walk_up=False`): `None` where Python raises `ValueError`.
fn relative_to(path: &Path, base: &Path) -> Option<PathBuf> {
    if base == Path::new(".") {
        return path.is_relative().then(|| normalize(path));
    }
    let relative = path.strip_prefix(base).ok()?;
    Some(if relative.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        relative.to_path_buf()
    })
}

/// `_relative_to(src, src_dir)`: as given, else both made absolute against the working
/// directory.
fn relative_or_absolute(src: &Path, src_dir: &Path) -> Option<PathBuf> {
    relative_to(src, src_dir).or_else(|| {
        let cwd = std::env::current_dir().ok()?;
        let absolute = |path: &Path| {
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                py_join(&cwd, path)
            }
        };
        relative_to(&absolute(src), &absolute(src_dir))
    })
}

/// `_display(path, base)`: relative to the base when it is under it.
fn display(path: &Path, base: &Path) -> PathBuf {
    relative_to(path, base).unwrap_or_else(|| path.to_path_buf())
}

#[cfg(test)]
mod scenarios;
#[cfg(test)]
mod tests;
