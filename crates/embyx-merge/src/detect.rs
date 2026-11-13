//! Finding multi-part titles in the library and where each one came from.
//!
//! Python origin: `src/embyx_manager/merge/detect.py`.
//!
//! The mapping pipeline's local tree holds one directory per title, and a title archived in
//! parts holds one `{AVID}-cdN.strm` per part. Each strm names the part's file on the
//! CloudDrive mount, so the scan never touches 115 itself. Emby stacks parts cd1 through cd9
//! into one item and shows every later part as an item of its own; both kinds can be merged.
//!
//! A title whose directory also holds a whole copy (`{AVID}.strm` next to the parts) is not
//! mergeable: the archive refuses a title the library already holds. Merging re-enters a title
//! through the archive flow, so it needs the intake route whose destination holds the title
//! today: the library directory names it whenever exactly one route feeds that directory,
//! otherwise the acquisition ledger's offline directory is asked.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use embyx_core::avid::normalize_number;
use embyx_core::config::models::ArchiveConfig;
use embyx_core::media::VIDEO_SUFFIXES;
use embyx_core::{pypath, pyre, pytext};

use crate::py;

/// `CD_STRM_RE`: `ABC-123-cd4.strm`; the stem before `-cd` is the title, the number its part.
static CD_STRM_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| pyre::compile(r"^(?P<stem>.+)-cd(?P<index>\d+)\.strm$", true, false));
/// `CD_SUFFIX_RE`: a part's `-cdN` at the end of a file stem.
static CD_SUFFIX_RE: LazyLock<regex::Regex> = LazyLock::new(|| pyre::compile(r"-cd\d+$", true, false));
/// Emby stacks a single digit only; `cd10` onwards each become their own item.
pub const MAX_STACKED_PARTS: usize = 9;
/// A strm is one short line; anything larger is not one.
pub const MAX_STRM_BYTES: usize = 4096;

/// `SourceBasis`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceBasis {
    /// Exactly one route feeds the library directory the parts live in.
    Library,
    /// The acquisition ledger remembers which offline directory fetched it.
    Ledger,
}

impl SourceBasis {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Library => "library",
            Self::Ledger => "ledger",
        }
    }
}

/// `TitleProblem`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TitleProblem {
    /// A strm could not be read or does not hold one absolute path.
    UnreadableStrm,
    /// A part's file is not under the archive library root.
    OutsideLibrary,
    /// The parts are spread over more than one brand directory.
    ScatteredParts,
    /// The library already holds the whole title next to its parts.
    WholeCopy,
}

impl TitleProblem {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnreadableStrm => "unreadable_strm",
            Self::OutsideLibrary => "outside_library",
            Self::ScatteredParts => "scattered_parts",
            Self::WholeCopy => "whole_copy",
        }
    }
}

/// `Part`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    pub index: i128,
    /// The strm, relative to the mapping tree's root.
    pub strm: String,
    /// The part's file on the CloudDrive mount, as the strm names it.
    pub target: String,
}

/// `MultipartTitle`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartTitle {
    pub avid: String,
    /// The title's directory in the mapping tree, relative to its root.
    pub directory: String,
    pub parts: Vec<Part>,
    /// Part numbers absent from 1..highest.
    pub missing: Vec<i128>,
    /// Where the parts live, relative to the archive library root, e.g. `type/vr`.
    pub library_dir: Option<String>,
    pub brand: Option<String>,
    pub problem: Option<TitleProblem>,
}

impl MultipartTitle {
    pub fn part_count(&self) -> usize {
        self.parts.len()
    }

    /// Emby already shows it as one item.
    pub fn stackable(&self) -> bool {
        self.part_count() <= MAX_STACKED_PARTS && self.missing.is_empty()
    }

    pub fn complete(&self) -> bool {
        self.missing.is_empty() && self.problem.is_none()
    }
}

/// `SourceRoute`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRoute {
    pub source: Option<String>,
    pub basis: Option<SourceBasis>,
}

/// `PurePosixPath(text).parts`: the root (`/` or `//`) first, then the names.
fn parts(text: &str) -> Vec<String> {
    let normal = pypath::PyPath::new(text);
    let normal = normal.as_str();
    let (root, rest) = if let Some(rest) = normal.strip_prefix("//") {
        (Some("//"), rest)
    } else if let Some(rest) = normal.strip_prefix('/') {
        (Some("/"), rest)
    } else {
        (None, normal)
    };
    let mut out: Vec<String> = root.map(str::to_owned).into_iter().collect();
    out.extend(
        rest.split('/')
            .filter(|part| !part.is_empty() && *part != ".")
            .map(str::to_owned),
    );
    out
}

/// `str(PurePosixPath(*parts))`.
fn from_parts(parts: &[String]) -> String {
    match parts.split_first() {
        None => ".".to_owned(),
        Some((root, rest)) if root.starts_with('/') => format!("{root}{}", rest.join("/")),
        Some(_) => parts.join("/"),
    }
}

/// `is_whole_copy`: whether a file stem names all of `avid` rather than one `-cdN` part. The
/// archive files a title under its ID with the number padded to three digits, so `VRKM-1429`
/// is a whole copy of `VRKM-01429`.
pub fn is_whole_copy(stem: &str, avid: &str) -> bool {
    if CD_SUFFIX_RE.is_match(stem) {
        return false;
    }
    normalize_number(&pytext::py_strip(stem).to_uppercase()) == normalize_number(&pytext::py_strip(avid).to_uppercase())
}

/// `whole_copies`: the video file names among `names` that hold the whole of `avid`.
pub fn whole_copies<'a>(avid: &str, names: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    names
        .into_iter()
        .filter(|name| {
            let suffix = pypath::suffix(name).to_lowercase();
            VIDEO_SUFFIXES.contains(&suffix.as_str()) && is_whole_copy(pypath::stem(name), avid)
        })
        .map(str::to_owned)
        .collect()
}

/// A directory listed as `os.walk` lists it: names in the order the file system returns them.
struct Listing {
    dirs: Vec<String>,
    files: Vec<String>,
}

/// `os.walk(top)` with `followlinks=False` and errors ignored: each directory before the ones
/// below it, sub-directories in listing order; a symbolic link to a directory is listed as a
/// directory but not entered.
fn walk(top: &str, visit: &mut dyn FnMut(&str, &Listing)) {
    let Ok(entries) = fs::read_dir(top) else {
        return;
    };
    let mut listing = Listing {
        dirs: Vec::new(),
        files: Vec::new(),
    };
    for entry in entries {
        // An error while listing ends the directory, as `os.walk` does (`cont = True`).
        let Ok(entry) = entry else {
            return;
        };
        let name = entry.file_name().to_string_lossy().into_owned();
        // `DirEntry.is_dir()` follows symbolic links; an error counts as not a directory.
        let is_dir = fs::metadata(entry.path()).is_ok_and(|meta| meta.is_dir());
        if is_dir {
            listing.dirs.push(name);
        } else {
            listing.files.push(name);
        }
    }
    visit(top, &listing);
    for name in &listing.dirs {
        let path = format!("{top}/{name}");
        if !fs::symlink_metadata(&path).is_ok_and(|meta| meta.file_type().is_symlink()) {
            walk(&path, visit);
        }
    }
}

/// `scan_multipart`: every title under `root` with at least two `-cdN` parts, most parts first.
pub fn scan_multipart(root: &Path, library_root: &str) -> Vec<MultipartTitle> {
    let root = pypath::PyPath::new(&root.to_string_lossy()).as_str().to_owned();
    let mut order: Vec<(String, String)> = Vec::new();
    let mut groups: HashMap<(String, String), Vec<(i128, String)>> = HashMap::new();
    // Each directory's strm stems that are not a part, to spot a whole copy.
    let mut singles: HashMap<String, Vec<String>> = HashMap::new();
    walk(&root, &mut |dirpath, listing| {
        for name in &listing.files {
            let Some(captures) = CD_STRM_RE.captures(name) else {
                if name.to_lowercase().ends_with(".strm") {
                    let keep = name.chars().count().saturating_sub(".strm".len());
                    singles
                        .entry(dirpath.to_owned())
                        .or_default()
                        .push(name.chars().take(keep).collect());
                }
                continue;
            };
            // `\d` matched digits of any script; Python's `int` reads them all.
            let Some(index) = pytext::py_int_decimal(&captures["index"]) else {
                continue;
            };
            let key = (dirpath.to_owned(), captures["stem"].to_uppercase());
            let path = pypath::join(dirpath, name);
            let entries = groups.entry(key.clone()).or_default();
            if entries.is_empty() {
                order.push(key);
            }
            entries.push((index, path));
        }
    });
    let library_root = parts(library_root);
    let mut titles: Vec<MultipartTitle> = order
        .into_iter()
        .filter_map(|key| {
            let mut entries = groups.remove(&key)?;
            if entries.len() <= 1 {
                return None;
            }
            entries.sort();
            let (dirpath, stem) = key;
            let whole = singles
                .get(&dirpath)
                .is_some_and(|names| names.iter().any(|single| is_whole_copy(single, &stem)));
            Some(title(&root, &stem, &entries, &library_root, whole))
        })
        .collect();
    titles.sort_by(|a, b| b.part_count().cmp(&a.part_count()).then_with(|| a.avid.cmp(&b.avid)));
    titles
}

/// `str(path.relative_to(root))` for a path known to be under `root`.
fn relative(path: &str, root: &str) -> String {
    let path = parts(path);
    let root = parts(root);
    from_parts(&path[root.len().min(path.len())..])
}

fn title(root: &str, stem: &str, entries: &[(i128, String)], library_root: &[String], whole: bool) -> MultipartTitle {
    let directory = pypath::PyPath::new(&entries[0].1);
    let directory = parent(directory.as_str());
    let highest = entries.iter().map(|(index, _)| *index).max().unwrap_or(0);
    let missing: Vec<i128> = (1..=highest)
        .filter(|index| !entries.iter().any(|(present, _)| present == index))
        .collect();
    let mut problem = None;
    let mut found = Vec::with_capacity(entries.len());
    for (index, path) in entries {
        let target = read_strm(Path::new(path)).unwrap_or_else(|| {
            problem = Some(TitleProblem::UnreadableStrm);
            String::new()
        });
        found.push(Part {
            index: *index,
            strm: relative(path, root),
            target,
        });
    }

    let mut library_dir = None;
    let mut brand = None;
    if problem.is_none() {
        let mut parents: Vec<Vec<String>> = Vec::new();
        for part in &found {
            let mut parent_parts = parts(&part.target);
            pop_name(&mut parent_parts);
            if !parents.contains(&parent_parts) {
                parents.push(parent_parts);
            }
        }
        if parents.len() > 1 {
            problem = Some(TitleProblem::ScatteredParts);
        } else if let Some(parent_parts) = parents.pop() {
            // `relative_to` needs the same anchor: `.` is no parent of `/x`.
            let anchored = |found: &[String]| found.first().is_some_and(|first| first.starts_with('/'));
            if anchored(&parent_parts) != anchored(library_root)
                || parent_parts.len() < library_root.len()
                || parent_parts[..library_root.len()] != *library_root
            {
                problem = Some(TitleProblem::OutsideLibrary);
            } else {
                let mut relative_parts = parent_parts[library_root.len()..].to_vec();
                // A library directory plus the brand.
                if relative_parts.len() < 2 {
                    problem = Some(TitleProblem::OutsideLibrary);
                } else {
                    brand = relative_parts.pop();
                    library_dir = Some(from_parts(&relative_parts));
                }
            }
        }
    }
    if problem.is_none() && whole {
        problem = Some(TitleProblem::WholeCopy);
    }
    MultipartTitle {
        avid: stem.to_owned(),
        directory: relative(&directory, root),
        parts: found,
        missing,
        library_dir,
        brand,
        problem,
    }
}

/// `PurePosixPath.parent` on a list of parts: the root and `.` stay as they are.
fn pop_name(parts: &mut Vec<String>) {
    let is_root = parts.len() == 1 && parts[0].starts_with('/');
    if !is_root {
        parts.pop();
    }
}

/// `str(PurePosixPath(path).parent)`.
fn parent(path: &str) -> String {
    let mut path_parts = parts(path);
    pop_name(&mut path_parts);
    from_parts(&path_parts)
}

/// `_read_strm`: the one absolute path a strm holds, or `None`.
fn read_strm(path: &Path) -> Option<String> {
    let payload = fs::read(path).ok()?;
    if payload.is_empty() || payload.len() > MAX_STRM_BYTES {
        return None;
    }
    let text = std::str::from_utf8(&payload).ok()?;
    let lines = py::splitlines(pytext::py_strip(text));
    if lines.len() != 1 || !lines[0].starts_with('/') {
        return None;
    }
    Some(pytext::py_strip(lines[0]).to_owned())
}

/// `route_sources`: every intake route, priority routes first, as the archive config lists them.
pub fn route_sources(archive: &ArchiveConfig) -> Vec<String> {
    archive
        .priority_mapping
        .iter()
        .chain(&archive.mapping)
        .map(|(source, _)| source.clone())
        .collect()
}

/// `{**archive.priority_mapping, **archive.mapping}`: a later route replaces an earlier one
/// of the same name in its first position.
fn routes(archive: &ArchiveConfig) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for (source, destination) in archive.priority_mapping.iter().chain(&archive.mapping) {
        if let Some(slot) = out.iter_mut().find(|(existing, _)| existing == source) {
            slot.1.clone_from(destination);
        } else {
            out.push((source.clone(), destination.clone()));
        }
    }
    out
}

/// `source_for_library_dir`: the one route whose destination is `library_dir`; `None` when
/// none or several are.
pub fn source_for_library_dir(archive: &ArchiveConfig, library_dir: Option<&str>) -> Option<String> {
    let library_dir = library_dir?;
    let sources: Vec<String> = routes(archive)
        .into_iter()
        .filter(|(_, destination)| destination == library_dir)
        .map(|(source, _)| source)
        .collect();
    match sources.as_slice() {
        [only] => Some(only.clone()),
        _ => None,
    }
}

/// `source_for_task_dir`: the route rooted at an offline directory, matched by path suffix
/// like the archive does.
pub fn source_for_task_dir(archive: &ArchiveConfig, task_dir_path: Option<&str>) -> Option<String> {
    let task_dir_path = task_dir_path.filter(|path| !path.is_empty())?;
    if archive.src_dir.is_empty() {
        return None;
    }
    let wanted: Vec<String> = parts(task_dir_path).into_iter().skip(1).collect();
    if wanted.is_empty() {
        return None;
    }
    route_sources(archive).into_iter().find(|source| {
        let route = parts(&pypath::join(&archive.src_dir, source));
        route.len() >= wanted.len() && route[route.len() - wanted.len()..] == *wanted
    })
}

/// `resolve_source`.
pub fn resolve_source(archive: &ArchiveConfig, title: &MultipartTitle, ledger_task_dir: Option<&str>) -> SourceRoute {
    if let Some(source) = source_for_library_dir(archive, title.library_dir.as_deref()) {
        return SourceRoute {
            source: Some(source),
            basis: Some(SourceBasis::Library),
        };
    }
    if let Some(source) = source_for_task_dir(archive, ledger_task_dir) {
        return SourceRoute {
            source: Some(source),
            basis: Some(SourceBasis::Ledger),
        };
    }
    SourceRoute {
        source: None,
        basis: None,
    }
}

/// The path of a part's strm under the mapping tree's root, for callers that read it.
pub fn strm_path(root: &Path, part: &Part) -> PathBuf {
    root.join(&part.strm)
}
