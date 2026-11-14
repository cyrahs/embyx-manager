//! Archiving one downloaded folder into the library.
//!
//! Python origin: `src/embyx_manager/monitor/archive.py`. A folder holds one video (possibly
//! in parts, possibly in several cuts of which only the best is wanted) plus whatever the
//! source bundled with it. Archiving reads the AVID, renames the video after it, moves it into
//! the library under its brand, and deletes the folder.
//!
//! The entry point is [`ArchivePipeline::archive_folder`]; the tracker calls it for one
//! finished download, the reconcile scan for whatever it finds under a route. Both get a
//! structured [`ArchiveResult`]: a folder that cannot be identified is reported as needing
//! attention instead of being guessed at or deleted.
//!
//! The work is synchronous, as in Python (which runs it in a worker thread); cancellation is
//! checked between folders. Every call crosses a shared mount that can refuse any single
//! call, so failures are contained per folder. All file-system access goes through [`Fs`],
//! with `pathlib`'s semantics ([`crate::fs`]); the order of the mutations is pinned by the
//! scenarios under `goldens/scenarios/archive/`.

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use embyx_core::avid::{AvidParser, get_brand, is_high_resolution, strip_variant_tags};
use embyx_core::config::models::ArchiveConfig;
use embyx_core::pypath::{self, PyPath};
use embyx_core::pytext::{is_py_decimal, is_py_space, py_int_decimal};
use indexmap::IndexMap;

use crate::fs::{self, Fs, LocalFs, PyOsError, path_text};
use crate::reports::{PyException, RunCancelledError, RunContext};

/// `MIN_MULTI_PART_VIDEOS`.
pub const MIN_MULTI_PART_VIDEOS: usize = 2;
/// `PHONE_RIP_MAX_SHARE`: a phone rip is at most half the main file's size.
pub const PHONE_RIP_MAX_SHARE: f64 = 0.5;

/// `Outcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outcome {
    Archived,
    /// No video worth keeping: ads, samples, or an empty shell.
    Junk,
    /// Videos are present but their identity is unclear; nothing was touched.
    NeedsAttention,
    Failed,
}

impl Outcome {
    /// `.value`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Archived => "archived",
            Self::Junk => "junk",
            Self::NeedsAttention => "needs_attention",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `ArchiveResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveResult {
    pub outcome: Outcome,
    pub avid: String,
    /// Library-relative paths of everything moved in, for the ledger to record.
    pub archived_paths: Vec<String>,
    pub reason: String,
}

impl ArchiveResult {
    fn new(outcome: Outcome, avid: &str, reason: &str) -> Self {
        Self {
            outcome,
            avid: avid.to_owned(),
            archived_paths: Vec::new(),
            reason: reason.to_owned(),
        }
    }

    fn archived(avid: &str, archived_paths: Vec<String>) -> Self {
        Self {
            outcome: Outcome::Archived,
            avid: avid.to_owned(),
            archived_paths,
            reason: String::new(),
        }
    }
}

/// `_VideoSet`: the videos in one folder worth archiving, in cd order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoSet {
    pub avid: String,
    pub videos: Vec<PathBuf>,
}

/// What [`ArchivePipeline::inspect_folder`] decided: the video to file, or why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inspection {
    Selected(VideoSet),
    Concluded(ArchiveResult),
}

/// Why a run (or a call that is not contained per folder) stopped: the operator asked, or the
/// file system raised.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArchiveError {
    #[error(transparent)]
    Cancelled(#[from] RunCancelledError),
    #[error(transparent)]
    Os(#[from] PyOsError),
}

impl PyException for ArchiveError {
    fn class_name(&self) -> &str {
        match self {
            Self::Cancelled(error) => error.class_name(),
            Self::Os(error) => error.class_name(),
        }
    }

    fn py_repr(&self) -> String {
        match self {
            Self::Cancelled(error) => error.py_repr(),
            Self::Os(error) => error.py_repr(),
        }
    }
}

/// An error `_isolate` either contains (any `Exception`) or lets through (cancellation).
pub trait Isolated: PyException {
    fn cancellation(&self) -> Option<RunCancelledError>;
}

impl Isolated for ArchiveError {
    fn cancellation(&self) -> Option<RunCancelledError> {
        match self {
            Self::Cancelled(error) => Some(*error),
            Self::Os(_) => None,
        }
    }
}

/// `_isolate(ctx, message)`: report a failure and carry on, so one bad entry cannot end the
/// run; cancellation is let through. `Ok(None)` when a failure was contained.
pub fn isolate<T, E: Isolated>(
    ctx: &RunContext,
    message: &str,
    result: Result<T, E>,
) -> Result<Option<T>, RunCancelledError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) => {
            if let Some(cancelled) = error.cancellation() {
                return Err(cancelled);
            }
            ctx.exception(message, Some(&error));
            ctx.add("items_failed", 1);
            Ok(None)
        }
    }
}

/// `video.name`.
fn name_of(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// `str(base / part)` as pathlib normalizes it, back as a path.
fn join(base: &Path, part: &str) -> PathBuf {
    PathBuf::from(pypath::join(&path_text(base), part))
}

/// `_display(path, base)`: `path.relative_to(base)`, or the path itself.
fn display(path: &Path, base: &Path) -> String {
    match path.strip_prefix(base) {
        Ok(relative) if relative.as_os_str().is_empty() => ".".to_owned(),
        Ok(relative) => path_text(relative),
        Err(_) => path_text(path),
    }
}

/// Where Python's `$` may match in `text`: at the end, or before a newline that ends it.
/// The returned slice is `text` without that final newline.
fn before_final_newline(text: &str) -> Option<&str> {
    text.strip_suffix('\n')
}

/// `multi_part_video_check(videos)`: whether these files are parts of one video.
pub fn multi_part_video_check(videos: &[PathBuf]) -> bool {
    if videos.len() < MIN_MULTI_PART_VIDEOS {
        return false;
    }
    // re.sub(r'\d+', '', name): only the digits differ.
    let non_digit: BTreeSet<String> = videos
        .iter()
        .map(|video| name_of(video).chars().filter(|&ch| !is_py_decimal(ch)).collect())
        .collect();
    if non_digit.len() == 1 {
        return true;
    }
    // re.sub(r'-[A-Z]', '', name): xxx-A.mp4 and xxx-B.mp4.
    let non_index: BTreeSet<String> = videos.iter().map(|video| strip_dash_letters(&name_of(video))).collect();
    non_index.len() == 1
}

/// `re.sub(r'-[A-Z]', '', text)`.
fn strip_dash_letters(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '-' && chars.peek().is_some_and(char::is_ascii_uppercase) {
            chars.next();
            continue;
        }
        out.push(ch);
    }
    out
}

/// `is_4k_video(video)`: the file name marks the higher-resolution cut.
pub fn is_4k_video(video: &Path) -> bool {
    is_high_resolution(&name_of(video))
}

/// `normalize_copy_suffix(stem)`: `re.sub(r'\s*\(\d+\)$', '', stem)`.
pub fn normalize_copy_suffix(stem: &str) -> String {
    // `$` matches at the end or before a final newline, which the match leaves in place.
    let (body, tail) = match before_final_newline(stem) {
        Some(body) if body.ends_with(')') => (body, "\n"),
        _ => (stem, ""),
    };
    let Some(inside) = body.strip_suffix(')') else {
        return stem.to_owned();
    };
    let digits_start = inside
        .char_indices()
        .rev()
        .take_while(|&(_, ch)| is_py_decimal(ch))
        .last()
        .map_or(inside.len(), |(at, _)| at);
    if digits_start == inside.len() {
        return stem.to_owned();
    }
    let Some(before) = inside[..digits_start].strip_suffix('(') else {
        return stem.to_owned();
    };
    let kept = before.trim_end_matches(is_py_space);
    format!("{kept}{tail}")
}

/// `_variant_base(name)`: the stem with quality tags stripped, separator runs collapsed to
/// one `-`, trimmed of `-_. ` and case-folded.
fn variant_base(name: &str) -> String {
    let stripped = strip_variant_tags(name);
    let mut collapsed = String::with_capacity(stripped.len());
    let mut run = String::new();
    for ch in stripped.chars() {
        if matches!(ch, '-' | '_' | '.' | ' ') {
            run.push(ch);
            continue;
        }
        flush_separator_run(&mut collapsed, &mut run);
        collapsed.push(ch);
    }
    flush_separator_run(&mut collapsed, &mut run);
    caseless::default_case_fold_str(collapsed.trim_matches(['-', '_', '.', ' ']))
}

/// `_SEPARATOR_RUN_RE.sub('-', ...)`: a run of two or more becomes one `-`.
fn flush_separator_run(out: &mut String, run: &mut String) {
    if run.chars().count() >= 2 {
        out.push('-');
    } else {
        out.push_str(run);
    }
    run.clear();
}

/// `\d{1,2}` then `$`: one or two decimal digits ending the text (or before its final
/// newline); their value.
fn trailing_index(rest: &str) -> Option<i128> {
    let digits = before_final_newline(rest).unwrap_or(rest);
    let count = digits.chars().count();
    if !(1..=2).contains(&count) || !digits.chars().all(is_py_decimal) {
        return None;
    }
    py_int_decimal(digits)
}

/// `_TRAILING_INDEX_RE.match(text)`: `^(?P<base>.+?)[-_ ](?P<index>\d{1,2})$`.
fn match_trailing_index(text: &str) -> Option<(&str, i128)> {
    for (at, ch) in text.char_indices().skip(1) {
        // `.` stops at a newline.
        if text[..at].ends_with('\n') {
            return None;
        }
        if matches!(ch, '-' | '_' | ' ')
            && let Some(index) = trailing_index(&text[at + 1..])
        {
            return Some((&text[..at], index));
        }
    }
    None
}

/// `^(?P<base>.+)(?P<tail>...)$` with a greedy base: the base is the text before `tail`
/// at the end (or before a final newline), non-empty and without a newline.
fn greedy_base(text: &str, tail: impl Fn(&str) -> Option<usize>) -> Option<(&str, &str)> {
    let candidates = [Some(text), before_final_newline(text)];
    for body in candidates.into_iter().flatten() {
        if let Some(length) = tail(body) {
            let base = &body[..body.len() - length];
            if !base.is_empty() && !base.contains('\n') {
                return Some((base, &body[body.len() - length..]));
            }
        }
    }
    None
}

/// `_TRAILING_LETTER_RE.match(text)`: `^(?P<base>.+)(?P<letter>[a-z])$`.
fn match_trailing_letter(text: &str) -> Option<(&str, char)> {
    let (base, letter) = greedy_base(text, |body| {
        body.ends_with(|ch: char| ch.is_ascii_lowercase()).then_some(1)
    })?;
    Some((base, letter.chars().next()?))
}

/// `_PHONE_RIP_RE.match(text)`: `^(?P<base>.+)[-_]5$`; the base.
fn match_phone_rip(text: &str) -> Option<&str> {
    greedy_base(text, |body| (body.ends_with("-5") || body.ends_with("_5")).then_some(2)).map(|(base, _)| base)
}

/// `_SINGLE_ZERO_PADDED_AVID_RE.fullmatch(text)`: `[A-Z]{2,10}-0\d{4}`; brand and number.
fn match_single_zero_padded(text: &str) -> Option<(&str, &str)> {
    let (brand, rest) = text.split_once('-')?;
    if !(2..=10).contains(&brand.len()) || !brand.bytes().all(|byte| byte.is_ascii_uppercase()) {
        return None;
    }
    let number = rest.strip_prefix('0')?;
    (number.chars().count() == 4 && number.chars().all(is_py_decimal)).then_some((brand, number))
}

/// `_avids_are_padding_equivalent(first, second)`: equal, or one differs by a single
/// five-digit padding zero.
fn avids_are_padding_equivalent(first: &str, second: &str) -> bool {
    if first == second {
        return true;
    }
    [(first, second), (second, first)]
        .into_iter()
        .any(|(padded, unpadded)| {
            match_single_zero_padded(padded).is_some_and(|(brand, number)| unpadded == format!("{brand}-{number}"))
        })
}

/// `_order_as_parts(videos)`: the videos as cd1..cdN, or `None` when they are not one set.
fn order_as_parts(videos: &[PathBuf]) -> Option<Vec<PathBuf>> {
    let bases: Vec<String> = videos.iter().map(|video| variant_base(&name_of(video))).collect();
    indexed_run(videos, &bases)
        .or_else(|| unindexed_first(videos, &bases))
        .or_else(|| letter_run(videos, &bases))
}

/// Picks `videos` in the order of their keys (a stable sort, as `sorted`).
fn ordered_by<K: Ord + Clone>(videos: &[PathBuf], keys: &[K]) -> (Vec<PathBuf>, Vec<K>) {
    let mut order: Vec<usize> = (0..videos.len()).collect();
    order.sort_by(|&a, &b| keys[a].cmp(&keys[b]));
    (
        order.iter().map(|&at| videos[at].clone()).collect(),
        order.iter().map(|&at| keys[at].clone()).collect(),
    )
}

fn indexed_run(videos: &[PathBuf], bases: &[String]) -> Option<Vec<PathBuf>> {
    let parsed: Vec<(&str, i128)> = bases
        .iter()
        .map(|base| match_trailing_index(base))
        .collect::<Option<_>>()?;
    if parsed.iter().map(|(base, _)| *base).collect::<BTreeSet<_>>().len() != 1 {
        return None;
    }
    let indices: Vec<i128> = parsed.iter().map(|(_, index)| *index).collect();
    let (ordered, sorted) = ordered_by(videos, &indices);
    (sorted.iter().copied().eq(1..=i128::try_from(videos.len()).ok()?)).then_some(ordered)
}

fn unindexed_first(videos: &[PathBuf], bases: &[String]) -> Option<Vec<PathBuf>> {
    for lead in 0..videos.len() {
        let mut rest = Vec::new();
        let mut indices = Vec::new();
        let mut all_match = true;
        for other in (0..videos.len()).filter(|&other| other != lead) {
            // re.escape(base) + r'[-_ ](\d{1,2})$', matched at the start.
            let sibling = bases[other]
                .strip_prefix(bases[lead].as_str())
                .and_then(|rest| rest.strip_prefix(['-', '_', ' ']))
                .and_then(trailing_index);
            let Some(index) = sibling else {
                all_match = false;
                break;
            };
            rest.push(videos[other].clone());
            indices.push(index);
        }
        if !all_match {
            continue;
        }
        let (ordered, sorted) = ordered_by(&rest, &indices);
        if sorted.iter().copied().eq(2..=i128::try_from(videos.len()).ok()?) {
            let mut parts = vec![videos[lead].clone()];
            parts.extend(ordered);
            return Some(parts);
        }
    }
    None
}

fn letter_run(videos: &[PathBuf], bases: &[String]) -> Option<Vec<PathBuf>> {
    let parsed: Vec<(&str, char)> = bases
        .iter()
        .map(|base| match_trailing_letter(base))
        .collect::<Option<_>>()?;
    if parsed.iter().map(|(base, _)| *base).collect::<BTreeSet<_>>().len() != 1 {
        return None;
    }
    let letters: Vec<char> = parsed.iter().map(|(_, letter)| *letter).collect();
    let (ordered, sorted) = ordered_by(videos, &letters);
    let expected = ('a'..='z').take(videos.len());
    (sorted.len() <= 26 && sorted.iter().copied().eq(expected)).then_some(ordered)
}

/// `dict.fromkeys(values)`: distinct, in first-seen order.
fn distinct<'a>(values: impl IntoIterator<Item = &'a String>) -> Vec<&'a String> {
    let mut seen = Vec::new();
    for value in values {
        if !seen.contains(&value) {
            seen.push(value);
        }
    }
    seen
}

/// `ArchivePipeline`.
#[derive(Debug, Clone)]
pub struct ArchivePipeline {
    config: Arc<ArchiveConfig>,
    avid: AvidParser,
    fs: Arc<dyn Fs>,
    /// `Path(config.src_dir)`.
    pub src_dir: PathBuf,
    /// `Path(config.dst_dir)`.
    pub dst_dir: PathBuf,
    route_roots: Vec<PathBuf>,
}

impl ArchivePipeline {
    /// `ArchivePipeline(config=, avid_parser=)` on the real file system.
    pub fn new(config: ArchiveConfig, avid_parser: AvidParser) -> Self {
        Self::with_fs(config, avid_parser, Arc::new(LocalFs))
    }

    pub fn with_fs(config: ArchiveConfig, avid_parser: AvidParser, fs: Arc<dyn Fs>) -> Self {
        let src_dir = PyPath::new(&config.src_dir).as_path().to_path_buf();
        let dst_dir = PyPath::new(&config.dst_dir).as_path().to_path_buf();
        let mut route_roots: Vec<PathBuf> = Vec::new();
        for (source, _) in config.priority_mapping.iter().chain(&config.mapping) {
            let root = join(&src_dir, source);
            if !route_roots.contains(&root) {
                route_roots.push(root);
            }
        }
        Self {
            config: Arc::new(config),
            avid: avid_parser,
            fs,
            src_dir,
            dst_dir,
            route_roots,
        }
    }

    pub fn config(&self) -> &ArchiveConfig {
        &self.config
    }

    pub fn fs(&self) -> &Arc<dyn Fs> {
        &self.fs
    }

    /// `avid_of(name)`: the AVID a file or folder name reads as; empty when none.
    pub fn avid_of(&self, name: &str) -> String {
        self.avid.get_avid(name)
    }

    /// `covers_other_route(folder, route_root=)`: whether a folder in one route is, or
    /// contains, another route's source.
    pub fn covers_other_route(&self, folder: &Path, route_root: &Path) -> bool {
        self.route_roots
            .iter()
            .any(|other| other != route_root && other.starts_with(folder))
    }

    /// The configured routes, priority routes first: `(src, dst, priority)`.
    pub fn routes(config: &ArchiveConfig) -> Vec<(String, String, bool)> {
        config
            .priority_mapping
            .iter()
            .map(|(src, dst)| (src.clone(), dst.clone(), true))
            .chain(
                config
                    .mapping
                    .iter()
                    .map(|(src, dst)| (src.clone(), dst.clone(), false)),
            )
            .collect()
    }

    /// `src_dir / src`.
    pub fn route_root(&self, source: &str) -> PathBuf {
        join(&self.src_dir, source)
    }

    // -- the full scan ---------------------------------------------------------

    /// `run(ctx)`: archive every folder under the configured routes, priority routes first.
    pub fn run(&self, ctx: &RunContext) -> Result<(), ArchiveError> {
        for (src, dst, priority) in Self::routes(&self.config) {
            ctx.check_cancelled()?;
            let root = self.route_root(&src);
            if !fs::is_dir(&*self.fs, &root)? {
                // A route whose source is absent has nothing to do; it is not a failure.
                ctx.info(&format!(
                    "skipping {}, its source directory does not exist",
                    display(&root, &self.src_dir)
                ));
                continue;
            }
            ctx.info(&format!(
                "processing {} -> {}{}",
                path_text(&root),
                path_text(&join(&self.dst_dir, &dst)),
                if priority { " (priority)" } else { "" }
            ));
            self.scan_route(&root, &dst, ctx, priority)?;
        }
        Ok(())
    }

    /// `scan_route(root, dst_subdir, ctx, priority=)`.
    pub fn scan_route(
        &self,
        root: &Path,
        dst_subdir: &str,
        ctx: &RunContext,
        priority: bool,
    ) -> Result<(), ArchiveError> {
        let mut entries = self.fs.iterdir(root)?;
        entries.sort();
        for folder in &entries {
            ctx.check_cancelled()?;
            if !fs::is_dir(&*self.fs, folder)? {
                continue;
            }
            if self.covers_other_route(folder, root) {
                ctx.info(&format!(
                    "skipping {}, it is another route of its own",
                    display(folder, &self.src_dir)
                ));
                continue;
            }
            self.archive_folder(folder, dst_subdir, ctx, priority, "");
        }
        let mut loose = Vec::new();
        for entry in entries {
            if fs::is_video(&*self.fs, &entry)? {
                loose.push(entry);
            }
        }
        if !loose.is_empty() {
            self.archive_loose_videos(root, loose, dst_subdir, ctx, priority)?;
        }
        Ok(())
    }

    /// `_archive_loose_videos`: videos directly in a route, grouped by AVID.
    fn archive_loose_videos(
        &self,
        root: &Path,
        videos: Vec<PathBuf>,
        dst_subdir: &str,
        ctx: &RunContext,
        priority: bool,
    ) -> Result<(), RunCancelledError> {
        let mut by_avid: IndexMap<String, Vec<PathBuf>> = IndexMap::new();
        for video in videos {
            let avid = self.avid.get_avid(&name_of(&video));
            if avid.is_empty() {
                ctx.warning(&format!(
                    "failed to read an avid for the loose file {}, skipping",
                    name_of(&video)
                ));
                continue;
            }
            by_avid.entry(avid).or_default().push(video);
        }
        for (avid, group) in by_avid {
            ctx.check_cancelled()?;
            let message = format!(
                "failed to archive the loose file {}",
                display(&join(root, &avid), &self.src_dir)
            );
            let result = self
                .archive_loose_group(&avid, group, dst_subdir, ctx, priority)
                .map_err(ArchiveError::Os);
            isolate(ctx, &message, result)?;
        }
        Ok(())
    }

    /// `archive_loose_group(avid, videos, dst_subdir, ctx, priority=)`: file one AVID's
    /// loose videos, in name order; there is no folder to delete.
    pub fn archive_loose_group(
        &self,
        avid: &str,
        mut videos: Vec<PathBuf>,
        dst_subdir: &str,
        ctx: &RunContext,
        priority: bool,
    ) -> Result<ArchiveResult, PyOsError> {
        videos.sort_by_key(|video| name_of(video));
        let selection = VideoSet {
            avid: avid.to_owned(),
            videos,
        };
        let archived = self.move_into_library(&selection, dst_subdir, ctx, priority)?;
        if archived.is_empty() {
            return Ok(ArchiveResult::new(
                Outcome::NeedsAttention,
                avid,
                "destination already holds this avid",
            ));
        }
        ctx.add("videos_archived", i64::try_from(archived.len()).unwrap_or(i64::MAX));
        Ok(ArchiveResult::archived(avid, archived))
    }

    // -- the one way in --------------------------------------------------------

    /// `archive_folder(folder, dst_subdir, ctx, priority=, expected_avid=)`: move one
    /// folder's video into the library and delete the folder. Failures are reported in the
    /// result, never raised.
    pub fn archive_folder(
        &self,
        folder: &Path,
        dst_subdir: &str,
        ctx: &RunContext,
        priority: bool,
        expected_avid: &str,
    ) -> ArchiveResult {
        match self.inspect_folder(folder, ctx, expected_avid) {
            Inspection::Concluded(result) => result,
            Inspection::Selected(selection) => self.file_selection(folder, &selection, dst_subdir, ctx, priority),
        }
    }

    /// `inspect_folder(folder, ctx, expected_avid=)`: which video to file, or why not.
    /// Nothing outside the folder is touched.
    pub fn inspect_folder(&self, folder: &Path, ctx: &RunContext, expected_avid: &str) -> Inspection {
        match self.select_videos(folder, ctx, expected_avid) {
            Ok(inspection) => inspection,
            Err(error) => {
                ctx.exception(
                    &format!("failed to inspect {}", display(folder, &self.src_dir)),
                    Some(&error),
                );
                ctx.add("items_failed", 1);
                Inspection::Concluded(ArchiveResult::new(Outcome::Failed, "", &error.to_string()))
            }
        }
    }

    /// `file_selection(folder, selection, dst_subdir, ctx, priority=)`: move the video in,
    /// delete the folder.
    pub fn file_selection(
        &self,
        folder: &Path,
        selection: &VideoSet,
        dst_subdir: &str,
        ctx: &RunContext,
        priority: bool,
    ) -> ArchiveResult {
        let archived = match self.move_into_library(selection, dst_subdir, ctx, priority) {
            Ok(archived) => archived,
            Err(error) => {
                ctx.exception(
                    &format!("failed to archive {}", display(folder, &self.src_dir)),
                    Some(&error),
                );
                ctx.add("items_failed", 1);
                return ArchiveResult::new(Outcome::Failed, &selection.avid, &error.to_string());
            }
        };
        if archived.is_empty() {
            return ArchiveResult::new(
                Outcome::NeedsAttention,
                &selection.avid,
                "destination already holds this avid",
            );
        }
        self.discard(folder, ctx);
        ctx.add("videos_archived", i64::try_from(archived.len()).unwrap_or(i64::MAX));
        ArchiveResult::archived(&selection.avid, archived)
    }

    /// `replaced_copies(avid, library_paths)`: the files of a copy being replaced that are
    /// still in the library; a path that does not name one of this AVID's videos inside the
    /// library (absolute, `..`, another title) is left out.
    pub fn replaced_copies(&self, avid: &str, library_paths: &[String]) -> Result<Vec<PathBuf>, PyOsError> {
        let mut found: Vec<PathBuf> = Vec::new();
        for relative in library_paths {
            let parts = PyPath::new(relative);
            if parts.as_str().starts_with('/') || parts.as_str().split('/').any(|part| part == "..") {
                continue;
            }
            let path = join(&self.dst_dir, relative);
            let parent = path.parent().map_or_else(|| path.clone(), Path::to_path_buf);
            if !found.contains(&path) && self.matching_archived(avid, &parent)?.contains(&path) {
                found.push(path);
            }
        }
        Ok(found)
    }

    // -- selection -------------------------------------------------------------

    /// `_select_videos`: decide which files in a folder are the video, or why we cannot tell.
    fn select_videos(&self, folder: &Path, ctx: &RunContext, expected_avid: &str) -> Result<Inspection, PyOsError> {
        if !fs::is_dir(&*self.fs, folder)? {
            return Ok(Inspection::Concluded(ArchiveResult::new(
                Outcome::Failed,
                "",
                &format!("{} is not a directory", path_text(folder)),
            )));
        }
        let min_size_bytes = i128::from(self.config.min_size_mb) * 1024 * 1024;
        // Recursive: sources nest the video inside a subfolder often enough, and the folder is
        // deleted afterwards, so missing one would delete it.
        let mut candidates = self.fs.rglob(folder)?;
        candidates.sort();
        let mut videos = Vec::new();
        for candidate in candidates {
            if fs::is_video(&*self.fs, &candidate)? && i128::from(fs::size(&*self.fs, &candidate)?) > min_size_bytes {
                videos.push(candidate);
            }
        }
        if videos.is_empty() {
            ctx.info(&format!(
                "{} holds no video above {}MB",
                name_of(folder),
                self.config.min_size_mb
            ));
            return Ok(Inspection::Concluded(ArchiveResult::new(
                Outcome::Junk,
                "",
                "no video above the size threshold",
            )));
        }
        if videos.len() > 1
            && videos
                .iter()
                .any(|video| video.file_name().is_some_and(|name| name.to_str().is_none()))
        {
            // Not in Python (D72): names are compared as text below, and lossy text could make
            // two different files look like copies or variants of one cut, which deletes one.
            let reason = "a video name is not UTF-8";
            ctx.warning(&format!("{reason} in {}, skipping", name_of(folder)));
            return Ok(Inspection::Concluded(ArchiveResult::new(
                Outcome::NeedsAttention,
                "",
                reason,
            )));
        }
        let videos = self.drop_duplicate_copies(folder, videos, ctx)?;
        let avid = match self.identify_avid(folder, &videos, ctx, expected_avid) {
            Ok(avid) => avid,
            Err(result) => return Ok(Inspection::Concluded(result)),
        };
        let videos = if videos.len() > 1 {
            if let Some(arranged) = self.arrange_videos(folder, &videos, &avid, ctx)? {
                arranged
            } else {
                let reason = "several unrelated videos in one folder";
                ctx.warning(&format!("{reason} in {}, skipping", name_of(folder)));
                return Ok(Inspection::Concluded(ArchiveResult::new(
                    Outcome::NeedsAttention,
                    &avid,
                    reason,
                )));
            }
        } else {
            videos
        };
        Ok(Inspection::Selected(VideoSet { avid, videos }))
    }

    /// `_identify_avid`: read the videos' AVID and reconcile it with a trusted expected value.
    fn identify_avid(
        &self,
        folder: &Path,
        videos: &[PathBuf],
        ctx: &RunContext,
        expected_avid: &str,
    ) -> Result<String, ArchiveResult> {
        let avids: BTreeSet<String> = videos.iter().map(|video| self.avid.get_avid(&name_of(video))).collect();
        let mut avid = if avids.len() > 1 {
            let found = avids.iter().map(String::as_str).collect::<Vec<_>>().join(", ");
            if !expected_avid.is_empty()
                && avids
                    .iter()
                    .all(|avid| avids_are_padding_equivalent(expected_avid, avid))
            {
                ctx.info(&format!(
                    "{found} in {} are padding-equivalent to expected {expected_avid}; using expected avid",
                    name_of(folder)
                ));
                expected_avid.to_owned()
            } else {
                let reason = format!("multiple avids in one folder: {found}");
                ctx.warning(&format!("{reason} in {}, skipping", name_of(folder)));
                return Err(ArchiveResult::new(Outcome::NeedsAttention, "", &reason));
            }
        } else {
            avids.into_iter().next().unwrap_or_default()
        };
        if avid.is_empty() {
            // The file names gave nothing; the folder name is the last clue.
            avid = self.avid.get_avid(&name_of(folder));
        }
        if avid.is_empty() {
            ctx.warning(&format!("failed to read an avid for {}, skipping", name_of(folder)));
            return Err(ArchiveResult::new(Outcome::NeedsAttention, "", "no avid could be read"));
        }
        if !expected_avid.is_empty() && avid != expected_avid {
            if avids_are_padding_equivalent(expected_avid, &avid) {
                ctx.info(&format!(
                    "{avid} in {} is padding-equivalent to expected {expected_avid}; using expected avid",
                    name_of(folder)
                ));
                expected_avid.clone_into(&mut avid);
            } else {
                let reason = format!("expected {expected_avid} but found {avid}");
                ctx.warning(&format!("{reason} in {}, skipping", name_of(folder)));
                return Err(ArchiveResult::new(Outcome::NeedsAttention, &avid, &reason));
            }
        }
        Ok(avid)
    }

    /// `_arrange_videos`: explain a folder holding several videos of one AVID, or give up
    /// (`None`, for an operator).
    fn arrange_videos(
        &self,
        folder: &Path,
        videos: &[PathBuf],
        avid: &str,
        ctx: &RunContext,
    ) -> Result<Option<Vec<PathBuf>>, PyOsError> {
        if multi_part_video_check(videos) {
            let mut sorted = videos.to_vec();
            sorted.sort_by_key(|video| name_of(video));
            return Ok(Some(sorted));
        }
        let Some(collapsed) = Self::collapse_resolution_variants(folder, videos, ctx) else {
            return Ok(None);
        };
        if collapsed.len() == 1 {
            return Ok(Some(collapsed));
        }
        if let Some(parts) = order_as_parts(&collapsed) {
            return Ok(Some(parts));
        }
        self.drop_phone_rips(folder, &collapsed, avid, ctx)
    }

    /// `_collapse_resolution_variants`: keep one file per cut when the same cut ships in
    /// several resolutions; the one marked high-resolution wins.
    fn collapse_resolution_variants(folder: &Path, videos: &[PathBuf], ctx: &RunContext) -> Option<Vec<PathBuf>> {
        let mut groups: IndexMap<String, Vec<&PathBuf>> = IndexMap::new();
        for video in videos {
            groups.entry(variant_base(&name_of(video))).or_default().push(video);
        }
        let mut kept: Vec<PathBuf> = Vec::new();
        for group in groups.values() {
            if let [only] = group.as_slice() {
                kept.push((*only).clone());
                continue;
            }
            let high_resolution: Vec<&&PathBuf> = group.iter().filter(|video| is_4k_video(video)).collect();
            let [winner] = high_resolution.as_slice() else {
                return None;
            };
            let dropped = group
                .iter()
                .filter(|video| !std::ptr::eq(**video, **winner))
                .map(|video| name_of(video))
                .collect::<Vec<_>>()
                .join(", ");
            ctx.info(&format!(
                "keeping the high-resolution {} in {} and dropping {dropped}",
                name_of(winner),
                name_of(folder)
            ));
            kept.push((**winner).clone());
        }
        kept.sort_by_key(|video| name_of(video));
        Some(kept)
    }

    /// `_drop_phone_rips`: keep only the main video when every extra is a `-5` phone rip at
    /// most half its size.
    fn drop_phone_rips(
        &self,
        folder: &Path,
        videos: &[PathBuf],
        avid: &str,
        ctx: &RunContext,
    ) -> Result<Option<Vec<PathBuf>>, PyOsError> {
        // max(videos, key=size): the first of the largest.
        let mut dominant = 0;
        let mut largest = None;
        for (at, video) in videos.iter().enumerate() {
            let size = fs::size(&*self.fs, video)?;
            if largest.is_none_or(|largest| size > largest) {
                largest = Some(size);
                dominant = at;
            }
        }
        // `dominant.stat().st_size * 0.5`: exact for any real file size, so a rip qualifies
        // while twice its size is at most the main file's.
        let limit = fs::size(&*self.fs, &videos[dominant])?;
        for (at, rip) in videos.iter().enumerate() {
            if at == dominant {
                continue;
            }
            let base = variant_base(&name_of(rip));
            let Some(rip_base) = match_phone_rip(&base) else {
                return Ok(None);
            };
            if !avids_are_padding_equivalent(&self.avid_of(rip_base), avid)
                || u128::from(fs::size(&*self.fs, rip)?) * 2 > u128::from(limit)
            {
                return Ok(None);
            }
        }
        let dropped = videos
            .iter()
            .enumerate()
            .filter(|&(at, _)| at != dominant)
            .map(|(_, rip)| name_of(rip))
            .collect::<Vec<_>>()
            .join(", ");
        ctx.info(&format!(
            "keeping {} in {} and dropping the phone rip {dropped}",
            name_of(&videos[dominant]),
            name_of(folder)
        ));
        Ok(Some(vec![videos[dominant].clone()]))
    }

    /// `_drop_duplicate_copies`: delete "name (1).mp4" copies that duplicate a same-sized
    /// original. A copy that cannot be deleted is still left out of the selection.
    fn drop_duplicate_copies(
        &self,
        folder: &Path,
        videos: Vec<PathBuf>,
        ctx: &RunContext,
    ) -> Result<Vec<PathBuf>, PyOsError> {
        type Key = (String, String, u64);
        let mut base_by_key: IndexMap<Key, usize> = IndexMap::new();
        let mut copies_by_key: IndexMap<Key, Vec<usize>> = IndexMap::new();
        for (at, video) in videos.iter().enumerate() {
            let name = name_of(video);
            let stem = pypath::stem(&name);
            let key = (
                normalize_copy_suffix(stem),
                pypath::suffix(&name).to_lowercase(),
                fs::size(&*self.fs, video)?,
            );
            if stem == key.0 {
                base_by_key.entry(key).or_insert(at);
            } else {
                copies_by_key.entry(key).or_default().push(at);
            }
        }
        let mut dropped = BTreeSet::new();
        for (key, copies) in &copies_by_key {
            let Some(&base) = base_by_key.get(key) else {
                continue;
            };
            ctx.info(&format!(
                "duplicate videos in {}, keeping {} and dropping {}",
                name_of(folder),
                name_of(&videos[base]),
                copies
                    .iter()
                    .map(|&at| name_of(&videos[at]))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            for &copy in copies {
                dropped.insert(copy);
                match self.fs.unlink(&videos[copy]) {
                    Ok(()) => ctx.add("duplicate_copies_deleted", 1),
                    Err(error) => ctx.exception(
                        &format!(
                            "failed to remove the duplicate {} in {}",
                            name_of(&videos[copy]),
                            name_of(folder)
                        ),
                        Some(&error),
                    ),
                }
            }
        }
        Ok(videos
            .into_iter()
            .enumerate()
            .filter(|(at, _)| !dropped.contains(at))
            .map(|(_, video)| video)
            .collect())
    }

    // -- moving ----------------------------------------------------------------

    /// `_move_into_library`: rename each part after the AVID and move it in; empty when it
    /// is already there. Every target is checked before the first move; a move that fails
    /// raises with the earlier parts already in the library.
    fn move_into_library(
        &self,
        selection: &VideoSet,
        dst_subdir: &str,
        ctx: &RunContext,
        priority: bool,
    ) -> Result<Vec<String>, PyOsError> {
        let route_dst = join(&self.dst_dir, dst_subdir);
        let Some(brand_dir) = self.find_dst_dir(&selection.avid, &route_dst, ctx) else {
            return Ok(Vec::new());
        };
        // Brands pinned by brand_mapping land in the same directory whatever the route.
        if let Some(brand) = get_brand(&selection.avid).filter(|brand| !brand.is_empty())
            && !self.brand_routed(&brand)
        {
            if priority {
                self.promote_from_normal(&selection.avid, &brand, &brand_dir, ctx)?;
            } else if self.held_by_priority(&selection.avid, &brand, &brand_dir, ctx)? {
                return Ok(Vec::new());
            }
        }
        let multi_part = selection.videos.len() > 1;
        let mut targets = Vec::new();
        // The selection's order is the cd order.
        for (index, video) in selection.videos.iter().enumerate() {
            let video_suffix = pypath::suffix(&name_of(video)).to_owned();
            let suffix = if multi_part {
                format!("-cd{}{video_suffix}", index + 1)
            } else {
                video_suffix
            };
            let target = join(&brand_dir, &format!("{}{suffix}", selection.avid));
            if fs::exists(&*self.fs, &target)? {
                ctx.warning(&format!("{} exists, skipping", display(&target, &self.dst_dir)));
                return Ok(Vec::new());
            }
            targets.push((video, target));
        }
        self.fs.mkdir_parents(&brand_dir)?;
        let mut archived = Vec::new();
        for (video, target) in targets {
            ctx.info(&format!(
                "moving {} to {}",
                name_of(video),
                display(&target, &self.dst_dir)
            ));
            self.fs.rename(video, &target)?;
            archived.push(display(&target, &self.dst_dir));
        }
        Ok(archived)
    }

    /// `_discard`: delete the folder once its video is in the library.
    fn discard(&self, folder: &Path, ctx: &RunContext) {
        match self.fs.rmtree(folder) {
            Ok(()) => ctx.add("folders_discarded", 1),
            Err(error) => ctx.exception(
                &format!("archived {} but failed to remove it", name_of(folder)),
                Some(&error),
            ),
        }
    }

    // -- destinations ----------------------------------------------------------

    /// `route_for_task_dir(task_dir_path)`: the `(dst_subdir, priority)` of the route rooted
    /// at this offline directory, matched by path suffix (the mount exposes the cloud tree
    /// unchanged).
    pub fn route_for_task_dir(&self, task_dir_path: &str) -> Option<(String, bool)> {
        let mut wanted = posix_parts(task_dir_path);
        if wanted.first().is_some_and(|first| first == "/") {
            wanted.remove(0);
        }
        if wanted.is_empty() {
            return None;
        }
        let tables = [(&self.config.priority_mapping, true), (&self.config.mapping, false)];
        for (table, priority) in tables {
            for (source, dst) in table {
                let parts = posix_parts(&path_text(&self.route_root(source)));
                if parts.len() >= wanted.len() && parts[parts.len() - wanted.len()..] == wanted[..] {
                    return Some((dst.clone(), priority));
                }
            }
        }
        None
    }

    /// `library_holdings(avid, ctx, task_dir_path=)`: library paths already holding this
    /// AVID. When the caller's directory feeds a priority route, copies under normal
    /// destinations are promoted into it first.
    pub fn library_holdings(
        &self,
        avid: &str,
        ctx: &RunContext,
        task_dir_path: Option<&str>,
    ) -> Result<Vec<String>, PyOsError> {
        let Some(brand) = get_brand(avid).filter(|brand| !brand.is_empty()) else {
            return Ok(Vec::new());
        };
        let route = task_dir_path.and_then(|task_dir| self.route_for_task_dir(task_dir));
        if let Some((dst, true)) = &route
            && !self.brand_routed(&brand)
            && let Some(target_dir) = self.find_dst_dir(avid, &join(&self.dst_dir, dst), ctx)
        {
            self.promote_from_normal(avid, &brand, &target_dir, ctx)?;
        }
        let mut found: Vec<String> = Vec::new();
        let destinations = self
            .config
            .priority_mapping
            .iter()
            .chain(&self.config.mapping)
            .map(|(_, dst)| dst);
        for dst in distinct(destinations) {
            let Some(brand_dir) = self.find_dst_dir(avid, &join(&self.dst_dir, dst), ctx) else {
                continue;
            };
            for archived in self.matching_archived(avid, &brand_dir)? {
                let path = display(&archived, &self.dst_dir);
                if !found.contains(&path) {
                    found.push(path);
                }
            }
        }
        Ok(found)
    }

    /// `find_dst_dir(avid, dst_dir, ctx)`: the brand directory under a route destination,
    /// or under the `brand_mapping` destination pinning the brand.
    pub fn find_dst_dir(&self, avid: &str, dst_dir: &Path, ctx: &RunContext) -> Option<PathBuf> {
        let Some(brand) = get_brand(avid).filter(|brand| !brand.is_empty()) else {
            ctx.warning(&format!("failed to get brand for {avid}, skipping find_dst"));
            return None;
        };
        for (brand_dst, brands) in &self.config.brand_mapping {
            if brands.contains(&brand) {
                return Some(join(&join(&self.dst_dir, brand_dst), &brand));
            }
        }
        Some(join(dst_dir, &brand))
    }

    fn brand_routed(&self, brand: &str) -> bool {
        self.config
            .brand_mapping
            .iter()
            .any(|(_, brands)| brands.iter().any(|routed| routed == brand))
    }

    /// `_matching_archived(avid, brand_dir)`: `AVID.ext` or `AVID-cdN.ext` videos, sorted.
    /// The `-` keeps ABC-12 from claiming ABC-123.mp4.
    fn matching_archived(&self, avid: &str, brand_dir: &Path) -> Result<Vec<PathBuf>, PyOsError> {
        if !fs::is_dir(&*self.fs, brand_dir)? {
            return Ok(Vec::new());
        }
        let prefix = format!("{avid}-");
        let mut matching = Vec::new();
        for entry in self.fs.iterdir(brand_dir)? {
            if !fs::is_video(&*self.fs, &entry)? {
                continue;
            }
            let name = name_of(&entry);
            let stem = pypath::stem(&name);
            if stem == avid || stem.starts_with(&prefix) {
                matching.push(entry);
            }
        }
        matching.sort();
        Ok(matching)
    }

    /// `_promote_from_normal`: move the AVID out of every normal route's destination into
    /// the priority one; a name already taken there is kept on both sides.
    fn promote_from_normal(
        &self,
        avid: &str,
        brand: &str,
        target_dir: &Path,
        ctx: &RunContext,
    ) -> Result<(), PyOsError> {
        for normal_dst in distinct(self.config.mapping.iter().map(|(_, dst)| dst)) {
            let source_dir = join(&join(&self.dst_dir, normal_dst), brand);
            if source_dir == target_dir {
                continue;
            }
            for archived in self.matching_archived(avid, &source_dir)? {
                let target = target_dir.join(archived.file_name().unwrap_or_default());
                if fs::exists(&*self.fs, &target)? {
                    ctx.warning(&format!(
                        "cannot promote {}, {} exists",
                        display(&archived, &self.dst_dir),
                        display(&target, &self.dst_dir)
                    ));
                    continue;
                }
                self.fs.mkdir_parents(target_dir)?;
                ctx.info(&format!(
                    "promoting {} to {}",
                    display(&archived, &self.dst_dir),
                    display(&target, &self.dst_dir)
                ));
                // The emptied brand directory is left behind.
                self.fs.rename(&archived, &target)?;
                ctx.add("duplicates_promoted", 1);
            }
        }
        Ok(())
    }

    /// `_held_by_priority`: whether a priority destination already holds the AVID.
    fn held_by_priority(&self, avid: &str, brand: &str, own_dir: &Path, ctx: &RunContext) -> Result<bool, PyOsError> {
        for priority_dst in distinct(self.config.priority_mapping.iter().map(|(_, dst)| dst)) {
            let holder_dir = join(&join(&self.dst_dir, priority_dst), brand);
            if holder_dir == own_dir {
                continue;
            }
            if !self.matching_archived(avid, &holder_dir)?.is_empty() {
                ctx.warning(&format!(
                    "{avid} already archived in priority {}, skipping",
                    display(&holder_dir, &self.dst_dir)
                ));
                ctx.add("skipped_priority", 1);
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// `PurePosixPath(text).parts`: the anchor (`/`, or `//` for exactly two leading slashes)
/// and the names, `.` parts dropped.
fn posix_parts(text: &str) -> Vec<String> {
    let normalized = PyPath::new(text);
    let normalized = normalized.as_str();
    let (anchor, rest) = if let Some(rest) = normalized.strip_prefix("//") {
        (Some("//"), rest)
    } else if let Some(rest) = normalized.strip_prefix('/') {
        (Some("/"), rest)
    } else {
        (None, normalized)
    };
    anchor
        .into_iter()
        .chain(rest.split('/').filter(|part| !part.is_empty() && *part != "."))
        .map(str::to_owned)
        .collect()
}

/// Run blocking file-system work from async code the way `asyncio.to_thread` keeps the event
/// loop free: on a multi-thread runtime the worker hands its other tasks off first; elsewhere
/// (single-thread test runtimes) it runs in place.
pub fn blocking<T>(work: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(work),
        _ => work(),
    }
}

#[cfg(test)]
pub(crate) mod scenarios;
#[cfg(test)]
mod tests;
