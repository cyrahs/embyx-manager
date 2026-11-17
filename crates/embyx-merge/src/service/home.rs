//! The home transcode path of the service and the home worker's calls.
//!
//! Python origin: the "home transcode" and "the home worker's calls" sections of
//! `MergeService` in `src/embyx_manager/merge/service.py`. The answers to the worker are
//! Python's dicts key for key (home worker protocol v1, cross-version contract 5), and the
//! files under `.transcode/<id>/` are laid out as Python lays them out, so a transcode one
//! build started is carried on by the other.

use std::path::{Path, PathBuf};

use embyx_clients::kube::{job_name, job_outcome};
use embyx_clients::pyvalue::{self, Raised};
use embyx_core::pyjson::{self, Dict, DumpOptions, Value};
use embyx_core::pypath;

use super::{
    LOGGER, MergePaths, MergeService, StepError, Tick, WAITING_FOR_SLOT, WAITING_FOR_WORKER, pyops,
    rmtree_ignore_errors, show, suppress_kube, unlink_missing_ok,
};
use crate::tasks::{MergeState, MergeTask, TaskField};
use crate::transcode::{
    CHUNK_GAP_SECONDS, CHUNK_SECONDS, Chunk, PART_DURATION_TOLERANCE_SECONDS, TranscodeRepository, WorkerReport,
    audio_job_name, audio_status_path, chunk_path, encoder, transcode_dir,
};
use crate::worker::{HOME_BUDGET_SHARE, HOME_SOURCE_SHARE, MAX_UPLOAD_BYTES, TRANSCODE_MUXERS, muxer, read_status};

/// `ChunkRefusedError`: a chunk or call from the home worker that does not fit the task;
/// `code` is the API error code, `details` go into the answer beside it.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkRefusedError {
    pub code: &'static str,
    pub status: u16,
    pub details: Dict,
}

impl ChunkRefusedError {
    pub fn new(code: &'static str, status: u16) -> Self {
        Self {
            code,
            status,
            details: Dict::new(),
        }
    }

    #[must_use]
    pub fn with(mut self, key: &str, value: Value) -> Self {
        self.details.insert(key, value);
        self
    }
}

impl std::fmt::Display for ChunkRefusedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code)
    }
}

impl std::error::Error for ChunkRefusedError {}

/// Why a call of the home worker was not answered normally.
#[derive(Debug, Clone, PartialEq)]
pub enum WorkerCallError {
    /// Refused with an API error code.
    Refused(ChunkRefusedError),
    /// Anything else Python lets through to the server's error handler.
    Error(StepError),
}

impl std::fmt::Display for WorkerCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refused) => refused.fmt(f),
            Self::Error(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for WorkerCallError {}

impl From<ChunkRefusedError> for WorkerCallError {
    fn from(refused: ChunkRefusedError) -> Self {
        Self::Refused(refused)
    }
}

impl<E: Into<StepError>> From<E> for WorkerCallError {
    fn from(error: E) -> Self {
        Self::Error(error.into())
    }
}

/// `{'task': None, 'wait': wait}`, with the task's id where the worker should know it.
fn waiting(wait: &str, task_id: Option<i64>) -> Value {
    let mut answer = Dict::new();
    answer.insert("task", Value::Null);
    answer.insert("wait", Value::from(wait));
    if let Some(task_id) = task_id {
        answer.insert("task_id", Value::Int(i128::from(task_id)));
    }
    Value::Dict(answer)
}

/// `sum(chunk.duration for chunk in chunks)`: Python's int `0` when there are none.
fn duration_sum(chunks: &[Chunk]) -> Value {
    if chunks.is_empty() {
        Value::Int(0)
    } else {
        Value::Float(crate::py::sum(chunks.iter().map(|chunk| chunk.duration)))
    }
}

/// `prepared.get('parts') or []`, the parts of the audio Job's report.
fn prepared_parts(prepared: &Dict) -> Value {
    let parts = prepared.get("parts").cloned().unwrap_or(Value::Null);
    if parts.is_truthy() {
        parts
    } else {
        Value::List(Vec::new())
    }
}

/// `len(parts)`.
fn py_len(parts: &Value) -> Result<usize, StepError> {
    match parts {
        Value::List(items) => Ok(items.len()),
        Value::Dict(dict) => Ok(dict.len()),
        Value::Str(text) => Ok(text.chars().count()),
        other => Err(StepError::Raised(Raised::new(
            "TypeError",
            format!("object of type '{}' has no len()", pyvalue::type_name(other)),
        ))),
    }
}

/// `float(parts[index]['duration'])` for an index below `len(parts)`.
fn part_duration(parts: &Value, index: usize) -> Result<f64, StepError> {
    let part = match parts {
        Value::List(items) => items[index].clone(),
        Value::Str(text) => Value::Str(text.chars().nth(index).map(String::from).unwrap_or_default()),
        _ => return Err(StepError::Raised(Raised::new("KeyError", index.to_string()))),
    };
    Ok(crate::py::float(pyvalue::item(&part, "duration")?)?)
}

/// The last chunk's end, or `0.0` before the first.
fn resume_at(mine: &[&Chunk]) -> f64 {
    mine.last().map_or(0.0, |chunk| chunk.end())
}

impl MergeService {
    fn transcode_dir_of(&self, task: &MergeTask) -> PathBuf {
        transcode_dir(&super::py_path(&self.work_dir()), task.id)
    }

    fn require_transcodes(&self) -> Result<&TranscodeRepository, StepError> {
        self.transcodes()
            .ok_or_else(|| StepError::runtime("home transcoding is not set up in this deployment"))
    }

    /// `home_bound`: whether the task waits for the home worker rather than going to the merge
    /// Job's encoder.
    pub fn home_bound(&self, task: &MergeTask) -> bool {
        if self.transcodes().is_none() || task.transcoder.as_deref() == Some("cpu") {
            return false;
        }
        if task.transcoder.as_deref() == Some("home") {
            return true;
        }
        if !self.home_transcode_enabled() {
            return false;
        }
        let total = self.total_of(task.id);
        let suffix = pypath::suffix(&task.merged_name).to_lowercase();
        total.is_some_and(|total| i128::from(total) > MAX_UPLOAD_BYTES)
            && muxer(&suffix).is_some_and(|muxer| TRANSCODE_MUXERS.contains(&muxer))
    }

    /// `parked`: a task waiting on the home worker while it is away, which holds up nothing
    /// else. Once the worker is there, the task holds auto-merge back like any other.
    pub fn parked(&self, task: &MergeTask) -> bool {
        if self.inner_presence_online() {
            return false;
        }
        if task.state == MergeState::Queued {
            return self.home_bound(task);
        }
        task.state == MergeState::Transcoding
    }

    /// `reserved_bytes`: what home transcodes still mean to write to the work volume, chunks
    /// plus the assembled file.
    pub fn reserved_bytes(&self, tasks: &[MergeTask]) -> i128 {
        let mut reserved: i128 = 0;
        for task in tasks {
            if !matches!(task.state, MergeState::Queued | MergeState::Transcoding) || !self.home_bound(task) {
                continue;
            }
            #[expect(clippy::cast_precision_loss, reason = "Python multiplies the int by a float")]
            let total = self
                .total_of(task.id)
                .map_or(MAX_UPLOAD_BYTES as f64, |total| total as f64);
            #[expect(clippy::cast_precision_loss, reason = "Python multiplies the int by a float")]
            let budget = MAX_UPLOAD_BYTES as f64 * HOME_BUDGET_SHARE;
            let share = crate::py::min(budget, total * HOME_SOURCE_SHARE);
            #[expect(clippy::cast_possible_truncation, reason = "int() truncates; the share is finite")]
            let share = share.trunc() as i128;
            reserved += 2 * share;
        }
        reserved
    }

    pub(crate) async fn note_waiting(&self, task: &MergeTask, online: bool) -> Result<(), StepError> {
        let notice = (!online).then(|| WAITING_FOR_WORKER.to_owned());
        if task.notice != notice {
            self.repository()
                .update(task.id, vec![TaskField::Notice(notice)])
                .await?;
        }
        Ok(())
    }

    /// `_start_prepare_job`: the Job that probes the parts and copies their audio.
    async fn start_prepare_job(&self, task: &MergeTask, paths: &MergePaths) -> Result<(), StepError> {
        let args = self.prepare_job_args(task, paths).await?;
        self.require_kube()?
            .create_job(&audio_job_name(task.id, &task.avid), &args, task.id)
            .await?;
        Ok(())
    }

    /// The audio Job's directory, made, and its arguments.
    async fn prepare_job_args(&self, task: &MergeTask, paths: &MergePaths) -> Result<Vec<String>, StepError> {
        let directory = self.transcode_dir_of(task);
        let made = directory.clone();
        tokio::task::spawn_blocking(move || std::fs::create_dir_all(&made))
            .await
            .map_err(|error| StepError::runtime(error.to_string()))?
            .map_err(|error| super::os_error(&error, &directory))?;
        let mut args = self.worker_args(paths, &audio_status_path(&directory));
        args.push("--home-prepare".to_owned());
        args.push(show(&directory));
        args.push("--".to_owned());
        args.extend(task.parts.iter().cloned());
        Ok(args)
    }

    /// `_discard_transcode`: stop the audio Job and drop the chunks and audio a home
    /// transcode left.
    pub(crate) async fn discard_transcode(&self, task: &MergeTask) -> Result<(), StepError> {
        if let Some(kube) = self.kube() {
            suppress_kube(kube.delete_job(&audio_job_name(task.id, &task.avid)).await)?;
        }
        let directory = self.transcode_dir_of(task);
        tokio::task::spawn_blocking(move || rmtree_ignore_errors(&directory))
            .await
            .map_err(|error| StepError::runtime(error.to_string()))?;
        if let Some(transcodes) = self.transcodes() {
            transcodes.clear(task.id).await?;
        }
        Ok(())
    }

    /// `_watch_transcode`: follow the audio Job and the worker; assemble once both are done.
    pub(crate) async fn watch_transcode(
        &self,
        tick: &mut Tick,
        task: &MergeTask,
        paths: &MergePaths,
    ) -> Result<(), StepError> {
        let transcodes = self.require_transcodes()?;
        let directory = self.transcode_dir_of(task);
        let prepared = read_status(&audio_status_path(&directory)).unwrap_or_default();
        let state = prepared.get("state").cloned().unwrap_or(Value::Null);
        if pyops::is_str(&state, "failed") {
            let error = prepared.get("error").cloned().unwrap_or(Value::Null);
            return Err(StepError::failed(if error.is_truthy() {
                pyvalue::py_str(&error)
            } else {
                "the audio Job failed".to_owned()
            }));
        }
        if !pyops::is_str(&state, "done") {
            self.watch_prepare_job(task, paths, &state).await?;
        }
        let chunks = transcodes.chunks(task.id).await?;
        let finished = transcodes.finished_parts(task.id).await?;
        let parts = prepared_parts(&prepared);
        let mut durations = Vec::new();
        for part in pyvalue::iterate(&parts)? {
            durations.push(crate::py::float(pyvalue::item(&part, "duration")?)?);
        }
        let total = if durations.is_empty() {
            None
        } else {
            Some(crate::py::sum(durations))
        };
        let encoded = crate::py::sum(chunks.iter().map(|chunk| chunk.duration));
        if parts.is_truthy() && pyops::is_str(&state, "done") && finished.len() == task.parts.len() {
            if tick.slot_taken {
                if task.notice.as_deref() != Some(WAITING_FOR_SLOT) {
                    self.repository()
                        .update(task.id, vec![TaskField::Notice(Some(WAITING_FOR_SLOT.to_owned()))])
                        .await?;
                }
                return Ok(());
            }
            tick.slot_taken = true;
            return self.start_assembly(task, paths, &chunks).await;
        }
        let progress = match total {
            Some(total) if total != 0.0 => crate::py::min(encoded / total, 1.0),
            _ => 0.0,
        };
        self.repository()
            .update(
                task.id,
                vec![
                    TaskField::Phase(Some(
                        if parts.is_truthy() { "transcoding" } else { "probing" }.to_owned(),
                    )),
                    TaskField::Progress(Some(progress)),
                    TaskField::Notice((!self.inner_presence_online()).then(|| WAITING_FOR_WORKER.to_owned())),
                ],
            )
            .await?;
        Ok(())
    }

    /// `_watch_prepare_job`.
    async fn watch_prepare_job(&self, task: &MergeTask, paths: &MergePaths, state: &Value) -> Result<(), StepError> {
        let kube = self.require_kube()?;
        let name = audio_job_name(task.id, &task.avid);
        let Some(job) = kube.get_job(&name).await? else {
            // Gone with a restart of the task, or never started: (re)start it; audio already
            // copied is kept.
            return self.start_prepare_job(task, paths).await;
        };
        let outcome = job_outcome(&job);
        if outcome == "failed" {
            let tail = suppress_kube(kube.log_tail(&name).await)?.unwrap_or_default();
            return Err(StepError::failed(super::job_failed(
                "the audio Job failed",
                &job,
                &tail,
            )));
        }
        if outcome == "succeeded" && !pyops::state_in(state, &["done", "failed"])? {
            let fresh = read_status(&audio_status_path(&self.transcode_dir_of(task))).unwrap_or_default();
            if !pyops::state_in(fresh.get("state").unwrap_or(&Value::Null), &["done", "failed"])? {
                return Err(StepError::failed("the audio Job ended without reporting a result"));
            }
        }
        Ok(())
    }

    /// `_start_assembly`: the merge Job joins the chunks and the copied audio.
    async fn start_assembly(&self, task: &MergeTask, paths: &MergePaths, chunks: &[Chunk]) -> Result<(), StepError> {
        let directory = self.transcode_dir_of(task);
        let parts: Vec<Value> = (0..task.parts.len())
            .map(|index| {
                let mut part = Dict::new();
                part.insert("index", Value::Int(i128::try_from(index).unwrap_or(i128::MAX)));
                part.insert(
                    "chunks",
                    Value::List(
                        chunks
                            .iter()
                            .filter(|chunk| usize::try_from(chunk.part_index).ok() == Some(index))
                            .map(|chunk| {
                                let mut entry = Dict::new();
                                entry.insert("index", Value::Int(i128::from(chunk.chunk_index)));
                                entry.insert("duration", Value::Float(chunk.duration));
                                Value::Dict(entry)
                            })
                            .collect(),
                    ),
                );
                Value::Dict(part)
            })
            .collect();
        let mut manifest = Dict::new();
        manifest.insert("parts", Value::List(parts));
        // `json.dumps(manifest)`: Python's default separators and ASCII escapes.
        let text = pyjson::dumps(&Value::Dict(manifest), &DumpOptions::python_default())
            .map_err(|error| StepError::raised("ValueError", error.to_string()))?;
        let target = directory.join("manifest.json");
        let written = target.clone();
        tokio::task::spawn_blocking(move || std::fs::write(&written, text))
            .await
            .map_err(|error| StepError::runtime(error.to_string()))?
            .map_err(|error| super::os_error(&error, &target))?;
        let name = job_name(task.id, &task.avid);
        unlink_missing_ok(&paths.status)?;
        let kube = self.require_kube()?;
        let mut args = self.worker_args(paths, &paths.status);
        args.push("--home-assemble".to_owned());
        args.push(show(&directory));
        kube.create_job(&name, &args, task.id).await?;
        let moved = self
            .repository()
            .transition(
                task.id,
                MergeState::Transcoding,
                MergeState::Merging,
                vec![
                    TaskField::JobName(Some(name)),
                    TaskField::Phase(Some("starting".to_owned())),
                    TaskField::Progress(Some(0.0)),
                    TaskField::Notice(None),
                ],
            )
            .await?;
        if moved.is_some() {
            suppress_kube(kube.delete_job(&audio_job_name(task.id, &task.avid)).await)?;
        }
        Ok(())
    }

    // -- the home worker's calls ---------------------------------------------------

    /// `claim(report)`: hand the worker its task, the one it was on or the oldest waiting
    /// once nothing else runs.
    pub async fn claim(&self, report: WorkerReport) -> Result<Value, StepError> {
        self.presence().report(report);
        let current = {
            let _guard = self.lock().await;
            if self.unavailable().is_some() || self.transcodes().is_none() {
                return Ok(waiting("unavailable", None));
            }
            let tasks = self.repository().open_tasks().await?;
            if let Some(current) = tasks.iter().find(|task| task.state == MergeState::Transcoding) {
                current.clone()
            } else {
                for task in &tasks {
                    if task.state == MergeState::Queued {
                        self.total_bytes(task).await?;
                    }
                }
                let waiting_tasks: Vec<&MergeTask> = tasks
                    .iter()
                    .filter(|task| task.state == MergeState::Queued && self.home_bound(task))
                    .collect();
                let Some(first) = waiting_tasks.first().copied() else {
                    return Ok(waiting("idle", None));
                };
                // The encode takes hours and its assembly waits for the merge slot anyway, so
                // it may start while an earlier title uploads, is verified or is archived.
                let others = tasks.iter().any(|task| {
                    matches!(task.state, MergeState::Queued | MergeState::Merging)
                        && !waiting_tasks.iter().any(|waiting| waiting.id == task.id)
                });
                if others {
                    return Ok(waiting("busy", None));
                }
                match self.begin_transcode(first).await {
                    Ok(Some(started)) => started,
                    Ok(None) => return Ok(waiting("busy", None)),
                    Err(StepError::Failed(failure)) => {
                        tracing::warn!(
                            logger = LOGGER,
                            "merge of {} failed before its transcode: {failure}",
                            first.avid
                        );
                        self.repository()
                            .transition(
                                first.id,
                                MergeState::Queued,
                                MergeState::Failed,
                                vec![
                                    TaskField::FailedState(Some(MergeState::Queued)),
                                    TaskField::Error(Some(failure)),
                                    TaskField::Notice(None),
                                ],
                            )
                            .await?;
                        return Ok(waiting("idle", None));
                    }
                    Err(error) => return Err(error),
                }
            }
        };
        self.wake();
        self.assignment(&current).await
    }

    /// `_begin_transcode`.
    async fn begin_transcode(&self, task: &MergeTask) -> Result<Option<MergeTask>, StepError> {
        self.discard_transcode(task).await?;
        let tick = self.tick_now();
        self.refuse_whole_copy(tick.cloud.as_ref(), task, &self.paths(task)?)
            .await?;
        let started = self
            .repository()
            .transition(
                task.id,
                MergeState::Queued,
                MergeState::Transcoding,
                vec![
                    TaskField::Transcoder(Some("home".to_owned())),
                    TaskField::JobName(None),
                    TaskField::Phase(Some("probing".to_owned())),
                    TaskField::Progress(Some(0.0)),
                    TaskField::Error(None),
                    TaskField::Notice(None),
                ],
            )
            .await?;
        if let Some(started) = &started {
            // Should the Job API fail now, the next tick finds no audio Job and starts it.
            let paths = self.paths(started)?;
            let args = self.prepare_job_args(started, &paths).await?;
            let kube = self.require_kube()?;
            suppress_kube(
                kube.create_job(&audio_job_name(started.id, &started.avid), &args, started.id)
                    .await,
            )?;
        }
        Ok(started)
    }

    /// `_assignment`: the task as the worker sees it.
    async fn assignment(&self, task: &MergeTask) -> Result<Value, StepError> {
        let prepared = read_status(&audio_status_path(&self.transcode_dir_of(task))).unwrap_or_default();
        let parts = prepared.get("parts").cloned().unwrap_or(Value::Null);
        let bitrate = prepared.get("video_bitrate").cloned().unwrap_or(Value::Null);
        let (Value::List(parts), Value::Int(_) | Value::Bool(_)) = (&parts, &bitrate) else {
            return Ok(waiting("probing", Some(task.id)));
        };
        let transcodes = self.require_transcodes()?;
        let chunks = transcodes.chunks(task.id).await?;
        let finished = transcodes.finished_parts(task.id).await?;
        if finished.len() >= task.parts.len() {
            // Nothing left to encode: the cluster joins the parts once the audio is ready.
            return Ok(waiting("assembling", Some(task.id)));
        }
        let mut views = Vec::with_capacity(parts.len());
        for (index, (part, info)) in task.parts.iter().zip(parts).enumerate() {
            let index_i32 = i32::try_from(index).unwrap_or(i32::MAX);
            let mine: Vec<&Chunk> = chunks.iter().filter(|chunk| chunk.part_index == index_i32).collect();
            let size = pyvalue::py_int(pyvalue::item(info, "size")?)?;
            let duration = crate::py::float(pyvalue::item(info, "duration")?)?;
            let mut view = Dict::new();
            view.insert("index", Value::Int(i128::from(index_i32)));
            view.insert("name", Value::from(pypath::name(part)));
            view.insert("size", Value::Int(size));
            view.insert("duration", Value::Float(duration));
            view.insert("done", Value::Bool(finished.contains(&index_i32)));
            view.insert(
                "next_chunk",
                Value::Int(i128::try_from(mine.len()).unwrap_or(i128::MAX)),
            );
            view.insert("resume_at", Value::Float(resume_at(&mine)));
            views.push(Value::Dict(view));
        }
        // `zip(..., strict=True)` notices the lengths differ once the shorter one runs out.
        if parts.len() != task.parts.len() {
            let how = if parts.len() < task.parts.len() {
                "shorter"
            } else {
                "longer"
            };
            return Err(StepError::raised(
                "ValueError",
                format!("zip() argument 2 is {how} than argument 1"),
            ));
        }
        let mut assigned = Dict::new();
        assigned.insert("id", Value::Int(i128::from(task.id)));
        assigned.insert("avid", Value::from(task.avid.as_str()));
        assigned.insert("video_bitrate", bitrate.clone());
        assigned.insert("chunk_seconds", Value::Int(i128::from(CHUNK_SECONDS)));
        assigned.insert("encoder", Value::Dict(encoder()));
        assigned.insert("paused", Value::Bool(task.transcode_paused));
        assigned.insert("parts", Value::List(views));
        let mut answer = Dict::new();
        answer.insert("task", Value::Dict(assigned));
        answer.insert("wait", Value::Null);
        Ok(Value::Dict(answer))
    }

    /// `heartbeat(report)`: record the worker's report; tell it whether to pause or drop its
    /// task.
    pub async fn heartbeat(&self, report: WorkerReport) -> Result<Value, StepError> {
        let task_id = report.task_id;
        self.presence().report(report);
        let task = match task_id {
            Some(task_id) => self.repository().get(task_id).await?,
            None => None,
        };
        let active = task.as_ref().is_some_and(|task| task.state == MergeState::Transcoding);
        let mut answer = Dict::new();
        answer.insert("active", Value::Bool(active));
        answer.insert(
            "paused",
            Value::Bool(task.as_ref().is_some_and(|task| task.transcode_paused)),
        );
        Ok(Value::Dict(answer))
    }

    /// `transcoding_task(task_id, part_index)`: the task the worker addresses, refused unless
    /// it is being transcoded and has the part.
    pub async fn transcoding_task(&self, task_id: i64, part_index: i64) -> Result<MergeTask, WorkerCallError> {
        let Some(task) = self.repository().get(task_id).await? else {
            return Err(ChunkRefusedError::new("transcode_task_not_found", 404).into());
        };
        if task.state != MergeState::Transcoding {
            return Err(ChunkRefusedError::new("transcode_task_not_active", 409).into());
        }
        if usize::try_from(part_index).map_or(true, |index| index >= task.parts.len()) {
            return Err(ChunkRefusedError::new("transcode_part_not_found", 404).into());
        }
        Ok(task)
    }

    /// `staging_chunk`: where an incoming chunk is written before it is checked.
    pub fn staging_chunk(&self, task: &MergeTask, part_index: i64, chunk_index: i64) -> PathBuf {
        let path = chunk_path(
            &self.transcode_dir_of(task),
            i128::from(part_index),
            i128::from(chunk_index),
        );
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        path.with_file_name(format!(".{name}.part"))
    }

    /// `accept_chunk`: keep a chunk written to `received`; `false` when it was a repeat of one
    /// already kept.
    pub async fn accept_chunk(
        &self,
        task: &MergeTask,
        chunk: &Chunk,
        received: &Path,
    ) -> Result<bool, WorkerCallError> {
        let transcodes = self.require_transcodes()?;
        let finished = transcodes.finished_parts(task.id).await?;
        let chunks = transcodes.chunks(task.id).await?;
        let mine: Vec<&Chunk> = chunks
            .iter()
            .filter(|item| item.part_index == chunk.part_index)
            .collect();
        let next = Value::Int(i128::try_from(mine.len()).unwrap_or(i128::MAX));
        if finished.contains(&chunk.part_index) {
            unlink_missing_ok(received)?;
            if mine
                .iter()
                .any(|item| item.chunk_index == chunk.chunk_index && item.sha256 == chunk.sha256)
            {
                return Ok(false);
            }
            return Err(ChunkRefusedError::new("transcode_part_finished", 409).into());
        }
        if let Some(kept) = usize::try_from(chunk.chunk_index)
            .ok()
            .and_then(|index| mine.get(index))
        {
            unlink_missing_ok(received)?;
            if kept.sha256 == chunk.sha256 {
                return Ok(false);
            }
            return Err(ChunkRefusedError::new("transcode_chunk_conflict", 409)
                .with("next_chunk", next)
                .into());
        }
        let expected = resume_at(&mine);
        let ahead = usize::try_from(chunk.chunk_index).map_or(chunk.chunk_index > 0, |index| index > mine.len());
        if ahead || (chunk.start - expected).abs() > CHUNK_GAP_SECONDS {
            unlink_missing_ok(received)?;
            return Err(ChunkRefusedError::new("transcode_chunk_out_of_order", 409)
                .with("next_chunk", next)
                .with("resume_at", Value::Float(expected))
                .into());
        }
        let target = chunk_path(
            &self.transcode_dir_of(task),
            i128::from(chunk.part_index),
            i128::from(chunk.chunk_index),
        );
        std::fs::rename(received, &target).map_err(|error| {
            StepError::raised(
                super::os_class(&error),
                crate::py::os_error(&error, Some(received), Some(&target)),
            )
        })?;
        transcodes.add_chunk(task.id, chunk).await?;
        Ok(true)
    }

    /// `finish_part`: the worker says the part is fully encoded; its chunks must cover it.
    pub async fn finish_part(&self, task: &MergeTask, part_index: i32) -> Result<(), WorkerCallError> {
        let transcodes = self.require_transcodes()?;
        let prepared = read_status(&audio_status_path(&self.transcode_dir_of(task))).unwrap_or_default();
        let parts = prepared_parts(&prepared);
        let chunks = transcodes.chunks(task.id).await?;
        let mine: Vec<&Chunk> = chunks.iter().filter(|item| item.part_index == part_index).collect();
        let index = usize::try_from(part_index).ok();
        let duration = match index {
            Some(index) if index < py_len(&parts)? => Some(part_duration(&parts, index)?),
            // A negative index counts from the end in Python; the route never passes one.
            _ => None,
        };
        let covered = resume_at(&mine);
        let short = duration.is_none_or(|duration| (covered - duration).abs() > PART_DURATION_TOLERANCE_SECONDS);
        if short {
            return Err(ChunkRefusedError::new("transcode_part_incomplete", 409)
                .with("covered", Value::Float(covered))
                .with("duration", duration.map_or(Value::Null, Value::Float))
                .into());
        }
        transcodes.finish_part(task.id, part_index).await?;
        self.wake();
        Ok(())
    }

    /// `transcode_view`: what the merge page shows of a home transcode.
    pub async fn transcode_view(&self, task: &MergeTask) -> Result<Option<Value>, StepError> {
        let Some(transcodes) = self.transcodes() else {
            return Ok(None);
        };
        if task.transcoder.as_deref() != Some("home") || task.state != MergeState::Transcoding {
            return Ok(None);
        }
        let prepared = read_status(&audio_status_path(&self.transcode_dir_of(task))).unwrap_or_default();
        let finished = transcodes.finished_parts(task.id).await?;
        let chunks = transcodes.chunks(task.id).await?;
        let field = |key: &str| prepared.get(key).cloned().unwrap_or(Value::Null);
        let mut view = Dict::new();
        view.insert(
            "parts_done",
            Value::Int(i128::try_from(finished.len()).unwrap_or(i128::MAX)),
        );
        view.insert(
            "part_count",
            Value::Int(i128::try_from(task.parts.len()).unwrap_or(i128::MAX)),
        );
        view.insert("encoded_seconds", duration_sum(&chunks));
        view.insert(
            "encoded_bytes",
            Value::Int(chunks.iter().map(|chunk| i128::from(chunk.bytes)).sum()),
        );
        view.insert("duration", field("duration"));
        view.insert("video_bitrate", field("video_bitrate"));
        view.insert("audio_done", field("audio_done"));
        view.insert("audio_state", field("state"));
        view.insert("paused", Value::Bool(task.transcode_paused));
        Ok(Some(Value::Dict(view)))
    }
}
