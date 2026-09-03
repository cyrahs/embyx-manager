// ---------- fill actor ----------

export type VideoState =
  | 'exists'
  | 'additional_found'
  | 'queued'
  | 'submitted'
  | 'already_tracked'
  | 'submit_failed'
  | 'missing'
  | 'invalid_video_id'
  | 'scan_failed'

export type MoveState = 'moved' | 'stale' | 'conflict' | 'invalid_path' | 'failed'
export type ApplyState = 'succeeded' | 'partial_failed' | 'failed'
export type JobState = 'queued' | 'running' | 'completed' | 'partial_failed' | 'failed'
export type ActorFeedState = 'queued' | 'warming' | 'ready' | 'failed'

export interface ActorPlan {
  actor_id: string
  scraped_count: number
  video_ids: string[]
  error_code: string | null
  /** Optional: older plans and older backends omit it entirely. */
  error_message?: string | null
}

export interface AvidActor {
  actor_id: string
  name: string
}

export interface AvidActors {
  avid: string
  actors: AvidActor[]
}

export interface MoveCandidate {
  candidate_id: string
  video_id: string
  file_name: string
  source_label: string
  destination_conflict: boolean
}

export interface VideoPlan {
  video_id: string
  actor_ids: string[]
  state: VideoState
  existing_files: string[]
  move_candidates: MoveCandidate[]
  warnings: string[]
}

export interface FillActorPlan {
  plan_id: string
  revision: string
  created_at: string
  expires_at: string
  actors: ActorPlan[]
  videos: VideoPlan[]
}

export interface MoveResult {
  candidate_id: string
  video_id: string
  file_name: string
  state: MoveState
  error_code: string | null
}

export interface ApplyResult {
  plan_id: string
  revision: string
  state: ApplyState
  results: MoveResult[]
}

export interface JobProgress {
  stage?: string | null
  completed?: number
  total?: number | null
  unit?: string | null
  current?: string | null
  stage_started_at?: string | null
  updated_at?: string | null
  percent?: number | null
  eta_seconds?: number | null
  elapsed_seconds?: number | null
  last_progress_seconds?: number | null
}

export interface PlanJob {
  id?: string
  job_id?: string
  plan_id?: string | null
  operation?: 'create_plan' | 'apply'
  state?: JobState
  status?: JobState
  error_code?: string | null
  updated_at?: string | null
  progress?: JobProgress | null
}

export interface ActorFeedStatus {
  actor_id: string
  state: ActorFeedState
  attempts: number
  updated_at: string
  error_code: string | null
  freshrss_add_url: string | null
  freshrss_url: string | null
}

export interface PlanEnvelope {
  plan: FillActorPlan | null
  job: PlanJob | null
  planId: string | null
  feeds: ActorFeedStatus[]
}

export interface ApplyJobEnvelope {
  job: PlanJob
  result: ApplyResult | null
}

export interface ActiveApplyRequest {
  planId: string
  revision: string
  candidateIds: string[]
  requestId: string
  jobId?: string
  retrySubmitIfMissing?: boolean
}

// ---------- monitor ----------

export type PipelineId = 'rss' | 'archive' | 'mapping'
export type RunState = 'running' | 'completed' | 'failed' | 'cancelled'

export interface RunSummary {
  run_id: string
  pipeline: PipelineId
  trigger: 'scheduled' | 'manual' | 'watchdog' | 'startup'
  state: RunState
  started_at: string
  finished_at: string | null
  stats: Record<string, number>
  error_count: number
}

export interface RunDetail extends RunSummary {
  errors: string[]
  log_tail: string[]
}

export interface PipelineStatus {
  pipeline: PipelineId
  enabled: boolean
  configured: boolean
  reason: string | null
  running_run_id: string | null
  next_scheduled_at: string | null
  last_run: RunSummary | null
}

// ---------- acquisition ledger ----------

export type AcquisitionState =
  | 'discovered'
  | 'downloading'
  | 'archived'
  | 'resolve_failed'
  | 'exhausted'
  | 'needs_attention'
  | 'ignored'

export type AttemptState =
  | 'pending'
  | 'submitted'
  | 'downloading'
  | 'finished'
  | 'archiving'
  | 'archived'
  | 'junk'
  | 'error'
  | 'stalled'
  | 'lost'

export interface Acquisition {
  avid: string
  state: AcquisitionState
  /** 'manual', 'reconcile', 'fill_actor', or 'rss:<category>'; older rows hold 'rss_actor'/'rss_rank'. */
  source: string
  note: string | null
  archived_paths: string[]
  next_action_at: string | null
  /** ISO date the source knew the release by; anchors the resolve schedule. */
  release_date: string | null
  created_at: string
  updated_at: string
}

export interface MagnetAttempt {
  attempt_no: number
  magnet_source: string
  state: AttemptState
  progress: number | null
  error: string | null
  submitted_at: string | null
  updated_at: string
  info_hash: string | null
}

export interface AcquisitionDetail extends Acquisition {
  attempts: MagnetAttempt[]
}

export interface AcquisitionPage {
  items: Acquisition[]
  counts: Partial<Record<AcquisitionState, number>>
}

// ---------- the manual input source ----------

export interface OfflineDirectory {
  path: string
  name: string
  /** An input source's own offline directory. */
  configured: boolean
  /** Has an archive route, which is what makes it submittable. */
  routed: boolean
}

export interface DirectoryListing {
  path: string
  parent: string | null
  entries: OfflineDirectory[]
  /** Where the picker opens: the directory the last manual batch used. */
  default_path: string | null
}

export type ManualOutcome =
  | 'submitted'
  | 'already_tracked'
  | 'already_in_library'
  | 'no_magnet'
  | 'submit_failed'
  | 'unreadable'

export interface ManualEntry {
  text: string
  avid: string | null
  outcome: ManualOutcome
  archived_paths: string[]
}

export interface ManualSubmission {
  task_dir_path: string
  items: ManualEntry[]
}

export interface TrackerStatus {
  running: boolean
  reason: string | null
  last_polled_at: string | null
  last_error: string | null
  last_stats: Record<string, number>
}

// ---------- config ----------

export interface ConfigSection {
  section: string
  values: Record<string, unknown>
  secrets: Record<string, boolean>
  version: number
}

export interface TestConnectionResult {
  ok: boolean
  detail: string
}

export type SubscriptionKind = 'rss' | 'avbase_talent'

export interface Subscription {
  id: number
  kind: SubscriptionKind
  /** One of the RSS section's category labels; decides the offline directory. */
  category: string
  enabled: boolean
  url: string | null
  /** The URL actually polled: `url` for rss, derived from `talent_id` for avbase_talent. */
  feed_url: string
  talent_id: number | null
  name: string | null
  aliases: string[]
  /** The first poll only records the feed's current items instead of ingesting them. */
  seed_pending: boolean
  last_polled_at: string | null
  last_error: string | null
  created_at: string
  updated_at: string
}

export interface SubscriptionList {
  items: Subscription[]
  categories: string[]
}

export type FreshRssImportStatus = 'new' | 'imported' | 'exists' | 'category_missing' | 'invalid_url'

export interface FreshRssImportEntry {
  url: string
  title: string | null
  category: string | null
  status: FreshRssImportStatus
}

export interface FreshRssImportResult {
  entries: FreshRssImportEntry[]
  imported: number
}
