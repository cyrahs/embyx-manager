import type {
  ActiveApplyRequest,
  ConfigSection,
  PipelineId,
  PipelineStatus,
  RunDetail,
  RunSummary,
  TestConnectionResult,
  ActorFeedStatus,
  ApplyJobEnvelope,
  ApplyResult,
  FillActorPlan,
  JobProgress,
  JobState,
  MoveResult,
  MoveState,
  PlanEnvelope,
  PlanJob,
} from './types'

const API_TOKEN_KEY = 'embyx-manager-api-token'
const AUTH_REQUIRED_KEY = 'embyx-manager-auth-required'
const ACTIVE_PLAN_KEY = 'embyx-manager-active-plan-id'
const ACTIVE_APPLY_KEY = 'embyx-manager-active-apply'
const PLAN_ID = /^[A-Za-z0-9_-]{1,256}$/
const REQUEST_ID = /^[A-Za-z0-9_-]{16,128}$/
const APPLY_JOB_STATES = new Set<JobState>(['queued', 'running', 'completed', 'partial_failed', 'failed'])
const MOVE_STATES = new Set<MoveState>(['moved', 'stale', 'conflict', 'invalid_path', 'failed'])
const MAX_APPLY_ITEMS = 5_000

const apiTokenListeners = new Set<() => void>()
/** Mirrors the stored token so a browser that blocks persistence still works for this tab. */
let memoryToken: string | null = null
/** Set once a write is refused (private mode, quota), which is when the mirror matters. */
let persistenceFailed = false

/** Null when the browser blocks persistence (private mode, disabled site data). */
function localStorageOrNull(): Storage | null {
  try {
    return window.localStorage as Storage | undefined ?? null
  } catch {
    return null
  }
}

function readLocalStorage(key: string): string | null {
  try {
    return localStorageOrNull()?.getItem(key) ?? null
  } catch {
    return null
  }
}

function writeLocalStorage(key: string, value: string | null): void {
  const storage = localStorageOrNull()
  if (storage === null) {
    persistenceFailed = true
    return
  }
  try {
    if (value === null) storage.removeItem(key)
    else storage.setItem(key, value)
  } catch {
    // Private-mode browsers reject persistence; the in-memory copy still serves this tab.
    persistenceFailed = true
  }
}

/** The signed-in token, persisted across tabs and restarts until the user signs out. */
export function readApiToken(): string | null {
  // Storage stays authoritative where it works, so a token cleared elsewhere stays cleared;
  // the mirror only answers once a write has proven that persistence is unavailable.
  return readLocalStorage(API_TOKEN_KEY) ?? (persistenceFailed ? memoryToken : null)
}

export function hasApiToken(): boolean {
  return Boolean(readApiToken())
}

export function setApiToken(value: string): void {
  const token = value.trim()
  memoryToken = token || null
  writeLocalStorage(API_TOKEN_KEY, memoryToken)
  for (const listener of apiTokenListeners) listener()
}

/** Signing out in one tab must sign out the others, so mirror cross-tab storage writes. */
function handleTokenStorageEvent(event: StorageEvent): void {
  if (event.key !== null && event.key !== API_TOKEN_KEY) return
  memoryToken = readLocalStorage(API_TOKEN_KEY)
  for (const listener of apiTokenListeners) listener()
}

/** Lets every page observe the shared token instead of caching its own copy. */
export function subscribeApiToken(listener: () => void): () => void {
  apiTokenListeners.add(listener)
  if (apiTokenListeners.size === 1) window.addEventListener('storage', handleTokenStorageEvent)
  return () => {
    apiTokenListeners.delete(listener)
    if (apiTokenListeners.size === 0) window.removeEventListener('storage', handleTokenStorageEvent)
  }
}

/**
 * Last known answer to "does this deployment demand a token?", so a returning visitor
 * lands on the login screen without a flash of the app while /api/health is in flight.
 */
export function readAuthRequired(): boolean {
  return readLocalStorage(AUTH_REQUIRED_KEY) === 'true'
}

export function rememberAuthRequired(value: boolean): void {
  writeLocalStorage(AUTH_REQUIRED_KEY, value ? 'true' : null)
}

export function getActivePlanId(): string | null {
  try {
    const value = window.sessionStorage.getItem(ACTIVE_PLAN_KEY)
    if (!value) return null
    if (PLAN_ID.test(value)) return value
    window.sessionStorage.removeItem(ACTIVE_PLAN_KEY)
    return null
  } catch {
    return null
  }
}

export function setActivePlanId(value: string | null): void {
  try {
    if (value && PLAN_ID.test(value)) window.sessionStorage.setItem(ACTIVE_PLAN_KEY, value)
    else window.sessionStorage.removeItem(ACTIVE_PLAN_KEY)
  } catch {
    // Session recovery is best-effort; storage restrictions must not block a scan.
  }
}

function normalizeActiveApplyRequest(value: unknown): ActiveApplyRequest | null {
  if (!isRecord(value)) return null
  if (
    typeof value.planId !== 'string' || !PLAN_ID.test(value.planId) ||
    typeof value.revision !== 'string' || value.revision.length < 1 || value.revision.length > 512 ||
    typeof value.requestId !== 'string' || !REQUEST_ID.test(value.requestId) ||
    (value.jobId !== undefined && (typeof value.jobId !== 'string' || !PLAN_ID.test(value.jobId))) ||
    (value.retrySubmitIfMissing !== undefined && typeof value.retrySubmitIfMissing !== 'boolean') ||
    !Array.isArray(value.candidateIds) ||
    !value.candidateIds.every((candidateId) => typeof candidateId === 'string' && candidateId.length > 0 && candidateId.length <= 512)
  ) return null
  return {
    planId: value.planId,
    revision: value.revision,
    candidateIds: [...new Set(value.candidateIds)],
    requestId: value.requestId,
    ...(value.jobId ? { jobId: value.jobId } : {}),
    ...(value.retrySubmitIfMissing ? { retrySubmitIfMissing: true } : {}),
  }
}

export function getActiveApplyRequest(): ActiveApplyRequest | null {
  try {
    const raw = window.sessionStorage.getItem(ACTIVE_APPLY_KEY)
    if (!raw) return null
    const request = normalizeActiveApplyRequest(JSON.parse(raw) as unknown)
    if (request) return request
    window.sessionStorage.removeItem(ACTIVE_APPLY_KEY)
    return null
  } catch {
    try {
      window.sessionStorage.removeItem(ACTIVE_APPLY_KEY)
    } catch {
      // Storage may be unavailable; recovery remains best-effort.
    }
    return null
  }
}

export function setActiveApplyRequest(value: ActiveApplyRequest | null): void {
  try {
    const request = normalizeActiveApplyRequest(value)
    if (request) window.sessionStorage.setItem(ACTIVE_APPLY_KEY, JSON.stringify(request))
    else window.sessionStorage.removeItem(ACTIVE_APPLY_KEY)
  } catch {
    // Storage restrictions must not block an already-authorized move.
  }
}

export class ApiError extends Error {
  readonly status: number
  readonly code: string

  constructor(status: number, code: string, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }
}

export function isUnauthorized(error: unknown): boolean {
  return error instanceof ApiError && (error.status === 401 || error.code === 'unauthorized')
}

const CODE_MESSAGES: Record<string, string> = {
  unauthorized: '登录状态已失效，请重新登录后重试。',
  not_ready: '服务尚未就绪，请稍后重试。',
}

/** `undefined` uses the signed-in token; `null` deliberately sends none. */
function requestHeaders(tokenOverride?: string | null): HeadersInit {
  const apiToken = tokenOverride === undefined ? readApiToken() : tokenOverride
  return {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    ...(apiToken ? { Authorization: `Bearer ${apiToken}` } : {}),
  }
}

async function request(
  path: string,
  init?: RequestInit,
  acceptedStatuses: readonly number[] = [],
  tokenOverride?: string | null,
): Promise<unknown> {
  let response: Response
  try {
    response = await fetch(path, { ...init, headers: { ...requestHeaders(tokenOverride), ...init?.headers } })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new ApiError(0, 'network_error', '无法连接到服务，请检查网络后重试。')
  }

  const body = (await response.json().catch(() => null)) as unknown
  if (!response.ok && !acceptedStatuses.includes(response.status)) {
    const record = isRecord(body) ? body : {}
    const detail = isRecord(record.error) ? record.error : isRecord(record.detail) ? record.detail : record
    const code = typeof detail.code === 'string' ? detail.code : `http_${response.status}`
    const message =
      typeof detail.message === 'string'
        ? detail.message
        : typeof detail.detail === 'string'
          ? detail.detail
          : (CODE_MESSAGES[code] ?? (response.status === 401 ? CODE_MESSAGES.unauthorized : '请求未能完成，请稍后重试。'))
    throw new ApiError(response.status, code, message)
  }
  return body
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function looksLikePlan(value: unknown): value is FillActorPlan {
  return isRecord(value) && typeof value.plan_id === 'string' && Array.isArray(value.videos)
}

function looksLikeJob(value: unknown): value is PlanJob {
  if (!isRecord(value)) return false
  return (
    typeof value.state === 'string' ||
    typeof value.status === 'string' ||
    typeof value.job_id === 'string' ||
    typeof value.id === 'string'
  )
}

function normalizeActorFeedStatus(value: unknown): ActorFeedStatus | null {
  if (!isRecord(value)) return null
  if (!(
    typeof value.actor_id === 'string' &&
    ['queued', 'warming', 'ready', 'failed'].includes(String(value.state)) &&
    typeof value.attempts === 'number' &&
    Number.isFinite(value.attempts) &&
    typeof value.updated_at === 'string' &&
    (value.error_code === null || typeof value.error_code === 'string') &&
    (value.freshrss_add_url === null || typeof value.freshrss_add_url === 'string') &&
    (value.freshrss_url === undefined || value.freshrss_url === null || typeof value.freshrss_url === 'string')
  )) return null
  return {
    actor_id: value.actor_id,
    state: value.state as ActorFeedStatus['state'],
    attempts: value.attempts,
    updated_at: value.updated_at,
    error_code: value.error_code,
    freshrss_add_url: value.freshrss_add_url,
    freshrss_url: value.freshrss_url ?? null,
  }
}

export function normalizePlanEnvelope(value: unknown): PlanEnvelope {
  if (looksLikePlan(value)) return { plan: value, job: null, planId: value.plan_id, feeds: [] }
  if (!isRecord(value)) return { plan: null, job: null, planId: null, feeds: [] }

  const plan = looksLikePlan(value.plan) ? value.plan : null
  const job = looksLikeJob(value.job) ? value.job : looksLikeJob(value) ? value : null
  const feeds = Array.isArray(value.feeds)
    ? value.feeds.flatMap((feed) => {
        const normalized = normalizeActorFeedStatus(feed)
        return normalized ? [normalized] : []
      })
    : []
  const planId =
    plan?.plan_id ??
    (typeof value.plan_id === 'string' ? value.plan_id : null) ??
    (job && typeof job.plan_id === 'string' ? job.plan_id : null) ??
    (job && typeof job.job_id === 'string' ? job.job_id : null) ??
    (job && typeof job.id === 'string' ? job.id : null)
  return { plan, job, planId, feeds }
}

function invalidApplyJobResponse(): never {
  throw new ApiError(0, 'invalid_apply_job_response', '移动任务响应无效，请稍后重试。')
}

function isBoundedString(value: unknown, maxLength: number): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= maxLength
}

function isOptionalTimestamp(value: unknown): value is string | undefined {
  return value === undefined || (
    typeof value === 'string' && value.length <= 64 && Number.isFinite(Date.parse(value))
  )
}

function isOptionalDuration(value: unknown): value is number | null | undefined {
  return value === undefined || value === null || (
    typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
  )
}

function normalizeApplyProgress(value: unknown): JobProgress {
  if (!isRecord(value)) invalidApplyJobResponse()
  if (
    !isBoundedString(value.stage, 64) ||
    typeof value.completed !== 'number' || !Number.isSafeInteger(value.completed) ||
    value.completed < 0 || value.completed > MAX_APPLY_ITEMS ||
    !(
      value.total === null ||
      (typeof value.total === 'number' && Number.isSafeInteger(value.total) && value.total >= 0 && value.total <= MAX_APPLY_ITEMS)
    ) ||
    (typeof value.total === 'number' && value.completed > value.total) ||
    !isBoundedString(value.unit, 32) ||
    !(value.current === undefined || value.current === null || isBoundedString(value.current, 4_096)) ||
    !(
      value.percent === null ||
      (typeof value.percent === 'number' && Number.isFinite(value.percent) && value.percent >= 0 && value.percent <= 100)
    ) ||
    !isOptionalTimestamp(value.stage_started_at) ||
    !isOptionalTimestamp(value.updated_at) ||
    !isOptionalDuration(value.eta_seconds) ||
    !isOptionalDuration(value.elapsed_seconds) ||
    !isOptionalDuration(value.last_progress_seconds)
  ) invalidApplyJobResponse()

  return {
    stage: value.stage,
    completed: value.completed,
    total: value.total,
    unit: value.unit,
    current: value.current ?? null,
    percent: value.percent,
    ...(value.stage_started_at !== undefined ? { stage_started_at: value.stage_started_at } : {}),
    ...(value.updated_at !== undefined ? { updated_at: value.updated_at } : {}),
    ...(value.eta_seconds !== undefined ? { eta_seconds: value.eta_seconds } : {}),
    ...(value.elapsed_seconds !== undefined ? { elapsed_seconds: value.elapsed_seconds } : {}),
    ...(value.last_progress_seconds !== undefined ? { last_progress_seconds: value.last_progress_seconds } : {}),
  }
}

function normalizeApplyJob(value: unknown): PlanJob {
  if (!isRecord(value)) invalidApplyJobResponse()
  if (
    typeof value.job_id !== 'string' || !PLAN_ID.test(value.job_id) ||
    value.operation !== 'apply' ||
    typeof value.state !== 'string' || !APPLY_JOB_STATES.has(value.state as JobState) ||
    !(value.plan_id === null || (typeof value.plan_id === 'string' && PLAN_ID.test(value.plan_id))) ||
    !(value.error_code === undefined || value.error_code === null || isBoundedString(value.error_code, 256)) ||
    !isOptionalTimestamp(value.updated_at)
  ) invalidApplyJobResponse()

  return {
    job_id: value.job_id,
    plan_id: value.plan_id,
    operation: 'apply',
    state: value.state as JobState,
    error_code: value.error_code ?? null,
    ...(value.updated_at !== undefined ? { updated_at: value.updated_at } : {}),
    progress: normalizeApplyProgress(value.progress),
  }
}

function normalizeMoveResult(value: unknown): MoveResult {
  if (!isRecord(value)) invalidApplyJobResponse()
  if (
    !isBoundedString(value.candidate_id, 512) ||
    !isBoundedString(value.video_id, 512) ||
    !isBoundedString(value.file_name, 4_096) ||
    typeof value.state !== 'string' || !MOVE_STATES.has(value.state as MoveState) ||
    !(value.error_code === null || isBoundedString(value.error_code, 256))
  ) invalidApplyJobResponse()
  return {
    candidate_id: value.candidate_id,
    video_id: value.video_id,
    file_name: value.file_name,
    state: value.state as MoveState,
    error_code: value.error_code,
  }
}

function normalizeApplyResult(value: unknown): ApplyResult {
  if (!isRecord(value)) invalidApplyJobResponse()
  if (
    typeof value.plan_id !== 'string' || !PLAN_ID.test(value.plan_id) ||
    !isBoundedString(value.revision, 256) ||
    typeof value.state !== 'string' || !['succeeded', 'partial_failed', 'failed'].includes(value.state) ||
    !Array.isArray(value.results) || value.results.length > MAX_APPLY_ITEMS
  ) invalidApplyJobResponse()
  const results = value.results.map(normalizeMoveResult)
  const candidateIds = results.map((result) => result.candidate_id)
  if (new Set(candidateIds).size !== candidateIds.length) invalidApplyJobResponse()
  return {
    plan_id: value.plan_id,
    revision: value.revision,
    state: value.state as ApplyResult['state'],
    results,
  }
}

interface ApplyEnvelopeExpectation {
  jobId?: string
  planId?: string
  revision?: string
}

export function normalizeApplyJobEnvelope(
  value: unknown,
  expected: ApplyEnvelopeExpectation = {},
): ApplyJobEnvelope {
  if (!isRecord(value) || !Object.hasOwn(value, 'result')) invalidApplyJobResponse()
  const job = normalizeApplyJob(value.job)
  const result = value.result === null ? null : normalizeApplyResult(value.result)
  const state = job.state ?? job.status
  if (
    (expected.jobId !== undefined && job.job_id !== expected.jobId) ||
    (result !== null && job.plan_id !== null && result.plan_id !== job.plan_id) ||
    (expected.planId !== undefined && (job.plan_id ?? result?.plan_id) !== expected.planId) ||
    (expected.revision !== undefined && result !== null && result.revision !== expected.revision) ||
    (result !== null && !['completed', 'partial_failed'].includes(state ?? '')) ||
    (result === null && ['completed', 'partial_failed'].includes(state ?? '')) ||
    (result !== null && (
      job.progress?.total !== result.results.length ||
      job.progress.completed !== result.results.length ||
      job.progress.percent !== 100
    ))
  ) invalidApplyJobResponse()
  return { job, result }
}

function normalizeLegacyApplyResult(value: unknown): ApplyResult {
  if (isRecord(value) && isRecord(value.result)) return normalizeApplyResult(value.result)
  return normalizeApplyResult(value)
}

export async function createPlan(actorIds: string[]): Promise<PlanEnvelope> {
  return normalizePlanEnvelope(
    await request('/api/fill-actor/plans', {
      method: 'POST',
      body: JSON.stringify({ actor_ids: actorIds }),
    }),
  )
}

export async function getPlan(planId: string, signal?: AbortSignal): Promise<PlanEnvelope> {
  return normalizePlanEnvelope(
    await request(`/api/fill-actor/plans/${encodeURIComponent(planId)}`, { cache: 'no-store', signal }),
  )
}

export async function cancelPlan(planId: string, signal?: AbortSignal): Promise<PlanEnvelope> {
  return normalizePlanEnvelope(
    await request(`/api/fill-actor/plans/${encodeURIComponent(planId)}/cancel`, { method: 'POST', signal }),
  )
}

export async function applyCandidates(
  planId: string,
  revision: string,
  candidateIds: string[],
): Promise<ApplyResult> {
  return normalizeLegacyApplyResult(
    await request(`/api/fill-actor/plans/${encodeURIComponent(planId)}/apply`, {
      method: 'POST',
      body: JSON.stringify({ revision, candidate_ids: candidateIds }),
    }),
  )
}

export async function startApplyJob(
  planId: string,
  revision: string,
  candidateIds: string[],
  requestId: string,
  signal?: AbortSignal,
): Promise<ApplyJobEnvelope> {
  return normalizeApplyJobEnvelope(
    await request(`/api/fill-actor/plans/${encodeURIComponent(planId)}/apply-jobs`, {
      method: 'POST',
      body: JSON.stringify({ revision, candidate_ids: candidateIds, request_id: requestId }),
      signal,
    }),
    { jobId: requestId, planId, revision },
  )
}

export async function getApplyJob(jobId: string, signal?: AbortSignal): Promise<ApplyJobEnvelope> {
  return normalizeApplyJobEnvelope(
    await request(`/api/fill-actor/apply-jobs/${encodeURIComponent(jobId)}`, { cache: 'no-store', signal }),
    { jobId },
  )
}

export interface HealthStatus {
  status: string
  database?: boolean | string
  roots?: boolean | string | Record<string, string | boolean>
  cloud?: boolean
  legacy_journal?: boolean
  apply_enabled?: boolean
  apply_ready?: boolean
  auth_required?: boolean
}

export async function getHealth(): Promise<HealthStatus> {
  const value = await request('/api/health', { cache: 'no-store' }, [503])
  if (!isRecord(value) || typeof value.status !== 'string') {
    throw new ApiError(0, 'invalid_health_response', '服务健康响应无效，请稍后重试。')
  }
  const health = value as unknown as HealthStatus
  if (typeof health.auth_required === 'boolean') rememberAuthRequired(health.auth_required)
  return health
}

/**
 * Checks a token against the server before it is trusted, so the login screen can
 * reject a wrong token instead of failing on the user's first real action.
 * Resolves for an accepted token (or for a deployment that requires none) and
 * throws `unauthorized` otherwise.
 */
export async function verifyApiToken(token: string | null): Promise<{ authRequired: boolean }> {
  const value = await request('/api/auth/session', { cache: 'no-store' }, [], token)
  if (!isRecord(value) || typeof value.auth_required !== 'boolean') {
    throw new ApiError(0, 'invalid_auth_response', '登录响应无效，请确认服务端版本。')
  }
  rememberAuthRequired(value.auth_required)
  return { authRequired: value.auth_required }
}

// ---------- monitor ----------

export async function getMonitorStatus(signal?: AbortSignal): Promise<PipelineStatus[]> {
  const body = await request('/api/monitor/status', { signal })
  if (!Array.isArray(body)) throw new ApiError(0, 'invalid_response', '监控状态响应无效。')
  return body as PipelineStatus[]
}

export async function triggerPipeline(pipeline: PipelineId, options?: { rank?: boolean }): Promise<string> {
  const body = await request(`/api/monitor/${pipeline}/trigger`, {
    method: 'POST',
    body: JSON.stringify({ rank: Boolean(options?.rank) }),
  })
  if (!isRecord(body) || typeof body.run_id !== 'string') {
    throw new ApiError(0, 'invalid_response', '触发响应无效。')
  }
  return body.run_id
}

export async function cancelPipeline(pipeline: PipelineId): Promise<void> {
  await request(`/api/monitor/${pipeline}/cancel`, { method: 'POST' })
}

export async function listRuns(
  pipeline: PipelineId | null,
  limit = 20,
  signal?: AbortSignal,
): Promise<RunSummary[]> {
  const params = new URLSearchParams()
  if (pipeline) params.set('pipeline', pipeline)
  params.set('limit', String(limit))
  const body = await request(`/api/monitor/runs?${params.toString()}`, { signal })
  if (!Array.isArray(body)) throw new ApiError(0, 'invalid_response', '运行历史响应无效。')
  return body as RunSummary[]
}

export async function getRun(runId: string, signal?: AbortSignal): Promise<RunDetail> {
  const body = await request(`/api/monitor/runs/${encodeURIComponent(runId)}`, { signal })
  if (!isRecord(body) || typeof body.run_id !== 'string') {
    throw new ApiError(0, 'invalid_response', '运行详情响应无效。')
  }
  return body as unknown as RunDetail
}

// ---------- config ----------

function looksLikeConfigSection(value: unknown): value is ConfigSection {
  return isRecord(value) && typeof value.section === 'string' && isRecord(value.values) && typeof value.version === 'number'
}

export async function getConfigSections(signal?: AbortSignal): Promise<ConfigSection[]> {
  const body = await request('/api/config', { signal })
  if (!Array.isArray(body) || !body.every(looksLikeConfigSection)) {
    throw new ApiError(0, 'invalid_response', '配置响应无效。')
  }
  return body
}

export async function updateConfigSection(
  section: string,
  values: Record<string, unknown>,
  version: number,
): Promise<ConfigSection> {
  const body = await request(`/api/config/${encodeURIComponent(section)}`, {
    method: 'PUT',
    body: JSON.stringify({ values, version }),
  })
  if (!looksLikeConfigSection(body)) throw new ApiError(0, 'invalid_response', '配置响应无效。')
  return body
}

export async function testConnection(
  target: 'clouddrive' | 'freshrss',
  values: Record<string, unknown>,
): Promise<TestConnectionResult> {
  const body = await request(`/api/config/${target}/test`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  if (!isRecord(body) || typeof body.ok !== 'boolean') {
    throw new ApiError(0, 'invalid_response', '测试连接响应无效。')
  }
  return { ok: body.ok, detail: typeof body.detail === 'string' ? body.detail : '' }
}
