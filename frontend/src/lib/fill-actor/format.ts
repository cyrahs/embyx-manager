import { ApiError } from '../../api'
import type {
  ActiveApplyRequest,
  ApplyJobEnvelope,
  FillActorPlan,
  JobProgress,
  JobState,
  MoveCandidate,
  PlanJob,
} from '../../types'
import { errorMessage as sharedErrorMessage } from '../errors'
import { ACTOR_ID, UNIT_LABELS } from './labels'

export function parseActorIds(value: string) {
  const values = value
    .split(/[\s,，;；]+/)
    .map((item) => item.trim())
    .filter(Boolean)
  const actorIds = [...new Set(values)]
  const invalid = actorIds.filter((item) => !ACTOR_ID.test(item))
  return { actorIds, invalid, duplicateCount: values.length - actorIds.length }
}

export function jobState(job: PlanJob | null): JobState | null {
  return job?.state ?? job?.status ?? null
}

export function isJobPending(job: PlanJob | null) {
  const state = jobState(job)
  return state === 'queued' || state === 'running'
}

export function isJobCancelled(job: PlanJob | null) {
  return jobState(job) === 'failed' && job?.error_code === 'job_cancelled'
}

export function candidateMap(plan: FillActorPlan | null) {
  const map = new Map<string, MoveCandidate>()
  plan?.videos.forEach((video) =>
    video.move_candidates.forEach((candidate) => map.set(candidate.candidate_id, candidate)),
  )
  return map
}

export function safeMagnet(magnet: string | null): string | null {
  if (!magnet || !magnet.toLowerCase().startsWith('magnet:?')) return null
  return [...magnet].every((character) => {
    const code = character.charCodeAt(0)
    return code > 31 && code !== 127
  })
    ? magnet
    : null
}

export function safeFreshRssUrl(value: string | null): string | null {
  if (!value) return null
  try {
    const url = new URL(value)
    return url.protocol === 'https:' || url.protocol === 'http:' ? value : null
  } catch {
    return null
  }
}

export function planMagnets(plan: FillActorPlan | null): string[] {
  const seen = new Set<string>()
  const magnets: string[] = []
  plan?.videos.forEach((video) => {
    const magnet = safeMagnet(video.magnet)
    if (magnet && !seen.has(magnet)) {
      seen.add(magnet)
      magnets.push(magnet)
    }
  })
  return magnets
}

/** Fill Actor's own error codes, layered over the app-wide fallback. */
export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    const messages: Record<string, string> = {
      move_disabled: '文件移动当前由管理员暂停。',
      legacy_plan_requires_rescan: '该计划来自旧版本，请重新扫描后再操作。',
      not_ready: '移动依赖尚未就绪，请稍后重试。',
    }
    const message = messages[error.code]
    if (message) return message
  }
  return sharedErrorMessage(error)
}

export function progressValue(progress?: JobProgress | null): number | null {
  if (!progress) return null
  if (typeof progress.percent === 'number') return Math.max(0, Math.min(100, progress.percent))
  if (typeof progress.completed === 'number' && typeof progress.total === 'number' && progress.total > 0) {
    return Math.round((progress.completed / progress.total) * 100)
  }
  return null
}

export function safeSeconds(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null
}

export function secondsSince(value: string | null | undefined, now: number): number | null {
  if (!value) return null
  const timestamp = Date.parse(value)
  return Number.isFinite(timestamp) ? Math.max(0, Math.floor((now - timestamp) / 1000)) : null
}

export function durationText(rawSeconds: number): string {
  const seconds = Math.max(0, Math.floor(rawSeconds))
  if (seconds < 60) return `${seconds} 秒`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes} 分 ${seconds % 60} 秒`
  const hours = Math.floor(minutes / 60)
  return `${hours} 小时 ${minutes % 60} 分`
}

export function stageElapsed(
  progress: JobProgress | null | undefined,
  now: number,
  pending = true,
): number | null {
  if (!progress) return null
  if (!pending) {
    const elapsed = safeSeconds(progress.elapsed_seconds)
    if (elapsed !== null) return elapsed
    const startedAt = progress.stage_started_at ? Date.parse(progress.stage_started_at) : Number.NaN
    const updatedAt = progress.updated_at ? Date.parse(progress.updated_at) : Number.NaN
    return Number.isFinite(startedAt) && Number.isFinite(updatedAt)
      ? Math.max(0, Math.floor((updatedAt - startedAt) / 1000))
      : null
  }
  const fromStart = secondsSince(progress.stage_started_at, now)
  if (fromStart !== null) return fromStart
  const elapsed = safeSeconds(progress.elapsed_seconds)
  if (elapsed === null) return null
  return elapsed + (secondsSince(progress.updated_at, now) ?? 0)
}

export function remainingEta(progress: JobProgress | null | undefined): number | null {
  return safeSeconds(progress?.eta_seconds)
}

export function lastProgressAge(progress: JobProgress | null | undefined, now: number): number | null {
  if (!progress) return null
  return secondsSince(progress.updated_at, now) ?? safeSeconds(progress.last_progress_seconds)
}

export function progressCount(
  progress: JobProgress | null | undefined,
  kind: 'scan' | 'apply' = 'scan',
): string | null {
  const completed = safeSeconds(progress?.completed)
  if (completed === null) return null
  const count = Math.floor(completed)
  const total = safeSeconds(progress?.total)
  if (kind === 'apply') {
    return total === null ? `已处理 ${count} 个文件` : `${count} / ${Math.floor(total)} 个文件`
  }
  const unit = progress?.unit ? (UNIT_LABELS[progress.unit] ?? progress.unit) : '项'
  return total === null ? `已完成 ${count} ${unit}` : `${count} / ${Math.floor(total)} ${unit}`
}

export function createApplyRequestId(): string {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID()
  const bytes = crypto.getRandomValues(new Uint8Array(16))
  return [...bytes].map((value) => value.toString(16).padStart(2, '0')).join('')
}

export function applyPlaceholder(request: ActiveApplyRequest): PlanJob {
  return {
    job_id: request.jobId ?? request.requestId,
    plan_id: request.planId,
    operation: 'apply',
    state: 'queued',
    progress: {
      stage: 'queued',
      completed: 0,
      total: request.candidateIds.length,
      unit: 'items',
      percent: 0,
      current: null,
    },
  }
}

export function terminalApplyJob(job: PlanJob, candidateCount: number): PlanJob {
  const total = candidateCount
  return {
    ...job,
    state: 'completed',
    progress: {
      ...job.progress,
      completed: total,
      total,
      unit: job.progress?.unit ?? 'items',
      current: null,
      percent: 100,
    },
  }
}

export function publicFileName(value: string): string {
  const parts = value.trim().split(/[\\/]/)
  return parts.at(-1) || '当前文件'
}

export function assertActiveApplyEnvelope(
  envelope: ApplyJobEnvelope,
  request: ActiveApplyRequest,
): void {
  const responsePlanId = envelope.job.plan_id ?? envelope.result?.plan_id ?? null
  if (
    responsePlanId !== request.planId ||
    (envelope.result && envelope.result.revision !== request.revision)
  ) {
    throw new ApiError(0, 'invalid_apply_job_response', '移动任务响应与当前计划不匹配，请稍后重试。')
  }
  if (envelope.result) {
    const expected = new Set(request.candidateIds)
    const actual = new Set(envelope.result.results.map((result) => result.candidate_id))
    if (expected.size !== actual.size || [...expected].some((candidateId) => !actual.has(candidateId))) {
      throw new ApiError(0, 'invalid_apply_job_response', '移动任务结果与已确认文件不匹配，请稍后重试。')
    }
  }
}

const TIME_FORMAT = new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit' })

export function clockText(value: string | number | Date): string {
  return TIME_FORMAT.format(new Date(value))
}

export function formatFeedUpdatedAt(value: string): string {
  const timestamp = Date.parse(value)
  if (!Number.isFinite(timestamp)) return '更新时间未知'
  return `更新于 ${clockText(timestamp)}`
}
