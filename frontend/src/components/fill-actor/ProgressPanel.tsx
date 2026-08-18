import {
  durationText,
  jobState,
  lastProgressAge,
  progressCount,
  progressValue,
  publicFileName,
  remainingEta,
  secondsSince,
  stageElapsed,
} from '../../lib/fill-actor/format'
import {
  BUSINESS_PROGRESS_WARNING_SECONDS,
  HEARTBEAT_WARNING_SECONDS,
  STAGE_LABELS,
} from '../../lib/fill-actor/labels'
import type { PlanJob } from '../../types'
import { CheckIcon, Spinner } from '../Icons'

function panelTitle(kind: 'scan' | 'apply', job: PlanJob | null, submitting: boolean) {
  const state = jobState(job)
  const stage = job?.progress?.stage ?? null
  if (kind === 'apply') {
    if (submitting) return '正在提交移动任务'
    if (state === 'queued') return '移动任务已排队'
    if (state === 'running') return '正在移入文件'
    if (state === 'completed' || state === 'partial_failed') return '文件移动已完成'
    return '文件移动未完成'
  }
  if (submitting) return '正在检查 FreshRSS'
  if (state === 'queued') return '任务已排队'
  if (stage) return STAGE_LABELS[stage] ?? '正在处理扫描任务'
  return '正在恢复扫描状态'
}

export function ProgressPanel({
  kind,
  job,
  planId,
  now,
  pollWarning,
  submitting,
  pending,
  cancelling,
  onCancel,
}: {
  kind: 'scan' | 'apply'
  job: PlanJob | null
  planId?: string | null
  now: number
  pollWarning: string | null
  submitting: boolean
  pending: boolean
  cancelling?: boolean
  onCancel?: () => void
}) {
  const progress = job?.progress
  const value = progressValue(progress)
  const state = jobState(job)
  const title = panelTitle(kind, job, submitting)
  const count = progressCount(progress, kind)
  const elapsed = stageElapsed(progress, now, pending)
  const eta = remainingEta(progress)
  const progressAge = lastProgressAge(progress, now)
  const heartbeatAge = secondsSince(job?.updated_at, now)
  const progressWarning =
    state === 'running' && progressAge !== null && progressAge >= BUSINESS_PROGRESS_WARNING_SECONDS
  const heartbeatWarning =
    state === 'running' && heartbeatAge !== null && heartbeatAge >= HEARTBEAT_WARNING_SECONDS
  const valueText = count ?? (value === null ? '进度计算中' : `${Math.round(value)}%`)
  const canCancel = kind === 'scan' && Boolean(planId && (state === 'queued' || state === 'running'))
  const titleId = `${kind}-progress-title`
  const current = progress?.current
    ? kind === 'apply'
      ? publicFileName(progress.current)
      : progress.current
    : null

  return (
    <section className="progress-panel" aria-busy={pending} aria-labelledby={titleId}>
      <div className="progress-head">
        <span className="progress-orbit">{pending ? <Spinner /> : <CheckIcon />}</span>
        <div className="progress-copy" role="status" aria-live="polite" aria-atomic="true">
          <strong id={titleId}>{title}</strong>
          <span>
            {current ? `当前：${current}` : pending ? '正在等待最新进度…' : '所有文件均已处理。'}
          </span>
        </div>
        {canCancel && onCancel && (
          <button
            className="button secondary cancel-scan-button"
            type="button"
            disabled={Boolean(cancelling)}
            onClick={onCancel}
          >
            {cancelling && <Spinner />}
            {cancelling ? '正在取消' : '取消扫描'}
          </button>
        )}
      </div>

      <div className="progress-meter">
        <div
          className="progress-track"
          role="progressbar"
          aria-label={`${title}进度`}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={value === null ? undefined : Math.round(value)}
          aria-valuetext={valueText}
        >
          <span
            className={value === null ? 'indeterminate' : ''}
            style={value === null ? undefined : { width: `${value}%` }}
          />
        </div>
        <b>{value === null ? '计算中' : `${Math.round(value)}%`}</b>
      </div>

      <dl className="progress-meta">
        <div>
          <dt>阶段进度</dt>
          <dd>{count ?? '等待统计'}</dd>
        </div>
        <div>
          <dt>阶段已用时</dt>
          <dd>{elapsed === null ? '等待统计' : durationText(elapsed)}</dd>
        </div>
        <div>
          <dt>当前阶段 ETA</dt>
          <dd>{eta === null ? '计算中' : `约 ${durationText(eta)}`}</dd>
        </div>
        <div>
          <dt>最后进展</dt>
          <dd>{progressAge === null ? '等待首个结果' : `${durationText(progressAge)}前`}</dd>
        </div>
      </dl>

      {(pollWarning || progressWarning || heartbeatWarning) && (
        <div className="progress-warnings" role="status" aria-live="polite">
          {pollWarning && <p>{pollWarning}</p>}
          {progressWarning && <p>较长时间无新结果，仍可能在等待外部服务。</p>}
          {heartbeatWarning && <p>执行器心跳已较长时间未更新，执行可能已经中断。</p>}
        </div>
      )}
    </section>
  )
}
