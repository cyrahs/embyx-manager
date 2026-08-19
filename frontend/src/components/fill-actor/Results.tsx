import { useEffect, useState } from 'react'
import { localizeBackendText } from '../../lib/backendText'
import { clockText } from '../../lib/fill-actor/format'
import {
  ACTOR_ERROR_LABELS,
  MOVE_ERROR_LABELS,
  MOVE_LABELS,
  VIDEO_GROUPS,
  videoWarningLabel,
  type VideoGroupDef,
} from '../../lib/fill-actor/labels'
import type { ApplyResult, FillActorPlan, MoveCandidate, VideoPlan } from '../../types'
import { AlertIcon, CheckIcon, ChevronIcon, FileIcon, StatusIcon } from '../Icons'

export function PlanSummary({ plan }: { plan: FillActorPlan }) {
  const actorFailures = plan.actors.filter((actor) => actor.error_code).length
  const counts = Object.fromEntries(
    VIDEO_GROUPS.map(({ state }) => [state, plan.videos.filter((video) => video.state === state).length]),
  )
  return (
    <section className="summary-strip" aria-label="扫描摘要">
      <div>
        <span>演员</span>
        <strong>{plan.actors.length}</strong>
        {actorFailures > 0 && <small>{actorFailures} 个失败</small>}
      </div>
      <div>
        <span>作品</span>
        <strong>{plan.videos.length}</strong>
      </div>
      <div>
        <span>已入库</span>
        <strong>{counts.exists}</strong>
      </div>
      <div>
        <span>可移入</span>
        <strong>{counts.additional_found}</strong>
      </div>
      <div>
        <span>已提交下载</span>
        <strong>{counts.submitted}</strong>
      </div>
      <div>
        <span>计划有效至</span>
        <strong className="expiry">{clockText(plan.expires_at)}</strong>
      </div>
    </section>
  )
}

export function ActorFailures({ plan }: { plan: FillActorPlan }) {
  const failures = plan.actors.filter((actor) => actor.error_code)
  if (failures.length === 0) return null
  return (
    <section className="actor-failures" aria-label="失败演员" role="alert">
      <span className="actor-failures-icon">
        <AlertIcon />
      </span>
      <div>
        <strong>失败演员（{failures.length}）</strong>
        <ul>
          {failures.map((actor) => {
            const code = actor.error_code as string
            // Unknown codes surface verbatim so a newly added backend error is never swallowed.
            const label = ACTOR_ERROR_LABELS[code]
            return (
              <li key={actor.actor_id}>
                <span className="actor-failure-id">{actor.actor_id}</span>
                <span className="actor-failure-label">{label ? `${label}（${code}）` : code}</span>
                {actor.error_message && <span className="actor-failure-detail">{actor.error_message}</span>}
              </li>
            )
          })}
        </ul>
        <p>这些演员的作品未计入本次扫描结果，可稍后重新扫描。</p>
      </div>
    </section>
  )
}

export function VideoGroup({
  group,
  videos,
  selected,
  toggleCandidate,
  applyResult,
  selectionLocked,
  filtered,
  actorNames,
}: {
  group: VideoGroupDef
  videos: VideoPlan[]
  selected: Set<string>
  toggleCandidate: (candidate: MoveCandidate) => void
  applyResult: ApplyResult | null
  selectionLocked: boolean
  filtered: boolean
  /** Actor display names keyed by lower-cased actor ID; missing ones fall back to the ID. */
  actorNames: Record<string, string>
}) {
  const [expanded, setExpanded] = useState(group.defaultExpanded)
  // An active filter reveals matches wherever they are; collapsing again would hide the answer.
  const open = expanded || filtered

  useEffect(() => {
    if (filtered) setExpanded(true)
  }, [filtered])

  return (
    <section className={`video-group tone-${group.tone}`}>
      <button
        className="group-header"
        type="button"
        onClick={() => setExpanded((value) => !value)}
        aria-expanded={open}
      >
        <span className="group-status">
          <StatusIcon state={group.state} />
        </span>
        <span className="group-title">
          <strong>{group.label}</strong>
          <small>{group.description}</small>
        </span>
        <span className="group-count">{videos.length}</span>
        <ChevronIcon expanded={open} />
      </button>
      {open && (
        <div className="video-list">
          {videos.map((video) => (
            <VideoRow
              key={video.video_id}
              video={video}
              selected={selected}
              toggleCandidate={toggleCandidate}
              applyResult={applyResult}
              selectionLocked={selectionLocked}
              actorNames={actorNames}
            />
          ))}
        </div>
      )}
    </section>
  )
}

function VideoRow({
  video,
  selected,
  toggleCandidate,
  applyResult,
  selectionLocked,
  actorNames,
}: {
  video: VideoPlan
  selected: Set<string>
  toggleCandidate: (candidate: MoveCandidate) => void
  applyResult: ApplyResult | null
  selectionLocked: boolean
  actorNames: Record<string, string>
}) {
  const warnings = video.warnings.length ? video.warnings.map(videoWarningLabel).join(' · ') : null
  // The column reads as a name once one is known; the ID stays in the tooltip, which is
  // where it is still wanted — two actors can share a stage name, IDs never collide.
  const actorLabels = video.actor_ids.map((actorId) => actorNames[actorId.toLowerCase()] ?? actorId)
  const actorTitle = video.actor_ids
    .map((actorId) => {
      const name = actorNames[actorId.toLowerCase()]
      return name ? `${name}（${actorId}）` : actorId
    })
    .join(' · ')
  return (
    <article className="video-row">
      <span className="video-id">{video.video_id}</span>
      <div className="video-detail">
        {video.existing_files.map((file) => (
          <span className="file-chip" key={file}>
            <FileIcon />
            <span className="file-chip-name" title={file}>
              {file}
            </span>
          </span>
        ))}
        {video.move_candidates.map((candidate) => {
          const result = applyResult?.results.find((item) => item.candidate_id === candidate.candidate_id)
          return (
            <label
              className={`candidate ${candidate.destination_conflict ? 'conflict' : ''}`}
              key={candidate.candidate_id}
            >
              <input
                type="checkbox"
                checked={selected.has(candidate.candidate_id)}
                disabled={candidate.destination_conflict || selectionLocked}
                onChange={() => toggleCandidate(candidate)}
              />
              <span className="custom-check">
                <CheckIcon />
              </span>
              <span className="candidate-name" title={candidate.file_name}>
                {candidate.file_name}
              </span>
              <span className="candidate-source">{localizeBackendText(candidate.source_label)}</span>
              {candidate.destination_conflict && (
                <span className="warning-pill">
                  <AlertIcon />
                  目标位置已有同名文件
                </span>
              )}
              {result && (
                <span className={`result-pill result-${result.state}`}>
                  {(result.error_code && MOVE_ERROR_LABELS[result.error_code]) ?? MOVE_LABELS[result.state]}
                </span>
              )}
            </label>
          )
        })}
        {warnings && <span className="row-warning">{warnings}</span>}
      </div>
      <span className="video-actors" title={actorTitle}>
        {actorLabels.join(' · ')}
      </span>
    </article>
  )
}
