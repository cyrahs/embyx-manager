import { Fragment, useCallback, useEffect, useRef, useState } from 'react'

import {
  ApiError,
  actOnAcquisition,
  addAcquisitionMagnet,
  getAcquisition,
  getTrackerStatus,
  isUnauthorized,
  listAcquisitions,
} from '../api'
import { localizeBackendText } from '../lib/backendText'
import { Notice } from './Feedback'
import { Spinner } from './Icons'
import { ManualIntakeDialog } from './ManualIntake'
import type {
  Acquisition,
  AcquisitionDetail,
  AcquisitionState,
  MagnetAttempt,
  TrackerStatus,
} from '../types'

const STATE_LABELS: Record<AcquisitionState, string> = {
  discovered: '待解析',
  downloading: '下载中',
  archived: '已入库',
  resolve_failed: '未找到磁力',
  exhausted: '磁力已用尽',
  needs_attention: '待处理',
  ignored: '已忽略',
}

/** Extra run-state pill tone; states missing here render the neutral pill. */
const STATE_TONES: Partial<Record<AcquisitionState, string>> = {
  archived: 'completed',
  downloading: 'running',
  needs_attention: 'failed',
  exhausted: 'failed',
  resolve_failed: 'failed',
}

const ACQUISITION_SOURCE_LABELS: Record<string, string> = {
  rss_actor: '演员订阅',
  rss_rank: '排行订阅',
  manual: '手动',
  reconcile: '目录扫描',
  fill_actor: '补全演员',
}

/** 'rss:<category>' renders as the category; fixed sources get their label. */
function sourceLabel(source: string): string {
  if (source.startsWith('rss:')) return `订阅 ${source.slice(4)}`
  return ACQUISITION_SOURCE_LABELS[source] ?? source
}

const ATTEMPT_LABELS: Record<string, string> = {
  pending: '待用',
  submitted: '已提交',
  downloading: '下载中',
  finished: '已完成',
  archiving: '归档中',
  archived: '已入库',
  junk: '无有效视频',
  error: '离线出错',
  stalled: '长期无进度',
  lost: '任务丢失',
}

const MAGNET_SOURCE_LABELS: Record<string, string> = {
  sukebei: 'Sukebei',
  rss_item: 'RSS 条目',
  javbus: 'JavBus',
  manual: '手动添加',
}

const ATTEMPT_TONES: Record<string, string> = {
  archived: 'completed',
  finished: 'completed',
  downloading: 'running',
  archiving: 'running',
  junk: 'failed',
  error: 'failed',
  stalled: 'failed',
  lost: 'failed',
}

/** Ordered so the states an operator must act on come first. */
const GROUPS: AcquisitionState[] = [
  'needs_attention',
  'exhausted',
  'resolve_failed',
  'downloading',
  'discovered',
  'archived',
  'ignored',
]

const REFRESH_INTERVAL_MS = 10_000

type AcquisitionAction = 'retry' | 'ignore' | 'resume'

function formatTime(value: string | null): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return date.toLocaleString('zh-CN', { hour12: false })
}

function ProgressBar({ value }: { value: number | null }) {
  if (value === null) return <span className="acq-muted">—</span>
  return (
    <span className="acq-progress" title={`${value.toFixed(1)}%`}>
      <span
        className="acq-progress-fill"
        style={{ width: `${Math.min(100, Math.max(0, value))}%` }}
      />
      <span className="acq-progress-text">{value.toFixed(0)}%</span>
    </span>
  )
}

function AcquisitionRow({
  item,
  busy,
  onOpen,
  onAct,
}: {
  item: Acquisition
  busy: boolean
  onOpen: (avid: string) => void
  onAct: (avid: string, action: AcquisitionAction) => void
}) {
  const closed = item.state === 'archived' || item.state === 'ignored'
  return (
    <tr onClick={() => onOpen(item.avid)}>
      <td>{item.avid}</td>
      <td>
        <span className={`run-state ${STATE_TONES[item.state] ?? ''}`}>
          {STATE_LABELS[item.state]}
        </span>
      </td>
      <td className="acq-muted">{sourceLabel(item.source)}</td>
      <td className="acq-muted">{item.note ? localizeBackendText(item.note) : '—'}</td>
      <td className="acq-muted">{formatTime(item.updated_at)}</td>
      <td onClick={(event) => event.stopPropagation()}>
        <div className="acq-actions">
          {item.state === 'needs_attention' && (
            <button type="button" className="text-button" disabled={busy} onClick={() => onAct(item.avid, 'resume')}>
              已处理，继续
            </button>
          )}
          {!closed && (
            <>
              <button type="button" className="text-button" disabled={busy} onClick={() => onAct(item.avid, 'retry')}>
                换下一个磁力
              </button>
              <button type="button" className="text-button" disabled={busy} onClick={() => onAct(item.avid, 'ignore')}>
                忽略
              </button>
            </>
          )}
        </div>
      </td>
    </tr>
  )
}

function AttemptRow({ attempt }: { attempt: MagnetAttempt }) {
  return (
    <tr>
      <td>#{attempt.attempt_no}</td>
      <td>{MAGNET_SOURCE_LABELS[attempt.magnet_source] ?? attempt.magnet_source}</td>
      <td>
        <span className={`run-state ${ATTEMPT_TONES[attempt.state] ?? ''}`}>
          {ATTEMPT_LABELS[attempt.state] ?? attempt.state}
        </span>
      </td>
      <td>
        <ProgressBar value={attempt.progress} />
      </td>
      <td className="acq-muted">{attempt.error ? localizeBackendText(attempt.error) : '—'}</td>
      <td className="acq-muted">{formatTime(attempt.updated_at)}</td>
    </tr>
  )
}

function DetailRow({
  detail,
  busy,
  magnet,
  onMagnetChange,
  onSubmitMagnet,
}: {
  detail: AcquisitionDetail
  busy: boolean
  magnet: string
  onMagnetChange: (value: string) => void
  onSubmitMagnet: () => void
}) {
  return (
    <tr className="acq-detail-row">
      <td colSpan={6}>
        <div className="acq-detail">
          <div className="run-table-wrap">
            <table className="run-table">
              <thead>
                <tr>
                  <th>尝试</th>
                  <th>来源</th>
                  <th>状态</th>
                  <th>进度</th>
                  <th>错误</th>
                  <th>更新</th>
                </tr>
              </thead>
              <tbody>
                {detail.attempts.map((attempt) => (
                  <AttemptRow key={attempt.attempt_no} attempt={attempt} />
                ))}
              </tbody>
            </table>
          </div>
          {detail.archived_paths.length > 0 && (
            <p className="acq-muted">已入库：{detail.archived_paths.join('、')}</p>
          )}
          {detail.state !== 'archived' && detail.state !== 'ignored' && (
            <div className="acq-magnet">
              <input
                type="text"
                value={magnet}
                placeholder="magnet:?xt=urn:btih:..."
                onChange={(event) => onMagnetChange(event.target.value)}
              />
              <button
                type="button"
                className="button primary"
                disabled={busy || !magnet.trim()}
                onClick={onSubmitMagnet}
              >
                手动提交磁力
              </button>
            </div>
          )}
        </div>
      </td>
    </tr>
  )
}

export function AcquisitionPanel({ onUnauthorized }: { onUnauthorized: () => void }) {
  const [page, setPage] = useState<{
    items: Acquisition[]
    counts: Partial<Record<AcquisitionState, number>>
  }>({
    items: [],
    counts: {},
  })
  const [tracker, setTracker] = useState<TrackerStatus | null>(null)
  const [filter, setFilter] = useState<AcquisitionState | null>('needs_attention')
  const [detail, setDetail] = useState<AcquisitionDetail | null>(null)
  const [loaded, setLoaded] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [magnet, setMagnet] = useState('')
  const [manualOpen, setManualOpen] = useState(false)
  const [tick, setTick] = useState(0)

  // Kept in a ref so the polling effect never restarts because a parent
  // re-render handed down a new callback identity: restarting it resets the
  // panel to its loading state, which reads as flicker.
  const onUnauthorizedRef = useRef(onUnauthorized)
  useEffect(() => {
    onUnauthorizedRef.current = onUnauthorized
  })

  const reload = useCallback(() => setTick((value) => value + 1), [])

  useEffect(() => {
    const controller = new AbortController()
    const load = async () => {
      try {
        const [nextPage, nextTracker] = await Promise.all([
          listAcquisitions(filter, 50, controller.signal),
          getTrackerStatus(controller.signal),
        ])
        setPage(nextPage)
        setTracker(nextTracker)
        setError(null)
      } catch (err) {
        if (controller.signal.aborted) return
        if (isUnauthorized(err)) {
          onUnauthorizedRef.current()
          return
        }
        setError(err instanceof ApiError ? err.message : '加载下载追踪失败。')
      } finally {
        if (!controller.signal.aborted) setLoaded(true)
      }
    }
    void load()
    const timer = window.setInterval(() => void load(), REFRESH_INTERVAL_MS)
    return () => {
      controller.abort()
      window.clearInterval(timer)
    }
  }, [filter, tick])

  const act = useCallback(
    async (avid: string, action: AcquisitionAction) => {
      setBusy(avid)
      setError(null)
      try {
        await actOnAcquisition(avid, action)
        if (detail?.avid === avid) setDetail(await getAcquisition(avid))
        reload()
      } catch (err) {
        if (isUnauthorized(err)) {
          onUnauthorizedRef.current()
          return
        }
        setError(err instanceof ApiError ? err.message : '操作失败。')
      } finally {
        setBusy(null)
      }
    },
    [detail, reload],
  )

  const submitMagnet = useCallback(async () => {
    if (!detail || !magnet.trim()) return
    setBusy(detail.avid)
    setError(null)
    try {
      await addAcquisitionMagnet(detail.avid, magnet.trim())
      setMagnet('')
      setDetail(await getAcquisition(detail.avid))
      reload()
    } catch (err) {
      if (isUnauthorized(err)) {
        onUnauthorizedRef.current()
        return
      }
      setError(err instanceof ApiError ? err.message : '提交磁力失败。')
    } finally {
      setBusy(null)
    }
  }, [detail, magnet, reload])

  const openDetail = useCallback(
    async (avid: string) => {
      if (detail?.avid === avid) {
        setDetail(null)
        return
      }
      try {
        setDetail(await getAcquisition(avid))
      } catch (err) {
        if (isUnauthorized(err)) onUnauthorizedRef.current()
      }
    },
    [detail],
  )

  return (
    <section className="panel dashboard-panel" aria-labelledby="acquisitions-title">
      <div className="panel-heading">
        <h2 id="acquisitions-title">下载追踪</h2>
        <button type="button" className="button secondary manual-open" onClick={() => setManualOpen(true)}>
          手动添加
        </button>
        <div className="run-filter" role="group" aria-label="筛选状态">
          <button
            type="button"
            className={`text-button${filter === null ? ' active' : ''}`}
            onClick={() => setFilter(null)}
          >
            全部
          </button>
          {GROUPS.map((state) => (
            <button
              key={state}
              type="button"
              className={`text-button${filter === state ? ' active' : ''}`}
              onClick={() => setFilter(state)}
            >
              {STATE_LABELS[state]}
              {page.counts[state] ? ` ${page.counts[state]}` : ''}
            </button>
          ))}
        </div>
      </div>

      {tracker && !tracker.running && (
        <Notice
          tone="warning"
          title="下载追踪未运行"
          body={tracker.reason ? localizeBackendText(tracker.reason) : 'CloudDrive 或归档路由尚未配置。'}
        />
      )}
      {tracker?.last_error && (
        <Notice tone="error" title="上次轮询出错" body={localizeBackendText(tracker.last_error)} />
      )}
      {error && <Notice tone="error" title="下载追踪请求失败" body={error} />}

      <p className="acq-meta">
        番号从发现到入库的全程跟踪；下载出错、长期无进度或内容无效时自动换下一个磁力。
        {tracker?.last_polled_at ? ` 上次轮询：${formatTime(tracker.last_polled_at)}` : ''}
      </p>

      {!loaded ? (
        <p className="dashboard-loading">
          <Spinner /> 正在加载…
        </p>
      ) : page.items.length === 0 ? (
        <p className="results-empty">没有符合条件的番号。</p>
      ) : (
        <div className="run-table-wrap">
          <table className="run-table">
            <thead>
              <tr>
                <th>番号</th>
                <th>状态</th>
                <th>来源</th>
                <th>说明</th>
                <th>更新时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {page.items.map((item) => (
                <Fragment key={item.avid}>
                  <AcquisitionRow
                    item={item}
                    busy={busy === item.avid}
                    onOpen={(avid) => void openDetail(avid)}
                    onAct={(avid, action) => void act(avid, action)}
                  />
                  {detail?.avid === item.avid && (
                    <DetailRow
                      detail={detail}
                      busy={busy === detail.avid}
                      magnet={magnet}
                      onMagnetChange={setMagnet}
                      onSubmitMagnet={() => void submitMagnet()}
                    />
                  )}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {manualOpen && (
        <ManualIntakeDialog
          onClose={() => setManualOpen(false)}
          onSubmitted={reload}
          onUnauthorized={() => onUnauthorizedRef.current()}
        />
      )}
    </section>
  )
}
