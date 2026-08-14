import { Fragment, useCallback, useEffect, useState } from 'react'

import {
  ApiError,
  actOnAcquisition,
  addAcquisitionMagnet,
  getAcquisition,
  getTrackerStatus,
  isUnauthorized,
  listAcquisitions,
} from '../api'
import { Notice } from './Feedback'
import { Spinner } from './Icons'
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

function AttemptRow({ attempt }: { attempt: MagnetAttempt }) {
  return (
    <tr>
      <td>#{attempt.attempt_no}</td>
      <td>{attempt.magnet_source}</td>
      <td>
        <span className={`run-state ${ATTEMPT_TONES[attempt.state] ?? ''}`}>
          {ATTEMPT_LABELS[attempt.state] ?? attempt.state}
        </span>
      </td>
      <td>
        <ProgressBar value={attempt.progress} />
      </td>
      <td className="acq-muted">{attempt.error ?? '—'}</td>
      <td className="acq-muted">{formatTime(attempt.updated_at)}</td>
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
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [magnet, setMagnet] = useState('')

  const refresh = useCallback(
    async (signal?: AbortSignal) => {
      try {
        const [nextPage, nextTracker] = await Promise.all([
          listAcquisitions(filter, 50, signal),
          getTrackerStatus(signal),
        ])
        setPage(nextPage)
        setTracker(nextTracker)
        setError(null)
      } catch (err) {
        if (signal?.aborted) return
        if (isUnauthorized(err)) {
          onUnauthorized()
          return
        }
        setError(err instanceof ApiError ? err.message : '加载下载追踪失败。')
      } finally {
        setLoading(false)
      }
    },
    [filter, onUnauthorized],
  )

  useEffect(() => {
    const controller = new AbortController()
    setLoading(true)
    void refresh(controller.signal)
    const timer = window.setInterval(() => void refresh(controller.signal), 10_000)
    return () => {
      controller.abort()
      window.clearInterval(timer)
    }
  }, [refresh])

  const act = useCallback(
    async (avid: string, action: 'retry' | 'ignore' | 'resume') => {
      setBusy(avid)
      setError(null)
      try {
        await actOnAcquisition(avid, action)
        await refresh()
        if (detail?.avid === avid) setDetail(await getAcquisition(avid))
      } catch (err) {
        if (isUnauthorized(err)) {
          onUnauthorized()
          return
        }
        setError(err instanceof ApiError ? err.message : '操作失败。')
      } finally {
        setBusy(null)
      }
    },
    [detail, onUnauthorized, refresh],
  )

  const submitMagnet = useCallback(async () => {
    if (!detail || !magnet.trim()) return
    setBusy(detail.avid)
    setError(null)
    try {
      await addAcquisitionMagnet(detail.avid, magnet.trim())
      setMagnet('')
      await refresh()
      setDetail(await getAcquisition(detail.avid))
    } catch (err) {
      if (isUnauthorized(err)) {
        onUnauthorized()
        return
      }
      setError(err instanceof ApiError ? err.message : '提交磁力失败。')
    } finally {
      setBusy(null)
    }
  }, [detail, magnet, onUnauthorized, refresh])

  const openDetail = useCallback(
    async (avid: string) => {
      if (detail?.avid === avid) {
        setDetail(null)
        return
      }
      try {
        setDetail(await getAcquisition(avid))
      } catch (err) {
        if (isUnauthorized(err)) onUnauthorized()
      }
    },
    [detail, onUnauthorized],
  )

  return (
    <section className="panel dashboard-panel" aria-labelledby="acquisitions-title">
      <div className="panel-heading">
        <h2 id="acquisitions-title">下载追踪</h2>
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
          body={tracker.reason ?? 'CloudDrive 或归档路由尚未配置。'}
        />
      )}
      {tracker?.last_error && (
        <Notice tone="error" title="上次轮询出错" body={tracker.last_error} />
      )}
      {error && <Notice tone="error" title="下载追踪请求失败" body={error} />}

      <p className="acq-meta">
        番号从发现到入库的全程跟踪；下载出错、长期无进度或内容无效时自动换下一个磁力。
        {tracker?.last_polled_at ? ` 上次轮询：${formatTime(tracker.last_polled_at)}` : ''}
      </p>

      {loading ? (
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
                <th>说明</th>
                <th>更新时间</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {page.items.map((item) => (
                <Fragment key={item.avid}>
                  <tr onClick={() => void openDetail(item.avid)}>
                    <td>{item.avid}</td>
                    <td>
                      <span className={`run-state ${STATE_TONES[item.state] ?? ''}`}>
                        {STATE_LABELS[item.state]}
                      </span>
                    </td>
                    <td className="acq-muted">{item.note ?? '—'}</td>
                    <td className="acq-muted">{formatTime(item.updated_at)}</td>
                    <td className="acq-actions" onClick={(event) => event.stopPropagation()}>
                      {item.state === 'needs_attention' && (
                        <button
                          type="button"
                          className="text-button"
                          disabled={busy === item.avid}
                          onClick={() => void act(item.avid, 'resume')}
                        >
                          已处理，继续
                        </button>
                      )}
                      {item.state !== 'archived' && item.state !== 'ignored' && (
                        <>
                          <button
                            type="button"
                            className="text-button"
                            disabled={busy === item.avid}
                            onClick={() => void act(item.avid, 'retry')}
                          >
                            换下一个磁力
                          </button>
                          <button
                            type="button"
                            className="text-button"
                            disabled={busy === item.avid}
                            onClick={() => void act(item.avid, 'ignore')}
                          >
                            忽略
                          </button>
                        </>
                      )}
                    </td>
                  </tr>
                  {detail?.avid === item.avid && (
                    <tr className="acq-detail-row">
                      <td colSpan={5}>
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
                            <p className="acq-muted">
                              已入库：{detail.archived_paths.join('、')}
                            </p>
                          )}
                          {detail.state !== 'archived' && detail.state !== 'ignored' && (
                            <div className="acq-magnet">
                              <input
                                type="text"
                                value={magnet}
                                placeholder="magnet:?xt=urn:btih:..."
                                onChange={(event) => setMagnet(event.target.value)}
                              />
                              <button
                                type="button"
                                className="button primary"
                                disabled={busy === detail.avid || !magnet.trim()}
                                onClick={() => void submitMagnet()}
                              >
                                手动提交磁力
                              </button>
                            </div>
                          )}
                        </div>
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}
