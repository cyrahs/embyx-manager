import { useCallback, useEffect, useMemo, useState } from 'react'
import { useOutletContext } from 'react-router-dom'

import {
  ApiError,
  cancelPipeline,
  getConfigSections,
  getMonitorStatus,
  getRun,
  isUnauthorized,
  listRuns,
  triggerPipeline,
  updateConfigSection,
} from '../api'
import type { AppContext } from '../App'
import { AcquisitionPanel } from '../components/AcquisitionPanel'
import { Notice } from '../components/Feedback'
import { ChevronIcon, Spinner } from '../components/Icons'
import { localizeBackendText } from '../lib/backendText'
import type { PipelineId, PipelineStatus, RunDetail, RunSummary } from '../types'

const PIPELINE_LABELS: Record<PipelineId, string> = {
  rss: 'RSS 磁力摄取',
  archive: '归档整理',
  mapping: 'STRM 映射',
}

const PIPELINE_DESCRIPTIONS: Record<PipelineId, string> = {
  rss: '订阅源新条目 → 磁力解析 → 115 离线任务',
  archive: '下载目录整理、重命名并按厂牌归档入库',
  mapping: '.strm 平铺结构镜像为每部一目录（含实时增量）',
}

const STATE_LABELS: Record<string, string> = {
  running: '运行中',
  completed: '已完成',
  failed: '失败',
  cancelled: '已取消',
}

const TRIGGER_LABELS: Record<string, string> = {
  scheduled: '定时',
  manual: '手动',
  watchdog: '增量',
  startup: '启动',
}

const STAT_LABELS: Record<string, string> = {
  items: 'RSS 条目',
  new_items: '新条目',
  subscriptions_polled: '订阅拉取',
  subscriptions_failed: '订阅拉取失败',
  subscriptions_seeded: '订阅初始化',
  unique_avids: '番号',
  categories_failed: '分类拉取失败',
  skipped_known: '已跟踪跳过',
  skipped_cooldown: '冷却跳过',
  already_in_library: '库内已有',
  magnets_found: '磁力命中',
  magnets_failed: '磁力失败',
  magnets_added: '已提交离线',
  candidates_recorded: '记录候选磁力',
  duplicates: '重复任务',
  add_failed: '提交失败',
  items_marked_read: '标记已读',
  finished_refreshed: '完成刷新',
  dirnames_cleared: '目录名修正',
  folders_flattened: '拍平文件夹',
  junk_folders_deleted: '清理垃圾夹',
  duplicate_copies_deleted: '删除重复副本',
  videos_renamed: '重命名',
  videos_archived: '归档入库',
  duplicates_promoted: '提级归档',
  skipped_priority: '优先库已有',
  items_failed: '处理失败',
  reconciled: '核对入库',
  folders_discarded: '清理源文件夹',
  needs_attention: '待处理',
  files_updated: '更新',
  files_skipped: '跳过',
  files_deleted: '删除',
  dirs_deleted: '清理目录',
  offline_tasks: '离线任务',
  duplicate_offline_tasks: '重复离线任务',
  archived: '已入库',
  retried: '换磁力重试',
  exhausted: '磁力用尽',
  junk: '无有效视频',
  error: '离线出错',
  stalled: '长期无进度',
  lost: '任务丢失',
}

function formatTime(value: string | null): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return date.toLocaleString('zh-CN', { hour12: false })
}

function formatDuration(run: RunSummary): string {
  if (!run.finished_at) return '进行中'
  const ms = new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()
  if (!Number.isFinite(ms) || ms < 0) return '—'
  if (ms < 1000) return '<1 秒'
  const seconds = Math.round(ms / 1000)
  if (seconds < 60) return `${seconds} 秒`
  return `${Math.floor(seconds / 60)} 分 ${seconds % 60} 秒`
}

function statChips(stats: Record<string, number>): Array<[string, number]> {
  return Object.entries(stats)
    .filter(([, value]) => value !== 0)
    .map(([key, value]) => [STAT_LABELS[key] ?? key, value])
}

export default function DashboardPage() {
  const { requestApiToken } = useOutletContext<AppContext>()
  const [statuses, setStatuses] = useState<PipelineStatus[] | null>(null)
  const [runs, setRuns] = useState<RunSummary[]>([])
  const [runFilter, setRunFilter] = useState<PipelineId | 'all'>('all')
  const [selectedRun, setSelectedRun] = useState<RunDetail | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [authRequired, setAuthRequired] = useState(false)
  const [busyAction, setBusyAction] = useState<string | null>(null)
  const [sectionVersions, setSectionVersions] = useState<Record<string, { enabled: boolean; version: number }>>({})
  const [refreshTick, setRefreshTick] = useState(0)

  const anyRunning = useMemo(
    () => Boolean(statuses?.some((status) => status.running_run_id)),
    [statuses],
  )

  const refresh = useCallback(async (signal?: AbortSignal) => {
    const [statusList, runList, sections] = await Promise.all([
      getMonitorStatus(signal),
      listRuns(runFilter === 'all' ? null : runFilter, 20, signal),
      getConfigSections(signal),
    ])
    setStatuses(statusList)
    setRuns(runList)
    const versions: Record<string, { enabled: boolean; version: number }> = {}
    for (const section of sections) {
      if (['rss', 'archive', 'mapping'].includes(section.section)) {
        versions[section.section] = {
          enabled: Boolean(section.values.enabled),
          version: section.version,
        }
      }
    }
    setSectionVersions(versions)
  }, [runFilter])

  useEffect(() => {
    const controller = new AbortController()
    let mounted = true
    const tick = () => {
      refresh(controller.signal)
        .then(() => {
          if (mounted) setError(null)
        })
        .catch((refreshError: unknown) => {
          if (!mounted) return
          if (refreshError instanceof DOMException && refreshError.name === 'AbortError') return
          setError(refreshError instanceof ApiError ? refreshError.message : '无法加载监控状态。')
        })
    }
    tick()
    const timer = window.setInterval(tick, anyRunning ? 2_000 : 10_000)
    return () => {
      mounted = false
      controller.abort()
      window.clearInterval(timer)
    }
  }, [refresh, anyRunning, refreshTick])

  // Stable identity: an inline closure would change on every poll re-render and
  // make the acquisition panel treat each tick as a configuration change.
  const onPanelUnauthorized = useCallback(() => {
    setAuthRequired(true)
    requestApiToken()
  }, [requestApiToken])

  async function runAction(key: string, action: () => Promise<unknown>) {
    setBusyAction(key)
    setError(null)
    try {
      await action()
      setAuthRequired(false)
      setRefreshTick((value) => value + 1)
    } catch (actionError) {
      if (isUnauthorized(actionError)) {
        // Reads stay open, so a 401 here means the write path needs the token: ask for it right away.
        setAuthRequired(true)
        requestApiToken()
      } else {
        setError(actionError instanceof ApiError ? actionError.message : '操作未能完成。')
      }
    } finally {
      setBusyAction(null)
    }
  }

  async function toggleEnabled(pipeline: PipelineId) {
    const current = sectionVersions[pipeline]
    if (!current) return
    await runAction(`toggle-${pipeline}`, () =>
      updateConfigSection(pipeline, { enabled: !current.enabled }, current.version))
  }

  async function openRun(runId: string) {
    try {
      setSelectedRun(await getRun(runId))
    } catch (detailError) {
      setError(detailError instanceof ApiError ? detailError.message : '无法加载运行详情。')
    }
  }

  // Escape parity with every other dialog in the app.
  useEffect(() => {
    if (!selectedRun) return
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setSelectedRun(null)
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [selectedRun])

  return (
    <main>
      <section className="panel dashboard-panel" aria-labelledby="dashboard-title">
        <div className="panel-heading">
          <h2 id="dashboard-title">流水线</h2>
        </div>
        {authRequired && (
          <Notice
            tone="warning"
            title="需要重新登录"
            body="触发运行和启停调度属于写操作，服务端拒绝了当前的登录状态。重新登录后再重试即可。"
            action={
              <button className="text-button" type="button" onClick={requestApiToken}>
                重新登录
              </button>
            }
          />
        )}
        {error && <Notice tone="error" title="监控请求失败" body={error} />}
        {!statuses && !error && <p className="dashboard-loading"><Spinner /> 正在加载…</p>}
        <div className="pipeline-grid">
          {(statuses ?? []).map((status) => {
            const pipeline = status.pipeline
            const toggle = sectionVersions[pipeline]
            return (
              <article key={pipeline} className="pipeline-card" aria-label={PIPELINE_LABELS[pipeline]}>
                <div className="pipeline-card-head">
                  <h3>{PIPELINE_LABELS[pipeline]}</h3>
                  <span
                    className={`pipeline-state ${status.running_run_id ? 'running' : status.enabled && status.configured ? 'idle' : 'off'}`}
                  >
                    {status.running_run_id ? '运行中' : !status.configured ? '未配置' : status.enabled ? '待机' : '已停用'}
                  </span>
                </div>
                <p className="pipeline-desc">{PIPELINE_DESCRIPTIONS[pipeline]}</p>
                {!status.configured && status.reason && (
                  <p className="pipeline-reason">{localizeBackendText(status.reason)}</p>
                )}
                <dl className="pipeline-meta">
                  <div>
                    <dt>上次运行</dt>
                    <dd>
                      {status.last_run
                        ? `${formatTime(status.last_run.started_at)} · ${STATE_LABELS[status.last_run.state] ?? status.last_run.state}`
                        : '—'}
                    </dd>
                  </div>
                  <div>
                    <dt>下次计划</dt>
                    <dd>{formatTime(status.next_scheduled_at)}</dd>
                  </div>
                </dl>
                {status.last_run && statChips(status.last_run.stats).length > 0 && (
                  <div className="stat-chips">
                    {statChips(status.last_run.stats).map(([label, value]) => (
                      <span key={label} className="stat-chip">
                        {label} <strong>{value}</strong>
                      </span>
                    ))}
                  </div>
                )}
                <div className="pipeline-actions">
                  {status.running_run_id ? (
                    <button
                      className="button secondary"
                      type="button"
                      disabled={busyAction === `cancel-${pipeline}`}
                      onClick={() => void runAction(`cancel-${pipeline}`, () => cancelPipeline(pipeline))}
                    >
                      {busyAction === `cancel-${pipeline}` ? <Spinner /> : null}
                      取消运行
                    </button>
                  ) : (
                    <button
                      className="button primary"
                      type="button"
                      disabled={!status.configured || busyAction === `run-${pipeline}`}
                      onClick={() => void runAction(`run-${pipeline}`, () => triggerPipeline(pipeline))}
                    >
                      {busyAction === `run-${pipeline}` ? <Spinner /> : null}
                      立即运行
                    </button>
                  )}
                  {toggle && (
                    <button
                      className="text-button"
                      type="button"
                      disabled={busyAction === `toggle-${pipeline}`}
                      onClick={() => void toggleEnabled(pipeline)}
                    >
                      {toggle.enabled ? '停用调度' : '启用调度'}
                    </button>
                  )}
                </div>
              </article>
            )
          })}
        </div>
      </section>

      <AcquisitionPanel onUnauthorized={onPanelUnauthorized} />

      <section className="panel dashboard-panel" aria-labelledby="runs-title">
        <div className="panel-heading">
          <h2 id="runs-title">运行历史</h2>
          <div className="run-filter" role="group" aria-label="筛选流水线">
            {(['all', 'rss', 'archive', 'mapping'] as const).map((option) => (
              <button
                key={option}
                type="button"
                className={`text-button${runFilter === option ? ' active' : ''}`}
                onClick={() => setRunFilter(option)}
              >
                {option === 'all' ? '全部' : PIPELINE_LABELS[option]}
              </button>
            ))}
          </div>
        </div>
        {runs.length === 0 ? (
          <p className="results-empty">还没有运行记录。</p>
        ) : (
          <div className="run-table-wrap">
            <table className="run-table">
              <thead>
                <tr>
                  <th>流水线</th>
                  <th>触发</th>
                  <th>状态</th>
                  <th>开始时间</th>
                  <th>耗时</th>
                  <th>结果</th>
                  <th className="row-chevron" aria-hidden="true" />
                </tr>
              </thead>
              <tbody>
                {runs.map((run) => (
                  <tr
                    key={run.run_id}
                    tabIndex={0}
                    aria-label={`查看 ${PIPELINE_LABELS[run.pipeline]} 运行详情`}
                    onClick={() => void openRun(run.run_id)}
                    onKeyDown={(event) => {
                      if (event.key !== 'Enter' && event.key !== ' ') return
                      event.preventDefault()
                      void openRun(run.run_id)
                    }}
                  >
                    <td>{PIPELINE_LABELS[run.pipeline]}</td>
                    <td>{TRIGGER_LABELS[run.trigger] ?? run.trigger}</td>
                    <td>
                      <span className={`run-state ${run.state}`}>{STATE_LABELS[run.state] ?? run.state}</span>
                      {run.error_count > 0 && <span className="run-errors">{run.error_count} 错误</span>}
                    </td>
                    <td>{formatTime(run.started_at)}</td>
                    <td>{formatDuration(run)}</td>
                    <td className="run-stats-cell">
                      {statChips(run.stats)
                        .slice(0, 4)
                        .map(([label, value]) => `${label} ${value}`)
                        .join(' · ') || '—'}
                    </td>
                    <td className="row-chevron">
                      <ChevronIcon expanded={false} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {selectedRun && (
        <div className="dialog-backdrop" role="presentation" onClick={() => setSelectedRun(null)}>
          <div
            className="dialog run-detail"
            role="dialog"
            aria-modal="true"
            aria-label="运行详情"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="panel-heading">
              <h2>
                {PIPELINE_LABELS[selectedRun.pipeline]} · {STATE_LABELS[selectedRun.state] ?? selectedRun.state}
              </h2>
              <button className="text-button" type="button" onClick={() => setSelectedRun(null)}>
                关闭
              </button>
            </div>
            <p className="run-detail-meta">
              {TRIGGER_LABELS[selectedRun.trigger] ?? selectedRun.trigger}触发 · {formatTime(selectedRun.started_at)} ·{' '}
              {formatDuration(selectedRun)}
            </p>
            {statChips(selectedRun.stats).length > 0 && (
              <div className="stat-chips">
                {statChips(selectedRun.stats).map(([label, value]) => (
                  <span key={label} className="stat-chip">
                    {label} <strong>{value}</strong>
                  </span>
                ))}
              </div>
            )}
            {selectedRun.errors.length > 0 && (
              <div className="run-detail-errors">
                <h3>错误</h3>
                <ul>
                  {selectedRun.errors.map((item, index) => (
                    <li key={index}>{item}</li>
                  ))}
                </ul>
              </div>
            )}
            {selectedRun.log_tail.length > 0 && (
              <div className="run-detail-log">
                <h3>日志</h3>
                <pre>{selectedRun.log_tail.join('\n')}</pre>
              </div>
            )}
          </div>
        </div>
      )}
    </main>
  )
}
