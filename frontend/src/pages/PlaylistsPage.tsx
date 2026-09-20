/** The ranking lists the playlists pipeline mirrors into Emby, with each list's gap.
 *
 * Three groups: the all-time charts, the JavDB yearly TOP250s, and the JAV
 * awards by edition. A list can be enabled or disabled here; the playlist itself
 * follows on the next sync (a disabled list's playlist is removed). The missing
 * column expands into the titles the library does not hold, in ranking order,
 * with the ledger's state for those already being acquired.
 */

import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useOutletContext } from 'react-router-dom'

import {
  ApiError,
  fillPlaylist,
  getMonitorStatus,
  getPlaylistMissing,
  isUnauthorized,
  listPlaylists,
  triggerPipeline,
  updatePlaylist,
} from '../api'
import type { AppContext } from '../App'
import { Notice } from '../components/Feedback'
import { ChevronIcon, Spinner } from '../components/Icons'
import { useApiTokenConfigured } from '../lib/apiToken'
import { localizeBackendText } from '../lib/backendText'
import { formatTime } from '../lib/subscriptions'
import type { AcquisitionState, ManualOutcome, PipelineStatus, Playlist, PlaylistFill, PlaylistMissing } from '../types'

const KIND_AWARDS = 4
const FIRST_YEAR_KIND = 2008

const TRACKED_LABELS: Record<AcquisitionState, string> = {
  discovered: '已排队',
  downloading: '下载中',
  archived: '已入库',
  resolve_failed: '暂无磁力',
  exhausted: '磁力用尽',
  needs_attention: '待处理',
  ignored: '已忽略',
}

const FILL_OUTCOME_LABELS: Record<ManualOutcome, string> = {
  submitted: '已提交',
  already_tracked: '已在跟踪',
  already_in_library: '库内已有',
  no_magnet: '暂无磁力',
  submit_failed: '提交失败',
  unreadable: '无法识别',
}

/** "已提交 12 · 暂无磁力 3": the outcomes a fill ended in, in the order they matter. */
function fillSummary(fill: PlaylistFill): string {
  const parts = (Object.keys(FILL_OUTCOME_LABELS) as ManualOutcome[])
    .filter((outcome) => (fill.counts[outcome] ?? 0) > 0)
    .map((outcome) => `${FILL_OUTCOME_LABELS[outcome]} ${fill.counts[outcome]}`)
  return parts.length ? parts.join(' · ') : '没有需要提交的番号'
}

interface Group {
  id: string
  title: string
  items: Playlist[]
}

/** Charts first, then years newest first, then each awards edition in source order. */
function groupPlaylists(items: Playlist[]): Group[] {
  const charts = items.filter((item) => item.kind < FIRST_YEAR_KIND && item.kind !== KIND_AWARDS)
  const years = items.filter((item) => item.kind >= FIRST_YEAR_KIND).sort((a, b) => b.kind - a.kind)
  const awards = items.filter((item) => item.kind === KIND_AWARDS)
  const editions = new Map<string, Playlist[]>()
  for (const item of awards) {
    const edition = item.note.split(' ')[0] ?? item.note
    editions.set(edition, [...(editions.get(edition) ?? []), item])
  }
  const groups: Group[] = []
  if (charts.length) groups.push({ id: 'charts', title: '总榜', items: charts })
  if (years.length) groups.push({ id: 'years', title: 'JavDB 年度榜', items: years })
  for (const [edition, editionItems] of editions) {
    groups.push({ id: `awards-${edition}`, title: edition, items: editionItems })
  }
  return groups
}

function stateLabel(item: Playlist): string {
  if (!item.enabled) return '停用'
  if (item.last_error) return '同步出错'
  return item.emby_playlist_id ? '已同步' : item.last_synced_at ? '库内无片' : '待同步'
}

function stateTone(item: Playlist): string {
  if (!item.enabled) return ''
  if (item.last_error) return 'failed'
  return item.emby_playlist_id ? 'completed' : 'running'
}

export default function PlaylistsPage() {
  const { requestApiToken } = useOutletContext<AppContext>()
  const tokenConfigured = useApiTokenConfigured()
  const [items, setItems] = useState<Playlist[]>([])
  const [sourceName, setSourceName] = useState<string | null>(null)
  const [fillDir, setFillDir] = useState<{ path: string | null; reason: string | null }>({ path: null, reason: null })
  const [armedFill, setArmedFill] = useState<string | null>(null)
  const [fills, setFills] = useState<Record<string, PlaylistFill>>({})
  const [pipeline, setPipeline] = useState<PipelineStatus | null>(null)
  const [loaded, setLoaded] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState<string | null>(null)
  const [expanded, setExpanded] = useState<string | null>(null)
  const [missing, setMissing] = useState<Record<string, PlaylistMissing>>({})

  const load = useCallback(async (signal?: AbortSignal) => {
    try {
      const [page, statuses] = await Promise.all([listPlaylists(signal), getMonitorStatus(signal)])
      setItems(page.items)
      setSourceName(page.source?.database_name ?? null)
      setFillDir({ path: page.fill_task_dir, reason: page.fill_reason })
      setPipeline(statuses.find((status) => status.pipeline === 'playlists') ?? null)
      setError(null)
    } catch (failure) {
      if (failure instanceof DOMException && failure.name === 'AbortError') return
      setError(failure instanceof ApiError ? failure.message : '无法加载列表。')
    } finally {
      setLoaded(true)
    }
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    void load(controller.signal)
    return () => controller.abort()
  }, [load])

  async function run(id: string, action: () => Promise<void>, fallback: string) {
    setBusy(id)
    try {
      await action()
      setError(null)
    } catch (failure) {
      if (isUnauthorized(failure)) requestApiToken()
      setError(failure instanceof ApiError ? failure.message : fallback)
    } finally {
      setBusy(null)
    }
  }

  const toggle = (item: Playlist) =>
    run(`toggle-${item.key}`, async () => {
      const updated = await updatePlaylist(item.key, { enabled: !item.enabled })
      setItems((current) => current.map((entry) => (entry.key === updated.key ? updated : entry)))
    }, '修改列表失败。')

  const fill = (item: Playlist) =>
    run(`fill-${item.key}`, async () => {
      const result = await fillPlaylist(item.key)
      setFills((current) => ({ ...current, [item.key]: result }))
      setArmedFill(null)
      // The gap is now partly in flight: the ledger states under the row are stale.
      setMissing((current) => {
        const next = { ...current }
        delete next[item.key]
        return next
      })
      if (expanded === item.key) {
        const detail = await getPlaylistMissing(item.key)
        setMissing((current) => ({ ...current, [item.key]: detail }))
      }
    }, '补全失败。')

  const syncNow = () =>
    run('sync', async () => {
      await triggerPipeline('playlists')
      await load()
    }, '触发同步失败。')

  async function expand(item: Playlist) {
    if (expanded === item.key) {
      setExpanded(null)
      return
    }
    setExpanded(item.key)
    if (missing[item.key]) return
    try {
      const detail = await getPlaylistMissing(item.key)
      setMissing((current) => ({ ...current, [item.key]: detail }))
    } catch (failure) {
      setError(failure instanceof ApiError ? failure.message : '无法加载缺失清单。')
    }
  }

  const groups = useMemo(() => groupPlaylists(items), [items])
  const lastSync = useMemo(
    () => items.reduce<string | null>((latest, item) => (item.last_synced_at && (!latest || item.last_synced_at > latest) ? item.last_synced_at : latest), null),
    [items],
  )
  const totals = useMemo(
    () => ({
      enabled: items.filter((item) => item.enabled).length,
      missing: items.reduce((sum, item) => sum + item.missing, 0),
    }),
    [items],
  )

  return (
    <main>
      {!tokenConfigured && (
        <Notice
          tone="warning"
          title="修改列表需要登录"
          body="查看列表无需认证；启用、停用或触发同步前，请先用部署时设置的 API Token 登录。"
        />
      )}
      <section className="panel settings-panel" aria-labelledby="playlists-title">
        <div className="panel-heading">
          <h2 id="playlists-title">列表</h2>
          <button
            className="button primary"
            type="button"
            disabled={busy !== null || !pipeline?.configured || Boolean(pipeline?.running_run_id)}
            onClick={() => void syncNow()}
          >
            {busy === 'sync' ? <Spinner /> : null}
            {pipeline?.running_run_id ? '同步中…' : '立即同步'}
          </button>
        </div>
        <p className="settings-desc">
          jinjier.art 的榜单按名次同步成 embyx 的播放列表，只放库里已有的影片；停用的列表会从 Emby 里移除，重新启用后重建。
          缺失一列展开后是库里没有的番号，以及其中已在下载追踪里的状态。
        </p>
        {pipeline && !pipeline.configured && (
          <Notice
            tone="warning"
            title="同步尚未就绪"
            body={pipeline.reason ? localizeBackendText(pipeline.reason) : '缺少配置。'}
            action={<Link className="text-button" to="/settings">前往设置</Link>}
          />
        )}
        {error && <Notice tone="error" title="列表操作失败" body={error} />}
        {loaded && (
          <p className="acq-meta">
            数据源 {sourceName ? `jinjier.sqlite3 · ${sourceName}` : '尚未下载'} · 上次同步 {formatTime(lastSync)} · 启用 {totals.enabled}/{items.length} 张 · 缺失 {totals.missing} 部
            {fillDir.path ? ` · 补全目录 ${fillDir.path}` : ''}
          </p>
        )}
        {loaded && !fillDir.path && fillDir.reason && (
          <p className="settings-hint">补全不可用：{localizeBackendText(fillDir.reason)}</p>
        )}
        {!loaded ? (
          <p className="dashboard-loading">
            <Spinner /> 正在加载…
          </p>
        ) : items.length === 0 ? (
          <p className="route-empty">还没有列表：配置好 Emby 后点「立即同步」拉取榜单。</p>
        ) : (
          groups.map((group) => (
            <section key={group.id} className="playlist-group" aria-labelledby={`playlist-group-${group.id}`}>
              <h3 id={`playlist-group-${group.id}`} className="playlist-group-title">
                {group.title}
              </h3>
              <div className="run-table-wrap">
                <table className="run-table">
                  <thead>
                    <tr>
                      <th className="row-chevron" aria-label="展开" />
                      <th>榜单</th>
                      <th>条目</th>
                      <th>已有</th>
                      <th>缺失</th>
                      <th>状态</th>
                      <th>上次同步</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {group.items.map((item) => {
                      const open = expanded === item.key
                      const detail = missing[item.key]
                      return [
                        <tr
                          key={item.key}
                          aria-expanded={open}
                          tabIndex={0}
                          onClick={() => void expand(item)}
                          onKeyDown={(event) => {
                            if (event.key === 'Enter' || event.key === ' ') {
                              event.preventDefault()
                              void expand(item)
                            }
                          }}
                        >
                          <td className="row-chevron">
                            <ChevronIcon expanded={open} />
                          </td>
                          <td className="subscription-url">
                            <strong>{item.name}</strong>
                            {fills[item.key] && (
                              <span className="acq-muted playlist-fill-summary">补全：{fillSummary(fills[item.key])}</span>
                            )}
                          </td>
                          <td>{item.total}</td>
                          <td>{item.present}</td>
                          <td>{item.missing}</td>
                          <td>
                            <span className={`run-state ${stateTone(item)}`}>{stateLabel(item)}</span>
                            {item.last_error && (
                              <small className="subscription-error">{localizeBackendText(item.last_error)}</small>
                            )}
                          </td>
                          <td className="acq-muted">{formatTime(item.last_synced_at)}</td>
                          <td>
                            <div className="acq-actions">
                              <button
                                type="button"
                                className="text-button"
                                disabled={busy !== null}
                                onClick={(event) => {
                                  event.stopPropagation()
                                  void toggle(item)
                                }}
                              >
                                {busy === `toggle-${item.key}` ? <Spinner /> : null}
                                {item.enabled ? '停用' : '启用'}
                              </button>
                              {armedFill === item.key ? (
                                <>
                                  <button
                                    type="button"
                                    className="text-button"
                                    disabled={busy !== null}
                                    onClick={(event) => {
                                      event.stopPropagation()
                                      void fill(item)
                                    }}
                                  >
                                    {busy === `fill-${item.key}` ? <Spinner /> : null}
                                    确认补全 {item.missing} 部
                                  </button>
                                  <button
                                    type="button"
                                    className="text-button"
                                    disabled={busy !== null}
                                    onClick={(event) => {
                                      event.stopPropagation()
                                      setArmedFill(null)
                                    }}
                                  >
                                    取消
                                  </button>
                                </>
                              ) : (
                                <button
                                  type="button"
                                  className="text-button"
                                  disabled={busy !== null || item.missing === 0 || !fillDir.path}
                                  title={!fillDir.path ? '还没有可用的补全目录' : undefined}
                                  onClick={(event) => {
                                    event.stopPropagation()
                                    setArmedFill(item.key)
                                  }}
                                >
                                  补全
                                </button>
                              )}
                            </div>
                          </td>
                        </tr>,
                        open && (
                          <tr key={`${item.key}-detail`} className="acq-detail-row">
                            <td colSpan={8}>
                              <div className="acq-detail">
                                {!detail ? (
                                  <p className="dashboard-loading">
                                    <Spinner /> 正在加载缺失清单…
                                  </p>
                                ) : detail.items.length === 0 ? (
                                  <p className="route-empty">这张榜的影片库里都有。</p>
                                ) : (
                                  <table className="run-table">
                                    <thead>
                                      <tr>
                                        <th>名次</th>
                                        <th>番号</th>
                                        <th>标题</th>
                                        <th>下载追踪</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {detail.items.map((entry) => (
                                        <tr key={entry.avid}>
                                          <td className="acq-muted">{entry.rank}</td>
                                          <td>{entry.avid}</td>
                                          <td className="playlist-title">{entry.title}</td>
                                          <td className="acq-muted">
                                            {entry.tracked ? TRACKED_LABELS[entry.tracked] ?? entry.tracked : '—'}
                                          </td>
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                )}
                              </div>
                            </td>
                          </tr>
                        ),
                      ]
                    })}
                  </tbody>
                </table>
              </div>
            </section>
          ))
        )}
      </section>
    </main>
  )
}
