/** Multi-part titles in the library, merging them, and the tasks doing so.
 *
 * Emby stacks parts cd1 through cd9 into one item and shows cd10 onwards as
 * items of their own, so titles with ten parts or more come first. Both groups
 * can be merged; titles with missing or unreadable parts are listed apart and
 * cannot be. A merge runs as a Job in the cluster, uploads through CloudDrive
 * into the 115 staging directory, and once the upload checks out replaces the
 * original parts and re-enters the archive through the title's intake route.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'

import { ApiError, cancelMergeTask, createMergeTask, listMergeTasks, listMergeTitles, retryMergeTask } from '../api'
import { Notice } from '../components/Feedback'
import { ChevronIcon, Spinner } from '../components/Icons'
import { localizeBackendText } from '../lib/backendText'
import { formatTime } from '../lib/subscriptions'
import type {
  MergeAutoState,
  MergeAutoStatus,
  MergeState,
  MergeTask,
  MergeTaskList,
  MergeTitle,
  MergeTitleList,
  MergeTitleProblem,
} from '../types'

/** Emby stacks a single digit only. */
const MAX_STACKED_PARTS = 9
const POLL_MS = 5_000

const PROBLEM_LABELS: Record<MergeTitleProblem, string> = {
  unreadable_strm: 'strm 无法读取',
  outside_library: '文件不在库目录下',
  scattered_parts: '分盘不在同一目录',
}

const STATE_LABELS: Record<MergeState, string> = {
  queued: '排队中',
  merging: '合并中',
  uploading: '上传中',
  verifying: '校验中',
  replacing: '替换原盘',
  archiving: '等待归档',
  done: '已完成',
  failed: '失败',
  cancelled: '已取消',
}

const PHASE_LABELS: Record<string, string> = {
  starting: '启动 Job',
  checking: '检查空间',
  probing: '读取分盘信息',
  merging: '合并',
  verifying: '核对时长',
  hashing: '计算 SHA-1',
}

const FINISHED: ReadonlySet<MergeState> = new Set(['done', 'cancelled'])
/** States the loop is still working through, which the page keeps polling for. */
const MOVING: ReadonlySet<MergeState> = new Set(['queued', 'merging', 'uploading', 'verifying', 'replacing', 'archiving'])

function formatBytes(bytes: number | null): string {
  if (bytes === null) return '—'
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(1)} GiB`
  return `${(bytes / 1024 ** 2).toFixed(0)} MiB`
}

function percent(value: number | null): string {
  return value === null ? '' : ` ${Math.round(value * 100)}%`
}

function taskProgress(task: MergeTask): string {
  if (task.state === 'merging') return `${PHASE_LABELS[task.phase ?? ''] ?? task.phase ?? ''}${percent(task.progress)}`
  if (task.state === 'uploading') return `${formatBytes(task.uploaded_bytes ?? 0)} / ${formatBytes(task.merged_bytes)}`
  if (task.state === 'failed' && task.failed_state) return `在「${STATE_LABELS[task.failed_state]}」时失败`
  return ''
}

function sourceLabel(title: MergeTitle): string {
  if (!title.source) return '未确定'
  if (title.source_basis === 'ledger') return `${title.source}（按下载记录）`
  return title.source
}

function problemLabel(title: MergeTitle): string {
  if (title.problem) return PROBLEM_LABELS[title.problem] ?? title.problem
  if (title.missing.length) return `缺 ${title.missing.map((index) => `cd${index}`).join('、')}`
  return ''
}

interface Group {
  id: string
  title: string
  hint: string
  items: MergeTitle[]
  collapsible: boolean
}

function groupTitles(items: MergeTitle[]): Group[] {
  const many = items.filter((item) => item.mergeable && item.part_count > MAX_STACKED_PARTS)
  const few = items.filter((item) => item.mergeable && item.part_count <= MAX_STACKED_PARTS)
  const broken = items.filter((item) => !item.mergeable)
  return [
    { id: 'many', title: `10 盘及以上 · ${many.length}`, hint: 'Emby 会把 cd10 起的每一盘显示成单独条目。', items: many, collapsible: false },
    { id: 'few', title: `9 盘及以下 · ${few.length}`, hint: 'Emby 能堆叠成一个条目，也可以合并。', items: few, collapsible: true },
    { id: 'broken', title: `缺盘或异常 · ${broken.length}`, hint: '不能合并，需要先补齐或修正。', items: broken, collapsible: true },
  ]
}

const AUTO_LABELS: Record<MergeAutoState, string> = {
  off: '已关闭',
  unavailable: '暂时不能运行',
  busy: '等当前任务归档后再排下一部',
  paused: '已暂停：有失败的任务，重试或移除后继续',
  idle: '没有放得下的作品，稍后再看',
}

const SKIP_REASONS: Record<string, string> = {
  too_big: '放不下',
  parts_missing: '分盘不全',
  size_unknown: '读不到大小',
  merge_source_required: '要先选来源',
}

function AutoLine({ auto }: { auto: MergeAutoStatus }) {
  if (auto.state === 'off') {
    return (
      <p className="settings-hint">
        自动合并已关闭，可以在 <Link to="/settings">设置</Link> 的「合并」里打开。
      </p>
    )
  }
  const room = auto.room === null ? '' : `，downloads 可用 ${formatBytes(auto.room)}（已扣预留）`
  const shown = auto.skipped
    .slice(0, 5)
    .map((item) => `${item.avid}（${SKIP_REASONS[item.reason] ?? item.reason}${item.size ? ` ${formatBytes(item.size)}` : ''}）`)
  const more = auto.skipped_count > shown.length ? ` 等 ${auto.skipped_count} 部` : ''
  return (
    <p className="settings-hint">
      自动合并：{AUTO_LABELS[auto.state]}
      {room}
      {shown.length > 0 && `。跳过 ${shown.join('、')}${more}`}
    </p>
  )
}

interface MergeControls {
  /** Merging is unavailable in this deployment. */
  blocked: boolean
  routes: string[]
  openTasks: Map<string, MergeTask>
  busy: string | null
  /** The action key waiting for its second, confirming click. */
  armed: string | null
  onArm: (key: string | null) => void
  onMerge: (title: MergeTitle, source: string | null) => void
}

function ArmedConfirm({
  label,
  hint,
  disabled,
  onConfirm,
  onDisarm,
}: {
  label: string
  hint: string
  disabled: boolean
  onConfirm: () => void
  onDisarm: () => void
}) {
  return (
    <span className="merge-confirm" title={hint}>
      <button className="text-button" type="button" disabled={disabled} onClick={onConfirm}>
        {label}
      </button>
      <button className="text-button" type="button" onClick={onDisarm}>
        算了
      </button>
    </span>
  )
}

const MERGE_HINT = '合并后的文件经 /115/upload 校验后放进来源资源库的 embyx_in 重新归档；校验通过后原分盘进 115 回收站。'

function MergeAction({ title, controls }: { title: MergeTitle; controls: MergeControls }) {
  const [source, setSource] = useState('')
  const task = controls.openTasks.get(title.avid)
  if (task) return <span className="acq-muted">{STATE_LABELS[task.state]}</span>
  if (!title.mergeable) return <span className="acq-muted">—</span>
  const disabled = controls.blocked || controls.busy !== null
  const key = `merge-${title.avid}`
  if (controls.busy === title.avid) {
    return (
      <span className="acq-muted">
        <Spinner /> 提交中
      </span>
    )
  }
  if (controls.armed === key) {
    return (
      <ArmedConfirm
        label={`确认合并 ${title.part_count} 盘（会删原盘）`}
        hint={MERGE_HINT}
        disabled={disabled}
        onConfirm={() => controls.onMerge(title, source || null)}
        onDisarm={() => controls.onArm(null)}
      />
    )
  }
  if (title.source) {
    return (
      <button className="text-button" type="button" disabled={disabled} onClick={() => controls.onArm(key)}>
        合并
      </button>
    )
  }
  return (
    <span className="merge-source-pick">
      <select aria-label={`${title.avid} 的来源资源库`} value={source} onChange={(event) => setSource(event.target.value)}>
        <option value="">选来源…</option>
        {controls.routes.map((route) => (
          <option key={route} value={route}>
            {route}
          </option>
        ))}
      </select>
      <button
        className="text-button"
        type="button"
        disabled={disabled || !source}
        onClick={() => controls.onArm(key)}
      >
        合并
      </button>
    </span>
  )
}

function TitleTable({ items, controls }: { items: MergeTitle[]; controls: MergeControls }) {
  if (items.length === 0) return <p className="route-empty">没有作品。</p>
  return (
    <div className="run-table-wrap">
      <table className="run-table">
        <thead>
          <tr>
            <th>番号</th>
            <th>盘数</th>
            <th>库目录</th>
            <th>来源资源库</th>
            <th>备注</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {items.map((item) => (
            <tr key={`${item.directory}/${item.avid}`}>
              <td>
                <strong>{item.avid}</strong>
              </td>
              <td>{item.part_count}</td>
              <td className="acq-muted">{item.library_dir ? `${item.library_dir}/${item.brand ?? ''}` : '—'}</td>
              <td className={item.source ? undefined : 'acq-muted'}>{sourceLabel(item)}</td>
              <td className="acq-muted">{problemLabel(item) || '—'}</td>
              <td>
                <MergeAction title={item} controls={controls} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function TaskTable({
  tasks,
  busy,
  armed,
  onArm,
  onCancel,
  onRetry,
}: {
  tasks: MergeTask[]
  busy: string | null
  armed: string | null
  onArm: (key: string | null) => void
  onCancel: (task: MergeTask) => void
  onRetry: (task: MergeTask) => void
}) {
  return (
    <div className="run-table-wrap">
      <table className="run-table">
        <thead>
          <tr>
            <th>番号</th>
            <th>盘数</th>
            <th>来源</th>
            <th>状态</th>
            <th>进度</th>
            <th>说明</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {tasks.map((task) => {
            const message = task.error ?? task.notice
            return (
              <tr key={task.id}>
                <td>
                  <strong>{task.avid}</strong>
                </td>
                <td>{task.part_count}</td>
                <td>{task.source}</td>
                <td>{STATE_LABELS[task.state]}</td>
                <td className="acq-muted">{taskProgress(task) || '—'}</td>
                <td className={task.error ? undefined : 'acq-muted'}>{message ? localizeBackendText(message) : '—'}</td>
                <td>
                  {task.retryable && (
                    <button className="text-button" type="button" disabled={busy !== null} onClick={() => onRetry(task)}>
                      重试
                    </button>
                  )}
                  {task.cancellable &&
                    (armed === `task-${task.id}` ? (
                      <ArmedConfirm
                        label={`确认${task.state === 'failed' ? '移除' : '取消'}`}
                        hint="已经合并或上传的中间文件会被删掉，原分盘不受影响。"
                        disabled={busy !== null}
                        onConfirm={() => onCancel(task)}
                        onDisarm={() => onArm(null)}
                      />
                    ) : (
                      <button
                        className="text-button"
                        type="button"
                        disabled={busy !== null}
                        onClick={() => onArm(`task-${task.id}`)}
                      >
                        {task.state === 'failed' ? '移除' : '取消'}
                      </button>
                    ))}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

export default function MergePage() {
  const [page, setPage] = useState<MergeTitleList | null>(null)
  const [tasks, setTasks] = useState<MergeTaskList | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState<string | null>(null)
  const [open, setOpen] = useState<Record<string, boolean>>({})
  const [armed, setArmed] = useState<string | null>(null)

  const load = useCallback(async (signal?: AbortSignal, refresh = false) => {
    setLoading(true)
    try {
      // Tasks answer at once; a fresh scan of the library can take a minute.
      const taskList = listMergeTasks(signal).then(setTasks)
      const [titles] = await Promise.all([listMergeTitles(signal, refresh), taskList])
      setPage(titles)
      setError(null)
    } catch (failure) {
      if (failure instanceof DOMException && failure.name === 'AbortError') return
      setError(failure instanceof ApiError ? failure.message : '无法加载分盘列表。')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    void load(controller.signal)
    return () => controller.abort()
  }, [load])

  const tasksRef = useRef<MergeTaskList | null>(null)
  useEffect(() => {
    tasksRef.current = tasks
  }, [tasks])

  // Automatic merging queues the next title by itself once the last one is filed.
  const moving = (tasks?.items.some((task) => MOVING.has(task.state)) ?? false) || tasks?.auto?.state === 'busy'
  useEffect(() => {
    if (!moving) return
    const timer = window.setInterval(() => {
      listMergeTasks()
        .then((next) => {
          const previous = tasksRef.current
          // A title that just finished leaves the library's multi-part list.
          const finished = next.items.some(
            (task) => task.state === 'done' && previous?.items.find((old) => old.id === task.id)?.state !== 'done',
          )
          setTasks(next)
          if (finished) void listMergeTitles(undefined, true).then(setPage).catch(() => undefined)
        })
        .catch(() => undefined)
    }, POLL_MS)
    return () => window.clearInterval(timer)
  }, [moving])

  const act = useCallback(async (key: string, action: () => Promise<unknown>) => {
    setBusy(key)
    setActionError(null)
    try {
      await action()
      setTasks(await listMergeTasks())
    } catch (failure) {
      setActionError(failure instanceof ApiError ? failure.message : '操作没有完成，请稍后重试。')
    } finally {
      setBusy(null)
    }
  }, [])

  const onMerge = useCallback(
    (title: MergeTitle, source: string | null) => {
      setArmed(null)
      void act(title.avid, () => createMergeTask(title.avid, source))
    },
    [act],
  )

  const groups = useMemo(() => groupTitles(page?.items ?? []), [page])
  const openTasks = useMemo(
    () => new Map((tasks?.items ?? []).filter((task) => !FINISHED.has(task.state)).map((task) => [task.avid, task])),
    [tasks],
  )
  const controls: MergeControls = {
    blocked: Boolean(tasks?.unavailable),
    routes: page?.routes ?? [],
    openTasks,
    busy,
    armed,
    onArm: setArmed,
    onMerge,
  }

  return (
    <main>
      <section className="panel settings-panel" aria-labelledby="merge-title">
        <div className="panel-heading">
          <h2 id="merge-title">合并</h2>
          <button className="button primary" type="button" disabled={loading} onClick={() => void load(undefined, true)}>
            {loading ? <Spinner /> : null}
            重新扫描
          </button>
        </div>
        <p className="settings-desc">
          扫描库里所有带 -cdN 分盘的作品。合并在集群里单独的 Job 中进行，合并后的文件经 /115/upload 中转并校验 SHA-1，再按来源资源库走 embyx_in 的默认归档流程入库。
        </p>
        {page?.reason && (
          <Notice
            tone="warning"
            title="还不能扫描"
            body={localizeBackendText(page.reason)}
            action={<Link className="text-button" to="/settings">前往设置</Link>}
          />
        )}
        {tasks?.auto && <AutoLine auto={tasks.auto} />}
        {tasks?.unavailable && (
          <Notice tone="warning" title="暂时不能合并" body={localizeBackendText(tasks.unavailable)} />
        )}
        {error && <Notice tone="error" title="扫描失败" body={error} />}
        {actionError && <Notice tone="error" title="操作失败" body={actionError} />}
        {tasks && tasks.items.length > 0 && (
          <section className="playlist-group" aria-labelledby="merge-tasks">
            <h3 id="merge-tasks" className="playlist-group-title">
              合并任务
            </h3>
            <TaskTable
              tasks={tasks.items}
              busy={busy}
              armed={armed}
              onArm={setArmed}
              onCancel={(task) => {
                setArmed(null)
                void act(`task-${task.id}`, () => cancelMergeTask(task.id))
              }}
              onRetry={(task) => void act(`task-${task.id}`, () => retryMergeTask(task.id))}
            />
          </section>
        )}
        {page && !page.reason && (
          <p className="acq-meta">
            扫描时间 {formatTime(page.scanned_at)} · 共 {page.items.length} 部分盘作品 · 资源库 {page.routes.join(' / ') || '—'}
          </p>
        )}
        {!page && loading ? (
          <p className="dashboard-loading">
            <Spinner /> 正在扫描…
          </p>
        ) : (
          page &&
          !page.reason &&
          groups.map((group) => {
            const expanded = !group.collapsible || open[group.id]
            return (
              <section key={group.id} className="playlist-group" aria-labelledby={`merge-group-${group.id}`}>
                <h3 id={`merge-group-${group.id}`} className="playlist-group-title">
                  {group.collapsible ? (
                    <button
                      type="button"
                      className="text-button"
                      aria-expanded={expanded}
                      onClick={() => setOpen((current) => ({ ...current, [group.id]: !current[group.id] }))}
                    >
                      <ChevronIcon expanded={expanded} /> {group.title}
                    </button>
                  ) : (
                    group.title
                  )}
                </h3>
                <p className="settings-hint">{group.hint}</p>
                {expanded && <TitleTable items={group.items} controls={controls} />}
              </section>
            )
          })
        )}
      </section>
    </main>
  )
}
