/** Multi-part titles in the library, and the intake route each would re-enter through.
 *
 * Emby stacks parts cd1 through cd9 into one item and shows cd10 onwards as
 * items of their own, so titles with ten parts or more come first. Both groups
 * can be merged; titles with missing or unreadable parts are listed apart and
 * cannot be.
 */

import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'

import { ApiError, listMergeTitles } from '../api'
import { Notice } from '../components/Feedback'
import { ChevronIcon, Spinner } from '../components/Icons'
import { localizeBackendText } from '../lib/backendText'
import { formatTime } from '../lib/subscriptions'
import type { MergeTitle, MergeTitleList, MergeTitleProblem } from '../types'

/** Emby stacks a single digit only. */
const MAX_STACKED_PARTS = 9

const PROBLEM_LABELS: Record<MergeTitleProblem, string> = {
  unreadable_strm: 'strm 无法读取',
  outside_library: '文件不在库目录下',
  scattered_parts: '分盘不在同一目录',
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

function TitleTable({ items }: { items: MergeTitle[] }) {
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
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default function MergePage() {
  const [page, setPage] = useState<MergeTitleList | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [open, setOpen] = useState<Record<string, boolean>>({})

  const load = useCallback(async (signal?: AbortSignal) => {
    setLoading(true)
    try {
      setPage(await listMergeTitles(signal))
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

  const groups = useMemo(() => groupTitles(page?.items ?? []), [page])

  return (
    <main>
      <section className="panel settings-panel" aria-labelledby="merge-title">
        <div className="panel-heading">
          <h2 id="merge-title">分盘合并</h2>
          <button className="button primary" type="button" disabled={loading} onClick={() => void load()}>
            {loading ? <Spinner /> : null}
            重新扫描
          </button>
        </div>
        <p className="settings-desc">
          扫描库里所有带 -cdN 分盘的作品。合并后的文件会经 /115/upload 中转，再按来源资源库走 embyx_in 的默认归档流程入库。
        </p>
        {page?.reason && (
          <Notice
            tone="warning"
            title="还不能扫描"
            body={localizeBackendText(page.reason)}
            action={<Link className="text-button" to="/settings">前往设置</Link>}
          />
        )}
        {error && <Notice tone="error" title="扫描失败" body={error} />}
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
                {expanded && <TitleTable items={group.items} />}
              </section>
            )
          })
        )}
      </section>
    </main>
  )
}
