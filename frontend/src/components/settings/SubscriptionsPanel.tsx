/** The rss pipeline's subscription list: which feeds are polled, under which category.
 *
 * Categories come from the RSS section above; a subscription only ever files
 * into one of them, because the category is what decides the offline directory.
 * The FreshRSS import is a one-time bridge: preview what its subscription list
 * maps to, then write the new ones with a pending seed so nothing is re-read.
 */

import { useCallback, useEffect, useState } from 'react'

import {
  ApiError,
  createSubscription,
  deleteSubscription,
  importFreshRssSubscriptions,
  isUnauthorized,
  listSubscriptions,
  updateSubscription,
} from '../../api'
import { localizeBackendText } from '../../lib/backendText'
import type { FreshRssImportEntry, Subscription } from '../../types'
import { Notice } from '../Feedback'
import { Spinner } from '../Icons'

const IMPORT_STATUS_LABELS: Record<string, string> = {
  new: '将导入',
  imported: '已导入',
  exists: '已存在',
  category_missing: '分类未配置',
  invalid_url: '地址无效',
}

function formatTime(value: string | null): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return date.toLocaleString('zh-CN', { hour12: false })
}

function stateLabel(item: Subscription): string {
  if (!item.enabled) return '停用'
  if (item.seed_pending) return '待初始化'
  return item.last_error ? '拉取出错' : '启用'
}

function stateTone(item: Subscription): string {
  if (!item.enabled) return ''
  if (item.last_error) return 'failed'
  return item.seed_pending ? 'running' : 'completed'
}

interface SubscriptionsPanelProps {
  onUnauthorized: () => void
}

export function SubscriptionsPanel({ onUnauthorized }: SubscriptionsPanelProps) {
  const [items, setItems] = useState<Subscription[]>([])
  const [categories, setCategories] = useState<string[]>([])
  const [loaded, setLoaded] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [url, setUrl] = useState('')
  const [category, setCategory] = useState('')
  const [pendingDelete, setPendingDelete] = useState<number | null>(null)
  const [preview, setPreview] = useState<FreshRssImportEntry[] | null>(null)
  const [importMessage, setImportMessage] = useState<string | null>(null)

  const load = useCallback(async (signal?: AbortSignal) => {
    try {
      const page = await listSubscriptions(signal)
      setItems(page.items)
      setCategories(page.categories)
      setCategory((current) => (current && page.categories.includes(current) ? current : (page.categories[0] ?? '')))
      setError(null)
    } catch (failure) {
      if (failure instanceof DOMException && failure.name === 'AbortError') return
      setError(failure instanceof ApiError ? failure.message : '无法加载订阅。')
    } finally {
      setLoaded(true)
    }
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    void load(controller.signal)
    return () => controller.abort()
  }, [load])

  async function run(action: () => Promise<void>, fallback: string) {
    setBusy(true)
    try {
      await action()
      setError(null)
    } catch (failure) {
      if (isUnauthorized(failure)) onUnauthorized()
      setError(failure instanceof ApiError ? failure.message : fallback)
    } finally {
      setBusy(false)
    }
  }

  const add = () =>
    run(async () => {
      await createSubscription(url.trim(), category)
      setUrl('')
      await load()
    }, '添加订阅失败。')

  const toggle = (item: Subscription) =>
    run(async () => {
      const updated = await updateSubscription(item.id, { enabled: !item.enabled })
      setItems((current) => current.map((entry) => (entry.id === updated.id ? updated : entry)))
    }, '更新订阅失败。')

  const remove = (item: Subscription) =>
    run(async () => {
      await deleteSubscription(item.id)
      setPendingDelete(null)
      setItems((current) => current.filter((entry) => entry.id !== item.id))
    }, '删除订阅失败。')

  const previewImport = () =>
    run(async () => {
      const result = await importFreshRssSubscriptions(false)
      setPreview(result.entries)
      setImportMessage(null)
    }, '读取 FreshRSS 订阅失败。')

  const applyImport = () =>
    run(async () => {
      const result = await importFreshRssSubscriptions(true)
      setPreview(result.entries)
      setImportMessage(`已导入 ${result.imported} 条订阅。首次拉取只记录当前条目，不会把 FreshRSS 已读过的重新下载。`)
      await load()
    }, '导入失败。')

  const groups = new Map<string, Subscription[]>()
  for (const label of categories) groups.set(label, [])
  for (const item of items) {
    const list = groups.get(item.category)
    if (list) list.push(item)
    else groups.set(item.category, [item])
  }
  const importable = preview?.filter((entry) => entry.status === 'new').length ?? 0

  return (
    <section className="panel settings-panel" aria-labelledby="settings-subscriptions">
      <div className="panel-heading">
        <h2 id="settings-subscriptions">订阅源</h2>
      </div>
      <p className="settings-desc">
        rss 流水线轮询的 feed 列表：RSSHub 路由、AVBase 演员 feed、sukebei 搜索等任意 RSS/Atom
        地址。每条订阅归属一个分类，分类决定它的下载落在哪个离线目录。
      </p>
      {error && <Notice tone="error" title="订阅操作失败" body={error} />}
      <form
        className="subscription-form"
        onSubmit={(event) => {
          event.preventDefault()
          void add()
        }}
      >
        <select aria-label="分类" value={category} onChange={(event) => setCategory(event.target.value)}>
          {categories.map((label) => (
            <option key={label} value={label}>
              {label}
            </option>
          ))}
        </select>
        <input
          type="text"
          aria-label="Feed 地址"
          placeholder="https://rsshub.example/javbus/star/rwt"
          autoComplete="off"
          spellCheck={false}
          value={url}
          onChange={(event) => setUrl(event.target.value)}
        />
        <button className="button primary" type="submit" disabled={busy || !url.trim() || !category}>
          添加
        </button>
      </form>
      {loaded && categories.length === 0 && (
        <p className="settings-hint">先在「RSS 摄取」里配置分类，订阅才有可归属的离线目录。</p>
      )}
      {!loaded ? (
        <p className="dashboard-loading">
          <Spinner /> 正在加载…
        </p>
      ) : items.length === 0 ? (
        <p className="route-empty">还没有订阅。</p>
      ) : (
        [...groups].map(([label, list]) =>
          list.length === 0 ? null : (
            <div className="subscription-group" key={label}>
              <h3>
                {label}
                {categories.includes(label) ? '' : '（分类未配置）'}
              </h3>
              <div className="run-table-wrap">
                <table className="run-table">
                  <thead>
                    <tr>
                      <th>订阅</th>
                      <th>状态</th>
                      <th>上次拉取</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {list.map((item) => (
                      <tr key={item.id}>
                        <td className="subscription-url">
                          {item.name && <strong>{item.name}</strong>}
                          <span className="acq-muted">{item.feed_url}</span>
                        </td>
                        <td>
                          <span className={`run-state ${stateTone(item)}`}>{stateLabel(item)}</span>
                          {item.last_error && (
                            <small className="subscription-error">{localizeBackendText(item.last_error)}</small>
                          )}
                        </td>
                        <td className="acq-muted">{formatTime(item.last_polled_at)}</td>
                        <td>
                          <div className="acq-actions">
                            <button type="button" className="text-button" disabled={busy} onClick={() => void toggle(item)}>
                              {item.enabled ? '停用' : '启用'}
                            </button>
                            {pendingDelete === item.id ? (
                              <button type="button" className="text-button" disabled={busy} onClick={() => void remove(item)}>
                                确认删除
                              </button>
                            ) : (
                              <button type="button" className="text-button" disabled={busy} onClick={() => setPendingDelete(item.id)}>
                                删除
                              </button>
                            )}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ),
        )
      )}
      <div className="settings-actions">
        <button className="button secondary" type="button" disabled={busy} onClick={() => void previewImport()}>
          {busy ? <Spinner /> : null}
          从 FreshRSS 导入（预览）
        </button>
        {importable > 0 && (
          <button className="button primary" type="button" disabled={busy} onClick={() => void applyImport()}>
            确认导入 {importable} 条
          </button>
        )}
      </div>
      {importMessage && (
        <p className="settings-hint" role="status">
          {importMessage}
        </p>
      )}
      {preview && (
        <div className="run-table-wrap">
          <table className="run-table">
            <thead>
              <tr>
                <th>FreshRSS 订阅</th>
                <th>分类</th>
                <th>结果</th>
              </tr>
            </thead>
            <tbody>
              {preview.length === 0 && (
                <tr>
                  <td colSpan={3} className="acq-muted">
                    FreshRSS 里没有订阅。
                  </td>
                </tr>
              )}
              {preview.map((entry) => (
                <tr key={entry.url}>
                  <td className="subscription-url">
                    {entry.title && <strong>{entry.title}</strong>}
                    <span className="acq-muted">{entry.url}</span>
                  </td>
                  <td>{entry.category ?? '—'}</td>
                  <td>{IMPORT_STATUS_LABELS[entry.status] ?? entry.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}
