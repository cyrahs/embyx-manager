/** The rss pipeline's subscription list: which feeds are polled, under which category.
 *
 * Categories come from the RSS section above; a subscription only ever files
 * into one of them, because the category is what decides the offline directory.
 * A feed URL can be corrected in place; a talent subscription's URL follows
 * from its id and is not edited here.
 */

import { useCallback, useEffect, useState } from 'react'

import {
  ApiError,
  createSubscription,
  deleteSubscription,
  isUnauthorized,
  listSubscriptions,
  updateSubscription,
} from '../../api'
import { localizeBackendText } from '../../lib/backendText'
import type { Subscription } from '../../types'
import { Notice } from '../Feedback'
import { Spinner } from '../Icons'

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
  const [editing, setEditing] = useState<{ id: number; url: string } | null>(null)

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

  const replace = (updated: Subscription) =>
    setItems((current) => current.map((entry) => (entry.id === updated.id ? updated : entry)))

  const add = () =>
    run(async () => {
      await createSubscription(url.trim(), category)
      setUrl('')
      await load()
    }, '添加订阅失败。')

  const toggle = (item: Subscription) =>
    run(async () => replace(await updateSubscription(item.id, { enabled: !item.enabled })), '更新订阅失败。')

  const saveUrl = () =>
    run(async () => {
      if (!editing) return
      replace(await updateSubscription(editing.id, { url: editing.url.trim() }))
      setEditing(null)
    }, '修改地址失败。')

  const remove = (item: Subscription) =>
    run(async () => {
      await deleteSubscription(item.id)
      setPendingDelete(null)
      setItems((current) => current.filter((entry) => entry.id !== item.id))
    }, '删除订阅失败。')

  const groups = new Map<string, Subscription[]>()
  for (const label of categories) groups.set(label, [])
  for (const item of items) {
    const list = groups.get(item.category)
    if (list) list.push(item)
    else groups.set(item.category, [item])
  }

  return (
    <section className="panel settings-panel" aria-labelledby="settings-subscriptions">
      <div className="panel-heading">
        <h2 id="settings-subscriptions">订阅源</h2>
      </div>
      <p className="settings-desc">
        rss 流水线轮询的 feed 列表：AVBase 演员订阅（在补全演员页面添加）、RSSHub 路由、sukebei 搜索等任意
        RSS/Atom 地址。每条订阅归属一个分类，分类决定它的下载落在哪个离线目录。
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
          placeholder="https://rsshub.example/javlibrary/mostwanted/cn"
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
                          {editing?.id === item.id ? (
                            <form
                              className="subscription-edit"
                              onSubmit={(event) => {
                                event.preventDefault()
                                void saveUrl()
                              }}
                            >
                              <input
                                type="text"
                                aria-label="新的 Feed 地址"
                                autoComplete="off"
                                spellCheck={false}
                                value={editing.url}
                                onChange={(event) => setEditing({ id: item.id, url: event.target.value })}
                              />
                              <button type="submit" className="text-button" disabled={busy || !editing.url.trim()}>
                                保存
                              </button>
                              <button type="button" className="text-button" disabled={busy} onClick={() => setEditing(null)}>
                                取消
                              </button>
                            </form>
                          ) : (
                            <span className="acq-muted">{item.feed_url}</span>
                          )}
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
                            {item.kind === 'rss' && editing?.id !== item.id && (
                              <button
                                type="button"
                                className="text-button"
                                disabled={busy}
                                onClick={() => setEditing({ id: item.id, url: item.url ?? '' })}
                              >
                                改地址
                              </button>
                            )}
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
    </section>
  )
}
