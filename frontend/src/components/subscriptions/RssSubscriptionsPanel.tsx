/** Plain feed subscriptions: RSSHub chart routes, sukebei searches, any RSS/Atom URL.
 *
 * A feed's URL can be corrected in place (a moved RSSHub host, a renamed
 * route); the poller keeps the cursor, so the correction does not replay items.
 */

import { useEffect, useState } from 'react'

import { defaultRankCategory } from '../../lib/subscriptions'
import type { Subscription } from '../../types'
import { CategorySelect, PolledCell, RowActions, StateCell } from './SubscriptionRowBits'

interface RssSubscriptionsPanelProps {
  items: Subscription[]
  categories: string[]
  busy: boolean
  onAdd: (url: string, category: string) => Promise<boolean>
  onToggle: (item: Subscription) => Promise<boolean>
  onSaveUrl: (item: Subscription, url: string) => Promise<boolean>
  onRemove: (item: Subscription) => Promise<boolean>
}

export function RssSubscriptionsPanel({
  items,
  categories,
  busy,
  onAdd,
  onToggle,
  onSaveUrl,
  onRemove,
}: RssSubscriptionsPanelProps) {
  const [url, setUrl] = useState('')
  const [category, setCategory] = useState('')
  const [editing, setEditing] = useState<{ id: number; url: string } | null>(null)
  const [pendingDelete, setPendingDelete] = useState<number | null>(null)

  useEffect(() => {
    setCategory((current) => (current && categories.includes(current) ? current : defaultRankCategory(categories)))
  }, [categories])

  return (
    <section aria-labelledby="rss-subscriptions-title">
      <h3 id="rss-subscriptions-title" className="visually-hidden">
        榜单订阅
      </h3>
      <p className="settings-desc">
        RSSHub 的榜单路由（如 javlibrary 最想要）、sukebei 搜索或任意 RSS/Atom 地址。标题或链接里带番号的条目会进入下载追踪。
      </p>
      <form
        className="subscription-form"
        onSubmit={(event) => {
          event.preventDefault()
          void onAdd(url, category).then((ok) => {
            if (ok) setUrl('')
          })
        }}
      >
        <CategorySelect label="分类" value={category} categories={categories} onChange={setCategory} />
        <input
          type="text"
          aria-label="Feed 地址"
          placeholder="http://rsshub.rss.svc.cluster.local/javlibrary/mostwanted/cn"
          autoComplete="off"
          spellCheck={false}
          value={url}
          onChange={(event) => setUrl(event.target.value)}
        />
        <button className="button primary" type="submit" disabled={busy || !url.trim() || !category}>
          添加
        </button>
      </form>
      {items.length === 0 ? (
        <p className="route-empty">还没有榜单订阅。</p>
      ) : (
        <div className="run-table-wrap">
          <table className="run-table">
            <thead>
              <tr>
                <th>订阅</th>
                <th>分类</th>
                <th>状态</th>
                <th>上次拉取</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={item.id}>
                  <td className="subscription-url">
                    {item.name && <strong>{item.name}</strong>}
                    {editing?.id === item.id ? (
                      <form
                        className="subscription-edit"
                        onSubmit={(event) => {
                          event.preventDefault()
                          void onSaveUrl(item, editing.url).then((ok) => {
                            if (ok) setEditing(null)
                          })
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
                  <td>{item.category}</td>
                  <StateCell item={item} />
                  <PolledCell item={item} />
                  <RowActions
                    item={item}
                    busy={busy}
                    pendingDelete={pendingDelete === item.id}
                    onToggle={() => void onToggle(item)}
                    onArmDelete={() => setPendingDelete(item.id)}
                    onDelete={() =>
                      void onRemove(item).then((ok) => {
                        if (ok) setPendingDelete(null)
                      })
                    }
                  >
                    {editing?.id !== item.id && (
                      <button
                        type="button"
                        className="text-button"
                        disabled={busy}
                        onClick={() => setEditing({ id: item.id, url: item.url ?? '' })}
                      >
                        改地址
                      </button>
                    )}
                  </RowActions>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}
