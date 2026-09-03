/** AVBase talent subscriptions: one row per actor, added by name, alias, id or AVBase link.
 *
 * The backend resolves whatever is pasted to a talent, so the panel only
 * needs the text and a category. A new subscription ingests the feed's current
 * items unless "只追新作" is ticked; the scan page's own subscribe button
 * always skips them, since the scan just covered the backlog.
 */

import { useState } from 'react'

import { categoryDir, defaultCategory, matchesFilter, talentPageUrl } from '../../lib/subscriptions'
import type { ConfigSection, Subscription } from '../../types'
import { ExternalIcon } from '../Icons'
import { CategoryTag, FilingHint, PolledCell, RowActions, StateCell } from './SubscriptionRowBits'

const FILTER_THRESHOLD = 8

interface TalentSubscriptionsPanelProps {
  items: Subscription[]
  categories: string[]
  sections: ConfigSection[]
  busy: boolean
  onAdd: (query: string, category: string, seed: boolean) => Promise<boolean>
  onToggle: (item: Subscription) => Promise<boolean>
  onRemove: (item: Subscription) => Promise<boolean>
}

export function TalentSubscriptionsPanel({
  items,
  categories,
  sections,
  busy,
  onAdd,
  onToggle,
  onRemove,
}: TalentSubscriptionsPanelProps) {
  const [query, setQuery] = useState('')
  const [seed, setSeed] = useState(false)
  const [filter, setFilter] = useState('')
  const [pendingDelete, setPendingDelete] = useState<number | null>(null)

  // The panel decides the category: the one filing into fill actor's own directory.
  const category = defaultCategory(sections, categories)

  const visible = items.filter((item) => matchesFilter(item, filter))

  return (
    <section aria-labelledby="talent-subscriptions-title">
      <h3 id="talent-subscriptions-title" className="visually-hidden">
        演员订阅
      </h3>
      <p className="settings-desc">
        每位演员对应 AVBase 上的一个 talent，改名与别名都归到同一条。新登记的作品会进入下载追踪；在补全演员页扫描后也可一键订阅。
      </p>
      <form
        className="subscription-form"
        onSubmit={(event) => {
          event.preventDefault()
          void onAdd(query, category, seed).then((ok) => {
            if (ok) setQuery('')
          })
        }}
      >
        <input
          type="text"
          aria-label="演员名或 AVBase 链接"
          placeholder="河北彩花，或 https://www.avbase.net/talents/5022"
          autoComplete="off"
          spellCheck={false}
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        <label className="checkbox">
          <input type="checkbox" checked={seed} onChange={(event) => setSeed(event.target.checked)} />
          只追新作（不补现有条目）
        </label>
        <button className="button primary" type="submit" disabled={busy || !query.trim() || !category}>
          添加演员
        </button>
      </form>
      <FilingHint category={category} dir={categoryDir(sections, category)} />
      {items.length > FILTER_THRESHOLD && (
        <div className="subscription-filter">
          <input
            type="text"
            aria-label="筛选演员"
            placeholder="按名字或别名筛选"
            autoComplete="off"
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
          />
        </div>
      )}
      {items.length === 0 ? (
        <p className="route-empty">还没有演员订阅。</p>
      ) : visible.length === 0 ? (
        <p className="route-empty">没有匹配的演员。</p>
      ) : (
        <div className="run-table-wrap">
          <table className="run-table">
            <thead>
              <tr>
                <th>演员</th>
                <th>状态</th>
                <th>上次拉取</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((item) => {
                const page = talentPageUrl(item)
                return (
                  <tr key={item.id}>
                    <td className="subscription-url">
                      <strong>
                        {item.name ?? `talent ${item.talent_id}`}
                        <CategoryTag item={item} panelCategory={category} />
                      </strong>
                      {item.aliases.length > 0 && <span className="acq-muted">别名：{item.aliases.join('、')}</span>}
                      {page && (
                        <a className="subscription-link" href={page} target="_blank" rel="noreferrer">
                          AVBase <ExternalIcon />
                        </a>
                      )}
                    </td>
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
                    />
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}
