/** Subscribe the scanned actors' AVBase feeds, right where their catalog was just walked.
 *
 * The scan already covered the backlog, so a subscription made here starts with
 * a pending seed: the first poll records the feed's current items and only
 * later ones are ingested. The category defaults to the one filing into fill
 * actor's own offline directory, so the downloads land where the scan's did.
 */

import { useCallback, useEffect, useState } from 'react'

import { ApiError, getConfigSections, isUnauthorized, listSubscriptions, subscribeTalent } from '../../api'
import { defaultCategory } from '../../lib/subscriptions'
import type { ActorPlan, Subscription } from '../../types'
import { Notice } from '../Feedback'
import { Spinner } from '../Icons'

interface ActorSubscriptionsProps {
  actors: ActorPlan[]
  onUnauthorized: () => void
}

export function ActorSubscriptions({ actors, onUnauthorized }: ActorSubscriptionsProps) {
  const talents = actors.filter((actor) => typeof actor.talent_id === 'number')
  const [subscribed, setSubscribed] = useState<Map<number, Subscription>>(new Map())
  const [categories, setCategories] = useState<string[]>([])
  const [category, setCategory] = useState('')
  const [loaded, setLoaded] = useState(false)
  const [busy, setBusy] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async (signal?: AbortSignal) => {
    try {
      const [page, sections] = await Promise.all([listSubscriptions(signal), getConfigSections(signal)])
      setSubscribed(new Map(page.items.filter((item) => item.talent_id !== null).map((item) => [item.talent_id as number, item])))
      setCategories(page.categories)
      setCategory((current) => (current && page.categories.includes(current) ? current : defaultCategory(sections, page.categories)))
      setError(null)
    } catch (failure) {
      if (failure instanceof DOMException && failure.name === 'AbortError') return
      setError(failure instanceof ApiError ? failure.message : '无法读取订阅状态。')
    } finally {
      setLoaded(true)
    }
  }, [])

  useEffect(() => {
    if (!talents.length) return undefined
    const controller = new AbortController()
    void load(controller.signal)
    return () => controller.abort()
    // The actor list only changes with the plan, which remounts this panel.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [load])

  async function subscribe(actor: ActorPlan) {
    if (typeof actor.talent_id !== 'number' || !category) return
    setBusy(actor.talent_id)
    try {
      const created = await subscribeTalent({
        talent_id: actor.talent_id,
        name: actor.actor_name ?? actor.actor_id,
        aliases: actor.aliases ?? [],
        category,
        seed: true,
      })
      setSubscribed((current) => new Map(current).set(created.talent_id as number, created))
      setError(null)
    } catch (failure) {
      if (isUnauthorized(failure)) onUnauthorized()
      setError(failure instanceof ApiError ? failure.message : '订阅失败。')
    } finally {
      setBusy(null)
    }
  }

  if (!actors.length) return null

  return (
    <section className="panel actor-subscriptions" aria-labelledby="actor-subscriptions-title">
      <div className="panel-heading">
        <h2 id="actor-subscriptions-title">演员订阅</h2>
        {categories.length > 1 && (
          <select aria-label="订阅分类" value={category} onChange={(event) => setCategory(event.target.value)}>
            {categories.map((label) => (
              <option key={label} value={label}>
                {label}
              </option>
            ))}
          </select>
        )}
      </div>
      <p className="settings-desc">
        订阅后，这位演员在 AVBase 登记的新作品会进入下载追踪；本次扫描已覆盖的旧作不会重复下载。
      </p>
      {error && <Notice tone="error" title="订阅操作失败" body={error} />}
      {talents.length === 0 ? (
        <p className="settings-hint">AVBase 没有收录这些演员，只能通过扫描补全，无法订阅新作。</p>
      ) : !loaded ? (
        <p className="dashboard-loading">
          <Spinner /> 正在读取订阅状态…
        </p>
      ) : (
        <ul className="actor-subscription-list">
          {talents.map((actor) => {
            const talentId = actor.talent_id as number
            const existing = subscribed.get(talentId)
            return (
              <li key={actor.actor_id} className="actor-subscription-row">
                <span className="actor-subscription-name">
                  <strong>{actor.actor_name ?? actor.actor_id}</strong>
                  {actor.aliases && actor.aliases.length > 0 && (
                    <small className="acq-muted">别名：{actor.aliases.join('、')}</small>
                  )}
                </span>
                {existing ? (
                  <span className="run-state completed">已订阅 · {existing.category}</span>
                ) : (
                  <button
                    type="button"
                    className="button secondary"
                    disabled={busy !== null || !category}
                    onClick={() => void subscribe(actor)}
                  >
                    {busy === talentId ? <Spinner /> : null}
                    订阅
                  </button>
                )}
              </li>
            )
          })}
        </ul>
      )}
    </section>
  )
}
