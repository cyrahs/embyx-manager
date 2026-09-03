/** The subscription list and every change to it, shared by the page's two panels. */

import { useCallback, useEffect, useState } from 'react'

import {
  ApiError,
  createSubscription,
  deleteSubscription,
  getConfigSections,
  isUnauthorized,
  listSubscriptions,
  subscribeTalent,
  updateSubscription,
} from '../../api'
import type { ConfigSection, Subscription } from '../../types'

export function useSubscriptions(onUnauthorized: () => void) {
  const [items, setItems] = useState<Subscription[]>([])
  const [categories, setCategories] = useState<string[]>([])
  const [sections, setSections] = useState<ConfigSection[]>([])
  const [loaded, setLoaded] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const load = useCallback(async (signal?: AbortSignal) => {
    try {
      const [page, config] = await Promise.all([listSubscriptions(signal), getConfigSections(signal)])
      setItems(page.items)
      setCategories(page.categories)
      setSections(config)
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

  async function run(action: () => Promise<void>, fallback: string): Promise<boolean> {
    setBusy(true)
    try {
      await action()
      setError(null)
      return true
    } catch (failure) {
      if (isUnauthorized(failure)) onUnauthorized()
      setError(failure instanceof ApiError ? failure.message : fallback)
      return false
    } finally {
      setBusy(false)
    }
  }

  const replace = (updated: Subscription) =>
    setItems((current) => current.map((entry) => (entry.id === updated.id ? updated : entry)))

  return {
    items,
    categories,
    sections,
    loaded,
    error,
    busy,
    addFeed: (url: string, category: string) =>
      run(async () => {
        const created = await createSubscription(url.trim(), category)
        setItems((current) => [...current, created])
      }, '添加订阅失败。'),
    addTalent: (query: string, category: string, seed: boolean) =>
      run(async () => {
        const created = await subscribeTalent({ name: query.trim(), category, seed })
        setItems((current) => [...current, created])
      }, '添加演员失败。'),
    toggle: (item: Subscription) =>
      run(async () => replace(await updateSubscription(item.id, { enabled: !item.enabled })), '更新订阅失败。'),
    saveUrl: (item: Subscription, url: string) =>
      run(async () => replace(await updateSubscription(item.id, { url: url.trim() })), '修改地址失败。'),
    remove: (item: Subscription) =>
      run(async () => {
        await deleteSubscription(item.id)
        setItems((current) => current.filter((entry) => entry.id !== item.id))
      }, '删除订阅失败。'),
  }
}

export type SubscriptionsState = ReturnType<typeof useSubscriptions>
