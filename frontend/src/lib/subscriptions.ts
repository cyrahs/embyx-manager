/** Shared readings of a subscription row and the category defaults for new ones. */

import type { ConfigSection, Subscription } from '../types'

/** The category whose offline directory is fill actor's own, else the first one. */
export function defaultCategory(sections: ConfigSection[], categories: string[]): string {
  const fillActor = sections.find((section) => section.section === 'fill_actor')?.values.task_dir_path
  const rss = sections.find((section) => section.section === 'rss')?.values.categories
  if (typeof fillActor === 'string' && fillActor && Array.isArray(rss)) {
    for (const entry of rss) {
      const record = (entry ?? {}) as Record<string, unknown>
      if (record.task_dir_path === fillActor && typeof record.label === 'string' && categories.includes(record.label)) {
        return record.label
      }
    }
  }
  return categories[0] ?? ''
}

/** The category a chart feed most likely belongs to: one named like a ranking, else the first. */
export function defaultRankCategory(categories: string[]): string {
  return categories.find((label) => /rank|榜/i.test(label)) ?? categories[0] ?? ''
}

/** The offline directory a category files into, for showing where new subscriptions land. */
export function categoryDir(sections: ConfigSection[], label: string): string | null {
  const rss = sections.find((section) => section.section === 'rss')?.values.categories
  if (!Array.isArray(rss)) return null
  for (const entry of rss) {
    const record = (entry ?? {}) as Record<string, unknown>
    if (record.label === label && typeof record.task_dir_path === 'string' && record.task_dir_path) {
      return record.task_dir_path
    }
  }
  return null
}

export function stateLabel(item: Subscription): string {
  if (!item.enabled) return '停用'
  if (item.seed_pending) return '待初始化'
  return item.last_error ? '拉取出错' : '启用'
}

export function stateTone(item: Subscription): string {
  if (!item.enabled) return ''
  if (item.last_error) return 'failed'
  return item.seed_pending ? 'running' : 'completed'
}

export function formatTime(value: string | null): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return date.toLocaleString('zh-CN', { hour12: false })
}

/** AVBase addresses talent pages by name, so the id in the feed URL is no use for a link. */
export function talentPageUrl(item: Subscription): string | null {
  return item.name ? `https://www.avbase.net/talents/${encodeURIComponent(item.name)}` : null
}

/** Case-insensitive match against the name and every alias. */
export function matchesFilter(item: Subscription, query: string): boolean {
  const needle = query.trim().toLowerCase()
  if (!needle) return true
  return [item.name ?? '', ...item.aliases, item.feed_url].some((text) => text.toLowerCase().includes(needle))
}
