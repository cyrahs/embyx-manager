import type { ConfigSection } from '../../types'

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
