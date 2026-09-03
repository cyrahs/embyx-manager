/** Row model for the RSS section's category list.
 *
 * A category pairs a subscription category name with the 115 offline directory its
 * downloads belong in. The stored shape is a list of objects; the settings UI
 * edits it as rows, and these helpers convert between the two while rejecting
 * entries the pipeline would refuse.
 */

import { nextRowId } from './routes'

export interface RssCategoryRow {
  id: string
  label: string
  taskDir: string
}

export interface StoredRssCategory {
  label: string
  task_dir_path: string
}

/** Offline directories are absolute CloudDrive API paths; a trailing slash is noise. */
export function trimTaskDir(value: string): string {
  const trimmed = value.trim()
  return trimmed.length > 1 ? trimmed.replace(/\/+$/, '') : trimmed
}

export function toRssCategories(value: unknown): RssCategoryRow[] {
  if (!Array.isArray(value)) return []
  return value.map((entry) => {
    const record = (entry ?? {}) as Record<string, unknown>
    return {
      id: nextRowId(),
      label: typeof record.label === 'string' ? record.label : '',
      taskDir: typeof record.task_dir_path === 'string' ? record.task_dir_path : '',
    }
  })
}

export function fromRssCategories(rows: RssCategoryRow[]): StoredRssCategory[] {
  const categories: StoredRssCategory[] = []
  const seen = new Set<string>()
  for (const row of rows) {
    const label = row.label.trim()
    const taskDir = trimTaskDir(row.taskDir)
    if (!label && !taskDir) continue
    if (!label) throw new Error('每个分类都要填写分类名')
    // A repeated category would ingest the same items twice in one run, and its
    // second directory would silently lose to the first.
    if (seen.has(label)) throw new Error(`分类「${label}」出现了多次`)
    if (!taskDir) throw new Error(`分类「${label}」还没有指定离线目录`)
    if (!taskDir.startsWith('/')) throw new Error(`分类「${label}」的离线目录要填绝对路径`)
    seen.add(label)
    categories.push({ label, task_dir_path: taskDir })
  }
  return categories
}
