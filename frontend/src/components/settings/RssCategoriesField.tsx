/** Row editor for the RSS section's categories.
 *
 * Each row is one FreshRSS category and the 115 offline directory its downloads
 * go to, with a preview of where a finished download will actually land so the
 * pairing is readable without knowing the stored shape.
 */

import type { RssCategoryRow } from '../../lib/settings/rssCategories'
import { trimTaskDir } from '../../lib/settings/rssCategories'
import { nextRowId } from '../../lib/settings/routes'

interface RssCategoriesFieldProps {
  rows: RssCategoryRow[]
  onChange: (rows: RssCategoryRow[]) => void
}

export function RssCategoriesField({ rows, onChange }: RssCategoriesFieldProps) {
  function patch(id: string, changes: Partial<RssCategoryRow>) {
    onChange(rows.map((row) => (row.id === id ? { ...row, ...changes } : row)))
  }

  return (
    <div className="route-rows">
      {rows.length === 0 && <p className="route-empty">还没有分类，RSS 摄取不会拉取任何条目。</p>}
      {rows.map((row) => {
        const target = trimTaskDir(row.taskDir)
        return (
          <div className="route-row" key={row.id}>
            <input
              type="text"
              aria-label="FreshRSS 分类名"
              placeholder="Rank"
              autoComplete="off"
              spellCheck={false}
              value={row.label}
              onChange={(event) => patch(row.id, { label: event.target.value })}
            />
            <span className="route-arrow" aria-hidden="true">
              →
            </span>
            <input
              type="text"
              aria-label="离线目录"
              placeholder="/115/embyx_in/rank"
              autoComplete="off"
              spellCheck={false}
              value={row.taskDir}
              onChange={(event) => patch(row.id, { taskDir: event.target.value })}
            />
            <button
              className="route-remove"
              type="button"
              aria-label={row.label ? `删除分类 ${row.label}` : '删除这个分类'}
              onClick={() => onChange(rows.filter((other) => other.id !== row.id))}
            >
              移除
            </button>
            {row.label.trim() && target && (
              <p className="route-preview">
                {row.label.trim()} 的条目离线到 {target}
              </p>
            )}
          </div>
        )
      })}
      <button
        className="text-button route-add"
        type="button"
        onClick={() => onChange([...rows, { id: nextRowId(), label: '', taskDir: '' }])}
      >
        + 添加分类
      </button>
    </div>
  )
}
