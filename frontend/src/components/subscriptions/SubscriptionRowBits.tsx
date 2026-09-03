/** The cells both panels render the same way: state, last poll, and the delete confirm. */

import { localizeBackendText } from '../../lib/backendText'
import { formatTime, stateLabel, stateTone } from '../../lib/subscriptions'
import type { Subscription } from '../../types'

export function StateCell({ item }: { item: Subscription }) {
  return (
    <td>
      <span className={`run-state ${stateTone(item)}`}>{stateLabel(item)}</span>
      {item.last_error && <small className="subscription-error">{localizeBackendText(item.last_error)}</small>}
    </td>
  )
}

export function PolledCell({ item }: { item: Subscription }) {
  return <td className="acq-muted">{formatTime(item.last_polled_at)}</td>
}

/** Where new rows of this panel file, and a tag for rows that were filed elsewhere. */
export function FilingHint({ category, dir }: { category: string; dir: string | null }) {
  if (!category) return null
  return (
    <p className="subscription-hint">
      新添加的归入分类「{category}」{dir ? `，下载落在 ${dir}` : ''}
    </p>
  )
}

export function CategoryTag({ item, panelCategory }: { item: Subscription; panelCategory: string }) {
  if (item.category === panelCategory) return null
  return <span className="subscription-tag">{item.category}</span>
}

interface RowActionsProps {
  item: Subscription
  busy: boolean
  pendingDelete: boolean
  onToggle: () => void
  onDelete: () => void
  onArmDelete: () => void
  children?: React.ReactNode
}

export function RowActions({ item, busy, pendingDelete, onToggle, onDelete, onArmDelete, children }: RowActionsProps) {
  return (
    <td>
      <div className="acq-actions">
        <button type="button" className="text-button" disabled={busy} onClick={onToggle}>
          {item.enabled ? '停用' : '启用'}
        </button>
        {children}
        {pendingDelete ? (
          <button type="button" className="text-button" disabled={busy} onClick={onDelete}>
            确认删除
          </button>
        ) : (
          <button type="button" className="text-button" disabled={busy} onClick={onArmDelete}>
            删除
          </button>
        )}
      </div>
    </td>
  )
}
