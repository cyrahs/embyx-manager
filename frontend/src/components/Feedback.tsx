import { AlertIcon, CancelIcon } from './Icons'

/** The one feedback element every page shares; feature-specific ones live with their feature. */
export function Notice({
  tone,
  title,
  body,
  action,
}: {
  tone: 'error' | 'warning' | 'neutral'
  title: string
  body: string
  action?: React.ReactNode
}) {
  return (
    <div className={`notice notice-${tone}`} role={tone === 'neutral' ? 'status' : 'alert'}>
      <span>{tone === 'neutral' ? <CancelIcon /> : <AlertIcon />}</span>
      <div>
        <strong>{title}</strong>
        <p>{body}</p>
      </div>
      {action}
    </div>
  )
}
