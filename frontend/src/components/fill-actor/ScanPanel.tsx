import { useEffect, useRef } from 'react'

import { MAX_ACTORS } from '../../lib/fill-actor/labels'
import { AlertIcon, ScanIcon, Spinner } from '../Icons'

export function ScanPanel({
  input,
  onInputChange,
  parsed,
  locked,
  submitting,
  jobPending,
  applyPending,
  onScan,
  authRequired,
  onRequestLogin,
}: {
  input: string
  onInputChange: (value: string) => void
  parsed: { actorIds: string[]; invalid: string[]; duplicateCount: number }
  locked: boolean
  submitting: boolean
  jobPending: boolean
  applyPending: boolean
  onScan: () => void
  authRequired: boolean
  onRequestLogin: () => void
}) {
  const tooMany = parsed.actorIds.length > MAX_ACTORS
  const blocked = !parsed.actorIds.length || Boolean(parsed.invalid.length) || tooMany

  return (
    <section className="scan-panel" aria-labelledby="scan-title">
      <div className="panel-heading">
        <h2 id="scan-title">输入演员 ID</h2>
        <span className={`field-count ${tooMany ? 'over' : ''}`}>
          {parsed.actorIds.length} / {MAX_ACTORS}
        </span>
      </div>

      <div className={`input-frame ${parsed.invalid.length || tooMany ? 'invalid' : ''}`}>
        <textarea
          aria-label="演员 ID"
          value={input}
          onChange={(event) => onInputChange(event.target.value)}
          placeholder={'例如：A12345, B67890\n支持空格、逗号或换行分隔'}
          rows={3}
          disabled={locked}
        />
        <div className="input-footer">
          <span>仅支持字母、数字、下划线和连字符</span>
          {parsed.duplicateCount > 0 && <span>{parsed.duplicateCount} 个重复项已自动合并</span>}
        </div>
      </div>

      {parsed.invalid.length > 0 && (
        <p className="validation" role="alert">
          无法识别：{parsed.invalid.join('、')}
        </p>
      )}
      {tooMany && (
        <p className="validation" role="alert">
          每次最多扫描 {MAX_ACTORS} 位演员。
        </p>
      )}

      {authRequired && (
        <div className="auth-prompt" role="group" aria-label="API 认证">
          <div>
            <strong>需要重新登录</strong>
            <span>服务端拒绝了刚才的操作，重新登录后可以继续。</span>
          </div>
          <button className="button secondary" type="button" onClick={onRequestLogin}>
            重新登录
          </button>
        </div>
      )}

      <div className="scan-actions">
        <button
          className="button primary scan-button"
          type="button"
          disabled={blocked || locked}
          onClick={onScan}
        >
          {submitting || jobPending || applyPending ? <Spinner /> : <ScanIcon />}
          {submitting ? '正在检查' : jobPending ? '正在扫描' : applyPending ? '移动处理中' : '开始扫描'}
        </button>
      </div>
    </section>
  )
}

export function ExistingSubscriptionsDialog({
  actorIds,
  onCancel,
  onConfirm,
}: {
  actorIds: string[]
  onCancel: () => void
  onConfirm: () => void
}) {
  const confirmRef = useRef<HTMLButtonElement>(null)

  useEffect(() => {
    confirmRef.current?.focus()
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onCancel()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [onCancel])

  return (
    <div className="dialog-backdrop" role="presentation" onMouseDown={onCancel}>
      <div
        className="dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="existing-subscriptions-title"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <span className="dialog-icon">
          <AlertIcon />
        </span>
        <h2 id="existing-subscriptions-title">FreshRSS 已有这些演员</h2>
        <p>以下演员已经订阅。仍要继续创建补全任务吗？</p>
        <div className="confirm-list">
          {actorIds.map((actorId) => <span key={actorId}>{actorId}</span>)}
        </div>
        <div className="dialog-actions">
          <button className="button secondary" type="button" onClick={onCancel}>
            取消
          </button>
          <button className="button primary" type="button" ref={confirmRef} onClick={onConfirm}>
            仍要继续
          </button>
        </div>
      </div>
    </div>
  )
}
