import { useEffect, useRef } from 'react'

import { MAX_ACTORS } from '../../lib/fill-actor/labels'
import type { AvidActor } from '../../types'
import { AlertIcon, ScanIcon, Spinner } from '../Icons'

export function ScanPanel({
  input,
  onInputChange,
  inputMode,
  onInputModeChange,
  parsed,
  locked,
  submitting,
  resolvingAvid,
  jobPending,
  applyPending,
  onScan,
  authRequired,
  onRequestLogin,
}: {
  input: string
  onInputChange: (value: string) => void
  inputMode: 'actor' | 'avid'
  onInputModeChange: (mode: 'actor' | 'avid') => void
  parsed: { actorIds: string[]; invalid: string[]; duplicateCount: number }
  locked: boolean
  submitting: boolean
  resolvingAvid: boolean
  jobPending: boolean
  applyPending: boolean
  onScan: () => void
  authRequired: boolean
  onRequestLogin: () => void
}) {
  const actorMode = inputMode === 'actor'
  const tooMany = actorMode && parsed.actorIds.length > MAX_ACTORS
  const blocked = actorMode
    ? !parsed.actorIds.length || Boolean(parsed.invalid.length) || tooMany
    : !input.trim()

  return (
    <section className="scan-panel" aria-labelledby="scan-title">
      <div className="panel-heading">
        <h2 id="scan-title">输入补全目标</h2>
        <div className="input-mode" role="group" aria-label="输入类型">
          <button
            type="button"
            className={actorMode ? 'active' : ''}
            aria-pressed={actorMode}
            disabled={locked}
            onClick={() => onInputModeChange('actor')}
          >
            演员 ID
          </button>
          <button
            type="button"
            className={!actorMode ? 'active' : ''}
            aria-pressed={!actorMode}
            disabled={locked}
            onClick={() => onInputModeChange('avid')}
          >
            AVID
          </button>
        </div>
      </div>

      <div className={`input-frame ${actorMode && (parsed.invalid.length || tooMany) ? 'invalid' : ''}`}>
        {actorMode ? (
          <textarea
            aria-label="演员 ID"
            value={input}
            onChange={(event) => onInputChange(event.target.value)}
            placeholder={'例如：A12345, B67890\n支持空格、逗号或换行分隔'}
            rows={3}
            disabled={locked}
          />
        ) : (
          <input
            aria-label="AVID"
            value={input}
            onChange={(event) => onInputChange(event.target.value)}
            placeholder="例如：ABC-123"
            disabled={locked}
          />
        )}
        <div className="input-footer">
          <span>{actorMode ? '仅支持字母、数字、下划线和连字符' : '将从 JavBus 影片页获取演员信息'}</span>
          {actorMode && <span className={`field-count ${tooMany ? 'over' : ''}`}>{parsed.actorIds.length} / {MAX_ACTORS}</span>}
          {actorMode && parsed.duplicateCount > 0 && <span>{parsed.duplicateCount} 个重复项已自动合并</span>}
        </div>
      </div>

      {actorMode && parsed.invalid.length > 0 && (
        <p className="validation" role="alert">
          无法识别：{parsed.invalid.join('、')}
        </p>
      )}
      {actorMode && tooMany && (
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
          {resolvingAvid || submitting || jobPending || applyPending ? <Spinner /> : <ScanIcon />}
          {resolvingAvid ? '正在获取演员' : submitting ? '正在检查' : jobPending ? '正在扫描' : applyPending ? '移动处理中' : '开始扫描'}
        </button>
      </div>
    </section>
  )
}

export function AvidActorChoiceDialog({
  avid,
  actors,
  onCancel,
  onConfirm,
}: {
  avid: string
  actors: AvidActor[]
  onCancel: () => void
  onConfirm: (actorId: string) => void
}) {
  const firstChoiceRef = useRef<HTMLInputElement>(null)
  const selectedRef = useRef(actors[0]?.actor_id ?? '')

  useEffect(() => {
    firstChoiceRef.current?.focus()
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
        aria-labelledby="avid-actor-choice-title"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <span className="dialog-icon"><ScanIcon /></span>
        <h2 id="avid-actor-choice-title">选择要补全的演员</h2>
        <p>{avid} 包含多位演员，请选择本次要扫描的演员。</p>
        <div className="actor-choice-list">
          {actors.map((actor, index) => (
            <label key={actor.actor_id}>
              <input
                ref={index === 0 ? firstChoiceRef : undefined}
                type="radio"
                name="avid-actor"
                value={actor.actor_id}
                defaultChecked={index === 0}
                onChange={() => { selectedRef.current = actor.actor_id }}
              />
              <span>
                <strong>{actor.name}</strong>
                <code>{actor.actor_id}</code>
              </span>
            </label>
          ))}
        </div>
        <div className="dialog-actions">
          <button className="button secondary" type="button" onClick={onCancel}>取消</button>
          <button className="button primary" type="button" onClick={() => onConfirm(selectedRef.current)}>
            扫描所选演员
          </button>
        </div>
      </div>
    </div>
  )
}

export function ExistingSubscriptionsDialog({
  actors,
  onCancel,
  onConfirm,
}: {
  actors: Array<{ actorId: string; actorName: string | null }>
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
          {actors.map((actor) => (
            <span key={actor.actorId}>
              {actor.actorName && actor.actorName.toLowerCase() !== actor.actorId.toLowerCase()
                ? `${actor.actorName}（${actor.actorId}）`
                : actor.actorId}
            </span>
          ))}
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
