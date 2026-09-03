import { useEffect, useRef } from 'react'

import { MAX_ACTORS, MAX_AVIDS } from '../../lib/fill-actor/labels'
import type { AvidActors } from '../../types'
import { ScanIcon, Spinner } from '../Icons'

export function ScanPanel({
  input,
  onInputChange,
  inputMode,
  onInputModeChange,
  parsed,
  parsedAvids,
  locked,
  submitting,
  resolvingAvid,
  avidProgress,
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
  parsedAvids: { avids: string[]; invalid: string[]; duplicateCount: number }
  locked: boolean
  submitting: boolean
  resolvingAvid: boolean
  /** Which AVID of how many is being looked up, so a batch does not look stuck. */
  avidProgress: { completed: number; total: number } | null
  jobPending: boolean
  applyPending: boolean
  onScan: () => void
  authRequired: boolean
  onRequestLogin: () => void
}) {
  const actorMode = inputMode === 'actor'
  const counted = actorMode ? parsed.actorIds : parsedAvids.avids
  const invalid = actorMode ? parsed.invalid : parsedAvids.invalid
  const duplicateCount = actorMode ? parsed.duplicateCount : parsedAvids.duplicateCount
  const limit = actorMode ? MAX_ACTORS : MAX_AVIDS
  const tooMany = counted.length > limit
  const blocked = !counted.length || Boolean(invalid.length) || tooMany
  const avidRef = useRef<HTMLTextAreaElement>(null)
  const actorRef = useRef<HTMLTextAreaElement>(null)

  // Enter belongs to the textarea — both fields take lists — so submitting takes the modifier.
  const submitOnModifiedEnter = (event: React.KeyboardEvent) => {
    if (event.key !== 'Enter' || !(event.metaKey || event.ctrlKey) || event.nativeEvent.isComposing) return
    event.preventDefault()
    if (!blocked && !locked) onScan()
  }

  // Put the caret where the operator is about to type, but only once they pick a mode:
  // stealing focus on first paint would scroll a recovered scan's results out of view.
  const focusedMode = useRef<'actor' | 'avid' | null>(null)
  useEffect(() => {
    if (locked || focusedMode.current === inputMode) return
    if (focusedMode.current !== null) (actorMode ? actorRef : avidRef).current?.focus()
    focusedMode.current = inputMode
  }, [actorMode, inputMode, locked])

  return (
    <section className="scan-panel" aria-labelledby="scan-title">
      <div className="panel-heading">
        <h2 id="scan-title">输入补全目标</h2>
        <div className="input-mode" role="group" aria-label="输入类型">
          <button
            type="button"
            className={!actorMode ? 'active' : ''}
            aria-pressed={!actorMode}
            disabled={locked}
            onClick={() => onInputModeChange('avid')}
          >
            AVID
          </button>
          <button
            type="button"
            className={actorMode ? 'active' : ''}
            aria-pressed={actorMode}
            disabled={locked}
            onClick={() => onInputModeChange('actor')}
          >
            演员
          </button>
        </div>
      </div>

      <div className={`input-frame ${invalid.length || tooMany ? 'invalid' : ''}`}>
        {actorMode ? (
          <textarea
            ref={actorRef}
            aria-label="演员"
            value={input}
            onChange={(event) => onInputChange(event.target.value)}
            onKeyDown={submitOnModifiedEnter}
            placeholder={'例如：石川澪, 河北彩伽, rwt\n支持空格、逗号或换行分隔'}
            rows={3}
            disabled={locked}
          />
        ) : (
          <textarea
            ref={avidRef}
            aria-label="AVID"
            value={input}
            onChange={(event) => onInputChange(event.target.value)}
            onKeyDown={submitOnModifiedEnter}
            placeholder={'例如：ABC-123\n支持换行分隔多个番号'}
            rows={3}
            disabled={locked}
          />
        )}
        <div className="input-footer">
          <span>{actorMode ? 'AVBase 演员名（任一别名）或 JavBus 演员 ID' : '将从 AVBase（或 JavBus）影片页获取出演者'}</span>
          <span className={`field-count ${tooMany ? 'over' : ''}`}>{counted.length} / {limit}</span>
          {duplicateCount > 0 && <span>{duplicateCount} 个重复项已自动合并</span>}
        </div>
      </div>

      {invalid.length > 0 && (
        <p className="validation" role="alert">
          无法识别：{invalid.join('、')}
        </p>
      )}
      {tooMany && (
        <p className="validation" role="alert">
          {actorMode ? `每次最多扫描 ${MAX_ACTORS} 位演员。` : `每次最多输入 ${MAX_AVIDS} 个番号。`}
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
          {resolvingAvid
            ? avidProgress && avidProgress.total > 1
              ? `正在获取演员 ${avidProgress.completed} / ${avidProgress.total}`
              : '正在获取演员'
            : submitting ? '正在检查' : jobPending ? '正在扫描' : applyPending ? '移动处理中' : '开始扫描'}
        </button>
      </div>
    </section>
  )
}

export function AvidActorChoiceDialog({
  choices,
  onCancel,
  onConfirm,
}: {
  /** Every AVID of this batch that credited more than one actor, in input order. */
  choices: AvidActors[]
  onCancel: () => void
  onConfirm: (actorIds: string[]) => void
}) {
  const firstChoiceRef = useRef<HTMLInputElement>(null)
  // One pick per AVID, pre-set to the first credit so confirming straight away still scans.
  const selectedRef = useRef(new Map(choices.map((choice) => [choice.avid, choice.actors[0]?.actor_id ?? ''])))

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
        className="dialog avid-choice-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="avid-actor-choice-title"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <span className="dialog-icon"><ScanIcon /></span>
        <h2 id="avid-actor-choice-title">选择要补全的演员</h2>
        <p>
          {choices.length > 1
            ? `${choices.length} 个番号各包含多位演员，请分别选择本次要扫描的演员。`
            : `${choices[0]?.avid} 包含多位演员，请选择本次要扫描的演员。`}
        </p>
        {choices.map((choice, groupIndex) => (
          <div className="actor-choice-group" key={choice.avid}>
            {choices.length > 1 && <span className="actor-choice-avid">{choice.avid}</span>}
            <div className="actor-choice-list">
              {choice.actors.map((actor, index) => (
                <label key={actor.actor_id}>
                  <input
                    ref={groupIndex === 0 && index === 0 ? firstChoiceRef : undefined}
                    type="radio"
                    name={`avid-actor-${choice.avid}`}
                    value={actor.actor_id}
                    defaultChecked={index === 0}
                    onChange={() => selectedRef.current.set(choice.avid, actor.actor_id)}
                  />
                  <span>
                    <strong>{actor.name}</strong>
                    <code>{actor.actor_id}</code>
                  </span>
                </label>
              ))}
            </div>
          </div>
        ))}
        <div className="dialog-actions">
          <button className="button secondary" type="button" onClick={onCancel}>取消</button>
          <button
            className="button primary"
            type="button"
            onClick={() => onConfirm([...selectedRef.current.values()].filter(Boolean))}
          >
            扫描所选演员
          </button>
        </div>
      </div>
    </div>
  )
}
