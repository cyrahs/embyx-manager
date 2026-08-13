import { useEffect, useRef } from 'react'
import type { ApplyResult, MoveCandidate } from '../../types'
import { AlertIcon, CheckIcon, MoveIcon } from '../Icons'

export function ApplySummary({ result }: { result: ApplyResult }) {
  const moved = result.results.filter((item) => item.state === 'moved').length
  const failed = result.results.length - moved
  const unknown = result.results.some((item) => item.error_code === 'cloud_move_status_unknown')
  return (
    <section className={`apply-summary ${failed ? 'has-errors' : ''}`} aria-live="polite">
      <span className="apply-icon">{failed ? <AlertIcon /> : <CheckIcon />}</span>
      <div>
        <strong>
          {unknown ? '部分远端状态仍在核验' : failed ? '文件处理完成，部分项目需要注意' : '所选文件已全部移入'}
        </strong>
        <p>
          {moved} 个成功
          {unknown
            ? `，${failed} 个状态未知；系统只会观察，不会自动重复移动。`
            : failed
              ? `，${failed} 个未移动。失败项目已在列表中标记。`
              : '。可继续选择其他文件或重新扫描。'}
        </p>
      </div>
    </section>
  )
}

export function ConfirmDialog({
  candidates,
  onCancel,
  onConfirm,
}: {
  candidates: MoveCandidate[]
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
        aria-labelledby="confirm-title"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <span className="dialog-icon">
          <MoveIcon />
        </span>
        <h2 id="confirm-title">确认移入 {candidates.length} 个文件？</h2>
        <p>文件将从附加片库移入待整理目录。操作会逐项执行，部分失败不会回滚已完成的文件。</p>
        <div className="confirm-list">
          {candidates.slice(0, 5).map((candidate) => (
            <span key={candidate.candidate_id} title={candidate.file_name}>
              {candidate.file_name}
            </span>
          ))}
          {candidates.length > 5 && <span>另有 {candidates.length - 5} 个文件</span>}
        </div>
        <div className="dialog-actions">
          <button className="button secondary" type="button" onClick={onCancel}>
            取消
          </button>
          <button className="button primary" type="button" ref={confirmRef} onClick={onConfirm}>
            确认移入
          </button>
        </div>
      </div>
    </div>
  )
}
