import { useCallback, useEffect, useRef, useState } from 'react'

import { ApiError, errorCodeMessage, isUnauthorized, planReplacement, startReplacement } from '../api'
import { Notice } from './Feedback'
import { Spinner } from './Icons'
import type { ReplacePlan, ReplaceResult } from '../types'

const OUTCOME_TEXT: Record<ReplaceResult['outcome'], { tone: 'neutral' | 'warning'; title: string; body: string }> = {
  submitted: {
    tone: 'neutral',
    title: '已提交离线',
    body: '新版本下载完成、确认是这个番号后，旧版本会先删进 115 回收站，再把新版本入库。进度在下载追踪里看。',
  },
  no_magnet: {
    tone: 'warning',
    title: '没找到新的磁力',
    body: '旧版本保持不动。下载追踪会按计划再找；也可以展开这个番号，手动提交一条磁力。',
  },
  submit_failed: {
    tone: 'warning',
    title: '离线提交失败',
    body: '旧版本保持不动。稍后在下载追踪里点「换下一个磁力」，或手动提交一条磁力。',
  },
}

/**
 * Replace a title the library holds with another copy (换版): preview what goes,
 * then fetch the new copy. The old copy stays until the new one is filed.
 */
export function ReplaceDialog({
  initialAvid,
  onClose,
  onStarted,
  onUnauthorized,
}: {
  initialAvid?: string
  onClose: () => void
  /** Called once the ledger took the replacement, so the panel behind can refresh. */
  onStarted: () => void
  onUnauthorized: () => void
}) {
  const [avid, setAvid] = useState(initialAvid ?? '')
  const [magnet, setMagnet] = useState('')
  const [plan, setPlan] = useState<ReplacePlan | null>(null)
  const [result, setResult] = useState<ReplaceResult | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const onUnauthorizedRef = useRef(onUnauthorized)
  useEffect(() => {
    onUnauthorizedRef.current = onUnauthorized
  })

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [onClose])

  const fail = useCallback((err: unknown, fallback: string) => {
    if (isUnauthorized(err)) {
      onUnauthorizedRef.current()
      return
    }
    setError(err instanceof ApiError ? err.message : fallback)
  }, [])

  const preview = useCallback(
    async (text: string) => {
      if (!text.trim()) return
      setBusy(true)
      setError(null)
      setResult(null)
      try {
        setPlan(await planReplacement(text.trim()))
      } catch (err) {
        setPlan(null)
        fail(err, '查询失败。')
      } finally {
        setBusy(false)
      }
    },
    [fail],
  )

  // Opened from a row: show what would go straight away.
  useEffect(() => {
    if (initialAvid) void preview(initialAvid)
  }, [initialAvid, preview])

  const start = useCallback(async () => {
    if (!plan || plan.blocked) return
    setBusy(true)
    setError(null)
    try {
      const started = await startReplacement(plan.avid, magnet.trim() || null)
      setResult(started)
      setMagnet('')
      onStarted()
    } catch (err) {
      fail(err, '换版失败。')
    } finally {
      setBusy(false)
    }
  }, [fail, magnet, onStarted, plan])

  const outcome = result ? OUTCOME_TEXT[result.outcome] : null

  return (
    <div className="dialog-backdrop" role="presentation" onMouseDown={onClose}>
      <div
        className="dialog manual-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="replace-dialog-title"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <h2 id="replace-dialog-title">换版</h2>
        <p>
          库里的视频不对（番号不符、版本太差）时，重新下载一个版本替换它。
          旧版本一直保留，直到新版本下载完成并确认是这个番号，才会删进 115 回收站。
          之前用过的磁力不会再试。
        </p>

        <form
          className="acq-magnet"
          onSubmit={(event) => {
            event.preventDefault()
            void preview(avid)
          }}
        >
          <input
            type="text"
            aria-label="番号"
            value={avid}
            placeholder="番号，例如 IPZ-661"
            onChange={(event) => {
              setAvid(event.target.value)
              setPlan(null)
              setResult(null)
            }}
          />
          <button type="submit" className="button secondary" disabled={busy || !avid.trim()}>
            查询
          </button>
        </form>

        {plan && (
          <div className="replace-plan">
            <p>
              <strong>{plan.avid}</strong>
              {plan.holdings.length > 0 ? ' 新版本入库后会删除：' : ''}
            </p>
            {plan.holdings.length > 0 && (
              <ul className="manual-results">
                {plan.holdings.map((path) => (
                  <li key={path}>
                    <code>{path}</code>
                  </li>
                ))}
              </ul>
            )}
            {plan.task_dir_path && (
              <p className="acq-muted">
                离线目录 <code>{plan.task_dir_path}</code>
              </p>
            )}
            {plan.blocked ? (
              <Notice tone="warning" title="不能换版" body={errorCodeMessage(plan.blocked)} />
            ) : (
              !result && (
                <div className="acq-magnet">
                  <input
                    type="text"
                    aria-label="磁力链接"
                    value={magnet}
                    placeholder="可选：指定磁力 magnet:?xt=urn:btih:...（留空自动查找）"
                    onChange={(event) => setMagnet(event.target.value)}
                  />
                </div>
              )
            )}
          </div>
        )}

        {error && <Notice tone="error" title="换版" body={error} />}
        {outcome &&
          (outcome.tone === 'neutral' ? (
            <p className="replace-done" role="status">
              <strong>{outcome.title}</strong> {outcome.body}
            </p>
          ) : (
            <Notice tone={outcome.tone} title={outcome.title} body={outcome.body} />
          ))}

        <div className="dialog-actions">
          <button type="button" className="button secondary" onClick={onClose}>
            关闭
          </button>
          <button
            type="button"
            className="button primary"
            disabled={busy || !plan || plan.blocked !== null || result !== null}
            onClick={() => void start()}
          >
            {busy ? <Spinner /> : null}
            开始换版
          </button>
        </div>
      </div>
    </div>
  )
}
