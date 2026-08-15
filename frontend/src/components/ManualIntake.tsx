import { useCallback, useEffect, useRef, useState } from 'react'

import { ApiError, browseOfflineDirectories, isUnauthorized, submitManualAcquisitions } from '../api'
import { Notice } from './Feedback'
import { Spinner } from './Icons'
import type { DirectoryListing, ManualEntry, ManualOutcome } from '../types'

const OUTCOME_LABELS: Record<ManualOutcome, string> = {
  submitted: '已提交离线',
  already_tracked: '已在追踪中',
  already_in_library: '库中已有',
  no_magnet: '未找到磁力',
  submit_failed: '提交失败',
  unreadable: '无法识别番号',
}

/** Pill tone per outcome; the neutral pill covers the rest. */
const OUTCOME_TONES: Partial<Record<ManualOutcome, string>> = {
  submitted: 'completed',
  already_in_library: 'completed',
  no_magnet: 'failed',
  submit_failed: 'failed',
  unreadable: 'failed',
}

/** Worth acting on first, so the operator sees the failures without scrolling. */
const OUTCOME_ORDER: ManualOutcome[] = [
  'submit_failed',
  'unreadable',
  'no_magnet',
  'already_tracked',
  'already_in_library',
  'submitted',
]

function sortEntries(entries: ManualEntry[]): ManualEntry[] {
  return [...entries].sort((a, b) => OUTCOME_ORDER.indexOf(a.outcome) - OUTCOME_ORDER.indexOf(b.outcome))
}

function ResultRow({ entry }: { entry: ManualEntry }) {
  return (
    <li>
      <span className="manual-result-id">{entry.avid ?? entry.text}</span>
      <span className={`run-state ${OUTCOME_TONES[entry.outcome] ?? ''}`}>{OUTCOME_LABELS[entry.outcome]}</span>
      {entry.archived_paths.length > 0 && (
        <span className="acq-muted manual-result-detail">{entry.archived_paths.join('、')}</span>
      )}
    </li>
  )
}

/** The CloudDrive tree, one directory at a time; only a routed one can be picked. */
function DirectoryBrowser({
  listing,
  loading,
  onOpen,
  onPick,
}: {
  listing: DirectoryListing
  loading: boolean
  onOpen: (path: string) => void
  onPick: (path: string) => void
}) {
  return (
    <div className="manual-browser">
      <div className="manual-browser-bar">
        <button
          type="button"
          className="text-button"
          disabled={listing.parent === null || loading}
          onClick={() => listing.parent !== null && onOpen(listing.parent)}
        >
          上一级
        </button>
        <code>{listing.path}</code>
        {loading && <Spinner />}
      </div>
      {listing.entries.length === 0 ? (
        <p className="results-empty">这个目录下没有子目录。</p>
      ) : (
        <ul className="manual-dir-list">
          {listing.entries.map((entry) => (
            <li key={entry.path}>
              <button type="button" className="manual-dir-name" onClick={() => onOpen(entry.path)}>
                {entry.name}
              </button>
              {entry.configured && <span className="run-state">来源目录</span>}
              {entry.routed ? (
                <button type="button" className="text-button" onClick={() => onPick(entry.path)}>
                  选择
                </button>
              ) : (
                <span className="acq-muted manual-dir-hint">无归档路由</span>
              )}
            </li>
          ))}
        </ul>
      )}
      <div className="manual-browser-foot">
        <span className="acq-muted">只有配置了归档路由的目录才能作为下载目录。</span>
      </div>
    </div>
  )
}

/** Add wanted AVIDs by hand: the third input source beside RSS and fill actor. */
export function ManualIntakeDialog({
  onClose,
  onSubmitted,
  onUnauthorized,
}: {
  onClose: () => void
  /** Called once anything reached the ledger, so the panel behind can refresh. */
  onSubmitted: () => void
  onUnauthorized: () => void
}) {
  const [text, setText] = useState('')
  const [taskDir, setTaskDir] = useState<string | null>(null)
  const [listing, setListing] = useState<DirectoryListing | null>(null)
  const [browsing, setBrowsing] = useState(false)
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState(false)
  const [entries, setEntries] = useState<ManualEntry[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  // Held in a ref because the panel behind re-renders on its poll interval: a
  // fresh callback identity in the dependency list would reload the listing
  // every few seconds, dropping the operator back at the root mid-browse.
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

  const open = useCallback(async (path: string) => {
    setLoading(true)
    setError(null)
    try {
      const next = await browseOfflineDirectories(path)
      setListing(next)
      // The remembered directory only fills an empty pick, so browsing around
      // never silently moves a choice the operator already made.
      setTaskDir((current) => current ?? next.default_path)
    } catch (err) {
      if (isUnauthorized(err)) {
        onUnauthorizedRef.current()
        return
      }
      setError(err instanceof ApiError ? err.message : '读取目录失败。')
    } finally {
      setLoading(false)
    }
  }, [])

  // The first listing doubles as the CloudDrive check and carries the remembered
  // directory, so it runs whether or not the operator opens the browser.
  useEffect(() => {
    void open('/')
  }, [open])

  const toggleBrowser = useCallback(() => {
    setBrowsing((value) => {
      // Opening lands on the directory already picked rather than the root, so
      // the remembered choice is the starting point for finding a sibling too.
      if (!value && taskDir !== null && listing?.path !== taskDir) void open(taskDir)
      return !value
    })
  }, [listing, open, taskDir])

  const submit = useCallback(async () => {
    const inputs = text.split('\n').map((line) => line.trim()).filter(Boolean)
    if (inputs.length === 0 || taskDir === null) return
    setBusy(true)
    setError(null)
    try {
      const submission = await submitManualAcquisitions(inputs, taskDir)
      setEntries(submission.items)
      setText('')
      // The results are what matters now; an open browser would push them out of view.
      setBrowsing(false)
      if (submission.items.some((item) => item.outcome === 'submitted')) onSubmitted()
    } catch (err) {
      if (isUnauthorized(err)) {
        onUnauthorizedRef.current()
        return
      }
      setError(err instanceof ApiError ? err.message : '手动添加失败。')
    } finally {
      setBusy(false)
    }
  }, [onSubmitted, taskDir, text])

  const count = text.split('\n').filter((line) => line.trim()).length

  return (
    <div className="dialog-backdrop" role="presentation" onMouseDown={onClose}>
      <div
        className="dialog manual-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="manual-dialog-title"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <h2 id="manual-dialog-title">手动添加</h2>
        <p>
          每行一个番号，可以直接粘贴文件名或标题，会按与订阅相同的规则解析。
          提交后由下载追踪接管：解析磁力、提交离线、失败自动换下一个。
        </p>

        <textarea
          className="manual-input"
          rows={6}
          value={text}
          placeholder={'ABC-123\n[JAV] def-456 1080p.mp4'}
          onChange={(event) => setText(event.target.value)}
        />

        <div className="manual-dir">
          <span className="acq-muted">离线目录</span>
          <code>{taskDir ?? '尚未选择'}</code>
          <button type="button" className="text-button" onClick={toggleBrowser}>
            {browsing ? '收起' : '浏览'}
          </button>
        </div>

        {browsing && listing !== null && (
          <DirectoryBrowser
            listing={listing}
            loading={loading}
            onOpen={(path) => void open(path)}
            onPick={(path) => {
              setTaskDir(path)
              setBrowsing(false)
            }}
          />
        )}

        {error && <Notice tone="error" title="手动添加" body={error} />}

        {entries !== null && (
          <ul className="manual-results">
            {sortEntries(entries).map((entry, index) => (
              <ResultRow key={`${entry.text}-${index}`} entry={entry} />
            ))}
          </ul>
        )}

        <div className="dialog-actions">
          <button type="button" className="button secondary" onClick={onClose}>
            关闭
          </button>
          <button
            type="button"
            className="button primary"
            disabled={busy || loading || count === 0 || taskDir === null}
            onClick={() => void submit()}
          >
            {busy ? <Spinner /> : null}
            {count > 0 ? `提交 ${count} 个番号` : '提交'}
          </button>
        </div>
      </div>
    </div>
  )
}
