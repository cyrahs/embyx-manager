import { useEffect, useRef, useState } from 'react'

import { isUnauthorized } from '../api'
import { signIn, signOut, useApiTokenConfigured } from '../lib/apiToken'
import { errorMessage } from '../lib/format'
import { KeyIcon, Spinner } from './Icons'

/**
 * Shared token entry: the server verifies the value before it is stored, so a wrong
 * token is rejected here instead of surfacing much later on the first write.
 */
function LoginForm({
  inputId,
  submitLabel,
  onSignedIn,
  children,
}: {
  inputId: string
  submitLabel: string
  onSignedIn?: () => void
  children?: React.ReactNode
}) {
  const [value, setValue] = useState('')
  const [pending, setPending] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    inputRef.current?.focus()
  }, [])

  async function submit(event: React.FormEvent) {
    event.preventDefault()
    const token = value.trim()
    if (!token || pending) return
    setPending(true)
    setError(null)
    try {
      await signIn(token)
      setValue('')
      onSignedIn?.()
    } catch (signInError: unknown) {
      setError(isUnauthorized(signInError) ? 'API Token 不正确，请检查后重试。' : errorMessage(signInError))
    } finally {
      setPending(false)
    }
  }

  return (
    <form className="login-form" onSubmit={(event) => void submit(event)}>
      <div className="token-field">
        <label htmlFor={inputId}>API Token</label>
        <input
          id={inputId}
          ref={inputRef}
          type="password"
          autoComplete="current-password"
          placeholder="粘贴部署时设置的 Token"
          value={value}
          disabled={pending}
          onChange={(event) => {
            setValue(event.target.value)
            setError(null)
          }}
        />
      </div>
      {error && (
        <p className="login-error" role="alert">
          {error}
        </p>
      )}
      <div className="login-actions">
        {children}
        <button className="button primary" type="submit" disabled={!value.trim() || pending}>
          {pending && <Spinner />}
          {pending ? '正在验证' : submitLabel}
        </button>
      </div>
    </form>
  )
}

/** Full-page gate: nothing else in the app renders until the token is accepted. */
export function LoginScreen({ notice }: { notice: string | null }) {
  return (
    <div className="login-screen">
      <main className="login-card" aria-labelledby="login-title">
        <span className="brand-mark" aria-hidden="true">E</span>
        <h1 id="login-title">登录 Embyx Manager</h1>
        <p className="login-intro">
          本服务需要 API Token 才能访问，Token 就是部署时设置的 <code>EMBYX_MANAGER_API_TOKEN</code>。
          验证通过后会保存在本浏览器，下次访问无需重新输入；退出登录即可清除。
        </p>
        {notice && (
          <p className="login-notice" role="status">
            {notice}
          </p>
        )}
        <LoginForm inputId="login-token" submitLabel="登录" />
      </main>
    </div>
  )
}

/** Mid-session re-login, so a rejected action does not throw away the page's state. */
export function LoginDialog({ onClose }: { onClose: () => void }) {
  const configured = useApiTokenConfigured()
  const [signedIn, setSignedIn] = useState(false)

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [onClose])

  return (
    <div className="dialog-backdrop" role="presentation" onMouseDown={onClose}>
      <div
        className="dialog token-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="token-dialog-title"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <span className="dialog-icon">
          <KeyIcon />
        </span>
        <h2 id="token-dialog-title">{configured ? '重新登录' : '需要登录'}</h2>
        <p>
          服务端拒绝了刚才的操作。请重新输入部署时设置的 <code>EMBYX_MANAGER_API_TOKEN</code>，
          验证通过后页面会继续之前的工作。
        </p>
        {signedIn ? (
          <>
            <p className="token-status on" role="status">
              已登录，可以重试刚才的操作。
            </p>
            <div className="dialog-actions">
              <button className="button primary" type="button" onClick={onClose}>
                关闭
              </button>
            </div>
          </>
        ) : (
          <LoginForm inputId="dialog-token" submitLabel="登录" onSignedIn={() => setSignedIn(true)}>
            <button className="button secondary" type="button" onClick={onClose}>
              关闭
            </button>
          </LoginForm>
        )}
      </div>
    </div>
  )
}

/** Top-bar account control; only meaningful on a deployment that demands a token. */
export function SignOutButton() {
  return (
    <button
      className="token-button configured"
      type="button"
      onClick={signOut}
      title="清除本浏览器保存的 API Token"
    >
      <KeyIcon />
      <span className="token-label">退出登录</span>
    </button>
  )
}
