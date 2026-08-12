import { useEffect, useRef, useState } from 'react'

import { setApiToken } from '../api'
import { useApiTokenConfigured } from '../lib/apiToken'
import { KeyIcon } from './Icons'

export function ApiTokenButton({ onClick }: { onClick: () => void }) {
  const configured = useApiTokenConfigured()
  return (
    <button
      className={`token-button${configured ? ' configured' : ''}`}
      type="button"
      onClick={onClick}
      title={configured ? '当前会话已配置 API Token' : '写操作需要 API Token'}
    >
      <KeyIcon />
      <span className="token-label">API Token</span>
      <span className={`token-dot${configured ? ' on' : ''}`} aria-hidden="true" />
      <span className="visually-hidden">{configured ? '已配置' : '未配置'}</span>
    </button>
  )
}

export function ApiTokenDialog({ onClose }: { onClose: () => void }) {
  const configured = useApiTokenConfigured()
  const [value, setValue] = useState('')
  const [saved, setSaved] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    inputRef.current?.focus()
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [onClose])

  function save() {
    if (!value.trim()) return
    setApiToken(value)
    setValue('')
    setSaved(true)
  }

  function clear() {
    setApiToken('')
    setValue('')
    setSaved(false)
  }

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
        <h2 id="token-dialog-title">API Token</h2>
        <p>
          触发流水线、保存配置和移动文件都需要它。Token 就是部署时设置的 <code>EMBYX_MANAGER_API_TOKEN</code>，
          只保存在当前浏览器会话（关闭标签页即失效），不会写入服务端或构建产物。
        </p>
        <div className="token-field">
          <label htmlFor="api-token-input">API Token</label>
          <input
            id="api-token-input"
            ref={inputRef}
            type="password"
            autoComplete="off"
            placeholder={configured ? '已配置（输入新值可替换）' : '粘贴 Token'}
            value={value}
            onChange={(event) => {
              setValue(event.target.value)
              setSaved(false)
            }}
            onKeyDown={(event) => {
              if (event.key === 'Enter') save()
            }}
          />
        </div>
        <p className={`token-status${configured ? ' on' : ''}`} role="status">
          {saved ? '已保存，可以重试刚才的操作。' : configured ? '当前会话已配置 API Token。' : '当前会话未配置 API Token。'}
        </p>
        <div className="dialog-actions">
          {configured && (
            <button className="text-button" type="button" onClick={clear}>
              清除
            </button>
          )}
          <button className="button secondary" type="button" onClick={onClose}>
            关闭
          </button>
          <button className="button primary" type="button" disabled={!value.trim()} onClick={save}>
            保存 Token
          </button>
        </div>
      </div>
    </div>
  )
}
