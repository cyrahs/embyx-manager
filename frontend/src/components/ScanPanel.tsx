import { MAX_ACTORS } from '../lib/labels'
import { ScanIcon, Spinner } from './Icons'

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
  authConfigured,
  apiTokenInput,
  onApiTokenChange,
  onSaveApiToken,
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
  authConfigured: boolean
  apiTokenInput: string
  onApiTokenChange: (value: string) => void
  onSaveApiToken: () => void
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
            <strong>需要 API Token</strong>
            <span>Token 仅保存在当前浏览器会话中，不会写入构建产物。</span>
          </div>
          <input
            aria-label="API Token"
            type="password"
            autoComplete="off"
            value={apiTokenInput}
            onChange={(event) => onApiTokenChange(event.target.value)}
          />
          <button
            className="button secondary"
            type="button"
            disabled={!apiTokenInput.trim()}
            onClick={onSaveApiToken}
          >
            保存 Token
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
          {submitting ? '正在提交' : jobPending ? '正在扫描' : applyPending ? '移动处理中' : '开始扫描'}
        </button>
        {authConfigured && !authRequired && <span className="auth-configured">当前会话已配置 API Token。</span>}
      </div>
    </section>
  )
}
