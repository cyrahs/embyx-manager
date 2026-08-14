import cronstrue from 'cronstrue'
import 'cronstrue/locales/zh_CN'
import { useEffect, useState } from 'react'
import { useOutletContext } from 'react-router-dom'

import { ApiError, getConfigSections, isUnauthorized, testConnection, updateConfigSection } from '../api'
import type { AppContext } from '../App'
import { Notice } from '../components/Feedback'
import { Spinner } from '../components/Icons'
import { BrandRoutesField, DirRoutesField } from '../components/settings/RouteEditors'
import { useApiTokenConfigured } from '../lib/apiToken'
import type { BrandRoute, DirRoute } from '../lib/settings/routes'
import { assertDisjointSources, fromBrandRoutes, fromDirRoutes, toBrandRoutes, toDirRoutes } from '../lib/settings/routes'
import type { ConfigSection } from '../types'

type FieldKind = 'text' | 'secret' | 'boolean' | 'number' | 'lines' | 'cron' | 'dir-routes' | 'brand-routes'

type FormValue = string | boolean | DirRoute[] | BrandRoute[]

interface FieldSpec {
  key: string
  label: string
  kind: FieldKind
  hint?: string
  placeholder?: string
  /** Route editors preview absolute paths built from these sibling fields. */
  rootKeys?: { src?: string; dst?: string }
}

interface SectionSpec {
  section: string
  title: string
  description: string
  fields: FieldSpec[]
  testTarget?: 'clouddrive' | 'freshrss'
  /** Cross-field check run after every field is collected; throws a message to show. */
  validate?: (values: Record<string, unknown>) => void
}

const SECTION_SPECS: SectionSpec[] = [
  {
    section: 'clouddrive',
    title: 'CloudDrive',
    description: '115 网盘的 CloudDrive2 gRPC 连接，供离线任务、归档刷新与文件移动使用。',
    testTarget: 'clouddrive',
    fields: [
      { key: 'address', label: '服务地址', kind: 'text', placeholder: 'clouddrive.internal:19798' },
      { key: 'api_token', label: 'API Token', kind: 'secret' },
      { key: 'secure', label: '使用 TLS 连接', kind: 'boolean' },
      {
        key: 'task_dir_path',
        label: '离线任务目录',
        kind: 'text',
        placeholder: '/115/task',
        hint: '唯一的离线目录配置：RSS 摄取把全部磁力离线到这个云端目录。它的本机挂载路径应配置为归档整理的某个来源子目录路由，下载追踪会在路由表中定位完成的下载并按该路由归档。',
      },
    ],
  },
  {
    section: 'freshrss',
    title: 'FreshRSS',
    description: 'Google Reader API 兼容接口，RSS 摄取流水线从这里读取未读条目。',
    testTarget: 'freshrss',
    fields: [
      { key: 'url', label: 'API 地址', kind: 'text', placeholder: 'https://freshrss.example/api/greader.php' },
      { key: 'api_key', label: 'API Key', kind: 'secret' },
      { key: 'proxy', label: 'HTTP 代理（可选）', kind: 'text' },
    ],
  },
  {
    section: 'fill_actor',
    title: '补全演员',
    description: '媒体库根目录与文件移入设置。留空则补全演员保持未配置，其余功能不受影响。',
    fields: [
      {
        key: 'actor_root',
        label: '主片库根目录',
        kind: 'text',
        placeholder: '/media/actor',
        hint: '判断作品是否已入库的参考库，按 <根目录>/<厂牌>/<番号>.ext 查找；只读，可与其他根目录不同盘',
      },
      {
        key: 'additional_roots',
        label: '附加片库根目录（每行一个）',
        kind: 'lines',
        hint: '主库没有时在这里找可移入的文件。必须与移入目录在同一文件系统，移动才能靠 rename 完成',
      },
      {
        key: 'move_in_root',
        label: '移入目标目录',
        kind: 'text',
        placeholder: '/media/move-in',
      },
      { key: 'move_in_by_brand', label: '按厂牌分目录移入（<移入目录>/<厂牌>/）', kind: 'boolean' },
      {
        key: 'root_sentinel',
        label: '根目录哨兵文件',
        kind: 'text',
        hint: '每个根目录下都必须存在该文件，用于防止挂载失败时把空目录当成真实片库',
      },
      {
        key: 'apply_enabled',
        label: '允许移动文件',
        kind: 'boolean',
        hint: '关闭时仅可扫描、查磁力和订阅。开启需要下面三项 CloudDrive 路径全部填写',
      },
      {
        key: 'cloud_strm_mount_prefix',
        label: 'CloudDrive 挂载前缀（.strm 内部路径）',
        kind: 'text',
      },
      {
        key: 'cloud_source_roots',
        label: 'CloudDrive 来源根目录（每行一个）',
        kind: 'lines',
        hint: '必须与上面的附加片库根目录一一对应、顺序一致',
      },
      { key: 'cloud_move_in_root', label: 'CloudDrive 移入根目录', kind: 'text' },
    ],
  },
  {
    section: 'feeds',
    title: 'RSSHub / FreshRSS 集成',
    description: '补全演员页面的订阅预热与 FreshRSS 跳转所用的地址。',
    fields: [
      { key: 'rsshub_url', label: 'RSSHub 地址（服务端可达）', kind: 'text' },
      { key: 'freshrss_url', label: 'FreshRSS 站点地址（浏览器可达）', kind: 'text' },
      { key: 'freshrss_rsshub_url', label: 'RSSHub 地址（FreshRSS 可达）', kind: 'text' },
    ],
  },
  {
    section: 'rss',
    title: 'RSS 摄取',
    description: '定时从 FreshRSS 拉取并提交 115 离线任务。',
    fields: [
      { key: 'enabled', label: '启用定时调度', kind: 'boolean' },
      { key: 'interval_seconds', label: '运行间隔（秒）', kind: 'number' },
      { key: 'actor_label', label: 'Actor 标签名', kind: 'text' },
      { key: 'rank_label', label: 'Rank 标签名', kind: 'text' },
      { key: 'failed_avid_cooldown_seconds', label: '失败番号冷却（秒）', kind: 'number' },
    ],
  },
  {
    section: 'archive',
    title: '归档整理',
    description: '把下载目录中的视频规范化命名并按厂牌移动到媒体库。下载追踪与兜底扫描共用这里的目录和路由。',
    fields: [
      { key: 'enabled', label: '启用调度（含下载追踪）', kind: 'boolean' },
      {
        key: 'scan_cron',
        label: '兜底扫描时间（Cron）',
        kind: 'cron',
        placeholder: '0 4 * * *',
        hint: '五段 cron（分 时 日 月 周），按服务器本地时间执行。只影响兜底扫描；下载追踪按下面的轮询间隔独立运行。',
      },
      { key: 'src_dir', label: '来源根目录', kind: 'text', placeholder: '/downloads' },
      { key: 'dst_dir', label: '目标根目录', kind: 'text', placeholder: '/media/library' },
      { key: 'min_size_mb', label: '最小视频大小（MB）', kind: 'number' },
      {
        key: 'priority_mapping',
        label: '优先子目录路由',
        kind: 'dir-routes',
        hint: '先于普通路由处理；若同一番号已归档在某个普通目标子目录，会先把已归档文件移动到这里的目标子目录，来源目录里新到的那份保留待人工处理。来源子目录不能与下面的普通路由重复。',
        rootKeys: { src: 'src_dir', dst: 'dst_dir' },
      },
      {
        key: 'mapping',
        label: '普通子目录路由',
        kind: 'dir-routes',
        hint: '每条规则整理一个来源子目录，并把视频按厂牌分目录放进对应的目标子目录；番号已归档在优先目标子目录时会跳过并保留来源文件。两张表都为空时归档不会运行。CloudDrive 离线任务目录的挂载位置也应是其中一条路由的来源子目录，下载追踪按该路由归档完成的下载。',
        rootKeys: { src: 'src_dir', dst: 'dst_dir' },
      },
      {
        key: 'brand_mapping',
        label: '厂牌路由（可选）',
        kind: 'brand-routes',
        hint: '这里列出的厂牌不再走上面的目标子目录，改为归入目标根目录下的指定子目录。同一个厂牌只能出现一次。',
        rootKeys: { dst: 'dst_dir' },
      },
      {
        key: 'tracker_interval_seconds',
        label: '下载追踪轮询间隔（秒）',
        kind: 'number',
        hint: '查询 CloudDrive 离线任务列表的间隔。',
      },
      {
        key: 'stall_timeout_hours',
        label: '下载停滞判定（小时）',
        kind: 'number',
        hint: '进度在此时长内没有变化就判定为卡住，自动换下一个磁力链接。',
      },
      {
        key: 'max_attempts',
        label: '每个番号最多尝试磁力数',
        kind: 'number',
        hint: '下载出错、卡住、或下完发现没有可用视频时会自动换下一个；用尽后标记为待处理。',
      },
    ],
    validate: (values) =>
      assertDisjointSources(
        (values.priority_mapping ?? {}) as Record<string, string>,
        (values.mapping ?? {}) as Record<string, string>,
      ),
  },
  {
    section: 'mapping',
    title: 'STRM 映射',
    description: '把平铺的 .strm 镜像成每部一目录的结构，供 Emby 正确识别。',
    fields: [
      { key: 'enabled', label: '启用调度与实时监听', kind: 'boolean' },
      { key: 'src_dir', label: '源目录（平铺）', kind: 'text' },
      { key: 'dst_dir', label: '目标目录（Emby）', kind: 'text' },
      { key: 'debounce_seconds', label: '增量防抖（秒）', kind: 'number' },
      { key: 'full_sync_interval_seconds', label: '全量兜底间隔（秒）', kind: 'number' },
    ],
  },
  {
    section: 'avid',
    title: '番号解析规则',
    description: '影响所有流水线的番号识别。',
    fields: [
      { key: 'id_exceptions', label: '特例番号（每行一个）', kind: 'lines', hint: '标题中包含时直接返回该番号' },
      { key: 'ignored_id_patterns', label: '忽略的正则（每行一个）', kind: 'lines', hint: '匹配部分会在解析前被移除' },
    ],
  },
]

/** Live Chinese reading of a cron expression, shown under the cron input. */
function describeCron(expr: string): string {
  const trimmed = expr.trim()
  if (!trimmed) return '需要 5 段 cron 表达式，例如 0 4 * * * 表示每天 04:00。'
  try {
    return `${cronstrue.toString(trimmed, { locale: 'zh_CN', use24HourTimeFormat: true })}（服务器本地时间）`
  } catch {
    return '无法解析的 cron 表达式。'
  }
}

function toFormValue(kind: FieldKind, value: unknown): FormValue {
  switch (kind) {
    case 'boolean':
      return Boolean(value)
    case 'secret':
      return ''
    case 'lines':
      return Array.isArray(value) ? value.join('\n') : ''
    case 'dir-routes':
      return toDirRoutes(value)
    case 'brand-routes':
      return toBrandRoutes(value)
    case 'number':
      return value === null || value === undefined ? '' : String(value)
    default:
      return typeof value === 'string' ? value : ''
  }
}

function fromFormValue(kind: FieldKind, raw: FormValue): unknown {
  switch (kind) {
    case 'boolean':
      return Boolean(raw)
    case 'number': {
      const parsed = Number(raw)
      if (!Number.isFinite(parsed)) throw new Error('数值无效')
      return parsed
    }
    case 'lines':
      return String(raw)
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean)
    case 'dir-routes':
      return fromDirRoutes(raw as DirRoute[])
    case 'brand-routes':
      return fromBrandRoutes(raw as BrandRoute[])
    default:
      return String(raw)
  }
}

interface SectionFormProps {
  spec: SectionSpec
  data: ConfigSection
  onSaved: (updated: ConfigSection) => void
  onUnauthorized: () => void
}

function SectionForm({ spec, data, onSaved, onUnauthorized }: SectionFormProps) {
  const [form, setForm] = useState<Record<string, FormValue>>(() =>
    Object.fromEntries(spec.fields.map((field) => [field.key, toFormValue(field.kind, data.values[field.key])])),
  )
  const [saving, setSaving] = useState(false)
  const [testing, setTesting] = useState(false)
  const [message, setMessage] = useState<{ tone: 'ok' | 'error'; text: string } | null>(null)

  useEffect(() => {
    setForm(Object.fromEntries(spec.fields.map((field) => [field.key, toFormValue(field.kind, data.values[field.key])])))
  }, [data, spec.fields])

  function collectValues(): Record<string, unknown> {
    const values: Record<string, unknown> = {}
    for (const field of spec.fields) {
      const raw = form[field.key]
      if (field.kind === 'secret' && (!raw || raw === '')) continue
      try {
        values[field.key] = fromFormValue(field.kind, raw)
      } catch (error) {
        throw new Error(error instanceof Error ? `${field.label}：${error.message}` : `${field.label} 格式无效`)
      }
    }
    spec.validate?.(values)
    return values
  }

  async function save() {
    setSaving(true)
    setMessage(null)
    try {
      const updated = await updateConfigSection(spec.section, collectValues(), data.version)
      onSaved(updated)
      setMessage({ tone: 'ok', text: '已保存并生效。' })
    } catch (saveError) {
      if (isUnauthorized(saveError)) {
        setMessage({ tone: 'error', text: '需要 API Token 才能保存，请在弹窗中填写后重试。' })
        onUnauthorized()
      } else if (saveError instanceof ApiError && saveError.code === 'config_version_conflict') {
        setMessage({ tone: 'error', text: '配置已被其他会话修改，请刷新页面后重试。' })
      } else {
        setMessage({ tone: 'error', text: saveError instanceof Error ? saveError.message : '保存失败。' })
      }
    } finally {
      setSaving(false)
    }
  }

  async function runTest() {
    if (!spec.testTarget) return
    setTesting(true)
    setMessage(null)
    try {
      const result = await testConnection(spec.testTarget, collectValues())
      setMessage({ tone: result.ok ? 'ok' : 'error', text: result.ok ? `连接成功：${result.detail}` : `连接失败：${result.detail}` })
    } catch (testError) {
      if (isUnauthorized(testError)) onUnauthorized()
      setMessage({ tone: 'error', text: testError instanceof Error ? testError.message : '测试请求失败。' })
    } finally {
      setTesting(false)
    }
  }

  return (
    <section className="panel settings-panel" aria-labelledby={`settings-${spec.section}`}>
      <div className="panel-heading">
        <h2 id={`settings-${spec.section}`}>{spec.title}</h2>
        <span className="settings-version">v{data.version}</span>
      </div>
      <p className="settings-desc">{spec.description}</p>
      <div className="settings-fields">
        {spec.fields.map((field) => {
          const id = `${spec.section}-${field.key}`
          if (field.kind === 'boolean') {
            return (
              <label key={field.key} className="settings-field checkbox" htmlFor={id}>
                <input
                  id={id}
                  type="checkbox"
                  checked={Boolean(form[field.key])}
                  onChange={(event) => setForm((current) => ({ ...current, [field.key]: event.target.checked }))}
                />
                <span>{field.label}</span>
              </label>
            )
          }
          if (field.kind === 'dir-routes' || field.kind === 'brand-routes') {
            const root = (key: string | undefined) => (key ? String(form[key] ?? '') : '')
            return (
              <div key={field.key} className="settings-field wide" role="group" aria-labelledby={`${id}-label`}>
                <span className="settings-label" id={`${id}-label`}>
                  {field.label}
                </span>
                {field.kind === 'dir-routes' ? (
                  <DirRoutesField
                    rows={form[field.key] as DirRoute[]}
                    srcRoot={root(field.rootKeys?.src)}
                    dstRoot={root(field.rootKeys?.dst)}
                    onChange={(rows) => setForm((current) => ({ ...current, [field.key]: rows }))}
                  />
                ) : (
                  <BrandRoutesField
                    rows={form[field.key] as BrandRoute[]}
                    dstRoot={root(field.rootKeys?.dst)}
                    onChange={(rows) => setForm((current) => ({ ...current, [field.key]: rows }))}
                  />
                )}
                {field.hint && <p className="settings-hint">{field.hint}</p>}
              </div>
            )
          }
          const secretConfigured = field.kind === 'secret' && data.secrets[field.key]
          const commonProps = {
            id,
            value: String(form[field.key] ?? ''),
            onChange: (event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
              setForm((current) => ({ ...current, [field.key]: event.target.value })),
          }
          return (
            <div key={field.key} className="settings-field">
              <label htmlFor={id}>{field.label}</label>
              {field.kind === 'lines' ? (
                <textarea rows={3} spellCheck={false} {...commonProps} />
              ) : (
                <input
                  type={field.kind === 'secret' ? 'password' : field.kind === 'number' ? 'number' : 'text'}
                  autoComplete="off"
                  placeholder={secretConfigured ? '已配置（留空保持不变）' : field.placeholder}
                  {...commonProps}
                />
              )}
              {field.kind === 'cron' && (
                <p className="settings-hint cron-readout">{describeCron(String(form[field.key] ?? ''))}</p>
              )}
              {field.hint && <p className="settings-hint">{field.hint}</p>}
            </div>
          )
        })}
      </div>
      {message && <p className={`settings-message ${message.tone}`} role="status">{message.text}</p>}
      <div className="settings-actions">
        {spec.testTarget && (
          <button className="button secondary" type="button" disabled={testing || saving} onClick={() => void runTest()}>
            {testing ? <Spinner /> : null}
            测试连接
          </button>
        )}
        <button className="button primary" type="button" disabled={saving || testing} onClick={() => void save()}>
          {saving ? <Spinner /> : null}
          保存
        </button>
      </div>
    </section>
  )
}

export default function SettingsPage() {
  const { requestApiToken } = useOutletContext<AppContext>()
  const tokenConfigured = useApiTokenConfigured()
  const [sections, setSections] = useState<Record<string, ConfigSection> | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    getConfigSections(controller.signal)
      .then((list) => {
        setSections(Object.fromEntries(list.map((section) => [section.section, section])))
        setError(null)
      })
      .catch((loadError: unknown) => {
        if (loadError instanceof DOMException && loadError.name === 'AbortError') return
        setError(loadError instanceof ApiError ? loadError.message : '无法加载配置。')
      })
    return () => controller.abort()
  }, [])

  return (
    <main>
      {!tokenConfigured && (
        <Notice
          tone="warning"
          title="保存与测试连接需要登录"
          body="读取配置无需认证；保存或测试连接前，请先用部署时设置的 API Token 登录。"
          action={
            <button className="text-button" type="button" onClick={requestApiToken}>
              登录
            </button>
          }
        />
      )}
      {error && <Notice tone="error" title="配置加载失败" body={error} />}
      {!sections && !error && (
        <p className="dashboard-loading">
          <Spinner /> 正在加载配置…
        </p>
      )}
      {sections &&
        SECTION_SPECS.map((spec) => {
          const data = sections[spec.section]
          if (!data) return null
          return (
            <SectionForm
              key={spec.section}
              spec={spec}
              data={data}
              onSaved={(updated) => setSections((current) => ({ ...(current ?? {}), [updated.section]: updated }))}
              onUnauthorized={requestApiToken}
            />
          )
        })}
    </main>
  )
}
