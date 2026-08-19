import type { VideoState } from '../../types'

export const ACTOR_ID = /^[A-Za-z0-9_-]{1,32}$/
export const MAX_ACTORS = 20
export const STALE_CODES = new Set([
  'expired_plan',
  'revision_mismatch',
  'unknown_plan',
  'legacy_plan_requires_rescan',
])
export const BUSINESS_PROGRESS_WARNING_SECONDS = 60
export const HEARTBEAT_WARNING_SECONDS = 35

export const FEED_STATE_LABELS = {
  queued: '等待缓存',
  warming: '缓存预热中',
  ready: '缓存已就绪',
  failed: '缓存失败',
} as const

export const STAGE_LABELS: Record<string, string> = {
  queued: '任务已排队',
  actor_catalog: '正在获取演员作品',
  actor_fetch: '正在获取演员作品',
  fetching_actors: '正在获取演员作品',
  actors: '正在获取演员作品',
  library_scan: '正在扫描本地片库',
  video_scan: '正在扫描本地片库',
  scanning_videos: '正在扫描本地片库',
  videos: '正在扫描本地片库',
  submitting: '正在提交下载任务',
  magnet_lookup: '正在查询磁力资源',
  magnet_search: '正在查询磁力资源',
  magnets: '正在查询磁力资源',
  persisting: '正在保存扫描结果',
  finalizing: '正在整理扫描结果',
  saving_plan: '正在保存扫描结果',
  done: '扫描已完成',
  unknown: '正在处理扫描任务',
}

export const UNIT_LABELS: Record<string, string> = {
  actor: '位演员',
  actors: '位演员',
  page: '页',
  pages: '页',
  video: '个作品',
  videos: '个作品',
  magnet: '个磁力查询',
  magnets: '个磁力查询',
  item: '项',
  items: '项',
  step: '步',
  steps: '步',
}

export interface VideoGroupDef {
  state: VideoState
  label: string
  description: string
  tone: string
  /** Actionable groups open on arrival; inert ones stay collapsed so the page stays scannable. */
  defaultExpanded: boolean
}

export const VIDEO_GROUPS: VideoGroupDef[] = [
  {
    state: 'additional_found',
    label: '可移入',
    description: '在附加片库中找到文件',
    tone: 'amber',
    defaultExpanded: true,
  },
  {
    state: 'submitted',
    label: '已提交下载',
    description: '已提交到下载追踪，进度见监控面板',
    tone: 'violet',
    defaultExpanded: false,
  },
  {
    state: 'submit_failed',
    label: '提交失败',
    description: '提交下载任务失败，可在监控面板手动处理',
    tone: 'red',
    defaultExpanded: true,
  },
  {
    state: 'invalid_video_id',
    label: '无法识别',
    description: '番号或厂牌无法解析',
    tone: 'red',
    defaultExpanded: false,
  },
  {
    state: 'scan_failed',
    label: '扫描失败',
    description: '扫描时发生局部错误',
    tone: 'red',
    defaultExpanded: false,
  },
  {
    state: 'missing',
    label: '未找到磁力',
    description: '各磁力源均无结果，已记录冷却，之后的扫描或订阅会重试',
    tone: 'muted',
    defaultExpanded: false,
  },
  {
    state: 'already_tracked',
    label: '已在追踪',
    description: '下载追踪系统已有该番号',
    tone: 'muted',
    defaultExpanded: false,
  },
  {
    state: 'exists',
    label: '已入库',
    description: '演员片库已有文件',
    tone: 'green',
    defaultExpanded: false,
  },
]

export const MOVE_LABELS = {
  moved: '已移入',
  stale: '源文件已变化',
  conflict: '目标冲突',
  invalid_path: '路径无效',
  failed: '移动失败',
} as const

export const ACTOR_ERROR_LABELS: Record<string, string> = {
  actor_catalog_error: '演员作品目录抓取失败',
}

/** Job-level failure codes shown in the "扫描/移动任务失败" banner. */
export const JOB_ERROR_LABELS: Record<string, string> = {
  job_cancelled: '任务已取消',
  job_interrupted: '任务被中断',
  plan_creation_failed: '扫描计划创建失败',
  apply_failed: '移动执行失败',
  apply_job_payload_missing: '任务结果缺失',
  move_disabled: '文件移动当前由管理员暂停',
  not_ready: '移动依赖尚未就绪',
  expired_plan: '计划已过期',
  revision_mismatch: '计划已更新，需要重新扫描',
  unknown_plan: '计划不存在或已被清理',
  legacy_plan_requires_rescan: '计划来自旧版本，需要重新扫描',
  network_error: '网络连接失败',
}

export function jobErrorLabel(code: string): string {
  // Unknown codes surface verbatim so a newly added backend error is never swallowed.
  return JOB_ERROR_LABELS[code] ?? code
}

export const FEED_ERROR_LABELS: Record<string, string> = {
  rsshub_timeout: 'RSSHub 请求超时',
  rsshub_network_error: 'RSSHub 网络错误',
  rsshub_http_error: 'RSSHub 响应出错',
  rsshub_invalid_feed: '订阅源内容无效',
  rsshub_not_ready: '缓存尚未就绪',
  rsshub_cancelled: '预热已取消',
}

export const MOVE_ERROR_LABELS: Record<string, string> = {
  cloud_move_status_unknown: '远端状态待确认，请勿重复操作',
  cloud_move_in_progress: '已有移动正在核验',
  cloud_move_not_observed: '云端未执行移动，请重新扫描后重试',
  cloud_move_rejected: '云端拒绝了移动请求',
  cloud_destination_missing: '无法准备目标目录',
  cloud_destination_exists: '目标位置已有文件',
  cloud_source_changed: '远端源文件已变化',
  strm_target_changed: '映射目标已变化，请重新扫描',
}
