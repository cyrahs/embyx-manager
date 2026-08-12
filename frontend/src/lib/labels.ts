import type { VideoState } from '../types'

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
    state: 'magnet_found',
    label: '可下载',
    description: '已找到磁力链接',
    tone: 'violet',
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
    label: '未找到',
    description: '本地与磁力源均无结果',
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

export const MOVE_ERROR_LABELS: Record<string, string> = {
  cloud_move_status_unknown: '远端状态待确认，请勿重复操作',
  cloud_move_in_progress: '已有移动正在核验',
  cloud_destination_missing: '无法准备目标目录',
  cloud_destination_exists: '目标位置已有文件',
  cloud_source_changed: '远端源文件已变化',
  strm_target_changed: '映射目标已变化，请重新扫描',
}
