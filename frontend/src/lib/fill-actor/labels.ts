import type { VideoState } from '../../types'

/** An AVBase name (any alias) or a JavBus star id: any short text without path separators. */
export const ACTOR_ID = /^[^/\\]{1,64}$/
export const MAX_ACTORS = 20
// Every AVID resolves to at least one actor, so more AVIDs than the actor cap can never scan.
export const MAX_AVIDS = MAX_ACTORS
/** The server's own bound on one AVID; anything longer is a paste accident, not an ID. */
export const MAX_AVID_LENGTH = 128
export const STALE_CODES = new Set([
  'expired_plan',
  'revision_mismatch',
  'unknown_plan',
  'legacy_plan_requires_rescan',
])
export const BUSINESS_PROGRESS_WARNING_SECONDS = 60
export const HEARTBEAT_WARNING_SECONDS = 35

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
  submitting: '正在加入下载队列',
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
    state: 'queued',
    label: '已排队下载',
    description: '已加入后台下载队列，自动查磁力并提交离线任务，进度见监控面板',
    tone: 'violet',
    defaultExpanded: false,
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
    description: '提交下载任务失败，原因见条目标注',
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

/** Per-video warning codes attached to scan results (fill_actor/service.py). */
export const VIDEO_WARNING_LABELS: Record<string, string> = {
  submit_failed: '提交离线任务失败',
  acquisition_failed: '提交时发生异常',
  cloud_not_configured: '未配置 CloudDrive，无法提交下载（设置 → CloudDrive）',
  task_dir_not_configured: '未配置缺失作品离线目录，无法提交下载（设置 → 补全演员）',
  brand_not_found: '无法从番号解析厂牌',
  scan_failed: '扫描时发生局部错误',
  cloud_scan_failed: '云端目录扫描失败',
  mapping_convergence_pending: '云端文件已移入，本地映射尚未同步',
  cloud_mapping_not_strm: '附加库文件不是 .strm 映射',
  invalid_strm_target: '.strm 指向的云端路径无效',
  cloud_source_missing: '云端找不到 .strm 指向的文件',
  cloud_source_name_mismatch: '云端文件名与番号不匹配',
}

export function videoWarningLabel(code: string): string {
  // Unknown codes surface verbatim so a newly added backend warning is never swallowed.
  return VIDEO_WARNING_LABELS[code] ?? code
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
