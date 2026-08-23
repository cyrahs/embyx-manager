/**
 * Backend diagnostics (readiness reasons, ledger notes, attempt errors, test
 * results) arrive as English sentences, including rows already stored in the
 * database. Known ones are translated here; anything unrecognized passes
 * through verbatim so new backend wording is never swallowed.
 */

const EXACT_TEXT: Record<string, string> = {
  // Pipeline / tracker readiness reasons (bootstrap.py, monitor/api.py).
  'archive is disabled': '归档流水线已停用',
  'archive source, destination, and mapping must be configured': '需要先配置归档的源目录、目标目录和厂牌映射',
  'archive source, destination, and routes must be configured': '需要先配置归档的源目录、目标目录和路由',
  'CloudDrive and its task directory must be configured': '需要先配置 CloudDrive 及其任务目录',
  'mapping source and destination directories must be configured': '需要先配置映射的源目录和目标目录',
  'FreshRSS is not configured': '尚未配置 FreshRSS',
  'CloudDrive is not configured': '尚未配置 CloudDrive',
  'CloudDrive must be configured': '需要先配置 CloudDrive',
  'CloudDrive task directory is not configured': '尚未配置 CloudDrive 任务目录',
  'at least one RSS category must be configured': '需要至少配置一个 RSS 分类',
  'at least one RSS category with an offline directory must be configured': '需要至少配置一个带离线目录的 RSS 分类',
  'at least one offline directory (an RSS category or fill actor) must be configured':
    '需要至少配置一个离线目录（RSS 分类或补全演员）',
  'the tracker is not wired up': '下载追踪尚未接入',
  // Acquisition ledger notes (tracker.py, reconcile.py, intake.py, monitor/api.py).
  'no magnet found': '未找到磁力',
  'no magnet accepted': '没有磁力提交成功',
  'no offline directory configured': '未配置离线目录',
  'every magnet was tried': '所有磁力都已尝试',
  'ignored by an operator': '已被手动忽略',
  'destination already holds this avid': '目标库中已有该番号',
  'no avid could be read': '无法从文件夹识别番号',
  'several unrelated videos in one folder': '同一文件夹中有多部无关视频',
  'no video above the size threshold': '没有超过大小阈值的视频',
  // Magnet attempt errors (tracker.py).
  'CloudDrive reported the download failed': 'CloudDrive 报告下载失败',
  'CloudDrive no longer lists this task': 'CloudDrive 中已找不到该任务',
  'failed to add the offline task': '提交离线任务失败',
  // Connection test results (config/api.py).
  connected: '服务连接正常',
  authenticated: '认证通过',
  'connection timed out': '连接超时',
  'address and api_token are required': '需要先填写服务地址和 API Token',
  'url and api_key are required': '需要先填写 API 地址和 API Key',
}

const TEXT_PATTERNS: Array<[RegExp, (match: RegExpMatchArray) => string]> = [
  // Plans stored before source_label carried the library path used this index form.
  [/^additional-(\d+)$/, (m) => `附加库 ${m[1]}`],
  [/^no progress for (\d+)d at (\d+)%$/, (m) => `已 ${m[1]} 天无进度（停在 ${m[2]}%）`],
  [/^expected (\S+) but found (\S+)$/, (m) => `预期番号 ${m[1]}，实际识别为 ${m[2]}`],
  [/^multiple avids in one folder: (.+)$/, (m) => `同一文件夹中有多个番号：${m[1]}`],
  [/^connected; task directory (.+) is accessible$/, (m) => `服务连接正常，任务目录 ${m[1]} 可访问`],
  [/^task directory not found: (.+)$/, (m) => `找不到任务目录：${m[1]}`],
  [/^authentication failed \(HTTP (\d+)\); check the API key$/, (m) => `认证失败（HTTP ${m[1]}），请检查 API Key`],
  [/^unexpected response: HTTP (\d+)$/, (m) => `响应异常（HTTP ${m[1]}）`],
  [/^connection failed: (.+)$/, (m) => `连接失败：${m[1]}`],
]

export function localizeBackendText(text: string): string {
  const exact = EXACT_TEXT[text]
  if (exact) return exact
  for (const [pattern, render] of TEXT_PATTERNS) {
    const match = text.match(pattern)
    if (match) return render(match)
  }
  return text
}
