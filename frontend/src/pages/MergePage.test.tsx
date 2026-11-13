import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Outlet, Route, Routes } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import MergePage from './MergePage'

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
}

const BIG = {
  avid: 'SQTEVR-009',
  directory: 'type/vr/SQTEVR/SQTEVR-009',
  part_count: 20,
  parts: Array.from({ length: 20 }, (_, index) => index + 1),
  missing: [],
  library_dir: 'type/vr',
  brand: 'SQTEVR',
  problem: null,
  source: 'vr',
  source_basis: 'library',
  stackable: false,
  mergeable: true,
}
const SMALL = {
  ...BIG,
  avid: 'ABP-123',
  directory: 'rank/ABP/ABP-123',
  part_count: 3,
  parts: [1, 2, 3],
  library_dir: 'rank',
  brand: 'ABP',
  source: 'clt',
  source_basis: 'ledger',
  stackable: true,
}
const BROKEN = { ...BIG, avid: 'XYZ-001', part_count: 10, missing: [1, 7], mergeable: false, source: null, source_basis: null }

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/merge']}>
      <Routes>
        <Route element={<Outlet context={{ requestApiToken: vi.fn() }} />}>
          <Route path="/merge" element={<MergePage />} />
        </Route>
      </Routes>
    </MemoryRouter>,
  )
}

const TASK = {
  id: 4,
  avid: 'OLD-001',
  source: 'rank',
  library_dir: 'rank',
  part_count: 12,
  state: 'uploading',
  failed_state: null,
  phase: null,
  progress: null,
  merged_bytes: 4 * 1024 ** 3,
  uploaded_bytes: 1024 ** 3,
  upload_attempts: 1,
  error: null,
  notice: 'waiting for 115 to report the SHA-1',
  created_at: '2026-10-02T20:00:00Z',
  updated_at: '2026-10-02T20:00:00Z',
  finished_at: null,
  cancellable: true,
  retryable: false,
}

const TRANSCODING = {
  ...TASK,
  id: 7,
  avid: 'UNVRSP-003',
  part_count: 33,
  state: 'transcoding',
  merged_bytes: null,
  uploaded_bytes: null,
  notice: null,
  transcoder: 'home',
  home: true,
  transcode_paused: false,
  transcode: {
    parts_done: 3,
    part_count: 33,
    encoded_seconds: 1800,
    encoded_bytes: 5 * 1024 ** 3,
    duration: 7200,
    video_bitrate: 5_000_000,
    audio_done: 10,
    audio_state: 'audio',
    paused: false,
  },
}

const WORKER = {
  online: true,
  seen_at: '2026-11-13T06:00:00Z',
  name: 'GAMING-PC',
  version: '0.1.0',
  task_id: 7,
  state: 'encoding',
  parts: [
    { index: 3, fps: 182.4, position: 100 },
    { index: 4, fps: 176.9, position: 80 },
  ],
  upload_bps: 28_000_000,
  queue_chunks: 2,
  queue_bytes: 300 * 1024 ** 2,
}

describe('merge page', () => {
  let titlesBody: unknown
  let tasksBody: { items: unknown[]; unavailable: string | null; auto?: unknown; worker?: unknown }
  let fetchMock: ReturnType<typeof vi.fn>

  beforeEach(() => {
    titlesBody = {
      items: [BIG, BROKEN, SMALL],
      routes: ['vr', 'clt', 'rank'],
      scanned_at: '2026-10-02T20:00:00Z',
      reason: null,
    }
    tasksBody = { items: [], unavailable: null }
    fetchMock = vi.fn().mockImplementation((input, init?: RequestInit) => {
      const url = String(input)
      if (url === '/api/merge/titles' || url === '/api/merge/titles?refresh=true') return jsonResponse(titlesBody)
      if (url === '/api/merge/tasks' && init?.method === 'POST') {
        const body = JSON.parse(String(init.body)) as { avid: string; source?: string }
        const created = { ...TASK, id: 9, avid: body.avid, source: body.source ?? 'vr', state: 'queued', notice: null }
        tasksBody = { ...tasksBody, items: [created, ...tasksBody.items] }
        return jsonResponse(created, 201)
      }
      if (url === '/api/merge/tasks') return jsonResponse(tasksBody)
      if (url === '/api/merge/tasks/4/cancel') return jsonResponse({ ...TASK, state: 'cancelled', cancellable: false })
      if (url === '/api/merge/tasks/7/pause') return jsonResponse({ ...TRANSCODING, transcode_paused: true })
      if (url === '/api/merge/tasks/7/use-cpu') return jsonResponse({ ...TRANSCODING, state: 'queued', home: false })
      return jsonResponse({ error: { code: 'not_found' } }, 404)
    })
    vi.stubGlobal('fetch', fetchMock)
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  it('lists titles with ten or more parts and folds the rest', async () => {
    renderPage()

    const many = await screen.findByRole('region', { name: '10 盘及以上 · 1' })
    expect(within(many).getByText('SQTEVR-009')).toBeInTheDocument()
    expect(within(many).getByText('type/vr/SQTEVR')).toBeInTheDocument()
    // The library holds the odd whole filename as an AVID; its cell wraps instead of widening the table.
    expect(within(many).getByText('SQTEVR-009').closest('td')).toHaveClass('cell-id')
    expect(within(many).getByText('vr')).toBeInTheDocument()
    expect(screen.queryByText('ABP-123')).not.toBeInTheDocument()

    await userEvent.click(screen.getByRole('button', { name: /9 盘及以下 · 1/ }))
    expect(screen.getByText('ABP-123')).toBeInTheDocument()
    expect(screen.getByText('clt（按下载记录）')).toBeInTheDocument()

    await userEvent.click(screen.getByRole('button', { name: /缺盘或异常 · 1/ }))
    expect(screen.getByText('缺 cd1、cd7')).toBeInTheDocument()
    expect(screen.getByText('未确定')).toBeInTheDocument()
  })

  it('reads the last scan on open and walks the library again on 重新扫描', async () => {
    renderPage()

    await screen.findByRole('region', { name: '10 盘及以上 · 1' })
    expect(fetchMock).toHaveBeenCalledWith('/api/merge/titles', expect.anything())
    expect(fetchMock).not.toHaveBeenCalledWith('/api/merge/titles?refresh=true', expect.anything())
    await userEvent.click(screen.getByRole('button', { name: '重新扫描' }))
    expect(fetchMock).toHaveBeenCalledWith('/api/merge/titles?refresh=true', expect.anything())
  })

  it('explains why nothing can be scanned yet', async () => {
    titlesBody = { items: [], routes: [], scanned_at: null, reason: 'mapping.dst_dir and archive.dst_dir must be configured' }

    renderPage()

    expect(await screen.findByText('需要先配置映射的目标目录和归档的目标目录')).toBeInTheDocument()
    expect(screen.queryByRole('region', { name: /10 盘及以上/ })).not.toBeInTheDocument()
  })

  it('queues a merge after confirmation and shows it as a task', async () => {
    renderPage()

    const many = await screen.findByRole('region', { name: '10 盘及以上 · 1' })
    await userEvent.click(within(many).getByRole('button', { name: '合并' }))
    expect(fetchMock.mock.calls.some(([, init]) => init?.method === 'POST')).toBe(false)
    await userEvent.click(within(many).getByRole('button', { name: '算了' }))
    await userEvent.click(within(many).getByRole('button', { name: '合并' }))
    await userEvent.click(within(many).getByRole('button', { name: '确认合并 20 盘（会删原盘）' }))

    const post = fetchMock.mock.calls.find(([, init]) => init?.method === 'POST')
    expect(post?.[0]).toBe('/api/merge/tasks')
    expect(JSON.parse(String(post?.[1]?.body))).toEqual({ avid: 'SQTEVR-009' })
    const tasks = await screen.findByRole('region', { name: '合并任务' })
    expect(within(tasks).getByText('SQTEVR-009')).toBeInTheDocument()
    expect(within(tasks).getByText('排队中')).toBeInTheDocument()
    expect(within(many).getByText('排队中')).toBeInTheDocument()
  })

  it('asks for a source when the title has none', async () => {
    titlesBody = { ...(titlesBody as object), items: [{ ...BIG, source: null, source_basis: null }] }
    renderPage()

    const many = await screen.findByRole('region', { name: '10 盘及以上 · 1' })
    const merge = within(many).getByRole('button', { name: '合并' })
    expect(merge).toBeDisabled()
    await userEvent.selectOptions(within(many).getByRole('combobox', { name: 'SQTEVR-009 的来源资源库' }), 'clt')
    await userEvent.click(merge)
    await userEvent.click(within(many).getByRole('button', { name: /确认合并/ }))

    const post = fetchMock.mock.calls.find(([, init]) => init?.method === 'POST')
    expect(JSON.parse(String(post?.[1]?.body))).toEqual({ avid: 'SQTEVR-009', source: 'clt' })
  })

  it('shows task progress and cancels a task', async () => {
    tasksBody = { items: [TASK], unavailable: null }
    renderPage()

    const tasks = await screen.findByRole('region', { name: '合并任务' })
    expect(within(tasks).getByText('上传中')).toBeInTheDocument()
    expect(within(tasks).getByText('1.0 GiB / 4.0 GiB')).toBeInTheDocument()
    expect(within(tasks).getByText('等 115 算出 SHA-1')).toBeInTheDocument()

    await userEvent.click(within(tasks).getByRole('button', { name: '取消' }))
    expect(fetchMock).not.toHaveBeenCalledWith('/api/merge/tasks/4/cancel', expect.anything())
    await userEvent.click(within(tasks).getByRole('button', { name: '确认取消' }))
    expect(fetchMock).toHaveBeenCalledWith('/api/merge/tasks/4/cancel', expect.objectContaining({ method: 'POST' }))
  })

  it('opens the task list on running work and filters by state', async () => {
    const failed = {
      ...TASK,
      id: 3,
      avid: 'PPB-004',
      state: 'failed',
      failed_state: 'merging',
      error: 'PPB-004-cd2.wmv carries different streams than PPB-004-cd1.wmv; a stream copy would break',
      notice: null,
      retryable: true,
    }
    const done = { ...TASK, id: 2, avid: 'DONE-001', state: 'done', notice: null, cancellable: false }
    tasksBody = { items: [failed, TASK, done], unavailable: null }
    renderPage()

    const tasks = await screen.findByRole('region', { name: '合并任务' })
    expect(within(tasks).getByRole('button', { name: '进行中 1' })).toHaveAttribute('aria-pressed', 'true')
    expect(within(tasks).getByText('OLD-001')).toBeInTheDocument()
    expect(within(tasks).queryByText('PPB-004')).not.toBeInTheDocument()

    await userEvent.click(within(tasks).getByRole('button', { name: '失败 1' }))
    expect(within(tasks).getByText('PPB-004')).toBeInTheDocument()
    expect(within(tasks).queryByText('OLD-001')).not.toBeInTheDocument()
    // A long explanation wraps in its own column, so the actions stay in view.
    expect(within(tasks).getByText(/PPB-004-cd2\.wmv/).closest('td')).toHaveClass('cell-text')
    expect(within(tasks).getByRole('button', { name: '重试' }).closest('td')).toHaveClass('cell-actions')
    // Merge rows keep the accent text buttons, not the muted ones download tracking uses.
    expect(within(tasks).getByRole('button', { name: '重试' }).closest('.acq-actions')).toBeNull()

    await userEvent.click(within(tasks).getByRole('button', { name: '已完成 1' }))
    expect(within(tasks).getByText('DONE-001')).toBeInTheDocument()

    // Everything, running work first, then failures, then finished.
    await userEvent.click(within(tasks).getByRole('button', { name: '全部 3' }))
    const order = within(tasks)
      .getAllByRole('row')
      .slice(1)
      .map((row) => within(row).getAllByRole('cell')[0].textContent)
    expect(order).toEqual(['OLD-001', 'PPB-004', 'DONE-001'])
  })

  it('falls back to failures, then everything, when nothing is running', async () => {
    tasksBody = { items: [{ ...TASK, state: 'done', notice: null, cancellable: false }], unavailable: null }
    renderPage()

    const tasks = await screen.findByRole('region', { name: '合并任务' })
    expect(within(tasks).getByRole('button', { name: '全部 1' })).toHaveAttribute('aria-pressed', 'true')
    await userEvent.click(within(tasks).getByRole('button', { name: '进行中 0' }))
    expect(within(tasks).getByText('没有进行中的任务。')).toBeInTheDocument()
    await userEvent.click(within(tasks).getByRole('button', { name: '查看全部' }))
    expect(within(tasks).getByText('OLD-001')).toBeInTheDocument()
  })

  it('searches the titles and opens the folded groups that match', async () => {
    renderPage()

    await screen.findByRole('region', { name: '10 盘及以上 · 1' })
    expect(screen.queryByText('ABP-123')).not.toBeInTheDocument()

    await userEvent.type(screen.getByRole('searchbox', { name: '筛选分盘作品' }), 'abp')
    expect(screen.getByRole('region', { name: '10 盘及以上 · 匹配 0 / 1' })).toHaveTextContent('没有匹配的作品。')
    expect(within(screen.getByRole('region', { name: '9 盘及以下 · 匹配 1 / 1' })).getByText('ABP-123')).toBeInTheDocument()
    expect(screen.queryByText('SQTEVR-009')).not.toBeInTheDocument()

    await userEvent.clear(screen.getByRole('searchbox', { name: '筛选分盘作品' }))
    expect(screen.getByText('SQTEVR-009')).toBeInTheDocument()
    expect(screen.queryByText('ABP-123')).not.toBeInTheDocument()
  })

  it('says what automatic merging is doing and what it skipped', async () => {
    tasksBody = {
      items: [],
      unavailable: null,
      auto: {
        state: 'idle',
        room: 30 * 1024 ** 3,
        skipped: [{ avid: 'VRKM-1497', size: 85 * 1024 ** 3, reason: 'too_big' }],
        skipped_count: 1,
        checked_at: '2026-10-05T19:00:00Z',
      },
    }
    renderPage()

    expect(
      await screen.findByText(
        '自动合并：没有放得下的作品，稍后再看，downloads 可用 30.0 GiB（已扣预留）。跳过 VRKM-1497（放不下 85.0 GiB）',
      ),
    ).toBeInTheDocument()
  })

  it('disables merging when the deployment cannot run it', async () => {
    tasksBody = { items: [], unavailable: 'the merge Job template is not mounted (EMBYX_MANAGER_MERGE_JOB_TEMPLATE)' }
    renderPage()

    expect(await screen.findByText('这个部署没有挂载合并 Job 的模板')).toBeInTheDocument()
    const many = await screen.findByRole('region', { name: '10 盘及以上 · 1' })
    expect(within(many).getByRole('button', { name: '合并' })).toBeDisabled()
  })

  it('shows a home GPU transcode with the worker and lets it pause or move to the cluster CPU', async () => {
    tasksBody = { items: [TRANSCODING], unavailable: null, worker: WORKER }
    renderPage()

    const tasks = await screen.findByRole('region', { name: '合并任务' })
    expect(within(tasks).getByText('转码中（家里 GPU）')).toBeInTheDocument()
    expect(within(tasks).getByText('3/33 盘 25% · 已传 5.0 GiB · 集群在复制音频')).toBeInTheDocument()
    expect(
      screen.getByText(
        '家里的 GPU worker（GAMING-PC）在线，在转码：cd4 182 fps、cd5 177 fps · 上传 28.0 Mbps · 待传 2 块 300 MiB',
      ),
    ).toBeInTheDocument()

    await userEvent.click(within(tasks).getByRole('button', { name: '暂停' }))
    expect(fetchMock).toHaveBeenCalledWith('/api/merge/tasks/7/pause', expect.objectContaining({ method: 'POST' }))

    await userEvent.click(within(tasks).getByRole('button', { name: '改用集群 CPU' }))
    expect(fetchMock).not.toHaveBeenCalledWith('/api/merge/tasks/7/use-cpu', expect.anything())
    await userEvent.click(within(tasks).getByRole('button', { name: '确认改用集群 CPU' }))
    expect(fetchMock).toHaveBeenCalledWith('/api/merge/tasks/7/use-cpu', expect.objectContaining({ method: 'POST' }))
  })

  it('says when an oversize title waits for the worker while it is away', async () => {
    const waiting = { ...TRANSCODING, state: 'queued', transcode: null, notice: 'waiting for the home transcode worker' }
    tasksBody = { items: [waiting], unavailable: null, worker: { ...WORKER, online: false, task_id: null, parts: [] } }
    renderPage()

    const tasks = await screen.findByRole('region', { name: '合并任务' })
    expect(within(tasks).getByText('等家里的 GPU 转码')).toBeInTheDocument()
    expect(within(tasks).getByText('等家里的 GPU worker 上线')).toBeInTheDocument()
    expect(screen.getByText(/家里的 GPU worker（GAMING-PC）离线/)).toBeInTheDocument()
  })
})
