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

describe('merge page', () => {
  let titlesBody: unknown
  let tasksBody: { items: unknown[]; unavailable: string | null }
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
      if (url === '/api/merge/titles') return jsonResponse(titlesBody)
      if (url === '/api/merge/tasks' && init?.method === 'POST') {
        const body = JSON.parse(String(init.body)) as { avid: string; source?: string }
        const created = { ...TASK, id: 9, avid: body.avid, source: body.source ?? 'vr', state: 'queued', notice: null }
        tasksBody = { ...tasksBody, items: [created, ...tasksBody.items] }
        return jsonResponse(created, 201)
      }
      if (url === '/api/merge/tasks') return jsonResponse(tasksBody)
      if (url === '/api/merge/tasks/4/cancel') return jsonResponse({ ...TASK, state: 'cancelled', cancellable: false })
      return jsonResponse({ error: { code: 'not_found' } }, 404)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.spyOn(window, 'confirm').mockReturnValue(true)
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
    expect(within(many).getByText('vr')).toBeInTheDocument()
    expect(screen.queryByText('ABP-123')).not.toBeInTheDocument()

    await userEvent.click(screen.getByRole('button', { name: /9 盘及以下 · 1/ }))
    expect(screen.getByText('ABP-123')).toBeInTheDocument()
    expect(screen.getByText('clt（按下载记录）')).toBeInTheDocument()

    await userEvent.click(screen.getByRole('button', { name: /缺盘或异常 · 1/ }))
    expect(screen.getByText('缺 cd1、cd7')).toBeInTheDocument()
    expect(screen.getByText('未确定')).toBeInTheDocument()
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

    expect(window.confirm).toHaveBeenCalledWith(expect.stringContaining('删除原分盘'))
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
    expect(fetchMock).toHaveBeenCalledWith('/api/merge/tasks/4/cancel', expect.objectContaining({ method: 'POST' }))
  })

  it('disables merging when the deployment cannot run it', async () => {
    tasksBody = { items: [], unavailable: 'the merge Job template is not mounted (EMBYX_MANAGER_MERGE_JOB_TEMPLATE)' }
    renderPage()

    expect(await screen.findByText('这个部署没有挂载合并 Job 的模板')).toBeInTheDocument()
    const many = await screen.findByRole('region', { name: '10 盘及以上 · 1' })
    expect(within(many).getByRole('button', { name: '合并' })).toBeDisabled()
  })
})
