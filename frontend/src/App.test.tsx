import { act, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import App from './App'
import { applyCandidates, getActiveApplyRequest, normalizeApplyJobEnvelope, normalizePlanEnvelope } from './api'
import type { FillActorPlan } from './types'

const plan: FillActorPlan = {
  plan_id: 'plan-1',
  revision: 'revision-1',
  created_at: '2026-07-13T10:00:00Z',
  expires_at: '2099-07-13T11:00:00Z',
  actors: [
    { actor_id: 'A123', scraped_count: 5, video_ids: ['ABC-001'], error_code: null },
    { actor_id: 'B456', scraped_count: 2, video_ids: ['XYZ-002'], error_code: null },
  ],
  videos: [
    {
      video_id: 'ABC-001',
      actor_ids: ['A123'],
      state: 'additional_found',
      existing_files: [],
      move_candidates: [
        { candidate_id: 'safe-1', video_id: 'ABC-001', file_name: 'ABC-001.mp4', source_label: 'additional-1', destination_conflict: false },
        { candidate_id: 'conflict-1', video_id: 'ABC-001', file_name: 'ABC-001-CD2.mp4', source_label: 'additional-2', destination_conflict: true },
      ],
      warnings: [],
    },
    {
      video_id: 'XYZ-002',
      actor_ids: ['B456'],
      state: 'submitted',
      existing_files: [],
      move_candidates: [],
      warnings: [],
    },
    {
      video_id: 'DONE-003',
      actor_ids: ['A123'],
      state: 'exists',
      existing_files: ['DONE-003.mkv'],
      move_candidates: [],
      warnings: [],
    },
  ],
}

const APPLY_REQUEST_ID = '00000000-0000-4000-8000-000000000001'

const submittedPlan: FillActorPlan = {
  ...plan,
  videos: [
    ...plan.videos,
    {
      video_id: 'XYZ-003',
      actor_ids: ['B456'],
      state: 'submitted',
      existing_files: [],
      move_candidates: [],
      warnings: [],
    },
    {
      video_id: 'XYZ-004',
      actor_ids: ['B456'],
      state: 'submit_failed',
      existing_files: [],
      move_candidates: [],
      warnings: ['submit_failed'],
    },
  ],
}

const twoCandidatePlan: FillActorPlan = {
  ...plan,
  videos: plan.videos.map((video) => video.video_id === 'ABC-001'
    ? {
        ...video,
        move_candidates: [
          video.move_candidates[0],
          {
            candidate_id: 'safe-2',
            video_id: 'ABC-002',
            file_name: 'ABC-002.mkv',
            source_label: 'additional-2',
            destination_conflict: false,
          },
          video.move_candidates[1],
        ],
      }
    : video),
}

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
}

function authorizationOf(init?: RequestInit): string | null {
  const headers = (init?.headers ?? {}) as Record<string, string>
  return headers.Authorization ?? null
}

describe('app shell', () => {
  beforeEach(() => {
    window.sessionStorage.clear()
    window.localStorage.clear()
    vi.stubGlobal('fetch', vi.fn().mockImplementation((input) => {
      const url = String(input)
      if (url.startsWith('/api/monitor/status') || url.startsWith('/api/monitor/runs')) return jsonResponse([])
      if (url.startsWith('/api/config')) return jsonResponse([])
      return jsonResponse({ status: 'ok', database: true })
    }))
  })

  afterEach(() => {
    vi.restoreAllMocks()
    window.history.pushState({}, '', '/')
  })

  it('orders the primary navigation as dashboard, fill actor, then settings', async () => {
    render(<App />)

    const navigation = await screen.findByRole('navigation', { name: '页面导航' })
    expect(within(navigation).getAllByRole('link').map((link) => link.textContent)).toEqual([
      '监控看板',
      '补全演员',
      '设置',
    ])
  })

  it('lands on the dashboard rather than a feature page when the app root is opened', async () => {
    window.history.pushState({}, '', '/')

    render(<App />)

    expect(await screen.findByRole('heading', { name: '流水线' })).toBeInTheDocument()
    expect(window.location.pathname).toBe('/dashboard')
  })

  it.each([
    ['/dashboard', '监控看板 · Embyx Manager'],
    ['/fill-actor', '补全演员 · Embyx Manager'],
    ['/settings', '设置 · Embyx Manager'],
  ])('names %s in the tab title', async (path, title) => {
    window.history.pushState({}, '', path)

    render(<App />)

    await waitFor(() => expect(document.title).toBe(title))
  })
})

describe('Fill Actor page', () => {
  beforeEach(() => {
    window.sessionStorage.clear()
    window.localStorage.clear()
    window.history.pushState({}, '', '/fill-actor')
    vi.spyOn(crypto, 'randomUUID').mockReturnValue(APPLY_REQUEST_ID)
    vi.stubGlobal('fetch', vi.fn().mockImplementation(() => jsonResponse({ status: 'ok', database: true })))
  })

  afterEach(() => {
    vi.restoreAllMocks()
    window.history.pushState({}, '', '/')
  })

  it('validates and deduplicates actor IDs before rendering grouped scan results', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' }, plan }))

    render(<App />)
    const input = screen.getByLabelText('演员 ID')
    await user.type(input, 'A123, B456 A123')
    expect(screen.getByText('1 个重复项已自动合并')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    await screen.findByText('扫描结果')
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      '/api/fill-actor/plans',
      expect.objectContaining({ body: JSON.stringify({ actor_ids: ['A123', 'B456'] }), method: 'POST' }),
    )
    expect(screen.getAllByText('可移入')).toHaveLength(2)
    expect(screen.getAllByText('已提交下载')).toHaveLength(2)
    expect(screen.getAllByText('已入库')).toHaveLength(2)
    expect(screen.getByText('目标位置已有同名文件')).toBeInTheDocument()
    expect(screen.getByRole('checkbox', { name: /ABC-001\.mp4/ })).toBeChecked()
    expect(screen.getByRole('checkbox', { name: /ABC-001-CD2\.mp4/ })).toBeDisabled()
  })

  it('resolves a single actor from an AVID and starts that actor scan directly', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        avid: 'ABC-123',
        actors: [{ actor_id: 'A123', name: '演员甲' }],
      }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
      }, 202))

    render(<App />)
    await user.click(screen.getByRole('button', { name: 'AVID' }))
    await user.type(screen.getByLabelText('AVID'), 'abc-123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    expect(await screen.findByText('扫描结果')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      '/api/fill-actor/avid-actors',
      expect.objectContaining({ body: JSON.stringify({ avid: 'abc-123' }), method: 'POST' }),
    )
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans',
      expect.objectContaining({ body: JSON.stringify({ actor_ids: ['A123'] }), method: 'POST' }),
    )
  })

  it('asks which actor to scan when an AVID has multiple actors', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        avid: 'ABC-123',
        actors: [
          { actor_id: 'A123', name: '演员甲' },
          { actor_id: 'B456', name: '演员乙' },
        ],
      }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
      }, 202))

    render(<App />)
    await user.click(screen.getByRole('button', { name: 'AVID' }))
    await user.type(screen.getByLabelText('AVID'), 'ABC-123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    const dialog = await screen.findByRole('dialog', { name: '选择要补全的演员' })
    await user.click(within(dialog).getByRole('radio', { name: /演员乙/ }))
    await user.click(within(dialog).getByRole('button', { name: '扫描所选演员' }))

    expect(await screen.findByText('扫描结果')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans',
      expect.objectContaining({ body: JSON.stringify({ actor_ids: ['B456'] }), method: 'POST' }),
    )
  })

  it('asks before scanning actors that already exist in FreshRSS', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        error: {
          code: 'actors_already_subscribed',
          actor_ids: ['A123'],
          actors: [{ actor_id: 'A123', actor_name: '演员甲' }],
        },
      }, 409))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
      }, 202))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123 B456')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    const dialog = await screen.findByRole('dialog', { name: 'FreshRSS 已有这些演员' })
    expect(within(dialog).getByText('演员甲（A123）')).toBeInTheDocument()
    expect(screen.queryByText('扫描结果')).not.toBeInTheDocument()
    expect(screen.getByLabelText('演员 ID')).toBeDisabled()

    await user.click(within(dialog).getByRole('button', { name: '仍要继续' }))

    expect(await screen.findByText('扫描结果')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      '/api/fill-actor/plans',
      expect.objectContaining({ body: JSON.stringify({ actor_ids: ['A123', 'B456'] }), method: 'POST' }),
    )
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans',
      expect.objectContaining({
        body: JSON.stringify({ actor_ids: ['A123', 'B456'], continue_if_subscribed: true }),
        method: 'POST',
      }),
    )
  })

  it('shows queued progress and polls until the persisted plan is ready', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'job-1', plan_id: 'plan-1', state: 'queued' }, plan: null }, 202))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' }, plan }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    expect(await screen.findByText('任务已排队')).toBeInTheDocument()
    expect(screen.getByRole('progressbar', { name: '任务已排队进度' })).not.toHaveAttribute('aria-valuenow')
    expect(screen.getByRole('progressbar', { name: '任务已排队进度' })).toHaveAttribute('aria-valuetext', '进度计算中')
    expect(await screen.findByText('扫描结果', {}, { timeout: 2_000 })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans/plan-1',
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
  })

  it('renders stage progress, timing, ETA, freshness warnings, and accessible progress semantics', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    const now = Date.now()
    const running = {
      job: {
        job_id: 'job-1',
        plan_id: 'plan-1',
        state: 'running',
        updated_at: new Date(now - 36_000).toISOString(),
        progress: {
          stage: 'library_scan',
          completed: 3,
          total: 10,
          unit: 'videos',
          current: 'ABC-004',
          stage_started_at: new Date(now - 125_000).toISOString(),
          updated_at: new Date(now - 61_000).toISOString(),
          percent: 30,
          eta_seconds: 95,
          elapsed_seconds: 125,
          last_progress_seconds: 61,
        },
      },
      plan: null,
    }
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementation(() => jsonResponse(running, 202))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    expect(await screen.findByText('正在扫描本地片库')).toBeInTheDocument()
    expect(screen.getByText('当前：ABC-004')).toBeInTheDocument()
    expect(screen.getByText('3 / 10 个作品')).toBeInTheDocument()
    expect(screen.getByText(/2 分 [5-7] 秒/)).toBeInTheDocument()
    expect(screen.getByText('约 1 分 35 秒')).toBeInTheDocument()
    expect(screen.getByText(/1 分 [1-3] 秒前/)).toBeInTheDocument()
    expect(screen.getByText('较长时间无新结果，仍可能在等待外部服务。')).toBeInTheDocument()
    expect(screen.getByText('执行器心跳已较长时间未更新，执行可能已经中断。')).toBeInTheDocument()
    expect(screen.getByRole('progressbar', { name: '正在扫描本地片库进度' })).toHaveAttribute('aria-valuenow', '30')
    expect(screen.getByRole('progressbar', { name: '正在扫描本地片库进度' })).toHaveAttribute(
      'aria-valuetext',
      '3 / 10 个作品',
    )
    const elapsedValue = screen.getByText('阶段已用时').parentElement?.querySelector('dd') ?? null
    expect(elapsedValue).not.toBeNull()
    const initialElapsed = elapsedValue?.textContent ?? ''
    await waitFor(() => expect(elapsedValue).not.toHaveTextContent(initialElapsed), { timeout: 1_800 })
  })

  it('keeps a running task through a transient poll failure and retries automatically', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'job-1', plan_id: 'plan-1', state: 'running' }, plan: null }, 202))
      .mockImplementationOnce(() => Promise.reject(new TypeError('temporary offline')))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' }, plan }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    expect(await screen.findByText('暂时无法刷新任务状态，将自动重试。', {}, { timeout: 2_000 })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '正在扫描' })).toBeDisabled()
    expect(screen.queryByText('操作未完成')).not.toBeInTheDocument()
    expect(await screen.findByText('扫描结果', {}, { timeout: 4_000 })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      4,
      '/api/fill-actor/plans/plan-1',
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
  })

  it('recovers an active scan from session storage and clears it after completion', async () => {
    window.sessionStorage.setItem('embyx-manager-fill-actor-plan-id', 'resume-1')
    const resumedPlan = { ...plan, plan_id: 'resume-1' }
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'resume-1', plan_id: 'resume-1', state: 'completed' }, plan: resumedPlan }))

    render(<App />)

    expect(await screen.findByText('正在恢复扫描状态')).toBeInTheDocument()
    expect(await screen.findByText('扫描结果', {}, { timeout: 2_000 })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      '/api/fill-actor/plans/resume-1',
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
    await waitFor(() => expect(window.sessionStorage.getItem('embyx-manager-fill-actor-plan-id')).toBeNull())
  })

  it('recovers a scan a previous version stored under the unprefixed session key', async () => {
    window.sessionStorage.setItem('embyx-manager-active-plan-id', 'legacy-1')
    const resumedPlan = { ...plan, plan_id: 'legacy-1' }
    vi.mocked(fetch)
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'legacy-1', plan_id: 'legacy-1', state: 'completed' }, plan: resumedPlan }))

    render(<App />)

    expect(await screen.findByText('扫描结果', {}, { timeout: 2_000 })).toBeInTheDocument()
    expect(window.sessionStorage.getItem('embyx-manager-active-plan-id')).toBeNull()
  })

  it('requires confirmation and displays per-file apply results', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok', fill_actor: { apply_enabled: true, apply_ready: true } }))
      .mockImplementationOnce(() => jsonResponse(plan))
      .mockImplementationOnce(() =>
        jsonResponse({
          job: {
            job_id: APPLY_REQUEST_ID,
            plan_id: 'plan-1',
            operation: 'apply',
            state: 'queued',
            progress: { stage: 'queued', completed: 0, total: 1, unit: 'items', percent: 0 },
          },
          result: null,
        }, 202),
      )
      .mockImplementationOnce(() =>
        jsonResponse({
          job: {
            job_id: APPLY_REQUEST_ID,
            plan_id: 'plan-1',
            operation: 'apply',
            state: 'completed',
            progress: { stage: 'done', completed: 1, total: 1, unit: 'items', percent: 100 },
          },
          result: {
            plan_id: 'plan-1',
            revision: 'revision-1',
            state: 'succeeded',
            results: [{ candidate_id: 'safe-1', video_id: 'ABC-001', file_name: 'ABC-001.mp4', state: 'moved', error_code: null }],
          },
        }),
      )

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')

    await user.click(screen.getByRole('button', { name: '确认并移入' }))
    const dialog = screen.getByRole('dialog')
    expect(within(dialog).getByText('确认移入 1 个文件？')).toBeInTheDocument()
    await user.click(within(dialog).getByRole('button', { name: '确认移入' }))

    await screen.findByText('所选文件已全部移入', {}, { timeout: 3_000 })
    expect(screen.getByText('已移入')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans/plan-1/apply-jobs',
      expect.objectContaining({ method: 'POST' }),
    )
    const applyBody = JSON.parse(String(fetchMock.mock.calls[2]?.[1]?.body)) as Record<string, unknown>
    expect(applyBody).toMatchObject({ revision: 'revision-1', candidate_ids: ['safe-1'] })
    expect(applyBody.request_id).toMatch(/^[A-Za-z0-9_-]{16,128}$/)
    expect(fetchMock).toHaveBeenNthCalledWith(
      4,
      `/api/fill-actor/apply-jobs/${APPLY_REQUEST_ID}`,
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
    expect(fetchMock).not.toHaveBeenCalledWith(
      '/api/fill-actor/plans/plan-1/apply',
      expect.anything(),
    )
  })

  it('shows true file progress, locks mutable controls, and keeps a terminal 100% panel', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    let resolveTerminal!: (response: Response) => void
    const terminalResponse = new Promise<Response>((resolve) => {
      resolveTerminal = resolve
    })
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok', fill_actor: { apply_enabled: true, apply_ready: true } }))
      .mockImplementationOnce(() => jsonResponse(twoCandidatePlan))
      .mockImplementationOnce(() => jsonResponse({
        job: {
          job_id: APPLY_REQUEST_ID,
          plan_id: 'plan-1',
          operation: 'apply',
          state: 'queued',
          progress: { stage: 'queued', completed: 0, total: 2, unit: 'items', percent: 0 },
        },
        result: null,
      }, 202))
      .mockImplementationOnce(() => jsonResponse({
        job: {
          job_id: APPLY_REQUEST_ID,
          plan_id: 'plan-1',
          operation: 'apply',
          state: 'running',
          progress: {
            stage: 'persisting',
            completed: 1,
            total: 2,
            unit: 'items',
            percent: 50,
            current: '/115/private/ABC-002.mkv',
          },
        },
        result: null,
      }))
      .mockImplementationOnce(() => terminalResponse)

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')
    await user.click(screen.getByRole('button', { name: '确认并移入' }))
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: '确认移入' }))

    expect(await screen.findByText('0 / 2 个文件')).toBeInTheDocument()
    expect(screen.getByRole('progressbar')).toHaveAttribute('aria-valuetext', '0 / 2 个文件')
    expect(screen.getByLabelText('演员 ID')).toBeDisabled()
    expect(screen.getByRole('button', { name: '移动处理中' })).toBeDisabled()
    expect(screen.getByRole('button', { name: '正在移入' })).toBeDisabled()
    expect(screen.getByRole('checkbox', { name: /ABC-001\.mp4/ })).toBeDisabled()
    expect(screen.queryByRole('button', { name: '取消扫描' })).not.toBeInTheDocument()

    expect(await screen.findByText('正在移入文件', {}, { timeout: 3_000 })).toBeInTheDocument()
    expect(screen.getByText('1 / 2 个文件')).toBeInTheDocument()
    expect(screen.getByText('当前：ABC-002.mkv')).toBeInTheDocument()
    expect(screen.queryByText(/\/115\/private/)).not.toBeInTheDocument()

    await act(async () => {
      resolveTerminal(new Response(JSON.stringify({
        job: {
          job_id: APPLY_REQUEST_ID,
          plan_id: 'plan-1',
          operation: 'apply',
          state: 'completed',
          progress: { stage: 'done', completed: 2, total: 2, unit: 'items', percent: 100, current: null },
        },
        result: {
          plan_id: 'plan-1',
          revision: 'revision-1',
          state: 'partial_failed',
          results: [
            { candidate_id: 'safe-1', video_id: 'ABC-001', file_name: 'ABC-001.mp4', state: 'moved', error_code: null },
            { candidate_id: 'safe-2', video_id: 'ABC-002', file_name: 'ABC-002.mkv', state: 'failed', error_code: 'cloud_destination_missing' },
          ],
        },
      }), { status: 200, headers: { 'Content-Type': 'application/json' } }))
    })

    expect(await screen.findByText('文件处理完成，部分项目需要注意')).toBeInTheDocument()
    expect(screen.getByRole('progressbar', { name: '文件移动已完成进度' })).toHaveAttribute('aria-valuenow', '100')
    expect(screen.getByRole('progressbar', { name: '文件移动已完成进度' })).toHaveAttribute('aria-valuetext', '2 / 2 个文件')
    expect(screen.getByText('所有文件均已处理。')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: '取消移动' })).not.toBeInTheDocument()
  })

  it('retains the last move progress through a transient polling failure and retries', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok', fill_actor: { apply_enabled: true, apply_ready: true } }))
      .mockImplementationOnce(() => jsonResponse(twoCandidatePlan))
      .mockImplementationOnce(() => jsonResponse({
        job: {
          job_id: APPLY_REQUEST_ID,
          plan_id: 'plan-1',
          operation: 'apply',
          state: 'running',
          progress: { stage: 'persisting', completed: 1, total: 2, unit: 'items', percent: 50, current: 'ABC-002.mkv' },
        },
        result: null,
      }, 202))
      .mockImplementationOnce(() => Promise.reject(new TypeError('temporary offline')))
      .mockImplementationOnce(() => jsonResponse({
        job: {
          job_id: APPLY_REQUEST_ID,
          plan_id: 'plan-1',
          operation: 'apply',
          state: 'completed',
          progress: { stage: 'done', completed: 2, total: 2, unit: 'items', percent: 100 },
        },
        result: {
          plan_id: 'plan-1',
          revision: 'revision-1',
          state: 'succeeded',
          results: [
            { candidate_id: 'safe-1', video_id: 'ABC-001', file_name: 'ABC-001.mp4', state: 'moved', error_code: null },
            { candidate_id: 'safe-2', video_id: 'ABC-002', file_name: 'ABC-002.mkv', state: 'moved', error_code: null },
          ],
        },
      }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')
    await user.click(screen.getByRole('button', { name: '确认并移入' }))
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: '确认移入' }))

    expect(await screen.findByText('暂时无法刷新移动任务状态，将自动重试。', {}, { timeout: 3_000 })).toBeInTheDocument()
    expect(screen.getByText('1 / 2 个文件')).toBeInTheDocument()
    expect(screen.queryByText('操作未完成')).not.toBeInTheDocument()
    expect(await screen.findByText('所选文件已全部移入', {}, { timeout: 5_000 })).toBeInTheDocument()
  })

  it('looks up the request ID after a lost POST response instead of blindly resubmitting', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok', fill_actor: { apply_enabled: true, apply_ready: true } }))
      .mockImplementationOnce(() => jsonResponse(plan))
      .mockImplementationOnce(() => Promise.reject(new TypeError('response lost')))
      .mockImplementationOnce(() => jsonResponse({
        job: {
          job_id: APPLY_REQUEST_ID,
          plan_id: 'plan-1',
          operation: 'apply',
          state: 'completed',
          progress: { stage: 'done', completed: 1, total: 1, unit: 'items', percent: 100 },
        },
        result: {
          plan_id: 'plan-1',
          revision: 'revision-1',
          state: 'succeeded',
          results: [{ candidate_id: 'safe-1', video_id: 'ABC-001', file_name: 'ABC-001.mp4', state: 'moved', error_code: null }],
        },
      }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')
    await user.click(screen.getByRole('button', { name: '确认并移入' }))
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: '确认移入' }))

    expect(await screen.findByText('所选文件已全部移入', {}, { timeout: 5_000 })).toBeInTheDocument()
    const postCalls = fetchMock.mock.calls.filter(([path, init]) =>
      path === '/api/fill-actor/plans/plan-1/apply-jobs' && init?.method === 'POST')
    expect(postCalls).toHaveLength(1)
    const requestBody = JSON.parse(String(postCalls[0]?.[1]?.body)) as { request_id: string }
    expect(fetchMock).toHaveBeenNthCalledWith(
      4,
      `/api/fill-actor/apply-jobs/${requestBody.request_id}`,
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
  })

  it('reuses the same request ID only after recovery lookup confirms the job is missing', async () => {
    const requestId = '1234567890abcdef'
    window.sessionStorage.setItem('embyx-manager-fill-actor-apply', JSON.stringify({
      planId: 'plan-1',
      revision: 'revision-1',
      candidateIds: ['safe-1'],
      requestId,
      jobId: requestId,
      retrySubmitIfMissing: true,
    }))
    const fetchMock = vi.mocked(fetch)
    fetchMock.mockImplementation((path, init) => {
      if (path === '/api/health') return jsonResponse({ status: 'ok', fill_actor: { apply_enabled: true, apply_ready: true } })
      if (path === '/api/fill-actor/plans/plan-1') return jsonResponse(plan)
      if (path === `/api/fill-actor/apply-jobs/${requestId}`) {
        return jsonResponse({ error: { code: 'unknown_apply_job' } }, 404)
      }
      if (path === '/api/fill-actor/plans/plan-1/apply-jobs' && init?.method === 'POST') {
        return jsonResponse({
          job: {
            job_id: requestId,
            plan_id: 'plan-1',
            operation: 'apply',
            state: 'completed',
            progress: { stage: 'done', completed: 1, total: 1, unit: 'items', percent: 100 },
          },
          result: {
            plan_id: 'plan-1',
            revision: 'revision-1',
            state: 'succeeded',
            results: [{ candidate_id: 'safe-1', video_id: 'ABC-001', file_name: 'ABC-001.mp4', state: 'moved', error_code: null }],
          },
        })
      }
      return jsonResponse({ error: { code: 'unexpected_request' } }, 500)
    })

    render(<App />)

    expect(await screen.findByText('所选文件已全部移入', {}, { timeout: 3_000 })).toBeInTheDocument()
    const post = fetchMock.mock.calls.find(([path, init]) =>
      path === '/api/fill-actor/plans/plan-1/apply-jobs' && init?.method === 'POST')
    expect(post).toBeDefined()
    expect(JSON.parse(String(post?.[1]?.body))).toMatchObject({ request_id: requestId })
  })

  it('restores an accepted move and its parent plan from session storage after refresh', async () => {
    window.sessionStorage.setItem('embyx-manager-fill-actor-apply', JSON.stringify({
      planId: 'plan-1',
      revision: 'revision-1',
      candidateIds: ['safe-1'],
      requestId: '1234567890abcdef',
      jobId: 'apply-resume',
    }))
    const fetchMock = vi.mocked(fetch)
    let applyPolls = 0
    fetchMock.mockImplementation((path) => {
      if (path === '/api/health') {
        return jsonResponse({ status: 'ok', fill_actor: { apply_enabled: false, apply_ready: false } })
      }
      if (path === '/api/fill-actor/plans/plan-1') {
        return jsonResponse({ job: { job_id: 'scan-1', plan_id: 'plan-1', state: 'completed' }, plan, feeds: [] })
      }
      if (path === '/api/fill-actor/apply-jobs/apply-resume') {
        applyPolls += 1
        if (applyPolls === 1) {
          return jsonResponse({
            job: {
              job_id: 'apply-resume',
              plan_id: 'plan-1',
              operation: 'apply',
              state: 'running',
              progress: { stage: 'persisting', completed: 0, total: 1, unit: 'items', percent: 0, current: 'ABC-001.mp4' },
            },
            result: null,
          })
        }
        return jsonResponse({
          job: {
            job_id: 'apply-resume',
            plan_id: 'plan-1',
            operation: 'apply',
            state: 'completed',
            progress: { stage: 'done', completed: 1, total: 1, unit: 'items', percent: 100 },
          },
          result: {
            plan_id: 'plan-1',
            revision: 'revision-1',
            state: 'succeeded',
            results: [{ candidate_id: 'safe-1', video_id: 'ABC-001', file_name: 'ABC-001.mp4', state: 'moved', error_code: null }],
          },
        })
      }
      return jsonResponse({ error: { code: 'unexpected_request' } }, 500)
    })

    render(<App />)

    expect(await screen.findByText('扫描结果')).toBeInTheDocument()
    expect(await screen.findByText('正在移入文件', {}, { timeout: 3_000 })).toBeInTheDocument()
    expect(screen.getByLabelText('演员 ID')).toBeDisabled()
    expect(screen.getByText('文件移动已暂停')).toBeInTheDocument()
    expect(await screen.findByText('所选文件已全部移入', {}, { timeout: 4_000 })).toBeInTheDocument()
    await waitFor(() => expect(window.sessionStorage.getItem('embyx-manager-fill-actor-apply')).toBeNull())
    expect(fetchMock.mock.calls.some(([path, init]) => String(path).endsWith('/apply-jobs') && init?.method === 'POST')).toBe(false)
  })

  it('restores only the confirmed candidate subset and clears the lost-response retry flag', async () => {
    const requestId = '1234567890abcdef'
    window.sessionStorage.setItem('embyx-manager-fill-actor-apply', JSON.stringify({
      planId: 'plan-1',
      revision: 'revision-1',
      candidateIds: ['safe-1'],
      requestId,
      jobId: requestId,
      retrySubmitIfMissing: true,
    }))
    const fetchMock = vi.mocked(fetch)
    fetchMock.mockImplementation((path) => {
      if (path === '/api/health') return jsonResponse({ status: 'ok', fill_actor: { apply_enabled: true, apply_ready: true } })
      if (path === '/api/fill-actor/plans/plan-1') {
        return jsonResponse({ job: { job_id: 'scan-1', plan_id: 'plan-1', state: 'completed' }, plan: twoCandidatePlan, feeds: [] })
      }
      if (path === `/api/fill-actor/apply-jobs/${requestId}`) {
        return jsonResponse({
          job: {
            job_id: requestId,
            plan_id: 'plan-1',
            operation: 'apply',
            state: 'running',
            progress: { stage: 'persisting', completed: 0, total: 1, unit: 'items', percent: 0 },
          },
          result: null,
        })
      }
      return jsonResponse({ error: { code: 'unexpected_request' } }, 500)
    })

    render(<App />)

    await screen.findByText('扫描结果')
    await waitFor(() => expect(screen.getByRole('checkbox', { name: /ABC-001\.mp4/ })).toBeChecked())
    expect(screen.getByRole('checkbox', { name: /ABC-002\.mkv/ })).not.toBeChecked()
    await waitFor(() => {
      const stored = JSON.parse(String(window.sessionStorage.getItem('embyx-manager-fill-actor-apply'))) as Record<string, unknown>
      expect(stored.retrySubmitIfMissing).toBeUndefined()
    }, { timeout: 2_000 })
  })

  it('rejects malformed apply progress and cross-plan terminal results at the API boundary', () => {
    const job = {
      job_id: '1234567890abcdef',
      plan_id: 'plan-1',
      operation: 'apply',
      state: 'running',
      progress: { stage: 'persisting', completed: 0, total: 1, unit: 'items', percent: 0, current: 42 },
    }
    expect(() => normalizeApplyJobEnvelope({ job, result: null })).toThrow(/移动任务响应无效/)

    expect(() => normalizeApplyJobEnvelope({
      job: {
        ...job,
        state: 'completed',
        progress: { stage: 'done', completed: 1, total: 1, unit: 'items', percent: 100 },
      },
      result: {
        plan_id: 'plan-2',
        revision: 'revision-1',
        state: 'succeeded',
        results: [{ candidate_id: 'safe-1', video_id: 'ABC-001', file_name: 'ABC-001.mp4', state: 'moved', error_code: null }],
      },
    })).toThrow(/移动任务响应无效/)
  })

  it('rejects overlong persisted request IDs and retains the legacy synchronous API helper', async () => {
    window.sessionStorage.setItem('embyx-manager-fill-actor-apply', JSON.stringify({
      planId: 'plan-1',
      revision: 'revision-1',
      candidateIds: ['safe-1'],
      requestId: 'x'.repeat(129),
    }))
    expect(getActiveApplyRequest()).toBeNull()
    expect(window.sessionStorage.getItem('embyx-manager-fill-actor-apply')).toBeNull()

    const fetchMock = vi.mocked(fetch)
    fetchMock.mockImplementationOnce(() => jsonResponse({
      plan_id: 'plan-1',
      revision: 'revision-1',
      state: 'succeeded',
      results: [],
    }))
    await expect(applyCandidates('plan-1', 'revision-1', [])).resolves.toMatchObject({ state: 'succeeded' })
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/fill-actor/plans/plan-1/apply',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({ revision: 'revision-1', candidate_ids: [] }),
      }),
    )
  })

  it('keeps an unknown CloudDrive result in observation-only mode and blocks a repeat apply', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok', fill_actor: { apply_enabled: true, apply_ready: true } }))
      .mockImplementationOnce(() => jsonResponse(plan))
      .mockImplementationOnce(() =>
        jsonResponse({
          job: {
            job_id: APPLY_REQUEST_ID,
            plan_id: 'plan-1',
            operation: 'apply',
            state: 'queued',
            progress: { stage: 'queued', completed: 0, total: 1, unit: 'items', percent: 0 },
          },
          result: null,
        }, 202),
      )
      .mockImplementationOnce(() =>
        jsonResponse({
          job: {
            job_id: APPLY_REQUEST_ID,
            plan_id: 'plan-1',
            operation: 'apply',
            state: 'completed',
            progress: { stage: 'done', completed: 1, total: 1, unit: 'items', percent: 100 },
          },
          result: {
            plan_id: 'plan-1',
            revision: 'revision-1',
            state: 'failed',
            results: [{
              candidate_id: 'safe-1',
              video_id: 'ABC-001',
              file_name: 'ABC-001.mp4',
              state: 'failed',
              error_code: 'cloud_move_status_unknown',
            }],
          },
        }),
      )

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')
    await user.click(screen.getByRole('button', { name: '确认并移入' }))
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: '确认移入' }))

    expect(await screen.findByText('部分远端状态仍在核验', {}, { timeout: 3_000 })).toBeInTheDocument()
    expect(screen.getByText('远端状态待确认，请勿重复操作')).toBeInTheDocument()
    expect(screen.getByText(/系统只会观察，不会自动重复移动/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '再次应用选择' })).toBeDisabled()
    expect(fetchMock).toHaveBeenCalledTimes(4)
  })

  it('explains and blocks file moves while leaving scan results available', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok', fill_actor: { apply_enabled: false, apply_ready: false } }))
      .mockImplementationOnce(() => jsonResponse(plan))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')

    expect(screen.getByText('文件移动已暂停')).toBeInTheDocument()
    expect(screen.getByText(/当前仅支持扫描、提交下载和订阅操作/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '确认并移入' })).toBeDisabled()
    expect(screen.getByText(/缺失作品已加入后台下载队列/)).toBeInTheDocument()
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('distinguishes a legacy journal safety block from an administrator kill switch', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({
        status: 'ok',
        fill_actor: { cloud: true, legacy_journal: false, apply_enabled: true, apply_ready: false },
      }))
      .mockImplementationOnce(() => jsonResponse(plan))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')

    expect(screen.getByText('文件移动等待管理员处理')).toBeInTheDocument()
    expect(screen.getByText(/检测到旧版本未完成的移动记录/)).toBeInTheDocument()
    expect(screen.queryByText('文件移动已暂停')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: '确认并移入' })).toBeDisabled()
  })

  it('accepts structured not-ready health and refreshes it on the polling interval', async () => {
    vi.useFakeTimers()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({
        status: 'not_ready',
        database: false,
        fill_actor: { roots: true, cloud: true, legacy_journal: true, apply_enabled: true, apply_ready: false },
      }, 503))
      .mockImplementationOnce(() => jsonResponse({
        status: 'ok',
        database: true,
        fill_actor: { roots: true, cloud: true, legacy_journal: true, apply_enabled: true, apply_ready: true },
      }))

    const view = render(<App />)
    try {
      await act(async () => {
        await Promise.resolve()
      })
      expect(screen.getByText('服务未就绪')).toBeInTheDocument()

      await act(async () => {
        await vi.advanceTimersByTimeAsync(30_000)
      })
      expect(screen.getByText('服务正常')).toBeInTheDocument()
      expect(fetchMock).toHaveBeenCalledTimes(2)
      expect(fetchMock).toHaveBeenNthCalledWith(
        1,
        '/api/health',
        expect.objectContaining({ cache: 'no-store' }),
      )
    } finally {
      view.unmount()
      vi.useRealTimers()
    }
  })

  it('points an unconfigured deployment at the settings page instead of blaming the mounts', async () => {
    vi.mocked(fetch).mockImplementation(() => jsonResponse({
      status: 'ok',
      database: true,
      fill_actor: { configured: false, roots: false, cloud: true, scan_ready: false, apply_ready: false },
    }))

    render(<App />)

    expect(await screen.findByText('补全演员尚未配置')).toBeInTheDocument()
    expect(screen.queryByText('媒体库根目录不可用')).not.toBeInTheDocument()
    expect(screen.getByRole('link', { name: '前往设置' })).toHaveAttribute('href', '/settings')
    expect(screen.getByText('服务正常')).toBeInTheDocument()
  })

  it('keeps the app healthy and warns only this page when fill actor dependencies are down', async () => {
    vi.mocked(fetch).mockImplementation(() => jsonResponse({
      status: 'ok',
      database: true,
      fill_actor: { configured: true, roots: false, cloud: true, scan_ready: false, apply_enabled: true, apply_ready: false },
    }))

    render(<App />)

    expect(await screen.findByText('媒体库根目录不可用')).toBeInTheDocument()
    expect(screen.getByText('服务正常')).toBeInTheDocument()
    expect(screen.queryByText('服务未就绪')).not.toBeInTheDocument()
  })

  it('shows submission outcomes with a dashboard hint instead of magnets', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse(submittedPlan))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')

    expect(screen.queryByRole('button', { name: /磁力/ })).not.toBeInTheDocument()
    expect(document.querySelector('a[href^="magnet:"]')).toBeNull()
    expect(document.querySelector('.magnet-text')).toBeNull()

    expect(screen.getAllByText('已提交下载')).toHaveLength(2)
    expect(screen.getByText('提交失败')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: '监控面板' })).toHaveAttribute('href', '/dashboard')
  })

  it('keeps polling completed plans while RSSHub warms and retains terminal feed states', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    const warmingFeed = {
      actor_id: 'A123',
      state: 'warming',
      attempts: 1,
      updated_at: '2026-07-13T10:01:00Z',
      error_code: null,
      freshrss_add_url: 'https://freshrss.example/i/?c=feed&a=add',
      freshrss_url: 'https://freshrss.example/',
    }
    const readyFeed = {
      ...warmingFeed,
      state: 'ready',
      attempts: 2,
      updated_at: '2026-07-13T10:02:00Z',
      freshrss_add_url: 'https://freshrss.example/i/?c=subscription&a=add&url_rss=https%3A%2F%2Frsshub.example%2Factress%2FA123',
    }
    const failedFeed = {
      actor_id: 'B456',
      state: 'failed',
      attempts: 3,
      updated_at: '2026-07-13T10:02:00Z',
      error_code: 'rsshub_timeout',
      freshrss_add_url: null,
      freshrss_url: null,
    }
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
        feeds: [warmingFeed],
      }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
        feeds: [readyFeed, failedFeed],
      }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    expect(await screen.findByText('缓存预热中')).toBeInTheDocument()
    expect(screen.getByText('RSSHub 正在预热缓存，页面会自动更新。')).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: '一键添加到 FreshRSS' })).not.toBeInTheDocument()
    expect(screen.queryByRole('link', { name: '打开 FreshRSS' })).not.toBeInTheDocument()

    const addLink = await screen.findByRole('link', { name: '一键添加到 FreshRSS' }, { timeout: 2_000 })
    const freshrssLink = screen.getByRole('link', { name: '打开 FreshRSS' })
    expect(addLink).toHaveAttribute('href', readyFeed.freshrss_add_url)
    expect(addLink).toHaveAttribute('target', '_blank')
    expect(addLink).toHaveAttribute('rel', 'noopener noreferrer')
    expect(freshrssLink).toHaveAttribute('href', readyFeed.freshrss_url)
    expect(freshrssLink).toHaveAttribute('target', '_blank')
    expect(freshrssLink).toHaveAttribute('rel', 'noopener noreferrer')
    expect(screen.getByText('缓存已就绪')).toBeInTheDocument()
    expect(screen.getByText('缓存失败')).toBeInTheDocument()
    expect(screen.getByText('错误：RSSHub 请求超时')).toBeInTheDocument()
    expect(screen.getByText('已尝试 2 次', { exact: false })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans/plan-1',
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
    expect(fetchMock).toHaveBeenCalledTimes(3)
  })

  it('hides unsafe FreshRSS actions even when the feed is ready', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
        feeds: [{
          actor_id: 'A123',
          state: 'ready',
          attempts: 2,
          updated_at: '2026-07-13T10:02:00Z',
          error_code: null,
          freshrss_add_url: 'javascript:alert(1)',
          freshrss_url: 'data:text/html,unsafe',
        }],
      }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    expect(await screen.findByText('缓存已就绪')).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: '一键添加到 FreshRSS' })).not.toBeInTheDocument()
    expect(screen.queryByRole('link', { name: '打开 FreshRSS' })).not.toBeInTheDocument()
  })

  it('normalizes old feed responses without a FreshRSS site URL', () => {
    const oldFeed = {
      actor_id: 'A123',
      state: 'ready',
      attempts: 2,
      updated_at: '2026-07-13T10:02:00Z',
      error_code: null,
      freshrss_add_url: 'https://freshrss.example/i/?c=feed&a=add',
    }

    expect(normalizePlanEnvelope({
      job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
      plan: null,
      feeds: [oldFeed],
    }).feeds).toEqual([{ ...oldFeed, freshrss_url: null }])
  })

  it('cancels a queued scan once, disables the action in flight, and renders a neutral terminal state', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    let resolveCancel!: (response: Response) => void
    const pendingCancel = new Promise<Response>((resolve) => {
      resolveCancel = resolve
    })
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'queued' },
        plan: null,
        feeds: [],
      }, 202))
      .mockImplementationOnce(() => pendingCancel)

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    await user.click(await screen.findByRole('button', { name: '取消扫描' }))
    expect(screen.getByRole('button', { name: '正在取消' })).toBeDisabled()
    resolveCancel(new Response(JSON.stringify({
      job: { job_id: 'job-1', plan_id: 'plan-1', state: 'failed', error_code: 'job_cancelled' },
      plan: null,
      feeds: [{
        actor_id: 'A123',
        state: 'failed',
        attempts: 1,
        updated_at: '2026-07-13T10:02:00Z',
        error_code: 'rsshub_cancelled',
        freshrss_add_url: null,
        freshrss_url: null,
      }],
    }), { status: 200, headers: { 'Content-Type': 'application/json' } }))

    expect(await screen.findByText('扫描已取消')).toBeInTheDocument()
    expect(screen.queryByText('操作未完成')).not.toBeInTheDocument()
    expect(screen.queryByText('缓存失败')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: '取消扫描' })).not.toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans/plan-1/cancel',
      expect.objectContaining({ method: 'POST', signal: expect.any(AbortSignal) }),
    )
    await waitFor(() => expect(window.sessionStorage.getItem('embyx-manager-fill-actor-plan-id')).toBeNull())
  })

  it('keeps polling and restores cancel after a network failure', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'running' },
        plan: null,
        feeds: [],
      }, 202))
      .mockImplementationOnce(() => Promise.reject(new TypeError('temporary offline')))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
        feeds: [],
      }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await user.click(await screen.findByRole('button', { name: '取消扫描' }))

    expect(await screen.findByText('无法连接到服务，请检查网络后重试。')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '取消扫描' })).toBeEnabled()
    expect(screen.queryByText('扫描已取消')).not.toBeInTheDocument()
    expect(await screen.findByText('扫描结果', {}, { timeout: 2_000 })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      4,
      '/api/fill-actor/plans/plan-1',
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
  })

  it('restores cancel without fabricating cancellation after a server error', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'running' },
        plan: null,
        feeds: [],
      }, 202))
      .mockImplementationOnce(() => jsonResponse({ error: { code: 'cancel_failed' } }, 503))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await user.click(await screen.findByRole('button', { name: '取消扫描' }))

    expect(await screen.findByText('操作未完成')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '取消扫描' })).toBeEnabled()
    expect(screen.queryByText('扫描已取消')).not.toBeInTheDocument()
  })

  it('keeps public status polling active when cancellation requires a token', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'running' },
        plan: null,
        feeds: [],
      }, 202))
      .mockImplementationOnce(() => jsonResponse({ error: { code: 'unauthorized' } }, 401))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
        feeds: [],
      }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await user.click(await screen.findByRole('button', { name: '取消扫描' }))

    expect(await screen.findByText('需要登录')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '取消扫描' })).toBeEnabled()
    expect(screen.queryByText('扫描已取消')).not.toBeInTheDocument()

    expect(await screen.findByText('扫描结果', {}, { timeout: 2_000 })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      4,
      '/api/fill-actor/plans/plan-1',
      expect.objectContaining({
        signal: expect.any(AbortSignal),
      }),
    )
    expect(window.localStorage.getItem('embyx-manager-api-token')).toBeNull()
  })

  it('refreshes the current plan after a plan_not_cancellable race', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'running' },
        plan: null,
        feeds: [],
      }, 202))
      .mockImplementationOnce(() => jsonResponse({ error: { code: 'plan_not_cancellable' } }, 409))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
        feeds: [],
      }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await user.click(await screen.findByRole('button', { name: '取消扫描' }))

    expect(await screen.findByText('扫描结果')).toBeInTheDocument()
    expect(screen.queryByText('操作未完成')).not.toBeInTheDocument()
    expect(screen.queryByText('扫描已取消')).not.toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      4,
      '/api/fill-actor/plans/plan-1',
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
  })

  it('ignores an older poll response after cancellation wins the race', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    let resolvePoll!: (response: Response) => void
    const pendingPoll = new Promise<Response>((resolve) => {
      resolvePoll = resolve
    })
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'running' },
        plan: null,
        feeds: [],
      }, 202))
      .mockImplementationOnce(() => pendingPoll)
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'failed', error_code: 'job_cancelled' },
        plan: null,
        feeds: [],
      }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    const cancelButton = await screen.findByRole('button', { name: '取消扫描' })
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3), { timeout: 2_000 })
    await user.click(cancelButton)

    expect(await screen.findByText('扫描已取消')).toBeInTheDocument()
    resolvePoll(new Response(JSON.stringify({
      job: { job_id: 'job-1', plan_id: 'plan-1', state: 'running' },
      plan: null,
      feeds: [],
    }), { status: 200, headers: { 'Content-Type': 'application/json' } }))
    await Promise.resolve()
    expect(screen.getByText('扫描已取消')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: '取消扫描' })).not.toBeInTheDocument()
  })

  it('shows an explicit recovery action when apply reports an expired plan', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok', fill_actor: { apply_enabled: true, apply_ready: true } }))
      .mockImplementationOnce(() => jsonResponse(plan))
      .mockImplementationOnce(() => jsonResponse({ error: { code: 'expired_plan' } }, 410))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')
    await user.click(screen.getByRole('button', { name: '确认并移入' }))
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: '确认移入' }))

    await waitFor(() => expect(screen.getByText('扫描结果已失效')).toBeInTheDocument())
    expect(screen.getByRole('button', { name: /重新扫描/ })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '确认并移入' })).toBeDisabled()
  })

  it('requires a rescan when apply rejects a legacy plan', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok', fill_actor: { apply_enabled: true, apply_ready: true } }))
      .mockImplementationOnce(() => jsonResponse(plan))
      .mockImplementationOnce(() => jsonResponse({ error: { code: 'legacy_plan_requires_rescan' } }, 409))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')
    await user.click(screen.getByRole('button', { name: '确认并移入' }))
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: '确认移入' }))

    expect(await screen.findByText('该计划来自旧版本，请重新扫描后再操作。')).toBeInTheDocument()
    expect(screen.getByText('扫描结果已失效')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /重新扫描/ })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '确认并移入' })).toBeDisabled()
  })

  it('verifies a token typed after a rejection and keeps it in local storage', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    let planRejected = false
    fetchMock.mockImplementation((path, init) => {
      if (path === '/api/health') return jsonResponse({ status: 'ok' })
      if (path === '/api/auth/session') {
        return authorizationOf(init) === 'Bearer rejected-then-saved'
          ? jsonResponse({ auth_required: true })
          : jsonResponse({ error: { code: 'unauthorized' } }, 401)
      }
      if (path === '/api/fill-actor/plans' && init?.method === 'POST') {
        if (planRejected) return jsonResponse(plan)
        planRejected = true
        return jsonResponse({ error: { code: 'unauthorized' } }, 401)
      }
      return jsonResponse({ error: { code: 'unexpected_request' } }, 500)
    })

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByRole('dialog')
    await user.type(screen.getByLabelText('API Token'), 'rejected-then-saved')
    await user.click(screen.getByRole('button', { name: '登录' }))
    await screen.findByText('已登录，可以重试刚才的操作。')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    await screen.findByText('扫描结果')
    expect(fetchMock).toHaveBeenLastCalledWith(
      '/api/fill-actor/plans',
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: 'Bearer rejected-then-saved' }),
      }),
    )
    expect(window.localStorage.getItem('embyx-manager-api-token')).toBe('rejected-then-saved')
  })
})

describe('login gate', () => {
  const TOKEN_KEY = 'embyx-manager-api-token'
  const AUTH_REQUIRED_KEY = 'embyx-manager-auth-required'

  beforeEach(() => {
    window.sessionStorage.clear()
    window.localStorage.clear()
    // The gate is page-agnostic; fill-actor is just the page these cases assert against.
    window.history.pushState({}, '', '/fill-actor')
    vi.stubGlobal('fetch', vi.fn().mockImplementation(() => jsonResponse({ status: 'ok', auth_required: true })))
  })

  afterEach(() => {
    vi.restoreAllMocks()
    window.history.pushState({}, '', '/')
  })

  /** Serves a deployment that demands `token`; anything else is rejected like the real API. */
  function stubTokenServer(token: string) {
    vi.mocked(fetch).mockImplementation((path, init) => {
      const authorized = authorizationOf(init) === `Bearer ${token}`
      if (path === '/api/health') return jsonResponse({ status: 'ok', auth_required: true })
      if (path === '/api/auth/session') {
        return authorized ? jsonResponse({ auth_required: true }) : jsonResponse({ error: { code: 'unauthorized' } }, 401)
      }
      if (!authorized) return jsonResponse({ error: { code: 'unauthorized' } }, 401)
      return jsonResponse({ status: 'ok' })
    })
  }

  it('hides the app until the server accepts the token, then persists it for the next visit', async () => {
    const user = userEvent.setup()
    stubTokenServer('gate-token')

    render(<App />)

    expect(await screen.findByRole('heading', { name: '登录 Embyx Manager' })).toBeInTheDocument()
    expect(screen.queryByLabelText('演员 ID')).not.toBeInTheDocument()

    await user.type(screen.getByLabelText('API Token'), 'wrong-token')
    await user.click(screen.getByRole('button', { name: '登录' }))
    expect(await screen.findByText('API Token 不正确，请检查后重试。')).toBeInTheDocument()
    expect(window.localStorage.getItem(TOKEN_KEY)).toBeNull()

    await user.clear(screen.getByLabelText('API Token'))
    await user.type(screen.getByLabelText('API Token'), 'gate-token')
    await user.click(screen.getByRole('button', { name: '登录' }))

    expect(await screen.findByLabelText('演员 ID')).toBeInTheDocument()
    expect(window.localStorage.getItem(TOKEN_KEY)).toBe('gate-token')
    expect(window.localStorage.getItem(AUTH_REQUIRED_KEY)).toBe('true')
  })

  it('shows the login screen before health answers when the last visit needed a token', () => {
    window.localStorage.setItem(AUTH_REQUIRED_KEY, 'true')

    render(<App />)

    expect(screen.getByRole('heading', { name: '登录 Embyx Manager' })).toBeInTheDocument()
    expect(screen.queryByLabelText('演员 ID')).not.toBeInTheDocument()
  })

  it('reuses a stored token across reloads and signs out on request', async () => {
    const user = userEvent.setup()
    window.localStorage.setItem(TOKEN_KEY, 'stored-token')
    window.localStorage.setItem(AUTH_REQUIRED_KEY, 'true')
    stubTokenServer('stored-token')

    render(<App />)

    expect(await screen.findByLabelText('演员 ID')).toBeInTheDocument()
    await waitFor(() => expect(fetch).toHaveBeenCalledWith(
      '/api/auth/session',
      expect.objectContaining({ headers: expect.objectContaining({ Authorization: 'Bearer stored-token' }) }),
    ))

    await user.click(await screen.findByRole('button', { name: '退出登录' }))

    expect(await screen.findByRole('heading', { name: '登录 Embyx Manager' })).toBeInTheDocument()
    expect(window.localStorage.getItem(TOKEN_KEY)).toBeNull()
  })

  it('drops a stored token the server no longer accepts and explains why', async () => {
    window.localStorage.setItem(TOKEN_KEY, 'rotated-away')
    window.localStorage.setItem(AUTH_REQUIRED_KEY, 'true')
    stubTokenServer('current-token')

    render(<App />)

    expect(await screen.findByText('登录状态已失效，请重新登录。')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: '登录 Embyx Manager' })).toBeInTheDocument()
    expect(window.localStorage.getItem(TOKEN_KEY)).toBeNull()
  })

  it('signs out every open tab when one of them clears the token', async () => {
    window.localStorage.setItem(TOKEN_KEY, 'shared-token')
    window.localStorage.setItem(AUTH_REQUIRED_KEY, 'true')
    stubTokenServer('shared-token')

    render(<App />)
    expect(await screen.findByLabelText('演员 ID')).toBeInTheDocument()

    act(() => {
      window.localStorage.removeItem(TOKEN_KEY)
      window.dispatchEvent(new StorageEvent('storage', { key: TOKEN_KEY, newValue: null }))
    })

    expect(await screen.findByRole('heading', { name: '登录 Embyx Manager' })).toBeInTheDocument()
  })

  it('keeps the app open when a deployment requires no token at all', async () => {
    vi.mocked(fetch).mockImplementation(() => jsonResponse({ status: 'ok', auth_required: false }))

    render(<App />)

    expect(await screen.findByLabelText('演员 ID')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: '退出登录' })).not.toBeInTheDocument()
    expect(window.localStorage.getItem(AUTH_REQUIRED_KEY)).toBeNull()
  })

  it('opens the login dialog when a dashboard action is rejected', async () => {
    const user = userEvent.setup()
    vi.mocked(fetch).mockImplementation((input) => {
      const url = String(input)
      if (url.startsWith('/api/monitor/status')) {
        return jsonResponse([
          {
            pipeline: 'rss',
            enabled: false,
            configured: true,
            reason: null,
            running_run_id: null,
            next_scheduled_at: null,
            last_run: null,
          },
        ])
      }
      if (url.startsWith('/api/monitor/runs')) return jsonResponse([])
      if (url.startsWith('/api/config')) {
        return jsonResponse([{ section: 'rss', values: { enabled: false }, secrets: {}, version: 1 }])
      }
      if (url.includes('/trigger')) return jsonResponse({ error: { code: 'unauthorized' } }, 401)
      return jsonResponse({ status: 'ok', database: true })
    })

    render(<App />)
    await user.click(screen.getByRole('link', { name: '监控看板' }))
    await user.click(await screen.findByRole('button', { name: '立即运行' }))

    expect(await screen.findByRole('dialog')).toBeInTheDocument()
    expect(screen.getByText('需要登录')).toBeInTheDocument()
  })
})
