import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Outlet, Route, Routes } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import PlaylistsPage from './PlaylistsPage'

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
}

const TOP = {
  key: 'k7',
  kind: 7,
  note: 'JavDB 有码 TOP250',
  name: 'JavDB 有码 TOP250',
  enabled: true,
  total: 250,
  present: 220,
  missing: 30,
  emby_playlist_id: '88945',
  last_synced_at: '2026-09-20T12:00:00Z',
  last_error: null,
}
const YEAR_2024 = { ...TOP, key: 'k2024', kind: 2024, note: 'JavDB 2024 TOP250', name: 'JavDB 2024 TOP250', missing: 44 }
const YEAR_2025 = { ...TOP, key: 'k2025', kind: 2025, note: 'JavDB 2025 TOP250', name: 'JavDB 2025 TOP250', missing: 66 }
const AWARD = {
  ...TOP,
  key: 'k4:第四届JAV金鸡儿奖 最佳故事片 获奖',
  kind: 4,
  note: '第四届JAV金鸡儿奖 最佳故事片 获奖',
  name: '第四届JAV金鸡儿奖 最佳故事片 获奖',
  total: 1,
  present: 0,
  missing: 1,
  enabled: false,
  emby_playlist_id: null,
  last_error: null,
}

const STATUS = [
  {
    pipeline: 'playlists',
    enabled: true,
    configured: true,
    reason: null,
    running_run_id: null,
    next_scheduled_at: null,
    last_run: null,
  },
]

function renderPage(requestApiToken = vi.fn()) {
  return render(
    <MemoryRouter initialEntries={['/playlists']}>
      <Routes>
        <Route element={<Outlet context={{ requestApiToken }} />}>
          <Route path="/playlists" element={<PlaylistsPage />} />
        </Route>
      </Routes>
    </MemoryRouter>,
  )
}

describe('playlists page', () => {
  beforeEach(() => {
    window.localStorage.clear()
    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((input, init?: RequestInit) => {
        const url = String(input)
        const method = init?.method ?? 'GET'
        if (url === '/api/monitor/status') return jsonResponse(STATUS)
        if (url === '/api/playlists' && method === 'GET') {
          return jsonResponse({
            items: [AWARD, TOP, YEAR_2024, YEAR_2025],
            source: { database_name: '20260112', fetched_at: '2026-09-20T11:00:00Z' },
            fill_task_dir: '/115/embyx_in/rank',
            fill_reason: null,
          })
        }
        if (url === '/api/playlists/k7/missing') {
          return jsonResponse({
            key: 'k7',
            name: TOP.name,
            items: [
              { rank: 3, avid: 'IPX-811', title: '嗑藥做愛住同房NTR姦', tracked: 'downloading' },
              { rank: 9, avid: 'SMBD-115', title: 'S Model 115', tracked: null },
            ],
          })
        }
        if (url === '/api/playlists/k7' && method === 'PATCH') {
          const body = JSON.parse(String(init?.body)) as { enabled: boolean }
          return jsonResponse({ ...TOP, enabled: body.enabled })
        }
        if (url === '/api/monitor/playlists/trigger' && method === 'POST') return jsonResponse({ run_id: 'r1' }, 202)
        if (url === '/api/playlists/k7/fill' && method === 'POST') {
          return jsonResponse({
            key: 'k7',
            task_dir_path: '/115/embyx_in/rank',
            items: [
              { avid: 'IPX-811', outcome: 'already_tracked' },
              { avid: 'SMBD-115', outcome: 'submitted' },
            ],
            counts: { submitted: 1, already_tracked: 1 },
          })
        }
        return jsonResponse({ error: { code: 'unknown' } }, 404)
      }),
    )
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('groups charts, years newest first, and awards by edition, with the gap summary', async () => {
    renderPage()

    expect(await screen.findByRole('heading', { name: '总榜' })).toBeInTheDocument()
    const groups = screen.getAllByRole('heading', { level: 3 }).map((heading) => heading.textContent)
    expect(groups).toEqual(['总榜', 'JavDB 年度榜', '第四届JAV金鸡儿奖'])

    const years = within(screen.getByRole('region', { name: 'JavDB 年度榜' }))
    const names = years.getAllByRole('row').slice(1).map((row) => within(row).getAllByRole('cell')[1].textContent)
    expect(names).toEqual(['JavDB 2025 TOP250', 'JavDB 2024 TOP250'])

    expect(screen.getByText(/数据源 jinjier\.sqlite3 · 20260112/)).toHaveTextContent('启用 3/4 张 · 缺失 141 部 · 补全目录 /115/embyx_in/rank')
    const awardRow = within(screen.getByRole('region', { name: '第四届JAV金鸡儿奖' })).getAllByRole('row')[1]
    expect(within(awardRow).getByText('停用')).toHaveClass('run-state')
    expect(within(awardRow).getByRole('button', { name: '启用' })).toBeInTheDocument()
  })

  it('expands a list into its missing titles with the ledger state', async () => {
    const user = userEvent.setup()
    renderPage()

    const row = (await screen.findByText('JavDB 有码 TOP250')).closest('tr')!
    await user.click(row)

    const detail = await screen.findByRole('cell', { name: 'IPX-811' })
    const detailRow = detail.closest('tr')!
    expect(within(detailRow).getAllByRole('cell').map((cell) => cell.textContent)).toEqual([
      '3',
      'IPX-811',
      '嗑藥做愛住同房NTR姦',
      '下载中',
    ])
    expect(within(screen.getByRole('cell', { name: 'SMBD-115' }).closest('tr')!).getByText('—')).toBeInTheDocument()

    await user.click(row)
    expect(screen.queryByRole('cell', { name: 'IPX-811' })).not.toBeInTheDocument()
  })

  it('toggles a list without expanding it and triggers a sync', async () => {
    const user = userEvent.setup()
    renderPage()

    const row = (await screen.findByText('JavDB 有码 TOP250')).closest('tr')!
    await user.click(within(row).getByRole('button', { name: '停用' }))

    await waitFor(() => expect(within(row).getByRole('button', { name: '启用' })).toBeInTheDocument())
    expect(screen.queryByRole('cell', { name: 'IPX-811' })).not.toBeInTheDocument()
    const patch = vi.mocked(fetch).mock.calls.find(([, init]) => init?.method === 'PATCH')
    expect(patch?.[0]).toBe('/api/playlists/k7')
    expect(JSON.parse(String(patch?.[1]?.body))).toEqual({ enabled: false })

    await user.click(screen.getByRole('button', { name: '立即同步' }))
    await waitFor(() =>
      expect(vi.mocked(fetch).mock.calls.some(([url]) => String(url) === '/api/monitor/playlists/trigger')).toBe(true),
    )
  })

  it('points at settings when the pipeline is not configured', async () => {
    vi.mocked(fetch).mockImplementation((input) => {
      const url = String(input)
      if (url === '/api/monitor/status') {
        return jsonResponse([{ ...STATUS[0], configured: false, reason: 'Emby address and API key must be configured' }])
      }
      if (url === '/api/playlists') {
        return jsonResponse({ items: [], source: null, fill_task_dir: null, fill_reason: 'no fill directory: set playlists.task_dir_path or an RSS category labelled Rank' })
      }
      return jsonResponse({ error: { code: 'unknown' } }, 404)
    })
    renderPage()

    expect(await screen.findByText('同步尚未就绪')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: '前往设置' })).toHaveAttribute('href', '/settings')
    expect(screen.getByRole('button', { name: '立即同步' })).toBeDisabled()
    expect(screen.getByText(/还没有列表/)).toBeInTheDocument()
    expect(screen.getByText(/补全不可用/)).toBeInTheDocument()
  })

  it('fills a list only after confirmation and shows the outcome under its name', async () => {
    const user = userEvent.setup()
    renderPage()

    const row = (await screen.findByText('JavDB 有码 TOP250')).closest('tr')!
    const awardRow = (await screen.findByText('第四届JAV金鸡儿奖 最佳故事片 获奖')).closest('tr')!
    expect(within(awardRow).getByRole('button', { name: '补全' })).toBeEnabled()

    await user.click(within(row).getByRole('button', { name: '补全' }))
    expect(vi.mocked(fetch).mock.calls.some(([url]) => String(url).endsWith('/fill'))).toBe(false)
    await user.click(within(row).getByRole('button', { name: '取消' }))
    expect(within(row).getByRole('button', { name: '补全' })).toBeInTheDocument()

    await user.click(within(row).getByRole('button', { name: '补全' }))
    await user.click(within(row).getByRole('button', { name: '确认补全 30 部' }))

    expect(await within(row).findByText('补全：已提交 1 · 已在跟踪 1')).toBeInTheDocument()
    const fill = vi.mocked(fetch).mock.calls.find(([url, init]) => String(url).endsWith('/fill') && init?.method === 'POST')
    expect(fill?.[0]).toBe('/api/playlists/k7/fill')
    expect(within(row).getByRole('button', { name: '补全' })).toBeInTheDocument()
    // Nothing was expanded, so the missing detail was not fetched.
    expect(vi.mocked(fetch).mock.calls.some(([url]) => String(url).endsWith('/missing'))).toBe(false)
  })
})
