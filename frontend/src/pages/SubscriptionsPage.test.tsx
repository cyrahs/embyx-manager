import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Outlet, Route, Routes } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import SubscriptionsPage from './SubscriptionsPage'

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
}

const SECTIONS = [
  {
    section: 'rss',
    values: {
      categories: [
        { label: 'Rank', task_dir_path: '/115/embyx_in/rank' },
        { label: 'Actor', task_dir_path: '/115/embyx_in/clt' },
      ],
    },
    secrets: {},
    version: 1,
  },
  { section: 'fill_actor', values: { task_dir_path: '/115/embyx_in/clt' }, secrets: {}, version: 1 },
]

const FEED = {
  id: 1,
  kind: 'rss',
  category: 'Rank',
  enabled: true,
  url: 'http://rsshub/javlibrary/mostwanted/cn',
  feed_url: 'http://rsshub/javlibrary/mostwanted/cn',
  talent_id: null,
  name: '最想要',
  aliases: [],
  seed_pending: false,
  cursor_size: 20,
  last_polled_at: null,
  last_error: 'ConnectError: [Errno -2] Name or service not known',
  created_at: '2026-09-02T00:00:00Z',
  updated_at: '2026-09-02T00:00:00Z',
}

const TALENT = {
  ...FEED,
  id: 2,
  kind: 'avbase_talent',
  category: 'Actor',
  url: null,
  feed_url: 'https://www.avbase.net/talents/46144/feed',
  talent_id: 46144,
  name: '石川澪',
  last_error: null,
}

const RESOLVED = {
  ...TALENT,
  id: 3,
  feed_url: 'https://www.avbase.net/talents/5022/feed',
  talent_id: 5022,
  name: '河北彩花',
  aliases: ['河北彩伽'],
}

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/subscriptions']}>
      <Routes>
        <Route element={<Outlet context={{ requestApiToken: vi.fn() }} />}>
          <Route path="/subscriptions" element={<SubscriptionsPage />} />
        </Route>
      </Routes>
    </MemoryRouter>,
  )
}

describe('subscriptions page', () => {
  beforeEach(() => {
    window.localStorage.clear()
    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((input, init?: RequestInit) => {
        const url = String(input)
        const method = init?.method ?? 'GET'
        if (url.startsWith('/api/config')) return jsonResponse(SECTIONS)
        if (url === '/api/monitor/subscriptions' && method === 'GET') {
          return jsonResponse({ items: [FEED, TALENT], categories: ['Actor', 'Rank'] })
        }
        if (url === '/api/monitor/subscriptions' && method === 'POST') {
          const body = JSON.parse(String(init?.body)) as { kind?: string; name?: string; url?: string; category: string }
          if (body.kind === 'avbase_talent') {
            if (body.name === 'nobody') return jsonResponse({ error: { code: 'talent_not_found' } }, 404)
            return jsonResponse({ ...RESOLVED, category: body.category }, 201)
          }
          return jsonResponse({ ...FEED, id: 4, name: null, url: body.url, feed_url: body.url, category: body.category, last_error: null }, 201)
        }
        if (url === '/api/monitor/subscriptions/1' && method === 'PATCH') {
          const body = JSON.parse(String(init?.body)) as { enabled?: boolean; url?: string }
          return jsonResponse({
            ...FEED,
            enabled: body.enabled ?? FEED.enabled,
            url: body.url ?? FEED.url,
            feed_url: body.url ?? FEED.feed_url,
            last_error: body.url ? null : FEED.last_error,
          })
        }
        if (url === '/api/monitor/subscriptions/2' && method === 'DELETE') return Promise.resolve(new Response(null, { status: 204 }))
        return jsonResponse({ error: { code: 'unknown' } }, 404)
      }),
    )
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('tags only the rows filed under another category than the panel files into', async () => {
    // The list is the first request the page makes.
    vi.mocked(fetch).mockImplementationOnce(() =>
      jsonResponse({
        items: [TALENT, { ...TALENT, id: 7, name: '七沢みあ', talent_id: 2037, category: 'Rank' }],
        categories: ['Actor', 'Rank'],
      }),
    )
    renderPage()

    const plain = (await screen.findByText('石川澪')).closest('tr') as HTMLElement
    const elsewhere = screen.getByText('七沢みあ').closest('tr') as HTMLElement
    expect(within(plain).queryByText('Actor')).not.toBeInTheDocument()
    expect(within(elsewhere).getByText('Rank')).toBeInTheDocument()
  })

  it('splits actors and charts into two tabs with their counts', async () => {
    const user = userEvent.setup()
    renderPage()

    expect(await screen.findByText('石川澪')).toBeInTheDocument()
    expect(screen.queryByText('最想要')).not.toBeInTheDocument()
    const actorTab = screen.getByRole('tab', { name: /演员/ })
    expect(actorTab).toHaveAttribute('aria-selected', 'true')
    expect(within(actorTab).getByText('1')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /AVBase/ })).toHaveAttribute(
      'href',
      `https://www.avbase.net/talents/${encodeURIComponent('石川澪')}`,
    )

    await user.click(screen.getByRole('tab', { name: /榜单/ }))

    expect(screen.getByText('最想要')).toBeInTheDocument()
    expect(screen.getByText('拉取出错')).toBeInTheDocument()
    expect(screen.queryByText('石川澪')).not.toBeInTheDocument()
    expect(window.localStorage.getItem('embyx-manager-subscriptions-tab')).toBe('rss')
  })

  it('adds an actor from a name and lets the backend resolve the talent', async () => {
    const user = userEvent.setup()
    renderPage()
    await screen.findByText('石川澪')

    // The panel files into the category whose directory is fill actor's own.
    expect(screen.getByText('新添加的归入分类「Actor」，下载落在 /115/embyx_in/clt')).toBeInTheDocument()
    expect(screen.queryByLabelText('分类')).not.toBeInTheDocument()
    await user.type(screen.getByLabelText('演员名或 AVBase 链接'), '河北彩伽')
    await user.click(screen.getByRole('button', { name: '添加演员' }))

    expect(await screen.findByText('河北彩花')).toBeInTheDocument()
    expect(screen.getByText('别名：河北彩伽')).toBeInTheDocument()
    expect(fetch).toHaveBeenCalledWith(
      '/api/monitor/subscriptions',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({ kind: 'avbase_talent', name: '河北彩伽', category: 'Actor', seed: false }),
      }),
    )
    expect(screen.getByLabelText('演员名或 AVBase 链接')).toHaveValue('')
    expect(within(screen.getByRole('tab', { name: /演员/ })).getByText('2')).toBeInTheDocument()
  })

  it('explains when AVBase has no such talent', async () => {
    const user = userEvent.setup()
    renderPage()
    await screen.findByText('石川澪')

    await user.type(screen.getByLabelText('演员名或 AVBase 链接'), 'nobody')
    await user.click(screen.getByRole('button', { name: '添加演员' }))

    expect(await screen.findByText('AVBase 上找不到这位演员，请检查名字或链接。')).toBeInTheDocument()
    expect(screen.getByLabelText('演员名或 AVBase 链接')).toHaveValue('nobody')
  })

  it('removes an actor after a confirmation click', async () => {
    const user = userEvent.setup()
    renderPage()
    const row = (await screen.findByText('石川澪')).closest('tr') as HTMLElement

    await user.click(within(row).getByRole('button', { name: '删除' }))
    await user.click(within(row).getByRole('button', { name: '确认删除' }))

    await waitFor(() => expect(screen.queryByText('石川澪')).not.toBeInTheDocument())
    expect(screen.getByText('还没有演员订阅。')).toBeInTheDocument()
  })

  it('adds, corrects and disables a chart feed on the charts tab', async () => {
    const user = userEvent.setup()
    renderPage()
    await screen.findByText('石川澪')
    await user.click(screen.getByRole('tab', { name: /榜单/ }))

    expect(screen.getByText('新添加的归入分类「Rank」，下载落在 /115/embyx_in/rank')).toBeInTheDocument()
    await user.type(screen.getByLabelText('Feed 地址'), 'https://rsshub.test/new')
    await user.click(screen.getByRole('button', { name: '添加' }))
    expect(await screen.findByText('https://rsshub.test/new')).toBeInTheDocument()
    expect(fetch).toHaveBeenCalledWith(
      '/api/monitor/subscriptions',
      expect.objectContaining({ method: 'POST', body: JSON.stringify({ url: 'https://rsshub.test/new', category: 'Rank' }) }),
    )

    const row = screen.getByText('最想要').closest('tr') as HTMLElement
    await user.click(within(row).getByRole('button', { name: '改地址' }))
    const field = within(row).getByLabelText('新的 Feed 地址')
    await user.clear(field)
    await user.type(field, 'http://rsshub.rss.svc.cluster.local/javlibrary/mostwanted/cn')
    await user.click(within(row).getByRole('button', { name: '保存' }))
    expect(await within(row).findByText('http://rsshub.rss.svc.cluster.local/javlibrary/mostwanted/cn')).toBeInTheDocument()
    expect(within(row).queryByText('拉取出错')).not.toBeInTheDocument()

    await user.click(within(row).getByRole('button', { name: '停用' }))
    expect(await within(row).findByText('停用', { selector: '.run-state' })).toBeInTheDocument()
    expect(fetch).toHaveBeenCalledWith(
      '/api/monitor/subscriptions/1',
      expect.objectContaining({ method: 'PATCH', body: JSON.stringify({ enabled: false }) }),
    )
  })
})
