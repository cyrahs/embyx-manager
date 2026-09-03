import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { SubscriptionsPanel } from './SubscriptionsPanel'

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
}

const SUBSCRIPTION = {
  id: 1,
  kind: 'rss',
  category: 'Actor',
  enabled: true,
  url: 'https://rsshub.test/javbus/star/rwt',
  feed_url: 'https://rsshub.test/javbus/star/rwt',
  talent_id: null,
  name: '演员甲',
  aliases: [],
  seed_pending: false,
  cursor_size: 0,
  last_polled_at: null,
  last_error: 'category Actor is not configured',
  created_at: '2026-09-02T00:00:00Z',
  updated_at: '2026-09-02T00:00:00Z',
}

describe('subscriptions panel', () => {
  beforeEach(() => {
    window.localStorage.clear()
    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((input, init?: RequestInit) => {
        const url = String(input)
        const method = init?.method ?? 'GET'
        if (url === '/api/monitor/subscriptions' && method === 'GET') {
          return jsonResponse({ items: [SUBSCRIPTION], categories: ['Actor', 'Rank'] })
        }
        if (url === '/api/monitor/subscriptions' && method === 'POST') {
          return jsonResponse({ ...SUBSCRIPTION, id: 2, url: 'https://rsshub.test/new', feed_url: 'https://rsshub.test/new' }, 201)
        }
        if (url === '/api/monitor/subscriptions/1' && method === 'PATCH') {
          return jsonResponse({ ...SUBSCRIPTION, enabled: false })
        }
        if (url === '/api/monitor/subscriptions/freshrss-import') {
          const apply = Boolean((JSON.parse(String(init?.body)) as { apply: boolean }).apply)
          return jsonResponse({
            entries: [{ url: 'https://rsshub.test/x', title: '甲', category: 'Actor', status: apply ? 'imported' : 'new' }],
            imported: apply ? 1 : 0,
          })
        }
        return jsonResponse({ error: { code: 'unknown' } }, 404)
      }),
    )
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('lists subscriptions under their category with the last poll error', async () => {
    render(<SubscriptionsPanel onUnauthorized={vi.fn()} />)

    expect(await screen.findByText('演员甲')).toBeInTheDocument()
    expect(screen.getByText('分类「Actor」未配置')).toBeInTheDocument()
    expect(screen.getByText('拉取出错')).toBeInTheDocument()
  })

  it('adds a feed to the chosen category', async () => {
    const user = userEvent.setup()
    render(<SubscriptionsPanel onUnauthorized={vi.fn()} />)
    await screen.findByText('演员甲')

    await user.type(screen.getByLabelText('Feed 地址'), 'https://rsshub.test/new')
    await user.click(screen.getByRole('button', { name: '添加' }))

    await waitFor(() =>
      expect(fetch).toHaveBeenCalledWith(
        '/api/monitor/subscriptions',
        expect.objectContaining({ method: 'POST', body: JSON.stringify({ url: 'https://rsshub.test/new', category: 'Actor' }) }),
      ),
    )
  })

  it('toggles a subscription off in place', async () => {
    const user = userEvent.setup()
    render(<SubscriptionsPanel onUnauthorized={vi.fn()} />)
    await screen.findByText('演员甲')

    await user.click(screen.getByRole('button', { name: '停用' }))

    expect(await screen.findByText('停用', { selector: '.run-state' })).toBeInTheDocument()
    expect(fetch).toHaveBeenCalledWith(
      '/api/monitor/subscriptions/1',
      expect.objectContaining({ method: 'PATCH', body: JSON.stringify({ enabled: false }) }),
    )
  })

  it('previews a FreshRSS import before applying it', async () => {
    const user = userEvent.setup()
    render(<SubscriptionsPanel onUnauthorized={vi.fn()} />)
    await screen.findByText('演员甲')

    await user.click(screen.getByRole('button', { name: '从 FreshRSS 导入（预览）' }))
    expect(await screen.findByText('将导入')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: '确认导入 1 条' }))
    expect(await screen.findByText('已导入')).toBeInTheDocument()
    expect(screen.getByRole('status')).toHaveTextContent('已导入 1 条订阅')
  })
})
