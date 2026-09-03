import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { SubscriptionsPanel } from './SubscriptionsPanel'

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
}

const SUBSCRIPTION = {
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
  ...SUBSCRIPTION,
  id: 2,
  kind: 'avbase_talent',
  category: 'Actor',
  url: null,
  feed_url: 'https://www.avbase.net/talents/46144/feed',
  talent_id: 46144,
  name: '石川澪',
  last_error: null,
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
          return jsonResponse({ items: [SUBSCRIPTION, TALENT], categories: ['Actor', 'Rank'] })
        }
        if (url === '/api/monitor/subscriptions' && method === 'POST') {
          return jsonResponse({ ...SUBSCRIPTION, id: 3, url: 'https://rsshub.test/new', feed_url: 'https://rsshub.test/new' }, 201)
        }
        if (url === '/api/monitor/subscriptions/1' && method === 'PATCH') {
          const body = JSON.parse(String(init?.body)) as { enabled?: boolean; url?: string }
          return jsonResponse({
            ...SUBSCRIPTION,
            enabled: body.enabled ?? SUBSCRIPTION.enabled,
            url: body.url ?? SUBSCRIPTION.url,
            feed_url: body.url ?? SUBSCRIPTION.feed_url,
            last_error: body.url ? null : SUBSCRIPTION.last_error,
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

    expect(await screen.findByText('最想要')).toBeInTheDocument()
    expect(screen.getByText('石川澪')).toBeInTheDocument()
    expect(screen.getByText('拉取出错')).toBeInTheDocument()
    // Only a plain feed has a URL to change.
    expect(screen.getAllByRole('button', { name: '改地址' })).toHaveLength(1)
  })

  it('adds a feed to the chosen category', async () => {
    const user = userEvent.setup()
    render(<SubscriptionsPanel onUnauthorized={vi.fn()} />)
    await screen.findByText('最想要')

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
    await screen.findByText('最想要')

    const row = screen.getByText('最想要').closest('tr') as HTMLElement
    await user.click(within(row).getByRole('button', { name: '停用' }))

    expect(await within(row).findByText('停用', { selector: '.run-state' })).toBeInTheDocument()
    expect(fetch).toHaveBeenCalledWith(
      '/api/monitor/subscriptions/1',
      expect.objectContaining({ method: 'PATCH', body: JSON.stringify({ enabled: false }) }),
    )
  })

  it('corrects a feed URL in place', async () => {
    const user = userEvent.setup()
    render(<SubscriptionsPanel onUnauthorized={vi.fn()} />)
    await screen.findByText('最想要')

    await user.click(screen.getByRole('button', { name: '改地址' }))
    const field = screen.getByLabelText('新的 Feed 地址')
    await user.clear(field)
    await user.type(field, 'http://rsshub.rss.svc.cluster.local/javlibrary/mostwanted/cn')
    await user.click(screen.getByRole('button', { name: '保存' }))

    expect(await screen.findByText('http://rsshub.rss.svc.cluster.local/javlibrary/mostwanted/cn')).toBeInTheDocument()
    expect(fetch).toHaveBeenCalledWith(
      '/api/monitor/subscriptions/1',
      expect.objectContaining({
        method: 'PATCH',
        body: JSON.stringify({ url: 'http://rsshub.rss.svc.cluster.local/javlibrary/mostwanted/cn' }),
      }),
    )
    expect(screen.queryByText('拉取出错')).not.toBeInTheDocument()
  })
})
