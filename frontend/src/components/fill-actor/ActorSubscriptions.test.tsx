import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { defaultCategory } from '../../lib/fill-actor/subscriptions'
import { ActorSubscriptions } from './ActorSubscriptions'

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

const SUBSCRIBED = {
  id: 9,
  kind: 'avbase_talent',
  category: 'Actor',
  enabled: true,
  url: null,
  feed_url: 'https://www.avbase.net/talents/46144/feed',
  talent_id: 46144,
  name: '石川澪',
  aliases: [],
  seed_pending: false,
  cursor_size: 30,
  last_polled_at: null,
  last_error: null,
  created_at: '2026-09-03T00:00:00Z',
  updated_at: '2026-09-03T00:00:00Z',
}

const ACTORS = [
  { actor_id: '石川澪', scraped_count: 184, video_ids: [], error_code: null, actor_name: '石川澪', talent_id: 46144, aliases: [] },
  {
    actor_id: '河北彩伽',
    scraped_count: 283,
    video_ids: [],
    error_code: null,
    actor_name: '河北彩花',
    talent_id: 5022,
    aliases: ['河北彩伽'],
  },
  { actor_id: 'rwt', scraped_count: 8, video_ids: [], error_code: null, actor_name: '塔乃花鈴', talent_id: null, aliases: [] },
]

describe('actor subscriptions', () => {
  beforeEach(() => {
    window.localStorage.clear()
    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((input, init?: RequestInit) => {
        const url = String(input)
        const method = init?.method ?? 'GET'
        if (url === '/api/monitor/subscriptions' && method === 'GET') {
          return jsonResponse({ items: [SUBSCRIBED], categories: ['Actor', 'Rank'] })
        }
        if (url === '/api/monitor/subscriptions' && method === 'POST') {
          const body = JSON.parse(String(init?.body)) as { talent_id: number; name: string; category: string }
          return jsonResponse({ ...SUBSCRIBED, id: 10, talent_id: body.talent_id, name: body.name, category: body.category }, 201)
        }
        if (url.startsWith('/api/config')) return jsonResponse(SECTIONS)
        return jsonResponse({ error: { code: 'unknown' } }, 404)
      }),
    )
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('picks the category filing into fill actor\'s own directory', () => {
    expect(defaultCategory(SECTIONS as never, ['Rank', 'Actor'])).toBe('Actor')
    expect(defaultCategory([], ['Rank', 'Actor'])).toBe('Rank')
  })

  it('shows who is subscribed and subscribes the rest with a pending seed', async () => {
    const user = userEvent.setup()
    render(<ActorSubscriptions actors={ACTORS} onUnauthorized={vi.fn()} />)

    expect(await screen.findByText('已订阅 · Actor')).toBeInTheDocument()
    expect(screen.getByText('别名：河北彩伽')).toBeInTheDocument()
    expect(screen.queryByText('塔乃花鈴')).not.toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: '订阅' }))

    await waitFor(() =>
      expect(fetch).toHaveBeenCalledWith(
        '/api/monitor/subscriptions',
        expect.objectContaining({
          method: 'POST',
          body: JSON.stringify({
            kind: 'avbase_talent',
            talent_id: 5022,
            name: '河北彩花',
            aliases: ['河北彩伽'],
            category: 'Actor',
            seed: true,
          }),
        }),
      ),
    )
    expect(await screen.findAllByText('已订阅 · Actor')).toHaveLength(2)
  })

  it('explains when none of the actors is on AVBase', () => {
    render(<ActorSubscriptions actors={[ACTORS[2]]} onUnauthorized={vi.fn()} />)

    expect(screen.getByText('AVBase 没有收录这些演员，只能通过扫描补全，无法订阅新作。')).toBeInTheDocument()
  })
})
