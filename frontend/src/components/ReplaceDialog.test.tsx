import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ReplaceDialog } from './ReplaceDialog'

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
}

const PLAN = {
  avid: 'IPZ-661',
  holdings: ['embyx/rank/IPZ/IPZ-661.mp4'],
  state: 'archived',
  task_dir_path: '/115/embyx_in/rank',
  blocked: null,
}

describe('replace dialog', () => {
  beforeEach(() => {
    window.localStorage.clear()
    vi.stubGlobal('fetch', vi.fn().mockImplementation((input, init?: RequestInit) => {
      const url = String(input)
      if (url.startsWith('/api/monitor/replace') && (init?.method ?? 'GET') === 'GET') return jsonResponse(PLAN)
      if (url === '/api/monitor/replace') {
        return jsonResponse({
          plan: PLAN,
          outcome: 'submitted',
          acquisition: { avid: 'IPZ-661', state: 'downloading', replacing: PLAN.holdings },
        })
      }
      return jsonResponse({})
    }))
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('previews the copy that goes, then starts with the pasted magnet', async () => {
    const user = userEvent.setup()
    const onStarted = vi.fn()
    render(<ReplaceDialog initialAvid="IPZ-661" onClose={vi.fn()} onStarted={onStarted} onUnauthorized={vi.fn()} />)

    expect(await screen.findByText('embyx/rank/IPZ/IPZ-661.mp4')).toBeInTheDocument()
    expect(screen.getByText('/115/embyx_in/rank')).toBeInTheDocument()

    await user.type(screen.getByRole('textbox', { name: '磁力链接' }), 'magnet:?xt=urn:btih:abc')
    await user.click(screen.getByRole('button', { name: '开始换版' }))

    expect(await screen.findByText('已提交离线')).toBeInTheDocument()
    expect(onStarted).toHaveBeenCalledOnce()
    const post = vi.mocked(fetch).mock.calls.find(([, init]) => (init as RequestInit | undefined)?.method === 'POST')
    expect(JSON.parse(String((post?.[1] as RequestInit).body))).toEqual({
      avid: 'IPZ-661',
      magnet: 'magnet:?xt=urn:btih:abc',
    })
  })

  it('explains why a title cannot be replaced', async () => {
    vi.mocked(fetch).mockImplementation(() => jsonResponse({ ...PLAN, holdings: [], blocked: 'not_in_library' }))
    const user = userEvent.setup()
    render(<ReplaceDialog onClose={vi.fn()} onStarted={vi.fn()} onUnauthorized={vi.fn()} />)

    await user.type(screen.getByRole('textbox', { name: '番号' }), 'xyz-001')
    await user.click(screen.getByRole('button', { name: '查询' }))

    expect(await screen.findByText(/库里没有这个番号/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '开始换版' })).toBeDisabled()
  })
})
