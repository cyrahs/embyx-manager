import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ManualIntakeDialog } from './ManualIntake'

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
}

const LISTINGS: Record<string, unknown> = {
  '/': {
    path: '/',
    parent: null,
    default_path: '/115/embyx_in',
    entries: [{ path: '/115', name: '115', configured: false, routed: false }],
  },
  '/115': {
    path: '/115',
    parent: '/',
    default_path: '/115/embyx_in',
    entries: [
      { path: '/115/embyx_in', name: 'embyx_in', configured: true, routed: true },
      { path: '/115/misc', name: 'misc', configured: false, routed: false },
    ],
  },
  '/115/embyx_in': {
    path: '/115/embyx_in',
    parent: '/115',
    default_path: '/115/embyx_in',
    entries: [{ path: '/115/embyx_in/rank', name: 'rank', configured: false, routed: true }],
  },
}

function renderDialog() {
  const onSubmitted = vi.fn()
  render(
    <ManualIntakeDialog onClose={vi.fn()} onSubmitted={onSubmitted} onUnauthorized={vi.fn()} />,
  )
  return { onSubmitted }
}

describe('manual intake dialog', () => {
  beforeEach(() => {
    window.localStorage.clear()
    vi.stubGlobal('fetch', vi.fn().mockImplementation((input) => {
      const url = String(input)
      if (url.startsWith('/api/monitor/manual/directories')) {
        const path = new URL(url, 'http://test').searchParams.get('path') ?? '/'
        const listing = LISTINGS[path]
        return listing ? jsonResponse(listing) : jsonResponse({ error: { code: 'directory_not_found' } }, 404)
      }
      return jsonResponse({})
    }))
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('submits the parsed lines to the remembered directory and reports each outcome', async () => {
    const user = userEvent.setup()
    const { onSubmitted } = renderDialog()

    // The remembered directory arrives with the first listing, so nothing has to be picked.
    expect(await screen.findByText('/115/embyx_in')).toBeInTheDocument()

    vi.mocked(fetch).mockImplementationOnce(() => jsonResponse({
      task_dir_path: '/115/embyx_in',
      items: [
        { text: 'abc-123', avid: 'ABC-123', outcome: 'submitted', archived_paths: [] },
        { text: '???', avid: null, outcome: 'unreadable', archived_paths: [] },
        { text: 'def-456', avid: 'DEF-456', outcome: 'already_in_library', archived_paths: ['embyx/DEF/DEF-456.mp4'] },
      ],
    }))
    await user.type(screen.getByRole('textbox'), 'abc-123\n???\ndef-456')
    await user.click(screen.getByRole('button', { name: '提交 3 个番号' }))

    expect(await screen.findByText('已提交离线')).toBeInTheDocument()
    expect(screen.getByText('无法识别番号')).toBeInTheDocument()
    expect(screen.getByText('embyx/DEF/DEF-456.mp4')).toBeInTheDocument()
    // The panel behind only refreshes when something actually reached the ledger.
    expect(onSubmitted).toHaveBeenCalledOnce()

    const [, init] = vi.mocked(fetch).mock.calls.at(-1) as [string, RequestInit]
    expect(JSON.parse(String(init.body))).toEqual({
      inputs: ['abc-123', '???', 'def-456'],
      task_dir_path: '/115/embyx_in',
    })
  })

  it('opens the browser at the remembered directory and offers only routed ones', async () => {
    const user = userEvent.setup()
    renderDialog()

    await screen.findByText('/115/embyx_in')
    await user.click(screen.getByRole('button', { name: '浏览' }))

    // Browsing starts where the last batch went, not back at the root.
    expect(await screen.findByRole('button', { name: 'rank' })).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: '上一级' }))

    expect(await screen.findByRole('button', { name: 'embyx_in' })).toBeInTheDocument()
    // The unrouted one can be opened but not chosen: a download there could never be filed.
    expect(screen.getByText('无归档路由')).toBeInTheDocument()
    expect(screen.getAllByRole('button', { name: '选择' })).toHaveLength(1)

    await user.click(screen.getByRole('button', { name: '选择' }))

    await waitFor(() => expect(screen.queryByText('无归档路由')).not.toBeInTheDocument())
    expect(screen.getByText('/115/embyx_in')).toBeInTheDocument()
  })

  it('shows why a submission was refused', async () => {
    const user = userEvent.setup()
    renderDialog()

    await screen.findByText('/115/embyx_in')
    vi.mocked(fetch).mockImplementationOnce(() =>
      jsonResponse({ error: { code: 'directory_not_routed' } }, 422),
    )
    await user.type(screen.getByRole('textbox'), 'abc-123')
    await user.click(screen.getByRole('button', { name: '提交 1 个番号' }))

    expect(await screen.findByText(/没有对应的归档路由/)).toBeInTheDocument()
  })
})
