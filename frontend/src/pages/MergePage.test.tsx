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

describe('merge page', () => {
  let titlesBody: unknown

  beforeEach(() => {
    titlesBody = {
      items: [BIG, BROKEN, SMALL],
      routes: ['vr', 'clt', 'rank'],
      scanned_at: '2026-10-02T20:00:00Z',
      reason: null,
    }
    vi.stubGlobal(
      'fetch',
      vi.fn().mockImplementation((input) => {
        if (String(input) === '/api/merge/titles') return jsonResponse(titlesBody)
        return jsonResponse({ error: { code: 'not_found' } }, 404)
      }),
    )
  })

  afterEach(() => {
    vi.unstubAllGlobals()
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
})
