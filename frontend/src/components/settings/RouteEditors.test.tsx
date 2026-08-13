import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useState } from 'react'
import { describe, expect, it } from 'vitest'

import type { BrandRoute, DirRoute } from '../../lib/settings/routes'
import { toBrandRoutes, toDirRoutes } from '../../lib/settings/routes'
import { BrandRoutesField, DirRoutesField } from './RouteEditors'

function DirHarness({ initial }: { initial: unknown }) {
  const [rows, setRows] = useState<DirRoute[]>(() => toDirRoutes(initial))
  return <DirRoutesField rows={rows} srcRoot="/downloads" dstRoot="/media/library" onChange={setRows} />
}

function BrandHarness({ initial }: { initial: unknown }) {
  const [rows, setRows] = useState<BrandRoute[]>(() => toBrandRoutes(initial))
  return <BrandRoutesField rows={rows} dstRoot="/media/library" onChange={setRows} />
}

describe('DirRoutesField', () => {
  it('previews the absolute paths a route resolves to', () => {
    render(<DirHarness initial={{ intake: 'sorted' }} />)
    expect(screen.getByText('/downloads/intake → /media/library/sorted/<厂牌>')).toBeInTheDocument()
  })

  it('adds and removes rows', async () => {
    const user = userEvent.setup()
    render(<DirHarness initial={{ intake: 'sorted' }} />)

    await user.click(screen.getByRole('button', { name: '+ 添加子目录路由' }))
    expect(screen.getAllByLabelText('来源子目录')).toHaveLength(2)

    await user.click(screen.getByRole('button', { name: '删除路由 intake' }))
    const remaining = screen.getAllByLabelText('来源子目录')
    expect(remaining).toHaveLength(1)
    expect(remaining[0]).toHaveValue('')
  })

  it('says so when nothing is routed', () => {
    render(<DirHarness initial={{}} />)
    expect(screen.getByText('还没有子目录路由，归档整理不会处理任何目录。')).toBeInTheDocument()
  })
})

describe('BrandRoutesField', () => {
  it('turns typed brands into upper-cased chips on enter', async () => {
    const user = userEvent.setup()
    render(<BrandHarness initial={{ special: [] }} />)

    await user.type(screen.getByLabelText('厂牌'), 'abc{Enter}')

    expect(screen.getByRole('button', { name: '移除厂牌 ABC' })).toBeInTheDocument()
  })

  it('splits a pasted list into one chip per brand', async () => {
    const user = userEvent.setup()
    render(<BrandHarness initial={{ special: [] }} />)

    await user.type(screen.getByLabelText('厂牌'), 'ABC, DEF ')

    expect(screen.getByRole('button', { name: '移除厂牌 ABC' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '移除厂牌 DEF' })).toBeInTheDocument()
  })

  it('removes a chip and previews where the remaining brand lands', async () => {
    const user = userEvent.setup()
    render(<BrandHarness initial={{ special: ['ABC', 'DEF'] }} />)

    await user.click(screen.getByRole('button', { name: '移除厂牌 DEF' }))

    expect(screen.queryByRole('button', { name: '移除厂牌 DEF' })).not.toBeInTheDocument()
    expect(screen.getByText('例：ABC-001 → /media/library/special/ABC')).toBeInTheDocument()
  })
})
