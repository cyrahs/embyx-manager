import { describe, expect, it } from 'vitest'

import type { BrandRoute, DirRoute } from './routes'
import { fromBrandRoutes, fromDirRoutes, toBrandRoutes, toDirRoutes } from './routes'

function dirRows(...pairs: [string, string][]): DirRoute[] {
  return pairs.map(([src, dst], index) => ({ id: `row-${index}`, src, dst }))
}

function brandRows(...pairs: [string, string[]][]): BrandRoute[] {
  return pairs.map(([dst, brands], index) => ({ id: `row-${index}`, dst, brands }))
}

describe('dir routes', () => {
  it('round-trips the stored mapping', () => {
    const rows = toDirRoutes({ intake: 'sorted', extra: 'library' })
    expect(rows.map((row) => [row.src, row.dst])).toEqual([
      ['intake', 'sorted'],
      ['extra', 'library'],
    ])
    expect(fromDirRoutes(rows)).toEqual({ intake: 'sorted', extra: 'library' })
  })

  it('reads a missing or malformed mapping as no rows', () => {
    expect(toDirRoutes(undefined)).toEqual([])
    expect(toDirRoutes('intake')).toEqual([])
  })

  it('trims surrounding slashes and drops fully blank rows', () => {
    expect(fromDirRoutes(dirRows(['  /intake/ ', 'sorted'], ['', '']))).toEqual({ intake: 'sorted' })
  })

  it('rejects a half-filled row', () => {
    expect(() => fromDirRoutes(dirRows(['intake', '']))).toThrow('都要填写')
  })

  it('rejects a repeated source subdirectory', () => {
    expect(() => fromDirRoutes(dirRows(['intake', 'sorted'], ['intake', 'other']))).toThrow('出现了多次')
  })
})

describe('brand routes', () => {
  it('round-trips the stored brand mapping', () => {
    const rows = toBrandRoutes({ special: ['ABC', 'DEF'] })
    expect(rows.map((row) => [row.dst, row.brands])).toEqual([['special', ['ABC', 'DEF']]])
    expect(fromBrandRoutes(rows)).toEqual({ special: ['ABC', 'DEF'] })
  })

  it('upper-cases brands and drops duplicates within a row', () => {
    expect(fromBrandRoutes(brandRows(['special', [' abc ', 'ABC', 'def']]))).toEqual({ special: ['ABC', 'DEF'] })
  })

  it('rejects a row missing its destination or its brands', () => {
    expect(() => fromBrandRoutes(brandRows(['', ['ABC']]))).toThrow('还没有指定目标子目录')
    expect(() => fromBrandRoutes(brandRows(['special', []]))).toThrow('还没有指定厂牌')
  })

  it('rejects a brand routed to two destinations, which the pipeline resolves silently', () => {
    expect(() => fromBrandRoutes(brandRows(['special', ['ABC']], ['other', ['ABC']]))).toThrow('同时归到了')
  })

  it('rejects a repeated destination', () => {
    expect(() => fromBrandRoutes(brandRows(['special', ['ABC']], ['special', ['DEF']]))).toThrow('出现了多次')
  })
})
