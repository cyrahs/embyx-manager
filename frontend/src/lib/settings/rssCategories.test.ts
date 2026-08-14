import { describe, expect, it } from 'vitest'

import type { RssCategoryRow } from './rssCategories'
import { fromRssCategories, toRssCategories } from './rssCategories'

function rows(...pairs: [string, string][]): RssCategoryRow[] {
  return pairs.map(([label, taskDir], index) => ({ id: `row-${index}`, label, taskDir }))
}

describe('rss categories', () => {
  it('round-trips the stored list', () => {
    const stored = [
      { label: 'Actor', task_dir_path: '/115/embyx_in' },
      { label: 'Rank', task_dir_path: '/115/embyx_in/rank' },
    ]
    const parsed = toRssCategories(stored)

    expect(parsed.map((row) => [row.label, row.taskDir])).toEqual([
      ['Actor', '/115/embyx_in'],
      ['Rank', '/115/embyx_in/rank'],
    ])
    expect(fromRssCategories(parsed)).toEqual(stored)
  })

  it('reads a missing or malformed list as no rows', () => {
    expect(toRssCategories(undefined)).toEqual([])
    expect(toRssCategories({ label: 'Rank' })).toEqual([])
  })

  it('trims a trailing slash and drops fully blank rows', () => {
    expect(fromRssCategories(rows([' Rank ', ' /115/rank/ '], ['', '']))).toEqual([
      { label: 'Rank', task_dir_path: '/115/rank' },
    ])
  })

  it('refuses a category without a directory', () => {
    // There is no shared default to fall back on.
    expect(() => fromRssCategories(rows(['Actor', '']))).toThrow('离线目录')
  })

  it('refuses a directory without a category', () => {
    expect(() => fromRssCategories(rows(['', '/115/rank']))).toThrow('分类名')
  })

  it('refuses a repeated category', () => {
    expect(() => fromRssCategories(rows(['Rank', '/115/a'], ['Rank', '/115/b']))).toThrow('出现了多次')
  })

  it('refuses a relative directory', () => {
    expect(() => fromRssCategories(rows(['Rank', 'embyx_in/rank']))).toThrow('绝对路径')
  })
})
