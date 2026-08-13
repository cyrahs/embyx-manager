/** Row editors for the archive section's two routing tables.
 *
 * Each entry gets its own row plus a preview of the absolute path the pipeline
 * will actually use, so the routing is readable without knowing the stored shape.
 */

import { useState } from 'react'

import type { BrandRoute, DirRoute } from '../../lib/settings/routes'
import { BRAND_SEPARATORS, joinPath, nextRowId, normalizeBrands, trimSubdir } from '../../lib/settings/routes'

interface DirRoutesFieldProps {
  rows: DirRoute[]
  srcRoot: string
  dstRoot: string
  onChange: (rows: DirRoute[]) => void
}

export function DirRoutesField({ rows, srcRoot, dstRoot, onChange }: DirRoutesFieldProps) {
  function patch(id: string, changes: Partial<DirRoute>) {
    onChange(rows.map((row) => (row.id === id ? { ...row, ...changes } : row)))
  }

  return (
    <div className="route-rows">
      {rows.length === 0 && <p className="route-empty">还没有子目录路由，归档整理不会处理任何目录。</p>}
      {rows.map((row) => (
        <div className="route-row" key={row.id}>
          <input
            type="text"
            aria-label="来源子目录"
            placeholder="intake"
            autoComplete="off"
            spellCheck={false}
            value={row.src}
            onChange={(event) => patch(row.id, { src: event.target.value })}
          />
          <span className="route-arrow" aria-hidden="true">
            →
          </span>
          <input
            type="text"
            aria-label="目标子目录"
            placeholder="sorted"
            autoComplete="off"
            spellCheck={false}
            value={row.dst}
            onChange={(event) => patch(row.id, { dst: event.target.value })}
          />
          <button
            className="route-remove"
            type="button"
            aria-label={row.src ? `删除路由 ${row.src}` : '删除这条路由'}
            onClick={() => onChange(rows.filter((other) => other.id !== row.id))}
          >
            移除
          </button>
          {trimSubdir(row.src) && trimSubdir(row.dst) && (
            <p className="route-preview">
              {joinPath(srcRoot, '<来源根目录>', row.src)} → {joinPath(dstRoot, '<目标根目录>', row.dst)}/&lt;厂牌&gt;
            </p>
          )}
        </div>
      ))}
      <button
        className="text-button route-add"
        type="button"
        onClick={() => onChange([...rows, { id: nextRowId(), src: '', dst: '' }])}
      >
        + 添加子目录路由
      </button>
    </div>
  )
}

interface BrandChipsProps {
  brands: string[]
  onChange: (brands: string[]) => void
}

function BrandChips({ brands, onChange }: BrandChipsProps) {
  const [draft, setDraft] = useState('')

  function commit(text: string) {
    const added = normalizeBrands(text.split(BRAND_SEPARATORS))
    if (added.length === 0) return
    onChange(normalizeBrands([...brands, ...added]))
  }

  return (
    <div className="chips">
      {brands.map((brand) => (
        <span className="chip" key={brand}>
          {brand}
          <button
            type="button"
            aria-label={`移除厂牌 ${brand}`}
            onClick={() => onChange(brands.filter((other) => other !== brand))}
          >
            ×
          </button>
        </span>
      ))}
      <input
        type="text"
        aria-label="厂牌"
        placeholder={brands.length === 0 ? '输入厂牌后回车' : ''}
        autoComplete="off"
        spellCheck={false}
        value={draft}
        onChange={(event) => {
          const value = event.target.value
          // Pasting "ABC, DEF" should land as two chips rather than one string.
          if (BRAND_SEPARATORS.test(value)) {
            commit(value)
            setDraft('')
          } else {
            setDraft(value)
          }
        }}
        onKeyDown={(event) => {
          if (event.key === 'Enter') {
            event.preventDefault()
            commit(draft)
            setDraft('')
          } else if (event.key === 'Backspace' && draft === '' && brands.length > 0) {
            onChange(brands.slice(0, -1))
          }
        }}
        onBlur={() => {
          commit(draft)
          setDraft('')
        }}
      />
    </div>
  )
}

interface BrandRoutesFieldProps {
  rows: BrandRoute[]
  dstRoot: string
  onChange: (rows: BrandRoute[]) => void
}

export function BrandRoutesField({ rows, dstRoot, onChange }: BrandRoutesFieldProps) {
  function patch(id: string, changes: Partial<BrandRoute>) {
    onChange(rows.map((row) => (row.id === id ? { ...row, ...changes } : row)))
  }

  return (
    <div className="route-rows">
      {rows.length === 0 && <p className="route-empty">没有厂牌例外，所有厂牌都按上面的子目录路由归档。</p>}
      {rows.map((row) => (
        <div className="route-row brand-row" key={row.id}>
          <BrandChips brands={row.brands} onChange={(brands) => patch(row.id, { brands })} />
          <span className="route-arrow" aria-hidden="true">
            →
          </span>
          <input
            type="text"
            aria-label="目标子目录"
            placeholder="special"
            autoComplete="off"
            spellCheck={false}
            value={row.dst}
            onChange={(event) => patch(row.id, { dst: event.target.value })}
          />
          <button
            className="route-remove"
            type="button"
            aria-label={row.dst ? `删除厂牌路由 ${row.dst}` : '删除这条厂牌路由'}
            onClick={() => onChange(rows.filter((other) => other.id !== row.id))}
          >
            移除
          </button>
          {trimSubdir(row.dst) && row.brands.length > 0 && (
            <p className="route-preview">
              例：{row.brands[0]}-001 → {joinPath(dstRoot, '<目标根目录>', row.dst, row.brands[0])}
            </p>
          )}
        </div>
      ))}
      <button
        className="text-button route-add"
        type="button"
        onClick={() => onChange([...rows, { id: nextRowId(), dst: '', brands: [] }])}
      >
        + 添加厂牌路由
      </button>
    </div>
  )
}
