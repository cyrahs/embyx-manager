/** Row model for the archive section's two routing tables.
 *
 * Both are dictionaries in the config store; the settings UI edits them as rows
 * so the shape stops being a guessing game, and these helpers convert between
 * the two representations while rejecting entries the pipeline cannot honour.
 */

export interface DirRoute {
  id: string
  src: string
  dst: string
}

export interface BrandRoute {
  id: string
  dst: string
  brands: string[]
}

export const BRAND_SEPARATORS = /[\s,，、;；]+/

let rowSeq = 0

export function nextRowId(): string {
  rowSeq += 1
  return `route-${rowSeq}`
}

/** Subdirectories are relative to a configured root, so slashes around them are noise. */
export function trimSubdir(value: string): string {
  return value.trim().replace(/^\/+|\/+$/g, '')
}

/** Brands are matched against upper-cased video IDs; duplicates collapse. */
export function normalizeBrands(brands: string[]): string[] {
  const normalized: string[] = []
  for (const brand of brands) {
    const name = brand.trim().toUpperCase()
    if (name && !normalized.includes(name)) normalized.push(name)
  }
  return normalized
}

export function joinPath(root: string, fallback: string, ...parts: string[]): string {
  const base = root.trim().replace(/\/+$/, '') || fallback
  return [base, ...parts.map(trimSubdir).filter(Boolean)].join('/')
}

export function toDirRoutes(value: unknown): DirRoute[] {
  if (!value || typeof value !== 'object') return []
  return Object.entries(value as Record<string, unknown>).map(([src, dst]) => ({
    id: nextRowId(),
    src,
    dst: typeof dst === 'string' ? dst : '',
  }))
}

export function fromDirRoutes(rows: DirRoute[]): Record<string, string> {
  const mapping: Record<string, string> = {}
  for (const row of rows) {
    const src = trimSubdir(row.src)
    const dst = trimSubdir(row.dst)
    if (!src && !dst) continue
    if (!src || !dst) throw new Error('每条路由的来源子目录和目标子目录都要填写')
    if (src in mapping) throw new Error(`来源子目录「${src}」出现了多次`)
    mapping[src] = dst
  }
  return mapping
}

export function toBrandRoutes(value: unknown): BrandRoute[] {
  if (!value || typeof value !== 'object') return []
  return Object.entries(value as Record<string, unknown>).map(([dst, brands]) => ({
    id: nextRowId(),
    dst,
    brands: Array.isArray(brands) ? brands.map(String) : [],
  }))
}

export function fromBrandRoutes(rows: BrandRoute[]): Record<string, string[]> {
  const mapping: Record<string, string[]> = {}
  const routedTo = new Map<string, string>()
  for (const row of rows) {
    const dst = trimSubdir(row.dst)
    const brands = normalizeBrands(row.brands)
    if (!dst && brands.length === 0) continue
    if (!dst) throw new Error(`厂牌「${brands.join('、')}」还没有指定目标子目录`)
    if (brands.length === 0) throw new Error(`目标子目录「${dst}」还没有指定厂牌`)
    if (dst in mapping) throw new Error(`目标子目录「${dst}」出现了多次`)
    for (const brand of brands) {
      // The pipeline stops at the first destination listing the brand, so the
      // same brand in two rows would silently follow whichever comes first.
      const previous = routedTo.get(brand)
      if (previous !== undefined) throw new Error(`厂牌「${brand}」同时归到了「${previous}」和「${dst}」`)
      routedTo.set(brand, dst)
    }
    mapping[dst] = brands
  }
  return mapping
}
