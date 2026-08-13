import { ApiError } from '../api'

/**
 * Turns any thrown value into something worth showing a user. Feature-specific
 * codes are translated by that feature, not here — see `lib/fill-actor/format`.
 */
export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) return error.message
  return '操作未能完成，请稍后重试。'
}
