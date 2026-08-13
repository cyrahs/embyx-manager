import { useEffect, useState, useSyncExternalStore } from 'react'

import {
  ApiError,
  hasApiToken,
  isUnauthorized,
  readApiToken,
  readAuthRequired,
  setApiToken,
  subscribeApiToken,
  verifyApiToken,
  type HealthStatus,
} from '../api'

/** True while this browser holds a signed-in token, shared across every page. */
export function useApiTokenConfigured(): boolean {
  return useSyncExternalStore(subscribeApiToken, hasApiToken, () => false)
}

/** The token itself, so a page can resume work whenever the value changes — not only
 *  when one first appears; re-logging in after a rotation swaps one token for another. */
export function useApiTokenValue(): string | null {
  return useSyncExternalStore(subscribeApiToken, readApiToken, () => null)
}

/** Tokens the server accepted during this page load, so the gate checks each one once. */
const verifiedTokens = new Set<string>()

/** Verifies the token before storing it: a wrong one must fail here, not on the first action. */
export async function signIn(token: string): Promise<void> {
  const value = token.trim()
  if (!value) throw new ApiError(0, 'empty_token', '请输入 API Token。')
  await verifyApiToken(value)
  verifiedTokens.add(value)
  setApiToken(value)
}

export function signOut(): void {
  const token = readApiToken()
  if (token) verifiedTokens.delete(token)
  setApiToken('')
}

export interface AuthGate {
  /** `login` hides the whole app behind the login screen. */
  screen: 'app' | 'login'
  /** Why the login screen came back, when it was not the user's own doing. */
  notice: string | null
  authRequired: boolean
}

/**
 * Decides whether the app or the login screen is shown. The server's `auth_required`
 * flag is cached from the previous visit so a returning visitor does not see the app
 * flash by while `/api/health` is still in flight.
 */
export function useAuthGate(health: HealthStatus | null): AuthGate {
  const token = useApiTokenValue()
  const [notice, setNotice] = useState<string | null>(null)
  const authRequired = health?.auth_required ?? readAuthRequired()

  useEffect(() => {
    if (token) setNotice(null)
  }, [token])

  // A token stored on an earlier visit may have been rotated since; confirm it once.
  useEffect(() => {
    if (!authRequired || !token || verifiedTokens.has(token)) return undefined
    let cancelled = false
    void verifyApiToken(token)
      .then(() => {
        verifiedTokens.add(token)
      })
      .catch((error: unknown) => {
        // Anything short of a rejection (offline, restarting server) leaves the token alone.
        if (cancelled || !isUnauthorized(error)) return
        signOut()
        setNotice('登录状态已失效，请重新登录。')
      })
    return () => {
      cancelled = true
    }
  }, [authRequired, token])

  return { screen: authRequired && !token ? 'login' : 'app', notice, authRequired }
}
