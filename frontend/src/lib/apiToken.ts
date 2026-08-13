import { useSyncExternalStore } from 'react'

import { hasApiToken, subscribeApiToken } from '../api'

/** True while the current browser session holds a token, shared across every page. */
export function useApiTokenConfigured(): boolean {
  return useSyncExternalStore(subscribeApiToken, hasApiToken, () => false)
}
