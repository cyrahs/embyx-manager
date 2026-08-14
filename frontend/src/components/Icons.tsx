import type { ActorFeedStatus, VideoState } from '../types'

export function Spinner() {
  return <span className="spinner" aria-hidden="true" />
}

export function ScanIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="11" cy="11" r="6" />
      <path d="m16 16 4 4M11 8v6M8 11h6" />
    </svg>
  )
}

export function MoveIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M4 12h15m-5-5 5 5-5 5" />
      <path d="M4 5v14" />
    </svg>
  )
}

export function ArrowIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="m9 18 6-6-6-6" />
    </svg>
  )
}

export function ChevronIcon({ expanded }: { expanded: boolean }) {
  return (
    <svg className={`chevron ${expanded ? 'expanded' : ''}`} viewBox="0 0 24 24" aria-hidden="true">
      <path d="m9 6 6 6-6 6" />
    </svg>
  )
}

export function CheckIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="m5 12 4 4L19 6" />
    </svg>
  )
}

export function AlertIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 3 2.7 20h18.6L12 3Z" />
      <path d="M12 9v5m0 3h.01" />
    </svg>
  )
}

export function CancelIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="12" cy="12" r="8" />
      <path d="m9 9 6 6m0-6-6 6" />
    </svg>
  )
}

export function CopyIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <rect x="8" y="8" width="11" height="11" rx="2" />
      <path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2" />
    </svg>
  )
}

export function ExternalIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M14 4h6v6m0-6-9 9" />
      <path d="M19 13v6a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1h6" />
    </svg>
  )
}

export function FeedIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M5 5a14 14 0 0 1 14 14M5 11a8 8 0 0 1 8 8" />
      <circle cx="5" cy="19" r="1" />
    </svg>
  )
}

export function MagnetIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M7 3v8a5 5 0 0 0 10 0V3M7 7h4m2 0h4" />
    </svg>
  )
}

export function KeyIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="8" cy="12" r="4" />
      <path d="M12 12h9m-3 0v3m-3-3v2" />
    </svg>
  )
}

export function SearchIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="11" cy="11" r="7" />
      <path d="m16 16 4 4" />
    </svg>
  )
}

export function FileIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M6 3h8l4 4v14H6z" />
      <path d="M14 3v5h4" />
    </svg>
  )
}

export function FeedStateIcon({ state }: { state: ActorFeedStatus['state'] }) {
  if (state === 'ready') return <CheckIcon />
  if (state === 'failed') return <AlertIcon />
  if (state === 'warming') return <Spinner />
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="12" cy="12" r="8" />
      <path d="M12 8v5l3 2" />
    </svg>
  )
}

export function StatusIcon({ state }: { state: VideoState }) {
  if (state === 'exists') return <CheckIcon />
  if (state === 'additional_found') return <MoveIcon />
  if (state === 'submitted') return <MagnetIcon />
  if (state === 'already_tracked') return <MagnetIcon />
  if (state === 'missing') return <SearchIcon />
  return <AlertIcon />
}
