import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useOutletContext } from 'react-router-dom'

import {
  ApiError,
  cancelPlan,
  createPlan,
  getActiveApplyRequest,
  getActivePlanId,
  getActorNames,
  getApplyJob,
  getPlan,
  resolveAvidActors,
  setActiveApplyRequest,
  setActivePlanId,
  setActorNames as storeActorNames,
  startApplyJob,
} from '../api'
import type { AppContext } from '../App'
import { Notice } from '../components/Feedback'
import { ActorFeeds } from '../components/fill-actor/ActorFeeds'
import { ApplySummary, ConfirmDialog } from '../components/fill-actor/ApplyFeedback'
import { ProgressPanel } from '../components/fill-actor/ProgressPanel'
import { ActorFailures, PlanSummary, VideoGroup } from '../components/fill-actor/Results'
import { AvidActorChoiceDialog, ExistingSubscriptionsDialog, ScanPanel } from '../components/fill-actor/ScanPanel'
import { ArrowIcon, MoveIcon, SearchIcon, Spinner } from '../components/Icons'
import { useApiTokenValue } from '../lib/apiToken'
import {
  applyPlaceholder,
  assertActiveApplyEnvelope,
  candidateMap,
  createApplyRequestId,
  errorMessage,
  isJobCancelled,
  isJobPending,
  jobState,
  parseActorIds,
  parseAvids,
  terminalApplyJob,
} from '../lib/fill-actor/format'
import { MAX_ACTORS, MAX_AVIDS, STALE_CODES, VIDEO_GROUPS, jobErrorLabel } from '../lib/fill-actor/labels'
import type {
  ActiveApplyRequest,
  AvidActors,
  ActorFeedStatus,
  ApplyJobEnvelope,
  ApplyResult,
  FillActorPlan,
  MoveCandidate,
  PlanEnvelope,
  PlanJob,
  VideoPlan,
} from '../types'

function matchesQuery(video: VideoPlan, query: string, actorNames: Record<string, string>) {
  if (!query) return true
  const needle = query.toLowerCase()
  return (
    video.video_id.toLowerCase().includes(needle) ||
    video.actor_ids.some((actorId) => actorId.toLowerCase().includes(needle)) ||
    video.actor_ids.some((actorId) => actorNames[actorId.toLowerCase()]?.toLowerCase().includes(needle)) ||
    video.move_candidates.some((candidate) => candidate.file_name.toLowerCase().includes(needle)) ||
    video.existing_files.some((file) => file.toLowerCase().includes(needle))
  )
}

export default function FillActorPage() {
  const [recoveredScanPlanId] = useState(getActivePlanId)
  const [recoveredApply] = useState(getActiveApplyRequest)
  const recoveredPlanId = recoveredScanPlanId ?? recoveredApply?.planId ?? null
  const [input, setInput] = useState('')
  // AVID is the everyday entry point — an operator has the code in hand far more often than
  // the actor ID — so the panel opens there and keeps actor IDs one click away.
  const [inputMode, setInputMode] = useState<'actor' | 'avid'>('avid')
  const [actorNames, setActorNames] = useState<Record<string, string>>(getActorNames)
  const [resolvingAvid, setResolvingAvid] = useState(false)
  const [avidProgress, setAvidProgress] = useState<{ completed: number; total: number } | null>(null)
  const [avidActorChoice, setAvidActorChoice] = useState<{ choices: AvidActors[]; settled: string[] } | null>(null)
  const [avidFailures, setAvidFailures] = useState<Array<{ avid: string; message: string }>>([])
  const [authRequired, setAuthRequired] = useState(false)
  const apiToken = useApiTokenValue()
  const { health, setHealth, requestApiToken } = useOutletContext<AppContext>()
  const [plan, setPlan] = useState<FillActorPlan | null>(null)
  const [feeds, setFeeds] = useState<ActorFeedStatus[]>([])
  const [planId, setPlanId] = useState<string | null>(recoveredPlanId)
  const [job, setJob] = useState<PlanJob | null>(() => recoveredScanPlanId
    ? { plan_id: recoveredScanPlanId, operation: 'create_plan', state: 'running' }
    : null)
  const [submitting, setSubmitting] = useState(false)
  const [subscriptionConflict, setSubscriptionConflict] = useState<{
    actorIds: string[]
    existingActors: Array<{ actorId: string; actorName: string | null }>
  } | null>(null)
  const [cancelling, setCancelling] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [pollWarning, setPollWarning] = useState<string | null>(null)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [activeApply, setActiveApply] = useState<ActiveApplyRequest | null>(recoveredApply)
  const [applyJob, setApplyJob] = useState<PlanJob | null>(() => recoveredApply ? applyPlaceholder(recoveredApply) : null)
  const [applyResult, setApplyResult] = useState<ApplyResult | null>(null)
  const [applyPollWarning, setApplyPollWarning] = useState<string | null>(null)
  const [applyPausedForAuth, setApplyPausedForAuth] = useState(false)
  const [applyRetryTick, setApplyRetryTick] = useState(0)
  const [applyPlanRetryTick, setApplyPlanRetryTick] = useState(0)
  const [needsFreshPlan, setNeedsFreshPlan] = useState(false)
  const [query, setQuery] = useState('')
  const [now, setNow] = useState(Date.now())
  const lastAutoSelectedPlanKey = useRef<string | null>(null)
  const lastScrolledPlanKey = useRef<string | null>(null)
  const resultsRef = useRef<HTMLElement>(null)
  const pollFailures = useRef(0)
  const requestGeneration = useRef(0)
  const applyPollFailures = useRef(0)
  const applyGeneration = useRef(0)
  const applyPlanGeneration = useRef(0)
  const activePollController = useRef<AbortController | null>(null)
  const activeCancelController = useRef<AbortController | null>(null)
  const activeApplyController = useRef<AbortController | null>(null)
  const activeApplyPlanController = useRef<AbortController | null>(null)
  const parsed = useMemo(() => parseActorIds(input), [input])
  const parsedAvids = useMemo(() => parseAvids(input), [input])
  const candidates = useMemo(() => candidateMap(plan), [plan])
  const submittedCount = useMemo(
    () =>
      (plan?.videos ?? []).filter(
        (video) => video.state === 'queued' || video.state === 'submitted' || video.state === 'submit_failed',
      ).length,
    [plan],
  )
  const selectedCandidates = [...selected].map((id) => candidates.get(id)).filter(Boolean) as MoveCandidate[]
  const selectableIds = useMemo(
    () =>
      (plan?.videos ?? []).flatMap((video) =>
        video.move_candidates
          .filter((candidate) => !candidate.destination_conflict)
          .map((candidate) => candidate.candidate_id),
      ),
    [plan],
  )
  const planExpired = Boolean(plan && new Date(plan.expires_at).getTime() <= now)
  const applyVerificationPending = Boolean(
    applyResult?.results.some((item) => item.error_code === 'cloud_move_status_unknown'),
  )
  const jobPending = isJobPending(job)
  const applyPending = Boolean(activeApply && isJobPending(applyJob))
  const jobCancelled = isJobCancelled(job)
  const feedsPending = feeds.some((feed) => feed.state === 'queued' || feed.state === 'warming')
  const envelopePending = jobPending || feedsPending
  // This page owns its readiness display: the top bar reports the app, not this feature.
  const fillActorHealth = health?.fill_actor
  const applyEnabled = fillActorHealth?.apply_ready === true
  const trimmedQuery = query.trim()
  const visibleVideos = useMemo(
    () => (plan?.videos ?? []).filter((video) => matchesQuery(video, trimmedQuery, actorNames)),
    [actorNames, plan, trimmedQuery],
  )
  const notConfigured = fillActorHealth?.configured === false
  const scanNotice = !fillActorHealth || fillActorHealth.scan_ready !== false
    ? null
    : notConfigured
      ? {
          title: '补全演员尚未配置',
          body: '请先在设置页填写主片库、附加片库和移入目录，扫描才能开始。其余功能不受影响。',
        }
      : fillActorHealth.roots === false
        ? {
            title: '媒体库根目录不可用',
            body: '已配置的根目录未挂载或缺少哨兵文件，扫描会被服务端拒绝。请检查容器挂载后重试。',
          }
        : {
            title: '扫描依赖尚未就绪',
            body: fillActorHealth.cloud === false
              ? 'CloudDrive 连接或授权尚未就绪，扫描会被服务端拒绝。'
              : '补全演员的依赖尚未就绪，请稍后重试。',
          }
  const applyNotice = !fillActorHealth || applyEnabled || scanNotice
    ? null
    : fillActorHealth.apply_enabled === false
      ? {
          title: '文件移动已暂停',
          body: '当前仅支持扫描、提交下载和订阅操作；确认移入功能已由管理员关闭。',
        }
      : fillActorHealth.legacy_journal === false
        ? {
            title: '文件移动等待管理员处理',
            body: '检测到旧版本未完成的移动记录。为避免误动派生映射文件，新的移入已被阻止。',
          }
        : {
            title: '文件移动尚未就绪',
            body: fillActorHealth.cloud === false
              ? 'CloudDrive 连接或授权尚未就绪，当前不会提交移动。'
              : '文件移动依赖尚未就绪，请稍后重试。',
          }

  useEffect(() => {
    setNow(Date.now())
    const timer = window.setInterval(() => setNow(Date.now()), jobPending || applyPending ? 1_000 : 30_000)
    return () => window.clearInterval(timer)
  }, [applyPending, jobPending])

  useEffect(() => {
    if (planId && envelopePending) setActivePlanId(planId)
    else if (job || plan) setActivePlanId(null)
  }, [envelopePending, job, plan, planId])

  // Names ride along with the recovered plan so a reload keeps naming the warming feeds.
  useEffect(() => {
    storeActorNames(actorNames)
  }, [actorNames])

  useEffect(() => {
    if (!plan) return
    const planKey = `${plan.plan_id}:${plan.revision}`
    if (lastAutoSelectedPlanKey.current === planKey) return
    lastAutoSelectedPlanKey.current = planKey
    const safeIds = plan.videos.flatMap((video) =>
      video.move_candidates.filter((candidate) => !candidate.destination_conflict).map((candidate) => candidate.candidate_id),
    )
    const safeIdSet = new Set(safeIds)
    const recoveredIds = activeApply?.planId === plan.plan_id
      ? activeApply.revision === plan.revision ? activeApply.candidateIds : []
      : applyResult?.plan_id === plan.plan_id
        ? applyResult.revision === plan.revision
          ? applyResult.results.map((result) => result.candidate_id)
          : []
        : null
    setSelected(new Set(recoveredIds === null ? safeIds : recoveredIds.filter((candidateId) => safeIdSet.has(candidateId))))
  }, [activeApply, applyResult, plan])

  // Bring freshly arrived results into view instead of leaving the operator parked on the form.
  useEffect(() => {
    if (!plan) return
    const planKey = `${plan.plan_id}:${plan.revision}`
    if (lastScrolledPlanKey.current === planKey) return
    lastScrolledPlanKey.current = planKey
    resultsRef.current?.scrollIntoView?.({ behavior: 'smooth', block: 'start' })
  }, [plan])

  useEffect(() => {
    if (!planId || (!isJobPending(job) && !feedsPending) || cancelling) return
    const generation = requestGeneration.current
    const controller = new AbortController()
    activePollController.current = controller
    const delay = Math.min(800 * 2 ** pollFailures.current, 10_000)
    const timer = window.setTimeout(() => {
      void getPlan(planId, controller.signal)
        .then((envelope) => {
          if (generation !== requestGeneration.current) return
          pollFailures.current = 0
          setPollWarning(null)
          consumeEnvelope(envelope, setPlan, setPlanId, setJob, setFeeds, setError)
        })
        .catch((pollError: unknown) => {
          if (generation !== requestGeneration.current) return
          if (pollError instanceof DOMException && pollError.name === 'AbortError') return
          if (pollError instanceof ApiError && pollError.code === 'unauthorized') {
            setAuthRequired(true)
            setError(errorMessage(pollError))
          } else if (pollError instanceof ApiError && STALE_CODES.has(pollError.code)) {
            setNeedsFreshPlan(true)
            setJob((current) => current ? { ...current, state: 'failed', error_code: pollError.code } : current)
          } else {
            pollFailures.current += 1
            setPollWarning('暂时无法刷新任务状态，将自动重试。')
            setJob((current) => current ? { ...current } : current)
          }
        })
    }, delay)
    return () => {
      window.clearTimeout(timer)
      controller.abort()
      if (activePollController.current === controller) activePollController.current = null
    }
  }, [cancelling, feedsPending, job, planId])

  useEffect(() => {
    if (!activeApply || applyPausedForAuth) return
    const generation = applyGeneration.current
    const controller = new AbortController()
    activeApplyController.current = controller
    const delay = activeApply.jobId || applyPollFailures.current > 0
      ? Math.min(800 * 2 ** applyPollFailures.current, 10_000)
      : 0
    const timer = window.setTimeout(() => {
      if (!activeApply.jobId) {
        setActiveApplyRequest({
          ...activeApply,
          jobId: activeApply.requestId,
          retrySubmitIfMissing: true,
        })
      }
      const pendingRequest = activeApply.jobId
        ? getApplyJob(activeApply.jobId, controller.signal)
        : startApplyJob(
            activeApply.planId,
            activeApply.revision,
            activeApply.candidateIds,
            activeApply.requestId,
            controller.signal,
          )
      void pendingRequest
        .then((envelope: ApplyJobEnvelope) => {
          if (generation !== applyGeneration.current) return
          const responseJobId = envelope.job.job_id ?? envelope.job.id
          if (!responseJobId || (envelope.job.operation && envelope.job.operation !== 'apply')) {
            throw new ApiError(0, 'invalid_apply_job_response', '移动任务响应无效，请稍后重试。')
          }
          assertActiveApplyEnvelope(envelope, activeApply)
          applyPollFailures.current = 0
          setApplyPollWarning(null)
          setApplyPausedForAuth(false)
          setError(null)

          if (envelope.result) {
            const finalJob = terminalApplyJob(envelope.job, activeApply.candidateIds.length)
            setApplyJob(finalJob)
            setApplyResult(envelope.result)
            if (envelope.result.results.some((item) => item.state === 'stale')) setNeedsFreshPlan(true)
            setActiveApplyRequest(null)
            setActiveApply(null)
            return
          }

          const state = jobState(envelope.job)
          setApplyJob(envelope.job)
          if (!isJobPending(envelope.job)) {
            if (envelope.job.error_code && STALE_CODES.has(envelope.job.error_code)) setNeedsFreshPlan(true)
            if (envelope.job.error_code === 'move_disabled') {
              setHealth((current) => current
                ? { ...current, fill_actor: { ...current.fill_actor, apply_enabled: false, apply_ready: false } }
                : current)
            }
            setError(envelope.job.error_code
              ? `移动任务失败：${jobErrorLabel(envelope.job.error_code)}`
              : state === 'completed'
                ? '移动任务已完成，但结果响应无效。'
                : '移动任务未能完成。')
            setActiveApplyRequest(null)
            setActiveApply(null)
            return
          }

          if (activeApply.jobId !== responseJobId || activeApply.retrySubmitIfMissing) {
            const accepted = { ...activeApply, jobId: responseJobId, retrySubmitIfMissing: false }
            setActiveApplyRequest(accepted)
            setActiveApply(accepted)
          } else {
            setApplyRetryTick((value) => value + 1)
          }
        })
        .catch((pollError: unknown) => {
          if (generation !== applyGeneration.current) return
          if (pollError instanceof DOMException && pollError.name === 'AbortError') return
          if (pollError instanceof ApiError && pollError.code === 'unauthorized') {
            setAuthRequired(true)
            setApplyPausedForAuth(true)
            setApplyPollWarning('移动任务仍在保留中；重新登录后会继续恢复。')
            return
          }
          if (
            activeApply.jobId &&
            activeApply.retrySubmitIfMissing &&
            pollError instanceof ApiError &&
            pollError.status === 404
          ) {
            const retryRequest = { ...activeApply }
            delete retryRequest.jobId
            delete retryRequest.retrySubmitIfMissing
            setActiveApplyRequest(retryRequest)
            setActiveApply(retryRequest)
            return
          }
          if (pollError instanceof ApiError && STALE_CODES.has(pollError.code)) setNeedsFreshPlan(true)
          if (pollError instanceof ApiError && pollError.code === 'move_disabled') {
            setHealth((current) => current
                ? { ...current, fill_actor: { ...current.fill_actor, apply_enabled: false, apply_ready: false } }
                : current)
          }
          const retryable = !(pollError instanceof ApiError) || pollError.status === 0 || pollError.status >= 500
          if (retryable) {
            applyPollFailures.current += 1
            setApplyPollWarning('暂时无法刷新移动任务状态，将自动重试。')
            if (!activeApply.jobId) {
              const recoveryRequest = {
                ...activeApply,
                jobId: activeApply.requestId,
                retrySubmitIfMissing: true,
              }
              setActiveApplyRequest(recoveryRequest)
              setActiveApply(recoveryRequest)
            } else {
              setApplyRetryTick((value) => value + 1)
            }
            return
          }
          setError(errorMessage(pollError))
          setActiveApplyRequest(null)
          setActiveApply(null)
        })
    }, delay)
    return () => {
      window.clearTimeout(timer)
      controller.abort()
      if (activeApplyController.current === controller) activeApplyController.current = null
    }
  }, [activeApply, applyPausedForAuth, applyRetryTick, setHealth])

  useEffect(() => {
    const targetPlanId = activeApply?.planId ?? applyResult?.plan_id ?? (applyResult ? applyJob?.plan_id : null)
    const targetRevision = activeApply?.revision ?? applyResult?.revision ?? null
    if (
      !targetPlanId ||
      (plan?.plan_id === targetPlanId && (targetRevision === null || plan.revision === targetRevision))
    ) return
    const generation = applyPlanGeneration.current + 1
    applyPlanGeneration.current = generation
    const controller = new AbortController()
    activeApplyPlanController.current = controller
    const timer = window.setTimeout(() => {
      void getPlan(targetPlanId, controller.signal)
        .then((envelope) => {
          if (generation !== applyPlanGeneration.current) return
          if (
            !envelope.plan ||
            envelope.plan.plan_id !== targetPlanId ||
            (targetRevision !== null && envelope.plan.revision !== targetRevision)
          ) {
            throw new ApiError(0, 'invalid_plan_response', '无法恢复移动任务对应的扫描结果。')
          }
          setPlan(envelope.plan)
          setPlanId(envelope.planId ?? targetPlanId)
          setFeeds(envelope.feeds)
          setApplyPlanRetryTick(0)
        })
        .catch((recoveryError: unknown) => {
          if (generation !== applyPlanGeneration.current) return
          if (recoveryError instanceof DOMException && recoveryError.name === 'AbortError') return
          const retryable = recoveryError instanceof ApiError && (
            recoveryError.code === 'network_error' || recoveryError.status >= 500
          )
          if (retryable) {
            setApplyPlanRetryTick((value) => value + 1)
            return
          }
          if (recoveryError instanceof ApiError && recoveryError.code === 'unauthorized') setAuthRequired(true)
          setError(errorMessage(recoveryError))
        })
    }, applyPlanRetryTick ? Math.min(800 * 2 ** applyPlanRetryTick, 10_000) : 0)
    return () => {
      window.clearTimeout(timer)
      controller.abort()
      if (activeApplyPlanController.current === controller) activeApplyPlanController.current = null
      if (applyPlanGeneration.current === generation) applyPlanGeneration.current += 1
    }
  }, [activeApply, applyJob?.plan_id, applyPlanRetryTick, applyResult, plan])

  useEffect(() => () => {
    requestGeneration.current += 1
    applyGeneration.current += 1
    applyPlanGeneration.current += 1
    activePollController.current?.abort()
    activeCancelController.current?.abort()
    activeApplyController.current?.abort()
    activeApplyPlanController.current?.abort()
  }, [])

  /** Learns actor names wherever a lookup happens to return them; unnamed actors keep their ID. */
  function rememberActorNames(entries: Array<{ actorId: string; actorName: string | null }>) {
    const named = entries.flatMap((entry) => {
      const actorId = entry.actorId.toLowerCase()
      const name = entry.actorName?.trim()
      // JavBus falls back to the ID when a page carries no name; storing that would only
      // print the ID twice, once as the headline and once in the line below it.
      return name && name.toLowerCase() !== actorId ? [[actorId, name] as const] : []
    })
    if (!named.length) return
    setActorNames((current) => ({ ...current, ...Object.fromEntries(named) }))
  }

  async function startScan(actorIds = parsed.actorIds, continueIfSubscribed = false) {
    if (
      applyPending
      || !actorIds.length
      || actorIds.length > MAX_ACTORS
    ) return
    setSubmitting(true)
    setCancelling(false)
    setError(null)
    setPollWarning(null)
    try {
      const envelope = await createPlan(actorIds, continueIfSubscribed)
      setSubscriptionConflict(null)
      setActivePlanId(null)
      setPlan(null)
      setFeeds([])
      setPlanId(null)
      setJob(null)
      setActiveApplyRequest(null)
      setActiveApply(null)
      setApplyJob(null)
      setApplyResult(null)
      setApplyPollWarning(null)
      setApplyPausedForAuth(false)
      setSelected(new Set())
      setNeedsFreshPlan(false)
      setQuery('')
      lastAutoSelectedPlanKey.current = null
      lastScrolledPlanKey.current = null
      pollFailures.current = 0
      setApplyPlanRetryTick(0)
      requestGeneration.current += 1
      applyGeneration.current += 1
      applyPlanGeneration.current += 1
      activePollController.current?.abort()
      activeCancelController.current?.abort()
      activeApplyController.current?.abort()
      activeApplyPlanController.current?.abort()
      consumeEnvelope(envelope, setPlan, setPlanId, setJob, setFeeds, setError)
    } catch (scanError) {
      if (scanError instanceof ApiError && scanError.code === 'actors_already_subscribed') {
        const requested = new Map(actorIds.map((actorId) => [actorId.toLowerCase(), actorId]))
        const detailedActors = Array.isArray(scanError.details.actors)
          ? scanError.details.actors.flatMap((actor) => {
              if (typeof actor !== 'object' || actor === null || Array.isArray(actor)) return []
              const rawActorId = (actor as Record<string, unknown>).actor_id
              if (typeof rawActorId !== 'string') return []
              const requestedActorId = requested.get(rawActorId.toLowerCase())
              if (!requestedActorId) return []
              const rawActorName = (actor as Record<string, unknown>).actor_name
              const actorName = typeof rawActorName === 'string' ? rawActorName.trim() || null : null
              return [{ actorId: requestedActorId, actorName }]
            })
          : []
        const fallbackActors = Array.isArray(scanError.details.actor_ids)
          ? scanError.details.actor_ids.flatMap((actorId) => {
              if (typeof actorId !== 'string') return []
              const requestedActorId = requested.get(actorId.toLowerCase())
              return requestedActorId ? [{ actorId: requestedActorId, actorName: null }] : []
            })
          : []
        const existingActors = detailedActors.length ? detailedActors : fallbackActors
        const uniqueActors = [...new Map(existingActors.map((actor) => [actor.actorId.toLowerCase(), actor])).values()]
        if (uniqueActors.length) {
          rememberActorNames(uniqueActors)
          setSubscriptionConflict({ actorIds: [...actorIds], existingActors: uniqueActors })
          return
        }
      }
      if (scanError instanceof ApiError && scanError.code === 'unauthorized') {
        setAuthRequired(true)
        requestApiToken()
      }
      setError(errorMessage(scanError))
    } finally {
      setSubmitting(false)
    }
  }

  /** Scans the actors an AVID batch resolved to, once every ambiguous AVID has been decided. */
  async function scanResolvedActors(actorIds: string[]) {
    const unique = [...new Map(actorIds.map((actorId) => [actorId.toLowerCase(), actorId])).values()]
    if (!unique.length) return
    if (unique.length > MAX_ACTORS) {
      // startScan refuses this silently, which from the panel looks like a dead button.
      setError(`这些番号共有 ${unique.length} 位演员，超过单次 ${MAX_ACTORS} 位的上限，请减少番号数量。`)
      return
    }
    await startScan(unique)
  }

  async function submitInput() {
    if (inputMode === 'actor') {
      await startScan(parsed.actorIds)
      return
    }
    const avids = parsedAvids.avids
    if (!avids.length || parsedAvids.invalid.length || avids.length > MAX_AVIDS) return
    if (resolvingAvid || submitting || jobPending || applyPending) return
    setResolvingAvid(true)
    setAvidProgress({ completed: 0, total: avids.length })
    setError(null)
    setAvidFailures([])
    const resolved: AvidActors[] = []
    const failures: Array<{ avid: string; message: string }> = []
    try {
      // One at a time: each lookup scrapes a JavBus page, and a batch should not fan out onto it.
      for (const avid of avids) {
        try {
          const result = await resolveAvidActors(avid)
          rememberActorNames(result.actors.map((actor) => ({ actorId: actor.actor_id, actorName: actor.name })))
          resolved.push(result)
        } catch (lookupError) {
          if (lookupError instanceof ApiError && lookupError.code === 'unauthorized') {
            setAuthRequired(true)
            requestApiToken()
            setError(errorMessage(lookupError))
            return
          }
          failures.push({ avid, message: errorMessage(lookupError) })
        } finally {
          setAvidProgress((current) => current && { ...current, completed: current.completed + 1 })
        }
      }
      // A bad AVID in the middle of a batch costs that one AVID, not the whole scan.
      setAvidFailures(failures)
      const settled = resolved.filter((result) => result.actors.length === 1).map((result) => result.actors[0].actor_id)
      const ambiguous = resolved.filter((result) => result.actors.length > 1)
      if (ambiguous.length) setAvidActorChoice({ choices: ambiguous, settled })
      else await scanResolvedActors(settled)
    } finally {
      setResolvingAvid(false)
      setAvidProgress(null)
    }
  }

  async function cancelScan() {
    const targetPlanId = planId
    if (!targetPlanId || !isJobPending(job) || cancelling) return
    const generation = requestGeneration.current + 1
    requestGeneration.current = generation
    activePollController.current?.abort()
    activeCancelController.current?.abort()
    const controller = new AbortController()
    activeCancelController.current = controller
    setCancelling(true)
    setError(null)
    setPollWarning(null)

    try {
      let envelope: PlanEnvelope
      try {
        envelope = await cancelPlan(targetPlanId, controller.signal)
      } catch (cancelError) {
        if (
          cancelError instanceof ApiError &&
          cancelError.status === 409 &&
          cancelError.code === 'plan_not_cancellable'
        ) {
          envelope = await getPlan(targetPlanId, controller.signal)
        } else {
          throw cancelError
        }
      }
      if (generation !== requestGeneration.current) return
      pollFailures.current = 0
      consumeEnvelope(envelope, setPlan, setPlanId, setJob, setFeeds, setError)
    } catch (cancelError) {
      if (generation !== requestGeneration.current) return
      if (cancelError instanceof DOMException && cancelError.name === 'AbortError') return
      if (cancelError instanceof ApiError && cancelError.code === 'unauthorized') {
        setAuthRequired(true)
        requestApiToken()
      }
      if (cancelError instanceof ApiError && STALE_CODES.has(cancelError.code)) {
        setNeedsFreshPlan(true)
        setJob((current) => current ? { ...current, state: 'failed', error_code: cancelError.code } : current)
      } else {
        setJob((current) => current ? { ...current } : current)
      }
      setError(errorMessage(cancelError))
    } finally {
      if (activeCancelController.current === controller) activeCancelController.current = null
      if (generation === requestGeneration.current) setCancelling(false)
    }
  }

  function toggleCandidate(candidate: MoveCandidate) {
    if (applyPending || candidate.destination_conflict) return
    setSelected((previous) => {
      const next = new Set(previous)
      if (next.has(candidate.candidate_id)) next.delete(candidate.candidate_id)
      else next.add(candidate.candidate_id)
      return next
    })
  }

  async function confirmApply() {
    if (!applyEnabled || applyPending || !plan || !selected.size) return
    const request: ActiveApplyRequest = {
      planId: plan.plan_id,
      revision: plan.revision,
      candidateIds: [...selected],
      requestId: createApplyRequestId(),
    }
    setConfirmOpen(false)
    applyGeneration.current += 1
    activeApplyController.current?.abort()
    setActiveApplyRequest(request)
    setActiveApply(request)
    setApplyJob(applyPlaceholder(request))
    setApplyResult(null)
    setApplyPollWarning(null)
    setApplyPausedForAuth(false)
    setApplyPlanRetryTick(0)
    applyPollFailures.current = 0
    setError(null)
  }

  // A fresh token — first one or a replacement after a rotation — resumes work paused by a 401.
  useEffect(() => {
    if (!apiToken) return
    setAuthRequired(false)
    setApplyPausedForAuth(false)
    setApplyRetryTick((value) => value + 1)
    setError(null)
  }, [apiToken])

  const scanLocked = resolvingAvid || submitting || jobPending || applyPending || subscriptionConflict !== null || avidActorChoice !== null

  return (
    <>
      <main id="top">
        <ScanPanel
          input={input}
          onInputChange={setInput}
          inputMode={inputMode}
          onInputModeChange={(mode) => {
            if (mode === inputMode) return
            // One AVID and a list of actor IDs never parse as each other, so carrying the
            // old text across the switch only produces a validation error to clear by hand.
            setInput('')
            setInputMode(mode)
          }}
          parsed={parsed}
          parsedAvids={parsedAvids}
          locked={scanLocked}
          submitting={submitting}
          resolvingAvid={resolvingAvid}
          avidProgress={avidProgress}
          jobPending={jobPending}
          applyPending={applyPending}
          onScan={() => void submitInput()}
          authRequired={authRequired}
          onRequestLogin={requestApiToken}
        />

        {scanNotice && (
          <Notice
            tone="warning"
            title={scanNotice.title}
            body={scanNotice.body}
            action={notConfigured ? <Link className="text-button" to="/settings">前往设置</Link> : undefined}
          />
        )}

        {(submitting || jobPending) && (
          <ProgressPanel
            kind="scan"
            job={job}
            planId={planId}
            now={now}
            pollWarning={pollWarning}
            submitting={submitting}
            pending={submitting || jobPending}
            cancelling={cancelling}
            onCancel={() => void cancelScan()}
          />
        )}

        {applyJob && (applyPending || applyResult) && (
          <ProgressPanel
            kind="apply"
            job={applyJob}
            now={now}
            pollWarning={applyPollWarning}
            submitting={Boolean(applyPending && activeApply && !activeApply.jobId)}
            pending={applyPending}
          />
        )}

        {applyResult && <ApplySummary result={applyResult} />}

        {avidFailures.length > 0 && (
          <Notice
            tone="warning"
            title={`${avidFailures.length} 个番号未能获取演员`}
            body={avidFailures.map((failure) => `${failure.avid}：${failure.message}`).join('\n')}
          />
        )}
        {error && <Notice tone="error" title="操作未完成" body={error} />}
        {jobCancelled && (
          <Notice tone="neutral" title="扫描已取消" body="任务已停止，未生成可应用的扫描结果。" />
        )}
        {(needsFreshPlan || planExpired) && (
          <Notice
            tone="warning"
            title="扫描结果已失效"
            body="文件状态或计划版本已经变化。请重新扫描后再选择文件，避免使用过期结果。"
            action={<button className="text-button" type="button" disabled={applyPending} onClick={() => void startScan(plan?.actors.map((actor) => actor.actor_id) ?? parsed.actorIds)}>重新扫描 <ArrowIcon /></button>}
          />
        )}

        {plan && (
          <>
            <PlanSummary plan={plan} />
            <ActorFailures plan={plan} />

            {applyNotice && <Notice tone="warning" title={applyNotice.title} body={applyNotice.body} />}

            <section className="results-section" aria-labelledby="results-title" ref={resultsRef}>
              <div className="results-bar">
                <h2 id="results-title">扫描结果</h2>
                <span className="result-total">
                  {trimmedQuery ? `${visibleVideos.length} / ${plan.videos.length}` : `共 ${plan.videos.length}`} 部作品
                </span>
                <div className="results-search">
                  <SearchIcon />
                  <input
                    type="search"
                    aria-label="筛选扫描结果"
                    placeholder="筛选番号、演员或文件名"
                    value={query}
                    onChange={(event) => setQuery(event.target.value)}
                  />
                </div>
              </div>
              {submittedCount > 0 && (
                <p className="submitted-hint">
                  缺失作品已加入后台下载队列，磁力解析与提交进度见<Link to="/dashboard">监控面板</Link>。
                </p>
              )}

              <div className="group-stack">
                {VIDEO_GROUPS.map((group) => {
                  const videos = visibleVideos.filter((video) => video.state === group.state)
                  if (!videos.length) return null
                  return (
                    <VideoGroup
                      key={group.state}
                      group={group}
                      videos={videos}
                      selected={selected}
                      toggleCandidate={toggleCandidate}
                      applyResult={applyResult}
                      selectionLocked={applyPending}
                      filtered={Boolean(trimmedQuery)}
                      actorNames={actorNames}
                    />
                  )
                })}
                {trimmedQuery && !visibleVideos.length && (
                  <p className="results-empty">没有匹配「{trimmedQuery}」的作品。</p>
                )}
              </div>
            </section>

            <div className="action-dock">
                <div className="dock-count">
                  <strong>{selected.size}</strong>
                  <span>
                    / {selectableIds.length} 个可移入文件
                  </span>
                </div>
                <div className="dock-actions">
                  <button
                    className="text-button"
                    type="button"
                    disabled={applyPending || selected.size === selectableIds.length}
                    onClick={() => setSelected(new Set(selectableIds))}
                  >
                    全选
                  </button>
                  <button
                    className="text-button"
                    type="button"
                    disabled={applyPending || !selected.size}
                    onClick={() => setSelected(new Set())}
                  >
                    清空
                  </button>
                  <button
                    className="button primary"
                    type="button"
                    disabled={
                      !applyEnabled
                      || !selected.size
                      || applyPending
                      || needsFreshPlan
                      || planExpired
                      || applyVerificationPending
                    }
                    onClick={() => applyEnabled && setConfirmOpen(true)}
                  >
                    {applyPending ? <Spinner /> : <MoveIcon />}
                    {applyPending ? '正在移入' : applyResult ? '再次应用选择' : '确认并移入'}
                  </button>
                </div>
            </div>
          </>
        )}

        {feeds.length > 0 && !jobCancelled && <ActorFeeds feeds={feeds} actorNames={actorNames} />}
      </main>

      {confirmOpen && applyEnabled && (
        <ConfirmDialog
          candidates={selectedCandidates}
          onCancel={() => setConfirmOpen(false)}
          onConfirm={() => void confirmApply()}
        />
      )}
      {subscriptionConflict && (
        <ExistingSubscriptionsDialog
          actors={subscriptionConflict.existingActors}
          onCancel={() => setSubscriptionConflict(null)}
          onConfirm={() => {
            const actorIds = subscriptionConflict.actorIds
            setSubscriptionConflict(null)
            void startScan(actorIds, true)
          }}
        />
      )}

      {avidActorChoice && (
        <AvidActorChoiceDialog
          choices={avidActorChoice.choices}
          onCancel={() => setAvidActorChoice(null)}
          onConfirm={(actorIds) => {
            const settled = avidActorChoice.settled
            setAvidActorChoice(null)
            void scanResolvedActors([...settled, ...actorIds])
          }}
        />
      )}
    </>
  )
}

function consumeEnvelope(
  envelope: PlanEnvelope,
  setPlan: (plan: FillActorPlan | null) => void,
  setPlanId: (id: string | null) => void,
  setJob: (job: PlanJob | null) => void,
  setFeeds: (feeds: ActorFeedStatus[]) => void,
  setError: (error: string | null) => void,
) {
  setPlanId(envelope.planId)
  setJob(envelope.job)
  setFeeds(envelope.feeds)
  if (envelope.plan) setPlan(envelope.plan)
  const state = jobState(envelope.job)
  if (isJobCancelled(envelope.job)) {
    setError(null)
    return
  }
  if ((state === 'failed' || state === 'partial_failed') && !envelope.plan) {
    setError(envelope.job?.error_code ? `扫描任务失败：${jobErrorLabel(envelope.job.error_code)}` : '扫描任务未能完成。')
  }
}
