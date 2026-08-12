import { render, screen, within } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { ActorFailures, PlanSummary } from './Results'
import type { ActorPlan, FillActorPlan } from '../types'

function makePlan(actors: ActorPlan[]): FillActorPlan {
  return {
    plan_id: 'plan-1',
    revision: 'revision-1',
    created_at: '2026-07-13T10:00:00Z',
    expires_at: '2099-07-13T11:00:00Z',
    actors,
    videos: [],
  }
}

describe('ActorFailures', () => {
  it('renders nothing when every actor succeeded', () => {
    const plan = makePlan([{ actor_id: 'x6h', scraped_count: 3, video_ids: ['ABC-001'], error_code: null }])
    const { container } = render(<ActorFailures plan={plan} />)
    expect(container).toBeEmptyDOMElement()
  })

  it('lists each failed actor with a readable label, its code and message', () => {
    const plan = makePlan([
      { actor_id: 'x6h', scraped_count: 3, video_ids: ['ABC-001'], error_code: null },
      {
        actor_id: 'ngv',
        scraped_count: 0,
        video_ids: [],
        error_code: 'actor_catalog_error',
        error_message: 'TimeoutError: Upstream request timed out',
      },
    ])

    render(<ActorFailures plan={plan} />)

    const panel = screen.getByRole('alert', { name: '失败演员' })
    expect(within(panel).getByText('失败演员（1）')).toBeInTheDocument()
    expect(within(panel).getByText('ngv')).toBeInTheDocument()
    expect(within(panel).getByText('演员作品目录抓取失败（actor_catalog_error）')).toBeInTheDocument()
    expect(within(panel).getByText('TimeoutError: Upstream request timed out')).toBeInTheDocument()
    expect(within(panel).queryByText('x6h')).not.toBeInTheDocument()
  })

  it('falls back to the raw code for unknown errors and omits a missing message', () => {
    const plan = makePlan([{ actor_id: 'ngv', scraped_count: 0, video_ids: [], error_code: 'brand_new_error' }])

    render(<ActorFailures plan={plan} />)

    expect(screen.getByText('brand_new_error')).toBeInTheDocument()
  })
})

describe('PlanSummary', () => {
  it('keeps the summary quiet when nothing failed', () => {
    const plan = makePlan([{ actor_id: 'x6h', scraped_count: 3, video_ids: ['ABC-001'], error_code: null }])
    render(<PlanSummary plan={plan} />)
    expect(screen.queryByText(/个失败/)).not.toBeInTheDocument()
  })

  it('counts failed actors', () => {
    const plan = makePlan([
      { actor_id: 'x6h', scraped_count: 3, video_ids: ['ABC-001'], error_code: null },
      { actor_id: 'ngv', scraped_count: 0, video_ids: [], error_code: 'actor_catalog_error' },
    ])
    render(<PlanSummary plan={plan} />)
    expect(screen.getByText('1 个失败')).toBeInTheDocument()
  })
})
