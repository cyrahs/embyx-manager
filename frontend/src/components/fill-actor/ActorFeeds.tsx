import { formatFeedUpdatedAt, safeFreshRssUrl } from '../../lib/fill-actor/format'
import { FEED_ERROR_LABELS, FEED_STATE_LABELS } from '../../lib/fill-actor/labels'
import type { ActorFeedStatus } from '../../types'
import { ExternalIcon, FeedIcon, FeedStateIcon } from '../Icons'

export function ActorFeeds({
  feeds,
  actorNames,
}: {
  feeds: ActorFeedStatus[]
  /** Actor display names keyed by lower-cased actor ID; missing ones fall back to the ID. */
  actorNames: Record<string, string>
}) {
  return (
    <section className="feed-panel" aria-labelledby="feed-title">
      <div className="feed-panel-heading">
        <span className="feed-panel-icon">
          <FeedIcon />
        </span>
        <div>
          <h2 id="feed-title">RSSHub 缓存</h2>
          <p>演员订阅源准备状态</p>
        </div>
        <span className="feed-panel-count">{feeds.length} 位演员</span>
      </div>
      <ul className="feed-list">
        {feeds.map((feed) => {
          const freshrssAddUrl = feed.state === 'ready' ? safeFreshRssUrl(feed.freshrss_add_url) : null
          const freshrssUrl = feed.state === 'ready' ? safeFreshRssUrl(feed.freshrss_url) : null
          const detail =
            feed.state === 'warming'
              ? 'RSSHub 正在预热缓存，页面会自动更新。'
              : feed.state === 'failed'
                ? feed.error_code
                  ? `错误：${FEED_ERROR_LABELS[feed.error_code] ?? feed.error_code}`
                  : '缓存预热未能完成。'
                : null
          const actorName = actorNames[feed.actor_id.toLowerCase()]
          const meta = [
            // The ID moves to the second line once a name headlines the row, so it stays readable.
            ...(actorName ? [feed.actor_id] : []),
            `已尝试 ${feed.attempts} 次`,
            formatFeedUpdatedAt(feed.updated_at),
          ]
          return (
            <li className={`feed-row feed-${feed.state}`} key={feed.actor_id}>
              <span className="feed-actor">
                <strong>{actorName ?? feed.actor_id}</strong>
                <small>{meta.join(' · ')}</small>
              </span>
              <span className="feed-state" role="status" aria-live="polite">
                <FeedStateIcon state={feed.state} />
                {FEED_STATE_LABELS[feed.state]}
              </span>
              <span className="feed-detail">{detail}</span>
              <span className="feed-actions">
                {freshrssAddUrl && (
                  <a
                    className="button secondary freshrss-button"
                    href={freshrssAddUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <ExternalIcon />
                    一键添加到 FreshRSS
                  </a>
                )}
                {freshrssUrl && (
                  <a
                    className="button secondary freshrss-button"
                    href={freshrssUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <ExternalIcon />
                    打开 FreshRSS
                  </a>
                )}
              </span>
            </li>
          )
        })}
      </ul>
    </section>
  )
}
