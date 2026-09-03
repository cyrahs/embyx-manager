/** The rss pipeline's subscriptions, split into the two kinds that are configured differently.
 *
 * Actors are AVBase talents resolved from a name or link; charts are plain
 * feed URLs. Both file into one of the RSS categories, which decide the
 * offline directory, so the categories themselves stay on the settings page.
 */

import { useState } from 'react'
import { useOutletContext } from 'react-router-dom'

import type { AppContext } from '../App'
import { Notice } from '../components/Feedback'
import { Spinner } from '../components/Icons'
import { RssSubscriptionsPanel } from '../components/subscriptions/RssSubscriptionsPanel'
import { TalentSubscriptionsPanel } from '../components/subscriptions/TalentSubscriptionsPanel'
import { useSubscriptions } from '../components/subscriptions/useSubscriptions'
import { useApiTokenConfigured } from '../lib/apiToken'

type Tab = 'talent' | 'rss'

const TAB_STORAGE_KEY = 'embyx-manager-subscriptions-tab'

function readTab(): Tab {
  try {
    return window.localStorage.getItem(TAB_STORAGE_KEY) === 'rss' ? 'rss' : 'talent'
  } catch {
    return 'talent'
  }
}

export default function SubscriptionsPage() {
  const { requestApiToken } = useOutletContext<AppContext>()
  const tokenConfigured = useApiTokenConfigured()
  const subscriptions = useSubscriptions(requestApiToken)
  const [tab, setTab] = useState<Tab>(readTab)

  const talents = subscriptions.items.filter((item) => item.kind === 'avbase_talent')
  const feeds = subscriptions.items.filter((item) => item.kind === 'rss')

  const select = (next: Tab) => {
    setTab(next)
    try {
      window.localStorage.setItem(TAB_STORAGE_KEY, next)
    } catch {
      // Remembering the tab is a convenience only.
    }
  }

  return (
    <main>
      {!tokenConfigured && (
        <Notice
          tone="warning"
          title="修改订阅需要登录"
          body="查看订阅无需认证；添加、停用、改地址或删除前，请先用部署时设置的 API Token 登录。"
        />
      )}
      <section className="panel settings-panel" aria-labelledby="subscriptions-title">
        <div className="panel-heading">
          <h2 id="subscriptions-title">订阅</h2>
        </div>
        <p className="settings-desc">
          rss 流水线按「设置 → RSS 摄取」里的间隔轮询这些订阅，新番号交给下载追踪。每条订阅归属一个分类，分类决定下载落到哪个离线目录。
        </p>
        {subscriptions.error && <Notice tone="error" title="订阅操作失败" body={subscriptions.error} />}
        {subscriptions.loaded && subscriptions.categories.length === 0 && (
          <p className="settings-hint">先在「设置 → RSS 摄取」里配置分类，订阅才有可归属的离线目录。</p>
        )}
        <div className="subtabs" role="tablist" aria-label="订阅类型">
          <button
            type="button"
            role="tab"
            id="subscriptions-tab-talent"
            aria-selected={tab === 'talent'}
            aria-controls="subscriptions-panel-talent"
            className="subtab"
            onClick={() => select('talent')}
          >
            演员<span className="subtab-count">{talents.length}</span>
          </button>
          <button
            type="button"
            role="tab"
            id="subscriptions-tab-rss"
            aria-selected={tab === 'rss'}
            aria-controls="subscriptions-panel-rss"
            className="subtab"
            onClick={() => select('rss')}
          >
            榜单<span className="subtab-count">{feeds.length}</span>
          </button>
        </div>
        {!subscriptions.loaded ? (
          <p className="dashboard-loading">
            <Spinner /> 正在加载…
          </p>
        ) : tab === 'talent' ? (
          <div role="tabpanel" id="subscriptions-panel-talent" aria-labelledby="subscriptions-tab-talent">
            <TalentSubscriptionsPanel
              items={talents}
              categories={subscriptions.categories}
              sections={subscriptions.sections}
              busy={subscriptions.busy}
              onAdd={subscriptions.addTalent}
              onToggle={subscriptions.toggle}
              onRemove={subscriptions.remove}
            />
          </div>
        ) : (
          <div role="tabpanel" id="subscriptions-panel-rss" aria-labelledby="subscriptions-tab-rss">
            <RssSubscriptionsPanel
              items={feeds}
              categories={subscriptions.categories}
              sections={subscriptions.sections}
              busy={subscriptions.busy}
              onAdd={subscriptions.addFeed}
              onToggle={subscriptions.toggle}
              onSaveUrl={subscriptions.saveUrl}
              onRemove={subscriptions.remove}
            />
          </div>
        )}
      </section>
    </main>
  )
}
