import { useCallback, useEffect, useState } from 'react'
import { BrowserRouter, NavLink, Outlet, Route, Routes } from 'react-router-dom'

import { getHealth, type HealthStatus } from './api'
import { ApiTokenButton, ApiTokenDialog } from './components/ApiTokenDialog'
import DashboardPage from './pages/DashboardPage'
import FillActorPage from './pages/FillActorPage'
import SettingsPage from './pages/SettingsPage'

export interface AppContext {
  health: HealthStatus | null
  healthFailed: boolean
  setHealth: React.Dispatch<React.SetStateAction<HealthStatus | null>>
  /** Opens the shared API token dialog; pages call it when a request comes back 401. */
  requestApiToken: () => void
}

/** @deprecated Kept for existing imports; the context now carries more than health. */
export type HealthContext = AppContext

function Layout() {
  const [health, setHealth] = useState<HealthStatus | null>(null)
  const [healthFailed, setHealthFailed] = useState(false)
  const [tokenDialogOpen, setTokenDialogOpen] = useState(false)
  const requestApiToken = useCallback(() => setTokenDialogOpen(true), [])

  useEffect(() => {
    let mounted = true
    const refresh = () => {
      void getHealth()
        .then((value) => {
          if (!mounted) return
          setHealth(value)
          setHealthFailed(false)
        })
        .catch(() => {
          if (!mounted) return
          setHealth(null)
          setHealthFailed(true)
        })
    }
    refresh()
    const timer = window.setInterval(refresh, 30_000)
    return () => {
      mounted = false
      window.clearInterval(timer)
    }
  }, [])

  const healthReady = Boolean(health && ['ok', 'healthy', 'ready'].includes(health.status.toLowerCase()))

  return (
    <div className="app-shell">
      <header className="topbar">
        <NavLink className="brand" to="/" aria-label="Embyx 首页">
          <span className="brand-mark" aria-hidden="true">E</span>
          <span>embyx</span>
        </NavLink>
        <nav className="topbar-nav" aria-label="页面导航">
          <NavLink to="/" end className={({ isActive }) => `nav-link${isActive ? ' active' : ''}`}>
            补全演员
          </NavLink>
          <NavLink to="/dashboard" className={({ isActive }) => `nav-link${isActive ? ' active' : ''}`}>
            监控看板
          </NavLink>
          <NavLink to="/settings" className={({ isActive }) => `nav-link${isActive ? ' active' : ''}`}>
            设置
          </NavLink>
        </nav>
        <div className="topbar-meta">
          <ApiTokenButton onClick={requestApiToken} />
          <span className={`health-dot ${healthReady ? 'online' : healthFailed || health ? 'offline' : ''}`} />
          {healthFailed ? '服务不可达' : health ? healthReady ? '服务正常' : '服务未就绪' : '正在连接'}
        </div>
      </header>
      <Outlet context={{ health, healthFailed, setHealth, requestApiToken } satisfies AppContext} />
      {tokenDialogOpen && <ApiTokenDialog onClose={() => setTokenDialogOpen(false)} />}
    </div>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<FillActorPage />} />
          <Route path="dashboard" element={<DashboardPage />} />
          <Route path="settings" element={<SettingsPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
