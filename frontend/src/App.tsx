import { useCallback, useEffect, useState } from 'react'
import { BrowserRouter, NavLink, Outlet, Route, Routes, useLocation } from 'react-router-dom'

import { getHealth, type HealthStatus } from './api'
import { LoginDialog, LoginScreen, SignOutButton } from './components/Login'
import { useAuthGate } from './lib/apiToken'
import DashboardPage from './pages/DashboardPage'
import FillActorPage from './pages/FillActorPage'
import SettingsPage from './pages/SettingsPage'

const APP_NAME = 'Embyx Manager'

/** Route paths, nav labels and tab titles share one source so they cannot drift apart. */
const NAV_ITEMS = [
  { to: '/dashboard', end: false, label: '监控看板' },
  { to: '/', end: true, label: '补全演员' },
  { to: '/settings', end: false, label: '设置' },
] as const

export interface AppContext {
  health: HealthStatus | null
  healthFailed: boolean
  setHealth: React.Dispatch<React.SetStateAction<HealthStatus | null>>
  /** Opens the shared re-login dialog; pages call it when a request comes back 401. */
  requestApiToken: () => void
}

/** @deprecated Kept for existing imports; the context now carries more than health. */
export type HealthContext = AppContext

interface LayoutProps extends AppContext {
  authRequired: boolean
}

/** Keeps the tab title on the app name, suffixed with the page so open tabs stay tellable apart. */
function useDocumentTitle() {
  const { pathname } = useLocation()
  useEffect(() => {
    const item = NAV_ITEMS.find((nav) => (nav.end ? pathname === nav.to : pathname.startsWith(nav.to)))
    document.title = item && !item.end ? `${item.label} · ${APP_NAME}` : APP_NAME
  }, [pathname])
}

/** Health drives the login gate as well as the status chip, so it is polled above the router. */
function useHealthPolling() {
  const [health, setHealth] = useState<HealthStatus | null>(null)
  const [healthFailed, setHealthFailed] = useState(false)

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

  return { health, healthFailed, setHealth }
}

function Layout({ health, healthFailed, setHealth, requestApiToken, authRequired }: LayoutProps) {
  useDocumentTitle()

  const healthReady = Boolean(health && ['ok', 'healthy', 'ready'].includes(health.status.toLowerCase()))

  return (
    <div className="app-shell">
      <header className="topbar">
        <NavLink className="brand" to="/" aria-label={`${APP_NAME} 首页`}>
          <span className="brand-mark" aria-hidden="true">E</span>
          <span>embyx<span className="brand-suffix"> manager</span></span>
        </NavLink>
        <nav className="topbar-nav" aria-label="页面导航">
          {NAV_ITEMS.map(({ to, end, label }) => (
            <NavLink key={to} to={to} end={end} className={({ isActive }) => `nav-link${isActive ? ' active' : ''}`}>
              {label}
            </NavLink>
          ))}
        </nav>
        <div className="topbar-meta">
          {authRequired && <SignOutButton />}
          <span className={`health-dot ${healthReady ? 'online' : healthFailed || health ? 'offline' : ''}`} />
          {healthFailed ? '服务不可达' : health ? healthReady ? '服务正常' : '服务未就绪' : '正在连接'}
        </div>
      </header>
      <Outlet context={{ health, healthFailed, setHealth, requestApiToken } satisfies AppContext} />
    </div>
  )
}

export default function App() {
  const { health, healthFailed, setHealth } = useHealthPolling()
  const gate = useAuthGate(health)
  const [loginDialogOpen, setLoginDialogOpen] = useState(false)
  const requestApiToken = useCallback(() => setLoginDialogOpen(true), [])

  // Signing out (or an expired token) must not leave the dialog waiting behind the gate.
  useEffect(() => {
    if (gate.screen === 'login') setLoginDialogOpen(false)
  }, [gate.screen])

  if (gate.screen === 'login') return <LoginScreen notice={gate.notice} />

  return (
    <BrowserRouter>
      <Routes>
        <Route
          element={
            <Layout
              health={health}
              healthFailed={healthFailed}
              setHealth={setHealth}
              requestApiToken={requestApiToken}
              authRequired={gate.authRequired}
            />
          }
        >
          <Route index element={<FillActorPage />} />
          <Route path="dashboard" element={<DashboardPage />} />
          <Route path="settings" element={<SettingsPage />} />
        </Route>
      </Routes>
      {loginDialogOpen && <LoginDialog onClose={() => setLoginDialogOpen(false)} />}
    </BrowserRouter>
  )
}
