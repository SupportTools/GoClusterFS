/**
 * Main dashboard layout with sidebar navigation
 */

import { useState } from 'react'
import { Outlet, NavLink, useLocation } from 'react-router-dom'
import { useHealth } from '@/hooks'

// Navigation items
const navItems = [
  { path: '/', label: 'Dashboard', icon: DashboardIcon },
  { path: '/cluster', label: 'Cluster', icon: ClusterIcon },
  { path: '/snapshots', label: 'Snapshots', icon: SnapshotsIcon },
  { path: '/statistics', label: 'Statistics', icon: StatsIcon },
  { path: '/settings', label: 'Settings', icon: SettingsIcon },
]

export function DashboardLayout() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const location = useLocation()
  const { data: health, error: healthError } = useHealth({ refreshInterval: 10000 })

  const getHealthStatus = () => {
    if (healthError) return 'disconnected'
    if (!health) return 'connecting'
    return health.status
  }

  const healthStatus = getHealthStatus()

  return (
    <div className="flex min-h-screen bg-gfs-bg-primary">
      {/* Sidebar */}
      <aside
        className={`fixed left-0 top-0 z-40 flex h-screen flex-col border-r border-gfs-border bg-gfs-bg-secondary transition-all duration-300 ${
          sidebarCollapsed ? 'w-16' : 'w-64'
        }`}
      >
        {/* Logo */}
        <div className="flex h-16 items-center justify-between border-b border-gfs-border px-4">
          {!sidebarCollapsed && (
            <span className="text-lg font-semibold text-gfs-text-primary">
              GoClusterFS
            </span>
          )}
          <button
            onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
            className="rounded p-1.5 text-gfs-text-muted hover:bg-gfs-bg-tertiary hover:text-gfs-text-primary"
            aria-label={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          >
            <ChevronIcon collapsed={sidebarCollapsed} />
          </button>
        </div>

        {/* Navigation */}
        <nav className="flex-1 overflow-y-auto p-3">
          <ul className="space-y-1">
            {navItems.map(({ path, label, icon: Icon }) => {
              const isActive = location.pathname === path
              return (
                <li key={path}>
                  <NavLink
                    to={path}
                    className={`flex items-center gap-3 rounded-md px-3 py-2.5 text-sm font-medium transition-colors ${
                      isActive
                        ? 'bg-gfs-accent-primary/10 text-gfs-accent-primary'
                        : 'text-gfs-text-secondary hover:bg-gfs-bg-tertiary hover:text-gfs-text-primary'
                    }`}
                    title={sidebarCollapsed ? label : undefined}
                  >
                    <Icon className="h-5 w-5 flex-shrink-0" />
                    {!sidebarCollapsed && <span>{label}</span>}
                  </NavLink>
                </li>
              )
            })}
          </ul>
        </nav>

        {/* Connection Status */}
        <div className="border-t border-gfs-border p-3">
          <div
            className={`flex items-center gap-2 rounded-md px-3 py-2 ${
              sidebarCollapsed ? 'justify-center' : ''
            }`}
            title={sidebarCollapsed ? `Status: ${healthStatus}` : undefined}
          >
            <StatusIndicator status={healthStatus} />
            {!sidebarCollapsed && (
              <span className="text-xs text-gfs-text-muted capitalize">
                {healthStatus}
              </span>
            )}
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <div
        className={`flex-1 transition-all duration-300 ${
          sidebarCollapsed ? 'ml-16' : 'ml-64'
        }`}
      >
        {/* Header */}
        <header className="sticky top-0 z-30 flex h-16 items-center justify-between border-b border-gfs-border bg-gfs-bg-primary/95 px-6 backdrop-blur">
          <h1 className="text-lg font-semibold text-gfs-text-primary">
            {navItems.find((item) => item.path === location.pathname)?.label || 'Dashboard'}
          </h1>
          <div className="flex items-center gap-4">
            {/* Health badge in header */}
            <HealthBadge status={healthStatus} />
          </div>
        </header>

        {/* Page Content */}
        <main className="p-6">
          <Outlet />
        </main>
      </div>
    </div>
  )
}

// ============================================================
// Icons
// ============================================================

function DashboardIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6A2.25 2.25 0 016 3.75h2.25A2.25 2.25 0 0110.5 6v2.25a2.25 2.25 0 01-2.25 2.25H6a2.25 2.25 0 01-2.25-2.25V6zM3.75 15.75A2.25 2.25 0 016 13.5h2.25a2.25 2.25 0 012.25 2.25V18a2.25 2.25 0 01-2.25 2.25H6A2.25 2.25 0 013.75 18v-2.25zM13.5 6a2.25 2.25 0 012.25-2.25H18A2.25 2.25 0 0120.25 6v2.25A2.25 2.25 0 0118 10.5h-2.25a2.25 2.25 0 01-2.25-2.25V6zM13.5 15.75a2.25 2.25 0 012.25-2.25H18a2.25 2.25 0 012.25 2.25V18A2.25 2.25 0 0118 20.25h-2.25A2.25 2.25 0 0113.5 18v-2.25z" />
    </svg>
  )
}

function ClusterIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M5.25 14.25h13.5m-13.5 0a3 3 0 01-3-3m3 3a3 3 0 100 6h13.5a3 3 0 100-6m-16.5-3a3 3 0 013-3h13.5a3 3 0 013 3m-19.5 0a4.5 4.5 0 01.9-2.7L5.737 5.1a3.375 3.375 0 012.7-1.35h7.126c1.062 0 2.062.5 2.7 1.35l2.587 3.45a4.5 4.5 0 01.9 2.7m0 0a3 3 0 01-3 3m0 3h.008v.008h-.008v-.008zm0-6h.008v.008h-.008v-.008zm-3 6h.008v.008h-.008v-.008zm0-6h.008v.008h-.008v-.008z" />
    </svg>
  )
}

function SnapshotsIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M6.827 6.175A2.31 2.31 0 015.186 7.23c-.38.054-.757.112-1.134.175C2.999 7.58 2.25 8.507 2.25 9.574V18a2.25 2.25 0 002.25 2.25h15A2.25 2.25 0 0021.75 18V9.574c0-1.067-.75-1.994-1.802-2.169a47.865 47.865 0 00-1.134-.175 2.31 2.31 0 01-1.64-1.055l-.822-1.316a2.192 2.192 0 00-1.736-1.039 48.774 48.774 0 00-5.232 0 2.192 2.192 0 00-1.736 1.039l-.821 1.316z" />
      <path strokeLinecap="round" strokeLinejoin="round" d="M16.5 12.75a4.5 4.5 0 11-9 0 4.5 4.5 0 019 0zM18.75 10.5h.008v.008h-.008V10.5z" />
    </svg>
  )
}

function StatsIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
    </svg>
  )
}

function SettingsIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M9.594 3.94c.09-.542.56-.94 1.11-.94h2.593c.55 0 1.02.398 1.11.94l.213 1.281c.063.374.313.686.645.87.074.04.147.083.22.127.324.196.72.257 1.075.124l1.217-.456a1.125 1.125 0 011.37.49l1.296 2.247a1.125 1.125 0 01-.26 1.431l-1.003.827c-.293.24-.438.613-.431.992a6.759 6.759 0 010 .255c-.007.378.138.75.43.99l1.005.828c.424.35.534.954.26 1.43l-1.298 2.247a1.125 1.125 0 01-1.369.491l-1.217-.456c-.355-.133-.75-.072-1.076.124a6.57 6.57 0 01-.22.128c-.331.183-.581.495-.644.869l-.213 1.28c-.09.543-.56.941-1.11.941h-2.594c-.55 0-1.02-.398-1.11-.94l-.213-1.281c-.062-.374-.312-.686-.644-.87a6.52 6.52 0 01-.22-.127c-.325-.196-.72-.257-1.076-.124l-1.217.456a1.125 1.125 0 01-1.369-.49l-1.297-2.247a1.125 1.125 0 01.26-1.431l1.004-.827c.292-.24.437-.613.43-.992a6.932 6.932 0 010-.255c.007-.378-.138-.75-.43-.99l-1.004-.828a1.125 1.125 0 01-.26-1.43l1.297-2.247a1.125 1.125 0 011.37-.491l1.216.456c.356.133.751.072 1.076-.124.072-.044.146-.087.22-.128.332-.183.582-.495.644-.869l.214-1.281z" />
      <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
    </svg>
  )
}

function ChevronIcon({ collapsed }: { collapsed: boolean }) {
  return (
    <svg
      className={`h-5 w-5 transition-transform ${collapsed ? 'rotate-180' : ''}`}
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      strokeWidth={1.5}
    >
      <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
    </svg>
  )
}

// ============================================================
// Status Components
// ============================================================

function StatusIndicator({ status }: { status: string }) {
  const colors: Record<string, string> = {
    ok: 'bg-gfs-status-ok',
    degraded: 'bg-gfs-status-warning',
    unhealthy: 'bg-gfs-status-error',
    disconnected: 'bg-gfs-status-error',
    connecting: 'bg-gfs-text-muted animate-pulse',
  }

  return (
    <span className={`h-2.5 w-2.5 rounded-full ${colors[status] || colors.connecting}`} />
  )
}

function HealthBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    ok: 'badge-ok',
    degraded: 'badge-warning',
    unhealthy: 'badge-error',
    disconnected: 'badge-error',
    connecting: 'badge bg-gfs-text-muted/10 text-gfs-text-muted',
  }

  const labels: Record<string, string> = {
    ok: 'Connected',
    degraded: 'Degraded',
    unhealthy: 'Unhealthy',
    disconnected: 'Disconnected',
    connecting: 'Connecting...',
  }

  return (
    <span className={styles[status] || styles.connecting}>
      ● {labels[status] || 'Unknown'}
    </span>
  )
}

export default DashboardLayout
