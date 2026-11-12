import { useState, useEffect } from 'react'
import { useSettings, REFRESH_INTERVAL_OPTIONS } from '@/stores'
import { useStatus } from '@/hooks'
import { api } from '@/api'

/**
 * Setting section wrapper
 */
function SettingSection({
  title,
  description,
  children,
}: {
  title: string
  description?: string
  children: React.ReactNode
}) {
  return (
    <div className="card">
      <h3 className="text-lg font-medium text-gfs-text-primary mb-1">{title}</h3>
      {description && (
        <p className="text-sm text-gfs-text-muted mb-4">{description}</p>
      )}
      <div className="space-y-4">{children}</div>
    </div>
  )
}

/**
 * Toggle switch component
 */
function Toggle({
  enabled,
  onChange,
  label,
  description,
}: {
  enabled: boolean
  onChange: (enabled: boolean) => void
  label: string
  description?: string
}) {
  return (
    <div className="flex items-center justify-between">
      <div>
        <span className="text-sm font-medium text-gfs-text-primary">{label}</span>
        {description && (
          <p className="text-xs text-gfs-text-muted mt-0.5">{description}</p>
        )}
      </div>
      <button
        type="button"
        role="switch"
        aria-checked={enabled}
        onClick={() => onChange(!enabled)}
        className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-gfs-accent-primary focus:ring-offset-2 focus:ring-offset-gfs-bg-primary ${
          enabled ? 'bg-gfs-accent-primary' : 'bg-gfs-bg-tertiary'
        }`}
      >
        <span
          className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
            enabled ? 'translate-x-6' : 'translate-x-1'
          }`}
        />
      </button>
    </div>
  )
}

/**
 * Select dropdown component
 */
function Select<T extends string | number>({
  value,
  onChange,
  options,
  label,
  description,
}: {
  value: T
  onChange: (value: T) => void
  options: readonly { value: T; label: string }[]
  label: string
  description?: string
}) {
  return (
    <div>
      <label className="block text-sm font-medium text-gfs-text-primary mb-1">
        {label}
      </label>
      {description && (
        <p className="text-xs text-gfs-text-muted mb-2">{description}</p>
      )}
      <select
        value={value}
        onChange={(e) => {
          const newValue = typeof value === 'number'
            ? Number(e.target.value) as T
            : e.target.value as T
          onChange(newValue)
        }}
        className="input w-full max-w-xs"
      >
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  )
}

function TokenForm() {
  const { token, setToken } = useSettings()
  const [draft, setDraft] = useState('')

  return (
    <form
      className="space-y-3"
      onSubmit={(e) => {
        e.preventDefault()
        setToken(draft)
        setDraft('')
      }}
    >
      <div>
        <label htmlFor="api-token" className="block text-sm font-medium text-gfs-text-primary mb-1">
          API token
        </label>
        <p className="text-xs text-gfs-text-muted mb-2">
          {token
            ? 'A token is set for this tab.'
            : 'No token set. Reads work on a loopback API without one; snapshots and clones always need it.'}
        </p>
        <input
          id="api-token"
          type="password"
          autoComplete="off"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder={token ? 'Enter a new token to replace it' : 'Paste the token from -api-token-file'}
          className="input w-full max-w-md"
        />
      </div>
      <div className="flex gap-2">
        <button
          type="submit"
          disabled={!draft.trim()}
          className="px-4 py-2 text-sm font-medium bg-gfs-accent-primary text-white rounded hover:bg-gfs-accent-hover disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          Save token
        </button>
        {token && (
          <button
            type="button"
            onClick={() => setToken('')}
            className="px-4 py-2 text-sm font-medium text-gfs-text-primary bg-gfs-bg-tertiary border border-gfs-border rounded hover:bg-gfs-bg-primary transition-colors"
          >
            Forget token
          </button>
        )}
      </div>
    </form>
  )
}

/**
 * API status indicator
 */
function ApiStatus() {
  const [status, setStatus] = useState<'checking' | 'healthy' | 'error'>('checking')
  const [latency, setLatency] = useState<number | null>(null)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)

  useEffect(() => {
    let mounted = true

    async function checkHealth() {
      const start = performance.now()
      try {
        await api.getHealth()
        const elapsed = Math.round(performance.now() - start)
        if (mounted) {
          setStatus('healthy')
          setLatency(elapsed)
          setErrorMessage(null)
        }
      } catch (error) {
        if (mounted) {
          setStatus('error')
          setLatency(null)
          setErrorMessage(error instanceof Error ? error.message : 'Connection failed')
        }
      }
    }

    checkHealth()

    // Re-check every 30 seconds
    const interval = setInterval(checkHealth, 30000)

    return () => {
      mounted = false
      clearInterval(interval)
    }
  }, [])

  const statusColors = {
    checking: 'bg-gfs-text-muted',
    healthy: 'bg-gfs-status-ok',
    error: 'bg-gfs-status-error',
  }

  const statusLabels = {
    checking: 'Checking...',
    healthy: 'Connected',
    error: 'Disconnected',
  }

  return (
    <div className="flex items-center gap-2">
      <span className={`w-2 h-2 rounded-full ${statusColors[status]} ${status === 'checking' ? 'animate-pulse' : ''}`} />
      <span className="text-sm text-gfs-text-primary">{statusLabels[status]}</span>
      {latency !== null && (
        <span className="text-xs text-gfs-text-muted">({latency}ms)</span>
      )}
      {errorMessage && (
        <span className="text-xs text-gfs-status-error ml-2">{errorMessage}</span>
      )}
    </div>
  )
}

/**
 * Info row for the About section
 */
function InfoRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex justify-between py-2 border-b border-gfs-border last:border-0">
      <dt className="text-sm text-gfs-text-muted">{label}</dt>
      <dd className="text-sm text-gfs-text-primary">{value}</dd>
    </div>
  )
}

export function SettingsPage() {
  const { settings, updateSettings, resetSettings } = useSettings()
  const status = useStatus()
  const [showResetConfirm, setShowResetConfirm] = useState(false)

  const handleReset = () => {
    resetSettings()
    setShowResetConfirm(false)
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gfs-text-primary">Settings</h1>
        <p className="text-sm text-gfs-text-muted mt-1">
          Dashboard preferences and API access
        </p>
      </div>

      {/* Data Fetching */}
      <SettingSection
        title="Data Fetching"
        description="Control how and when the dashboard refreshes data"
      >
        <Toggle
          enabled={settings.autoRefresh}
          onChange={(enabled) => updateSettings({ autoRefresh: enabled })}
          label="Auto-refresh"
          description="Automatically refresh data at the selected interval"
        />

        <Select
          value={settings.refreshInterval}
          onChange={(value) => updateSettings({ refreshInterval: value })}
          options={REFRESH_INTERVAL_OPTIONS}
          label="Refresh Interval"
          description="How often to fetch new data from the API"
        />
      </SettingSection>

      <SettingSection
        title="API Access"
        description="The token stays in this browser tab (sessionStorage) and is gone when you close it."
      >
        <TokenForm />
      </SettingSection>

      {/* About */}
      <SettingSection title="About">
        <dl>
          <InfoRow label="Server Version" value={status.data?.version ?? '-'} />
          <InfoRow label="API Status" value={<ApiStatus />} />
          <InfoRow
            label="Settings Storage"
            value={
              <span className="text-xs font-mono text-gfs-text-secondary">
                localStorage, token in sessionStorage
              </span>
            }
          />
        </dl>
      </SettingSection>

      {/* Actions */}
      <div className="card">
        <h3 className="text-lg font-medium text-gfs-text-primary mb-4">Actions</h3>

        {!showResetConfirm ? (
          <button
            type="button"
            onClick={() => setShowResetConfirm(true)}
            className="px-4 py-2 text-sm font-medium text-gfs-status-error bg-gfs-bg-tertiary border border-gfs-status-error/30 rounded hover:bg-gfs-status-error/10 transition-colors"
          >
            Reset All Settings
          </button>
        ) : (
          <div className="flex items-center gap-3">
            <span className="text-sm text-gfs-text-secondary">
              Reset the refresh settings to their defaults? The token stays.
            </span>
            <button
              type="button"
              onClick={handleReset}
              className="px-4 py-2 text-sm font-medium text-white bg-gfs-status-error rounded hover:bg-gfs-status-error/80 transition-colors"
            >
              Confirm Reset
            </button>
            <button
              type="button"
              onClick={() => setShowResetConfirm(false)}
              className="px-4 py-2 text-sm font-medium text-gfs-text-primary bg-gfs-bg-tertiary border border-gfs-border rounded hover:bg-gfs-bg-primary transition-colors"
            >
              Cancel
            </button>
          </div>
        )}
      </div>
    </div>
  )
}

export default SettingsPage
