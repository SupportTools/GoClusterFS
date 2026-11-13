// Kept apart from the provider component for React Fast Refresh.

export const REFRESH_INTERVAL_OPTIONS = [
  { value: 5000, label: '5 seconds' },
  { value: 10000, label: '10 seconds' },
  { value: 30000, label: '30 seconds' },
  { value: 60000, label: '1 minute' },
  { value: 300000, label: '5 minutes' },
] as const

export interface Settings {
  refreshInterval: number
  autoRefresh: boolean
}

export const DEFAULT_SETTINGS: Settings = {
  refreshInterval: 10000,
  autoRefresh: true,
}

export const STORAGE_KEY = 'goclusterfs-dashboard-settings'

// The token unlocks writes, so it lives in sessionStorage and goes away with the tab.
export const TOKEN_STORAGE_KEY = 'goclusterfs-api-token'

export interface SettingsContextType {
  settings: Settings
  updateSettings: (updates: Partial<Settings>) => void
  resetSettings: () => void
  token: string
  setToken: (token: string) => void
}
