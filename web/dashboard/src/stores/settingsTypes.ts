/**
 * Settings types and constants
 * Separated from components for React Fast Refresh compatibility
 */

// Available refresh interval options (in milliseconds)
export const REFRESH_INTERVAL_OPTIONS = [
  { value: 0, label: 'Disabled' },
  { value: 5000, label: '5 seconds' },
  { value: 10000, label: '10 seconds' },
  { value: 30000, label: '30 seconds' },
  { value: 60000, label: '1 minute' },
  { value: 300000, label: '5 minutes' },
] as const

// Theme options
export type ThemeMode = 'dark' | 'light' | 'system'

// Settings interface
export interface Settings {
  // Data fetching
  refreshInterval: number
  autoRefresh: boolean

  // Appearance
  theme: ThemeMode
  compactMode: boolean

  // API
  apiEndpoint: string

  // Dashboard
  showNodeLatency: boolean
  showLockPanel: boolean
}

// Default settings
export const DEFAULT_SETTINGS: Settings = {
  refreshInterval: 10000,
  autoRefresh: true,
  theme: 'dark',
  compactMode: false,
  apiEndpoint: '',
  showNodeLatency: true,
  showLockPanel: true,
}

export const STORAGE_KEY = 'goclusterfs-dashboard-settings'

// Context type
export interface SettingsContextType {
  settings: Settings
  updateSettings: (updates: Partial<Settings>) => void
  resetSettings: () => void
}
