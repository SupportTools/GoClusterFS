/**
 * Settings hooks
 * Separated from provider component for React Fast Refresh compatibility
 */

import { useContext } from 'react'
import { SettingsContext } from './settingsContext'
import type { SettingsContextType } from './settingsTypes'

/**
 * Hook to use settings
 * Must be used within a SettingsProvider
 */
export function useSettings(): SettingsContextType {
  const context = useContext(SettingsContext)
  if (!context) {
    throw new Error('useSettings must be used within a SettingsProvider')
  }
  return context
}

/**
 * Hook to get just the refresh interval (for data hooks)
 * Returns 0 if auto-refresh is disabled
 */
export function useRefreshInterval(): number {
  const { settings } = useSettings()
  return settings.autoRefresh ? settings.refreshInterval : 0
}
