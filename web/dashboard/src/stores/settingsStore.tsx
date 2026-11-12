import { useState, useCallback, useEffect, type ReactNode } from 'react'
import { apiClient } from '@/api'
import { SettingsContext } from './settingsContext'
import { type Settings, DEFAULT_SETTINGS, STORAGE_KEY } from './settingsTypes'
import { loadToken, saveToken } from './token'

function loadSettings(): Settings {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored) {
      const parsed = JSON.parse(stored) as Partial<Settings>
      return {
        refreshInterval: typeof parsed.refreshInterval === 'number' && parsed.refreshInterval > 0
          ? parsed.refreshInterval
          : DEFAULT_SETTINGS.refreshInterval,
        autoRefresh: typeof parsed.autoRefresh === 'boolean' ? parsed.autoRefresh : DEFAULT_SETTINGS.autoRefresh,
      }
    }
  } catch {
    console.warn('Failed to load settings from localStorage')
  }
  return DEFAULT_SETTINGS
}

function saveSettings(settings: Settings): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(settings))
  } catch {
    console.warn('Failed to save settings to localStorage')
  }
}

export function SettingsProvider({ children }: { children: ReactNode }) {
  const [settings, setSettings] = useState<Settings>(loadSettings)
  const [token, setTokenState] = useState(loadToken)

  useEffect(() => {
    saveSettings(settings)
  }, [settings])

  const updateSettings = useCallback((updates: Partial<Settings>) => {
    setSettings((prev) => ({ ...prev, ...updates }))
  }, [])

  const resetSettings = useCallback(() => {
    setSettings(DEFAULT_SETTINGS)
  }, [])

  const setToken = useCallback((next: string) => {
    const trimmed = next.trim()
    saveToken(trimmed)
    apiClient.setAuthToken(trimmed)
    setTokenState(trimmed)
  }, [])

  return (
    <SettingsContext.Provider value={{ settings, updateSettings, resetSettings, token, setToken }}>
      {children}
    </SettingsContext.Provider>
  )
}
