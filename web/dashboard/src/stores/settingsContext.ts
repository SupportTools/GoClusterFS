/**
 * Settings context
 * Separated for import by both provider and hooks
 */

import { createContext } from 'react'
import type { SettingsContextType } from './settingsTypes'

export const SettingsContext = createContext<SettingsContextType | null>(null)
