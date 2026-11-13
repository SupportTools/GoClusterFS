/**
 * Formatting utilities for displaying data
 */

/**
 * Format bytes to human readable string
 */
export function formatBytes(bytes: number, decimals = 2): string {
  if (bytes === 0) return '0 B'

  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(decimals))} ${sizes[i]}`
}

/**
 * Format number with thousand separators
 */
export function formatNumber(num: number): string {
  return num.toLocaleString()
}

/**
 * Format percentage
 */
export function formatPercent(value: number, decimals = 1): string {
  return `${value.toFixed(decimals)}%`
}

/**
 * Format duration from seconds to human readable
 */
export function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`
  if (seconds < 86400) {
    const hours = Math.floor(seconds / 3600)
    const mins = Math.floor((seconds % 3600) / 60)
    return `${hours}h ${mins}m`
  }
  const days = Math.floor(seconds / 86400)
  const hours = Math.floor((seconds % 86400) / 3600)
  return `${days}d ${hours}h`
}

/**
 * Format latency in microseconds to readable string
 */
export function formatLatency(us: number): string {
  if (us < 1000) return `${us.toFixed(0)} µs`
  if (us < 1000000) return `${(us / 1000).toFixed(2)} ms`
  return `${(us / 1000000).toFixed(2)} s`
}

/**
 * Format timestamp to relative time (e.g., "2 minutes ago")
 */
export function formatRelativeTime(date: Date | null): string {
  if (!date) return 'Never'

  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffSec = Math.floor(diffMs / 1000)

  if (diffSec < 5) return 'Just now'
  if (diffSec < 60) return `${diffSec}s ago`
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`
  return `${Math.floor(diffSec / 86400)}d ago`
}

/**
 * Calculate hit rate from hits and misses
 */
export function calculateHitRate(hits: number, misses: number): number {
  const total = hits + misses
  if (total === 0) return 0
  return (hits / total) * 100
}
