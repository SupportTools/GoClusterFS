/**
 * Custom React hooks for GoClusterFS API data fetching.
 *
 * These hooks provide:
 * - Auto-refresh with configurable intervals
 * - Loading and error states
 * - Request cancellation on unmount
 * - Manual refetch capability
 */

import { useState, useEffect, useCallback, useRef } from 'react'
import { api, ApiError } from '@/api'
import type {
  HealthResponse,
  StatusResponse,
  CacheStatsResponse,
  JournalStatsResponse,
  AllStatsResponse,
  ClusterNodesResponse,
  ClusterLocksResponse,
  TrimStatsResponse,
  LockStatsResponse,
  SFBStatsResponse,
  ClusterInfoResponse,
  SnapshotsListResponse,
} from '@/api'

// Hook result type
export interface UseApiResult<T> {
  data: T | null
  loading: boolean
  error: ApiError | null
  refetch: () => void
  lastUpdated: Date | null
}

// Hook options
export interface UseApiOptions {
  /** Auto-refresh interval in milliseconds (0 = disabled) */
  refreshInterval?: number
  /** Whether to fetch immediately on mount */
  immediate?: boolean
}

const DEFAULT_OPTIONS: UseApiOptions = {
  refreshInterval: 0,
  immediate: true,
}

/**
 * Generic hook for API data fetching
 */
function useApiData<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  options: UseApiOptions = {}
): UseApiResult<T> {
  const opts = { ...DEFAULT_OPTIONS, ...options }

  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(opts.immediate ?? true)
  const [error, setError] = useState<ApiError | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  const abortControllerRef = useRef<AbortController | null>(null)
  const mountedRef = useRef(true)

  const fetchData = useCallback(async () => {
    // Cancel any pending request
    if (abortControllerRef.current) {
      abortControllerRef.current.abort()
    }

    abortControllerRef.current = new AbortController()

    setLoading(true)
    setError(null)

    try {
      const result = await fetcher(abortControllerRef.current.signal)
      if (mountedRef.current) {
        setData(result)
        setLastUpdated(new Date())
      }
    } catch (err) {
      if (mountedRef.current) {
        // Ignore abort errors
        if (err instanceof DOMException && err.name === 'AbortError') {
          return
        }
        setError(err instanceof ApiError ? err : new ApiError(
          err instanceof Error ? err.message : 'Unknown error',
          0
        ))
      }
    } finally {
      if (mountedRef.current) {
        setLoading(false)
      }
    }
  }, [fetcher])

  // Initial fetch
  useEffect(() => {
    mountedRef.current = true

    if (opts.immediate) {
      fetchData()
    }

    return () => {
      mountedRef.current = false
      if (abortControllerRef.current) {
        abortControllerRef.current.abort()
      }
    }
  }, [fetchData, opts.immediate])

  // Auto-refresh
  useEffect(() => {
    if (opts.refreshInterval && opts.refreshInterval > 0) {
      const interval = setInterval(fetchData, opts.refreshInterval)
      return () => clearInterval(interval)
    }
  }, [fetchData, opts.refreshInterval])

  return {
    data,
    loading,
    error,
    refetch: fetchData,
    lastUpdated,
  }
}

// ============================================================
// Specialized Hooks
// ============================================================

/**
 * Hook for health status
 */
export function useHealth(options?: UseApiOptions): UseApiResult<HealthResponse> {
  return useApiData(
    (signal) => api.getHealth({ signal }),
    options
  )
}

/**
 * Hook for system status
 */
export function useStatus(options?: UseApiOptions): UseApiResult<StatusResponse> {
  return useApiData(
    (signal) => api.getStatus({ signal }),
    options
  )
}

/**
 * Hook for all stats combined
 */
export function useAllStats(options?: UseApiOptions): UseApiResult<AllStatsResponse> {
  return useApiData(
    (signal) => api.getAllStats({ signal }),
    options
  )
}

/**
 * Hook for cache stats
 */
export function useCacheStats(options?: UseApiOptions): UseApiResult<CacheStatsResponse> {
  return useApiData(
    (signal) => api.getCacheStats({ signal }),
    options
  )
}

/**
 * Hook for journal stats
 */
export function useJournalStats(options?: UseApiOptions): UseApiResult<JournalStatsResponse> {
  return useApiData(
    (signal) => api.getJournalStats({ signal }),
    options
  )
}

/**
 * Hook for trim stats
 */
export function useTrimStats(options?: UseApiOptions): UseApiResult<TrimStatsResponse> {
  return useApiData(
    (signal) => api.getTrimStats({ signal }),
    options
  )
}

/**
 * Hook for lock stats
 */
export function useLockStats(options?: UseApiOptions): UseApiResult<LockStatsResponse> {
  return useApiData(
    (signal) => api.getLockStats({ signal }),
    options
  )
}

/**
 * Hook for SFB (space/free block) stats
 */
export function useSFBStats(options?: UseApiOptions): UseApiResult<SFBStatsResponse> {
  return useApiData(
    (signal) => api.getSFBStats({ signal }),
    options
  )
}

/**
 * Hook for cluster stats
 */
export function useClusterStats(options?: UseApiOptions): UseApiResult<ClusterInfoResponse> {
  return useApiData(
    (signal) => api.getClusterStats({ signal }),
    options
  )
}

/**
 * Hook for cluster nodes
 */
export function useClusterNodes(options?: UseApiOptions): UseApiResult<ClusterNodesResponse> {
  return useApiData(
    (signal) => api.getClusterNodes({ signal }),
    options
  )
}

/**
 * Hook for cluster locks
 */
export function useClusterLocks(options?: UseApiOptions): UseApiResult<ClusterLocksResponse> {
  return useApiData(
    (signal) => api.getClusterLocks({ signal }),
    options
  )
}

// ============================================================
// Convenience Hooks with Defaults
// ============================================================

/** Default refresh interval for dashboard (5 seconds) */
export const DASHBOARD_REFRESH_INTERVAL = 5000

/** Default refresh interval for status checks (10 seconds) */
export const STATUS_REFRESH_INTERVAL = 10000

/**
 * Hook for dashboard data with auto-refresh
 */
export function useDashboardData() {
  const status = useStatus({ refreshInterval: STATUS_REFRESH_INTERVAL })
  const stats = useAllStats({ refreshInterval: DASHBOARD_REFRESH_INTERVAL })
  const health = useHealth({ refreshInterval: STATUS_REFRESH_INTERVAL })

  return {
    status,
    stats,
    health,
    loading: status.loading || stats.loading || health.loading,
    error: status.error || stats.error || health.error,
  }
}

/**
 * Hook for cluster page data with auto-refresh
 */
export function useClusterData() {
  const nodes = useClusterNodes({ refreshInterval: DASHBOARD_REFRESH_INTERVAL })
  const locks = useClusterLocks({ refreshInterval: DASHBOARD_REFRESH_INTERVAL })
  const stats = useClusterStats({ refreshInterval: DASHBOARD_REFRESH_INTERVAL })

  return {
    nodes,
    locks,
    stats,
    loading: nodes.loading || locks.loading || stats.loading,
    error: nodes.error || locks.error || stats.error,
  }
}

/**
 * Hook for snapshots list
 */
export function useSnapshots(options?: UseApiOptions): UseApiResult<SnapshotsListResponse> {
  return useApiData(
    (signal) => api.listSnapshots({ signal }),
    options
  )
}

/**
 * Hook for snapshots page data with auto-refresh
 */
export function useSnapshotsData() {
  const snapshots = useSnapshots({ refreshInterval: STATUS_REFRESH_INTERVAL })

  return {
    snapshots,
    loading: snapshots.loading,
    error: snapshots.error,
  }
}
