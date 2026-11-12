import { useState, useEffect, useCallback, useRef } from 'react'
import { api, ApiError } from '@/api'
import { useRefreshInterval, useSettings } from '@/stores'
import type {
  HealthResponse,
  StatusResponse,
  AllStatsResponse,
  LockStatsResponse,
  ClusterInfoResponse,
  SnapshotListResponse,
} from '@/api'

export interface UseApiResult<T> {
  data: T | null
  loading: boolean
  error: ApiError | null
  refetch: () => void
  lastUpdated: Date | null
}

export interface UseApiOptions {
  /** Auto-refresh interval in milliseconds (0 = disabled) */
  refreshInterval?: number
  /** Refetches from scratch whenever this changes, as the API token does */
  key?: string
}

// The fetcher lives in a ref, so a new closure on every render doesn't start a new request.
function useApiData<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  { refreshInterval = 0, key = '' }: UseApiOptions = {}
): UseApiResult<T> {
  const { token } = useSettings()
  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<ApiError | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  const fetcherRef = useRef(fetcher)
  const abortControllerRef = useRef<AbortController | null>(null)
  const mountedRef = useRef(true)

  useEffect(() => {
    fetcherRef.current = fetcher
  })

  const fetchData = useCallback(async () => {
    abortControllerRef.current?.abort()
    const controller = new AbortController()
    abortControllerRef.current = controller

    setLoading(true)
    try {
      const result = await fetcherRef.current(controller.signal)
      if (mountedRef.current && !controller.signal.aborted) {
        setData(result)
        setError(null)
        setLastUpdated(new Date())
      }
    } catch (err) {
      if (!mountedRef.current || controller.signal.aborted) {
        return
      }
      setError(err instanceof ApiError ? err : new ApiError(err instanceof Error ? err.message : 'Unknown error', 0))
    } finally {
      if (mountedRef.current && abortControllerRef.current === controller) {
        setLoading(false)
      }
    }
  }, [])

  useEffect(() => {
    mountedRef.current = true
    setData(null)
    setError(null)
    fetchData()
    return () => {
      mountedRef.current = false
      abortControllerRef.current?.abort()
    }
  }, [fetchData, key, token])

  useEffect(() => {
    if (refreshInterval > 0) {
      const interval = setInterval(fetchData, refreshInterval)
      return () => clearInterval(interval)
    }
  }, [fetchData, refreshInterval])

  return { data, loading, error, refetch: fetchData, lastUpdated }
}

export function useHealth(options?: UseApiOptions): UseApiResult<HealthResponse> {
  return useApiData((signal) => api.getHealth({ signal }), options)
}

export function useStatus(options?: UseApiOptions): UseApiResult<StatusResponse> {
  return useApiData((signal) => api.getStatus({ signal }), options)
}

export function useAllStats(options?: UseApiOptions): UseApiResult<AllStatsResponse> {
  return useApiData((signal) => api.getAllStats({ signal }), options)
}

export function useLockStats(options?: UseApiOptions): UseApiResult<LockStatsResponse> {
  return useApiData((signal) => api.getLockStats({ signal }), options)
}

export function useClusterStats(options?: UseApiOptions): UseApiResult<ClusterInfoResponse> {
  return useApiData((signal) => api.getClusterStats({ signal }), options)
}

export function useSnapshots(path: string, options?: UseApiOptions): UseApiResult<SnapshotListResponse> {
  return useApiData((signal) => api.listSnapshots(path || undefined, { signal }), { ...options, key: path })
}

export function useDashboardData() {
  const refreshInterval = useRefreshInterval()
  const status = useStatus({ refreshInterval })
  const stats = useAllStats({ refreshInterval })
  const health = useHealth({ refreshInterval })

  return {
    status,
    stats,
    health,
    loading: status.loading || stats.loading || health.loading,
    error: status.error || stats.error || health.error,
  }
}

export function useClusterData() {
  const refreshInterval = useRefreshInterval()
  const cluster = useClusterStats({ refreshInterval })
  const locks = useLockStats({ refreshInterval })

  return {
    cluster,
    locks,
    loading: cluster.loading || locks.loading,
    error: cluster.error || locks.error,
  }
}
