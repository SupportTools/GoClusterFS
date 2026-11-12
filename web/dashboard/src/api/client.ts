// Typed fetch client for the GoClusterFS HTTP API. Calls go to the page's own origin.

import type {
  HealthResponse,
  StatusResponse,
  CacheStatsResponse,
  JournalStatsResponse,
  TrimStatsResponse,
  LockStatsResponse,
  ClusterInfoResponse,
  AllStatsResponse,
  ErrorResponse,
  SnapshotListResponse,
  SnapshotRequest,
  CloneRequest,
  WriteResponse,
  AdminOperation,
  AdminResponse,
} from './types'

export interface ApiConfig {
  baseUrl: string
  authToken?: string
  timeout: number
}

const defaultConfig: ApiConfig = {
  baseUrl: '',
  timeout: 30_000,
}

// The server gives a snapshot or clone of a large file up to 10 minutes.
const WRITE_TIMEOUT = 10 * 60_000

export class ApiError extends Error {
  readonly status: number
  readonly code?: number
  readonly details?: string

  constructor(message: string, status: number, code?: number, details?: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
    this.details = details
  }

  static fromResponse(response: ErrorResponse, status: number): ApiError {
    return new ApiError(response.error, status, response.code, response.details)
  }
}

interface RequestOptions {
  signal?: AbortSignal
  headers?: Record<string, string>
  body?: string
  timeout?: number
}

export class ApiClient {
  private config: ApiConfig

  constructor(config: Partial<ApiConfig> = {}) {
    this.config = { ...defaultConfig, ...config }
  }

  setAuthToken(token: string | undefined): void {
    this.config.authToken = token || undefined
  }

  hasAuthToken(): boolean {
    return this.config.authToken !== undefined
  }

  private async request<T>(method: string, path: string, options: RequestOptions = {}): Promise<T> {
    const headers: Record<string, string> = {
      Accept: 'application/json',
      ...options.headers,
    }
    if (this.config.authToken) {
      headers['Authorization'] = `Bearer ${this.config.authToken}`
    }

    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), options.timeout ?? this.config.timeout)
    const signal = options.signal ? AbortSignal.any([options.signal, controller.signal]) : controller.signal

    try {
      const response = await fetch(`${this.config.baseUrl}${path}`, {
        method,
        headers,
        body: options.body,
        signal,
        credentials: 'omit',
      })

      if (!response.ok) {
        let errorData: ErrorResponse | undefined
        try {
          errorData = (await response.json()) as ErrorResponse
        } catch {
          errorData = undefined
        }
        if (errorData?.error) {
          throw ApiError.fromResponse(errorData, response.status)
        }
        throw new ApiError(`HTTP ${response.status}: ${response.statusText}`, response.status)
      }

      const text = await response.text()
      return (text ? JSON.parse(text) : {}) as T
    } catch (error) {
      if (error instanceof ApiError) throw error
      if (error instanceof DOMException && error.name === 'AbortError') {
        if (options.signal?.aborted) throw error
        throw new ApiError('Request timed out', 408)
      }
      if (error instanceof TypeError) {
        throw new ApiError('Network error: unable to reach the API', 0)
      }
      throw new ApiError(error instanceof Error ? error.message : 'Unknown error', 0)
    } finally {
      clearTimeout(timeoutId)
    }
  }

  private get<T>(path: string, options?: RequestOptions): Promise<T> {
    return this.request<T>('GET', path, options)
  }

  private send<T>(method: 'POST' | 'DELETE', path: string, body?: unknown): Promise<T> {
    return this.request<T>(method, path, {
      headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      timeout: WRITE_TIMEOUT,
    })
  }

  getHealth(options?: RequestOptions): Promise<HealthResponse> {
    return this.get('/health', options)
  }

  getStatus(options?: RequestOptions): Promise<StatusResponse> {
    return this.get('/api/v1/status', options)
  }

  getAllStats(options?: RequestOptions): Promise<AllStatsResponse> {
    return this.get('/api/v1/stats/all', options)
  }

  getCacheStats(options?: RequestOptions): Promise<CacheStatsResponse> {
    return this.get('/api/v1/stats/cache', options)
  }

  getJournalStats(options?: RequestOptions): Promise<JournalStatsResponse> {
    return this.get('/api/v1/stats/journal', options)
  }

  getTrimStats(options?: RequestOptions): Promise<TrimStatsResponse> {
    return this.get('/api/v1/stats/trim', options)
  }

  getLockStats(options?: RequestOptions): Promise<LockStatsResponse> {
    return this.get('/api/v1/stats/lock', options)
  }

  getClusterStats(options?: RequestOptions): Promise<ClusterInfoResponse> {
    return this.get('/api/v1/stats/cluster', options)
  }

  // Without a path, lists every snapshot in the filesystem.
  listSnapshots(path?: string, options?: RequestOptions): Promise<SnapshotListResponse> {
    const query = path ? `?path=${encodeURIComponent(path)}` : ''
    return this.get(`/api/v1/snapshots${query}`, options)
  }

  createSnapshot(request: SnapshotRequest): Promise<WriteResponse> {
    return this.send('POST', '/api/v1/snapshots', request)
  }

  // Deletion finishes in the background: the snapshot lists as "deleting" until the worker frees it.
  deleteSnapshot(name: string, path: string): Promise<WriteResponse> {
    return this.send('DELETE', `/api/v1/snapshots/${encodeURIComponent(name)}?path=${encodeURIComponent(path)}`)
  }

  cloneSnapshot(name: string, request: CloneRequest): Promise<WriteResponse> {
    return this.send('POST', `/api/v1/snapshots/${encodeURIComponent(name)}/clone`, request)
  }

  cloneFile(request: CloneRequest): Promise<WriteResponse> {
    return this.send('POST', '/api/v1/clones', request)
  }

  admin(operation: AdminOperation): Promise<AdminResponse> {
    return this.send('POST', `/api/v1/admin/${operation}`)
  }
}

export const apiClient = new ApiClient()

export const api = {
  getHealth: (options?: RequestOptions) => apiClient.getHealth(options),
  getStatus: (options?: RequestOptions) => apiClient.getStatus(options),
  getAllStats: (options?: RequestOptions) => apiClient.getAllStats(options),
  getCacheStats: (options?: RequestOptions) => apiClient.getCacheStats(options),
  getJournalStats: (options?: RequestOptions) => apiClient.getJournalStats(options),
  getTrimStats: (options?: RequestOptions) => apiClient.getTrimStats(options),
  getLockStats: (options?: RequestOptions) => apiClient.getLockStats(options),
  getClusterStats: (options?: RequestOptions) => apiClient.getClusterStats(options),
  listSnapshots: (path?: string, options?: RequestOptions) => apiClient.listSnapshots(path, options),
  createSnapshot: (request: SnapshotRequest) => apiClient.createSnapshot(request),
  deleteSnapshot: (name: string, path: string) => apiClient.deleteSnapshot(name, path),
  cloneSnapshot: (name: string, request: CloneRequest) => apiClient.cloneSnapshot(name, request),
  cloneFile: (request: CloneRequest) => apiClient.cloneFile(request),
  admin: (operation: AdminOperation) => apiClient.admin(operation),
}
