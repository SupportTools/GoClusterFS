/**
 * GoClusterFS API Client
 *
 * Typed fetch-based client for communicating with the GoClusterFS HTTP API.
 * Handles authentication, error handling, and request/response processing.
 */

import type {
  HealthResponse,
  StatusResponse,
  CacheStatsResponse,
  JournalStatsResponse,
  TrimStatsResponse,
  LockStatsResponse,
  SFBStatsResponse,
  ClusterInfoResponse,
  AllStatsResponse,
  ClusterNodesResponse,
  ClusterLocksResponse,
  StatsOverviewResponse,
  ErrorResponse,
  SnapshotsListResponse,
  SnapshotInfo,
  CreateSnapshotRequest,
  CreateSnapshotResponse,
  DeleteSnapshotResponse,
} from './types'

// API configuration
export interface ApiConfig {
  baseUrl: string
  authToken?: string
  timeout?: number
}

// Default configuration
const defaultConfig: ApiConfig = {
  baseUrl: '',  // Uses relative URLs by default (proxied by Vite in dev)
  timeout: 30000,
}

// Custom API error class
export class ApiError extends Error {
  readonly status: number
  readonly code?: number
  readonly details?: string

  constructor(
    message: string,
    status: number,
    code?: number,
    details?: string
  ) {
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

// Request options for API calls
interface RequestOptions {
  signal?: AbortSignal
  headers?: Record<string, string>
  body?: string
}

/**
 * GoClusterFS API Client
 */
export class ApiClient {
  private config: ApiConfig

  constructor(config: Partial<ApiConfig> = {}) {
    this.config = { ...defaultConfig, ...config }
  }

  /**
   * Set the authentication token
   */
  setAuthToken(token: string | undefined): void {
    this.config.authToken = token
  }

  /**
   * Make an authenticated request to the API
   */
  private async request<T>(
    method: string,
    path: string,
    options: RequestOptions = {}
  ): Promise<T> {
    const url = `${this.config.baseUrl}${path}`

    const headers: Record<string, string> = {
      'Accept': 'application/json',
      ...options.headers,
    }

    if (this.config.authToken) {
      headers['Authorization'] = `Bearer ${this.config.authToken}`
    }

    // Create abort controller for timeout
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), this.config.timeout)

    try {
      const response = await fetch(url, {
        method,
        headers,
        body: options.body,
        signal: options.signal ?? controller.signal,
      })

      clearTimeout(timeoutId)

      if (!response.ok) {
        // Try to parse error response
        try {
          const errorData = await response.json() as ErrorResponse
          throw ApiError.fromResponse(errorData, response.status)
        } catch (e) {
          if (e instanceof ApiError) throw e
          throw new ApiError(
            `HTTP ${response.status}: ${response.statusText}`,
            response.status
          )
        }
      }

      // Handle empty responses
      const text = await response.text()
      if (!text) {
        return {} as T
      }

      return JSON.parse(text) as T
    } catch (error) {
      clearTimeout(timeoutId)

      if (error instanceof ApiError) {
        throw error
      }

      if (error instanceof DOMException && error.name === 'AbortError') {
        throw new ApiError('Request timed out', 408)
      }

      if (error instanceof TypeError) {
        throw new ApiError('Network error: Unable to connect', 0)
      }

      throw new ApiError(
        error instanceof Error ? error.message : 'Unknown error',
        0
      )
    }
  }

  /**
   * GET request helper
   */
  private get<T>(path: string, options?: RequestOptions): Promise<T> {
    return this.request<T>('GET', path, options)
  }

  /**
   * POST request helper
   */
  private post<T>(path: string, body?: unknown, options?: RequestOptions): Promise<T> {
    return this.request<T>('POST', path, {
      ...options,
      headers: {
        ...options?.headers,
        'Content-Type': 'application/json',
      },
      body: body ? JSON.stringify(body) : undefined,
    } as RequestOptions & { body?: string })
  }

  /**
   * DELETE request helper
   */
  private delete<T>(path: string, options?: RequestOptions): Promise<T> {
    return this.request<T>('DELETE', path, options)
  }

  // ============================================================
  // Health Endpoints
  // ============================================================

  /**
   * Check API health status
   * GET /health
   */
  async getHealth(options?: RequestOptions): Promise<HealthResponse> {
    return this.get<HealthResponse>('/health', options)
  }

  // ============================================================
  // Status Endpoints
  // ============================================================

  /**
   * Get system status overview
   * GET /api/v1/status
   */
  async getStatus(options?: RequestOptions): Promise<StatusResponse> {
    return this.get<StatusResponse>('/api/v1/status', options)
  }

  // ============================================================
  // Statistics Endpoints
  // ============================================================

  /**
   * Get stats overview
   * GET /api/v1/stats
   */
  async getStatsOverview(options?: RequestOptions): Promise<StatsOverviewResponse> {
    return this.get<StatsOverviewResponse>('/api/v1/stats', options)
  }

  /**
   * Get all stats combined
   * GET /api/v1/stats/all
   */
  async getAllStats(options?: RequestOptions): Promise<AllStatsResponse> {
    return this.get<AllStatsResponse>('/api/v1/stats/all', options)
  }

  /**
   * Get cache statistics
   * GET /api/v1/stats/cache
   */
  async getCacheStats(options?: RequestOptions): Promise<CacheStatsResponse> {
    return this.get<CacheStatsResponse>('/api/v1/stats/cache', options)
  }

  /**
   * Get journal statistics
   * GET /api/v1/stats/journal
   */
  async getJournalStats(options?: RequestOptions): Promise<JournalStatsResponse> {
    return this.get<JournalStatsResponse>('/api/v1/stats/journal', options)
  }

  /**
   * Get trim statistics
   * GET /api/v1/stats/trim
   */
  async getTrimStats(options?: RequestOptions): Promise<TrimStatsResponse> {
    return this.get<TrimStatsResponse>('/api/v1/stats/trim', options)
  }

  /**
   * Get lock statistics
   * GET /api/v1/stats/lock
   */
  async getLockStats(options?: RequestOptions): Promise<LockStatsResponse> {
    return this.get<LockStatsResponse>('/api/v1/stats/lock', options)
  }

  /**
   * Get space/free block statistics
   * GET /api/v1/stats/sfb
   */
  async getSFBStats(options?: RequestOptions): Promise<SFBStatsResponse> {
    return this.get<SFBStatsResponse>('/api/v1/stats/sfb', options)
  }

  /**
   * Get cluster statistics
   * GET /api/v1/stats/cluster
   */
  async getClusterStats(options?: RequestOptions): Promise<ClusterInfoResponse> {
    return this.get<ClusterInfoResponse>('/api/v1/stats/cluster', options)
  }

  // ============================================================
  // Cluster Endpoints
  // ============================================================

  /**
   * Get cluster nodes
   * GET /api/v1/cluster/nodes
   */
  async getClusterNodes(options?: RequestOptions): Promise<ClusterNodesResponse> {
    return this.get<ClusterNodesResponse>('/api/v1/cluster/nodes', options)
  }

  /**
   * Get cluster locks
   * GET /api/v1/cluster/locks
   */
  async getClusterLocks(options?: RequestOptions): Promise<ClusterLocksResponse> {
    return this.get<ClusterLocksResponse>('/api/v1/cluster/locks', options)
  }

  // ============================================================
  // Snapshot Endpoints
  // ============================================================

  /**
   * List all snapshots
   * GET /api/v1/snapshots
   */
  async listSnapshots(options?: RequestOptions): Promise<SnapshotsListResponse> {
    return this.get<SnapshotsListResponse>('/api/v1/snapshots', options)
  }

  /**
   * Get a specific snapshot
   * GET /api/v1/snapshots/:id
   */
  async getSnapshot(snapshotId: string, options?: RequestOptions): Promise<SnapshotInfo> {
    return this.get<SnapshotInfo>(`/api/v1/snapshots/${encodeURIComponent(snapshotId)}`, options)
  }

  /**
   * Create a new snapshot
   * POST /api/v1/snapshots
   */
  async createSnapshot(request: CreateSnapshotRequest, options?: RequestOptions): Promise<CreateSnapshotResponse> {
    return this.post<CreateSnapshotResponse>('/api/v1/snapshots', request, options)
  }

  /**
   * Delete a snapshot
   * DELETE /api/v1/snapshots/:id
   */
  async deleteSnapshot(snapshotId: string, options?: RequestOptions): Promise<DeleteSnapshotResponse> {
    return this.delete<DeleteSnapshotResponse>(`/api/v1/snapshots/${encodeURIComponent(snapshotId)}`, options)
  }

  // ============================================================
  // Metrics Endpoint
  // ============================================================

  /**
   * Get Prometheus metrics (raw text format)
   * GET /metrics
   */
  async getMetrics(options?: RequestOptions): Promise<string> {
    const url = `${this.config.baseUrl}/metrics`

    const headers: Record<string, string> = {
      'Accept': 'text/plain',
    }

    if (this.config.authToken) {
      headers['Authorization'] = `Bearer ${this.config.authToken}`
    }

    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), this.config.timeout)

    try {
      const response = await fetch(url, {
        method: 'GET',
        headers,
        signal: options?.signal ?? controller.signal,
      })

      clearTimeout(timeoutId)

      if (!response.ok) {
        throw new ApiError(
          `HTTP ${response.status}: ${response.statusText}`,
          response.status
        )
      }

      return response.text()
    } catch (error) {
      clearTimeout(timeoutId)

      if (error instanceof ApiError) throw error

      if (error instanceof DOMException && error.name === 'AbortError') {
        throw new ApiError('Request timed out', 408)
      }

      throw new ApiError(
        error instanceof Error ? error.message : 'Unknown error',
        0
      )
    }
  }
}

// Default client instance
export const apiClient = new ApiClient()

// Export convenience functions using default client
export const api = {
  getHealth: (options?: RequestOptions) => apiClient.getHealth(options),
  getStatus: (options?: RequestOptions) => apiClient.getStatus(options),
  getStatsOverview: (options?: RequestOptions) => apiClient.getStatsOverview(options),
  getAllStats: (options?: RequestOptions) => apiClient.getAllStats(options),
  getCacheStats: (options?: RequestOptions) => apiClient.getCacheStats(options),
  getJournalStats: (options?: RequestOptions) => apiClient.getJournalStats(options),
  getTrimStats: (options?: RequestOptions) => apiClient.getTrimStats(options),
  getLockStats: (options?: RequestOptions) => apiClient.getLockStats(options),
  getSFBStats: (options?: RequestOptions) => apiClient.getSFBStats(options),
  getClusterStats: (options?: RequestOptions) => apiClient.getClusterStats(options),
  getClusterNodes: (options?: RequestOptions) => apiClient.getClusterNodes(options),
  getClusterLocks: (options?: RequestOptions) => apiClient.getClusterLocks(options),
  getMetrics: (options?: RequestOptions) => apiClient.getMetrics(options),
  // Snapshot endpoints
  listSnapshots: (options?: RequestOptions) => apiClient.listSnapshots(options),
  getSnapshot: (id: string, options?: RequestOptions) => apiClient.getSnapshot(id, options),
  createSnapshot: (request: CreateSnapshotRequest, options?: RequestOptions) => apiClient.createSnapshot(request, options),
  deleteSnapshot: (id: string, options?: RequestOptions) => apiClient.deleteSnapshot(id, options),
}
