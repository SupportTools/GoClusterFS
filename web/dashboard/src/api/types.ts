// Mirrors the Go structs in internal/api/types.go and internal/api/writes.go.

// Health status values
export type HealthStatus = 'ok' | 'degraded' | 'unhealthy'

// Mount state values
export type MountState = 'unmounted' | 'mounting' | 'mounted' | 'unmounting' | 'error'

// Component health for individual system components
export interface ComponentHealth {
  status: HealthStatus
  message?: string
}

// GET /health response
export interface HealthResponse {
  status: HealthStatus
  checks?: Record<string, ComponentHealth>
}

// API error response
export interface ErrorResponse {
  error: string
  code: number
  details?: string
}

// Storage capacity information
export interface CapacityInfo {
  total_bytes: number
  used_bytes: number
  free_bytes: number
  used_percent: number
  total_blocks: number
  used_blocks: number
  free_blocks: number
  block_size_bytes: number
}

// Cluster membership information
export interface ClusterInfo {
  node_id?: string
  peer_count: number
  peers?: string[]
  is_leader?: boolean
  leader_id?: string
  cluster_state?: string
}

// GET /api/v1/status response
export interface StatusResponse {
  status: string
  version: string
  git_commit?: string
  build_time?: string
  go_version?: string
  uptime_seconds: number
  mount_path?: string
  device_path?: string
  mount_state?: MountState
  read_only?: boolean
  capacity?: CapacityInfo
  cluster?: ClusterInfo
}

// Cache hit rate metrics
export interface CacheHitRateInfo {
  size: number
  hits: number
  misses: number
  hit_rate: number
}

// GET /api/v1/stats/cache response
export interface CacheStatsResponse {
  inode_cache: CacheHitRateInfo
  extent_cache: CacheHitRateInfo
  negative_cache: CacheHitRateInfo
  lock_hint_cache?: CacheHitRateInfo
}

// GET /api/v1/stats/journal response; state is clean, dirty, recovering or corrupt
export interface JournalStatsResponse {
  state: string
  head_offset: number
  tail_offset: number
  available_space: number
  active_txn_count: number
  commit_count: number
  bytes_written: number
  last_checkpoint_seq: number
  batch_sync_count: number
}

// I/O operation statistics
export interface IOStatsResponse {
  read_ops: number
  write_ops: number
  bytes_read: number
  bytes_written: number
  avg_read_latency_us: number
  avg_write_latency_us: number
}

// GET /api/v1/stats/metrics response
export interface MetricsStatsResponse {
  io?: IOStatsResponse
  start_time_unix: number
  uptime_seconds: number
}

// GET /api/v1/stats/trim response
export interface TrimStatsResponse {
  trim_operations: number
  trim_bytes: number
  trim_blocks: number
  pending_trim_blocks?: number
  last_trim_at?: string
  trim_errors?: number
  trim_enabled: boolean
  batch_size?: number
}

// GET /api/v1/stats/lock response
export interface LockStatsResponse {
  hint_cache_hits: number
  hint_cache_misses: number
  hint_cache_hit_rate: number
  active_locks?: number
  pending_locks?: number
}

// GET /api/v1/stats/sfb response
export interface SFBStatsResponse {
  total_blocks: number
  allocated_blocks: number
  free_blocks: number
  allocation_percent: number
  fragmentation_ratio?: number
  largest_free_extent?: number
  allocations_total: number
  deallocations_total: number
  allocation_failures?: number
}

// Cluster peer node information
export interface ClusterPeerInfo {
  node_id: string
  address: string
  state: string
  last_seen?: string
  latency_ms?: number
}

// GET /api/v1/stats/cluster response
export interface ClusterInfoResponse {
  node_id: string
  node_address?: string
  role: string
  state: string
  cluster_size: number
  healthy_nodes: number
  leader_id?: string
  last_heartbeat_at?: string
  joined_at?: string
  peers?: ClusterPeerInfo[]
}

// GET /api/v1/stats/all response
export interface AllStatsResponse {
  collected_at: string
  cache?: CacheStatsResponse
  journal?: JournalStatsResponse
  trim?: TrimStatsResponse
  lock?: LockStatsResponse
  sfb?: SFBStatsResponse
  cluster?: ClusterInfoResponse
}

// GET /api/v1/stats response (overview)
export interface StatsOverviewResponse {
  cache?: CacheStatsResponse
  journal?: JournalStatsResponse
  io?: IOStatsResponse
}

// ============================================================
// Snapshot, clone and admin types (internal/api/writes.go)
// ============================================================

// Registry states; "deleting" lasts until the snapshot worker frees the blocks
export type SnapshotState = 'creating' | 'active' | 'deleting' | 'invalid'

export interface SnapshotInfo {
  id: number
  name: string
  inode: number
  state: SnapshotState
  size_bytes: number
  blocks: number
  created_at?: string
  /** Its file is gone, or it predates the record of which file it belongs to; only the full list shows it */
  orphan?: boolean
}

// GET /api/v1/snapshots[?path=]
export interface SnapshotListResponse {
  snapshots: SnapshotInfo[]
  total: number
}

// POST /api/v1/snapshots
export interface SnapshotRequest {
  path: string
  name: string
}

// POST /api/v1/clones and POST /api/v1/snapshots/{name}/clone
export interface CloneRequest {
  path: string
  dest: string
}

export interface WriteResponse {
  action: string
  path: string
  name?: string
  dest?: string
}

export type AdminOperation = 'flush-journal' | 'force-trim' | 'clear-cache'

// POST /api/v1/admin/{operation}; acts on this node only
export interface AdminResponse {
  operation: string
  message: string
}
