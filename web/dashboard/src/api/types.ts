/**
 * TypeScript interfaces matching GoClusterFS API response types.
 * These types mirror the Go structs in internal/api/types.go
 */

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

// GET /api/v1/stats/journal response
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

// Cluster node detailed information
export interface ClusterNodeInfo {
  node_id: string
  address: string
  role: string
  state: string
  last_heartbeat?: string
  joined_at?: string
  version?: string
  latency_ms?: number
  is_self: boolean
}

// GET /api/v1/cluster/nodes response
export interface ClusterNodesResponse {
  cluster_id?: string
  total_nodes: number
  healthy_nodes: number
  leader_id?: string
  nodes: ClusterNodeInfo[]
}

// Distributed lock information
export interface DistributedLockInfo {
  lock_id: string
  resource: string
  owner_node_id: string
  owner_process?: string
  acquired_at: string
  expires_at?: string
  lock_type: string
}

// GET /api/v1/cluster/locks response
export interface ClusterLocksResponse {
  active_locks: number
  pending_locks: number
  locks: DistributedLockInfo[]
}

// GET /api/v1/stats response (overview)
export interface StatsOverviewResponse {
  cache?: CacheStatsResponse
  journal?: JournalStatsResponse
  io?: IOStatsResponse
}

// ============================================================
// Snapshot Types
// ============================================================

// Snapshot status values
export type SnapshotStatus = 'creating' | 'active' | 'deleting' | 'error' | 'archived'

// Individual snapshot information
export interface SnapshotInfo {
  snapshot_id: string
  name: string
  description?: string
  status: SnapshotStatus
  created_at: string
  created_by?: string
  size_bytes: number
  parent_snapshot_id?: string
  metadata?: Record<string, string>
  // Retention info
  retention_policy?: string
  expires_at?: string
  // Progress info (for creating/deleting states)
  progress_percent?: number
  error_message?: string
}

// GET /api/v1/snapshots response
export interface SnapshotsListResponse {
  snapshots: SnapshotInfo[]
  total_count: number
  total_size_bytes: number
}

// POST /api/v1/snapshots request
export interface CreateSnapshotRequest {
  name: string
  description?: string
  metadata?: Record<string, string>
  retention_policy?: string
}

// POST /api/v1/snapshots response
export interface CreateSnapshotResponse {
  snapshot: SnapshotInfo
}

// DELETE /api/v1/snapshots/:id response
export interface DeleteSnapshotResponse {
  success: boolean
  message?: string
}
