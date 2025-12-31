package client

// HealthStatus represents the overall health state.
type HealthStatus string

const (
	HealthStatusOK        HealthStatus = "ok"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// ComponentHealth represents the health of a single component.
type ComponentHealth struct {
	Status  HealthStatus `json:"status"`
	Message string       `json:"message,omitempty"`
}

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	Status HealthStatus               `json:"status"`
	Checks map[string]ComponentHealth `json:"checks,omitempty"`
}

// MountState represents the current mount state.
type MountState string

const (
	MountStateUnmounted  MountState = "unmounted"
	MountStateMounting   MountState = "mounting"
	MountStateMounted    MountState = "mounted"
	MountStateUnmounting MountState = "unmounting"
	MountStateError      MountState = "error"
)

// CapacityInfo provides storage capacity information.
type CapacityInfo struct {
	TotalBytes     uint64  `json:"total_bytes"`
	UsedBytes      uint64  `json:"used_bytes"`
	FreeBytes      uint64  `json:"free_bytes"`
	UsedPercent    float64 `json:"used_percent"`
	TotalBlocks    uint64  `json:"total_blocks"`
	UsedBlocks     uint64  `json:"used_blocks"`
	FreeBlocks     uint64  `json:"free_blocks"`
	BlockSizeBytes uint64  `json:"block_size_bytes"`
}

// ClusterInfo provides cluster membership information.
type ClusterInfo struct {
	NodeID       string   `json:"node_id,omitempty"`
	PeerCount    int      `json:"peer_count"`
	Peers        []string `json:"peers,omitempty"`
	IsLeader     bool     `json:"is_leader,omitempty"`
	LeaderID     string   `json:"leader_id,omitempty"`
	ClusterState string   `json:"cluster_state,omitempty"`
}

// StatusResponse is returned by GET /api/v1/status.
type StatusResponse struct {
	Status        string        `json:"status"`
	Version       string        `json:"version"`
	GitCommit     string        `json:"git_commit,omitempty"`
	BuildTime     string        `json:"build_time,omitempty"`
	GoVersion     string        `json:"go_version,omitempty"`
	UptimeSeconds int64         `json:"uptime_seconds"`
	MountPath     string        `json:"mount_path,omitempty"`
	DevicePath    string        `json:"device_path,omitempty"`
	MountState    MountState    `json:"mount_state,omitempty"`
	ReadOnly      bool          `json:"read_only,omitempty"`
	Capacity      *CapacityInfo `json:"capacity,omitempty"`
	Cluster       *ClusterInfo  `json:"cluster,omitempty"`
}

// CacheHitRateInfo contains hit rate metrics for a cache type.
type CacheHitRateInfo struct {
	Size    int     `json:"size"`
	Hits    uint64  `json:"hits"`
	Misses  uint64  `json:"misses"`
	HitRate float64 `json:"hit_rate"`
}

// CacheStatsResponse is returned by GET /api/v1/stats/cache.
type CacheStatsResponse struct {
	InodeCache    CacheHitRateInfo `json:"inode_cache"`
	ExtentCache   CacheHitRateInfo `json:"extent_cache"`
	NegativeCache CacheHitRateInfo `json:"negative_cache"`
	LockHintCache CacheHitRateInfo `json:"lock_hint_cache,omitempty"`
}

// JournalStatsResponse is returned by GET /api/v1/stats/journal.
type JournalStatsResponse struct {
	State             string `json:"state"`
	HeadOffset        uint32 `json:"head_offset"`
	TailOffset        uint32 `json:"tail_offset"`
	AvailableSpace    uint32 `json:"available_space"`
	ActiveTxnCount    int    `json:"active_txn_count"`
	CommitCount       uint64 `json:"commit_count"`
	BytesWritten      uint64 `json:"bytes_written"`
	LastCheckpointSeq uint64 `json:"last_checkpoint_seq"`
	BatchSyncCount    uint64 `json:"batch_sync_count"`
}

// TrimStatsResponse is returned by GET /api/v1/stats/trim.
type TrimStatsResponse struct {
	TrimOperations    uint64 `json:"trim_operations"`
	TrimBytes         uint64 `json:"trim_bytes"`
	TrimBlocks        uint64 `json:"trim_blocks"`
	PendingTrimBlocks uint64 `json:"pending_trim_blocks,omitempty"`
	LastTrimAt        string `json:"last_trim_at,omitempty"`
	TrimErrors        uint64 `json:"trim_errors,omitempty"`
	TrimEnabled       bool   `json:"trim_enabled"`
	BatchSize         int    `json:"batch_size,omitempty"`
}

// LockStatsResponse is returned by GET /api/v1/stats/lock.
type LockStatsResponse struct {
	HintCacheHits    uint64  `json:"hint_cache_hits"`
	HintCacheMisses  uint64  `json:"hint_cache_misses"`
	HintCacheHitRate float64 `json:"hint_cache_hit_rate"`
	ActiveLocks      int     `json:"active_locks,omitempty"`
	PendingLocks     int     `json:"pending_locks,omitempty"`
}

// SFBStatsResponse is returned by GET /api/v1/stats/sfb.
type SFBStatsResponse struct {
	TotalBlocks        uint64  `json:"total_blocks"`
	AllocatedBlocks    uint64  `json:"allocated_blocks"`
	FreeBlocks         uint64  `json:"free_blocks"`
	AllocationPercent  float64 `json:"allocation_percent"`
	FragmentationRatio float64 `json:"fragmentation_ratio,omitempty"`
	LargestFreeExtent  uint64  `json:"largest_free_extent,omitempty"`
	AllocationsTotal   uint64  `json:"allocations_total"`
	DeallocationsTotal uint64  `json:"deallocations_total"`
	AllocationFailures uint64  `json:"allocation_failures,omitempty"`
}

// ClusterPeerInfo contains information about a cluster peer node.
type ClusterPeerInfo struct {
	NodeID   string  `json:"node_id"`
	Address  string  `json:"address"`
	State    string  `json:"state"`
	LastSeen string  `json:"last_seen,omitempty"`
	Latency  float64 `json:"latency_ms,omitempty"`
}

// ClusterInfoResponse is returned by GET /api/v1/stats/cluster.
type ClusterInfoResponse struct {
	NodeID          string            `json:"node_id"`
	NodeAddress     string            `json:"node_address,omitempty"`
	Role            string            `json:"role"`
	State           string            `json:"state"`
	ClusterSize     int               `json:"cluster_size"`
	HealthyNodes    int               `json:"healthy_nodes"`
	LeaderID        string            `json:"leader_id,omitempty"`
	LastHeartbeatAt string            `json:"last_heartbeat_at,omitempty"`
	JoinedAt        string            `json:"joined_at,omitempty"`
	Peers           []ClusterPeerInfo `json:"peers,omitempty"`
}

// ClusterNodeInfo provides detailed information about a cluster node.
type ClusterNodeInfo struct {
	NodeID        string  `json:"node_id"`
	Address       string  `json:"address"`
	Role          string  `json:"role"`
	State         string  `json:"state"`
	LastHeartbeat string  `json:"last_heartbeat,omitempty"`
	JoinedAt      string  `json:"joined_at,omitempty"`
	Version       string  `json:"version,omitempty"`
	LatencyMs     float64 `json:"latency_ms,omitempty"`
	IsSelf        bool    `json:"is_self"`
}

// ClusterNodesResponse is returned by GET /api/v1/cluster/nodes.
type ClusterNodesResponse struct {
	ClusterID    string            `json:"cluster_id,omitempty"`
	TotalNodes   int               `json:"total_nodes"`
	HealthyNodes int               `json:"healthy_nodes"`
	LeaderID     string            `json:"leader_id,omitempty"`
	Nodes        []ClusterNodeInfo `json:"nodes"`
}

// DistributedLockInfo provides information about an active distributed lock.
type DistributedLockInfo struct {
	LockID       string `json:"lock_id"`
	Resource     string `json:"resource"`
	OwnerNodeID  string `json:"owner_node_id"`
	OwnerProcess string `json:"owner_process,omitempty"`
	AcquiredAt   string `json:"acquired_at"`
	ExpiresAt    string `json:"expires_at,omitempty"`
	LockType     string `json:"lock_type"`
}

// ClusterLocksResponse is returned by GET /api/v1/cluster/locks.
type ClusterLocksResponse struct {
	ActiveLocks  int                   `json:"active_locks"`
	PendingLocks int                   `json:"pending_locks"`
	Locks        []DistributedLockInfo `json:"locks"`
}

// AllStatsResponse is returned by GET /api/v1/stats/all.
type AllStatsResponse struct {
	CollectedAt string                `json:"collected_at"`
	Cache       *CacheStatsResponse   `json:"cache,omitempty"`
	Journal     *JournalStatsResponse `json:"journal,omitempty"`
	Trim        *TrimStatsResponse    `json:"trim,omitempty"`
	Lock        *LockStatsResponse    `json:"lock,omitempty"`
	SFB         *SFBStatsResponse     `json:"sfb,omitempty"`
	Cluster     *ClusterInfoResponse  `json:"cluster,omitempty"`
}

// StatsOverviewResponse is returned by GET /api/v1/stats.
type StatsOverviewResponse struct {
	Cache   *CacheStatsResponse   `json:"cache,omitempty"`
	Journal *JournalStatsResponse `json:"journal,omitempty"`
}

// SnapshotInfo provides information about a snapshot.
type SnapshotInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	CreatedAt   string `json:"created_at"`
	SizeBytes   uint64 `json:"size_bytes"`
	Status      string `json:"status"`
	Description string `json:"description,omitempty"`
}

// SnapshotListResponse is returned by GET /api/v1/snapshots.
type SnapshotListResponse struct {
	Snapshots []SnapshotInfo `json:"snapshots"`
	Total     int            `json:"total"`
}

// SnapshotCreateRequest is sent to POST /api/v1/snapshots.
type SnapshotCreateRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// SnapshotCreateResponse is returned by POST /api/v1/snapshots.
type SnapshotCreateResponse struct {
	Snapshot SnapshotInfo `json:"snapshot"`
	Message  string       `json:"message"`
}

// SnapshotDeleteResponse is returned by DELETE /api/v1/snapshots/:id.
type SnapshotDeleteResponse struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

// AdminOperationResponse is returned by admin operations.
type AdminOperationResponse struct {
	Operation string `json:"operation"`
	Status    string `json:"status"`
	Message   string `json:"message"`
	Details   string `json:"details,omitempty"`
}

// FlushJournalRequest is sent to POST /api/v1/admin/flush-journal.
type FlushJournalRequest struct {
	Force bool `json:"force,omitempty"`
}

// ClearCacheRequest is sent to POST /api/v1/admin/clear-cache.
type ClearCacheRequest struct {
	CacheType string `json:"cache_type,omitempty"` // all, inode, extent, negative
}

// ForceTrimRequest is sent to POST /api/v1/admin/force-trim.
type ForceTrimRequest struct {
	Force bool `json:"force,omitempty"`
}
