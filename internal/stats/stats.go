package stats

import "time"

// Config holds configuration for the statistics collector.
type Config struct {
	// CacheTTL is how long collected statistics are cached before re-collection.
	// Shorter values provide fresher data but may impact performance.
	// Default: 1 second
	CacheTTL time.Duration

	// EnableComponentStats controls whether per-component stats are collected.
	// Disable for minimal overhead in production.
	// Default: true
	EnableComponentStats bool
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		CacheTTL:             1 * time.Second,
		EnableComponentStats: true,
	}
}

// HighFrequencyConfig returns a Config optimized for frequent collection.
// Use when stats need to be very fresh (e.g., real-time dashboards).
func HighFrequencyConfig() Config {
	return Config{
		CacheTTL:             100 * time.Millisecond,
		EnableComponentStats: true,
	}
}

// LowOverheadConfig returns a Config optimized for minimal overhead.
// Use in production when stats are infrequently accessed.
func LowOverheadConfig() Config {
	return Config{
		CacheTTL:             10 * time.Second,
		EnableComponentStats: false,
	}
}

// Validate checks that the configuration is valid.
func (c *Config) Validate() error {
	if c.CacheTTL < 0 {
		return ErrInvalidTTL
	}
	return nil
}

// Snapshot contains a point-in-time snapshot of all system statistics.
// All fields are optional and may be nil if the corresponding component
// is not available or statistics collection is disabled.
type Snapshot struct {
	// CollectedAt is when this snapshot was collected.
	CollectedAt time.Time `json:"collected_at"`

	// Cache contains cache hit/miss statistics.
	Cache *CacheStats `json:"cache,omitempty"`

	// Journal contains journal transaction statistics.
	Journal *JournalStats `json:"journal,omitempty"`

	// Filesystem contains filesystem capacity and file count statistics.
	Filesystem *FilesystemStats `json:"filesystem,omitempty"`

	// Lock contains lock contention statistics.
	Lock *LockStats `json:"lock,omitempty"`

	// Trim contains TRIM/discard operation statistics.
	Trim *TrimStats `json:"trim,omitempty"`

	// SFB contains Sparse File Bitmap (block allocation) statistics.
	SFB *SFBStats `json:"sfb,omitempty"`

	// Cluster contains cluster membership and node information.
	Cluster *ClusterInfo `json:"cluster,omitempty"`
}

// CacheStats contains aggregated cache statistics.
type CacheStats struct {
	// Inode cache
	InodeCacheSize   int     `json:"inode_cache_size"`
	InodeCacheHits   uint64  `json:"inode_cache_hits"`
	InodeCacheMisses uint64  `json:"inode_cache_misses"`
	InodeHitRate     float64 `json:"inode_hit_rate"`

	// Extent cache
	ExtentCacheSize   int     `json:"extent_cache_size"`
	ExtentCacheHits   uint64  `json:"extent_cache_hits"`
	ExtentCacheMisses uint64  `json:"extent_cache_misses"`
	ExtentHitRate     float64 `json:"extent_hit_rate"`

	// Negative cache (caches "not found" results)
	NegativeCacheSize int    `json:"negative_cache_size"`
	NegativeHits      uint64 `json:"negative_hits"`
	NegativeMisses    uint64 `json:"negative_misses"`

	// Broadcast/network invalidation stats
	BroadcastEnabled      bool   `json:"broadcast_enabled"`
	InvalidationsSent     uint64 `json:"invalidations_sent,omitempty"`
	InvalidationsReceived uint64 `json:"invalidations_received,omitempty"`
	BroadcastDuplicates   uint64 `json:"broadcast_duplicates,omitempty"`
	MulticastEnabled      bool   `json:"multicast_enabled,omitempty"`
	PeerCount             int    `json:"peer_count,omitempty"`
}

// JournalStats contains journal transaction statistics.
type JournalStats struct {
	// State is the current journal state (clean, dirty, recovering).
	State string `json:"state"`

	// HeadOffset is the current write position in the journal.
	HeadOffset uint32 `json:"head_offset"`

	// TailOffset is the oldest valid entry position.
	TailOffset uint32 `json:"tail_offset"`

	// AvailableSpace is bytes available for new entries.
	AvailableSpace uint32 `json:"available_space"`

	// ActiveTxnCount is the number of in-flight transactions.
	ActiveTxnCount int `json:"active_txn_count"`

	// CommitCount is the total number of committed transactions.
	CommitCount uint64 `json:"commit_count"`

	// BytesWritten is the total bytes written to the journal.
	BytesWritten uint64 `json:"bytes_written"`

	// LastCheckpointSeq is the sequence number of the last checkpoint.
	LastCheckpointSeq uint64 `json:"last_checkpoint_seq"`

	// BatchSyncCount is the number of batch syncs (for CommitBatch mode).
	BatchSyncCount uint64 `json:"batch_sync_count,omitempty"`
}

// FilesystemStats contains filesystem capacity and usage statistics.
type FilesystemStats struct {
	// TotalBytes is the total filesystem capacity in bytes.
	TotalBytes uint64 `json:"total_bytes"`

	// UsedBytes is the bytes currently in use.
	UsedBytes uint64 `json:"used_bytes"`

	// FreeBytes is the bytes available for new data.
	FreeBytes uint64 `json:"free_bytes"`

	// UsedPercent is the percentage of capacity in use.
	UsedPercent float64 `json:"used_percent"`

	// TotalBlocks is the total number of blocks.
	TotalBlocks uint64 `json:"total_blocks"`

	// UsedBlocks is the number of blocks in use.
	UsedBlocks uint64 `json:"used_blocks"`

	// FreeBlocks is the number of free blocks.
	FreeBlocks uint64 `json:"free_blocks"`

	// FileCount is the total number of files.
	FileCount uint64 `json:"file_count,omitempty"`

	// DirectoryCount is the total number of directories.
	DirectoryCount uint64 `json:"directory_count,omitempty"`

	// SnapshotCount is the number of active snapshots.
	SnapshotCount int `json:"snapshot_count,omitempty"`
}

// LockStats contains lock contention and hint cache statistics.
type LockStats struct {
	// HintCacheHits is the number of lock hint cache hits.
	HintCacheHits uint64 `json:"hint_cache_hits"`

	// HintCacheMisses is the number of lock hint cache misses.
	HintCacheMisses uint64 `json:"hint_cache_misses"`

	// HintCacheHitRate is the hit rate as a ratio (0.0-1.0).
	HintCacheHitRate float64 `json:"hint_cache_hit_rate"`

	// ActiveLocks is the number of currently held locks.
	ActiveLocks int `json:"active_locks,omitempty"`

	// PendingLocks is the number of locks waiting to be acquired.
	PendingLocks int `json:"pending_locks,omitempty"`
}

// TrimStats contains TRIM/discard operation statistics.
type TrimStats struct {
	// TrimOperations is the total number of TRIM operations performed.
	TrimOperations uint64 `json:"trim_operations"`

	// TrimBytes is the total bytes trimmed.
	TrimBytes uint64 `json:"trim_bytes"`

	// TrimBlocks is the total blocks trimmed.
	TrimBlocks uint64 `json:"trim_blocks"`

	// PendingTrimBlocks is blocks queued for TRIM.
	PendingTrimBlocks uint64 `json:"pending_trim_blocks,omitempty"`

	// LastTrimAt is when the last TRIM operation occurred.
	LastTrimAt time.Time `json:"last_trim_at,omitempty"`

	// TrimErrors is the count of failed TRIM operations.
	TrimErrors uint64 `json:"trim_errors,omitempty"`

	// TrimEnabled indicates if TRIM is enabled.
	TrimEnabled bool `json:"trim_enabled"`

	// BatchSize is the current batch size for TRIM operations.
	BatchSize int `json:"batch_size,omitempty"`
}

// SFBStats contains Sparse File Bitmap (block allocation) statistics.
type SFBStats struct {
	// TotalBlocks is the total number of blocks in the bitmap.
	TotalBlocks uint64 `json:"total_blocks"`

	// AllocatedBlocks is the number of blocks currently allocated.
	AllocatedBlocks uint64 `json:"allocated_blocks"`

	// FreeBlocks is the number of blocks available for allocation.
	FreeBlocks uint64 `json:"free_blocks"`

	// AllocationPercent is the percentage of blocks allocated (0.0-100.0).
	AllocationPercent float64 `json:"allocation_percent"`

	// FragmentationRatio estimates fragmentation (0.0-1.0, higher = more fragmented).
	FragmentationRatio float64 `json:"fragmentation_ratio,omitempty"`

	// LargestFreeExtent is the size of the largest contiguous free region in blocks.
	LargestFreeExtent uint64 `json:"largest_free_extent,omitempty"`

	// AllocationsTotal is the total number of allocation operations.
	AllocationsTotal uint64 `json:"allocations_total"`

	// DeallocationsTotal is the total number of deallocation operations.
	DeallocationsTotal uint64 `json:"deallocations_total"`

	// AllocationFailures is the count of failed allocations (e.g., no space).
	AllocationFailures uint64 `json:"allocation_failures,omitempty"`
}

// ClusterInfo contains cluster node and membership information.
type ClusterInfo struct {
	// NodeID is this node's unique identifier.
	NodeID string `json:"node_id"`

	// NodeAddress is this node's network address.
	NodeAddress string `json:"node_address,omitempty"`

	// Role is this node's role (e.g., "primary", "replica", "standalone").
	Role string `json:"role"`

	// State is this node's current state (e.g., "active", "syncing", "offline").
	State string `json:"state"`

	// ClusterSize is the total number of nodes in the cluster.
	ClusterSize int `json:"cluster_size"`

	// HealthyNodes is the number of nodes currently healthy.
	HealthyNodes int `json:"healthy_nodes"`

	// LeaderID is the current cluster leader's ID (if applicable).
	LeaderID string `json:"leader_id,omitempty"`

	// LastHeartbeatAt is when the last heartbeat was received/sent.
	LastHeartbeatAt time.Time `json:"last_heartbeat_at,omitempty"`

	// JoinedAt is when this node joined the cluster.
	JoinedAt time.Time `json:"joined_at,omitempty"`

	// Peers contains information about peer nodes.
	Peers []PeerInfo `json:"peers,omitempty"`
}

// PeerInfo contains information about a cluster peer node.
type PeerInfo struct {
	// NodeID is the peer's unique identifier.
	NodeID string `json:"node_id"`

	// Address is the peer's network address.
	Address string `json:"address"`

	// State is the peer's current state.
	State string `json:"state"`

	// LastSeen is when the peer was last seen.
	LastSeen time.Time `json:"last_seen,omitempty"`

	// Latency is the round-trip latency to this peer in milliseconds.
	Latency float64 `json:"latency_ms,omitempty"`
}

// CacheStatsProvider is implemented by components that provide cache statistics.
type CacheStatsProvider interface {
	// CacheStats returns current cache statistics.
	// Returns nil if cache statistics are not available.
	CacheStats() *CacheStats
}

// JournalStatsProvider is implemented by components that provide journal statistics.
type JournalStatsProvider interface {
	// JournalStats returns current journal statistics.
	// Returns nil if journal statistics are not available.
	JournalStats() *JournalStats
}

// FilesystemStatsProvider is implemented by components that provide filesystem statistics.
type FilesystemStatsProvider interface {
	// FilesystemStats returns current filesystem statistics.
	// Returns nil if filesystem statistics are not available.
	FilesystemStats() *FilesystemStats
}

// LockStatsProvider is implemented by components that provide lock statistics.
type LockStatsProvider interface {
	// LockStats returns current lock statistics.
	// Returns nil if lock statistics are not available.
	LockStats() *LockStats
}

// TrimStatsProvider is implemented by components that provide TRIM statistics.
type TrimStatsProvider interface {
	// TrimStats returns current TRIM/discard statistics.
	// Returns nil if TRIM statistics are not available.
	TrimStats() *TrimStats
}

// SFBStatsProvider is implemented by components that provide block allocation statistics.
type SFBStatsProvider interface {
	// SFBStats returns current Sparse File Bitmap statistics.
	// Returns nil if SFB statistics are not available.
	SFBStats() *SFBStats
}

// ClusterInfoProvider is implemented by components that provide cluster information.
type ClusterInfoProvider interface {
	// ClusterInfo returns current cluster membership and node information.
	// Returns nil if cluster information is not available.
	ClusterInfo() *ClusterInfo
}
