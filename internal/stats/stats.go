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
