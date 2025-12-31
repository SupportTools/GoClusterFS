package api

import (
	"encoding/json"
	"net/http"
)

// HealthStatus represents the overall health state.
type HealthStatus string

const (
	HealthStatusOK       HealthStatus = "ok"
	HealthStatusDegraded HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// ComponentHealth represents the health of a single component.
type ComponentHealth struct {
	Status  HealthStatus `json:"status"`
	Message string       `json:"message,omitempty"`
}

// HealthResponse is returned by GET /health.
// Provides overall system health and individual component checks.
type HealthResponse struct {
	Status HealthStatus               `json:"status"`
	Checks map[string]ComponentHealth `json:"checks,omitempty"`
}

// ErrorResponse is returned for API errors.
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Details string `json:"details,omitempty"`
}

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

// MountState represents the current mount state.
type MountState string

const (
	MountStateUnmounted  MountState = "unmounted"
	MountStateMounting   MountState = "mounting"
	MountStateMounted    MountState = "mounted"
	MountStateUnmounting MountState = "unmounting"
	MountStateError      MountState = "error"
)

// StatusResponse is returned by GET /api/v1/status.
// Provides comprehensive system status information.
type StatusResponse struct {
	Status       string        `json:"status"`
	Version      string        `json:"version"`
	GitCommit    string        `json:"git_commit,omitempty"`
	BuildTime    string        `json:"build_time,omitempty"`
	GoVersion    string        `json:"go_version,omitempty"`
	UptimeSeconds int64        `json:"uptime_seconds"`
	MountPath    string        `json:"mount_path,omitempty"`
	DevicePath   string        `json:"device_path,omitempty"`
	MountState   MountState    `json:"mount_state,omitempty"`
	ReadOnly     bool          `json:"read_only,omitempty"`
	Capacity     *CapacityInfo `json:"capacity,omitempty"`
	Cluster      *ClusterInfo  `json:"cluster,omitempty"`
}

// CacheHitRateInfo contains hit rate metrics for a cache type.
type CacheHitRateInfo struct {
	Size     int     `json:"size"`
	Hits     uint64  `json:"hits"`
	Misses   uint64  `json:"misses"`
	HitRate  float64 `json:"hit_rate"`
}

// CacheStatsResponse is returned by GET /api/v1/stats/cache.
// Matches internal/cache.Stats structure.
type CacheStatsResponse struct {
	InodeCache    CacheHitRateInfo `json:"inode_cache"`
	ExtentCache   CacheHitRateInfo `json:"extent_cache"`
	NegativeCache CacheHitRateInfo `json:"negative_cache"`
	LockHintCache CacheHitRateInfo `json:"lock_hint_cache,omitempty"`
}

// JournalStatsResponse is returned by GET /api/v1/stats/journal.
// Matches internal/journal.JournalStats structure.
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

// IOStatsResponse contains I/O operation statistics.
type IOStatsResponse struct {
	ReadOps       uint64  `json:"read_ops"`
	WriteOps      uint64  `json:"write_ops"`
	BytesRead     uint64  `json:"bytes_read"`
	BytesWritten  uint64  `json:"bytes_written"`
	AvgReadLatUs  float64 `json:"avg_read_latency_us"`
	AvgWriteLatUs float64 `json:"avg_write_latency_us"`
}

// MetricsStatsResponse is returned by GET /api/v1/stats/metrics.
type MetricsStatsResponse struct {
	IO        *IOStatsResponse `json:"io,omitempty"`
	StartTime int64            `json:"start_time_unix"`
	Uptime    int64            `json:"uptime_seconds"`
}

// StatsOverviewResponse is returned by GET /api/v1/stats.
// Provides a summary of all component stats.
type StatsOverviewResponse struct {
	Cache   *CacheStatsResponse   `json:"cache,omitempty"`
	Journal *JournalStatsResponse `json:"journal,omitempty"`
	IO      *IOStatsResponse      `json:"io,omitempty"`
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		json.NewEncoder(w).Encode(v)
	}
}

// writeError writes an error response with the given status code.
func writeError(w http.ResponseWriter, status int, err string, details string) {
	writeJSON(w, status, ErrorResponse{
		Error:   err,
		Code:    status,
		Details: details,
	})
}
