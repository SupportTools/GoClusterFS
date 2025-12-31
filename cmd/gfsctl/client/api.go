package client

import (
	"context"
)

// Health fetches the health status from the API.
func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	var resp HealthResponse
	if err := c.Get(ctx, "/health", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Status fetches the filesystem status from the API.
func (c *Client) Status(ctx context.Context) (*StatusResponse, error) {
	var resp StatusResponse
	if err := c.Get(ctx, "/api/v1/status", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// StatsOverview fetches the stats overview from the API.
func (c *Client) StatsOverview(ctx context.Context) (*StatsOverviewResponse, error) {
	var resp StatsOverviewResponse
	if err := c.Get(ctx, "/api/v1/stats", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CacheStats fetches cache statistics from the API.
func (c *Client) CacheStats(ctx context.Context) (*CacheStatsResponse, error) {
	var resp CacheStatsResponse
	if err := c.Get(ctx, "/api/v1/stats/cache", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// JournalStats fetches journal statistics from the API.
func (c *Client) JournalStats(ctx context.Context) (*JournalStatsResponse, error) {
	var resp JournalStatsResponse
	if err := c.Get(ctx, "/api/v1/stats/journal", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// TrimStats fetches TRIM statistics from the API.
func (c *Client) TrimStats(ctx context.Context) (*TrimStatsResponse, error) {
	var resp TrimStatsResponse
	if err := c.Get(ctx, "/api/v1/stats/trim", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// LockStats fetches lock statistics from the API.
func (c *Client) LockStats(ctx context.Context) (*LockStatsResponse, error) {
	var resp LockStatsResponse
	if err := c.Get(ctx, "/api/v1/stats/lock", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SFBStats fetches SFB (Sub-File Block) statistics from the API.
func (c *Client) SFBStats(ctx context.Context) (*SFBStatsResponse, error) {
	var resp SFBStatsResponse
	if err := c.Get(ctx, "/api/v1/stats/sfb", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// AllStats fetches all statistics from the API.
func (c *Client) AllStats(ctx context.Context) (*AllStatsResponse, error) {
	var resp AllStatsResponse
	if err := c.Get(ctx, "/api/v1/stats/all", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ClusterStatus fetches cluster status from the API.
func (c *Client) ClusterStatus(ctx context.Context) (*ClusterInfoResponse, error) {
	var resp ClusterInfoResponse
	if err := c.Get(ctx, "/api/v1/cluster/status", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ClusterNodes fetches cluster node information from the API.
func (c *Client) ClusterNodes(ctx context.Context) (*ClusterNodesResponse, error) {
	var resp ClusterNodesResponse
	if err := c.Get(ctx, "/api/v1/cluster/nodes", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ClusterLocks fetches distributed lock information from the API.
func (c *Client) ClusterLocks(ctx context.Context) (*ClusterLocksResponse, error) {
	var resp ClusterLocksResponse
	if err := c.Get(ctx, "/api/v1/cluster/locks", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SnapshotList lists all snapshots.
func (c *Client) SnapshotList(ctx context.Context) (*SnapshotListResponse, error) {
	var resp SnapshotListResponse
	if err := c.Get(ctx, "/api/v1/snapshots", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SnapshotInfo gets information about a specific snapshot.
func (c *Client) SnapshotInfo(ctx context.Context, id string) (*SnapshotInfo, error) {
	var resp SnapshotInfo
	if err := c.Get(ctx, "/api/v1/snapshots/"+id, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SnapshotCreate creates a new snapshot.
func (c *Client) SnapshotCreate(ctx context.Context, req *SnapshotCreateRequest) (*SnapshotCreateResponse, error) {
	var resp SnapshotCreateResponse
	if err := c.Post(ctx, "/api/v1/snapshots", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SnapshotDelete deletes a snapshot.
func (c *Client) SnapshotDelete(ctx context.Context, id string) (*SnapshotDeleteResponse, error) {
	var resp SnapshotDeleteResponse
	if err := c.Delete(ctx, "/api/v1/snapshots/"+id, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// AdminFlushJournal flushes the journal to disk.
func (c *Client) AdminFlushJournal(ctx context.Context, req *FlushJournalRequest) (*AdminOperationResponse, error) {
	var resp AdminOperationResponse
	if err := c.Post(ctx, "/api/v1/admin/flush-journal", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// AdminClearCache clears the specified cache.
func (c *Client) AdminClearCache(ctx context.Context, req *ClearCacheRequest) (*AdminOperationResponse, error) {
	var resp AdminOperationResponse
	if err := c.Post(ctx, "/api/v1/admin/clear-cache", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// AdminForceTrim forces a TRIM operation.
func (c *Client) AdminForceTrim(ctx context.Context, req *ForceTrimRequest) (*AdminOperationResponse, error) {
	var resp AdminOperationResponse
	if err := c.Post(ctx, "/api/v1/admin/force-trim", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
