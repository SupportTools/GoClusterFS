package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// Health fetches the health status from the API. An unhealthy mount answers
// 503 with the same body, which Health returns without an error.
func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	body, status, err := c.send(ctx, http.MethodGet, "/health", nil)
	if err != nil {
		return nil, err
	}
	var resp HealthResponse
	if (status == http.StatusOK || status == http.StatusServiceUnavailable) && json.Unmarshal(body, &resp) == nil && resp.Status != "" {
		return &resp, nil
	}
	if status < 400 {
		return nil, fmt.Errorf("decode health response: %q", body)
	}
	return nil, apiError(status, body)
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

// ClusterInfo returns cluster membership from /api/v1/stats/cluster.
func (c *Client) ClusterInfo(ctx context.Context) (*ClusterInfoResponse, error) {
	var resp ClusterInfoResponse
	if err := c.Get(ctx, "/api/v1/stats/cluster", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Snapshots returns every snapshot, orphans included, from /api/v1/snapshots.
// The API answers only with the token.
func (c *Client) Snapshots(ctx context.Context) (*SnapshotListResponse, error) {
	var resp SnapshotListResponse
	if err := c.Get(ctx, "/api/v1/snapshots", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Quota returns the project quota of the directory at path in the mount, from GET /api/v1/quotas. The API answers
// only with the token.
func (c *Client) Quota(ctx context.Context, path string) (*QuotaInfo, error) {
	var resp QuotaInfo
	if err := c.Get(ctx, "/api/v1/quotas?path="+url.QueryEscape(path), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SetQuota sets the project quota of the directory at path through PUT /api/v1/quotas; a nil limit is left out, which
// the API takes as 0, no limit.
func (c *Client) SetQuota(ctx context.Context, path string, bytes, inodes *uint64) (*QuotaInfo, error) {
	var resp QuotaInfo
	if err := c.Put(ctx, "/api/v1/quotas", QuotaRequest{Path: path, Bytes: bytes, Inodes: inodes}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Metrics returns the Prometheus text from /metrics.
func (c *Client) Metrics(ctx context.Context) ([]byte, error) {
	return c.GetRaw(ctx, "/metrics")
}
