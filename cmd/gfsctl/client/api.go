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
