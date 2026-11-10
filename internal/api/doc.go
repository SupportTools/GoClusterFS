// Package api provides the HTTP JSON API server for GoClusterFS.
//
// The API server runs embedded within the mount process and exposes
// endpoints for health checks, status queries, statistics, and
// administrative operations. It is consumed by the gfsctl CLI tool
// and the web dashboard.
//
// # Endpoints
//
// The following endpoints are available:
//
//   - GET /health         - Health check (always returns 200)
//   - GET /api/v1/status  - Filesystem status overview
//   - GET /api/v1/stats/* - Detailed component statistics
//   - GET /metrics        - Prometheus metrics export
//
// # Authentication
//
// Authentication is via Bearer token when configured. Set the AuthToken
// in Config to enable authentication. When enabled, all endpoints except
// /health require a valid Authorization header:
//
//	Authorization: Bearer <token>
//
// # Configuration
//
// Use DefaultConfig() to get sensible defaults, then customize as needed:
//
//	cfg := api.DefaultConfig()
//	cfg.ListenAddr = ":9090"
//	cfg.AuthToken = "secret-token"
//	server, err := api.NewServer(cfg)
//
// # Lifecycle
//
// The server follows the standard Start/Stop pattern:
//
//	server, err := api.NewServer(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if err := server.Start(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	defer server.Stop()
package api
