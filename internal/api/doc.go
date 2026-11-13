// Package api provides the HTTP JSON API server for GoClusterFS.
//
// The API server runs embedded within the mount process. It serves health
// checks, status and statistics, and, when a token is set, endpoints that
// create, clone and delete snapshots, clone files and run admin tasks.
//
// # Endpoints
//
// The following endpoints are available:
//
//   - GET /health              - Health check (503 when a checker reports unhealthy)
//   - GET /api/v1/status       - Filesystem status overview
//   - GET /api/v1/stats/*      - Detailed component statistics
//   - GET /metrics             - Prometheus metrics export
//   - /api/v1/snapshots/*      - List, create, clone and delete snapshots
//   - POST /api/v1/clones      - Clone a file
//   - POST /api/v1/admin/*     - Flush the journal, force TRIM, clear caches
//   - GET /ui/                 - Web dashboard
//
// # Authentication
//
// Authentication is via Bearer token when configured. Set the AuthToken
// in Config to enable authentication. When enabled, all endpoints except
// /health, /metrics and /ui/ require a valid Authorization header:
//
//	Authorization: Bearer <token>
//
// Without a token, Config.CheckExposure refuses non-loopback addresses and
// CORS "*", the server rejects requests whose Host is not a loopback name,
// and the snapshot, clone and admin endpoints answer 403.
//
// # Configuration
//
// Use DefaultConfig() to get sensible defaults, then customize as needed:
//
//	cfg := api.DefaultConfig()
//	cfg.ListenAddr = "127.0.0.1:9090"
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
