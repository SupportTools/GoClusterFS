package api

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// Server is the HTTP API server for GoClusterFS.
type Server struct {
	mu     sync.RWMutex
	config Config

	// HTTP server components
	server   *http.Server
	router   *http.ServeMux
	listener net.Listener

	// Health checkers for component health reporting
	healthCheckers []HealthChecker

	// Status provider for filesystem status (optional)
	statusProvider StatusProvider

	// Stats provider for statistics collection (optional)
	statsProvider StatsProvider

	// Rate limiter (optional)
	rateLimiter *RateLimiter

	// Prometheus metrics handler (lazily initialized)
	metricsHandler http.Handler

	// Lifecycle state
	started   bool
	startTime time.Time
	stopOnce  sync.Once
	stopErr   error
}

// NewServer creates a new API server with the given configuration.
// The server is not started until Start is called.
func NewServer(cfg Config) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	s := &Server{
		config: cfg,
		router: http.NewServeMux(),
	}

	// Create rate limiter if enabled
	if cfg.RateLimitEnabled {
		s.rateLimiter = NewRateLimiter(cfg.RateLimitRequestsPerSecond, cfg.RateLimitBurst)
	}

	// Register routes
	s.registerRoutes()

	// Build middleware chain (order: rate limit -> logging -> loopback Host check -> CORS -> auth)
	middlewares := []Middleware{RateLimitMiddleware(s.rateLimiter), LoggingMiddleware}
	if cfg.AuthToken == "" && cfg.LoopbackOnly() {
		middlewares = append(middlewares, LoopbackHostMiddleware)
	}
	middlewares = append(middlewares,
		CORSMiddleware(cfg.CORSOrigins),
		AuthMiddleware(cfg.AuthToken, "/health", "/metrics"),
	)
	handler := Chain(s.router, middlewares...)

	// Create HTTP server
	s.server = &http.Server{
		Handler:      handler,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	return s, nil
}

// Start starts the API server and begins accepting connections.
// The server runs in a background goroutine and returns immediately.
// Use Stop to shut down the server gracefully.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return ErrServerAlreadyStarted
	}

	// Create listener
	ln, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.ListenAddr, err)
	}
	s.listener = ln

	// Mark as started
	s.started = true
	s.startTime = time.Now()

	// Start serving in background
	go func() {
		log.Printf("api: server listening on %s", ln.Addr())
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("api: server error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the API server.
// It waits for active connections to complete up to the configured
// ShutdownTimeout before forcefully closing them.
func (s *Server) Stop() error {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		if !s.started {
			s.mu.Unlock()
			s.stopErr = ErrServerNotStarted
			return
		}
		timeout := s.config.ShutdownTimeout
		s.mu.Unlock()

		log.Printf("api: shutting down server...")

		// Create shutdown context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// Attempt graceful shutdown
		if err := s.server.Shutdown(ctx); err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				s.stopErr = ErrShutdownTimeout
			} else {
				s.stopErr = fmt.Errorf("shutdown error: %w", err)
			}
			return
		}

		log.Printf("api: server stopped")
	})

	return s.stopErr
}

// Addr returns the address the server is listening on.
// Returns an empty string if the server has not been started.
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Started returns true if the server has been started.
func (s *Server) Started() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.started
}

// Uptime returns the duration since the server was started.
// Returns 0 if the server has not been started.
func (s *Server) Uptime() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started {
		return 0
	}
	return time.Since(s.startTime)
}

// RegisterHealthChecker adds a health checker to the server.
// Health checkers are called when the /health endpoint is accessed.
func (s *Server) RegisterHealthChecker(checker HealthChecker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthCheckers = append(s.healthCheckers, checker)
}

// GetHealthCheckers returns a copy of the registered health checkers.
func (s *Server) GetHealthCheckers() []HealthChecker {
	s.mu.RLock()
	defer s.mu.RUnlock()
	checkers := make([]HealthChecker, len(s.healthCheckers))
	copy(checkers, s.healthCheckers)
	return checkers
}

// StatusProvider provides filesystem status information.
type StatusProvider interface {
	// MountPath returns the current mount path.
	MountPath() string

	// DevicePath returns the underlying device path.
	DevicePath() string

	// MountState returns the current mount state.
	MountState() MountState

	// Capacity returns storage capacity information.
	Capacity() *CapacityInfo
}

// SetStatusProvider sets the status provider for filesystem status.
func (s *Server) SetStatusProvider(provider StatusProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusProvider = provider
}

// GetStatusProvider returns the current status provider.
func (s *Server) GetStatusProvider() StatusProvider {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.statusProvider
}

// StatsProvider provides statistics from the filesystem components.
type StatsProvider interface {
	// CacheStats returns cache statistics, or nil if not available.
	CacheStats() *CacheStatsResponse

	// JournalStats returns journal statistics, or nil if not available.
	JournalStats() *JournalStatsResponse

	// TrimStats returns TRIM/discard statistics, or nil if not available.
	TrimStats() *TrimStatsResponse

	// LockStats returns lock statistics, or nil if not available.
	LockStats() *LockStatsResponse

	// SFBStats returns Sparse File Bitmap statistics, or nil if not available.
	SFBStats() *SFBStatsResponse

	// ClusterInfo returns cluster information, or nil if not available.
	ClusterInfo() *ClusterInfoResponse

	// ClusterNodes returns detailed node information, or nil if not available.
	ClusterNodes() *ClusterNodesResponse

	// ClusterLocks returns active distributed locks, or nil if not available.
	ClusterLocks() *ClusterLocksResponse

	// AllStats returns a combined snapshot of all statistics.
	AllStats() *AllStatsResponse
}

// SetStatsProvider sets the stats provider for statistics collection.
func (s *Server) SetStatsProvider(provider StatsProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statsProvider = provider
}

// GetStatsProvider returns the current stats provider.
func (s *Server) GetStatsProvider() StatsProvider {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.statsProvider
}

// GetMetricsHandler returns the Prometheus metrics HTTP handler.
// The handler is lazily initialized on first call.
func (s *Server) GetMetricsHandler() http.Handler {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metricsHandler == nil {
		s.metricsHandler = s.MetricsHandler()
	}
	return s.metricsHandler
}
