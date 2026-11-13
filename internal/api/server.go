package api

import (
	"context"
	"errors"
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

	// Services behind the write endpoints (optional)
	fileService  FileService
	adminService AdminService

	// Requests in flight; Stop waits for them so the mount outlives them.
	requests       sync.WaitGroup
	stopping       bool
	closed         bool
	cancelRequests context.CancelFunc
	served         chan struct{}

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

	// Build middleware chain (order: rate limit -> logging -> loopback Host check -> CORS -> auth).
	// Probes and scrapers on loopback share one bucket, so /health and /metrics skip the limiter.
	// The dashboard's static files hold no data, and a browser can't send a token when it loads them.
	exempt := []string{"/health", "/metrics"}
	if cfg.Dashboard != nil {
		exempt = append(exempt, "/ui/")
	}
	middlewares := []Middleware{RateLimitMiddleware(s.rateLimiter, exempt...), LoggingMiddleware}
	if cfg.AuthToken == "" && cfg.LoopbackOnly() {
		middlewares = append(middlewares, LoopbackHostMiddleware)
	}
	middlewares = append(middlewares,
		CORSMiddleware(cfg.CORSOrigins),
		AuthMiddleware(cfg.AuthToken, exempt...),
	)
	handler := s.track(Chain(s.router, middlewares...))

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

	base, cancel := context.WithCancel(context.Background())
	s.cancelRequests = cancel
	s.server.BaseContext = func(net.Listener) context.Context { return base }
	s.served = make(chan struct{})

	// Start serving in background
	go func() {
		defer close(s.served)
		log.Printf("api: server listening on %s", ln.Addr())
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("api: server error: %v", err)
		}
	}()

	return nil
}

// Stop shuts down the API server. Requests in flight get ShutdownTimeout to
// finish; after that their contexts are cancelled and their connections
// closed. Stop returns once every handler has returned.
func (s *Server) Stop() error {
	s.stopOnce.Do(func() {
		if s.rateLimiter != nil {
			s.rateLimiter.Stop()
		}
		s.mu.Lock()
		s.stopping = true
		started, timeout := s.started, s.config.ShutdownTimeout
		s.mu.Unlock()
		if !started {
			s.stopErr = ErrServerNotStarted
			return
		}

		log.Printf("api: shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := s.server.Shutdown(ctx); err != nil {
			s.cancelRequests()
			_ = s.server.Close()
			if errors.Is(err, context.DeadlineExceeded) {
				s.stopErr = ErrShutdownTimeout
			} else {
				s.stopErr = fmt.Errorf("shutdown error: %w", err)
			}
		}
		<-s.served

		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.waitForRequests()
		s.cancelRequests()

		log.Printf("api: server stopped")
	})

	return s.stopErr
}

// track counts each request so Stop can wait for it, and refuses requests
// that arrive after Stop stopped waiting.
func (s *Server) track(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			writeError(w, http.StatusServiceUnavailable, "server is shutting down", "")
			return
		}
		s.requests.Add(1)
		s.mu.Unlock()
		defer s.requests.Done()
		next.ServeHTTP(w, r)
	})
}

// waitForRequests waits for handlers that are still running. Snapshot, clone
// and admin requests cannot be cancelled once the filesystem has started them.
func (s *Server) waitForRequests() {
	done := make(chan struct{})
	go func() {
		s.requests.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		log.Printf("api: waiting for requests in flight to finish")
		<-done
	}
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
