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

	// Register routes
	s.registerRoutes()

	// Build middleware chain (order: logging -> CORS -> auth)
	handler := Chain(
		s.router,
		LoggingMiddleware,
		CORSMiddleware(cfg.CORSOrigins),
		AuthMiddleware(cfg.AuthToken, "/health", "/metrics"),
	)

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
