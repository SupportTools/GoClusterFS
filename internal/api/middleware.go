package api

import (
	"crypto/subtle"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Middleware is a function that wraps an http.Handler.
type Middleware func(http.Handler) http.Handler

// Chain applies middlewares in order (first middleware is outermost).
func Chain(h http.Handler, middlewares ...Middleware) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

// responseWriter wraps http.ResponseWriter to capture status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// LoggingMiddleware logs HTTP requests with method, path, status, and duration.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)

		log.Printf("api: %s %s %d %s",
			r.Method,
			r.URL.Path,
			rw.status,
			time.Since(start).Round(time.Microsecond),
		)
	})
}

// AuthMiddleware returns a middleware that validates Bearer tokens.
// If token is empty, authentication is disabled.
// Paths in skipPaths are not authenticated (e.g., /health, /metrics).
func AuthMiddleware(token string, skipPaths ...string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// If no token configured, skip auth
			if token == "" {
				next.ServeHTTP(w, r)
				return
			}

			// Check if path should skip auth
			for _, path := range skipPaths {
				if r.URL.Path == path {
					next.ServeHTTP(w, r)
					return
				}
			}

			// Extract Bearer token from Authorization header
			auth := r.Header.Get("Authorization")
			if auth == "" {
				writeError(w, http.StatusUnauthorized, "missing authorization header", "")
				return
			}

			if !strings.HasPrefix(auth, "Bearer ") {
				writeError(w, http.StatusUnauthorized, "invalid authorization format", "use Bearer token")
				return
			}

			providedToken := strings.TrimPrefix(auth, "Bearer ")

			// Constant-time comparison to prevent timing attacks
			if subtle.ConstantTimeCompare([]byte(providedToken), []byte(token)) != 1 {
				writeError(w, http.StatusUnauthorized, "invalid token", "")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// CORSMiddleware returns a middleware that handles CORS headers.
// If origins is empty, CORS is disabled.
// Use "*" to allow all origins (not recommended for production).
func CORSMiddleware(origins []string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// If no origins configured, skip CORS
			if len(origins) == 0 {
				next.ServeHTTP(w, r)
				return
			}
			w.Header().Add("Vary", "Origin")

			origin := r.Header.Get("Origin")
			if origin == "" {
				next.ServeHTTP(w, r)
				return
			}

			// Check if origin is allowed
			allowOrigin := ""
			for _, o := range origins {
				if o == "*" || o == origin {
					allowOrigin = o
					break
				}
			}

			if allowOrigin == "" {
				next.ServeHTTP(w, r)
				return
			}

			// Set CORS headers
			w.Header().Set("Access-Control-Allow-Origin", allowOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.Header().Set("Access-Control-Max-Age", "86400")

			// Handle preflight requests
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// RateLimiter implements a token bucket rate limiter per client IP.
type RateLimiter struct {
	mu       sync.Mutex
	clients  map[string]*tokenBucket
	rate     float64 // tokens per second
	burst    int     // max tokens (bucket capacity)
	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

// tokenBucket tracks rate limit state for a single client.
type tokenBucket struct {
	tokens   float64
	lastSeen time.Time
}

// NewRateLimiter creates a new rate limiter with the specified rate and burst.
func NewRateLimiter(requestsPerSecond float64, burst int) *RateLimiter {
	rl := &RateLimiter{
		clients: make(map[string]*tokenBucket),
		rate:    requestsPerSecond,
		burst:   burst,
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	// Start cleanup goroutine to prevent memory leaks
	go rl.cleanup()
	return rl
}

// Allow checks if a request from the given IP should be allowed.
func (rl *RateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	bucket, exists := rl.clients[ip]

	if !exists {
		// New client starts with a full bucket
		rl.clients[ip] = &tokenBucket{
			tokens:   float64(rl.burst) - 1, // Consume one token for this request
			lastSeen: now,
		}
		return true
	}

	// Calculate tokens to add based on time elapsed
	elapsed := now.Sub(bucket.lastSeen).Seconds()
	bucket.tokens += elapsed * rl.rate
	if bucket.tokens > float64(rl.burst) {
		bucket.tokens = float64(rl.burst)
	}
	bucket.lastSeen = now

	// Check if we have tokens available
	if bucket.tokens < 1 {
		return false
	}

	bucket.tokens--
	return true
}

// cleanup removes stale entries to prevent memory leaks.
func (rl *RateLimiter) cleanup() {
	defer close(rl.stopped)
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-rl.stop:
			return
		case <-ticker.C:
		}
		rl.mu.Lock()
		cutoff := time.Now().Add(-10 * time.Minute)
		for ip, bucket := range rl.clients {
			if bucket.lastSeen.Before(cutoff) {
				delete(rl.clients, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// Stop ends the cleanup goroutine. It is safe to call more than once.
func (rl *RateLimiter) Stop() {
	rl.stopOnce.Do(func() { close(rl.stop) })
}

// RateLimitMiddleware returns a middleware that enforces rate limiting per client IP.
// If limiter is nil, no rate limiting is applied.
func RateLimitMiddleware(limiter *RateLimiter) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// If no limiter configured, skip rate limiting
			if limiter == nil {
				next.ServeHTTP(w, r)
				return
			}

			// Extract client IP
			ip := getClientIP(r)

			// Check rate limit
			if !limiter.Allow(ip) {
				w.Header().Set("Retry-After", "1")
				writeError(w, http.StatusTooManyRequests, "rate limit exceeded", "please slow down")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// getClientIP returns the connection's IP. Proxy headers are ignored: any client can set them.
func getClientIP(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// LoopbackHostMiddleware rejects requests whose Host header is not a loopback
// name, so a DNS-rebinding web page cannot read an API that has no token.
func LoopbackHostMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isLoopbackHost(r.Host) {
			writeError(w, http.StatusForbidden, "host not allowed", "without an auth token, use localhost, 127.0.0.1 or [::1]")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isLoopbackHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.Trim(host, "[]"), ".")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
