package api

import (
	"context"
	"math"
	"net/http"
	"testing"
	"time"
)

func TestAuthMiddleware_NoTokenConfigured(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	// Request without auth should succeed when no token configured
	resp, err := http.Get("http://" + server.Addr() + "/api/v1/status")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAuthMiddleware_ValidToken(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0").WithAuthToken("secret-token")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", "http://"+server.Addr()+"/api/v1/status", nil)
	req.Header.Set("Authorization", "Bearer secret-token")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAuthMiddleware_MissingHeader(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0").WithAuthToken("secret-token")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/api/v1/status")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAuthMiddleware_InvalidFormat(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0").WithAuthToken("secret-token")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", "http://"+server.Addr()+"/api/v1/status", nil)
	req.Header.Set("Authorization", "Basic secret-token")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAuthMiddleware_WrongToken(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0").WithAuthToken("secret-token")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", "http://"+server.Addr()+"/api/v1/status", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAuthMiddleware_SkipPaths(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0").WithAuthToken("secret-token")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	// /health and /metrics should be accessible without auth
	paths := []string{"/health", "/metrics"}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get("http://" + server.Addr() + path)
			if err != nil {
				t.Fatalf("request error: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("expected 200 for %s, got %d", path, resp.StatusCode)
			}
		})
	}
}

func TestCORSMiddleware_NoOrigins(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", "http://"+server.Addr()+"/health", nil)
	req.Header.Set("Origin", "http://example.com")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	// No CORS headers should be set
	if cors := resp.Header.Get("Access-Control-Allow-Origin"); cors != "" {
		t.Errorf("expected no CORS header, got %q", cors)
	}
}

func TestCORSMiddleware_AllowedOrigin(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0").WithCORSOrigins("http://example.com")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", "http://"+server.Addr()+"/health", nil)
	req.Header.Set("Origin", "http://example.com")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	cors := resp.Header.Get("Access-Control-Allow-Origin")
	if cors != "http://example.com" {
		t.Errorf("expected CORS header 'http://example.com', got %q", cors)
	}
}

func TestCORSMiddleware_DisallowedOrigin(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0").WithCORSOrigins("http://example.com")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", "http://"+server.Addr()+"/health", nil)
	req.Header.Set("Origin", "http://evil.com")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	// No CORS headers for disallowed origin
	if cors := resp.Header.Get("Access-Control-Allow-Origin"); cors != "" {
		t.Errorf("expected no CORS header for disallowed origin, got %q", cors)
	}
}

func TestCORSMiddleware_Wildcard(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0").WithCORSOrigins("*")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	client := &http.Client{}
	req, _ := http.NewRequest("GET", "http://"+server.Addr()+"/health", nil)
	req.Header.Set("Origin", "http://any-origin.com")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	cors := resp.Header.Get("Access-Control-Allow-Origin")
	if cors != "*" {
		t.Errorf("expected CORS header '*', got %q", cors)
	}
}

func TestCORSMiddleware_Preflight(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0").WithCORSOrigins("http://example.com")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	client := &http.Client{}
	req, _ := http.NewRequest("OPTIONS", "http://"+server.Addr()+"/api/v1/status", nil)
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204 for preflight, got %d", resp.StatusCode)
	}

	if cors := resp.Header.Get("Access-Control-Allow-Origin"); cors != "http://example.com" {
		t.Errorf("expected CORS header, got %q", cors)
	}

	if methods := resp.Header.Get("Access-Control-Allow-Methods"); methods == "" {
		t.Error("expected Access-Control-Allow-Methods header")
	}
}

// Rate Limiting Tests

func TestRateLimiter_Allow(t *testing.T) {
	// Create a rate limiter with 10 req/s and burst of 5
	rl := NewRateLimiter(10, 5)

	// First 5 requests should be allowed (burst)
	for i := 0; i < 5; i++ {
		if !rl.Allow("192.168.1.1") {
			t.Errorf("request %d should be allowed", i+1)
		}
	}

	// 6th request should be denied (burst exhausted)
	if rl.Allow("192.168.1.1") {
		t.Error("6th request should be denied")
	}

	// Different IP should be allowed (separate bucket)
	if !rl.Allow("192.168.1.2") {
		t.Error("request from different IP should be allowed")
	}
}

func TestRateLimiter_TokenRefill(t *testing.T) {
	// Create a rate limiter with 1000 req/s (fast refill for testing)
	rl := NewRateLimiter(1000, 2)

	// Exhaust the bucket
	rl.Allow("192.168.1.1")
	rl.Allow("192.168.1.1")
	if rl.Allow("192.168.1.1") {
		t.Error("should be rate limited after exhausting burst")
	}

	// Wait for refill (at 1000/s, 1ms = 1 token)
	time.Sleep(2 * time.Millisecond)

	// Should be allowed again after refill
	if !rl.Allow("192.168.1.1") {
		t.Error("should be allowed after token refill")
	}
}

func TestRateLimitMiddleware_Disabled(t *testing.T) {
	// No rate limiting when not configured
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	// Make many requests - all should succeed
	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://" + server.Addr() + "/health")
		if err != nil {
			t.Fatalf("request error: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("request %d: expected 200, got %d", i, resp.StatusCode)
		}
	}
}

func TestRateLimitMiddleware_Enabled(t *testing.T) {
	// Enable rate limiting with small burst for easy testing
	cfg := DefaultConfig().WithListenAddr(":0").WithRateLimit(1, 3)
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	// First 3 requests should succeed (burst)
	for i := 0; i < 3; i++ {
		resp, err := http.Get("http://" + server.Addr() + "/api/v1/status")
		if err != nil {
			t.Fatalf("request error: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("request %d: expected 200, got %d", i+1, resp.StatusCode)
		}
	}

	// 4th request should be rate limited
	resp, err := http.Get("http://" + server.Addr() + "/api/v1/status")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("4th request: expected 429, got %d", resp.StatusCode)
	}

	// Check Retry-After header
	if retry := resp.Header.Get("Retry-After"); retry != "1" {
		t.Errorf("expected Retry-After header '1', got %q", retry)
	}
}

func TestGetClientIP_RemoteAddr(t *testing.T) {
	req, _ := http.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.100:12345"

	ip := getClientIP(req)
	if ip != "192.168.1.100" {
		t.Errorf("expected '192.168.1.100', got %q", ip)
	}
}

func TestGetClientIP_IgnoresProxyHeaders(t *testing.T) {
	req, _ := http.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	req.Header.Set("X-Forwarded-For", "203.0.113.50")
	req.Header.Set("X-Real-IP", "198.51.100.78")

	if ip := getClientIP(req); ip != "10.0.0.1" {
		t.Errorf("expected the connection's IP 10.0.0.1, got %q", ip)
	}
}

func TestRateLimitIgnoresForwardedFor(t *testing.T) {
	srv := startServer(t, DefaultConfig().WithListenAddr("127.0.0.1:0").WithRateLimit(0.001, 1))
	if got := get(t, srv, "/api/v1/status", "", map[string]string{"X-Forwarded-For": "203.0.113.1"}).StatusCode; got != http.StatusOK {
		t.Fatalf("first request: status %d, want 200", got)
	}
	if got := get(t, srv, "/api/v1/status", "", map[string]string{"X-Forwarded-For": "203.0.113.2"}).StatusCode; got != http.StatusTooManyRequests {
		t.Fatalf("second request with a new X-Forwarded-For: status %d, want 429", got)
	}
}

func TestRateLimiterStopEndsCleanup(t *testing.T) {
	rl := NewRateLimiter(1, 1)
	rl.Stop()
	rl.Stop()
	select {
	case <-rl.stopped:
	case <-time.After(time.Second):
		t.Fatal("cleanup goroutine still running after Stop")
	}
}

func TestServerStopStopsRateLimiter(t *testing.T) {
	for _, start := range []bool{true, false} {
		srv, err := NewServer(DefaultConfig().WithListenAddr("127.0.0.1:0").WithRateLimit(10, 20))
		if err != nil {
			t.Fatal(err)
		}
		if start {
			if err := srv.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		srv.Stop()
		select {
		case <-srv.rateLimiter.stopped:
		case <-time.After(time.Second):
			t.Fatalf("started=%v: rate limiter cleanup still running after Stop", start)
		}
	}
}

func TestRateLimiterCapsClients(t *testing.T) {
	rl := NewRateLimiter(1, 2)
	defer rl.Stop()
	rl.maxClients = 2
	if !rl.Allow("a") || !rl.Allow("b") {
		t.Fatal("first two clients refused")
	}
	if rl.Allow("c") {
		t.Fatal("third client allowed past the cap")
	}
	if !rl.Allow("a") {
		t.Fatal("known client refused at the cap")
	}
}

func TestRateLimiterSweepDropsFullBuckets(t *testing.T) {
	rl := NewRateLimiter(1, 2)
	defer rl.Stop()
	rl.Allow("a")
	count := func() int {
		rl.mu.Lock()
		defer rl.mu.Unlock()
		return len(rl.clients)
	}
	rl.sweep(time.Now().Add(time.Second))
	if count() != 1 {
		t.Fatal("bucket dropped before it refilled")
	}
	rl.sweep(time.Now().Add(3 * time.Second))
	if count() != 0 {
		t.Fatal("full bucket not dropped")
	}
}

func TestRateLimitKey(t *testing.T) {
	key := func(remote string) string {
		req, _ := http.NewRequest("GET", "/", nil)
		req.RemoteAddr = remote
		return rateLimitKey(req)
	}
	if key("[2001:db8::1]:1") != key("[2001:db8::2]:2") {
		t.Error("addresses in one IPv6 /64 got different keys")
	}
	if key("[2001:db8::1]:1") == key("[2001:db8:0:1::1]:1") {
		t.Error("different IPv6 /64s share a key")
	}
	if got := key("[::ffff:192.0.2.1]:1"); got != "192.0.2.1" {
		t.Errorf("mapped IPv4 key = %q, want 192.0.2.1", got)
	}
	if got := key("192.0.2.1:1"); got != "192.0.2.1" {
		t.Errorf("IPv4 key = %q, want 192.0.2.1", got)
	}
}

func TestRateLimitSkipsHealthAndMetrics(t *testing.T) {
	srv := startServer(t, DefaultConfig().WithListenAddr("127.0.0.1:0").WithRateLimit(0.25, 1))
	for i := 0; i < 3; i++ {
		for _, path := range []string{"/health", "/metrics"} {
			if got := get(t, srv, path, "", nil).StatusCode; got != http.StatusOK {
				t.Fatalf("%s request %d: status %d, want 200", path, i+1, got)
			}
		}
	}
	get(t, srv, "/api/v1/status", "", nil)
	resp := get(t, srv, "/api/v1/status", "", nil)
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") != "4" {
		t.Fatalf("second status request: %d with Retry-After %q, want 429 with 4", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}

func TestConfigRejectsBadRateLimits(t *testing.T) {
	for _, rate := range []float64{0, -1, math.NaN(), math.Inf(1), 2 * MaxRateLimit} {
		cfg := DefaultConfig().WithRateLimit(rate, 1)
		if err := cfg.Validate(); err == nil {
			t.Errorf("rate %v accepted", rate)
		}
	}
	cfg := DefaultConfig().WithRateLimit(MaxRateLimit, 1)
	if err := cfg.Validate(); err != nil {
		t.Errorf("rate %v refused: %v", MaxRateLimit, err)
	}
}
