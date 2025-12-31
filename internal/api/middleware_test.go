package api

import (
	"context"
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
	if cors != "http://any-origin.com" {
		t.Errorf("expected CORS header 'http://any-origin.com', got %q", cors)
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
