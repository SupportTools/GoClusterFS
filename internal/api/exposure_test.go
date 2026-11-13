package api

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestCheckExposure(t *testing.T) {
	tests := []struct {
		addr    string
		token   string
		refused bool
	}{
		{"127.0.0.1:8080", "", false},
		{"127.1.2.3:8080", "", false},
		{"localhost:8080", "", false},
		{"[::1]:8080", "", false},
		{":8080", "", true},
		{"0.0.0.0:8080", "", true},
		{"[::]:8080", "", true},
		{"10.0.0.5:8080", "", true},
		{"node1.example.com:8080", "", true},
		{":8080", "secret", false},
		{"10.0.0.5:8080", "secret", false},
	}
	for _, tt := range tests {
		cfg := DefaultConfig().WithListenAddr(tt.addr).WithAuthToken(tt.token)
		err := cfg.CheckExposure()
		if tt.refused != errors.Is(err, ErrUnauthenticatedRemote) {
			t.Errorf("CheckExposure(%q, token=%q) = %v, want refused=%v", tt.addr, tt.token, err, tt.refused)
		}
	}
}

func TestDefaultConfigPassesExposureCheck(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.CheckExposure(); err != nil {
		t.Fatalf("default config refused: %v", err)
	}
}

func TestCheckExposureRefusesWildcardCORSWithoutToken(t *testing.T) {
	cfg := DefaultConfig().WithCORSOrigins("*")
	if err := cfg.CheckExposure(); !errors.Is(err, ErrWildcardCORSWithoutToken) {
		t.Fatalf("CORS * without token: got %v, want ErrWildcardCORSWithoutToken", err)
	}
	cfg = cfg.WithAuthToken("secret")
	if err := cfg.CheckExposure(); err != nil {
		t.Fatalf("CORS * with token refused: %v", err)
	}
	cfg = DefaultConfig().WithCORSOrigins("http://localhost:5173")
	if err := cfg.CheckExposure(); err != nil {
		t.Fatalf("listed CORS origin refused: %v", err)
	}
}

func get(t *testing.T, srv *Server, path, host string, header map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+srv.Addr()+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if host != "" {
		req.Host = host
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	closeBody(resp)
	return resp
}

func startServer(t *testing.T, cfg Config) *Server {
	t.Helper()
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopServer(t, srv) })
	return srv
}

func TestTokenlessLoopbackServerRejectsForeignHost(t *testing.T) {
	srv := startServer(t, DefaultConfig().WithListenAddr("127.0.0.1:0"))
	for host, want := range map[string]int{
		"":                http.StatusOK,
		"localhost:8080":  http.StatusOK,
		"LOCALHOST":       http.StatusOK,
		"[::1]:8080":      http.StatusOK,
		"127.0.0.2":       http.StatusOK,
		"evil.example":    http.StatusForbidden,
		"evil.example:80": http.StatusForbidden,
		"10.0.0.5:8080":   http.StatusForbidden,
	} {
		if got := get(t, srv, "/api/v1/status", host, nil).StatusCode; got != want {
			t.Errorf("Host %q: status %d, want %d", host, got, want)
		}
	}
}

func TestTokenServerSkipsHostCheck(t *testing.T) {
	srv := startServer(t, DefaultConfig().WithListenAddr("127.0.0.1:0").WithAuthToken("secret"))
	resp := get(t, srv, "/api/v1/status", "evil.example", map[string]string{"Authorization": "Bearer secret"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
}

func TestAuthSkipsOnlyExactPaths(t *testing.T) {
	srv := startServer(t, DefaultConfig().WithListenAddr("127.0.0.1:0").WithAuthToken("secret"))
	for _, path := range []string{"/health/x", "/metrics/x"} {
		if got := get(t, srv, path, "", nil).StatusCode; got != http.StatusUnauthorized {
			t.Errorf("%s without token: status %d, want 401", path, got)
		}
	}
	if got := get(t, srv, "/health", "", nil).StatusCode; got != http.StatusOK {
		t.Errorf("/health without token: status %d, want 200", got)
	}
}

func TestCORSHeaders(t *testing.T) {
	srv := startServer(t, DefaultConfig().WithListenAddr("127.0.0.1:0").WithCORSOrigins("http://localhost:5173"))
	resp := get(t, srv, "/health", "", map[string]string{"Origin": "http://other.example"})
	if resp.Header.Get("Vary") != "Origin" {
		t.Errorf("Vary = %q, want Origin", resp.Header.Get("Vary"))
	}
	resp = get(t, srv, "/health", "", map[string]string{"Origin": "http://localhost:5173"})
	if got := resp.Header.Get("Access-Control-Allow-Methods"); got != "GET, HEAD, OPTIONS" {
		t.Errorf("Allow-Methods = %q, want GET, HEAD, OPTIONS", got)
	}
}
