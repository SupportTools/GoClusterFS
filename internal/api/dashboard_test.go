package api

import (
	"net/http"
	"testing"
)

func dashboardConfig() Config {
	cfg := DefaultConfig().WithListenAddr("127.0.0.1:0").WithAuthToken("secret")
	cfg.Dashboard = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Dashboard-Path", r.URL.Path)
	})
	return cfg
}

func TestExempt(t *testing.T) {
	paths := []string{"/health", "/metrics", "/ui/"}
	for p, want := range map[string]bool{
		"/health":              true,
		"/metrics":             true,
		"/ui":                  true,
		"/ui/":                 true,
		"/ui/assets/a.js":      true,
		"/ui/snapshots":        true,
		"/health/x":            false,
		"/healthz":             false,
		"/uix":                 false,
		"/ui/../api/v1/status": false,
		"/ui/./../metrics/x":   false,
		"/api/v1/status":       false,
	} {
		if got := exempt(p, paths); got != want {
			t.Errorf("exempt(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestDashboardNeedsNoToken(t *testing.T) {
	srv := startServer(t, dashboardConfig())
	for path, want := range map[string]string{
		"/ui":             "/",
		"/ui/":            "/",
		"/ui/assets/a.js": "/assets/a.js",
		"/ui/snapshots":   "/snapshots",
	} {
		resp := get(t, srv, path, "", nil)
		if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Dashboard-Path") != want {
			t.Errorf("%s: status %d, dashboard saw %q; want 200 and %q", path, resp.StatusCode, resp.Header.Get("X-Dashboard-Path"), want)
		}
	}
	for _, path := range []string{"/ui/../api/v1/status", "/ui/%2e%2e/api/v1/status", "/uix", "/api/v1/status"} {
		if got := get(t, srv, path, "", nil).StatusCode; got != http.StatusUnauthorized {
			t.Errorf("%s without token: status %d, want 401", path, got)
		}
	}
}

func TestDashboardSkipsRateLimit(t *testing.T) {
	srv := startServer(t, dashboardConfig().WithRateLimit(0.25, 1))
	for i := 0; i < 5; i++ {
		if got := get(t, srv, "/ui/assets/a.js", "", nil).StatusCode; got != http.StatusOK {
			t.Fatalf("request %d: status %d, want 200", i+1, got)
		}
	}
}

func TestNoDashboardMeansNoUIRoute(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr("127.0.0.1:0").WithAuthToken("secret")
	srv := startServer(t, cfg)
	if got := get(t, srv, "/ui/", "", nil).StatusCode; got != http.StatusUnauthorized {
		t.Errorf("/ui/ without token: status %d, want 401", got)
	}
	auth := map[string]string{"Authorization": "Bearer secret"}
	if got := get(t, srv, "/ui/", "", auth).StatusCode; got != http.StatusNotFound {
		t.Errorf("/ui/ with token: status %d, want 404", got)
	}
}

func TestDashboardKeepsLoopbackHostCheck(t *testing.T) {
	cfg := dashboardConfig().WithAuthToken("")
	srv := startServer(t, cfg)
	if got := get(t, srv, "/ui/", "evil.example", nil).StatusCode; got != http.StatusForbidden {
		t.Errorf("/ui/ with a foreign Host: status %d, want 403", got)
	}
}
