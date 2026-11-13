package commands

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/api"
)

func serveHealth(t *testing.T, code int, status api.HealthStatus) *fakeAPI {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/status" && r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(api.ErrorResponse{Error: "unauthorized", Code: http.StatusUnauthorized})
			return
		}
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(api.HealthResponse{Status: status, Checks: map[string]api.ComponentHealth{
			"recovery": {Status: status, Message: "replaying the journal of the crashed node in slot 3"},
		}})
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	return &fakeAPI{host: u.Hostname(), port: u.Port()}
}

func TestExitCodesAndHints(t *testing.T) {
	ok := serveHealth(t, http.StatusOK, api.HealthStatusOK)
	degraded := serveHealth(t, http.StatusOK, api.HealthStatusDegraded)
	down := serveHealth(t, http.StatusServiceUnavailable, api.HealthStatusUnhealthy)
	noStats := startFakeAPI(t, map[string]any{"/api/v1/stats/cache": http.StatusServiceUnavailable})

	tests := []struct {
		name   string
		run    func() (string, string, error)
		code   int
		stdout string
		stderr string
	}{
		{"healthy", func() (string, string, error) { return ok.run(t, "health") }, 0, "ok", ""},
		{"degraded", func() (string, string, error) { return degraded.run(t, "health") }, 1, "slot 3", "filesystem is degraded"},
		{"unhealthy answers 503", func() (string, string, error) { return down.run(t, "health") }, 4, "slot 3", "filesystem is unhealthy"},
		{"API error", func() (string, string, error) { return noStats.run(t, "stats", "cache") }, 8, "", "no stats provider configured"},
		{"no token", func() (string, string, error) { return ok.run(t, "status") }, 8, "", "set GFSCTL_TOKEN"},
		{"nothing listening", func() (string, string, error) { return run(t, "--port", "1", "metrics") }, 8, "", "is goclusterfs-mount running"},
		{"unknown flag", func() (string, string, error) { return run(t, "status", "--bogus") }, 16, "", "run gfsctl status --help"},
		{"unknown command", func() (string, string, error) { return run(t, "bogus") }, 16, "", "--help"},
		{"unknown component", func() (string, string, error) { return run(t, "stats", "bogus") }, 16, "", "unknown component"},
		{"watch with json", func() (string, string, error) { return run(t, "status", "-w", "-o", "json") }, 16, "", "drop one of them"},
		{"bad setting", func() (string, string, error) { return run(t, "-o", "yaml", "metrics") }, 16, "", "unknown output format"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout, stderr, err := tt.run()
			if got := ExitCode(err); got != tt.code {
				t.Errorf("exit code = %d, want %d (err %v)", got, tt.code, err)
			}
			if !strings.Contains(stdout, tt.stdout) || !strings.Contains(stderr, tt.stderr) {
				t.Errorf("stdout:\n%s\nstderr:\n%s\nwant %q and %q", stdout, stderr, tt.stdout, tt.stderr)
			}
			if tt.code == 0 && stderr != "" {
				t.Errorf("stderr on success: %q", stderr)
			}
		})
	}
}

func TestDebugLogsEachRequest(t *testing.T) {
	f := startFakeAPI(t, map[string]any{"/metrics": "x\n"})
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(log.Writer())

	if _, _, err := f.run(t, "metrics"); err != nil || logs.Len() != 0 {
		t.Fatalf("without --debug: err=%v logs=%q", err, logs.String())
	}
	if _, _, err := f.run(t, "--debug", "metrics"); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"[DEBUG] GET http://", "/metrics", "200 OK"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("--debug logs lack %q:\n%s", want, logs.String())
		}
	}
}
