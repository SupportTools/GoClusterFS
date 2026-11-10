package commands

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/supporttools/GoClusterFS/internal/api"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "gfsctl-test")
	if err != nil {
		panic(err)
	}
	os.Setenv("XDG_CONFIG_HOME", dir)
	for _, name := range []string{"GFSCTL_HOST", "GFSCTL_PORT", "GFSCTL_TOKEN", "GFSCTL_OUTPUT", "GFSCTL_TIMEOUT", "NO_COLOR"} {
		os.Unsetenv(name)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type fakeAPI struct {
	host   string
	port   string
	mu     sync.Mutex
	tokens []string
}

func (f *fakeAPI) lastToken() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.tokens) == 0 {
		return ""
	}
	return f.tokens[len(f.tokens)-1]
}

// startFakeAPI serves routes: an int answers with that status, a string as text, anything else as JSON.
func startFakeAPI(t *testing.T, routes map[string]any) *fakeAPI {
	t.Helper()
	f := &fakeAPI{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.tokens = append(f.tokens, r.Header.Get("Authorization"))
		f.mu.Unlock()
		switch v := routes[r.URL.Path].(type) {
		case nil:
			http.NotFound(w, r)
		case int:
			w.WriteHeader(v)
			json.NewEncoder(w).Encode(api.ErrorResponse{Error: "stats not available", Code: v, Details: "no stats provider configured"})
		case string:
			w.Write([]byte(v))
		default:
			json.NewEncoder(w).Encode(v)
		}
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	f.host, f.port = u.Hostname(), u.Port()
	return f
}

func resetFlags(c *cobra.Command) {
	reset := func(fl *pflag.Flag) {
		fl.Value.Set(fl.DefValue)
		fl.Changed = false
	}
	c.Flags().VisitAll(reset)
	c.PersistentFlags().VisitAll(reset)
	for _, sub := range c.Commands() {
		resetFlags(sub)
	}
}

func run(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	resetFlags(rootCmd)
	var stdout, stderr bytes.Buffer
	rootCmd.SetOut(&stdout)
	rootCmd.SetErr(&stderr)
	rootCmd.SetArgs(args)
	err := Execute()
	return stdout.String(), stderr.String(), err
}

func (f *fakeAPI) run(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	return run(t, append([]string{"--host", f.host, "--port", f.port}, args...)...)
}

var sampleStatus = api.StatusResponse{
	Status:        "ok",
	Version:       "1.2.3",
	GitCommit:     "abc123",
	BuildTime:     "2026-11-10T00:00:00Z",
	GoVersion:     "go1.26.0",
	UptimeSeconds: 3661,
	MountPath:     "/mnt/cluster",
	DevicePath:    "/dev/sdb",
	MountState:    api.MountStateMounted,
	Capacity: &api.CapacityInfo{
		TotalBytes: 1 << 30, UsedBytes: 1 << 29, FreeBytes: 1 << 29, UsedPercent: 50,
		TotalBlocks: 1024, UsedBlocks: 512, FreeBlocks: 512, BlockSizeBytes: 1 << 20,
	},
}

func TestStatusText(t *testing.T) {
	f := startFakeAPI(t, map[string]any{"/api/v1/status": sampleStatus})
	out, _, err := f.run(t, "status")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"1.2.3", "/mnt/cluster", "1h 1m", "512.00 MB (50.0%)"} {
		if !strings.Contains(out, want) {
			t.Errorf("status output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "\033[") {
		t.Errorf("status output to a buffer has color codes:\n%q", out)
	}
}

// TestJSONMatchesServerTypes checks that every read command keeps every field the server sends.
func TestJSONMatchesServerTypes(t *testing.T) {
	cache := api.CacheHitRateInfo{Size: 100, Hits: 90, Misses: 10, HitRate: 0.9}
	cluster := &api.ClusterInfoResponse{
		NodeID: "n1", NodeAddress: "10.0.0.1", Role: "member", State: "active", ClusterSize: 2, HealthyNodes: 2,
		Peers: []api.ClusterPeerInfo{{NodeID: "n2", Address: "10.0.0.2", State: "active", LastSeen: "now", Latency: 1.5}},
	}
	cacheStats := &api.CacheStatsResponse{InodeCache: cache, ExtentCache: cache, NegativeCache: cache, LockHintCache: cache}
	trim := &api.TrimStatsResponse{TrimOperations: 1, TrimBytes: 2, TrimBlocks: 3, PendingTrimBlocks: 4, LastTrimAt: "now", TrimErrors: 5, TrimEnabled: true, BatchSize: 6}
	lock := &api.LockStatsResponse{HintCacheHits: 1, HintCacheMisses: 2, HintCacheHitRate: 0.5, ActiveLocks: 3, PendingLocks: 4}
	sfb := &api.SFBStatsResponse{TotalBlocks: 1, AllocatedBlocks: 2, FreeBlocks: 3, AllocationPercent: 4.5, FragmentationRatio: 0.5, LargestFreeExtent: 6, AllocationsTotal: 7, DeallocationsTotal: 8, AllocationFailures: 9}
	io := &api.IOStatsResponse{ReadOps: 1, WriteOps: 2, BytesRead: 3, BytesWritten: 4, AvgReadLatUs: 5.5, AvgWriteLatUs: 6.5}
	journal := &api.JournalStatsResponse{State: "clean", HeadOffset: 1, TailOffset: 2, AvailableSpace: 3, ActiveTxnCount: 4, CommitCount: 5, BytesWritten: 6, LastCheckpointSeq: 7, BatchSyncCount: 8}
	cases := []struct {
		args  []string
		path  string
		value any
	}{
		{[]string{"status"}, "/api/v1/status", &sampleStatus},
		{[]string{"health"}, "/health", &api.HealthResponse{Status: api.HealthStatusOK, Checks: map[string]api.ComponentHealth{"api": {Status: api.HealthStatusOK, Message: "fine"}}}},
		{[]string{"stats"}, "/api/v1/stats", &api.StatsOverviewResponse{Cache: cacheStats, Journal: journal, IO: io}},
		{[]string{"stats", "cache"}, "/api/v1/stats/cache", cacheStats},
		{[]string{"stats", "journal"}, "/api/v1/stats/journal", journal},
		{[]string{"stats", "trim"}, "/api/v1/stats/trim", trim},
		{[]string{"stats", "lock"}, "/api/v1/stats/lock", lock},
		{[]string{"stats", "sfb"}, "/api/v1/stats/sfb", sfb},
		{[]string{"stats", "cluster"}, "/api/v1/stats/cluster", cluster},
		{[]string{"stats", "all"}, "/api/v1/stats/all", &api.AllStatsResponse{CollectedAt: "now", Cache: cacheStats, Journal: journal, Trim: trim, Lock: lock, SFB: sfb, Cluster: cluster}},
	}
	for _, tc := range cases {
		f := startFakeAPI(t, map[string]any{tc.path: tc.value})
		out, _, err := f.run(t, append(tc.args, "-o", "json")...)
		if err != nil {
			t.Fatalf("%v: %v", tc.args, err)
		}
		got := reflect.New(reflect.TypeOf(tc.value).Elem()).Interface()
		if err := json.Unmarshal([]byte(out), got); err != nil {
			t.Fatalf("%v: decode output: %v\n%s", tc.args, err, out)
		}
		if !reflect.DeepEqual(got, tc.value) {
			t.Errorf("%v dropped or changed server fields:\nserver: %+v\ngfsctl: %+v", tc.args, tc.value, got)
		}
	}
}

func TestHealthFailsWhenUnhealthy(t *testing.T) {
	f := startFakeAPI(t, map[string]any{"/health": api.HealthResponse{
		Status: api.HealthStatusUnhealthy,
		Checks: map[string]api.ComponentHealth{"journal": {Status: api.HealthStatusUnhealthy, Message: "replay failed"}},
	}})
	out, stderr, err := f.run(t, "health")
	if err == nil {
		t.Fatal("health of an unhealthy mount returned no error")
	}
	if !strings.Contains(out, "replay failed") || !strings.Contains(stderr, "Error: filesystem is unhealthy") {
		t.Errorf("stdout:\n%s\nstderr:\n%s", out, stderr)
	}
}

func TestErrorsArePrintedOnce(t *testing.T) {
	f := startFakeAPI(t, map[string]any{"/api/v1/stats/cache": http.StatusServiceUnavailable})
	_, stderr, err := f.run(t, "stats", "cache")
	if err == nil || strings.Count(stderr, "Error:") != 1 || !strings.Contains(stderr, "no stats provider configured") {
		t.Errorf("err=%v stderr:\n%s", err, stderr)
	}
	_, stderr, err = f.run(t, "stats", "bogus")
	if err == nil || !strings.Contains(stderr, "unknown component: bogus") {
		t.Errorf("unknown component: err=%v stderr:\n%s", err, stderr)
	}
}

func TestMetricsPrintsText(t *testing.T) {
	const text = "# TYPE goclusterfs_up gauge\ngoclusterfs_up 1\n"
	f := startFakeAPI(t, map[string]any{"/metrics": text})
	out, _, err := f.run(t, "metrics")
	if err != nil || out != text {
		t.Fatalf("err=%v output=%q", err, out)
	}
}

func writeConfig(t *testing.T, content string) {
	t.Helper()
	path := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), "gfsctl", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(path) })
}

func TestSettingsPrecedence(t *testing.T) {
	f := startFakeAPI(t, map[string]any{"/metrics": "x\n"})

	writeConfig(t, "host: "+f.host+"\nport: "+f.port+"\ntoken: from-file\n")
	if _, _, err := run(t, "metrics"); err != nil || f.lastToken() != "Bearer from-file" {
		t.Fatalf("config file: err=%v token=%q", err, f.lastToken())
	}

	t.Setenv("GFSCTL_TOKEN", "from-env")
	if _, _, err := run(t, "metrics"); err != nil || f.lastToken() != "Bearer from-env" {
		t.Fatalf("env over file: err=%v token=%q", err, f.lastToken())
	}

	if _, _, err := run(t, "--token", "from-flag", "metrics"); err != nil || f.lastToken() != "Bearer from-flag" {
		t.Fatalf("flag over env: err=%v token=%q", err, f.lastToken())
	}

	writeConfig(t, "host: "+f.host+"\nport: 1\n")
	t.Setenv("GFSCTL_PORT", f.port)
	if _, _, err := run(t, "metrics"); err != nil {
		t.Fatalf("GFSCTL_PORT over file: %v", err)
	}
}

func TestBadSettingsAreRefused(t *testing.T) {
	for _, tc := range []struct {
		env, value string
		args       []string
		want       string
	}{
		{"GFSCTL_PORT", "abc", []string{"metrics"}, "GFSCTL_PORT"},
		{"GFSCTL_TIMEOUT", "soon", []string{"metrics"}, "GFSCTL_TIMEOUT"},
		{"", "", []string{"-o", "yaml", "metrics"}, "unknown output format"},
	} {
		if tc.env != "" {
			t.Setenv(tc.env, tc.value)
		}
		_, stderr, err := run(t, tc.args...)
		if err == nil || !strings.Contains(stderr, tc.want) {
			t.Errorf("%s=%q %v: err=%v stderr=%q", tc.env, tc.value, tc.args, err, stderr)
		}
		if tc.env != "" {
			os.Unsetenv(tc.env)
		}
	}
}

func TestVersionNeedsNoServer(t *testing.T) {
	out, _, err := run(t, "--port", strconv.Itoa(1), "version")
	if err != nil || !strings.HasPrefix(out, "gfsctl version ") {
		t.Fatalf("err=%v output=%q", err, out)
	}
}

func TestConfigShowHidesToken(t *testing.T) {
	const token = "abcd1234wxyz"
	out, _, err := run(t, "--token", token, "config", "show")
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"abcd", "wxyz", token} {
		if strings.Contains(out, part) {
			t.Errorf("config show reveals %q:\n%s", part, out)
		}
	}
	if !strings.Contains(out, "set (12 chars)") {
		t.Errorf("config show does not say the token is set:\n%s", out)
	}
}
