package api

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func scrape(t *testing.T, srv *Server) string {
	t.Helper()
	resp, err := http.Get("http://" + srv.Addr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer closeBody(resp)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func requireMetrics(t *testing.T, body string, want ...string) {
	t.Helper()
	for _, line := range want {
		if !strings.Contains(body, line+"\n") {
			t.Errorf("/metrics lacks %q", line)
		}
	}
}

func TestMetricsForOperators(t *testing.T) {
	srv := startServer(t, DefaultConfig().WithListenAddr("127.0.0.1:0"))
	srv.SetStatsProvider(&mockStatsProvider{
		lock: &LockStatsResponse{Types: []LockTypeStats{{Type: "inode", Acquires: 9, Blocked: 4, WaitSeconds: 2, Failed: 3}}},
		all: &AllStatsResponse{Node: &NodeStatsResponse{
			HeartbeatAgeSeconds: 1.5, PeerReplays: 2, PeerReplayFailures: 1, ReplayingSlots: []int{5},
			MountReplayedTransactions: 4, MountDiscardedJournal: true, ReservationKeyRestores: 1,
		}},
		cluster: &ClusterInfoResponse{State: "active", ClusterSize: 4, HealthyNodes: 2, Peers: []ClusterPeerInfo{
			{State: "active"}, {State: "stale"}, {State: "dead"},
		}},
	})
	requireMetrics(t, scrape(t, srv),
		`goclusterfs_lock_failures_total{type="inode"} 3`,
		`goclusterfs_node_heartbeat_age_seconds 1.5`,
		`goclusterfs_fencing_key_restores_total 1`,
		`goclusterfs_journal_peer_replays_total{result="ok"} 2`,
		`goclusterfs_journal_peer_replays_total{result="failed"} 1`,
		`goclusterfs_journal_peer_replays_running 1`,
		`goclusterfs_journal_mount_replayed_transactions 4`,
		`goclusterfs_journal_mount_discarded 1`,
		`goclusterfs_cluster_nodes{state="active"} 2`,
		`goclusterfs_cluster_nodes{state="stale"} 1`,
		`goclusterfs_cluster_nodes{state="dead"} 1`,
		`goclusterfs_cluster_nodes{state="leaving"} 0`,
	)
}

func TestMetricsCountAPIResponses(t *testing.T) {
	srv := startServer(t, DefaultConfig().WithListenAddr("127.0.0.1:0").WithAuthToken("secret").WithRateLimit(0.25, 1))
	for _, path := range []string{"/health", "/health", "/api/v1/status", "/api/v1/status", "/api/v1/snapshots/x"} {
		get(t, srv, path, "", nil)
	}
	requireMetrics(t, scrape(t, srv),
		`goclusterfs_api_requests_total{code="200",route="/health"} 2`,
		`goclusterfs_api_requests_total{code="401",route="/api/v1/status"} 1`,
		`goclusterfs_api_requests_total{code="429",route="/api/v1/status"} 1`,
		`goclusterfs_api_requests_total{code="429",route="/api/v1/snapshots"} 1`,
	)
}

func TestRouteOf(t *testing.T) {
	for p, want := range map[string]string{
		"/health":                  "/health",
		"/metrics":                 "/metrics",
		"/ui":                      "/ui",
		"/ui/assets/a.js":          "/ui",
		"/api/v1/status":           "/api/v1/status",
		"/api/v1/stats/lock":       "/api/v1/stats",
		"/api/v1/snapshots/a%2Fb":  "/api/v1/snapshots",
		"/api/v1/clones":           "/api/v1/clones",
		"/api/v1/admin/force-trim": "/api/v1/admin",
		"/api/v1/statsx":           "other",
		"/api/v1/../health":        "other",
		"/ui/../health":            "/health",
		"/x/y/z":                   "other",
	} {
		if got := routeOf(p); got != want {
			t.Errorf("routeOf(%q) = %q, want %q", p, got, want)
		}
	}
}
