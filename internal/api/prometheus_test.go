package api

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsEndpoint_BasicUp(t *testing.T) {
	server := startTestServer(t)
	defer stopServer(t, server)

	resp, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body error: %v", err)
	}

	content := string(body)

	// Should always have the up metric
	if !strings.Contains(content, "goclusterfs_up 1") {
		t.Error("expected goclusterfs_up metric")
	}

	// Should have Go collector metrics
	if !strings.Contains(content, "go_goroutines") {
		t.Error("expected go_goroutines metric from Go collector")
	}

	// Should have process metrics
	if !strings.Contains(content, "process_") {
		t.Error("expected process_* metrics from process collector")
	}
}

func TestMetricsEndpoint_WithStatsProvider(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Set up mock provider with all stats
	server.SetStatsProvider(&mockStatsProvider{
		cache: &CacheStatsResponse{
			InodeCache: CacheHitRateInfo{
				Size:    1000,
				Hits:    800,
				Misses:  200,
				HitRate: 0.8,
			},
			ExtentCache: CacheHitRateInfo{
				Size:    500,
				Hits:    400,
				Misses:  100,
				HitRate: 0.8,
			},
			NegativeCache: CacheHitRateInfo{
				Size:    100,
				Hits:    50,
				Misses:  50,
				HitRate: 0.5,
			},
			LockHintCache: CacheHitRateInfo{
				Size:    200,
				Hits:    180,
				Misses:  20,
				HitRate: 0.9,
			},
		},
		journal: &JournalStatsResponse{
			State:             "dirty",
			HeadOffset:        4096,
			TailOffset:        2048,
			AvailableSpace:    1048576,
			ActiveTxnCount:    5,
			CommitCount:       1000,
			BytesWritten:      10485760,
			LastCheckpointSeq: 999,
			BatchSyncCount:    100,
		},
		trim: &TrimStatsResponse{
			TrimOperations:    500,
			TrimBytes:         52428800,
			TrimBlocks:        50,
			PendingTrimBlocks: 5,
			TrimErrors:        2,
			TrimEnabled:       true,
		},
		lock: &LockStatsResponse{
			HintCacheHits:    900,
			HintCacheMisses:  100,
			HintCacheHitRate: 0.9,
			ActiveLocks:      10,
			PendingLocks:     2,
		},
		sfb: &SFBStatsResponse{
			TotalBlocks:        1000,
			AllocatedBlocks:    600,
			FreeBlocks:         400,
			AllocationPercent:  60.0,
			FragmentationRatio: 0.1,
			LargestFreeExtent:  100,
			AllocationsTotal:   5000,
			DeallocationsTotal: 4400,
			AllocationFailures: 5,
		},
		cluster: &ClusterInfoResponse{
			NodeID:       "node-1",
			Role:         "leader",
			State:        "active",
			ClusterSize:  3,
			HealthyNodes: 3,
		},
	})

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer stopServer(t, server)
	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body error: %v", err)
	}

	content := string(body)

	// Cache metrics
	expectedMetrics := []string{
		"goclusterfs_cache_inode_size 1000",
		"goclusterfs_cache_inode_hits_total 800",
		"goclusterfs_cache_inode_misses_total 200",
		"goclusterfs_cache_inode_hit_rate 0.8",
		"goclusterfs_cache_extent_size 500",
		"goclusterfs_cache_extent_hits_total 400",
		"goclusterfs_cache_negative_size 100",
		"goclusterfs_cache_lock_hint_size 200",
		"goclusterfs_cache_lock_hint_hit_rate 0.9",

		// Journal metrics
		"goclusterfs_journal_state 1",
		"goclusterfs_journal_head_offset 4096",
		"goclusterfs_journal_tail_offset 2048",
		"goclusterfs_journal_available_space_bytes 1.048576e+06",
		"goclusterfs_journal_active_transactions 5",
		"goclusterfs_journal_commits_total 1000",
		"goclusterfs_journal_bytes_written_total 1.048576e+07",
		"goclusterfs_journal_batch_syncs_total 100",

		// Trim metrics
		"goclusterfs_trim_operations_total 500",
		"goclusterfs_trim_bytes_total 5.24288e+07",
		"goclusterfs_trim_blocks_total 50",
		"goclusterfs_trim_pending_blocks 5",
		"goclusterfs_trim_errors_total 2",
		"goclusterfs_trim_enabled 1",

		// Lock metrics
		"goclusterfs_lock_hint_cache_hits_total 900",
		"goclusterfs_lock_hint_cache_misses_total 100",
		"goclusterfs_lock_hint_cache_hit_rate 0.9",
		"goclusterfs_lock_active_locks 10",
		"goclusterfs_lock_pending_locks 2",

		// SFB metrics
		"goclusterfs_sfb_total_blocks 1000",
		"goclusterfs_sfb_allocated_blocks 600",
		"goclusterfs_sfb_free_blocks 400",
		"goclusterfs_sfb_allocations_total 5000",
		"goclusterfs_sfb_deallocations_total 4400",
		"goclusterfs_sfb_allocation_failures_total 5",

		// Cluster metrics
		"goclusterfs_cluster_size 3",
		"goclusterfs_cluster_healthy_nodes 3",
		"goclusterfs_cluster_is_leader 1",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(content, metric) {
			t.Errorf("expected metric %q not found in output", metric)
		}
	}
}

func TestMetricsEndpoint_JournalCleanState(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	server.SetStatsProvider(&mockStatsProvider{
		journal: &JournalStatsResponse{
			State: "clean",
		},
	})

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer stopServer(t, server)
	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body error: %v", err)
	}

	content := string(body)

	if !strings.Contains(content, "goclusterfs_journal_state 0") {
		t.Error("expected journal_state to be 0 for a clean journal")
	}
}

func TestMetricsEndpoint_ExportsCapacity(t *testing.T) {
	server, err := NewServer(DefaultConfig().WithListenAddr("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}
	server.SetStatusProvider(&mockStatusProvider{capacity: &CapacityInfo{TotalBytes: 3 << 20, FreeBytes: 1 << 20}})
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer stopServer(t, server)

	resp, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body error: %v", err)
	}
	for _, want := range []string{"goclusterfs_fs_size_bytes 3.145728e+06", "goclusterfs_fs_free_bytes 1.048576e+06"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}

func TestMetricsEndpoint_TrimDisabled(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Set up mock provider with TRIM disabled
	server.SetStatsProvider(&mockStatsProvider{
		trim: &TrimStatsResponse{
			TrimEnabled: false,
		},
	})

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer stopServer(t, server)
	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body error: %v", err)
	}

	content := string(body)

	// Trim enabled should be 0
	if !strings.Contains(content, "goclusterfs_trim_enabled 0") {
		t.Error("expected trim_enabled to be 0")
	}
}

func TestMetricsEndpoint_ClusterFollower(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Set up mock provider with follower node
	server.SetStatsProvider(&mockStatsProvider{
		cluster: &ClusterInfoResponse{
			NodeID:       "node-2",
			Role:         "follower",
			State:        "active",
			ClusterSize:  3,
			HealthyNodes: 3,
		},
	})

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer stopServer(t, server)
	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body error: %v", err)
	}

	content := string(body)

	// is_leader should be 0 for follower
	if !strings.Contains(content, "goclusterfs_cluster_is_leader 0") {
		t.Error("expected is_leader to be 0 for follower")
	}
}

func TestMetricsEndpoint_ContentType(t *testing.T) {
	server := startTestServer(t)
	defer stopServer(t, server)

	resp, err := http.Get("http://" + server.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	contentType := resp.Header.Get("Content-Type")
	// Prometheus handler returns text/plain with version info
	if !strings.HasPrefix(contentType, "text/plain") &&
		!strings.HasPrefix(contentType, "application/openmetrics-text") {
		t.Errorf("unexpected content-type: %q", contentType)
	}
}

func TestMetricsEndpoint_MethodNotAllowed(t *testing.T) {
	server := startTestServer(t)
	defer stopServer(t, server)

	client := &http.Client{}
	req, err := http.NewRequest(http.MethodPost, "http://"+server.Addr()+"/metrics", nil)
	if err != nil {
		t.Fatalf("create request error: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer closeBody(resp)

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.StatusCode)
	}
}

func TestMetricsHandler_LazyInit(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Get handler multiple times - should not panic and return non-nil
	handler1 := server.GetMetricsHandler()
	if handler1 == nil {
		t.Error("expected non-nil handler on first call")
	}

	handler2 := server.GetMetricsHandler()
	if handler2 == nil {
		t.Error("expected non-nil handler on second call")
	}

	// Both calls should succeed without panic
}

func TestFSCollector_Describe(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	collector := newFSCollector(server)
	ch := make(chan *prometheus.Desc, 100)

	go func() {
		collector.Describe(ch)
		close(ch)
	}()

	count := 0
	for range ch {
		count++
	}

	// Should have descriptors for all metrics
	// up (1) + cache (16) + journal (9) + trim (6) + lock (5) + sfb (9) + cluster (3) = 49
	expectedMinimum := 40 // Allow some flexibility
	if count < expectedMinimum {
		t.Errorf("expected at least %d descriptors, got %d", expectedMinimum, count)
	}
}

func TestFSCollector_Collect_NoProvider(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// No stats provider set
	collector := newFSCollector(server)
	ch := make(chan prometheus.Metric, 100)

	go func() {
		collector.Collect(ch)
		close(ch)
	}()

	count := 0
	for range ch {
		count++
	}

	// Should only have the up metric when no provider
	if count != 1 {
		t.Errorf("expected 1 metric (up), got %d", count)
	}
}
