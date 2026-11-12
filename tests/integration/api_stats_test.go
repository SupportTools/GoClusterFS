package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/api"
)

var apiClient = &http.Client{Timeout: 2 * time.Second}

func getAPI(t *testing.T, url string, into any) int {
	t.Helper()
	resp, err := apiClient.Get(url)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK && into != nil {
		if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
	}
	return resp.StatusCode
}

func waitForAPI(t *testing.T, base string) {
	t.Helper()
	if _, ok := waitFor(10*time.Second, func() bool { return getAPI(t, base+"/health", nil) == http.StatusOK }); !ok {
		t.Fatalf("API at %s never answered", base)
	}
}

func metricValue(body, name string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if v, ok := strings.CutPrefix(line, name+" "); ok {
			f, err := strconv.ParseFloat(v, 64)
			return f, err == nil
		}
	}
	return 0, false
}

func TestAPIStatsReportLiveValues(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)
	addrs := []string{freeLoopbackAddr(t), freeLoopbackAddr(t)}
	for addrs[1] == addrs[0] {
		addrs[1] = freeLoopbackAddr(t)
	}
	for i, addr := range addrs {
		cluster.Node(i).ExtraArgs = []string{"-api-addr", addr, "-broadcast=false", "-trim-age", "1s"}
	}
	first, second := "http://"+addrs[0], "http://"+addrs[1]

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and mount node 0: %v", err)
	}
	waitForAPI(t, first)

	fs := cluster.Node(0)
	data := bytes.Repeat([]byte("stats"), 3<<20/5)
	if err := fs.WriteFile("kept.bin", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if got, err := fs.ReadFile("kept.bin"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Read back failed: %v", err)
	}
	if err := fs.WriteFile("freed.bin", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := fs.Remove("freed.bin"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if err := fs.Mkdir("dir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if _, err := fs.Stat("kept.bin"); err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	var (
		cacheStats   api.CacheStatsResponse
		journalStats api.JournalStatsResponse
		lockStats    api.LockStatsResponse
		trimStats    api.TrimStatsResponse
	)
	_, ok := waitFor(15*time.Second, func() bool {
		return getAPI(t, first+"/api/v1/stats/cache", &cacheStats) == http.StatusOK &&
			getAPI(t, first+"/api/v1/stats/journal", &journalStats) == http.StatusOK &&
			getAPI(t, first+"/api/v1/stats/lock", &lockStats) == http.StatusOK &&
			getAPI(t, first+"/api/v1/stats/trim", &trimStats) == http.StatusOK &&
			cacheStats.InodeCache.Hits+cacheStats.InodeCache.Misses > 0 &&
			journalStats.CommitCount > 0 && journalStats.BytesWritten > 0 &&
			!trimStats.TrimEnabled
	})
	if !ok {
		t.Fatalf("stats never showed node 0's I/O:\ncache=%+v\njournal=%+v\nlock=%+v\ntrim=%+v", cacheStats, journalStats, lockStats, trimStats)
	}
	if journalStats.State != "clean" && journalStats.State != "dirty" {
		t.Errorf("journal state = %q, want clean or dirty", journalStats.State)
	}
	if trimStats.TrimBlocks != 0 || trimStats.TrimOperations != 0 || trimStats.LastTrimAt != "" {
		t.Errorf("trim on an image file = %+v, want nothing counted as discarded", trimStats)
	}

	if code := getAPI(t, first+"/api/v1/stats/sfb", nil); code != http.StatusNotFound {
		t.Errorf("GET /api/v1/stats/sfb = %d, want 404 while SFB is disabled", code)
	}
	var all api.AllStatsResponse
	if code := getAPI(t, first+"/api/v1/stats/all", &all); code != http.StatusOK || all.Cache == nil || all.Journal == nil || all.Cluster == nil || all.SFB != nil {
		t.Errorf("GET /api/v1/stats/all = %d %+v", code, all)
	}
	var local api.ClusterInfoResponse
	if code := getAPI(t, first+"/api/v1/stats/cluster", &local); code != http.StatusOK || local.NodeID == "" || local.State != "active" || local.ClusterSize != 1 {
		t.Fatalf("node 0 cluster = %d %+v", code, local)
	}

	resp, err := apiClient.Get(first + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	metrics := string(body)
	for _, name := range []string{"goclusterfs_journal_commits_total", "goclusterfs_journal_bytes_written_total", "goclusterfs_cache_inode_size", "goclusterfs_fs_size_bytes", "goclusterfs_fs_free_bytes", "goclusterfs_cluster_size"} {
		if v, ok := metricValue(metrics, name); !ok || v <= 0 {
			t.Errorf("/metrics %s = %v (found %v), want > 0", name, v, ok)
		}
	}
	if v, ok := metricValue(metrics, "goclusterfs_trim_enabled"); !ok || v != 0 {
		t.Errorf("/metrics goclusterfs_trim_enabled = %v (found %v), want 0 on an image file", v, ok)
	}
	if v, ok := metricValue(metrics, "goclusterfs_journal_state"); !ok || (v != 0 && v != 1) {
		t.Errorf("/metrics goclusterfs_journal_state = %v (found %v), want 0 or 1", v, ok)
	}
	if strings.Contains(metrics, "goclusterfs_sfb_") {
		t.Error("/metrics exports sfb metrics while SFB is disabled")
	}

	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to mount node 1: %v", err)
	}
	waitForAPI(t, second)

	var view api.ClusterInfoResponse
	if code := getAPI(t, second+"/api/v1/stats/cluster", &view); code != http.StatusOK {
		t.Fatalf("node 1 GET /api/v1/stats/cluster = %d", code)
	}
	if view.ClusterSize != 2 || view.HealthyNodes != 2 || view.NodeID == local.NodeID ||
		len(view.Peers) != 1 || view.Peers[0].NodeID != local.NodeID || view.Peers[0].State != "active" {
		t.Errorf("node 1 cluster = %+v, want 2 healthy nodes with node 0 (%s) as an active peer", view, local.NodeID)
	}

	var status api.StatusResponse
	if code := getAPI(t, second+"/api/v1/status", &status); code != http.StatusOK || status.Capacity == nil {
		t.Fatalf("node 1 GET /api/v1/status = %d %+v", code, status)
	}
	if c := status.Capacity; c.TotalBytes == 0 || c.UsedBlocks < 3 || c.UsedBlocks+c.FreeBlocks != c.TotalBlocks {
		t.Errorf("node 1 capacity = %+v, want node 0's 3 MB file counted", *c)
	}
}
