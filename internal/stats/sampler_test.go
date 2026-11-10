package stats

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/api"
	"github.com/supporttools/GoClusterFS/internal/cache"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/trim"
)

type fakeCache struct {
	calls atomic.Int64
	hits  atomic.Uint64
}

func (f *fakeCache) Stats() cache.Stats {
	f.calls.Add(1)
	return cache.Stats{InodeCacheSize: 7, InodeCacheHits: f.hits.Load(), InodeCacheMisses: 1}
}

type fakeLocks struct{ calls atomic.Int64 }

func (f *fakeLocks) Stats() lock.Stats {
	f.calls.Add(1)
	return lock.Stats{Held: 2, Waiting: 1, HintCacheSize: 5, HintHits: 9, HintMisses: 1}
}

type fakeBlocks struct {
	calls atomic.Int64
	free  atomic.Uint64
}

func (f *fakeBlocks) TotalBlocks() uint64 { return 100 }
func (f *fakeBlocks) FreeCount() uint64 {
	f.calls.Add(1)
	return f.free.Load()
}

type fakeNodes struct{ calls atomic.Int64 }

func (f *fakeNodes) LocalUUID() [16]byte { return [16]byte{0xab} }
func (f *fakeNodes) LocalSlot() int      { return 0 }
func (f *fakeNodes) Slots() ([]*node.Slot, error) {
	f.calls.Add(1)
	return []*node.Slot{activeSlot("local", time.Now())}, nil
}

func activeSlot(host string, beat time.Time) *node.Slot {
	slot := &node.Slot{State: node.NodeStateActive, HeartbeatTime: beat.Unix(), MountTime: beat.Add(-time.Hour).Unix()}
	copy(slot.Hostname[:], host)
	return slot
}

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestCacheResponseReportsHitRatesAsFractions(t *testing.T) {
	c := cache.Stats{
		InodeCacheSize: 10, InodeCacheHits: 3, InodeCacheMisses: 1, InodeHitRate: 75,
		ExtentCacheSize: 4, ExtentCacheHits: 1, ExtentCacheMisses: 1,
		NegativeCacheSize: 2,
	}
	got := cacheResponse(c, lock.Stats{HintCacheSize: 5, HintHits: 9, HintMisses: 1})

	want := &api.CacheStatsResponse{
		InodeCache:    api.CacheHitRateInfo{Size: 10, Hits: 3, Misses: 1, HitRate: 0.75},
		ExtentCache:   api.CacheHitRateInfo{Size: 4, Hits: 1, Misses: 1, HitRate: 0.5},
		NegativeCache: api.CacheHitRateInfo{Size: 2},
		LockHintCache: api.CacheHitRateInfo{Size: 5, Hits: 9, Misses: 1, HitRate: 0.9},
	}
	if *got != *want {
		t.Errorf("cacheResponse = %+v, want %+v", *got, *want)
	}
}

func TestLockAndJournalResponses(t *testing.T) {
	l := lockResponse(lock.Stats{Held: 3, Waiting: 2, HintHits: 1, HintMisses: 3})
	if l.ActiveLocks != 3 || l.PendingLocks != 2 || l.HintCacheHitRate != 0.25 {
		t.Errorf("lockResponse = %+v", *l)
	}

	j := journalResponse(journal.JournalStats{State: journal.JournalStateDirty, CommitCount: 4, BytesWritten: 8192, AvailableSpace: 100})
	if j.State != "dirty" || j.CommitCount != 4 || j.BytesWritten != 8192 || j.AvailableSpace != 100 {
		t.Errorf("journalResponse = %+v", *j)
	}
}

func TestTrimResponseCountsBytesAndLastDiscard(t *testing.T) {
	last := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	got := trimResponse(trim.Stats{BlocksDiscarded: 3, DiscardCalls: 2, DiscardErrors: 1, PendingBlocks: 4, BatchSize: 256, LastDiscard: last})
	want := api.TrimStatsResponse{
		TrimOperations: 2, TrimBytes: 3 << 20, TrimBlocks: 3, PendingTrimBlocks: 4,
		LastTrimAt: "2026-11-10T12:00:00Z", TrimErrors: 1, TrimEnabled: true, BatchSize: 256,
	}
	if *got != want {
		t.Errorf("trimResponse = %+v, want %+v", *got, want)
	}
	if never := trimResponse(trim.Stats{}); never.LastTrimAt != "" {
		t.Errorf("LastTrimAt before any discard = %q, want empty", never.LastTrimAt)
	}
}

func TestCapacityInfo(t *testing.T) {
	got := capacityInfo(200, 50, 1<<20)
	want := api.CapacityInfo{
		TotalBytes: 200 << 20, UsedBytes: 150 << 20, FreeBytes: 50 << 20, UsedPercent: 75,
		TotalBlocks: 200, UsedBlocks: 150, FreeBlocks: 50, BlockSizeBytes: 1 << 20,
	}
	if *got != want {
		t.Errorf("capacityInfo = %+v, want %+v", *got, want)
	}
	if empty := capacityInfo(0, 0, 1<<20); empty.UsedPercent != 0 {
		t.Errorf("UsedPercent of an empty bitmap = %v, want 0", empty.UsedPercent)
	}
	if over := capacityInfo(10, 20, 1); over.FreeBlocks != 10 || over.UsedBlocks != 0 {
		t.Errorf("free above total: %+v", *over)
	}
}

func TestClusterResponseClassifiesSlots(t *testing.T) {
	now := time.Now()
	stale := activeSlot("stale", now.Add(-2*node.HeartbeatTimeout))
	dead := activeSlot("dead", now.Add(-time.Minute))
	dead.State = node.NodeStateDead
	peer := activeSlot("peer", now)
	peer.UUID = [16]byte{0x01, 0x02}
	slots := []*node.Slot{nil, activeSlot("local", now), peer, nil, stale, dead}

	got := clusterResponse([16]byte{0xab}, 1, slots, now)

	if got.NodeID != "ab000000000000000000000000000000" || got.NodeAddress != "local" || got.State != "active" || got.Role != "peer" {
		t.Errorf("local node = %+v", *got)
	}
	if got.ClusterSize != 4 || got.HealthyNodes != 2 {
		t.Errorf("ClusterSize=%d HealthyNodes=%d, want 4 and 2", got.ClusterSize, got.HealthyNodes)
	}
	if got.LastHeartbeatAt != now.UTC().Format(time.RFC3339) || got.JoinedAt == "" {
		t.Errorf("LastHeartbeatAt=%q JoinedAt=%q", got.LastHeartbeatAt, got.JoinedAt)
	}
	states := map[string]string{}
	for _, p := range got.Peers {
		states[p.Address] = p.State
	}
	if want := map[string]string{"peer": "active", "stale": "stale", "dead": "dead"}; len(states) != len(want) ||
		states["peer"] != want["peer"] || states["stale"] != want["stale"] || states["dead"] != want["dead"] {
		t.Errorf("peer states = %v, want %v", states, want)
	}
	if got.Peers[0].NodeID != "01020000000000000000000000000000" {
		t.Errorf("peer NodeID = %q", got.Peers[0].NodeID)
	}

	if alone := clusterResponse([16]byte{}, 3, make([]*node.Slot, node.MaxNodes), now); alone.State != "unknown" || alone.ClusterSize != 0 {
		t.Errorf("unreadable local slot = %+v", *alone)
	}
}

func TestSamplerAnswersFromSnapshotWithoutCallingSources(t *testing.T) {
	c, l, b, n := &fakeCache{}, &fakeLocks{}, &fakeBlocks{}, &fakeNodes{}
	b.free.Store(40)
	s := startSampler(Sources{Cache: c, Locks: l, Blocks: b, Nodes: n, BlockSize: 1 << 20}, time.Hour, time.Hour)
	defer s.Stop()

	for range 100 {
		s.CacheStats()
		s.LockStats()
		s.ClusterInfo()
		s.AllStats()
		s.Capacity()
	}
	if c.calls.Load() != 1 || l.calls.Load() != 1 || b.calls.Load() != 1 || n.calls.Load() != 1 {
		t.Errorf("sources called cache=%d locks=%d free=%d slots=%d times, want 1 each (the first sample)",
			c.calls.Load(), l.calls.Load(), b.calls.Load(), n.calls.Load())
	}

	if got := s.Capacity(); got == nil || got.FreeBlocks != 40 || got.UsedBlocks != 60 {
		t.Errorf("Capacity = %+v", got)
	}
	if got := s.CacheStats(); got == nil || got.InodeCache.Size != 7 || got.LockHintCache.Size != 5 {
		t.Errorf("CacheStats = %+v", got)
	}
	if got := s.ClusterInfo(); got == nil || got.ClusterSize != 1 {
		t.Errorf("ClusterInfo = %+v", got)
	}
	if s.JournalStats() != nil {
		t.Error("JournalStats without a journal source should be nil")
	}
	if got := s.TrimStats(); got == nil || got.TrimEnabled {
		t.Errorf("TrimStats without a TRIM source = %+v, want a disabled report", got)
	}
	if s.SFBStats() != nil {
		t.Error("SFBStats should be nil while SFB containers are disabled")
	}
	all := s.AllStats()
	if all.CollectedAt == "" || all.Cache != s.CacheStats() || all.Cluster != s.ClusterInfo() || all.SFB != nil {
		t.Errorf("AllStats = %+v", all)
	}
}

func TestSamplerRefreshesInBackgroundUntilStopped(t *testing.T) {
	c, b, n := &fakeCache{}, &fakeBlocks{}, &fakeNodes{}
	b.free.Store(10)
	s := startSampler(Sources{Cache: c, Blocks: b, Nodes: n, BlockSize: 1}, 5*time.Millisecond, 10*time.Millisecond)

	c.hits.Store(42)
	b.free.Store(90)
	waitFor(t, "new cache hits", func() bool { return s.CacheStats().InodeCache.Hits == 42 })
	waitFor(t, "new free count", func() bool { return s.Capacity().FreeBlocks == 90 })
	if s.ClusterInfo() == nil {
		t.Error("device samples dropped the cluster info")
	}

	s.Stop()
	s.Stop()
	calls := c.calls.Load() + b.calls.Load() + n.calls.Load()
	time.Sleep(50 * time.Millisecond)
	if after := c.calls.Load() + b.calls.Load() + n.calls.Load(); after != calls {
		t.Errorf("sources called %d more times after Stop", after-calls)
	}
	if s.CacheStats().InodeCache.Hits != 42 {
		t.Error("the last snapshot should stay readable after Stop")
	}
}
