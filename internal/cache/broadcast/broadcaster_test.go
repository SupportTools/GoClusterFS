package broadcast

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockHandler records invalidation calls for testing.
type mockHandler struct {
	mu               sync.Mutex
	inodeInvalidated []uint32
	extentInvalid    []uint32
	clearCalled      int
}

func newMockHandler() *mockHandler {
	return &mockHandler{
		inodeInvalidated: make([]uint32, 0),
		extentInvalid:    make([]uint32, 0),
	}
}

func (m *mockHandler) InvalidateInode(inodeNum uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inodeInvalidated = append(m.inodeInvalidated, inodeNum)
}

func (m *mockHandler) InvalidateExtentsForInode(inodeNum uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.extentInvalid = append(m.extentInvalid, inodeNum)
}

func (m *mockHandler) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clearCalled++
}

func (m *mockHandler) getInodeInvalidations() []uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]uint32, len(m.inodeInvalidated))
	copy(result, m.inodeInvalidated)
	return result
}

func (m *mockHandler) getExtentInvalidations() []uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]uint32, len(m.extentInvalid))
	copy(result, m.extentInvalid)
	return result
}

func (m *mockHandler) getClearCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clearCalled
}

func TestBroadcasterDisabled(t *testing.T) {
	cfg := DisabledConfig()
	handler := newMockHandler()
	uuid := [16]byte{1, 2, 3, 4}

	b, err := NewBroadcaster(cfg, uuid, 0, handler)
	if err != nil {
		t.Fatalf("NewBroadcaster failed: %v", err)
	}

	ctx := context.Background()
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Operations should be no-ops when disabled
	b.BroadcastInvalidateInode(42, 1, 1)
	b.BroadcastInvalidateExtent(100, 1)
	b.BroadcastInvalidateAll()

	if b.IsEnabled() {
		t.Error("Expected broadcaster to report disabled")
	}

	stats := b.Stats()
	if stats.InvalidationsSent != 0 {
		t.Errorf("Expected 0 invalidations sent, got %d", stats.InvalidationsSent)
	}

	if err := b.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestBroadcasterDuplication(t *testing.T) {
	peer := [16]byte{9}
	b, err := NewBroadcaster(DisabledConfig(), [16]byte{1, 2, 3, 4}, 0, newMockHandler())
	if err != nil {
		t.Fatalf("NewBroadcaster failed: %v", err)
	}

	steps := []struct {
		seq    uint64
		dup    bool
		missed uint64
	}{
		{seq: 1000},
		{seq: 1000, dup: true},
		{seq: 1001},
		{seq: 1004, missed: 2},
		{seq: 1003},
		{seq: 1003, dup: true},
		{seq: 1002},
		{seq: 1000, dup: true},
		{seq: 1004, dup: true},
		{seq: 1004 + seqWindow + 1, missed: seqWindow},
		{seq: 1004, missed: 1},
		{seq: 1005},
	}
	for i, s := range steps {
		dup, missed := b.track(peer, s.seq)
		if dup != s.dup || missed != s.missed {
			t.Errorf("step %d: track(%d) = dup %v, missed %d; want dup %v, missed %d", i, s.seq, dup, missed, s.dup, s.missed)
		}
	}
}

func TestBroadcasterForgetsQuietSenders(t *testing.T) {
	b, err := NewBroadcaster(DisabledConfig(), [16]byte{1, 2, 3, 4}, 0, newMockHandler())
	if err != nil {
		t.Fatalf("NewBroadcaster failed: %v", err)
	}
	quiet, active := [16]byte{8}, [16]byte{9}
	b.track(quiet, 1)
	b.track(active, 1)
	b.senders[quiet].heard = time.Now().Add(-senderExpiry - time.Second)

	b.forgetQuietSenders()

	if _, ok := b.senders[quiet]; ok {
		t.Error("sender not heard from for longer than senderExpiry is still tracked")
	}
	if _, ok := b.senders[active]; !ok {
		t.Error("recently heard sender was forgotten")
	}
}

func TestBroadcasterHandleInvalidateInode(t *testing.T) {
	handler := newMockHandler()
	uuid := [16]byte{1, 2, 3, 4}
	remoteUUID := [16]byte{5, 6, 7, 8}
	cfg := DefaultConfig()
	cfg.Enabled = false

	b, err := NewBroadcaster(cfg, uuid, 0, handler)
	if err != nil {
		t.Fatalf("NewBroadcaster failed: %v", err)
	}

	// Create a message from another node
	builder := NewMessageBuilder(remoteUUID)
	msg := builder.InvalidateInode(42, 100, 200)

	// Handle the message
	b.handleMessage(msg, nil)

	// Check handler was called
	invalidations := handler.getInodeInvalidations()
	if len(invalidations) != 1 {
		t.Fatalf("Expected 1 inode invalidation, got %d", len(invalidations))
	}
	if invalidations[0] != 42 {
		t.Errorf("Expected inode 42 to be invalidated, got %d", invalidations[0])
	}
}

func TestBroadcasterHandleInvalidateExtent(t *testing.T) {
	handler := newMockHandler()
	uuid := [16]byte{1, 2, 3, 4}
	remoteUUID := [16]byte{5, 6, 7, 8}
	cfg := DefaultConfig()
	cfg.Enabled = false

	b, err := NewBroadcaster(cfg, uuid, 0, handler)
	if err != nil {
		t.Fatalf("NewBroadcaster failed: %v", err)
	}

	builder := NewMessageBuilder(remoteUUID)
	msg := builder.InvalidateExtent(100, 50)

	b.handleMessage(msg, nil)

	invalidations := handler.getExtentInvalidations()
	if len(invalidations) != 1 {
		t.Fatalf("Expected 1 extent invalidation, got %d", len(invalidations))
	}
	if invalidations[0] != 100 {
		t.Errorf("Expected inode 100 extent invalidation, got %d", invalidations[0])
	}
}

func TestBroadcasterHandleInvalidateAll(t *testing.T) {
	handler := newMockHandler()
	uuid := [16]byte{1, 2, 3, 4}
	remoteUUID := [16]byte{5, 6, 7, 8}
	cfg := DefaultConfig()
	cfg.Enabled = false

	b, err := NewBroadcaster(cfg, uuid, 0, handler)
	if err != nil {
		t.Fatalf("NewBroadcaster failed: %v", err)
	}

	builder := NewMessageBuilder(remoteUUID)
	msg := builder.InvalidateAll()

	b.handleMessage(msg, nil)

	if handler.getClearCount() != 1 {
		t.Errorf("Expected Clear to be called once, got %d", handler.getClearCount())
	}
}

func TestBroadcasterIgnoresOwnMessages(t *testing.T) {
	handler := newMockHandler()
	uuid := [16]byte{1, 2, 3, 4}
	cfg := DefaultConfig()
	cfg.Enabled = false

	b, err := NewBroadcaster(cfg, uuid, 0, handler)
	if err != nil {
		t.Fatalf("NewBroadcaster failed: %v", err)
	}

	// Create a message from ourselves
	builder := NewMessageBuilder(uuid) // Same UUID!
	msg := builder.InvalidateInode(42, 100, 200)

	// Handle the message
	b.handleMessage(msg, nil)

	// Handler should NOT be called
	invalidations := handler.getInodeInvalidations()
	if len(invalidations) != 0 {
		t.Errorf("Expected 0 invalidations (message from self), got %d", len(invalidations))
	}

	// Check stats
	stats := b.Stats()
	if stats.MessagesIgnored != 1 {
		t.Errorf("Expected 1 message ignored, got %d", stats.MessagesIgnored)
	}
}

func TestBroadcasterDuplicateMessages(t *testing.T) {
	handler := newMockHandler()
	uuid := [16]byte{1, 2, 3, 4}
	remoteUUID := [16]byte{5, 6, 7, 8}
	cfg := DefaultConfig()
	cfg.Enabled = false

	b, err := NewBroadcaster(cfg, uuid, 0, handler)
	if err != nil {
		t.Fatalf("NewBroadcaster failed: %v", err)
	}

	builder := NewMessageBuilder(remoteUUID)
	msg := builder.InvalidateInode(42, 100, 200)

	// Handle the same message twice
	b.handleMessage(msg, nil)
	b.handleMessage(msg, nil)

	// Handler should only be called once
	invalidations := handler.getInodeInvalidations()
	if len(invalidations) != 1 {
		t.Errorf("Expected 1 invalidation (duplicate dropped), got %d", len(invalidations))
	}

	stats := b.Stats()
	if stats.DuplicatesDropped != 1 {
		t.Errorf("Expected 1 duplicate dropped, got %d", stats.DuplicatesDropped)
	}
}

func TestBroadcasterStatsReset(t *testing.T) {
	handler := newMockHandler()
	uuid := [16]byte{1, 2, 3, 4}
	cfg := DefaultConfig()
	cfg.Enabled = false

	b, err := NewBroadcaster(cfg, uuid, 0, handler)
	if err != nil {
		t.Fatalf("NewBroadcaster failed: %v", err)
	}

	// Generate some stats
	remoteUUID := [16]byte{5, 6, 7, 8}
	builder := NewMessageBuilder(remoteUUID)
	msg := builder.InvalidateInode(42, 100, 200)
	b.handleMessage(msg, nil)

	stats := b.Stats()
	if stats.InvalidationsReceived == 0 {
		t.Error("Expected some invalidations received")
	}

	// Reset
	b.ResetStats()

	stats = b.Stats()
	if stats.InvalidationsReceived != 0 {
		t.Errorf("Expected 0 invalidations after reset, got %d", stats.InvalidationsReceived)
	}
}

func TestTransportDisabled(t *testing.T) {
	cfg := DisabledConfig()

	transport, err := NewTransport(cfg)
	if err != nil {
		t.Fatalf("NewTransport failed: %v", err)
	}

	ctx := context.Background()
	if err := transport.Start(ctx, nil); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Send should be no-op
	msg := &Message{Version: ProtocolVersion, Type: TypeHeartbeat}
	if err := transport.Send(msg); err != nil {
		t.Errorf("Send on disabled transport should not error: %v", err)
	}

	if err := transport.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestTransportPeerManagement(t *testing.T) {
	cfg := DisabledConfig()

	transport, err := NewTransport(cfg)
	if err != nil {
		t.Fatalf("NewTransport failed: %v", err)
	}

	if transport.PeerCount() != 0 {
		t.Errorf("Expected 0 peers initially, got %d", transport.PeerCount())
	}

	// Add peers
	transport.AddPeer("peer1", nil)
	transport.AddPeer("peer2", nil)

	if transport.PeerCount() != 2 {
		t.Errorf("Expected 2 peers, got %d", transport.PeerCount())
	}

	// Remove peer
	transport.RemovePeer("peer1")

	if transport.PeerCount() != 1 {
		t.Errorf("Expected 1 peer after removal, got %d", transport.PeerCount())
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := DefaultConfig()

	if !cfg.Enabled {
		t.Error("DefaultConfig should have Enabled=true")
	}
	if cfg.MulticastGroup != "239.192.74.75" {
		t.Errorf("Expected multicast group 239.192.74.75, got %s", cfg.MulticastGroup)
	}
	if cfg.MulticastPort != 31337 {
		t.Errorf("Expected port 31337, got %d", cfg.MulticastPort)
	}
	if cfg.UnicastPort != 31338 {
		t.Errorf("Expected unicast port 31338, got %d", cfg.UnicastPort)
	}
	if cfg.MulticastTTL != 1 {
		t.Errorf("Expected TTL 1, got %d", cfg.MulticastTTL)
	}
	if cfg.MaxMulticastFailures != 3 {
		t.Errorf("Expected max failures 3, got %d", cfg.MaxMulticastFailures)
	}
	if cfg.HeartbeatInterval != 30*time.Second {
		t.Errorf("Expected heartbeat 30s, got %v", cfg.HeartbeatInterval)
	}
}

func TestConfigDisabled(t *testing.T) {
	cfg := DisabledConfig()

	if cfg.Enabled {
		t.Error("DisabledConfig should have Enabled=false")
	}
}

func TestConcurrentDeduplication(t *testing.T) {
	peer := [16]byte{9}
	b, err := NewBroadcaster(DisabledConfig(), [16]byte{1, 2, 3, 4}, 0, newMockHandler())
	if err != nil {
		t.Fatalf("NewBroadcaster failed: %v", err)
	}

	var wg sync.WaitGroup
	var duplicates atomic.Int64
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if dup, _ := b.track(peer, 12345); dup {
					duplicates.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := duplicates.Load(); got != 999 {
		t.Errorf("duplicates = %d, want 999", got)
	}
}

// TestSameSequenceFromTwoSenders checks that dedup is per sender. Each node's
// sequence starts at its boot time in nanoseconds, so ranges can overlap.
func TestSameSequenceFromTwoSenders(t *testing.T) {
	handler := newMockHandler()
	cfg := DefaultConfig()
	cfg.Enabled = false

	b, err := NewBroadcaster(cfg, [16]byte{1}, 0, handler)
	if err != nil {
		t.Fatalf("NewBroadcaster failed: %v", err)
	}

	for i, sender := range [][16]byte{{2}, {3}} {
		msg := NewMessageBuilder(sender).InvalidateInode(uint32(10+i), 1, 1)
		msg.Sequence = 777
		b.handleMessage(msg, nil)
	}

	handler.mu.Lock()
	defer handler.mu.Unlock()
	if len(handler.inodeInvalidated) != 2 {
		t.Errorf("invalidated inodes = %v, want both senders' inodes [10 11]", handler.inodeInvalidated)
	}
}
