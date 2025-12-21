package trim

import (
	"os"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func TestCoalesceBlocks(t *testing.T) {
	m := &Manager{
		pending: []pendingBlock{
			{blockNum: 10, queuedAt: time.Now()},
			{blockNum: 11, queuedAt: time.Now()},
			{blockNum: 12, queuedAt: time.Now()},
			{blockNum: 20, queuedAt: time.Now()},
			{blockNum: 21, queuedAt: time.Now()},
			{blockNum: 50, queuedAt: time.Now()},
		},
	}

	ranges := m.coalesceBlocks()

	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}

	// Range 1: blocks 10-12
	if ranges[0].startBlock != 10 || ranges[0].count != 3 {
		t.Errorf("range 0: expected [10, 3), got [%d, %d)",
			ranges[0].startBlock, ranges[0].count)
	}

	// Range 2: blocks 20-21
	if ranges[1].startBlock != 20 || ranges[1].count != 2 {
		t.Errorf("range 1: expected [20, 2), got [%d, %d)",
			ranges[1].startBlock, ranges[1].count)
	}

	// Range 3: block 50
	if ranges[2].startBlock != 50 || ranges[2].count != 1 {
		t.Errorf("range 2: expected [50, 1), got [%d, %d)",
			ranges[2].startBlock, ranges[2].count)
	}
}

func TestCoalesceBlocksEmpty(t *testing.T) {
	m := &Manager{
		pending: []pendingBlock{},
	}

	ranges := m.coalesceBlocks()
	if ranges != nil {
		t.Errorf("expected nil for empty pending, got %v", ranges)
	}
}

func TestCoalesceBlocksSingleBlock(t *testing.T) {
	m := &Manager{
		pending: []pendingBlock{
			{blockNum: 42, queuedAt: time.Now()},
		},
	}

	ranges := m.coalesceBlocks()
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range, got %d", len(ranges))
	}
	if ranges[0].startBlock != 42 || ranges[0].count != 1 {
		t.Errorf("expected [42, 1), got [%d, %d)",
			ranges[0].startBlock, ranges[0].count)
	}
}

func TestCoalesceBlocksUnsorted(t *testing.T) {
	// Blocks out of order - should still coalesce correctly
	m := &Manager{
		pending: []pendingBlock{
			{blockNum: 12, queuedAt: time.Now()},
			{blockNum: 10, queuedAt: time.Now()},
			{blockNum: 11, queuedAt: time.Now()},
		},
	}

	ranges := m.coalesceBlocks()
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range (after sorting), got %d", len(ranges))
	}
	if ranges[0].startBlock != 10 || ranges[0].count != 3 {
		t.Errorf("expected [10, 3), got [%d, %d)",
			ranges[0].startBlock, ranges[0].count)
	}
}

func createTestDevice(t *testing.T) (*storage.Device, func()) {
	t.Helper()

	// Create temp file for testing
	f, err := os.CreateTemp("", "trim-test-*.img")
	if err != nil {
		t.Fatal(err)
	}

	// Extend to reasonable size
	if err := f.Truncate(100 * 1024 * 1024); err != nil {
		os.Remove(f.Name())
		t.Fatal(err)
	}
	f.Close()

	dev, err := storage.Open(f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatal(err)
	}

	cleanup := func() {
		dev.Close()
		os.Remove(f.Name())
	}

	return dev, cleanup
}

func TestBatchThreshold(t *testing.T) {
	dev, cleanup := createTestDevice(t)
	defer cleanup()

	cfg := Config{
		BatchSize:  10,
		MaxAge:     time.Hour, // Long enough not to trigger
		DataOffset: 0,
		Enabled:    false, // Don't actually issue ioctl on file
	}

	m := NewManager(dev, cfg)
	m.Start()
	defer m.Stop()

	// Queue 9 blocks - should not trigger flush
	for i := uint64(0); i < 9; i++ {
		m.Queue(i)
	}

	time.Sleep(50 * time.Millisecond)

	stats := m.Stats()
	if stats.BlocksDiscarded != 0 {
		t.Errorf("expected no discards yet, got %d", stats.BlocksDiscarded)
	}
	if stats.PendingBlocks != 9 {
		t.Errorf("expected 9 pending blocks, got %d", stats.PendingBlocks)
	}

	// Queue 10th block - should trigger flush
	m.Queue(9)

	time.Sleep(50 * time.Millisecond)

	stats = m.Stats()
	if stats.BlocksDiscarded != 10 {
		t.Errorf("expected 10 discards, got %d", stats.BlocksDiscarded)
	}
	if stats.PendingBlocks != 0 {
		t.Errorf("expected 0 pending blocks after flush, got %d", stats.PendingBlocks)
	}
}

func TestAgeThreshold(t *testing.T) {
	dev, cleanup := createTestDevice(t)
	defer cleanup()

	cfg := Config{
		BatchSize:  1000, // High enough not to trigger
		MaxAge:     100 * time.Millisecond,
		DataOffset: 0,
		Enabled:    false,
	}

	m := NewManager(dev, cfg)
	m.Start()
	defer m.Stop()

	// Queue a few blocks
	m.Queue(100)
	m.Queue(101)
	m.Queue(102)

	// Verify they're pending
	stats := m.Stats()
	if stats.PendingBlocks != 3 {
		t.Errorf("expected 3 pending, got %d", stats.PendingBlocks)
	}

	// Wait for age threshold (MaxAge + ticker interval)
	time.Sleep(200 * time.Millisecond)

	stats = m.Stats()
	if stats.BlocksDiscarded != 3 {
		t.Errorf("expected 3 discards after age threshold, got %d", stats.BlocksDiscarded)
	}
	if stats.PendingBlocks != 0 {
		t.Errorf("expected 0 pending after flush, got %d", stats.PendingBlocks)
	}
}

func TestQueueRange(t *testing.T) {
	m := &Manager{
		config:  Config{BatchSize: 1000},
		pending: make([]pendingBlock, 0),
	}

	m.QueueRange(100, 5)

	if len(m.pending) != 5 {
		t.Errorf("expected 5 pending, got %d", len(m.pending))
	}

	// Verify blocks are contiguous
	for i, p := range m.pending {
		if p.blockNum != 100+uint64(i) {
			t.Errorf("pending[%d] expected block %d, got %d", i, 100+i, p.blockNum)
		}
	}
}

func TestGracefulShutdown(t *testing.T) {
	dev, cleanup := createTestDevice(t)
	defer cleanup()

	cfg := Config{
		BatchSize:  1000, // Won't trigger during test
		MaxAge:     time.Hour,
		DataOffset: 0,
		Enabled:    false,
	}

	m := NewManager(dev, cfg)
	m.Start()

	// Queue blocks without triggering flush
	for i := uint64(0); i < 50; i++ {
		m.Queue(i)
	}

	// Verify blocks are pending
	stats := m.Stats()
	if stats.PendingBlocks != 50 {
		t.Errorf("expected 50 pending, got %d", stats.PendingBlocks)
	}

	// Stop should flush pending
	m.Stop()

	stats = m.Stats()
	if stats.BlocksDiscarded != 50 {
		t.Errorf("expected 50 discards on shutdown, got %d", stats.BlocksDiscarded)
	}
	if stats.PendingBlocks != 0 {
		t.Errorf("expected 0 pending after shutdown, got %d", stats.PendingBlocks)
	}
}

func TestCoalesceRatio(t *testing.T) {
	dev, cleanup := createTestDevice(t)
	defer cleanup()

	cfg := Config{
		BatchSize:  10,
		MaxAge:     time.Hour,
		DataOffset: 0,
		Enabled:    false,
	}

	m := NewManager(dev, cfg)
	m.Start()
	defer m.Stop()

	// Queue contiguous blocks 0-9 - should coalesce to 1 range
	for i := uint64(0); i < 10; i++ {
		m.Queue(i)
	}

	time.Sleep(50 * time.Millisecond)

	stats := m.Stats()
	if stats.DiscardCalls != 1 {
		t.Errorf("expected 1 discard call (all contiguous), got %d", stats.DiscardCalls)
	}
	// Coalesce ratio: 10 blocks / 1 call = 10.0
	if stats.CoalesceRatio != 10.0 {
		t.Errorf("expected coalesce ratio 10.0, got %.1f", stats.CoalesceRatio)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.BatchSize != DefaultBatchSize {
		t.Errorf("expected BatchSize %d, got %d", DefaultBatchSize, cfg.BatchSize)
	}
	if cfg.MaxAge != DefaultMaxAge {
		t.Errorf("expected MaxAge %v, got %v", DefaultMaxAge, cfg.MaxAge)
	}
	if !cfg.Enabled {
		t.Error("expected Enabled true by default")
	}
}

func TestStatsAreThreadSafe(t *testing.T) {
	dev, cleanup := createTestDevice(t)
	defer cleanup()

	cfg := Config{
		BatchSize:  1000,
		MaxAge:     time.Hour,
		DataOffset: 0,
		Enabled:    false,
	}

	m := NewManager(dev, cfg)
	m.Start()
	defer m.Stop()

	// Concurrent Queue and Stats calls
	done := make(chan bool)
	go func() {
		for i := uint64(0); i < 100; i++ {
			m.Queue(i)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			_ = m.Stats()
		}
		done <- true
	}()

	<-done
	<-done

	// Should not panic or race
	stats := m.Stats()
	if stats.BlocksQueued != 100 {
		t.Errorf("expected 100 queued, got %d", stats.BlocksQueued)
	}
}
