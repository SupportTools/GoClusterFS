package cache

import (
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/inode"
)

// TestLRU tests the generic LRU cache implementation.
func TestLRU(t *testing.T) {
	t.Run("BasicPutGet", func(t *testing.T) {
		lru := NewLRU[int, string](10)

		lru.Put(1, "one")
		lru.Put(2, "two")
		lru.Put(3, "three")

		v, ok := lru.Get(1)
		if !ok || v != "one" {
			t.Errorf("Get(1) = %v, %v; want 'one', true", v, ok)
		}

		v, ok = lru.Get(2)
		if !ok || v != "two" {
			t.Errorf("Get(2) = %v, %v; want 'two', true", v, ok)
		}

		_, ok = lru.Get(99)
		if ok {
			t.Error("Get(99) should return false for non-existent key")
		}
	})

	t.Run("Eviction", func(t *testing.T) {
		lru := NewLRU[int, string](3)

		lru.Put(1, "one")
		lru.Put(2, "two")
		lru.Put(3, "three")

		// Access 1 to make it most recent
		lru.Get(1)

		// Add 4, should evict 2 (least recently used)
		_, _, evicted := lru.Put(4, "four")
		if !evicted {
			t.Error("Expected eviction when exceeding capacity")
		}

		// 2 should be gone
		_, ok := lru.Get(2)
		if ok {
			t.Error("Key 2 should have been evicted")
		}

		// 1, 3, 4 should still exist
		if _, ok := lru.Get(1); !ok {
			t.Error("Key 1 should still exist")
		}
		if _, ok := lru.Get(3); !ok {
			t.Error("Key 3 should still exist")
		}
		if _, ok := lru.Get(4); !ok {
			t.Error("Key 4 should still exist")
		}
	})

	t.Run("Update", func(t *testing.T) {
		lru := NewLRU[int, string](10)

		lru.Put(1, "one")
		lru.Put(1, "ONE") // Update existing

		v, ok := lru.Get(1)
		if !ok || v != "ONE" {
			t.Errorf("Get(1) after update = %v, %v; want 'ONE', true", v, ok)
		}

		if lru.Len() != 1 {
			t.Errorf("Len() = %d; want 1", lru.Len())
		}
	})

	t.Run("Delete", func(t *testing.T) {
		lru := NewLRU[int, string](10)

		lru.Put(1, "one")
		lru.Put(2, "two")

		v, ok := lru.Delete(1)
		if !ok || v != "one" {
			t.Errorf("Delete(1) = %v, %v; want 'one', true", v, ok)
		}

		_, ok = lru.Get(1)
		if ok {
			t.Error("Key 1 should be deleted")
		}

		if lru.Len() != 1 {
			t.Errorf("Len() = %d; want 1", lru.Len())
		}
	})

	t.Run("Peek", func(t *testing.T) {
		lru := NewLRU[int, string](3)

		lru.Put(1, "one")
		lru.Put(2, "two")
		lru.Put(3, "three")

		// Peek at 1 (shouldn't promote it)
		v, ok := lru.Peek(1)
		if !ok || v != "one" {
			t.Errorf("Peek(1) = %v, %v; want 'one', true", v, ok)
		}

		// Add 4, should evict 1 (still LRU since Peek doesn't promote)
		lru.Put(4, "four")

		_, ok = lru.Get(1)
		if ok {
			t.Error("Key 1 should have been evicted (Peek shouldn't promote)")
		}
	})

	t.Run("Stats", func(t *testing.T) {
		lru := NewLRU[int, string](10)

		lru.Put(1, "one")

		// 2 hits
		lru.Get(1)
		lru.Get(1)

		// 3 misses
		lru.Get(99)
		lru.Get(100)
		lru.Get(101)

		hits, misses := lru.Stats()
		if hits != 2 {
			t.Errorf("hits = %d; want 2", hits)
		}
		if misses != 3 {
			t.Errorf("misses = %d; want 3", misses)
		}

		hitRate := lru.HitRate()
		expected := 40.0 // 2/(2+3) = 40%
		if hitRate != expected {
			t.Errorf("HitRate() = %v; want %v", hitRate, expected)
		}
	})

	t.Run("ZeroCapacity", func(t *testing.T) {
		lru := NewLRU[int, string](0)

		lru.Put(1, "one")

		_, ok := lru.Get(1)
		if ok {
			t.Error("Zero capacity cache should always miss")
		}
	})

	t.Run("Clear", func(t *testing.T) {
		lru := NewLRU[int, string](10)

		lru.Put(1, "one")
		lru.Put(2, "two")

		lru.Clear()

		if lru.Len() != 0 {
			t.Errorf("Len() after Clear() = %d; want 0", lru.Len())
		}

		_, ok := lru.Get(1)
		if ok {
			t.Error("Key 1 should not exist after Clear()")
		}
	})
}

// TestCachedInode tests the CachedInode validation methods.
func TestCachedInode(t *testing.T) {
	t.Run("IsStale", func(t *testing.T) {
		cached := &CachedInode{
			DataGeneration: 10,
			MetaGeneration: 20,
		}

		// Same generations - not stale
		if cached.IsStale(10, 20) {
			t.Error("IsStale() should return false when generations match")
		}

		// Different data generation - stale
		if !cached.IsStale(11, 20) {
			t.Error("IsStale() should return true when DataGeneration differs")
		}

		// Different meta generation - stale
		if !cached.IsStale(10, 21) {
			t.Error("IsStale() should return true when MetaGeneration differs")
		}
	})

	t.Run("NeedsValidation", func(t *testing.T) {
		now := time.Now()

		cached := &CachedInode{
			LastValidated: now,
		}

		// Fresh entry with TTL = 1s
		if cached.NeedsValidation(time.Second, false) {
			t.Error("Fresh entry should not need validation")
		}

		// Old entry
		cached.LastValidated = now.Add(-2 * time.Second)
		if !cached.NeedsValidation(time.Second, false) {
			t.Error("Old entry should need validation")
		}

		// Strict mode always needs validation
		cached.LastValidated = now
		if !cached.NeedsValidation(time.Second, true) {
			t.Error("Strict mode should always need validation")
		}
	})
}

// TestConfig tests configuration validation.
func TestConfig(t *testing.T) {
	t.Run("DefaultConfig", func(t *testing.T) {
		cfg := DefaultConfig()

		if err := cfg.Validate(); err != nil {
			t.Errorf("DefaultConfig() validation failed: %v", err)
		}

		if cfg.InodeCacheSize != 10000 {
			t.Errorf("InodeCacheSize = %d; want 10000", cfg.InodeCacheSize)
		}
	})

	t.Run("HighConsistencyConfig", func(t *testing.T) {
		cfg := HighConsistencyConfig()

		if !cfg.StrictMode {
			t.Error("HighConsistencyConfig should have StrictMode enabled")
		}

		if cfg.InodeTTL != time.Second {
			t.Errorf("InodeTTL = %v; want 1s", cfg.InodeTTL)
		}
	})

	t.Run("InvalidConfig", func(t *testing.T) {
		cfg := Config{InodeCacheSize: -1}
		if err := cfg.Validate(); err == nil {
			t.Error("Negative InodeCacheSize should fail validation")
		}

		cfg = Config{InodeCacheSize: 10, InodeTTL: -1}
		if err := cfg.Validate(); err == nil {
			t.Error("Negative InodeTTL should fail validation")
		}
	})
}

// mockInodeTable implements a simple mock for testing cache operations.
type mockInodeTable struct {
	inodes map[uint32]*inode.Inode
}

func newMockInodeTable() *mockInodeTable {
	return &mockInodeTable{
		inodes: make(map[uint32]*inode.Inode),
	}
}

func (m *mockInodeTable) Read(inodeNum uint32) (*inode.Inode, error) {
	ino, ok := m.inodes[inodeNum]
	if !ok {
		return &inode.Inode{}, nil // Return empty inode (Magic == 0)
	}
	return ino, nil
}

func (m *mockInodeTable) Write(ino *inode.Inode) error {
	m.inodes[ino.InodeNum] = ino
	return nil
}

func (m *mockInodeTable) setInode(ino *inode.Inode) {
	m.inodes[ino.InodeNum] = ino
}

// Note: Full integration tests for Manager.GetInode require a real inode.Table
// which needs storage.Device. Those tests belong in integration tests.
// Here we test what we can in isolation.

func TestManagerStats(t *testing.T) {
	// Full manager tests require a real inode.Table which needs storage.Device.
	// Those tests belong in integration tests. Here we just document the gap.
	t.Run("InitialStats", func(t *testing.T) {
		t.Skip("Full manager tests require integration test setup with storage.Device")
	})
}

// TestLRUConcurrency tests thread safety of LRU operations.
func TestLRUConcurrency(t *testing.T) {
	lru := NewLRU[int, int](1000)

	// Run many concurrent operations
	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			lru.Put(i, i*2)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			lru.Get(i)
		}
		done <- true
	}()

	// Deleter goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			lru.Delete(i)
		}
		done <- true
	}()

	// Wait for all goroutines
	<-done
	<-done
	<-done

	// No panic = success
}

func TestLRUForEach(t *testing.T) {
	lru := NewLRU[int, string](5)

	lru.Put(1, "one")
	lru.Put(2, "two")
	lru.Put(3, "three")

	// Access 1 to make it most recent
	lru.Get(1)

	// Collect order (should be: 1, 3, 2)
	var order []int
	lru.ForEach(func(key int, value string) bool {
		order = append(order, key)
		return true
	})

	if len(order) != 3 {
		t.Errorf("ForEach visited %d items; want 3", len(order))
	}

	// First item should be 1 (most recently accessed)
	if order[0] != 1 {
		t.Errorf("First item = %d; want 1 (most recent)", order[0])
	}
}

func TestLRUKeys(t *testing.T) {
	lru := NewLRU[int, string](10)

	lru.Put(1, "one")
	lru.Put(2, "two")
	lru.Put(3, "three")

	keys := lru.Keys()

	if len(keys) != 3 {
		t.Errorf("Keys() returned %d keys; want 3", len(keys))
	}

	// Check all keys exist (order not guaranteed)
	keySet := make(map[int]bool)
	for _, k := range keys {
		keySet[k] = true
	}

	for _, expected := range []int{1, 2, 3} {
		if !keySet[expected] {
			t.Errorf("Key %d missing from Keys()", expected)
		}
	}
}
