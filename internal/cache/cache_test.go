package cache

import (
	"testing"
	"time"
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

// TestCachedExtent tests the CachedExtent validation methods.
func TestCachedExtent(t *testing.T) {
	t.Run("IsStale", func(t *testing.T) {
		cached := &CachedExtent{
			PhysicalBlock: 100,
			InodeDataGen:  5,
			InodeMetaGen:  7,
		}

		if cached.IsStale(5, 7) {
			t.Error("IsStale() should return false when generations match")
		}
		if !cached.IsStale(6, 7) {
			t.Error("IsStale() should return true when DataGeneration differs")
		}
		if !cached.IsStale(5, 8) {
			t.Error("IsStale() should return true when MetaGeneration differs")
		}
	})
}

// TestExtentCacheKey tests the ExtentCacheKey as a map key.
func TestExtentCacheKey(t *testing.T) {
	t.Run("Equality", func(t *testing.T) {
		key1 := ExtentCacheKey{InodeNum: 1, LogicalBlock: 10}
		key2 := ExtentCacheKey{InodeNum: 1, LogicalBlock: 10}
		key3 := ExtentCacheKey{InodeNum: 1, LogicalBlock: 11}
		key4 := ExtentCacheKey{InodeNum: 2, LogicalBlock: 10}

		if key1 != key2 {
			t.Error("Identical keys should be equal")
		}
		if key1 == key3 {
			t.Error("Keys with different LogicalBlock should not be equal")
		}
		if key1 == key4 {
			t.Error("Keys with different InodeNum should not be equal")
		}
	})

	t.Run("AsMapKey", func(t *testing.T) {
		m := make(map[ExtentCacheKey]int)
		key1 := ExtentCacheKey{InodeNum: 1, LogicalBlock: 10}
		key2 := ExtentCacheKey{InodeNum: 1, LogicalBlock: 10}

		m[key1] = 100
		if m[key2] != 100 {
			t.Error("Same key should retrieve same value from map")
		}
	})
}

// TestExtentCache tests the extent cache operations.
func TestExtentCache(t *testing.T) {
	t.Run("GetPutBasic", func(t *testing.T) {
		lru := NewLRU[ExtentCacheKey, *CachedExtent](100)

		key := ExtentCacheKey{InodeNum: 1, LogicalBlock: 5}
		cached := &CachedExtent{
			PhysicalBlock: 200,
			InodeDataGen:  10,
			CachedAt:      time.Now(),
		}

		lru.Put(key, cached)

		got, ok := lru.Get(key)
		if !ok {
			t.Fatal("Get() should find cached extent")
		}
		if got.PhysicalBlock != 200 {
			t.Errorf("PhysicalBlock = %d; want 200", got.PhysicalBlock)
		}
		if got.InodeDataGen != 10 {
			t.Errorf("InodeDataGen = %d; want 10", got.InodeDataGen)
		}
	})

	t.Run("HoleRepresentation", func(t *testing.T) {
		lru := NewLRU[ExtentCacheKey, *CachedExtent](100)

		// Physical block 0 represents a hole (sparse file)
		key := ExtentCacheKey{InodeNum: 1, LogicalBlock: 5}
		cached := &CachedExtent{
			PhysicalBlock: 0, // Hole
			InodeDataGen:  10,
			CachedAt:      time.Now(),
		}

		lru.Put(key, cached)

		got, ok := lru.Get(key)
		if !ok {
			t.Fatal("Get() should find cached hole")
		}
		if got.PhysicalBlock != 0 {
			t.Errorf("PhysicalBlock = %d; want 0 (hole)", got.PhysicalBlock)
		}
	})

	t.Run("StaleDetection", func(t *testing.T) {
		lru := NewLRU[ExtentCacheKey, *CachedExtent](100)

		key := ExtentCacheKey{InodeNum: 1, LogicalBlock: 5}
		cached := &CachedExtent{
			PhysicalBlock: 200,
			InodeDataGen:  10,
			CachedAt:      time.Now(),
		}

		lru.Put(key, cached)

		got, ok := lru.Get(key)
		if !ok {
			t.Fatal("Get() should find cached extent")
		}

		if got.IsStale(10, 0) {
			t.Error("Extent should not be stale with matching generations")
		}
		if !got.IsStale(11, 0) {
			t.Error("Extent should be stale with different DataGeneration")
		}
		if !got.IsStale(10, 1) {
			t.Error("Extent should be stale with different MetaGeneration")
		}
	})

	t.Run("MultipleInodes", func(t *testing.T) {
		lru := NewLRU[ExtentCacheKey, *CachedExtent](100)

		// Cache extents for multiple inodes
		for ino := uint32(1); ino <= 3; ino++ {
			for blk := uint64(0); blk < 5; blk++ {
				key := ExtentCacheKey{InodeNum: ino, LogicalBlock: blk}
				cached := &CachedExtent{
					PhysicalBlock: uint64(ino)*100 + blk,
					InodeDataGen:  uint64(ino),
					CachedAt:      time.Now(),
				}
				lru.Put(key, cached)
			}
		}

		// Verify each inode's extents
		for ino := uint32(1); ino <= 3; ino++ {
			for blk := uint64(0); blk < 5; blk++ {
				key := ExtentCacheKey{InodeNum: ino, LogicalBlock: blk}
				got, ok := lru.Get(key)
				if !ok {
					t.Errorf("Missing extent for inode %d block %d", ino, blk)
					continue
				}
				expectedPhys := uint64(ino)*100 + blk
				if got.PhysicalBlock != expectedPhys {
					t.Errorf("Inode %d block %d: PhysicalBlock = %d; want %d",
						ino, blk, got.PhysicalBlock, expectedPhys)
				}
			}
		}
	})

	t.Run("Eviction", func(t *testing.T) {
		// Small cache to force evictions
		lru := NewLRU[ExtentCacheKey, *CachedExtent](3)

		// Add 3 entries
		for i := uint64(0); i < 3; i++ {
			key := ExtentCacheKey{InodeNum: 1, LogicalBlock: i}
			lru.Put(key, &CachedExtent{PhysicalBlock: i * 10, InodeDataGen: 1})
		}

		// Access block 0 to make it most recent
		lru.Get(ExtentCacheKey{InodeNum: 1, LogicalBlock: 0})

		// Add block 3, should evict block 1 (LRU)
		key3 := ExtentCacheKey{InodeNum: 1, LogicalBlock: 3}
		lru.Put(key3, &CachedExtent{PhysicalBlock: 30, InodeDataGen: 1})

		// Block 1 should be evicted
		_, ok := lru.Get(ExtentCacheKey{InodeNum: 1, LogicalBlock: 1})
		if ok {
			t.Error("Block 1 should have been evicted")
		}

		// Blocks 0, 2, 3 should still exist
		for _, blk := range []uint64{0, 2, 3} {
			_, ok := lru.Get(ExtentCacheKey{InodeNum: 1, LogicalBlock: blk})
			if !ok {
				t.Errorf("Block %d should still exist", blk)
			}
		}
	})
}
