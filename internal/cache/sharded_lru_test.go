package cache

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestShardedLRUBasicOperations(t *testing.T) {
	cache := NewShardedLRU[int, string](100)

	// Test Put and Get
	cache.Put(1, "one")
	cache.Put(2, "two")
	cache.Put(3, "three")

	if v, ok := cache.Get(1); !ok || v != "one" {
		t.Errorf("Get(1) = %v, %v; want one, true", v, ok)
	}
	if v, ok := cache.Get(2); !ok || v != "two" {
		t.Errorf("Get(2) = %v, %v; want two, true", v, ok)
	}
	if v, ok := cache.Get(3); !ok || v != "three" {
		t.Errorf("Get(3) = %v, %v; want three, true", v, ok)
	}

	// Test Get for non-existent key
	if v, ok := cache.Get(999); ok {
		t.Errorf("Get(999) = %v, %v; want '', false", v, ok)
	}

	if cache.Len() != 3 {
		t.Errorf("Len() = %d; want 3", cache.Len())
	}
}

func TestShardedLRUPeek(t *testing.T) {
	cache := NewShardedLRU[int, string](10)

	cache.Put(1, "one")

	// Peek should find the value
	if v, ok := cache.Peek(1); !ok || v != "one" {
		t.Errorf("Peek(1) = %v, %v; want one, true", v, ok)
	}

	// Peek for non-existent key
	if v, ok := cache.Peek(999); ok {
		t.Errorf("Peek(999) = %v, %v; want '', false", v, ok)
	}
}

func TestShardedLRUDelete(t *testing.T) {
	cache := NewShardedLRU[int, string](100) // Large enough to avoid cross-shard eviction

	cache.Put(1, "one")
	cache.Put(2, "two")

	// Delete existing key
	if v, ok := cache.Delete(1); !ok || v != "one" {
		t.Errorf("Delete(1) = %v, %v; want one, true", v, ok)
	}

	// Verify it's gone
	if _, ok := cache.Get(1); ok {
		t.Error("Get(1) should return false after delete")
	}

	// Delete non-existent key
	if _, ok := cache.Delete(999); ok {
		t.Error("Delete(999) should return false")
	}

	if cache.Len() != 1 {
		t.Errorf("Len() = %d; want 1", cache.Len())
	}
}

func TestShardedLRUClear(t *testing.T) {
	cache := NewShardedLRU[int, string](1000) // Large capacity to avoid eviction

	for i := 0; i < 50; i++ {
		cache.Put(i, "value")
	}

	// Check that items were added (some might collide in same shard)
	if cache.Len() < 45 || cache.Len() > 50 {
		t.Errorf("Len() = %d; want ~50", cache.Len())
	}

	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("Len() after Clear() = %d; want 0", cache.Len())
	}
}

func TestShardedLRUEviction(t *testing.T) {
	// Small capacity to force eviction
	cache := NewShardedLRU[int, string](16) // 1 per shard

	// Add more items than capacity
	for i := 0; i < 32; i++ {
		cache.Put(i, "value")
	}

	// Cache should be at or near capacity
	if cache.Len() > 16 {
		t.Errorf("Len() = %d; want <= 16 (capacity)", cache.Len())
	}
}

func TestShardedLRUStats(t *testing.T) {
	cache := NewShardedLRU[int, string](100)

	// Initial stats should be zero
	hits, misses := cache.Stats()
	if hits != 0 || misses != 0 {
		t.Errorf("initial Stats() = %d, %d; want 0, 0", hits, misses)
	}

	// Generate some hits and misses
	cache.Put(1, "one")
	cache.Put(2, "two")

	cache.Get(1) // hit
	cache.Get(2) // hit
	cache.Get(3) // miss
	cache.Get(4) // miss
	cache.Get(5) // miss

	hits, misses = cache.Stats()
	if hits != 2 || misses != 3 {
		t.Errorf("Stats() = %d, %d; want 2, 3", hits, misses)
	}

	// Test hit rate
	rate := cache.HitRate()
	expected := 40.0 // 2 / 5 * 100
	if rate != expected {
		t.Errorf("HitRate() = %.2f; want %.2f", rate, expected)
	}

	// Reset stats
	cache.ResetStats()
	hits, misses = cache.Stats()
	if hits != 0 || misses != 0 {
		t.Errorf("Stats() after reset = %d, %d; want 0, 0", hits, misses)
	}
}

func TestShardedLRUKeys(t *testing.T) {
	cache := NewShardedLRU[int, string](100)

	expected := map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true}
	for k := range expected {
		cache.Put(k, "value")
	}

	keys := cache.Keys()
	if len(keys) != len(expected) {
		t.Errorf("len(Keys()) = %d; want %d", len(keys), len(expected))
	}

	for _, k := range keys {
		if !expected[k] {
			t.Errorf("unexpected key in Keys(): %d", k)
		}
	}
}

func TestShardedLRUForEach(t *testing.T) {
	cache := NewShardedLRU[int, string](100)

	for i := 0; i < 10; i++ {
		cache.Put(i, "value")
	}

	count := 0
	cache.ForEach(func(key int, value string) bool {
		count++
		return true
	})

	if count != 10 {
		t.Errorf("ForEach visited %d entries; want 10", count)
	}

	// Test early termination
	count = 0
	cache.ForEach(func(key int, value string) bool {
		count++
		return count < 5 // Stop after 5
	})

	if count != 5 {
		t.Errorf("ForEach with early stop visited %d entries; want 5", count)
	}
}

func TestShardedLRUShardedStats(t *testing.T) {
	cache := NewShardedLRU[int, string](160) // 10 per shard

	// Add items - they should distribute across shards
	for i := 0; i < 100; i++ {
		cache.Put(i, "value")
	}

	stats := cache.ShardedStats()
	if len(stats) != NumShards {
		t.Errorf("len(ShardedStats()) = %d; want %d", len(stats), NumShards)
	}

	// Verify each shard has correct index
	for i, s := range stats {
		if s.Index != i {
			t.Errorf("stats[%d].Index = %d; want %d", i, s.Index, i)
		}
		if s.Capacity != 10 {
			t.Errorf("stats[%d].Capacity = %d; want 10", i, s.Capacity)
		}
	}
}

func TestShardedLRUConcurrent(t *testing.T) {
	cache := NewShardedLRU[int, string](1000)

	const numGoroutines = 16
	const numOps = 1000

	var wg sync.WaitGroup
	var totalOps atomic.Int64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				key := id*numOps + i
				cache.Put(key, "value")
				cache.Get(key)
				totalOps.Add(2)
			}
		}(g)
	}

	wg.Wait()

	expectedOps := int64(numGoroutines * numOps * 2)
	if totalOps.Load() != expectedOps {
		t.Errorf("totalOps = %d; want %d", totalOps.Load(), expectedOps)
	}

	// All items should be in cache or evicted normally
	if cache.Len() > 1000 {
		t.Errorf("Len() = %d; exceeds capacity 1000", cache.Len())
	}
}

func TestShardedLRUCapacity(t *testing.T) {
	cache := NewShardedLRU[int, string](160)

	// Total capacity should be sum of all shard capacities
	// 160 / 16 = 10 per shard, total = 160
	if cache.Capacity() != 160 {
		t.Errorf("Capacity() = %d; want 160", cache.Capacity())
	}
}

func TestShardedLRUSmallCapacity(t *testing.T) {
	// Capacity smaller than NumShards
	cache := NewShardedLRU[int, string](5)

	// Each shard should have capacity of 1
	if cache.Capacity() != 16 { // 16 shards * 1 = 16
		t.Errorf("Capacity() = %d; want 16", cache.Capacity())
	}

	// Should still work
	cache.Put(1, "one")
	if v, ok := cache.Get(1); !ok || v != "one" {
		t.Errorf("Get(1) = %v, %v; want one, true", v, ok)
	}
}

func TestShardedLRUZeroCapacity(t *testing.T) {
	cache := NewShardedLRU[int, string](0)

	// Should create valid cache with zero capacity per shard
	cache.Put(1, "one")

	// Get should miss (zero capacity)
	if _, ok := cache.Get(1); ok {
		t.Error("Get should miss with zero capacity")
	}

	_, misses := cache.Stats()
	if misses != 1 {
		t.Errorf("misses = %d; want 1", misses)
	}
}

func TestShardedLRUUpdate(t *testing.T) {
	cache := NewShardedLRU[int, string](100)

	cache.Put(1, "one")
	cache.Put(1, "updated")

	if v, ok := cache.Get(1); !ok || v != "updated" {
		t.Errorf("Get(1) = %v, %v; want updated, true", v, ok)
	}

	// Should not increase length
	if cache.Len() != 1 {
		t.Errorf("Len() = %d; want 1", cache.Len())
	}
}

func TestShardedLRUExtentCacheKey(t *testing.T) {
	// Test with actual ExtentCacheKey type used in the project
	cache := NewShardedLRU[ExtentCacheKey, *CachedExtent](100)

	key1 := ExtentCacheKey{InodeNum: 1, LogicalBlock: 0}
	key2 := ExtentCacheKey{InodeNum: 1, LogicalBlock: 1}
	key3 := ExtentCacheKey{InodeNum: 2, LogicalBlock: 0}

	extent1 := &CachedExtent{PhysicalBlock: 100}
	extent2 := &CachedExtent{PhysicalBlock: 101}
	extent3 := &CachedExtent{PhysicalBlock: 200}

	cache.Put(key1, extent1)
	cache.Put(key2, extent2)
	cache.Put(key3, extent3)

	if v, ok := cache.Get(key1); !ok || v.PhysicalBlock != 100 {
		t.Errorf("Get(key1) = %v, %v; want PhysicalBlock=100", v, ok)
	}
	if v, ok := cache.Get(key2); !ok || v.PhysicalBlock != 101 {
		t.Errorf("Get(key2) = %v, %v; want PhysicalBlock=101", v, ok)
	}
	if v, ok := cache.Get(key3); !ok || v.PhysicalBlock != 200 {
		t.Errorf("Get(key3) = %v, %v; want PhysicalBlock=200", v, ok)
	}
}

// BenchmarkShardedLRUConcurrent measures concurrent access performance.
func BenchmarkShardedLRUConcurrent(b *testing.B) {
	cache := NewShardedLRU[int, string](10000)

	// Pre-populate
	for i := 0; i < 5000; i++ {
		cache.Put(i, "value")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				cache.Get(i % 5000)
			} else {
				cache.Put(i%5000, "value")
			}
			i++
		}
	})
}

// BenchmarkLRUConcurrent measures concurrent access on non-sharded LRU.
func BenchmarkLRUConcurrent(b *testing.B) {
	cache := NewLRU[int, string](10000)

	// Pre-populate
	for i := 0; i < 5000; i++ {
		cache.Put(i, "value")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				cache.Get(i % 5000)
			} else {
				cache.Put(i%5000, "value")
			}
			i++
		}
	})
}

// BenchmarkShardedLRUGet measures Get performance.
func BenchmarkShardedLRUGet(b *testing.B) {
	cache := NewShardedLRU[int, string](10000)

	for i := 0; i < 5000; i++ {
		cache.Put(i, "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(i % 5000)
	}
}

// BenchmarkLRUGet measures Get performance on non-sharded LRU.
func BenchmarkLRUGet(b *testing.B) {
	cache := NewLRU[int, string](10000)

	for i := 0; i < 5000; i++ {
		cache.Put(i, "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(i % 5000)
	}
}
