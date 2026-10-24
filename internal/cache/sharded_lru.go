package cache

import (
	"hash/maphash"
	"sync/atomic"
)

const (
	// NumShards is the number of shards for the ShardedLRU.
	// Using 16 shards provides good parallelism without excessive memory overhead.
	// Power of 2 allows fast modulo via bitwise AND.
	NumShards = 16
	shardMask = NumShards - 1
)

// ShardedLRU is a thread-safe LRU cache that distributes entries across
// multiple shards to reduce lock contention. Each shard is an independent
// LRU cache with its own mutex.
//
// This is a drop-in replacement for LRU with the same interface, but provides
// better concurrent performance for workloads with many parallel accesses.
type ShardedLRU[K comparable, V any] struct {
	shards [NumShards]*LRU[K, V]
	seed   maphash.Seed
}

// NewShardedLRU creates a new sharded LRU cache with the specified total capacity.
// The capacity is distributed evenly across all shards.
// If capacity is less than NumShards, each shard gets capacity of 1.
func NewShardedLRU[K comparable, V any](capacity int) *ShardedLRU[K, V] {
	shardCapacity := capacity / NumShards
	if shardCapacity < 1 && capacity > 0 {
		shardCapacity = 1
	}

	s := &ShardedLRU[K, V]{
		seed: maphash.MakeSeed(),
	}

	for i := 0; i < NumShards; i++ {
		s.shards[i] = NewLRU[K, V](shardCapacity)
	}

	return s
}

// shard returns the shard index for a given key using maphash.Comparable.
func (s *ShardedLRU[K, V]) shard(key K) int {
	h := maphash.Comparable(s.seed, key)
	return int(h & shardMask)
}

// Get retrieves a value from the cache.
// If found, the entry is promoted to most-recently-used within its shard.
func (s *ShardedLRU[K, V]) Get(key K) (V, bool) {
	return s.shards[s.shard(key)].Get(key)
}

// GetIf retrieves a value if valid accepts it; otherwise the entry is removed.
func (s *ShardedLRU[K, V]) GetIf(key K, valid func(V) bool) (V, bool) {
	return s.shards[s.shard(key)].GetIf(key, valid)
}

// Peek retrieves a value without affecting LRU ordering.
func (s *ShardedLRU[K, V]) Peek(key K) (V, bool) {
	return s.shards[s.shard(key)].Peek(key)
}

// Put adds or updates a value in the cache.
// If the shard is at capacity, the least recently used entry is evicted.
func (s *ShardedLRU[K, V]) Put(key K, value V) (evictedKey K, evictedValue V, evicted bool) {
	return s.shards[s.shard(key)].Put(key, value)
}

// PutIf stores value unless key is already present and replace(old) returns false.
func (s *ShardedLRU[K, V]) PutIf(key K, value V, replace func(old V) bool) bool {
	return s.shards[s.shard(key)].PutIf(key, value, replace)
}

// Delete removes an entry from the cache.
func (s *ShardedLRU[K, V]) Delete(key K) (V, bool) {
	return s.shards[s.shard(key)].Delete(key)
}

// Clear removes all entries from all shards.
func (s *ShardedLRU[K, V]) Clear() {
	for i := 0; i < NumShards; i++ {
		s.shards[i].Clear()
	}
}

// Len returns the total number of entries across all shards.
func (s *ShardedLRU[K, V]) Len() int {
	total := 0
	for i := 0; i < NumShards; i++ {
		total += s.shards[i].Len()
	}
	return total
}

// Capacity returns the total capacity across all shards.
func (s *ShardedLRU[K, V]) Capacity() int {
	total := 0
	for i := 0; i < NumShards; i++ {
		total += s.shards[i].Capacity()
	}
	return total
}

// Stats returns aggregate cache hit/miss statistics across all shards.
func (s *ShardedLRU[K, V]) Stats() (hits, misses uint64) {
	for i := 0; i < NumShards; i++ {
		h, m := s.shards[i].Stats()
		hits += h
		misses += m
	}
	return
}

// HitRate returns the aggregate cache hit rate as a percentage (0-100).
func (s *ShardedLRU[K, V]) HitRate() float64 {
	hits, misses := s.Stats()
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total) * 100
}

// ResetStats resets the hit/miss counters for all shards.
func (s *ShardedLRU[K, V]) ResetStats() {
	for i := 0; i < NumShards; i++ {
		s.shards[i].ResetStats()
	}
}

// Keys returns all keys currently in the cache across all shards.
func (s *ShardedLRU[K, V]) Keys() []K {
	var keys []K
	for i := 0; i < NumShards; i++ {
		keys = append(keys, s.shards[i].Keys()...)
	}
	return keys
}

// ForEach calls the provided function for each entry in the cache.
// Iteration order is by shard, then by LRU order within each shard.
// If the function returns false, iteration stops.
func (s *ShardedLRU[K, V]) ForEach(fn func(key K, value V) bool) {
	for i := 0; i < NumShards; i++ {
		cont := true
		s.shards[i].ForEach(func(key K, value V) bool {
			cont = fn(key, value)
			return cont
		})
		if !cont {
			break
		}
	}
}

// ShardStats returns per-shard statistics for monitoring.
type ShardStats struct {
	Index    int
	Len      int
	Capacity int
	Hits     uint64
	Misses   uint64
}

// ShardedStats returns statistics for each individual shard.
// Useful for diagnosing uneven key distribution.
func (s *ShardedLRU[K, V]) ShardedStats() []ShardStats {
	stats := make([]ShardStats, NumShards)
	for i := 0; i < NumShards; i++ {
		hits, misses := s.shards[i].Stats()
		stats[i] = ShardStats{
			Index:    i,
			Len:      s.shards[i].Len(),
			Capacity: s.shards[i].Capacity(),
			Hits:     hits,
			Misses:   misses,
		}
	}
	return stats
}

// Cache is an interface that both LRU and ShardedLRU implement.
// This allows code to use either implementation interchangeably.
type Cache[K comparable, V any] interface {
	Get(key K) (V, bool)
	Peek(key K) (V, bool)
	Put(key K, value V) (evictedKey K, evictedValue V, evicted bool)
	Delete(key K) (V, bool)
	Clear()
	Len() int
	Capacity() int
	Stats() (hits, misses uint64)
	HitRate() float64
	ResetStats()
	Keys() []K
	ForEach(fn func(key K, value V) bool)
}

// Verify interface compliance at compile time.
var (
	_ Cache[int, string] = (*LRU[int, string])(nil)
	_ Cache[int, string] = (*ShardedLRU[int, string])(nil)
	_ atomic.Value       // Import check for atomic (used in stats aggregation)
)
