package cache

import (
	"container/list"
	"sync"
)

// LRU implements a thread-safe Least Recently Used cache with O(1) operations.
// It uses generics for type safety and can cache any key-value pair type.
//
// Implementation details:
//   - Hash map for O(1) lookups by key
//   - Doubly-linked list for O(1) eviction ordering
//   - Most recently accessed items move to front
//   - Least recently used items are at back (evicted first)
type LRU[K comparable, V any] struct {
	mu       sync.RWMutex
	capacity int
	items    map[K]*list.Element
	order    *list.List // Front = most recent, Back = least recent

	// Stats for monitoring
	hits   uint64
	misses uint64
}

// lruEntry holds both key and value so we can remove from map during eviction.
type lruEntry[K comparable, V any] struct {
	key   K
	value V
}

// NewLRU creates a new LRU cache with the specified capacity.
// If capacity is 0, the cache is disabled (Get always returns miss).
func NewLRU[K comparable, V any](capacity int) *LRU[K, V] {
	return &LRU[K, V]{
		capacity: capacity,
		items:    make(map[K]*list.Element, capacity),
		order:    list.New(),
	}
}

// Get retrieves a value from the cache.
// If found, the entry is promoted to most-recently-used.
// Returns the value and true if found, zero value and false otherwise.
func (l *LRU[K, V]) Get(key K) (V, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.capacity == 0 {
		var zero V
		l.misses++
		return zero, false
	}

	if elem, exists := l.items[key]; exists {
		l.order.MoveToFront(elem)
		l.hits++
		return elem.Value.(*lruEntry[K, V]).value, true
	}

	var zero V
	l.misses++
	return zero, false
}

// Peek retrieves a value without affecting LRU ordering.
// Use this for validation checks that shouldn't promote entries.
func (l *LRU[K, V]) Peek(key K) (V, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if elem, exists := l.items[key]; exists {
		return elem.Value.(*lruEntry[K, V]).value, true
	}
	var zero V
	return zero, false
}

// Put adds or updates a value in the cache.
// If the cache is at capacity, the least recently used entry is evicted.
// Returns the evicted entry's key and value if eviction occurred, plus a bool
// indicating whether an eviction happened.
func (l *LRU[K, V]) Put(key K, value V) (evictedKey K, evictedValue V, evicted bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.capacity == 0 {
		return
	}

	// Update existing entry
	if elem, exists := l.items[key]; exists {
		elem.Value.(*lruEntry[K, V]).value = value
		l.order.MoveToFront(elem)
		return
	}

	// Evict if at capacity
	if l.order.Len() >= l.capacity {
		oldest := l.order.Back()
		if oldest != nil {
			entry := l.order.Remove(oldest).(*lruEntry[K, V])
			delete(l.items, entry.key)
			evictedKey = entry.key
			evictedValue = entry.value
			evicted = true
		}
	}

	// Add new entry
	entry := &lruEntry[K, V]{key: key, value: value}
	elem := l.order.PushFront(entry)
	l.items[key] = elem

	return
}

// Delete removes an entry from the cache.
// Returns the value and true if the entry existed, zero value and false otherwise.
func (l *LRU[K, V]) Delete(key K) (V, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if elem, exists := l.items[key]; exists {
		entry := l.order.Remove(elem).(*lruEntry[K, V])
		delete(l.items, key)
		return entry.value, true
	}

	var zero V
	return zero, false
}

// Clear removes all entries from the cache.
func (l *LRU[K, V]) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.items = make(map[K]*list.Element, l.capacity)
	l.order.Init()
}

// Len returns the current number of entries in the cache.
func (l *LRU[K, V]) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.order.Len()
}

// Capacity returns the maximum capacity of the cache.
func (l *LRU[K, V]) Capacity() int {
	return l.capacity
}

// Stats returns cache hit/miss statistics.
func (l *LRU[K, V]) Stats() (hits, misses uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.hits, l.misses
}

// HitRate returns the cache hit rate as a percentage (0-100).
// Returns 0 if no operations have been performed.
func (l *LRU[K, V]) HitRate() float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	total := l.hits + l.misses
	if total == 0 {
		return 0
	}
	return float64(l.hits) / float64(total) * 100
}

// ResetStats resets the hit/miss counters.
func (l *LRU[K, V]) ResetStats() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.hits = 0
	l.misses = 0
}

// Keys returns all keys currently in the cache (in no particular order).
// This is mainly useful for debugging and testing.
func (l *LRU[K, V]) Keys() []K {
	l.mu.RLock()
	defer l.mu.RUnlock()

	keys := make([]K, 0, len(l.items))
	for k := range l.items {
		keys = append(keys, k)
	}
	return keys
}

// ForEach calls the provided function for each entry in the cache,
// from most recently used to least recently used.
// If the function returns false, iteration stops.
// Note: The cache is locked during iteration; avoid long-running operations.
func (l *LRU[K, V]) ForEach(fn func(key K, value V) bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for elem := l.order.Front(); elem != nil; elem = elem.Next() {
		entry := elem.Value.(*lruEntry[K, V])
		if !fn(entry.key, entry.value) {
			break
		}
	}
}
