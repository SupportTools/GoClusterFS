package cache

import "errors"

// Cache-specific errors.
var (
	// ErrInvalidCacheSize indicates a cache size configuration is invalid.
	ErrInvalidCacheSize = errors.New("cache: invalid cache size (must be >= 0)")

	// ErrInvalidTTL indicates a TTL configuration is invalid.
	ErrInvalidTTL = errors.New("cache: invalid TTL (must be >= 0)")

	// ErrCacheMiss indicates the requested item was not in the cache.
	ErrCacheMiss = errors.New("cache: miss")

	// ErrCacheStale indicates the cached item's generation doesn't match disk.
	ErrCacheStale = errors.New("cache: stale entry")

	// ErrNegativeCached indicates the item was previously looked up and not found.
	// The caller should check disk again after NegativeTTL expires.
	ErrNegativeCached = errors.New("cache: negative cached (not found)")
)
