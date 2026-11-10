package stats

import "errors"

// Package-level errors for stats collection.
var (
	// ErrInvalidTTL is returned when the cache TTL is invalid.
	ErrInvalidTTL = errors.New("stats: invalid cache TTL")

	// ErrCollectorStopped is returned when operations are attempted on a stopped collector.
	ErrCollectorStopped = errors.New("stats: collector is stopped")
)
