// Package metrics provides lightweight performance metrics collection for GoClusterFS.
// It uses atomic operations to avoid lock contention in hot paths.
package metrics

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Counter is a thread-safe monotonically increasing counter.
type Counter struct {
	value uint64
}

// Add increments the counter by delta.
func (c *Counter) Add(delta uint64) {
	atomic.AddUint64(&c.value, delta)
}

// Inc increments the counter by 1.
func (c *Counter) Inc() {
	atomic.AddUint64(&c.value, 1)
}

// Value returns the current counter value.
func (c *Counter) Value() uint64 {
	return atomic.LoadUint64(&c.value)
}

// Reset resets the counter to zero and returns the previous value.
func (c *Counter) Reset() uint64 {
	return atomic.SwapUint64(&c.value, 0)
}

// Gauge is a thread-safe value that can go up or down.
type Gauge struct {
	value int64
}

// Set sets the gauge to a specific value.
func (g *Gauge) Set(value int64) {
	atomic.StoreInt64(&g.value, value)
}

// Add adds delta to the gauge (can be negative).
func (g *Gauge) Add(delta int64) {
	atomic.AddInt64(&g.value, delta)
}

// Value returns the current gauge value.
func (g *Gauge) Value() int64 {
	return atomic.LoadInt64(&g.value)
}

// Histogram tracks the distribution of values using predefined buckets.
// It's lock-free for recording values but requires a lock to read stats.
type Histogram struct {
	buckets     []uint64   // Upper bounds for each bucket
	counts      []uint64   // Count of values in each bucket
	sum         uint64     // Sum of all values (for average calculation)
	count       uint64     // Total count of values
	min         uint64     // Minimum value seen
	max         uint64     // Maximum value seen
	minSet      uint32     // Whether min has been set
}

// NewHistogram creates a histogram with the given bucket upper bounds.
// Buckets should be sorted in ascending order.
// A final +Inf bucket is implicit and not needed.
func NewHistogram(buckets []uint64) *Histogram {
	return &Histogram{
		buckets: buckets,
		counts:  make([]uint64, len(buckets)+1), // +1 for overflow bucket
		min:     ^uint64(0),                     // Max uint64 initially
	}
}

// Record records a value in the histogram.
func (h *Histogram) Record(value uint64) {
	// Find bucket
	bucket := len(h.buckets) // Default to overflow bucket
	for i, upper := range h.buckets {
		if value <= upper {
			bucket = i
			break
		}
	}
	atomic.AddUint64(&h.counts[bucket], 1)
	atomic.AddUint64(&h.sum, value)
	atomic.AddUint64(&h.count, 1)

	// Update min/max (racy but approximate is OK for metrics)
	if value < atomic.LoadUint64(&h.min) {
		atomic.StoreUint64(&h.min, value)
		atomic.StoreUint32(&h.minSet, 1)
	}
	if value > atomic.LoadUint64(&h.max) {
		atomic.StoreUint64(&h.max, value)
	}
}

// RecordDuration is a convenience method for recording time.Duration values as microseconds.
func (h *Histogram) RecordDuration(d time.Duration) {
	h.Record(uint64(d.Microseconds()))
}

// Stats returns summary statistics for the histogram.
func (h *Histogram) Stats() HistogramStats {
	count := atomic.LoadUint64(&h.count)
	if count == 0 {
		return HistogramStats{}
	}
	sum := atomic.LoadUint64(&h.sum)
	return HistogramStats{
		Count: count,
		Sum:   sum,
		Avg:   float64(sum) / float64(count),
		Min:   atomic.LoadUint64(&h.min),
		Max:   atomic.LoadUint64(&h.max),
	}
}

// HistogramStats contains summary statistics from a histogram.
type HistogramStats struct {
	Count uint64
	Sum   uint64
	Avg   float64
	Min   uint64
	Max   uint64
}

// String returns a human-readable summary.
func (s HistogramStats) String() string {
	if s.Count == 0 {
		return "count=0"
	}
	return fmt.Sprintf("count=%d avg=%.2fµs min=%dµs max=%dµs",
		s.Count, s.Avg, s.Min, s.Max)
}

// DefaultLatencyBuckets are bucket boundaries in microseconds for latency histograms.
// Covers 10µs to 10s range.
var DefaultLatencyBuckets = []uint64{
	10,      // 10µs
	50,      // 50µs
	100,     // 100µs
	250,     // 250µs
	500,     // 500µs
	1000,    // 1ms
	2500,    // 2.5ms
	5000,    // 5ms
	10000,   // 10ms
	25000,   // 25ms
	50000,   // 50ms
	100000,  // 100ms
	250000,  // 250ms
	500000,  // 500ms
	1000000, // 1s
	10000000, // 10s
}

// DefaultThroughputBuckets are bucket boundaries in bytes for throughput histograms.
var DefaultThroughputBuckets = []uint64{
	4096,      // 4KB
	16384,     // 16KB
	65536,     // 64KB
	262144,    // 256KB
	1048576,   // 1MB
	4194304,   // 4MB
	16777216,  // 16MB
	67108864,  // 64MB
	268435456, // 256MB
}

// Registry holds all metrics for the filesystem.
type Registry struct {
	mu      sync.RWMutex
	metrics map[string]interface{}
}

// NewRegistry creates a new metrics registry.
func NewRegistry() *Registry {
	return &Registry{
		metrics: make(map[string]interface{}),
	}
}

// Counter returns or creates a counter with the given name.
func (r *Registry) Counter(name string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()

	if m, ok := r.metrics[name]; ok {
		if c, ok := m.(*Counter); ok {
			return c
		}
	}
	c := &Counter{}
	r.metrics[name] = c
	return c
}

// Gauge returns or creates a gauge with the given name.
func (r *Registry) Gauge(name string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()

	if m, ok := r.metrics[name]; ok {
		if g, ok := m.(*Gauge); ok {
			return g
		}
	}
	g := &Gauge{}
	r.metrics[name] = g
	return g
}

// Histogram returns or creates a histogram with the given name and buckets.
func (r *Registry) Histogram(name string, buckets []uint64) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()

	if m, ok := r.metrics[name]; ok {
		if h, ok := m.(*Histogram); ok {
			return h
		}
	}
	h := NewHistogram(buckets)
	r.metrics[name] = h
	return h
}

// LatencyHistogram returns or creates a latency histogram with default buckets.
func (r *Registry) LatencyHistogram(name string) *Histogram {
	return r.Histogram(name, DefaultLatencyBuckets)
}

// Snapshot returns a copy of all current metric values.
func (r *Registry) Snapshot() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snap := make(map[string]interface{}, len(r.metrics))
	for name, m := range r.metrics {
		switch v := m.(type) {
		case *Counter:
			snap[name] = v.Value()
		case *Gauge:
			snap[name] = v.Value()
		case *Histogram:
			snap[name] = v.Stats()
		}
	}
	return snap
}

// String returns a human-readable summary of all metrics.
func (r *Registry) String() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Sort names for consistent output
	names := make([]string, 0, len(r.metrics))
	for name := range r.metrics {
		names = append(names, name)
	}
	sort.Strings(names)

	var result string
	for _, name := range names {
		m := r.metrics[name]
		switch v := m.(type) {
		case *Counter:
			result += fmt.Sprintf("%s: %d\n", name, v.Value())
		case *Gauge:
			result += fmt.Sprintf("%s: %d\n", name, v.Value())
		case *Histogram:
			result += fmt.Sprintf("%s: %s\n", name, v.Stats())
		}
	}
	return result
}

// Global is the default global metrics registry.
var Global = NewRegistry()

// Convenience functions that operate on the global registry.

// GetCounter returns a counter from the global registry.
func GetCounter(name string) *Counter {
	return Global.Counter(name)
}

// GetGauge returns a gauge from the global registry.
func GetGauge(name string) *Gauge {
	return Global.Gauge(name)
}

// GetLatencyHistogram returns a latency histogram from the global registry.
func GetLatencyHistogram(name string) *Histogram {
	return Global.LatencyHistogram(name)
}

// RecordLatency records a latency value to a named histogram in the global registry.
func RecordLatency(name string, d time.Duration) {
	Global.LatencyHistogram(name).RecordDuration(d)
}
