package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestCounter(t *testing.T) {
	c := &Counter{}

	c.Inc()
	if c.Value() != 1 {
		t.Errorf("Expected 1, got %d", c.Value())
	}

	c.Add(5)
	if c.Value() != 6 {
		t.Errorf("Expected 6, got %d", c.Value())
	}

	prev := c.Reset()
	if prev != 6 {
		t.Errorf("Expected reset to return 6, got %d", prev)
	}
	if c.Value() != 0 {
		t.Errorf("Expected 0 after reset, got %d", c.Value())
	}
}

func TestCounterConcurrent(t *testing.T) {
	c := &Counter{}
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Inc()
			}
		}()
	}
	wg.Wait()

	if c.Value() != 100000 {
		t.Errorf("Expected 100000, got %d", c.Value())
	}
}

func TestGauge(t *testing.T) {
	g := &Gauge{}

	g.Set(10)
	if g.Value() != 10 {
		t.Errorf("Expected 10, got %d", g.Value())
	}

	g.Add(-5)
	if g.Value() != 5 {
		t.Errorf("Expected 5, got %d", g.Value())
	}

	g.Add(3)
	if g.Value() != 8 {
		t.Errorf("Expected 8, got %d", g.Value())
	}
}

func TestHistogram(t *testing.T) {
	h := NewHistogram([]uint64{10, 100, 1000})

	h.Record(5)    // bucket 0 (<=10)
	h.Record(50)   // bucket 1 (<=100)
	h.Record(500)  // bucket 2 (<=1000)
	h.Record(5000) // overflow bucket

	stats := h.Stats()
	if stats.Count != 4 {
		t.Errorf("Expected count 4, got %d", stats.Count)
	}
	if stats.Min != 5 {
		t.Errorf("Expected min 5, got %d", stats.Min)
	}
	if stats.Max != 5000 {
		t.Errorf("Expected max 5000, got %d", stats.Max)
	}
}

func TestHistogramDuration(t *testing.T) {
	h := NewHistogram(DefaultLatencyBuckets)

	h.RecordDuration(100 * time.Microsecond)
	h.RecordDuration(1 * time.Millisecond)
	h.RecordDuration(10 * time.Millisecond)

	stats := h.Stats()
	if stats.Count != 3 {
		t.Errorf("Expected count 3, got %d", stats.Count)
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()

	c1 := r.Counter("test.counter")
	c1.Inc()

	c2 := r.Counter("test.counter")
	if c1 != c2 {
		t.Error("Expected same counter instance")
	}
	if c2.Value() != 1 {
		t.Errorf("Expected 1, got %d", c2.Value())
	}

	g := r.Gauge("test.gauge")
	g.Set(42)

	h := r.LatencyHistogram("test.latency")
	h.RecordDuration(100 * time.Microsecond)

	snap := r.Snapshot()
	if snap["test.counter"].(uint64) != 1 {
		t.Error("Snapshot counter value mismatch")
	}
	if snap["test.gauge"].(int64) != 42 {
		t.Error("Snapshot gauge value mismatch")
	}
}

func TestGlobalRegistry(t *testing.T) {
	// Test global convenience functions
	c := GetCounter("global.test")
	c.Inc()

	c2 := GetCounter("global.test")
	if c.Value() != c2.Value() {
		t.Error("Global counter not consistent")
	}

	RecordLatency("global.latency", 500*time.Microsecond)
	h := GetLatencyHistogram("global.latency")
	if h.Stats().Count != 1 {
		t.Error("Global latency not recorded")
	}
}
