package cache

import "testing"

// HitRate is already a percentage; the summary must not scale it again.
func TestStatsStringShowsHitRatePercent(t *testing.T) {
	inodes := NewShardedLRU[uint32, int](16)
	inodes.Put(1, 1)
	for range 3 {
		inodes.Get(1)
	}
	inodes.Get(2)

	s := Stats{InodeCacheSize: inodes.Len(), InodeHitRate: inodes.HitRate()}
	if got, want := s.String(), "inodes=1 (hit=75.0%), extents=0 (hit=0.0%)"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
