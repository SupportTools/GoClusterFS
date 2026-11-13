package cache

import (
	"runtime"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/inode"
)

func heapInUse() int64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc)
}

const churn = 40

// TestCachesStayBounded runs forty times each cache's default capacity
// through it and checks that it keeps at most its capacity, and that the heap
// grows no further once the cache is full.
func TestCachesStayBounded(t *testing.T) {
	caches := []struct {
		name string
		add  func(m *Manager, i int)
		size func(s Stats) (n, capacity int)
	}{
		{"inode", func(m *Manager, i int) {
			m.CacheInode(&inode.Inode{InodeNum: uint32(i + 2), Magic: inode.InodeMagic, DataGeneration: 1, MetaGeneration: 1})
		}, func(s Stats) (int, int) { return s.InodeCacheSize, s.InodeCacheCapacity }},
		{"extent", func(m *Manager, i int) {
			m.CacheExtent(&inode.Inode{InodeNum: uint32(i/4 + 2), DataGeneration: 1, MetaGeneration: 1}, uint64(i%4), uint64(i))
		}, func(s Stats) (int, int) { return s.ExtentCacheSize, s.ExtentCacheCapacity }},
		{"negative", func(m *Manager, i int) {
			m.addNegative(uint32(i + 2))
		}, func(s Stats) (int, int) { return s.NegativeCacheSize, s.NegativeCacheCapacity }},
	}
	for _, c := range caches {
		t.Run(c.name, func(t *testing.T) {
			m, err := NewManager(DefaultConfig(), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, capacity := c.size(m.Stats())
			if capacity == 0 {
				t.Fatal("cache has no capacity")
			}

			base := heapInUse()
			for i := 0; i < capacity; i++ {
				c.add(m, i)
			}
			full := heapInUse() - base
			for i := capacity; i < churn*capacity; i++ {
				c.add(m, i)
				if i%capacity == 0 {
					if n, _ := c.size(m.Stats()); n > capacity {
						t.Fatalf("after %d entries the cache holds %d, capacity %d", i+1, n, capacity)
					}
				}
			}
			after := heapInUse() - base

			n, _ := c.size(m.Stats())
			t.Logf("capacity %d: %d bytes per entry when full; heap %d KB at capacity, %d KB after %dx", capacity, full/int64(capacity), full>>10, after>>10, churn)
			if n > capacity {
				t.Errorf("cache holds %d entries, capacity %d", n, capacity)
			}
			if after > full*3/2+(256<<10) {
				t.Errorf("heap grew from %d KB at capacity to %d KB after %dx capacity", full>>10, after>>10, churn)
			}
			runtime.KeepAlive(m)
		})
	}
}
