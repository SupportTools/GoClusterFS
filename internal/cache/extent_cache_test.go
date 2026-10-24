package cache

import (
	"testing"

	"github.com/supporttools/GoClusterFS/internal/inode"
)

func TestGetExtentMissesAfterInodeWrite(t *testing.T) {
	mgr, err := NewManager(DefaultConfig(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	v1 := &inode.Inode{InodeNum: 3, DataGeneration: 1, MetaGeneration: 4}
	mgr.CacheExtent(v1, 0, 100)
	if phys, ok := mgr.GetExtent(v1, 0); !ok || phys != 100 {
		t.Fatalf("GetExtent with the caching version = (%d, %v), want (100, true)", phys, ok)
	}

	v2 := &inode.Inode{InodeNum: 3, DataGeneration: 1, MetaGeneration: 5}
	if phys, ok := mgr.GetExtent(v2, 0); ok {
		t.Errorf("GetExtent after an inode write hit with block %d, want a miss", phys)
	}
}

func TestInvalidateExtentsForInodeDropsCachedInode(t *testing.T) {
	mgr, err := NewManager(DefaultConfig(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	mgr.CacheInode(&inode.Inode{InodeNum: 3, MetaGeneration: 1})
	mgr.InvalidateExtentsForInode(3)
	if mgr.IsCached(3) {
		t.Error("inode 3 still cached after InvalidateExtentsForInode")
	}
}

func TestGetExtentCountsStaleEntryAsMiss(t *testing.T) {
	mgr, err := NewManager(DefaultConfig(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	mgr.CacheExtent(&inode.Inode{InodeNum: 3, MetaGeneration: 4}, 0, 100)
	mgr.GetExtent(&inode.Inode{InodeNum: 3, MetaGeneration: 5}, 0)

	if _, hits, misses, _ := mgr.ExtentCacheStats(); hits != 0 || misses != 1 {
		t.Errorf("stale lookup counted hits=%d misses=%d, want 0 and 1", hits, misses)
	}
	if size, _, _, _ := mgr.ExtentCacheStats(); size != 0 {
		t.Errorf("stale entry still cached, size=%d", size)
	}
}
