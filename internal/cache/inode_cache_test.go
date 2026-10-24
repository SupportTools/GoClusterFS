package cache

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const testTableSize = 64 * inode.SectorSize

func newTestInodeTable(t *testing.T) *inode.Table {
	t.Helper()

	dir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if dir == "" {
		dir = "."
	}
	f, err := os.CreateTemp(dir, "test_cache_inodes_*.bin")
	if err != nil {
		t.Skipf("Cannot create test file: %v", err)
	}
	path := f.Name()
	t.Cleanup(func() { os.Remove(path) })
	err = f.Truncate(testTableSize)
	f.Close()
	if err != nil {
		t.Skipf("Cannot truncate test file: %v", err)
	}

	dev, err := storage.Open(path)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	t.Cleanup(func() { dev.Close() })

	return inode.NewTable(dev, 0, testTableSize, nil, [16]byte{})
}

func claimWithSize(t *testing.T, tbl *inode.Table, num uint32, size uint64) *inode.Inode {
	t.Helper()
	ctx := context.Background()
	ino, err := tbl.ClaimSlot(ctx, num)
	if err != nil {
		t.Fatalf("ClaimSlot(%d): %v", num, err)
	}
	ino.Size = size
	if err := tbl.Write(ctx, ino); err != nil {
		t.Fatalf("Write(%d): %v", num, err)
	}
	return ino
}

func TestGetInodeDetectsSlotReuse(t *testing.T) {
	tbl := newTestInodeTable(t)
	cfg := DefaultConfig()
	cfg.InodeTTL = time.Millisecond
	mgr, err := NewManager(cfg, tbl, nil)
	if err != nil {
		t.Fatal(err)
	}

	old := claimWithSize(t, tbl, inode.InodeFirstUsable, 3)
	if _, err := mgr.GetInode(old.InodeNum); err != nil {
		t.Fatalf("GetInode: %v", err)
	}

	if err := tbl.Free(context.Background(), old.InodeNum); err != nil {
		t.Fatalf("Free: %v", err)
	}
	reused := claimWithSize(t, tbl, old.InodeNum, 10)
	if reused.DataGeneration <= old.DataGeneration || reused.MetaGeneration <= old.MetaGeneration {
		t.Errorf("reused generations (%d, %d) not above old (%d, %d)",
			reused.DataGeneration, reused.MetaGeneration, old.DataGeneration, old.MetaGeneration)
	}

	time.Sleep(2 * cfg.InodeTTL)
	got, err := mgr.GetInode(old.InodeNum)
	if err != nil {
		t.Fatalf("GetInode after reuse: %v", err)
	}
	if got.Size != 10 {
		t.Errorf("GetInode after reuse returned Size %d, want 10", got.Size)
	}
}

func TestCacheInodeClearsNegativeEntry(t *testing.T) {
	tbl := newTestInodeTable(t)
	mgr, err := NewManager(DefaultConfig(), tbl, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := mgr.GetInode(inode.InodeFirstUsable); err != inode.ErrInodeNotInUse {
		t.Fatalf("GetInode on free slot: err=%v, want ErrInodeNotInUse", err)
	}

	mgr.CacheInode(claimWithSize(t, tbl, inode.InodeFirstUsable, 5))

	got, err := mgr.GetInode(inode.InodeFirstUsable)
	if err != nil {
		t.Fatalf("GetInode after CacheInode: %v", err)
	}
	if got.Size != 5 {
		t.Errorf("GetInode returned Size %d, want 5", got.Size)
	}
}

func TestCacheInodeKeepsNewerCopy(t *testing.T) {
	tbl := newTestInodeTable(t)
	mgr, err := NewManager(DefaultConfig(), tbl, nil)
	if err != nil {
		t.Fatal(err)
	}

	claimWithSize(t, tbl, inode.InodeFirstUsable, 3)
	older, err := tbl.Read(inode.InodeFirstUsable)
	if err != nil {
		t.Fatal(err)
	}
	newer, err := tbl.Read(inode.InodeFirstUsable)
	if err != nil {
		t.Fatal(err)
	}
	newer.Size = 10
	if err := tbl.Write(context.Background(), newer); err != nil {
		t.Fatal(err)
	}

	mgr.CacheInode(newer)
	mgr.CacheInode(older)

	got, err := mgr.GetInode(inode.InodeFirstUsable)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if got.Size != 10 {
		t.Errorf("GetInode returned Size %d from the older copy, want 10", got.Size)
	}
}
