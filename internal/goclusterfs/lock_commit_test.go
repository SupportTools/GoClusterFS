package goclusterfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// openLockedTreeFS is openTreeFS with a lock manager, as on a real mount.
func openLockedTreeFS(t *testing.T, path string) *FS {
	t.Helper()
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { dev.Close() })
	lockPath := filepath.Join(t.TempDir(), "locks.img")
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(lockPath, 1<<20); err != nil {
		t.Fatal(err)
	}
	lockDev, err := storage.Open(lockPath)
	if err != nil {
		t.Fatalf("open %s: %v", lockPath, err)
	}
	t.Cleanup(func() { lockDev.Close() })

	var nodeUUID [16]byte
	copy(nodeUUID[:], "testnode12345678")
	lm := lock.NewManager(lockDev, 0, 1<<20, nodeUUID, 1)
	bm := bitmap.NewRegion(dev, treeFSBitmapOffset, treeFSDataBlocks, lm, nodeUUID)
	inodeTbl := inode.NewTable(dev, treeFSInodeOffset, 4096*inode.InodeSize, lm, nodeUUID)
	extentMgr := extent.NewManager(dev, bm, lm, treeFSDataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	return &FS{
		dev:        dev,
		lockMgr:    lm,
		bitmapMgr:  bm,
		inodeTbl:   inodeTbl,
		extentMgr:  extentMgr,
		dirMgr:     dir.NewManager(dev, inodeTbl, extentMgr, lm, treeFSDataOffset),
		nodeUUID:   nodeUUID,
		dataOffset: treeFSDataOffset,
	}
}

// A write over two new blocks commits the first one to the on-disk inode before
// it allocates the second. If the final inode write then fails, the retry must
// find the first block still allocated, not handed out again as the second.
func TestFailedWriteKeepsBlocksCommittedMidWrite(t *testing.T) {
	fsys := openLockedTreeFS(t, newTreeFSImage(t))
	f := newRegularFile(t, fsys)

	conflicts := 1
	testHookBeforeInodeCommit = func() error {
		if conflicts > 0 {
			conflicts--
			return extent.ErrTreeConflict
		}
		return nil
	}
	defer func() { testHookBeforeInodeCommit = nil }()

	data := pattern(3, BlockSize+4096)
	writeAt(t, f, data, 0)
	if conflicts != 0 {
		t.Fatal("the final inode write never ran")
	}

	ino, err := fsys.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatalf("read inode: %v", err)
	}
	seen := map[uint64]bool{}
	for blk := uint64(0); blk < 2; blk++ {
		ext, off, err := fsys.extentMgr.Lookup(ino, blk)
		if err != nil {
			t.Fatalf("lookup block %d: %v", blk, err)
		}
		phys := ext.PhysicalBlock + off
		if seen[phys] {
			t.Fatalf("logical blocks 0 and 1 both map to physical block %d", phys)
		}
		seen[phys] = true
		if free, err := fsys.bitmapMgr.IsFree(phys); err != nil || free {
			t.Fatalf("physical block %d is mapped but free in the bitmap (err=%v)", phys, err)
		}
	}
	checkAt(t, f, data, 0)
}
