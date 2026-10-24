package goclusterfs

import (
	"context"
	"fmt"
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
	return openLockedTreeFSNode(t, path, newLockImage(t), 1)
}

func newLockImage(t *testing.T) string {
	t.Helper()
	lockPath := filepath.Join(t.TempDir(), "locks.img")
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(lockPath, 1<<20); err != nil {
		t.Fatal(err)
	}
	return lockPath
}

// openLockedTreeFSNode opens the image as one node of a cluster whose lock region
// is lockPath; nodes differ by slot.
func openLockedTreeFSNode(t *testing.T, path, lockPath string, slot int) *FS {
	t.Helper()
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { dev.Close() })
	lockDev, err := storage.Open(lockPath)
	if err != nil {
		t.Fatalf("open %s: %v", lockPath, err)
	}
	t.Cleanup(func() { lockDev.Close() })

	var nodeUUID [16]byte
	copy(nodeUUID[:], fmt.Sprintf("testnode%08d", slot))
	lm := lock.NewManager(lockDev, 0, 1<<20, nodeUUID, slot)
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

// Node A fills a new file with fresh blocks, writing them without the extent
// lock, while node B keeps reading it from disk. B must only ever see a hole,
// the old size, or a block's own data: never another block's data and never
// what the disk held before.
func TestReaderNeverSeesUncommittedBlocks(t *testing.T) {
	path, lockPath := newTreeFSImage(t), newLockImage(t)
	fsA := openLockedTreeFSNode(t, path, lockPath, 1)
	fsB := openLockedTreeFSNode(t, path, lockPath, 2)
	for b := uint64(0); b < treeFSDataBlocks; b++ {
		buf := storage.GetBuffer(BlockSize)
		for i := range buf.Bytes() {
			buf.Bytes()[i] = 0xEE
		}
		if _, err := fsA.dev.WriteAt(buf.Bytes(), int64(fsA.dataOffset+b*BlockSize)); err != nil {
			t.Fatalf("fill block %d: %v", b, err)
		}
		storage.PutBuffer(buf)
	}
	fileA := newRegularFile(t, fsA)
	fileB := &File{fs: fsB, inodeNum: fileA.inodeNum}

	const blocks, chunk = 16, 256 * 1024
	data := make([]byte, blocks*BlockSize)
	for i := range data {
		data[i] = byte(i/BlockSize + 1)
	}
	done := make(chan error, 1)
	go func() {
		for off := 0; off < len(data); off += chunk {
			if n, errno := fileA.Write(context.Background(), nil, data[off:off+chunk], int64(off)); errno != 0 || int(n) != chunk {
				done <- fmt.Errorf("write at %d: n=%d errno=%v", off, n, errno)
				return
			}
		}
		done <- nil
	}()

	sizes := map[int]bool{}
	buf := make([]byte, len(data))
	for writing := true; writing; {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("node A: %v", err)
			}
			writing = false
		default:
		}
		res, errno := fileB.Read(context.Background(), nil, buf, 0)
		if errno != 0 {
			t.Fatalf("node B read: errno=%v", errno)
		}
		got, _ := res.Bytes(buf)
		if len(got) > 0 {
			sizes[len(got)] = true
		}
		for off, b := range got {
			if b != 0 && b != data[off] {
				t.Fatalf("node B read byte %#x at offset %d (block %d), want 0 or %#x", b, off, off/BlockSize, data[off])
			}
		}
	}
	if len(sizes) < 3 {
		t.Fatalf("node B saw the file at only %d non-empty sizes while node A wrote it", len(sizes))
	}
}
