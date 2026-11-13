package goclusterfs

import (
	"context"
	"errors"
	"os"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// newSubdirFile adds sub/doomed.bin, holding data, with one link.
func newSubdirFile(t *testing.T, fsys *FS, data []byte) (*Dir, *File) {
	t.Helper()
	ctx := context.Background()
	sub := newSubdir(t, fsys, "sub")
	f := newRegularFile(t, fsys)
	if _, err := fsys.updateInode(ctx, f.inodeNum, func(ino *inode.Inode) error { ino.LinkCount = 1; return nil }); err != nil {
		t.Fatal(err)
	}
	subIno, err := fsys.inodeTbl.Read(sub.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.Add(ctx, subIno, "doomed.bin", uint64(f.inodeNum), dir.DT_REG); err != nil {
		t.Fatal(err)
	}
	writeAt(t, f, data, 0)
	return sub, f
}

// cloneShares clones f to /clone.bin and returns f's blocks once the clone shares them.
func cloneShares(t *testing.T, fsys *FS, f *File, n uint64) []uint64 {
	t.Helper()
	setxattr(t, f, "user.clone", "/clone.bin")
	var blocks []uint64
	for i := range n {
		blocks = append(blocks, physicalBlock(t, fsys, f.inodeNum, i))
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if !slices.ContainsFunc(blocks, func(b uint64) bool { return refcountOf(t, fsys, b) != 2 }) {
			return blocks
		}
		if time.Now().After(deadline) {
			t.Fatal("the clone never shared the file's blocks")
		}
	}
}

// expectRepairedClean runs fsck -repair and requires the check after it to be clean.
func expectRepairedClean(t *testing.T, fsys *FS) {
	t.Helper()
	before := runFsck(t, fsys, fsck.DefaultOptions())
	t.Logf("fsck before repair:%s", problemList(before))
	repair := fsck.DefaultOptions()
	repair.Mode = fsck.Repair
	runFsck(t, fsys, repair)
	expectCleanFsck(t, fsys)
}

func expectRefcounts(t *testing.T, fsys *FS, blocks []uint64, want uint32) {
	t.Helper()
	for _, b := range blocks {
		if got := refcountOf(t, fsys, b); got != want {
			t.Errorf("block %d refcount %d, want %d", b, got, want)
		}
	}
}

// An unlink waits for the file's inode lock, which another node holds, and the node crashes: replay drops the file's
// reference to the blocks a clone shares and leaves them to the clone.
func TestUnlinkCrashBeforeInodeFreeKeepsCloneBlocks(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	ctx := context.Background()
	data := pattern(3, 2*BlockSize)
	sub, f := newSubdirFile(t, fsys, data)
	blocks := cloneShares(t, fsys, f, 2)

	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	otherDev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { otherDev.Close() })
	slot := int(snapFSNodes.Add(1)-1) % 64
	var otherUUID [16]byte
	copy(otherUUID[:], "inodeholder12345")
	otherUUID[15] = byte(slot)
	other := lock.NewManager(otherDev, sb.LockBitmapOffset, sb.LockBitmapSize, otherUUID, slot)
	if err := other.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
		t.Fatal(err)
	}

	go sub.Unlink(ctx, "doomed.bin")
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		subIno, err := fsys.inodeTbl.Read(sub.inodeNum)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := fsys.dirMgr.Lookup(ctx, subIno, "doomed.bin"); errors.Is(err, dir.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the unlink never removed its entry")
		}
	}
	time.Sleep(200 * time.Millisecond)
	if ino, err := fsys.inodeTbl.Read(f.inodeNum); err != nil || ino.Magic != inode.InodeMagic {
		t.Fatalf("inode %d freed while another node holds its lock (magic %#x, err %v)", f.inodeNum, ino.Magic, err)
	}
	expectRefcounts(t, fsys, blocks, 2)
	fsys.dev.Close()
	if err := other.Release(lock.LockTypeInode, uint64(f.inodeNum)); err != nil {
		t.Fatal(err)
	}

	remounted := remountAndReplay(t, path, offset, size)
	expectCleanFsck(t, remounted)
	expectAllocated(t, remounted, blocks, true)
	expectRefcounts(t, remounted, blocks, 1)
	checkFile(t, lookupFile(t, remounted, "clone.bin"), data)
}

// A crash between an unlink's inode free and its block free: replay must not free the blocks a clone still maps.
func TestUnlinkCrashAfterInodeFreeKeepsCloneBlocks(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	data := pattern(3, 2*BlockSize)
	sub, f := newSubdirFile(t, fsys, data)
	blocks := cloneShares(t, fsys, f, 2)

	crashed := crashAfterCommit(t, fsys, "unlink: inode freed")
	go sub.Unlink(context.Background(), "doomed.bin")
	<-crashed

	remounted := remountAndReplay(t, path, offset, size)
	expectAllocated(t, remounted, blocks, true)
	checkFile(t, lookupFile(t, remounted, "clone.bin"), data)
	expectRepairedClean(t, remounted)
	expectAllocated(t, remounted, blocks, true)
	expectRefcounts(t, remounted, blocks, 1)
}

// An unlink frees its blocks, another file reuses one, and the node crashes before closing the intent: replay must
// not free the reused block.
func TestUnlinkCrashAfterBlockFreeKeepsReusedBlock(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	sub, f := newSubdirFile(t, fsys, pattern(3, 2*BlockSize))
	freed := []uint64{physicalBlock(t, fsys, f.inodeNum, 0), physicalBlock(t, fsys, f.inodeNum, 1)}
	other := createFile(t, fsys, "other.bin")

	crashed := crashAfterCommit(t, fsys, "unlink: blocks freed")
	go sub.Unlink(context.Background(), "doomed.bin")
	<-crashed

	data := pattern(9, 4*BlockSize)
	writeAt(t, other, data, 0)
	reused := false
	for i := range uint64(4) {
		reused = reused || slices.Contains(freed, physicalBlock(t, fsys, other.inodeNum, i))
	}
	if !reused {
		t.Fatal("other.bin reused none of the freed blocks")
	}

	remounted := remountAndReplay(t, path, offset, size)
	expectCleanFsck(t, remounted)
	checkFile(t, lookupFile(t, remounted, "other.bin"), data)
}

// A write fails after allocating a block, another file reuses the block, and the node crashes: replay must not map the
// block into the failed file again. The journal is restored to its state when the write freed the block.
func TestFailedWriteCrashKeepsReusedBlock(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	f := createFile(t, fsys, "f.bin")
	var journalAtFree []byte
	testHookBeforeInodeCommit = func() error { return errors.New("injected inode write failure") }
	testHookBeforeFreeingUnused = func() {
		var err error
		if journalAtFree, err = readImageRange(path, offset, size); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { testHookBeforeInodeCommit, testHookBeforeFreeingUnused = nil, nil })
	if _, errno := f.Write(context.Background(), nil, pattern(8, 4096), 0); errno != syscall.EIO {
		t.Fatalf("write with a failing inode write: errno %v, want EIO", errno)
	}
	testHookBeforeInodeCommit, testHookBeforeFreeingUnused = nil, nil
	if journalAtFree == nil {
		t.Fatal("the failed write freed no blocks")
	}

	other := createFile(t, fsys, "other.bin")
	data := pattern(9, BlockSize)
	writeAt(t, other, data, 0)
	if err := writeImageRange(path, offset, journalAtFree); err != nil {
		t.Fatal(err)
	}
	fsys.dev.Close()

	remounted := remountAndReplay(t, path, offset, size)
	ino, err := remounted.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if phys, err := remounted.extentMgr.LookupPhysical(ino, 0); err != nil || phys != extent.Hole {
		t.Errorf("f.bin block 0 maps disk block %d (err %v) after replay, want a hole", phys, err)
	}
	expectCleanFsck(t, remounted)
	checkFile(t, lookupFile(t, remounted, "other.bin"), data)
}

func readImageRange(path string, off, n uint64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b := make([]byte, n)
	_, err = f.ReadAt(b, int64(off))
	return b, err
}

func writeImageRange(path string, off uint64, b []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteAt(b, int64(off)); err != nil {
		return err
	}
	return f.Sync()
}
