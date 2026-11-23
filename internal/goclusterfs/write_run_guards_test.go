package goclusterfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// After a clone's first block is copied, the original's extent still says
// shared for all four blocks, though only the first is back to one owner. A
// four-block overwrite of the original copies the other three and leaves the
// clone's data alone, also when the write holds the inode lock from the start.
func TestRunStopsAtBlocksACloneShares(t *testing.T) {
	for _, grow := range []bool{false, true} {
		t.Run(fmt.Sprintf("grows the file %v", grow), func(t *testing.T) {
			fsys := openSnapshotFS(t, newSnapshotFSImage(t))
			if err := fsys.InitRoot(context.Background()); err != nil {
				t.Fatal(err)
			}
			original := pattern(1, 4*BlockSize)
			f := createFile(t, fsys, "orig")
			writeAt(t, f, original, 0)
			setxattr(t, f, "user.clone", "/clone")
			clone := lookupFile(t, fsys, "clone")
			writeAt(t, clone, pattern(2, 4096), 0)
			first, second := physicalBlock(t, fsys, f.inodeNum, 0), physicalBlock(t, fsys, f.inodeNum, 1)
			if refcountOf(t, fsys, first) != 1 || refcountOf(t, fsys, second) != 2 {
				t.Fatalf("refcounts %d and %d after the clone's copy, want 1 and 2", refcountOf(t, fsys, first), refcountOf(t, fsys, second))
			}

			size := 4 * BlockSize
			if grow {
				size++
			}
			data := pattern(3, size)
			writeAt(t, f, data, 0)
			checkFile(t, clone, overwritten(original, 0, pattern(2, 4096)))
			checkFile(t, f, data)
		})
	}
}

// A write that maps new blocks one at a time persists the inode between them.
// A crash right after such an inode write leaves every block the inode maps
// holding this write's data, never the old bytes of a block freed earlier.
func TestNewBlockDataLandsBeforeTheInodeMapsIt(t *testing.T) {
	path := newTreeFSImage(t)
	fsys := openLockedTreeFSNode(t, path, newLockImage(t), 1)
	if _, err := fsys.dev.WriteAt(bytes.Repeat([]byte{0xA5}, 3*BlockSize), treeFSDataOffset); err != nil {
		t.Fatal(err)
	}
	f := newRegularFile(t, fsys)
	truncateFile(t, f, 3*BlockSize)

	var crashed atomic.Bool
	fsys.dev.SetWriteFault(func(off int64) error {
		if crashed.Load() {
			return errors.New("crashed")
		}
		if off >= treeFSInodeOffset && off < treeFSDataOffset {
			crashed.Store(true)
		}
		return nil
	})
	data := pattern(4, 3*BlockSize)
	_, _ = f.Write(context.Background(), nil, data, 0)
	fsys.dev.SetWriteFault(func(int64) error { return nil })
	if !crashed.Load() {
		t.Fatal("the write never wrote the inode")
	}

	after := openLockedTreeFSNode(t, path, newLockImage(t), 2)
	res, errno := fileNode(after, f.inodeNum).Read(context.Background(), nil, make([]byte, 3*BlockSize), 0)
	if errno != 0 {
		t.Fatalf("read after the crash: %v", errno)
	}
	got, _ := res.Bytes(make([]byte, 3*BlockSize))
	for b := range 3 {
		block := got[b*BlockSize : (b+1)*BlockSize]
		if !bytes.Equal(block, data[b*BlockSize:(b+1)*BlockSize]) && !bytes.Equal(block, make([]byte, BlockSize)) {
			t.Errorf("after the crash, block %d reads neither the write's data nor zeros: an inode write mapped it before its data landed", b)
		}
	}
}

// A first write into fallocated blocks over old disk contents zeroes the part
// of the last block it does not cover, below the end of the file.
func TestFallocatedRunZeroesTheTailOfItsLastBlock(t *testing.T) {
	const blockSize = 8 << 10
	path, lockPath := newTreeFSImage(t), newLockImage(t)
	fsys := withBlockSize(openLockedTreeFSNode(t, path, lockPath, 1), blockSize)
	if _, err := fsys.dev.WriteAt(bytes.Repeat([]byte{0xA5}, 16*blockSize), treeFSDataOffset); err != nil {
		t.Fatal(err)
	}
	f := newRegularFile(t, fsys)
	if errno := f.Allocate(context.Background(), nil, 0, 3*blockSize, 0); errno != 0 {
		t.Fatalf("fallocate: %v", errno)
	}
	data := pattern(5, 2*blockSize)
	writeAt(t, f, data, blockSize/2)
	want := make([]byte, 3*blockSize)
	copy(want[blockSize/2:], data)
	checkAt(t, f, want, 0)
}

// A run planned from an unlocked inode read is checked against the extent the
// inode maps under the lock. When another node shortened the extent meanwhile,
// the write retries instead of writing into blocks that now belong to another
// file.
func TestRunRetriesWhenItsExtentShrinks(t *testing.T) {
	const blockSize = 8 << 10
	path, lockPath := newTreeFSImage(t), newLockImage(t)
	fsys := withBlockSize(openLockedTreeFSNode(t, path, lockPath, 1), blockSize)
	peer := withBlockSize(openLockedTreeFSNode(t, path, lockPath, 2), blockSize)
	f := newRegularFile(t, fsys)
	writeAt(t, f, pattern(1, 4*blockSize), 0)
	freed := physicalBlock(t, fsys, f.inodeNum, 2)
	fsys.lockMgr.ReleaseCached()

	var other *File
	otherData := pattern(2, 2*blockSize)
	shrunk := false
	t.Cleanup(func() { beforeInPlaceWrite = nil })
	beforeInPlaceWrite = func() {
		if shrunk {
			return
		}
		shrunk = true
		peerFile := fileNode(peer, f.inodeNum)
		truncateBelowIOLock(t, peerFile, 2*blockSize)
		truncateBelowIOLock(t, peerFile, 4*blockSize)
		other = newRegularFile(t, peer)
		writeAt(t, other, otherData, 0)
		peer.lockMgr.ReleaseCached()
	}
	data := pattern(3, 4*blockSize)
	writeAt(t, f, data, 0)
	beforeInPlaceWrite = nil
	if !shrunk {
		t.Fatal("the write never wrote in place")
	}
	if got := physicalBlock(t, peer, other.inodeNum, 0); got != freed {
		t.Fatalf("the other file got block %d, not block %d that the truncate freed", got, freed)
	}
	checkAt(t, other, otherData, 0)
	checkAt(t, f, data, 0)
}
