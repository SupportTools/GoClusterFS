package goclusterfs

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func runConcurrently(t *testing.T, n int, fn func(i int) error) {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- fn(i)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func tryWrite(f *File, data []byte, off int64) error {
	n, errno := f.Write(context.Background(), nil, data, off)
	if errno != 0 || int(n) != len(data) {
		return fmt.Errorf("inode %d: write %d bytes at %d: n=%d errno=%v", f.inodeNum, len(data), off, n, errno)
	}
	return nil
}

func tryTruncate(f *File, size uint64) error {
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE
	in.Size = size
	if errno := f.Setattr(context.Background(), nil, in, &fuse.AttrOut{}); errno != 0 {
		return fmt.Errorf("inode %d: truncate to %d: %v", f.inodeNum, size, errno)
	}
	return nil
}

func truncate(t *testing.T, f *File, size uint64) {
	t.Helper()
	if err := tryTruncate(f, size); err != nil {
		t.Fatal(err)
	}
}

// Threads on one node making first writes into the same fallocated block must
// not zero-fill over each other's data.
func TestConcurrentFirstWritesToFallocatedBlock(t *testing.T) {
	fsys := openTreeFS(t, newTreeFSImage(t))
	ctx := context.Background()
	f := newRegularFile(t, fsys)
	const writers, rounds = 16, 200
	for round := 0; round < rounds; round++ {
		truncate(t, f, 0)
		if errno := f.Allocate(ctx, nil, 0, BlockSize, 0); errno != 0 {
			t.Fatalf("Allocate: %v", errno)
		}
		runConcurrently(t, writers, func(w int) error {
			return tryWrite(f, pattern(byte(w+1), 4096), int64(w)<<16)
		})
		for w := 0; w < writers; w++ {
			checkAt(t, f, pattern(byte(w+1), 4096), int64(w)<<16)
		}
	}
}

// A write must not store its stale copy of the inode over a truncate that
// committed meanwhile; that copy maps blocks the truncate freed.
func TestTruncateRacingWrite(t *testing.T) {
	fsys := openTreeFS(t, newTreeFSImage(t))
	f := newRegularFile(t, fsys)
	for round := 0; round < 100; round++ {
		writeAt(t, f, pattern(1, 2*BlockSize), 0)
		runConcurrently(t, 2, func(i int) error {
			if i == 0 {
				return tryWrite(f, pattern(2, 4096), 0)
			}
			return tryTruncate(f, 0)
		})

		ino, err := fsys.inodeTbl.Read(f.inodeNum)
		if err != nil {
			t.Fatal(err)
		}
		blocks, err := fsys.extentMgr.DataBlocks(ino)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range blocks {
			if ok, err := fsys.bitmapMgr.IsAllocated(b); err != nil || !ok {
				t.Fatalf("round %d: file maps block %d, which the bitmap has free", round, b)
			}
		}
	}
}
