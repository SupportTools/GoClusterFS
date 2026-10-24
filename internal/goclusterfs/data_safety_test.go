package goclusterfs

import (
	"context"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func fillBlock(t *testing.T, fsys *FS, physBlock uint64, seed byte) {
	t.Helper()
	buf := storage.GetBuffer(BlockSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), pattern(seed, BlockSize))
	if _, err := fsys.dev.WriteAt(buf.Bytes(), int64(fsys.dataOffset+physBlock*BlockSize)); err != nil {
		t.Fatalf("fill block %d: %v", physBlock, err)
	}
}

// A partial write into one block of a fallocated range must not expose what
// the disk held before, in that block or in the other fallocated blocks.
func TestFallocatedBlocksReadZerosAfterPartialWrite(t *testing.T) {
	path := newTreeFSImage(t)
	fsys := openTreeFS(t, path)
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatalf("InitRoot: %v", err)
	}

	f := newRegularFile(t, fsys)
	if errno := f.Allocate(ctx, nil, 0, 3*BlockSize, 0); errno != 0 {
		t.Fatalf("Allocate: %v", errno)
	}
	ino, err := fsys.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	for lb := uint64(0); lb < 3; lb++ {
		ext, off, err := fsys.extentMgr.Lookup(ino, lb)
		if err != nil {
			t.Fatalf("lookup block %d: %v", lb, err)
		}
		fillBlock(t, fsys, ext.PhysicalBlock+off, byte(0xA0+lb))
	}

	data := pattern(7, 4096)
	writeAt(t, f, data, BlockSize+8192)

	verify := func(f *File) {
		t.Helper()
		checkAt(t, f, make([]byte, BlockSize), 0)
		checkAt(t, f, make([]byte, 8192), BlockSize)
		checkAt(t, f, data, BlockSize+8192)
		checkAt(t, f, make([]byte, BlockSize-8192-4096), BlockSize+8192+4096)
		checkAt(t, f, make([]byte, BlockSize), 2*BlockSize)
	}
	verify(f)
	verify(&File{fs: openTreeFS(t, path), inodeNum: f.inodeNum})
}

// Physical block 0 is an ordinary data block, not a hole.
func TestReadDataInPhysicalBlockZero(t *testing.T) {
	fsys := openTreeFS(t, newTreeFSImage(t))
	f := newRegularFile(t, fsys)
	data := pattern(3, 4096)
	writeAt(t, f, data, 0)

	ino, err := fsys.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	ext, off, err := fsys.extentMgr.Lookup(ino, 0)
	if err != nil {
		t.Fatal(err)
	}
	if phys := ext.PhysicalBlock + off; phys != 0 {
		t.Fatalf("file got physical block %d; this test needs block 0", phys)
	}
	checkAt(t, f, data, 0)
}
