package goclusterfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	treeFSBitmapOffset = 1 << 20
	treeFSInodeOffset  = 2 << 20
	treeFSDataOffset   = 3 << 20
	treeFSDataBlocks   = 61
)

// openTreeFS builds an FS over an existing image with fresh managers, as a mount would.
func openTreeFS(t *testing.T, path string) *FS {
	t.Helper()
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { dev.Close() })

	var nodeUUID [16]byte
	copy(nodeUUID[:], "testnode12345678")
	bm := bitmap.NewRegion(dev, treeFSBitmapOffset, treeFSDataBlocks, nil, nodeUUID)
	inodeTbl := inode.NewTable(dev, treeFSInodeOffset, 4096*inode.InodeSize, nil, nodeUUID)
	extentMgr := extent.NewManager(dev, bm, nil, treeFSDataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	return &FS{
		dev:        dev,
		bitmapMgr:  bm,
		inodeTbl:   inodeTbl,
		extentMgr:  extentMgr,
		dirMgr:     dir.NewManager(dev, inodeTbl, extentMgr, nil, treeFSDataOffset),
		nodeUUID:   nodeUUID,
		dataOffset: treeFSDataOffset,
	}
}

func newTreeFSImage(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fs.img")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, treeFSDataOffset+treeFSDataBlocks*BlockSize); err != nil {
		t.Fatal(err)
	}
	return path
}

func newRegularFile(t *testing.T, fsys *FS) *File {
	t.Helper()
	num, err := fsys.inodeTbl.FindFreeSlot()
	if err != nil {
		t.Fatalf("find free inode: %v", err)
	}
	ino, err := fsys.inodeTbl.ClaimSlot(context.Background(), num)
	if err != nil {
		t.Fatalf("claim inode %d: %v", num, err)
	}
	ino.Mode = inode.S_IFREG | 0o644
	if err := fsys.inodeTbl.Write(context.Background(), ino); err != nil {
		t.Fatalf("write inode: %v", err)
	}
	return &File{fs: fsys, inodeNum: ino.InodeNum}
}

func pattern(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i%251)
	}
	return b
}

func writeAt(t *testing.T, f *File, data []byte, off int64) {
	t.Helper()
	n, errno := f.Write(context.Background(), nil, data, off)
	if errno != 0 || int(n) != len(data) {
		t.Fatalf("inode %d: write %d bytes at %d: n=%d errno=%v", f.inodeNum, len(data), off, n, errno)
	}
}

func checkAt(t *testing.T, f *File, want []byte, off int64) {
	t.Helper()
	res, errno := f.Read(context.Background(), nil, make([]byte, len(want)), off)
	if errno != 0 {
		t.Fatalf("inode %d: read at %d: errno=%v", f.inodeNum, off, errno)
	}
	got, _ := res.Bytes(make([]byte, len(want)))
	if !bytes.Equal(got, want) {
		t.Fatalf("inode %d: data mismatch at offset %d (%d bytes)", f.inodeNum, off, len(want))
	}
}

func TestWriteBeyondTwoExtents(t *testing.T) {
	path := newTreeFSImage(t)
	fsys := openTreeFS(t, path)
	if err := fsys.InitRoot(context.Background()); err != nil {
		t.Fatalf("InitRoot: %v", err)
	}

	sparse := newRegularFile(t, fsys)
	sparseOffsets := []int64{0, 5 * BlockSize, 10 * BlockSize, 3*BlockSize + 4096, 200 * BlockSize}
	for i, off := range sparseOffsets {
		writeAt(t, sparse, pattern(byte(i+1), 4096), off)
	}

	a, b := newRegularFile(t, fsys), newRegularFile(t, fsys)
	const appendBlocks = 6
	for blk := int64(0); blk < appendBlocks; blk++ {
		writeAt(t, a, pattern(byte(blk+10), BlockSize), blk*BlockSize)
		writeAt(t, b, pattern(byte(blk+40), BlockSize), blk*BlockSize)
	}

	verify := func(fsys *FS) {
		t.Helper()
		sparse := &File{fs: fsys, inodeNum: sparse.inodeNum}
		for i, off := range sparseOffsets {
			checkAt(t, sparse, pattern(byte(i+1), 4096), off)
		}
		checkAt(t, sparse, make([]byte, 4096), 7*BlockSize)

		for blk := int64(0); blk < appendBlocks; blk++ {
			checkAt(t, &File{fs: fsys, inodeNum: a.inodeNum}, pattern(byte(blk+10), BlockSize), blk*BlockSize)
			checkAt(t, &File{fs: fsys, inodeNum: b.inodeNum}, pattern(byte(blk+40), BlockSize), blk*BlockSize)
		}
		for _, num := range []uint32{sparse.inodeNum, a.inodeNum, b.inodeNum} {
			ino, err := fsys.inodeTbl.Read(num)
			if err != nil {
				t.Fatal(err)
			}
			if !ino.HasExtentTree() {
				t.Errorf("inode %d should use an extent tree, has %d extents", num, ino.ExtentCount)
			}
		}
	}

	verify(fsys)
	verify(openTreeFS(t, path))
}

// updateInode redoes an update whose write lost to another node's tree change,
// so neither change is lost.
func TestUpdateInodeRetriesTreeConflict(t *testing.T) {
	fsys := openTreeFS(t, newTreeFSImage(t))
	if err := fsys.InitRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	f := newRegularFile(t, fsys)
	for blk := int64(0); blk < 3; blk++ {
		writeAt(t, f, pattern(byte(blk+1), 4096), blk*2*BlockSize)
	}

	ctx := context.Background()
	attempts := 0
	updated, err := fsys.updateInode(ctx, f.inodeNum, func(ino *inode.Inode) error {
		attempts++
		if attempts == 1 {
			// Another node commits a tree change between this read and this write.
			writeAt(t, f, pattern(9, 4096), 20*BlockSize)
		}
		ino.Mtime = 12345
		return nil
	})
	if err != nil {
		t.Fatalf("updateInode: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("update ran %d times, want 2", attempts)
	}
	if updated.Mtime != 12345 {
		t.Fatalf("Mtime = %d, want 12345", updated.Mtime)
	}
	checkAt(t, f, pattern(9, 4096), 20*BlockSize)
}
