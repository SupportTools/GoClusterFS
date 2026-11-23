package goclusterfs

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
)

// A 1 MB request used to reach the device as 256 I/Os of 4 KB, and a write
// across a block boundary as one I/O per block.
func TestDataReachesTheDeviceInOneIOPerContiguousSpan(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	f := createFile(t, fsys, "f")
	content := pattern(1, 2*BlockSize)
	writeAt(t, f, content, 0)

	dataStart := int64(fsys.dataOffset)
	var reads, writes atomic.Int32
	fsys.dev.SetReadFault(func(off int64) error {
		if off >= dataStart {
			reads.Add(1)
		}
		return nil
	})
	fsys.dev.SetWriteObserver(func(off int64, _ int) {
		if off >= dataStart {
			writes.Add(1)
		}
	})
	t.Cleanup(func() {
		fsys.dev.SetReadFault(func(int64) error { return nil })
		fsys.dev.SetWriteObserver(nil)
	})
	contiguous := 1
	if a, b := fileBlock(t, fsys, f, 0), fileBlock(t, fsys, f, 1); b != a+1 {
		contiguous = 2
	}

	for i, c := range []struct {
		name        string
		off, n      int
		reads, writ int
	}{
		{"whole block", 0, BlockSize, 0, 1},
		{"one byte", 100, 1, 1, 1},
		{"partial sectors at both ends", 4000, 8300, 2, 1},
		{"whole sectors", 8192, 65536, 0, 1},
		{"across a block boundary", BlockSize - 4096, 8192, 0, contiguous},
	} {
		data := pattern(byte(i+2), c.n)
		copy(content[c.off:], data)
		reads.Store(0)
		writes.Store(0)
		writeAt(t, f, data, int64(c.off))
		if r, w := int(reads.Load()), int(writes.Load()); r != c.reads || w != c.writ {
			t.Errorf("%s: %d data reads and %d data writes, want %d and %d", c.name, r, w, c.reads, c.writ)
		}
	}

	for _, c := range []struct {
		name   string
		off, n int
		reads  int
	}{
		{"one block", 0, BlockSize, 1},
		{"a few bytes", 5000, 10, 1},
		{"both blocks", 0, 2 * BlockSize, contiguous},
	} {
		reads.Store(0)
		res, errno := f.Read(context.Background(), nil, make([]byte, c.n), int64(c.off))
		if errno != 0 {
			t.Fatalf("%s: read: %v", c.name, errno)
		}
		got, _ := res.Bytes(make([]byte, c.n))
		if !bytes.Equal(got, content[c.off:c.off+c.n]) {
			t.Errorf("%s: read wrong data", c.name)
		}
		if r := int(reads.Load()); r != c.reads {
			t.Errorf("%s: %d data reads, want %d", c.name, r, c.reads)
		}
	}
}

func fileBlock(t *testing.T, fsys *FS, f *File, logical uint64) uint64 {
	t.Helper()
	ino := readInode(t, fsys, f.inodeNum)
	phys, err := fsys.lookupPhysical(ino, logical)
	if err != nil {
		t.Fatal(err)
	}
	return phys
}
