package integration

import (
	"bytes"
	"os"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const mb = 1024 * 1024

// chunk returns size bytes that differ per seed, so misplaced blocks are caught.
func chunk(seed byte, size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = seed ^ byte(i%253)
	}
	return b
}

func remount(t *testing.T, mfs *MountedFS) {
	t.Helper()
	if err := mfs.Unmount(); err != nil {
		t.Fatalf("Unmount failed: %v", err)
	}
	if err := mfs.Mount(false); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
}

func checkFile(t *testing.T, mfs *MountedFS, name string, want []byte) {
	t.Helper()
	got, err := mfs.ReadFile(name)
	if err != nil {
		t.Fatalf("ReadFile %s failed: %v", name, err)
	}
	if len(got) != len(want) {
		t.Fatalf("%s: size %d, want %d", name, len(got), len(want))
	}
	if !bytes.Equal(got, want) {
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s: first mismatch at offset %d (block %d)", name, i, i/mb)
			}
		}
	}
}

// TestExtentTreeSparseWrites writes at seven scattered 1MB blocks, more than
// the two extents an inode holds inline, and reads the file back before and
// after a remount.
func TestExtentTreeSparseWrites(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// The first write lands past 64KB so the file never uses an SFB slot.
	blocks := []int{12, 3, 7, 0, 20, 15, 30}
	const size = 64 * 1024
	want := make([]byte, 30*mb+size)
	for i, blk := range blocks {
		data := chunk(byte(i+1), size)
		if err := mfs.WriteFileAt("sparse.bin", data, int64(blk*mb)); err != nil {
			t.Fatalf("write at block %d failed: %v", blk, err)
		}
		copy(want[blk*mb:], data)
	}

	var st syscall.Stat_t
	if err := syscall.Stat(mfs.Path("sparse.bin"), &st); err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if wantBlocks := int64(len(blocks)) * mb / 512; st.Blocks != wantBlocks {
		t.Errorf("st_blocks = %d, want %d (one 1MB block per write)", st.Blocks, wantBlocks)
	}

	checkFile(t, mfs, "sparse.bin", want)
	remount(t, mfs)
	checkFile(t, mfs, "sparse.bin", want)
}

// TestExtentTreeInterleavedAppends appends to two files alternately, 1MB at a
// time, so neither file gets contiguous blocks, and grows both past the old
// 2MB limit.
func TestExtentTreeInterleavedAppends(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	names := []string{"left.bin", "right.bin"}
	files := make([]*os.File, len(names))
	want := make([][]byte, len(names))
	for i, name := range names {
		f, err := os.OpenFile(mfs.Path(name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			t.Fatalf("OpenFile %s failed: %v", name, err)
		}
		files[i] = f
	}
	for blk := 0; blk < 6; blk++ {
		for i, f := range files {
			data := chunk(byte(16*i+blk), mb)
			if _, err := f.Write(data); err != nil {
				t.Fatalf("%s: append block %d failed: %v", names[i], blk, err)
			}
			want[i] = append(want[i], data...)
		}
	}
	for i, f := range files {
		if err := f.Close(); err != nil {
			t.Fatalf("Close %s failed: %v", names[i], err)
		}
	}

	for i, name := range names {
		checkFile(t, mfs, name, want[i])
	}
	remount(t, mfs)
	for i, name := range names {
		checkFile(t, mfs, name, want[i])
	}
}

// TestExtentTreeTruncate truncates a fragmented file and checks that the kept
// head survives and that the freed data blocks and the tree block return to statfs.
func TestExtentTreeTruncate(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	frag, err := os.OpenFile(mfs.Path("frag.bin"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	filler, err := os.OpenFile(mfs.Path("filler.bin"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var want []byte
	for blk := 0; blk < 6; blk++ {
		data := chunk(byte(blk+1), mb)
		if _, err := frag.Write(data); err != nil {
			t.Fatalf("append block %d failed: %v", blk, err)
		}
		want = append(want, data...)
		if _, err := filler.Write(chunk(0xAA, mb)); err != nil {
			t.Fatalf("filler append %d failed: %v", blk, err)
		}
	}
	frag.Close()
	filler.Close()

	var before syscall.Statfs_t
	if err := syscall.Statfs(mfs.MountPoint, &before); err != nil {
		t.Fatal(err)
	}
	const keep = mb + mb/2
	if err := os.Truncate(mfs.Path("frag.bin"), keep); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}
	var after syscall.Statfs_t
	if err := syscall.Statfs(mfs.MountPoint, &after); err != nil {
		t.Fatal(err)
	}

	// Blocks 2-5 are freed, and the two remaining extents move back inline,
	// which frees the tree block as well.
	if freed := after.Bfree - before.Bfree; freed != 5 {
		t.Errorf("truncate freed %d blocks, want 5 (4 data + 1 extent tree)", freed)
	}
	checkFile(t, mfs, "frag.bin", want[:keep])
	remount(t, mfs)
	checkFile(t, mfs, "frag.bin", want[:keep])
}

// TestExtentTreeSnapshot covers snapshots and clones of a file with more than
// 2 extents. Snapshots have no read interface yet, clone-from-snapshot clones
// the live file, and copy-on-write of full blocks does not trigger for any file
// (refcounts only reach 1). So the test checks what does hold: the clone gets
// its own extent tree, blocks shared with it survive truncating and deleting
// the source, and deleting the snapshot frees nothing in use.
func TestExtentTreeSnapshot(t *testing.T) {
	mfs := NewMountedFS(t, 96)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	frag, err := os.OpenFile(mfs.Path("frag.bin"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	filler, err := os.OpenFile(mfs.Path("filler.bin"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var want []byte
	for blk := 0; blk < 6; blk++ {
		data := chunk(byte(blk+1), mb)
		if _, err := frag.Write(data); err != nil {
			t.Fatalf("append block %d failed: %v", blk, err)
		}
		want = append(want, data...)
		if _, err := filler.Write(chunk(0xAA, mb)); err != nil {
			t.Fatalf("filler append %d failed: %v", blk, err)
		}
	}
	frag.Close()
	filler.Close()

	path := mfs.Path("frag.bin")
	if err := unix.Setxattr(path, "user.snapshot", []byte("tree-snap"), 0); err != nil {
		if err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP {
			t.Skip("Snapshot xattr not supported")
		}
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	time.Sleep(500 * time.Millisecond) // background refcount job

	if err := unix.Setxattr(path, "user.snapshot.clone", []byte("tree-snap:/clone.bin"), 0); err != nil {
		t.Fatalf("clone from snapshot failed: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	cloneWant := append([]byte(nil), want...)
	checkFile(t, mfs, "clone.bin", cloneWant)

	freeBlocks := func() int64 {
		var st syscall.Statfs_t
		if err := syscall.Statfs(mfs.MountPoint, &st); err != nil {
			t.Fatal(err)
		}
		return int64(st.Bfree)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	tail := chunk(0x77, mb)
	if _, err := f.Write(tail); err != nil {
		t.Fatalf("append after snapshot failed: %v", err)
	}
	f.Close()
	want = append(want, tail...)

	// Blocks 4 and 5 are shared with the clone; only the new block 6 is freed.
	before := freeBlocks()
	if err := os.Truncate(path, 4*mb); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}
	want = want[:4*mb]
	if got := freeBlocks() - before; got != 1 {
		t.Errorf("truncating shared and private blocks freed %d blocks, want 1", got)
	}
	checkFile(t, mfs, "frag.bin", want)
	checkFile(t, mfs, "clone.bin", cloneWant)

	before = freeBlocks()
	if err := unix.Removexattr(path, "user.snapshot.tree-snap"); err != nil && err != syscall.ENODATA {
		t.Fatalf("deleting snapshot failed: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if got := freeBlocks() - before; got != 0 {
		t.Errorf("deleting the snapshot freed %d blocks, want 0", got)
	}

	remount(t, mfs)
	checkFile(t, mfs, "frag.bin", want)
	checkFile(t, mfs, "clone.bin", cloneWant)

	if err := mfs.Remove("frag.bin"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	checkFile(t, mfs, "clone.bin", cloneWant)
}
