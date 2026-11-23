package integration

import (
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// A file whose blocks alternate with another file's needs an extent tree.
// Snapshotting it, overwriting it whole and deleting the snapshot gives back
// the snapshot's blocks and, when the copies made the file contiguous, the
// file's old tree node, and nothing a file still uses: later allocations,
// without reserves so they reuse freed blocks at once, leave both files' data
// intact and fsck clean.
func TestSnapshotDeleteOfAFileWithATree(t *testing.T) {
	mfs := NewMountedFS(t, 200)
	mfs.ExtraArgs = []string{"-block-reserve=0"}
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}
	bs := blockSizeOf(t, mfs)
	const blocks = 64

	a, err := os.OpenFile(mfs.Path("a.bin"), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.OpenFile(mfs.Path("b.bin"), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	aData, bData := chunk(1, blocks*bs), chunk(2, blocks*bs)
	for i := 0; i < blocks; i++ {
		if _, err := a.WriteAt(aData[i*bs:(i+1)*bs], int64(i*bs)); err != nil {
			t.Fatal(err)
		}
		if _, err := b.WriteAt(bData[i*bs:(i+1)*bs], int64(i*bs)); err != nil {
			t.Fatal(err)
		}
	}
	_ = a.Close()
	_ = b.Close()

	free := freeBlocks(t, mfs)
	setxattr(t, mfs.Path("a.bin"), "user.snapshot", "temp")
	t.Logf("snapshot: %+d free blocks", int64(freeBlocks(t, mfs))-int64(free))
	updated := chunk(3, blocks*bs)
	if err := mfs.WriteFileAt("a.bin", updated, 0); err != nil {
		t.Fatal(err)
	}
	t.Logf("overwrite: %+d free blocks", int64(freeBlocks(t, mfs))-int64(free))
	if err := unix.Removexattr(mfs.Path("a.bin"), "user.snapshot.temp"); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}
	waitForSnapshotState(t, mfs.Path("a.bin"), "", "temp")
	nodeBlocks := uint64(max(bs, 64<<10) / bs)
	if got := freeBlocks(t, mfs); got != free && got != free+nodeBlocks {
		t.Fatalf("free blocks after deleting the snapshot = %d, want %d, or %d if the file's tree node went too", got, free, free+nodeBlocks)
	}

	if err := mfs.WriteFile("filler.bin", chunk(4, 16*bs)); err != nil {
		t.Fatal(err)
	}
	checkFile(t, mfs, "a.bin", updated)
	checkFile(t, mfs, "b.bin", bData)
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}
	if code, out := runFsck(t, mfs.ImagePath); code != 0 {
		t.Fatalf("fsck exit %d:\n%s", code, strings.Join(problemLines(out), "\n"))
	}
}
