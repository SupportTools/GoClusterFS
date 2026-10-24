package integration

import (
	"syscall"
	"testing"
)

// TestTreeReservationsDoNotLeakBlocks appends to a file whose two inline extents
// are full, so each append reserves an extent-tree block and then merges into the
// last extent without using it. Unused reservations must go back to the bitmap,
// not to an in-memory list that unmount drops, so the whole volume is writable
// again once the file is deleted.
func TestTreeReservationsDoNotLeakBlocks(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// 128KB at block 0 keeps the file off the SFB path; block 2 makes the second extent.
	if err := mfs.WriteFileAt("append.bin", chunk(1, 128*1024), 0); err != nil {
		t.Fatalf("write at block 0 failed: %v", err)
	}
	for blk := 2; blk <= 7; blk++ {
		if err := mfs.WriteFileAt("append.bin", chunk(byte(blk), 64*1024), int64(blk*mb)); err != nil {
			t.Fatalf("write at block %d failed: %v", blk, err)
		}
	}
	remount(t, mfs)
	if err := mfs.Remove("append.bin"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	var st syscall.Statfs_t
	if err := syscall.Statfs(mfs.MountPoint, &st); err != nil {
		t.Fatalf("Statfs failed: %v", err)
	}
	// Every data block except the root directory's.
	fill := int(st.Blocks-1) * mb
	if err := mfs.WriteFile("fill.bin", make([]byte, fill)); err != nil {
		t.Fatalf("writing %d MB into an empty volume of %d blocks failed: %v", fill/mb, st.Blocks, err)
	}
}

// TestWriteInsideFallocatedRange writes 4KB into the middle of a fallocated
// range. Clearing the unwritten flag splits one extent into three, which needs
// extent-tree blocks, and the rest of the block must read as zeros even though
// the block held another file's data before.
func TestWriteInsideFallocatedRange(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}
	old := make([]byte, 4*mb)
	for i := range old {
		old[i] = 0xEE
	}
	if err := mfs.WriteFile("old.bin", old); err != nil {
		t.Fatalf("write old.bin failed: %v", err)
	}
	if err := mfs.Remove("old.bin"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if err := mfs.WriteFile("falloc.bin", nil); err != nil {
		t.Fatalf("create falloc.bin failed: %v", err)
	}
	f, err := mfs.OpenFile("falloc.bin")
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, 4*mb); err != nil {
		f.Close()
		t.Fatalf("Fallocate failed: %v", err)
	}
	data := chunk(7, 4096)
	const at = mb + 512*1024
	if _, err := f.WriteAt(data, at); err != nil {
		f.Close()
		t.Fatalf("write inside the fallocated range failed: %v", err)
	}
	f.Close()

	remount(t, mfs)
	want := make([]byte, 4*mb)
	copy(want[at:], data)
	checkFile(t, mfs, "falloc.bin", want)
}
