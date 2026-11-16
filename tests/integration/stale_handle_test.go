package integration

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// A handle that outlives its file, here an O_PATH descriptor that never opened it, must never reach the next file in its inode slot.
func TestStaleHandleNeverSeesTheNextFileInItsSlot(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount: %v", err)
	}
	oldData, newData := bytes.Repeat([]byte{'o'}, 5000), bytes.Repeat([]byte{'n'}, 7000)
	writeOrFail(t, mfs, "old", oldData, 0)
	oldIno := inodeNumber(t, mfs, "old")
	fd, err := unix.Open(mfs.Path("old"), unix.O_PATH, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(fd) }()

	if err := os.Remove(mfs.Path("old")); err != nil {
		t.Fatal(err)
	}
	writeOrFail(t, mfs, "new", newData, 0)
	if ino := inodeNumber(t, mfs, "new"); ino != oldIno {
		t.Fatalf("the new file got inode %d; the test needs it in the freed slot %d", ino, oldIno)
	}

	reopened, err := os.Open(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err == nil {
		got, _ := io.ReadAll(reopened)
		_ = reopened.Close()
		t.Fatalf("reopening the old handle opened %d bytes starting %q, want ESTALE", len(got), got[:min(len(got), 8)])
	}
	if !errors.Is(err, syscall.ESTALE) && !errors.Is(err, syscall.ENOENT) {
		t.Errorf("reopening the old handle: %v, want ESTALE", err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err == nil && st.Size == int64(len(newData)) {
		t.Errorf("fstat through the old handle shows the new file's size %d", st.Size)
	}
	expectContent(t, mfs, "new", newData)
}
