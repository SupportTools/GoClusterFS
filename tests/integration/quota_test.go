package integration

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

const quotaXattr = "trusted.goclusterfs.quota"

// The kernel lets only CAP_SYS_ADMIN change trusted.* attributes, so a user without it, such as a PVC's tenant,
// cannot set or raise a directory's quota through the mount. With it, setting the quota gives the directory a project.
func TestQuotaXattrThroughTheMount(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}
	dir := filepath.Join(mfs.MountPoint, "pvc")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	if _, err := unix.Getxattr(dir, quotaXattr, buf); !errors.Is(err, unix.ENODATA) {
		t.Errorf("reading the quota of a directory with no project: %v, want ENODATA", err)
	}

	err := unix.Setxattr(dir, quotaXattr, []byte("bytes=2097152 inodes=10"), 0)
	if os.Geteuid() != 0 {
		if !errors.Is(err, unix.EPERM) {
			t.Fatalf("setting the quota without CAP_SYS_ADMIN: %v, want EPERM", err)
		}
		t.Log("not root: the refusal is checked; setting a quota runs only as root")
		return
	}
	if err != nil {
		t.Fatalf("setting the quota as root: %v", err)
	}
	n, err := unix.Getxattr(dir, quotaXattr, buf)
	if err != nil || !strings.HasPrefix(string(buf[:n]), "project=1 bytes=2097152 inodes=10 ") {
		t.Errorf("quota after setting it: %q, %v; want project 1 with the limits", buf[:max(n, 0)], err)
	}
}

// statfs on a directory below the root reports the volume, as on the root, through the mount.
func TestStatfsOnASubdirectoryThroughTheMount(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}
	dir := filepath.Join(mfs.MountPoint, "sub")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var root, sub unix.Statfs_t
	if err := unix.Statfs(mfs.MountPoint, &root); err != nil {
		t.Fatal(err)
	}
	if err := unix.Statfs(dir, &sub); err != nil {
		t.Fatal(err)
	}
	if sub.Blocks == 0 || sub.Blocks != root.Blocks || sub.Files != root.Files || sub.Bsize != root.Bsize {
		t.Errorf("statfs on a subdirectory = %d blocks of %d, %d inodes; want the root's %d blocks of %d, %d inodes",
			sub.Blocks, sub.Bsize, sub.Files, root.Blocks, root.Bsize, root.Files)
	}
}
