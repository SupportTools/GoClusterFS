package integration

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// findFsckBinary locates goclusterfs-fsck next to goclusterfs-mount.
func findFsckBinary(t *testing.T) string {
	t.Helper()
	mount, err := findBinary()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(mount), "goclusterfs-fsck")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("goclusterfs-fsck not found next to %s; run 'make build' first", mount)
	}
	return path
}

// runFsck runs goclusterfs-fsck and returns its exit code and output.
func runFsck(t *testing.T, args ...string) (int, string) {
	t.Helper()
	out, err := exec.Command(findFsckBinary(t), args...).CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	}
	t.Fatalf("run goclusterfs-fsck %v: %v", args, err)
	return 0, ""
}

// problemLines returns the problems fsck printed.
func problemLines(out string) []string {
	var lines []string
	for _, line := range strings.Split(out, "\n") {
		for _, sev := range []string{"[INFO]", "[WARNING]", "[ERROR]", "[CRITICAL]", "[FATAL]"} {
			if strings.HasPrefix(strings.TrimSpace(line), sev) {
				lines = append(lines, line)
			}
		}
	}
	return lines
}

func expectFsck(t *testing.T, wantCode int, wantText string, args ...string) string {
	t.Helper()
	code, out := runFsck(t, args...)
	if code != wantCode || !strings.Contains(out, wantText) {
		t.Fatalf("goclusterfs-fsck %v: exit %d, want %d with %q:\n%s", args, code, wantCode, wantText, out)
	}
	return out
}

func inodeNum(t *testing.T, path string) uint32 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return uint32(fi.Sys().(*syscall.Stat_t).Ino)
}

// A filesystem the runtime wrote, with snapshots, clones, copies on write
// and extent trees, passes fsck without a single problem. fsck refuses to
// repair it while it is mounted, and repairs nothing after a clean unmount.
func TestFsckCleanAfterWorkload(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount: %v", err)
	}
	write := func(name string, data []byte, off int64) {
		t.Helper()
		if err := mfs.WriteFileAt(name, data, off); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	setxattr := func(name, attr, value string) {
		t.Helper()
		if err := unix.Setxattr(mfs.Path(name), attr, []byte(value), 0); err != nil {
			t.Fatalf("setxattr %s=%s on %s: %v", attr, value, name, err)
		}
	}

	write("small.txt", bytes.Repeat([]byte("s"), 100), 0)
	write("big.bin", bytes.Repeat([]byte("b"), 3<<20), 0)
	for i := int64(0); i < 5; i++ {
		write("sparse.bin", bytes.Repeat([]byte{byte('0' + i)}, 4096), i*2<<20)
	}
	setxattr("big.bin", "user.snapshot", "s1")
	write("big.bin", []byte("copy on write"), 10)
	setxattr("big.bin", "user.clone", "/big-clone.bin")
	write("big-clone.bin", []byte("clone write"), 1<<20)
	setxattr("big.bin", "user.snapshot.clone", "s1:/from-s1.bin")
	setxattr("sparse.bin", "user.snapshot", "s2")
	write("sparse.bin", []byte("tree copy on write"), 4<<20)
	if err := unix.Removexattr(mfs.Path("sparse.bin"), "user.snapshot.s2"); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}
	waitForSnapshotState(t, mfs.Path("sparse.bin"), "", "s2")
	if err := mfs.Remove("small.txt"); err != nil {
		t.Fatal(err)
	}

	expectFsck(t, 4, "mounted by", "-repair", mfs.ImagePath)
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}

	out := expectFsck(t, 0, "Status: CLEAN", mfs.ImagePath)
	if lines := problemLines(out); len(lines) > 0 {
		t.Fatalf("fsck found problems on a runtime image:\n%s", strings.Join(lines, "\n"))
	}
	// The mount binary leaves its slot behind with a clean journal.
	expectFsck(t, 0, "Status: CLEAN", "-repair", mfs.ImagePath)
	expectFsck(t, 4, "-dry-run needs -repair", "-dry-run", mfs.ImagePath)
	expectFsck(t, 4, "Failed to open device", filepath.Join(t.TempDir(), "missing.img"))
}

// shareLikeOlderVersions edits an unmounted image the way versions before
// counted sharing left it: src marked shared without FlagCounted, and clone
// mapping src's blocks with nobody counting either owner.
func shareLikeOlderVersions(t *testing.T, image string, src, clone uint32) {
	t.Helper()
	ctx := context.Background()
	dev, err := storage.Open(image)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()
	sb, err := superblock.Read(dev)
	if err != nil {
		t.Fatal(err)
	}
	tbl := inode.NewTable(dev, sb.InodeTableOffset, sb.InodeCount*inode.InodeSize, nil, [16]byte{})
	srcIno, err := tbl.Read(src)
	if err != nil {
		t.Fatal(err)
	}
	if srcIno.HasExtentTree() {
		t.Fatalf("inode %d: want inline extents", src)
	}
	for i := range srcIno.InlineExtents {
		if srcIno.InlineExtents[i].Length > 0 {
			srcIno.InlineExtents[i].Flags |= extent.FlagShared
		}
	}
	if err := tbl.Write(ctx, srcIno); err != nil {
		t.Fatal(err)
	}
	cloneIno, err := tbl.Read(clone)
	if err != nil {
		t.Fatal(err)
	}
	cloneIno.InlineExtents, cloneIno.ExtentCount = srcIno.InlineExtents, srcIno.ExtentCount
	cloneIno.Size, cloneIno.AllocatedBlocks, cloneIno.WrittenBlocks = srcIno.Size, srcIno.AllocatedBlocks, srcIno.WrittenBlocks
	if err := tbl.Write(ctx, cloneIno); err != nil {
		t.Fatal(err)
	}
}

// An image from before counted sharing refuses snapshots of shared files
// with ENOTSUP. goclusterfs-fsck -repair rebuilds the refcounts, after which
// snapshots work and writes to either owner leave the other intact.
func TestFsckRepairsOlderSharing(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount: %v", err)
	}
	srcData := bytes.Repeat([]byte("src!"), 2<<20/4)
	if err := mfs.WriteFile("src.bin", srcData); err != nil {
		t.Fatal(err)
	}
	if err := mfs.WriteFile("old-clone.bin", nil); err != nil {
		t.Fatal(err)
	}
	src, clone := inodeNum(t, mfs.Path("src.bin")), inodeNum(t, mfs.Path("old-clone.bin"))
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}
	shareLikeOlderVersions(t, mfs.ImagePath, src, clone)

	if err := mfs.Mount(false); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(mfs.Path("src.bin"), "user.snapshot", []byte("refused"), 0); !errors.Is(err, unix.ENOTSUP) {
		t.Fatalf("snapshot before repair: %v, want ENOTSUP", err)
	}
	waitForSnapshotState(t, mfs.Path("src.bin"), "", "refused")
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}

	before := expectFsck(t, 1, "older versions", mfs.ImagePath)
	expectFsck(t, 1, "nothing was written", "-repair", "-dry-run", mfs.ImagePath)
	if after := expectFsck(t, 1, "older versions", mfs.ImagePath); strings.Join(problemLines(after), "\n") != strings.Join(problemLines(before), "\n") {
		t.Fatalf("dry run changed the image:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	expectFsck(t, 2, "Status: REPAIRED", "-repair", mfs.ImagePath)
	if out := expectFsck(t, 0, "Status: CLEAN", mfs.ImagePath); len(problemLines(out)) > 0 {
		t.Fatalf("problems left after repair:\n%s", out)
	}

	if err := mfs.Mount(false); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(mfs.Path("src.bin"), "user.snapshot", []byte("after"), 0); err != nil {
		t.Fatalf("snapshot after repair: %v", err)
	}
	if err := mfs.WriteFileAt("old-clone.bin", []byte("clone"), 10); err != nil {
		t.Fatal(err)
	}
	if err := mfs.WriteFileAt("src.bin", []byte("source"), 1<<20+10); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(mfs.Path("src.bin"), "user.snapshot.clone", []byte("after:/from-after.bin"), 0); err != nil {
		t.Fatalf("clone snapshot: %v", err)
	}
	for name, want := range map[string][]byte{
		"src.bin":        overwrite(srcData, 1<<20+10, []byte("source")),
		"old-clone.bin":  overwrite(srcData, 10, []byte("clone")),
		"from-after.bin": srcData,
	} {
		if got, err := mfs.ReadFile(name); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: wrong data after repair (err=%v)", name, err)
		}
	}
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}
	if out := expectFsck(t, 0, "Status: CLEAN", mfs.ImagePath); len(problemLines(out)) > 0 {
		t.Fatalf("problems after using the repaired image:\n%s", out)
	}
}
