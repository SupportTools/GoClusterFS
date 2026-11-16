package integration

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// POSIX keeps an unlinked file until its last close: a descriptor opened before
// the unlink keeps reading and writing that file, and no other file sees it.
func TestFileUnlinkedWhileOpenKeepsItsData(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount: %v", err)
	}
	fill := func(b byte) []byte { return bytes.Repeat([]byte{b}, 12288) }

	f, err := os.OpenFile(mfs.Path("victim"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteAt(fill('a'), 0); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(mfs.Path("victim")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(fill('b'), 0); err != nil {
		t.Errorf("write through the open descriptor after unlink: %v", err)
	}

	writeOrFail(t, mfs, "other", fill('c'), 0)
	if _, err := f.WriteAt(fill('d'), 0); err != nil {
		t.Errorf("second write through the open descriptor: %v", err)
	}
	expectContent(t, mfs, "other", fill('c'))
	got := make([]byte, 12288)
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Errorf("read through the open descriptor: %v", err)
	} else if !bytes.Equal(got, fill('d')) {
		t.Errorf("the open descriptor reads %q..., want its own write 'd'", got[:4])
	}
	again, err := os.OpenFile(fmt.Sprintf("/proc/self/fd/%d", f.Fd()), os.O_RDONLY, 0)
	if err != nil {
		t.Errorf("reopen through /proc/self/fd: %v", err)
	} else {
		if _, err := again.ReadAt(got, 0); err != nil || !bytes.Equal(got, fill('d')) {
			t.Errorf("reopened descriptor reads %q... (%v), want 'd'", got[:4], err)
		}
		_ = again.Close()
	}
	if err := f.Close(); err != nil {
		t.Errorf("close: %v", err)
	}

	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}
	if code, out := runFsck(t, mfs.ImagePath); code != 0 {
		t.Errorf("fsck after the last close: exit %d:\n%s", code, strings.Join(problemLines(out), "\n"))
	}
}

// A file node 1 unlinks stays usable on node 0, which has it open, until node 0 closes it.
func TestFileUnlinkedByAPeerWhileOpenKeepsItsData(t *testing.T) {
	cluster, f, num := openOnNode0UnlinkOnNode1(t)
	fill := func(b byte) []byte { return bytes.Repeat([]byte{b}, 12288) }

	writeOrFail(t, cluster.Node(1), "other", fill('c'), 0)
	if _, err := f.WriteAt(fill('d'), 0); err != nil {
		t.Fatalf("node 0 writes after node 1's unlink: %v", err)
	}
	got := make([]byte, 12288)
	if _, err := f.ReadAt(got, 0); err != nil || !bytes.Equal(got, fill('d')) {
		t.Errorf("node 0 reads %q... (%v), want its own write 'd'", got[:4], err)
	}
	expectContent(t, cluster.Node(1), "other", fill('c'))
	if err := f.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
	expectFreed(t, cluster.ImagePath, num, 10*time.Second, "after node 0's last close")
	cluster.StopAllNodes()
	if code, out := runFsck(t, cluster.ImagePath); code != 0 {
		t.Errorf("fsck: exit %d:\n%s", code, strings.Join(problemLines(out), "\n"))
	}
}

// When node 0 dies holding an unlinked file open, node 1 frees the file after replaying node 0's journal.
func TestKilledHolderLetsGoOfAnUnlinkedFile(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for failure detection, about 30s")
	}
	cluster, f, num := openOnNode0UnlinkOnNode1(t)
	if err := cluster.Node(0).Kill(); err != nil {
		t.Fatalf("kill node 0: %v", err)
	}
	_ = f.Close()
	_ = cluster.Node(0).Unmount()

	expectFreed(t, cluster.ImagePath, num, 2*time.Minute, "after node 0 died")
	cluster.StopAllNodes()
	if code, out := runFsck(t, cluster.ImagePath); code != 0 {
		t.Errorf("fsck: exit %d:\n%s", code, strings.Join(problemLines(out), "\n"))
	}
}

// Removing more files than one orphan list holds, faster than the failure scan, must not fail with EBUSY,
// and the unmount must leave no orphan behind.
func TestPeerRemovesManyFilesQuickly(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 400)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node 0: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("start node 1: %v", err)
	}
	const files = 1000
	for i := range files {
		if err := os.WriteFile(cluster.Node(0).Path(fmt.Sprintf("f%d", i)), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	for i := range files {
		if err := os.Remove(cluster.Node(1).Path(fmt.Sprintf("f%d", i))); err != nil {
			t.Fatalf("node 1 removes file %d after %v: %v", i, time.Since(start), err)
		}
	}
	t.Logf("node 1 removed %d files in %v", files, time.Since(start))
	cluster.StopAllNodes()
	if code, out := runFsck(t, cluster.ImagePath); code != 0 {
		t.Errorf("fsck: exit %d:\n%s", code, strings.Join(problemLines(out), "\n"))
	}
}

func openOnNode0UnlinkOnNode1(t *testing.T) (*MultiNodeCluster, *os.File, uint32) {
	t.Helper()
	cluster := NewMultiNodeCluster(t, 2, 400)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node 0: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("start node 1: %v", err)
	}
	f, err := os.OpenFile(cluster.Node(0).Path("victim"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.WriteAt(bytes.Repeat([]byte{'a'}, 12288), 0); err != nil {
		t.Fatal(err)
	}
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	num := uint32(fi.Sys().(*syscall.Stat_t).Ino)
	if err := os.Remove(cluster.Node(1).Path("victim")); err != nil {
		t.Fatalf("node 1 unlinks: %v", err)
	}
	if ino := imageInode(t, cluster.ImagePath, num); ino.Magic != inode.InodeMagic || ino.LinkCount != 0 || ino.Holders == 0 {
		t.Fatalf("after node 1's unlink: inode %d magic %#x, links %d, holders %#b; want an orphan node 0 holds", num, ino.Magic, ino.LinkCount, ino.Holders)
	}
	return cluster, f, num
}

// The kernel sends FUSE RELEASE after close returns, so the file is freed a little later.
func expectFreed(t *testing.T, image string, num uint32, timeout time.Duration, when string) {
	t.Helper()
	if _, ok := waitFor(timeout, func() bool { return imageInode(t, image, num).Magic == 0 }); !ok {
		ino := imageInode(t, image, num)
		t.Fatalf("%v %s: inode %d magic %#x, links %d, holders %#b; want it free", timeout, when, num, ino.Magic, ino.LinkCount, ino.Holders)
	}
}

func imageInode(t *testing.T, image string, num uint32) *inode.Inode {
	t.Helper()
	dev, err := storage.Open(image)
	if err != nil {
		t.Fatal(err)
	}
	defer mustClose(t, dev)
	sb, err := superblock.Read(dev)
	if err != nil {
		t.Fatal(err)
	}
	ino, err := inode.NewTable(dev, sb.InodeTableOffset, sb.InodeCount*inode.InodeSize, nil, [16]byte{}).Read(num)
	if err != nil {
		t.Fatal(err)
	}
	return ino
}
