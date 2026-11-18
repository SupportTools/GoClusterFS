package goclusterfs

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
)

func snapshotList(t *testing.T, f *File) string {
	t.Helper()
	buf := make([]byte, 4096)
	n, errno := f.Getxattr(context.Background(), "user.snapshots", buf)
	if errno != 0 {
		t.Fatalf("inode %d: getxattr user.snapshots: %v", f.inodeNum, errno)
	}
	return string(buf[:n])
}

// snapshotThenReuseSlot snapshots a file as s, deletes the file, and creates a new file in the freed inode slot.
func snapshotThenReuseSlot(t *testing.T) (*FS, *File) {
	t.Helper()
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	old := createFile(t, fsys, "old")
	writeAt(t, old, pattern(1, BlockSize), 0)
	setxattr(t, old, "user.snapshot", "s")
	if got := snapshotList(t, old); !strings.HasPrefix(got, "s:") {
		t.Fatalf("the file lists snapshots %q, want s", got)
	}
	if errno := fsys.Unlink(ctx, "old"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	reused := createFile(t, fsys, "new")
	if reused.inodeNum != old.inodeNum {
		t.Fatalf("the new file got inode %d, want the freed inode %d", reused.inodeNum, old.inodeNum)
	}
	writeAt(t, reused, pattern(2, BlockSize), 0)
	return fsys, reused
}

// A snapshot belongs to the file it was taken of. A later file in the same inode slot must not see, clone or delete it.
func TestSnapshotStaysWithItsFile(t *testing.T) {
	ctx := context.Background()
	fsys, reused := snapshotThenReuseSlot(t)

	if got := snapshotList(t, reused); got != "" {
		t.Errorf("the new file lists %q, the deleted file's snapshots", got)
	}
	if errno := reused.Setxattr(ctx, "user.snapshot.clone", []byte("s:/leak"), 0); errno != syscall.ENOENT {
		t.Errorf("clone of the deleted file's snapshot through the new file: errno %v, want ENOENT", errno)
	}
	if errno := reused.Setxattr(ctx, "user.snapshot.delete", []byte("s"), 0); errno != syscall.ENOENT {
		t.Errorf("delete of the deleted file's snapshot through the new file: errno %v, want ENOENT", errno)
	}
	if _, err := fsys.snapshotReg.GetByName(ctx, "s"); err != nil {
		t.Errorf("snapshot s: %v, want it kept as an orphan", err)
	}
	expectOrphanReport(t, fsys, `snapshot "s" is an orphan: inode`)
}

// expectOrphanReport requires fsck to warn about the orphan with want in its message, and to find no error.
func expectOrphanReport(t *testing.T, fsys *FS, want string) {
	t.Helper()
	r := runFsck(t, fsys, fsck.DefaultOptions())
	found := false
	for _, p := range r.Problems {
		found = found || p.Severity == fsck.Warning && strings.Contains(p.Message, want)
		if p.Severity != fsck.Warning {
			t.Errorf("fsck: %s", p)
		}
	}
	if !found {
		t.Errorf("fsck does not report the orphan snapshot:%s", problemList(r))
	}
}

// The full listing marks a deleted file's snapshot as an orphan, and only an orphan can be deleted without its file.
func TestOrphanSnapshotListedAndDeletable(t *testing.T) {
	ctx := context.Background()
	fsys, reused := snapshotThenReuseSlot(t)
	setxattr(t, reused, "user.snapshot", "live")

	all, err := fsys.AllSnapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	orphan := map[string]bool{}
	for _, e := range all {
		orphan[e.GetName()] = e.Orphan
	}
	if len(orphan) != 2 || !orphan["s"] || orphan["live"] {
		t.Fatalf("full listing marks orphans %v, want s an orphan and live not", orphan)
	}
	if err := fsys.DeleteOrphanSnapshot(ctx, "live"); err == nil {
		t.Error("deleting the live file's snapshot as an orphan succeeded")
	}
	if err := fsys.DeleteOrphanSnapshot(ctx, "s"); err != nil {
		t.Fatalf("delete orphan s: %v", err)
	}
	waitSnapshotGone(t, fsys, "s")
	if got := snapshotList(t, reused); !strings.HasPrefix(got, "live:") {
		t.Errorf("the new file lists %q, want its own snapshot live", got)
	}
}

// A file unlinked while another node holds it open has no name to delete its snapshots through, so they are orphans
// at once. Deleting one leaves the holder's data alone, since the snapshot holds its own references to the blocks.
func TestSnapshotOfAnUnlinkedFileIsAnOrphanWhileHeld(t *testing.T) {
	path := newFormattedImage(t)
	cluster := newFakeCluster(0, 1)
	holder := cluster.join(t, withBridge(openFormattedFS(t, path)), 0)
	peer := cluster.join(t, openFormattedFS(t, path), 1)
	ctx := context.Background()

	f := createOpen(t, holder, "f")
	data := pattern(1, 2*BlockSize)
	writeAt(t, f, data, 0)
	setxattr(t, f, "user.snapshot", "s")
	if err := peer.DeleteOrphanSnapshot(ctx, "s"); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("deleting a named file's snapshot without the file: %v, want EBUSY", err)
	}
	if errno := peer.Unlink(ctx, "f"); errno != 0 {
		t.Fatalf("unlink on the peer: %v", errno)
	}
	if ino := readInode(t, peer, f.inodeNum); ino.Magic != inode.InodeMagic || ino.LinkCount != 0 {
		t.Fatalf("after the unlink: magic %#x, links %d; want an inode the holder still has open", ino.Magic, ino.LinkCount)
	}

	all, err := peer.AllSnapshots(ctx)
	if err != nil || len(all) != 1 || all[0].GetName() != "s" || !all[0].Orphan {
		t.Fatalf("full listing after the unlink: %+v, %v; want s as an orphan", all, err)
	}
	if errno := f.Setxattr(ctx, "user.snapshot", []byte("t"), 0); errno != syscall.ENOENT {
		t.Errorf("a new snapshot through the unlinked file's open handle: %v, want ENOENT", errno)
	}
	if err := peer.DeleteOrphanSnapshot(ctx, "s"); err != nil {
		t.Fatalf("delete of the orphan s: %v", err)
	}
	waitSnapshotGone(t, peer, "s")

	checkAt(t, f, data, 0)
	writeAt(t, f, pattern(2, BlockSize), 0)
	checkAt(t, f, append(pattern(2, BlockSize), data[BlockSize:]...), 0)
	if errno := f.Release(ctx, nil); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
	if ino := readInode(t, holder, f.inodeNum); ino.Magic != 0 {
		t.Fatalf("after the holder's last close: magic %#x; want the inode freed", ino.Magic)
	}
	expectNoProblems(t, peer, "after the orphan snapshot was deleted and the file closed")
}

// Descriptors written before snapshots recorded their file's birth generation cannot prove which file they belong to,
// so they are orphans.
func TestSnapshotWithoutBirthIsOrphan(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	f := createFile(t, fsys, "f")
	desc, err := fsys.snapshotReg.Create(ctx, "legacy", f.inodeNum, 0, 0, 0, 0, snapshot.FlagReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.snapshotReg.Activate(ctx, desc.SnapshotID); err != nil {
		t.Fatal(err)
	}
	if got := snapshotList(t, f); got != "" {
		t.Errorf("the file lists %q, a snapshot that cannot prove it belongs to it", got)
	}
	all, err := fsys.AllSnapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || !all[0].Orphan {
		t.Errorf("full listing %+v, want legacy as an orphan", all)
	}
	expectOrphanReport(t, fsys, `snapshot "legacy" is an orphan: an older version wrote it`)
}
