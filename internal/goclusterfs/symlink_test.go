package goclusterfs

import (
	"context"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

func symlinkFS(t *testing.T) *FS {
	t.Helper()
	fsys := withBridge(openFormattedFS(t, newFormattedImage(t)))
	if err := fsys.InitRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	return fsys
}

func linkTarget(n int) string {
	return strings.Repeat("abcdefg/", n/8+1)[:n]
}

func makeSymlink(t *testing.T, fsys *FS, name, target string) *Link {
	t.Helper()
	node, errno := fsys.Symlink(context.Background(), target, name, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("symlink %q to a %d-byte target: %v", name, len(target), errno)
	}
	return node.Operations().(*Link)
}

func lookupLink(t *testing.T, fsys *FS, name string) *Link {
	t.Helper()
	var out fuse.EntryOut
	node, errno := fsys.Lookup(context.Background(), name, &out)
	if errno != 0 {
		t.Fatalf("lookup %q: %v", name, errno)
	}
	if out.Attr.Mode&syscall.S_IFMT != syscall.S_IFLNK {
		t.Fatalf("lookup %q: mode %o is not a symlink", name, out.Attr.Mode)
	}
	return node.Operations().(*Link)
}

func expectTarget(t *testing.T, l *Link, want string) {
	t.Helper()
	got, errno := l.Readlink(context.Background())
	if errno != 0 {
		t.Fatalf("readlink inode %d: %v", l.inodeNum, errno)
	}
	if string(got) != want {
		t.Fatalf("readlink inode %d: %d bytes, want the %d-byte target", l.inodeNum, len(got), len(want))
	}
}

// Targets at the inline limit, just past it and at PATH_MAX read back exactly, with lstat showing their length.
func TestSymlinkTargetsReadBack(t *testing.T) {
	fsys := symlinkFS(t)
	for _, n := range []int{1, inode.MaxInlineTarget, inode.MaxInlineTarget + 1, inode.MaxSymlinkTarget} {
		name, target := fmt.Sprintf("link%d", n), linkTarget(n)
		expectTarget(t, makeSymlink(t, fsys, name, target), target)
		l := lookupLink(t, fsys, name)
		expectTarget(t, l, target)
		var attr fuse.AttrOut
		if errno := l.Getattr(context.Background(), nil, &attr); errno != 0 {
			t.Fatal(errno)
		}
		if attr.Mode != syscall.S_IFLNK|0o777 || attr.Size != uint64(n) || attr.Nlink != 1 {
			t.Errorf("%d-byte link: mode %o, size %d, nlink %d", n, attr.Mode, attr.Size, attr.Nlink)
		}
		ino, err := fsys.inodeTbl.Read(l.inodeNum)
		if err != nil {
			t.Fatal(err)
		}
		if inline := n <= inode.MaxInlineTarget; ino.HasInlineTarget() != inline || ino.AllocatedBlocks != map[bool]uint64{true: 0, false: 1}[inline] {
			t.Errorf("%d-byte link: inline=%v with %d blocks", n, ino.HasInlineTarget(), ino.AllocatedBlocks)
		}
	}
	expectCleanFsck(t, fsys)
}

// Readdir lists a symlink as one, and a subdirectory holds symlinks too.
func TestSymlinkInDirectoryListing(t *testing.T) {
	fsys := symlinkFS(t)
	node, errno := fsys.Mkdir(context.Background(), "d", 0o755, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	sub := node.Operations().(*Dir)
	if _, errno := sub.Symlink(context.Background(), "../x", "l", &fuse.EntryOut{}); errno != 0 {
		t.Fatal(errno)
	}
	stream, errno := sub.Readdir(context.Background())
	if errno != 0 {
		t.Fatal(errno)
	}
	found := false
	for stream.HasNext() {
		e, _ := stream.Next()
		if e.Name == "l" {
			found = e.Mode&syscall.S_IFMT == syscall.S_IFLNK
		}
	}
	if !found {
		t.Error("readdir does not list l as a symlink")
	}
	expectCleanFsck(t, fsys)
}

// Empty and over-long targets, existing names and volumes formatted before symlinks are refused.
func TestSymlinkRefusals(t *testing.T) {
	fsys := symlinkFS(t)
	ctx := context.Background()
	for target, want := range map[string]syscall.Errno{"": syscall.ENOENT, linkTarget(inode.MaxSymlinkTarget + 1): syscall.ENAMETOOLONG} {
		if _, errno := fsys.Symlink(ctx, target, "bad", &fuse.EntryOut{}); errno != want {
			t.Errorf("symlink to a %d-byte target: %v, want %v", len(target), errno, want)
		}
	}
	makeSymlink(t, fsys, "l", "x")
	if _, errno := fsys.Symlink(ctx, "y", "l", &fuse.EntryOut{}); errno != syscall.EEXIST {
		t.Errorf("symlink over an existing name: %v, want EEXIST", errno)
	}
	expectTarget(t, lookupLink(t, fsys, "l"), "x")
	fsys.SetSymlinks(false)
	if _, errno := fsys.Symlink(ctx, "x", "old", &fuse.EntryOut{}); errno != syscall.EOPNOTSUPP {
		t.Errorf("symlink on a volume without symlinks: %v, want EOPNOTSUPP", errno)
	}
}

// Unlinking a symlink frees its inode and, for a long target, its block.
func TestUnlinkSymlinkFreesBlock(t *testing.T) {
	fsys := symlinkFS(t)
	free := fsys.bitmapMgr.FreeCount()
	l := makeSymlink(t, fsys, "long", linkTarget(inode.MaxInlineTarget+1))
	makeSymlink(t, fsys, "short", "x")
	if got := fsys.bitmapMgr.FreeCount(); got != free-1 {
		t.Fatalf("%d free blocks with a long symlink, want %d", got, free-1)
	}
	for _, name := range []string{"long", "short"} {
		if errno := fsys.Unlink(context.Background(), name); errno != 0 {
			t.Fatal(errno)
		}
	}
	if got := fsys.bitmapMgr.FreeCount(); got != free {
		t.Errorf("%d free blocks after the unlinks, want %d", got, free)
	}
	if _, errno := l.Readlink(context.Background()); errno != syscall.ESTALE {
		t.Errorf("readlink of an unlinked symlink: %v, want ESTALE", errno)
	}
	expectCleanFsck(t, fsys)
}

// lchown and lutimes change a symlink; chmod, truncate and xattrs do not apply to one.
func TestSymlinkSetattr(t *testing.T) {
	fsys := symlinkFS(t)
	l := makeSymlink(t, fsys, "l", "x")
	ctx := context.Background()
	mtime := time.Unix(1_700_000_000, 0)
	in := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{Valid: fuse.FATTR_UID | fuse.FATTR_GID | fuse.FATTR_MTIME, Owner: fuse.Owner{Uid: 1234, Gid: 5678}, Mtime: uint64(mtime.Unix())}}
	var out fuse.AttrOut
	if errno := l.Setattr(ctx, nil, in, &out); errno != 0 {
		t.Fatal(errno)
	}
	if out.Uid != 1234 || out.Gid != 5678 || out.Mtime != uint64(mtime.Unix()) || out.Mode != syscall.S_IFLNK|0o777 {
		t.Errorf("after setattr: uid %d, gid %d, mtime %d, mode %o", out.Uid, out.Gid, out.Mtime, out.Mode)
	}
	for valid, want := range map[uint32]syscall.Errno{fuse.FATTR_MODE: syscall.EOPNOTSUPP, fuse.FATTR_SIZE: syscall.EINVAL} {
		if errno := l.Setattr(ctx, nil, &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{Valid: valid}}, &out); errno != want {
			t.Errorf("setattr with valid %x: %v, want %v", valid, errno, want)
		}
	}
	if _, ok := any(l).(fs.NodeSetxattrer); ok {
		t.Error("a symlink node takes xattrs")
	}
	expectTarget(t, l, "x")
}

// A crash after the commit, or after the claim, leaves a symlink that replay finishes with its full target.
func TestSymlinkReplayAfterCrash(t *testing.T) {
	for _, n := range []int{inode.MaxInlineTarget, inode.MaxInlineTarget + 1} {
		for _, point := range []string{"symlink", "symlink: inode claimed"} {
			t.Run(fmt.Sprintf("%d_bytes/%s", n, point), func(t *testing.T) {
				fsys, path, offset, size := journaledFS(t)
				withBridge(fsys)
				target := linkTarget(n)
				crashed := crashAfterCommit(t, fsys, point)
				go func() { _, _ = fsys.Symlink(context.Background(), target, "l", &fuse.EntryOut{}) }()
				<-crashed
				fsys.snapshotWkr.Stop()
				fsys.dev.SetWriteGate(func(int64) error { return errPowerCut })

				after := withBridge(remountAndReplay(t, path, offset, size))
				expectTarget(t, lookupLink(t, after, "l"), target)
				expectCleanFsck(t, after)
			})
		}
	}
}

// Replay leaves a symlink's slot alone once another node took it.
func TestSymlinkReplaySkipsReusedSlot(t *testing.T) {
	fsys, path, offset, size := journaledFS(t)
	withBridge(fsys)
	crashed := crashAfterCommit(t, fsys, "symlink")
	slot, err := fsys.inodeTbl.FindFreeSlot()
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = fsys.Symlink(context.Background(), "target", "l", &fuse.EntryOut{}) }()
	<-crashed
	fsys.snapshotWkr.Stop()
	fsys.dev.SetWriteGate(func(int64) error { return errPowerCut })

	peer := withBridge(openFormattedFS(t, path))
	taken, err := peer.inodeTbl.ClaimSlot(context.Background(), slot)
	if err != nil {
		t.Fatal(err)
	}
	taken.Mode, taken.LinkCount = inode.S_IFREG|0o644, 1
	if err := peer.inodeTbl.Write(context.Background(), taken); err != nil {
		t.Fatal(err)
	}
	replayCrashedJournal(t, peer, offset, size)
	if _, errno := peer.Lookup(context.Background(), "l", &fuse.EntryOut{}); errno != syscall.ENOENT {
		t.Errorf("lookup after replay over a reused slot: %v, want ENOENT", errno)
	}
	ino, err := peer.inodeTbl.Read(slot)
	if err != nil {
		t.Fatal(err)
	}
	if !ino.IsFile() {
		t.Errorf("replay turned the peer's inode %d into mode %o", slot, ino.Mode)
	}
}

// After a power cut during a symlink create, the name is missing or holds the whole target, and nothing leaks.
func TestPowerCutDuringSymlink(t *testing.T) {
	for _, n := range []int{inode.MaxInlineTarget, inode.MaxInlineTarget + 1} {
		target := linkTarget(n)
		t.Run(fmt.Sprintf("%d_bytes", n), func(t *testing.T) {
			runPowerCuts(t, powerCutCase{
				setup: func(t *testing.T, fsys *FS) {
					withBridge(fsys)
					withReserve(t, fsys, 8, time.Hour)
				},
				op: func(fsys *FS, _ *power) {
					_, _ = fsys.Symlink(context.Background(), target, "l", &fuse.EntryOut{})
				},
				afterMount: expectNoLeaks,
				check: func(t *testing.T, fsys *FS) {
					withBridge(fsys)
					if _, errno := fsys.Lookup(context.Background(), "l", &fuse.EntryOut{}); errno == syscall.ENOENT {
						return
					}
					expectTarget(t, lookupLink(t, fsys, "l"), target)
				},
			})
		})
	}
}

func fsckProblems(t *testing.T, fsys *FS) string {
	t.Helper()
	return problemList(runFsck(t, fsys, fsck.DefaultOptions()))
}

// fsck reports each kind of broken symlink and repairs none of them.
func TestFsckReportsBadSymlinks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		long   bool
		damage func(*inode.Inode)
		want   string
	}{
		{"link count", false, func(ino *inode.Inode) { ino.LinkCount = 2 }, "symlink has link count 2"},
		{"inline too long", false, func(ino *inode.Inode) { ino.Size = inode.MaxInlineTarget + 1 }, "stored inline=true"},
		{"nul", false, func(ino *inode.Inode) { ino.Target[0] = 0 }, "target contains a NUL byte"},
		{"padding", false, func(ino *inode.Inode) { ino.Target[5] = 'x' }, "has bytes after its"},
		{"empty", false, func(ino *inode.Inode) { ino.Size = 0 }, "target length 0"},
		{"two blocks", true, func(ino *inode.Inode) { ino.InlineExtents[0].Length = 2 }, "maps more or less than its one target block"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fsys := symlinkFS(t)
			target := "abc"
			if tc.long {
				target = linkTarget(inode.MaxInlineTarget + 1)
			}
			l := makeSymlink(t, fsys, "l", target)
			if _, err := fsys.updateInode(context.Background(), l.inodeNum, func(ino *inode.Inode) error {
				tc.damage(ino)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			repair := fsck.DefaultOptions()
			repair.Mode = fsck.Repair
			r := runFsck(t, fsys, repair)
			if !strings.Contains(problemList(r), tc.want) {
				t.Errorf("fsck did not report %q:%s", tc.want, problemList(r))
			}
			if ino, err := fsys.inodeTbl.Read(l.inodeNum); err != nil || !ino.IsSymlink() {
				t.Errorf("repair removed the bad symlink: %v", err)
			}
		})
	}
}

// fsck reports a symlink whose directory entry is not typed as one, and an entry typed as one for another inode.
func TestFsckReportsSymlinkEntryTypes(t *testing.T) {
	fsys := symlinkFS(t)
	ctx := context.Background()
	l := makeSymlink(t, fsys, "l", "x")
	f := createFile(t, fsys, "f")
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	for name, typ := range map[string]uint8{"l2": dir.DT_REG, "f2": dir.DT_LNK} {
		num := uint64(l.inodeNum)
		if name == "f2" {
			num = uint64(f.inodeNum)
		}
		if err := fsys.dirMgr.Add(ctx, root, name, num, typ); err != nil {
			t.Fatal(err)
		}
	}
	got := fsckProblems(t, fsys)
	for _, want := range []string{`entry "l2" has file type 1, but inode`, `entry "f2" is typed as a symlink`} {
		if !strings.Contains(got, want) {
			t.Errorf("fsck did not report %q:%s", want, got)
		}
	}
}

// A symlink no directory names, as a crash between the claim and the name leaves, is freed by repair with its block.
func TestFsckRepairFreesUnnamedSymlink(t *testing.T) {
	fsys := symlinkFS(t)
	ctx := context.Background()
	free := fsys.bitmapMgr.FreeCount()
	l := makeSymlink(t, fsys, "l", linkTarget(inode.MaxInlineTarget+1))
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.Remove(ctx, root, "l"); err != nil {
		t.Fatal(err)
	}
	repair := fsck.DefaultOptions()
	repair.Mode = fsck.Repair
	if r := runFsck(t, fsys, repair); r.RepairedCount == 0 {
		t.Fatalf("repair fixed nothing:%s", problemList(r))
	}
	ino, err := fsys.inodeTbl.Read(l.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if ino.Magic != 0 || fsys.bitmapMgr.FreeCount() != free {
		t.Errorf("after repair: inode magic %x, %d free blocks, want 0 and %d", ino.Magic, fsys.bitmapMgr.FreeCount(), free)
	}
	expectCleanFsck(t, fsys)
}

// With another node mounted, unlinking a symlink frees it at once instead of listing it as an orphan.
func TestUnlinkSymlinkWithPeerFreesAtOnce(t *testing.T) {
	cluster := newFakeCluster(0, 1)
	fsys := cluster.join(t, symlinkFS(t), 0)
	free := fsys.bitmapMgr.FreeCount()
	l := makeSymlink(t, fsys, "l", linkTarget(inode.MaxInlineTarget+1))
	if errno := fsys.Unlink(context.Background(), "l"); errno != 0 {
		t.Fatal(errno)
	}
	if ino := readInode(t, fsys, l.inodeNum); ino.Magic != 0 {
		t.Errorf("symlink inode %d still in use, holders %#b", l.inodeNum, ino.Holders)
	}
	if orphans := cluster.list(0); len(orphans) != 0 {
		t.Errorf("unlinking a symlink listed orphans %v", orphans)
	}
	if got := fsys.bitmapMgr.FreeCount(); got != free {
		t.Errorf("%d free blocks after the unlink, want %d", got, free)
	}
}

