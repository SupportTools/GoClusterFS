package goclusterfs

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/inode"
)

func asCaller(uid, gid uint32) context.Context {
	return fuse.NewContext(context.Background(), &fuse.Caller{Owner: fuse.Owner{Uid: uid, Gid: gid}})
}

func setOwner(t *testing.T, fsys *FS, num, uid, gid uint32, mode uint16) {
	t.Helper()
	if _, err := fsys.updateInode(context.Background(), num, func(ino *inode.Inode) error {
		ino.UID, ino.GID = uid, gid
		ino.Mode = ino.Mode&inode.S_IFMT | mode
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func checkOwner(t *testing.T, fsys *FS, what string, num, uid, gid uint32, mode uint16) {
	t.Helper()
	ino := readInode(t, fsys, num)
	if ino.UID != uid || ino.GID != gid || ino.Mode != mode {
		t.Errorf("%s: uid %d gid %d mode %#o, want uid %d gid %d mode %#o", what, ino.UID, ino.GID, ino.Mode, uid, gid, mode)
	}
}

func TestNewFilesTakeCallerAndSetgidGroup(t *testing.T) {
	fsys := withBridge(openFormattedFS(t, newFormattedImage(t)))
	const alice, team = 2001, 3000
	ctx := asCaller(alice, alice)

	teamNode, errno := fsys.Mkdir(asCaller(0, 0), "team", 0o2770, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("mkdir team: %v", errno)
	}
	teamDir := teamNode.Operations().(*Dir)
	checkOwner(t, fsys, "team as root made it", teamDir.inodeNum, 0, 0, inode.S_IFDIR|0o2770)
	setOwner(t, fsys, teamDir.inodeNum, alice, team, 0o2770)

	fileNode, _, _, errno := teamDir.Create(ctx, "f", syscall.O_RDWR|syscall.O_CREAT, 0o640, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("create f: %v", errno)
	}
	f := fileNode.Operations().(*File)
	checkOwner(t, fsys, "file in a setgid directory", f.inodeNum, alice, team, inode.S_IFREG|0o640)

	subNode, errno := teamDir.Mkdir(ctx, "sub", 0o750, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("mkdir sub: %v", errno)
	}
	sub := subNode.Operations().(*Dir)
	checkOwner(t, fsys, "directory in a setgid directory", sub.inodeNum, alice, team, inode.S_IFDIR|inode.S_ISGID|0o750)

	linkNode, errno := teamDir.Symlink(ctx, "f", "l", &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("symlink l: %v", errno)
	}
	checkOwner(t, fsys, "symlink in a setgid directory", linkNode.Operations().(*Link).inodeNum, alice, team, inode.S_IFLNK|0o777)

	pubNode, errno := fsys.Mkdir(ctx, "pub", 0o1777, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("mkdir pub: %v", errno)
	}
	pub := pubNode.Operations().(*Dir)
	checkOwner(t, fsys, "sticky directory outside a setgid one", pub.inodeNum, alice, alice, inode.S_IFDIR|inode.S_ISVTX|0o777)

	for _, c := range []struct {
		what string
		node fs.NodeSetattrer
		num  uint32
		want uint16
	}{
		{"chmod 4750 on a file", f, f.inodeNum, inode.S_IFREG | 0o4750},
		{"chmod 3775 on a directory", sub, sub.inodeNum, inode.S_IFDIR | 0o3775},
	} {
		var out fuse.AttrOut
		if errno := c.node.Setattr(ctx, nil, chmodIn(uint32(c.want&^inode.S_IFMT)), &out); errno != 0 {
			t.Fatalf("%s: %v", c.what, errno)
		}
		if got := readInode(t, fsys, c.num).Mode; got != c.want || out.Mode != uint32(c.want) {
			t.Errorf("%s: stored %#o, reported %#o, want %#o", c.what, got, out.Mode, c.want)
		}
	}
	expectNoProblems(t, fsys, "after the creates")
}

func TestCloneChecksCallerAccess(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	const alice, bob = 2001, 2002
	src := createFile(t, fsys, "src")
	writeAt(t, src, pattern(1, 4096), 0)
	setOwner(t, fsys, src.inodeNum, alice, alice, 0o4622)
	setOwner(t, fsys, inode.InodeRoot, 0, 0, 0o755)

	clone := func(ctx context.Context, name string) syscall.Errno {
		return src.Setxattr(ctx, "user.clone", []byte("/"+name), 0)
	}
	if errno := clone(asCaller(bob, bob), "by-bob"); errno != syscall.EACCES {
		t.Errorf("clone by a user who cannot read the file: %v, want EACCES", errno)
	}
	if errno := clone(asCaller(alice, alice), "by-alice"); errno != syscall.EACCES {
		t.Errorf("clone into a root directory alice cannot write: %v, want EACCES", errno)
	}

	setOwner(t, fsys, inode.InodeRoot, 0, 0, 0o777)
	if errno := clone(asCaller(alice, alice), "by-alice"); errno != 0 {
		t.Fatalf("clone by the owner into a writable root: %v", errno)
	}
	checkOwner(t, fsys, "alice's clone", lookupFile(t, fsys, "by-alice").inodeNum, alice, alice, inode.S_IFREG|0o622)

	if errno := clone(asCaller(0, 0), "by-root"); errno != 0 {
		t.Fatalf("clone by root: %v", errno)
	}
	checkOwner(t, fsys, "root's clone", lookupFile(t, fsys, "by-root").inodeNum, alice, alice, inode.S_IFREG|0o4622)

	setOwner(t, fsys, src.inodeNum, alice, alice, 0o646)
	setxattr(t, src, "user.snapshot", "old")
	setOwner(t, fsys, src.inodeNum, alice, alice, 0o602)
	snapshotClone := func(name string) syscall.Errno {
		return src.Setxattr(asCaller(bob, bob), "user.snapshot.clone", []byte("old:/"+name), 0)
	}
	if errno := snapshotClone("bob-old"); errno != syscall.EACCES {
		t.Errorf("clone of a snapshot taken while bob could read the file, which he no longer can: %v, want EACCES", errno)
	}
	setOwner(t, fsys, src.inodeNum, alice, alice, 0o646)
	if errno := snapshotClone("bob-old"); errno != 0 {
		t.Fatalf("clone of a snapshot of a file bob can read: %v", errno)
	}
	checkOwner(t, fsys, "bob's snapshot clone", lookupFile(t, fsys, "bob-old").inodeNum, bob, bob, inode.S_IFREG|0o646)
	expectNoProblems(t, fsys, "after the clones")
}
