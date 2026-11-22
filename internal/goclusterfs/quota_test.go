package goclusterfs

import (
	"context"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/inode"
)

func mkdirIn(t *testing.T, parent fs.NodeMkdirer, name string) *Dir {
	t.Helper()
	node, errno := parent.Mkdir(context.Background(), name, 0o755, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("mkdir %s: %v", name, errno)
	}
	return node.Operations().(*Dir)
}

func createIn(t *testing.T, parent fs.NodeCreater, name string) *File {
	t.Helper()
	node, _, _, errno := parent.Create(context.Background(), name, syscall.O_RDWR, 0o644, &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("create %s: %v", name, errno)
	}
	return node.Operations().(*File)
}

func quotaOf(t *testing.T, d *Dir) string {
	t.Helper()
	buf := make([]byte, 256)
	n, errno := d.Getxattr(context.Background(), quotaXattr, buf)
	if errno != 0 {
		t.Fatalf("getxattr on directory inode %d: %v", d.inodeNum, errno)
	}
	return string(buf[:n])
}

func projectOf(t *testing.T, fsys *FS, num uint32) (uint32, bool) {
	t.Helper()
	ino, err := fsys.inodeTbl.Read(num)
	if err != nil {
		t.Fatal(err)
	}
	return ino.ProjectID, ino.Flags&inode.FlagProjInherit != 0
}

// The quota attribute gives an empty directory a new project. Files, directories and symlinks made under it join
// the project, and new directories pass it on. Only the project's root takes new limits, and removing the attribute
// lifts the limits but keeps the project.
func TestQuotaXattrMakesAProject(t *testing.T) {
	t.Parallel()
	fsys := openFormattedFS(t, newFormattedImage(t))
	ctx := context.Background()

	pvc := mkdirIn(t, fsys, "pvc")
	if errno := pvc.Setxattr(ctx, quotaXattr, []byte("bytes=3145728 inodes=10"), 0); errno != 0 {
		t.Fatalf("set the quota on an empty directory: %v", errno)
	}
	if got, want := quotaOf(t, pvc), "project=1 bytes=3145728 inodes=10 used_bytes=0 used_inodes=0"; got != want {
		t.Errorf("quota = %q, want %q", got, want)
	}
	if p, inherit := projectOf(t, fsys, pvc.inodeNum); p != 1 || !inherit {
		t.Errorf("the project root has project %d, PROJINHERIT %v; want 1 and set", p, inherit)
	}

	file := createIn(t, pvc, "f")
	sub := mkdirIn(t, pvc, "sub")
	link, errno := sub.Symlink(ctx, "target", "s", &fuse.EntryOut{})
	if errno != 0 {
		t.Fatalf("symlink: %v", errno)
	}
	deep := createIn(t, sub, "g")
	for name, num := range map[string]uint32{"file": file.inodeNum, "subdirectory": sub.inodeNum, "symlink": link.Operations().(*Link).inodeNum, "nested file": deep.inodeNum} {
		if p, _ := projectOf(t, fsys, num); p != 1 {
			t.Errorf("the %s has project %d, want 1", name, p)
		}
	}
	if _, inherit := projectOf(t, fsys, sub.inodeNum); !inherit {
		t.Error("a new directory in the project lacks PROJINHERIT")
	}

	if errno := sub.Setxattr(ctx, quotaXattr, []byte("bytes=1"), 0); errno != syscall.EPERM {
		t.Errorf("set the quota on a directory inside the project: %v, want EPERM", errno)
	}
	if errno := pvc.Setxattr(ctx, quotaXattr, []byte("bytes=5242880"), 0); errno != 0 {
		t.Fatalf("change the project's limits: %v", errno)
	}
	if got := quotaOf(t, pvc); !strings.HasPrefix(got, "project=1 bytes=5242880 inodes=0 ") {
		t.Errorf("after the change the quota is %q", got)
	}
	if errno := pvc.Removexattr(ctx, quotaXattr); errno != 0 {
		t.Fatalf("remove the quota: %v", errno)
	}
	if got := quotaOf(t, pvc); !strings.HasPrefix(got, "project=1 bytes=0 inodes=0 ") {
		t.Errorf("after the removal the quota is %q, want project 1 with no limits", got)
	}

	full := mkdirIn(t, fsys, "full")
	createIn(t, full, "x")
	if errno := full.Setxattr(ctx, quotaXattr, []byte("bytes=1"), 0); errno != syscall.ENOTEMPTY {
		t.Errorf("set the quota on a non-empty directory: %v, want ENOTEMPTY", errno)
	}
	if errno := full.Setxattr(ctx, quotaXattr, []byte("bytes=1 size=2"), 0); errno != syscall.EINVAL {
		t.Errorf("a value with an unknown field: %v, want EINVAL", errno)
	}
	if _, errno := full.Getxattr(ctx, quotaXattr, make([]byte, 64)); errno != syscall.ENODATA {
		t.Errorf("read the quota of a directory with no project: %v, want ENODATA", errno)
	}
	other := mkdirIn(t, fsys, "other")
	if errno := other.Setxattr(ctx, quotaXattr, []byte("inodes=5"), 0); errno != 0 {
		t.Fatalf("a second project: %v", errno)
	}
	if got := quotaOf(t, other); !strings.HasPrefix(got, "project=2 ") {
		t.Errorf("the second project's quota is %q, want project 2", got)
	}
}

// Links, renames and clones may not move a file into another project or out of its own, since its usage would no
// longer count where its directory says. Only a project's root moves, between directories outside every project.
// A snapshot's data counts against the project of the file it was taken of.
func TestQuotaProjectsKeepTheirFiles(t *testing.T) {
	t.Parallel()
	fsys := openFormattedFS(t, newFormattedImage(t))
	ctx := context.Background()
	a, b, plain := mkdirIn(t, fsys, "a"), mkdirIn(t, fsys, "b"), mkdirIn(t, fsys, "plain")
	for _, d := range []*Dir{a, b} {
		if errno := d.Setxattr(ctx, quotaXattr, []byte("bytes=10485760"), 0); errno != 0 {
			t.Fatal(errno)
		}
	}
	inA := createIn(t, a, "f")
	outside := createIn(t, plain, "g")

	for _, tc := range []struct {
		name string
		do   func() syscall.Errno
		want syscall.Errno
	}{
		{"link into another project", func() syscall.Errno { _, e := b.Link(ctx, inA, "l", &fuse.EntryOut{}); return e }, syscall.EXDEV},
		{"link out of the project", func() syscall.Errno { _, e := plain.Link(ctx, inA, "l", &fuse.EntryOut{}); return e }, syscall.EXDEV},
		{"link into the project", func() syscall.Errno { _, e := a.Link(ctx, outside, "l", &fuse.EntryOut{}); return e }, syscall.EXDEV},
		{"rename into another project", func() syscall.Errno { return a.Rename(ctx, "f", b, "f", 0) }, syscall.EXDEV},
		{"rename out of the project", func() syscall.Errno { return a.Rename(ctx, "f", plain, "f", 0) }, syscall.EXDEV},
		{"rename into the project", func() syscall.Errno { return plain.Rename(ctx, "g", a, "g", 0) }, syscall.EXDEV},
		{"clone out of the project", func() syscall.Errno { return inA.Setxattr(ctx, "user.clone", []byte("/c"), 0) }, syscall.EXDEV},
		{"move a project root into another project", func() syscall.Errno { return fsys.Rename(ctx, "a", b, "a", 0) }, syscall.EXDEV},
		{"link within the project", func() syscall.Errno { _, e := a.Link(ctx, inA, "f2", &fuse.EntryOut{}); return e }, 0},
		{"rename within the project", func() syscall.Errno { return a.Rename(ctx, "f2", a, "f3", 0) }, 0},
		{"move a project root outside every project", func() syscall.Errno { return fsys.Rename(ctx, "a", plain, "a", 0) }, 0},
		{"clone a file in no project", func() syscall.Errno { return outside.Setxattr(ctx, "user.clone", []byte("/c"), 0) }, 0},
	} {
		if got := tc.do(); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}

	writeAt(t, inA, pattern(1, 100), 0)
	setxattr(t, inA, "user.snapshot", "s1")
	found := false
	for num := inode.InodeFirstUsable; num < 64; num++ {
		ino, err := fsys.inodeTbl.Read(num)
		if err != nil || ino.Magic != inode.InodeMagic || ino.Flags&inode.FlagSnapshotData == 0 {
			continue
		}
		found = true
		if ino.ProjectID != 1 {
			t.Errorf("the snapshot's data inode %d has project %d, want the file's 1", num, ino.ProjectID)
		}
	}
	if !found {
		t.Error("found no snapshot data inode")
	}
}
