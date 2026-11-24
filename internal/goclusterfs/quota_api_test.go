package goclusterfs

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/quota"
)

// The API's quota calls do what the xattr does on the directory at a path: setting one on an empty directory gives
// it a project, setting it again changes the limits, and reading it returns what getfattr shows, from any directory
// in the project.
func TestProjectQuotaByPath(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	ctx := context.Background()
	pvc := mkdirIn(t, fsys, "pvc")
	bs := fsys.dataBlockSize()

	q, err := fsys.SetProjectQuota(ctx, "/pvc", 1<<30, 100)
	if err != nil {
		t.Fatal(err)
	}
	if want := (quota.Status{Project: 1, LimitBytes: 1 << 30, LimitInodes: 100, UsedBytes: bs, UsedInodes: 1}); *q != want {
		t.Errorf("after setting the quota: %+v, want %+v", *q, want)
	}
	mkdirIn(t, pvc, "sub")
	q, err = fsys.SetProjectQuota(ctx, "/pvc", 2<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	if q.LimitBytes != 2<<30 || q.LimitInodes != 0 || q.UsedInodes != 2 {
		t.Errorf("after changing the limits: %+v, want 2 GiB, no inode limit and 2 inodes used", *q)
	}
	if got, want := quotaOf(t, pvc), fmt.Sprintf("project=1 bytes=%d inodes=0 used_bytes=%d used_inodes=2", 2<<30, q.UsedBytes); got != want {
		t.Errorf("getfattr reads %q, want %q", got, want)
	}
	if sub, err := fsys.ProjectQuota(ctx, "/pvc/sub"); err != nil || *sub != *q {
		t.Errorf("the quota read from a subdirectory: %+v, %v; want the project's %+v", sub, err, *q)
	}
}

// The API's quota calls refuse what the xattr refuses, and paths that are not a directory.
func TestProjectQuotaByPathRefuses(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	ctx := context.Background()
	mkdirIn(t, quotaProject(t, fsys, "pvc"), "sub")
	createIn(t, mkdirIn(t, fsys, "full"), "f")
	createFile(t, fsys, "f")
	makeSymlink(t, fsys, "ln", "pvc")
	mkdirIn(t, fsys, "plain")

	for p, want := range map[string]syscall.Errno{
		"/pvc/sub":  syscall.EPERM,
		"/":         syscall.EPERM,
		"/full":     syscall.ENOTEMPTY,
		"/f":        syscall.ENOTDIR,
		"/ln":       syscall.ELOOP,
		"/ln/sub":   syscall.ELOOP,
		"/missing":  syscall.ENOENT,
		"/pvc/../f": syscall.EINVAL,
		"pvc":       syscall.EINVAL,
	} {
		if _, err := fsys.SetProjectQuota(ctx, p, 1<<20, 1); !errors.Is(err, want) {
			t.Errorf("set the quota of %s: %v, want %v", p, err, want)
		}
	}
	for p, want := range map[string]syscall.Errno{
		"/":      syscall.ENODATA,
		"/plain": syscall.ENODATA,
		"/f":     syscall.ENOTDIR,
		"/ln":    syscall.ELOOP,
	} {
		if _, err := fsys.ProjectQuota(ctx, p); !errors.Is(err, want) {
			t.Errorf("read the quota of %s: %v, want %v", p, err, want)
		}
	}
}
