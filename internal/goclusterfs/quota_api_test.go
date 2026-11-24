package goclusterfs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/quota"
)

func limit(n uint64) *uint64 { return &n }

// The API's quota calls do what the xattr does on the directory at a path: setting one on an empty directory gives
// it a project, setting it again changes the limits it names and keeps the others, and reading it returns what
// getfattr shows, from any directory in the project.
func TestProjectQuotaByPath(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	ctx := context.Background()
	pvc := mkdirIn(t, fsys, "pvc")
	bs := fsys.dataBlockSize()

	q, err := fsys.SetProjectQuota(ctx, "/pvc", limit(1<<30), limit(100))
	if err != nil {
		t.Fatal(err)
	}
	if want := (quota.Status{Project: 1, LimitBytes: 1 << 30, LimitInodes: 100, UsedBytes: bs, UsedInodes: 1}); *q != want {
		t.Errorf("after setting the quota: %+v, want %+v", *q, want)
	}
	mkdirIn(t, pvc, "sub")
	q, err = fsys.SetProjectQuota(ctx, "/pvc", limit(2<<30), nil)
	if err != nil {
		t.Fatal(err)
	}
	if q.LimitBytes != 2<<30 || q.LimitInodes != 100 || q.UsedInodes != 2 {
		t.Errorf("after a resize that names only bytes: %+v, want 2 GiB, the 100-inode limit kept and 2 inodes used", *q)
	}
	if q, err = fsys.SetProjectQuota(ctx, "/pvc", limit(bs+1), nil); err != nil || q.LimitBytes != 2*bs {
		t.Errorf("after a limit of a block and a byte: %+v, %v; want it rounded up to 2 blocks", q, err)
	}
	if q, err = fsys.SetProjectQuota(ctx, "/pvc", limit(2<<30), limit(0)); err != nil || q.LimitBytes != 2<<30 || q.LimitInodes != 0 {
		t.Errorf("after lifting the inode limit: %+v, %v; want 2 GiB and no inode limit", q, err)
	}
	if got, want := quotaOf(t, pvc), fmt.Sprintf("project=1 bytes=%d inodes=0 used_bytes=%d used_inodes=2", 2<<30, q.UsedBytes); got != want {
		t.Errorf("getfattr reads %q, want %q", got, want)
	}
	if sub, err := fsys.ProjectQuota(ctx, "/pvc/sub"); err != nil || *sub != *q {
		t.Errorf("the quota read from a subdirectory: %+v, %v; want the project's %+v", sub, err, *q)
	}
}

// A key the xattr's value leaves out keeps the project's limit, and on a new project is none.
func TestQuotaXattrKeepsTheLimitsItLeavesOut(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	ctx := context.Background()
	pvc := mkdirIn(t, fsys, "pvc")
	for _, c := range []struct{ value, want string }{
		{"inodes=10", "bytes=0 inodes=10"},
		{"bytes=1048576", "bytes=1048576 inodes=10"},
		{"inodes=0", "bytes=1048576 inodes=0"},
	} {
		if errno := pvc.Setxattr(ctx, quotaXattr, []byte(c.value), 0); errno != 0 {
			t.Fatalf("set %q: %v", c.value, errno)
		}
		if got := quotaOf(t, pvc); !strings.HasPrefix(got, "project=1 "+c.want+" ") {
			t.Errorf("after %q the quota reads %q, want %q", c.value, got, c.want)
		}
	}
}

// A caller that gives up, such as an HTTP client that timed out, does not stop a quota change halfway: the change
// waits out a peer's quota lock although its caller is gone.
func TestProjectQuotaByPathOutlivesItsCaller(t *testing.T) {
	fsys, path, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	mkdirIn(t, fsys, "pvc")
	peer := openFormattedComponents(t, path)
	unlock, err := peer.quotaGuard.Lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(200*time.Millisecond, unlock)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q, err := fsys.SetProjectQuota(ctx, "/pvc", limit(1<<30), nil)
	if err != nil || q.Project != 1 || q.LimitBytes != 1<<30 {
		t.Fatalf("set the quota with a canceled context: %+v, %v; want project 1 with 1 GiB", q, err)
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
		if _, err := fsys.SetProjectQuota(ctx, p, limit(1<<20), limit(1)); !errors.Is(err, want) {
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
