package goclusterfs

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func statfsOf(t *testing.T, node interface {
	Statfs(context.Context, *fuse.StatfsOut) syscall.Errno
}) fuse.StatfsOut {
	t.Helper()
	var out fuse.StatfsOut
	if errno := node.Statfs(context.Background(), &out); errno != 0 {
		t.Fatalf("statfs: %v", errno)
	}
	return out
}

// statfs on a directory or file in a project with limits reports the project: its limits as the size, and what they
// leave as the free blocks and inodes. Outside a project, for a limit left unset, and where a limit promises more than
// the volume has, it reports the volume.
func TestStatfsReportsTheProjectLimits(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	bs := int64(fsys.dataBlockSize())
	plain := mkdirIn(t, fsys, "plain")
	pvc := limitedProject(t, fsys, "pvc", "bytes=10485760 inodes=5")
	only := limitedProject(t, fsys, "only-inodes", "inodes=4")
	big := limitedProject(t, fsys, "overcommitted", "bytes=1099511627776")
	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(1, int(2*bs)), 0)

	vol := statfsOf(t, fsys)
	if got := statfsOf(t, plain); got.Blocks != vol.Blocks || got.Bfree != vol.Bfree || got.Files != vol.Files || got.Ffree != vol.Ffree {
		t.Errorf("statfs outside a project = %+v, want the volume's %+v", got, vol)
	}
	// The project's root directory takes 1 block and 1 inode, and the file 2 blocks and 1 inode.
	for name, got := range map[string]fuse.StatfsOut{"the project's root": statfsOf(t, pvc), "a file in it": statfsOf(t, f)} {
		if got.Blocks != 10 || got.Bfree != 7 || got.Bavail != 7 || got.Files != 5 || got.Ffree != 3 || got.Bsize != vol.Bsize {
			t.Errorf("statfs on %s = blocks %d free %d avail %d, inodes %d free %d; want 10, 7, 7, 5, 3",
				name, got.Blocks, got.Bfree, got.Bavail, got.Files, got.Ffree)
		}
	}
	if got := statfsOf(t, big); got.Blocks != vol.Blocks || got.Bfree != vol.Bfree || got.Bavail != vol.Bavail {
		t.Errorf("statfs on a project whose 1 TB limit passes the volume = blocks %d free %d avail %d; want the volume's %d, %d, %d",
			got.Blocks, got.Bfree, got.Bavail, vol.Blocks, vol.Bfree, vol.Bavail)
	}
	if got := statfsOf(t, only); got.Blocks != vol.Blocks || got.Files != 4 || got.Ffree != 3 {
		t.Errorf("statfs on a project with only an inode limit = blocks %d, inodes %d free %d; want the volume's %d blocks and 4, 3",
			got.Blocks, got.Files, got.Ffree, vol.Blocks)
	}
}
