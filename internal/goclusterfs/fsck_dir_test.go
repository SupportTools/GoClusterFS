package goclusterfs

import (
	"context"
	"fmt"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// fsck checks the type of every directory entry against its inode, whatever
// the inode number, and reports an entry past the inode table instead of
// reading the inode its number wraps to.
func TestFsckChecksEveryDirectoryEntry(t *testing.T) {
	t.Parallel()
	fsys := openFormattedFS(t, newFormattedImage(t))
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatal(err)
	}
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	total := sb.InodeTableSize / inode.InodeSize
	high := uint32(total - 1)
	if _, err := fsys.inodeTbl.ClaimSlotAs(ctx, high, func(ino *inode.Inode) { ino.Mode = inode.S_IFREG | 0o644 }); err != nil {
		t.Fatal(err)
	}
	past := uint64(1)<<32 | uint64(inode.InodeRoot)
	for name, num := range map[string]uint64{"high": uint64(high), "past": past} {
		root, err := fsys.inodeTbl.Read(inode.InodeRoot)
		if err != nil {
			t.Fatal(err)
		}
		if err := fsys.dirMgr.Add(ctx, root, name, num, dir.DT_DIR); err != nil {
			t.Fatal(err)
		}
	}

	r := runFsck(t, fsys, fsck.DefaultOptions())
	expectProblem(t, r, fmt.Sprintf("entry %q has file type %d, but inode %d has type %d", "high", dir.DT_DIR, high, dir.DT_REG))
	expectProblem(t, r, fmt.Sprintf("entry %q references inode %d, past the inode table's %d inodes", "past", past, total))
}
