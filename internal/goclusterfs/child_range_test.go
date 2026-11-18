package goclusterfs

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

func TestEntryPastTheInodeTableIsEIO(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	sub := newSubdir(t, fsys, "sub")
	createFile(t, fsys, "kept")
	const wrapsToRoot = 1<<32 + uint64(inode.InodeRoot)
	for _, parent := range []uint32{inode.InodeRoot, sub.inodeNum} {
		parentIno, err := fsys.inodeTbl.Read(parent)
		if err != nil {
			t.Fatal(err)
		}
		if err := fsys.dirMgr.Add(ctx, parentIno, "bad", wrapsToRoot, dir.DT_REG); err != nil {
			t.Fatal(err)
		}
	}
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	log.SetOutput(&logs)
	_, errno := fsys.Lookup(ctx, "bad", &fuse.EntryOut{})
	log.SetOutput(os.Stderr)
	if errno != syscall.EIO || !strings.Contains(logs.String(), `entry "bad" names inode 4294967298, past the inode table`) {
		t.Errorf("lookup in the root: %v, logged %q; want EIO and a log line naming the entry", errno, logs.String())
	}
	for what, errno := range map[string]syscall.Errno{
		"lookup in a subdirectory":          func() syscall.Errno { _, errno := sub.Lookup(ctx, "bad", &fuse.EntryOut{}); return errno }(),
		"unlink in the root":                fsys.Unlink(ctx, "bad"),
		"unlink in a subdirectory":          sub.Unlink(ctx, "bad"),
		"rename of the entry":               fsys.Rename(ctx, "bad", sub, "moved", 0),
		"rename onto the entry":             fsys.Rename(ctx, "kept", sub, "bad", 0),
		"API path lookup in the root":       func() syscall.Errno { _, err := fsys.lookupFile(ctx, "/bad"); return errnoOf(err) }(),
		"API path lookup in a subdirectory": func() syscall.Errno { _, err := fsys.lookupFile(ctx, "/sub/bad"); return errnoOf(err) }(),
	} {
		if errno != syscall.EIO {
			t.Errorf("%s: %v, want EIO", what, errno)
		}
	}
	after, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil || after.MetaGeneration != root.MetaGeneration || after.LinkCount != root.LinkCount {
		t.Errorf("root inode after the refused operations: %+v, %v; want it unchanged", after, err)
	}
}

func errnoOf(err error) syscall.Errno {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	return 0
}
