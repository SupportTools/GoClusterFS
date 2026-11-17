package goclusterfs

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

func linkCount(t *testing.T, fsys *FS, num uint32) uint32 {
	t.Helper()
	ino, err := fsys.inodeTbl.Read(num)
	if err != nil {
		t.Fatal(err)
	}
	return ino.LinkCount
}

func mustLink(t *testing.T, fsys *FS, parent uint32, file *File, name string) {
	t.Helper()
	if _, errno := fsys.addLink(context.Background(), parent, file, name); errno != 0 {
		t.Fatalf("link %q to inode %d: %v", name, file.inodeNum, errno)
	}
}

// Each link adds one to the count and each unlink takes one away; the data stays readable through every name left,
// and the file is freed with its last name.
func TestLinkCountsNames(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	sub := newSubdir(t, fsys, "sub")
	a := createFile(t, fsys, "a")
	writeAt(t, a, pattern(1, BlockSize+100), 0)

	mustLink(t, fsys, inode.InodeRoot, a, "b")
	mustLink(t, fsys, sub.inodeNum, a, "c")
	if got := linkCount(t, fsys, a.inodeNum); got != 3 {
		t.Fatalf("link count after two links: %d, want 3", got)
	}
	if b := lookupFile(t, fsys, "b"); b.inodeNum != a.inodeNum {
		t.Fatalf("b names inode %d, want %d", b.inodeNum, a.inodeNum)
	}

	if errno := fsys.RootDir().Unlink(ctx, "a"); errno != 0 {
		t.Fatalf("unlink a: %v", errno)
	}
	if got := linkCount(t, fsys, a.inodeNum); got != 2 {
		t.Fatalf("link count after one unlink: %d, want 2", got)
	}
	checkFile(t, lookupFile(t, fsys, "b"), pattern(1, BlockSize+100))
	expectCleanFsck(t, fsys)

	if errno := fsys.RootDir().Unlink(ctx, "b"); errno != 0 {
		t.Fatalf("unlink b: %v", errno)
	}
	if errno := sub.Unlink(ctx, "c"); errno != 0 {
		t.Fatalf("unlink c: %v", errno)
	}
	if ino, err := fsys.inodeTbl.Read(a.inodeNum); err != nil || ino.Magic != 0 {
		t.Errorf("inode %d after its last name went: magic %#x, %v; want it freed", a.inodeNum, ino.Magic, err)
	}
	expectCleanFsck(t, fsys)
}

func TestLinkRefuses(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	sub := newSubdir(t, fsys, "sub")
	a := createFile(t, fsys, "a")
	createFile(t, fsys, "taken")

	if _, errno := fsys.link(ctx, &fsys.Inode, inode.InodeRoot, sub, "dirlink", &fuse.EntryOut{}); errno != syscall.EPERM {
		t.Errorf("link to a directory: %v, want EPERM", errno)
	}
	if _, errno := fsys.addLink(ctx, inode.InodeRoot, a, "taken"); errno != syscall.EEXIST {
		t.Errorf("link over a taken name: %v, want EEXIST", errno)
	}
	setLinks := func(n uint32) {
		t.Helper()
		if _, err := fsys.updateInode(ctx, a.inodeNum, func(ino *inode.Inode) error { ino.LinkCount = n; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	setLinks(maxLinks)
	if _, errno := fsys.addLink(ctx, inode.InodeRoot, a, "many"); errno != syscall.EMLINK {
		t.Errorf("link at %d links: %v, want EMLINK", maxLinks, errno)
	}
	setLinks(0)
	if _, errno := fsys.addLink(ctx, inode.InodeRoot, a, "orphan"); errno != syscall.ENOENT {
		t.Errorf("link to a file with no link left: %v, want ENOENT", errno)
	}
	if got := linkCount(t, fsys, a.inodeNum); got != 0 {
		t.Errorf("a refused link changed the count to %d", got)
	}
}

// rootNames returns the names in the root directory that name inode num, sorted.
func rootNames(t *testing.T, fsys *FS, num uint32) []string {
	t.Helper()
	entries, err := fsys.dirMgr.List(context.Background(), readRoot(t, fsys))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if e.InodeNum == uint64(num) {
			names = append(names, e.Name)
		}
	}
	slices.Sort(names)
	return names
}

// A link crashes at each step, sometimes after a write changed the file, and its journal is replayed. Replay finishes
// the link only if nothing changed the file since its record. A count that landed without its name stays an extra
// link, which fsck reports and repair takes back.
func TestLinkReplaysAfterCrash(t *testing.T) {
	for _, tc := range []struct {
		crashPoint string
		changed    bool
		names      []string
		links      uint32
	}{
		{"link", false, []string{"a", "b"}, 2},
		{"link", true, []string{"a"}, 1},
		{"link: counted", false, []string{"a"}, 2},
		{"link: counted", true, []string{"a"}, 2},
		{"link: named", false, []string{"a", "b"}, 2},
	} {
		name := strings.ReplaceAll(tc.crashPoint, ": ", "_")
		if tc.changed {
			name += "_then_changed"
		}
		t.Run(name, func(t *testing.T) {
			fsys, path, offset, size := journaledFS(t)
			a := createFile(t, fsys, "a")
			want := pattern(2, 300)
			writeAt(t, a, want, 0)
			crashed := crashAfterCommit(t, fsys, tc.crashPoint)
			go func() { _, _ = fsys.addLink(context.Background(), inode.InodeRoot, a, "b") }()
			<-crashed
			if tc.changed {
				afterCommit = nil
				writeAt(t, a, pattern(3, 100), BlockSize)
				want = append(append(want, make([]byte, BlockSize-300)...), pattern(3, 100)...)
			}

			after := remountAndReplay(t, path, offset, size)
			if got := rootNames(t, after, a.inodeNum); !slices.Equal(got, tc.names) {
				t.Fatalf("names after replay: %v, want %v", got, tc.names)
			}
			for _, n := range tc.names {
				checkFile(t, lookupFile(t, after, n), want)
			}
			if got := linkCount(t, after, a.inodeNum); got != tc.links {
				t.Errorf("link count after replay: %d, want %d", got, tc.links)
			}
			if int(tc.links) == len(tc.names) {
				expectCleanFsck(t, after)
				return
			}
			extra := fmt.Sprintf("has %d links but %d names", tc.links, len(tc.names))
			if r := runFsck(t, after, fsck.DefaultOptions()); !strings.Contains(problemList(r), extra) {
				t.Errorf("fsck should report the extra link:%s", problemList(r))
			}
			expectRepairedClean(t, after)
			if got := linkCount(t, after, a.inodeNum); got != uint32(len(tc.names)) {
				t.Errorf("link count after repair: %d, want %d", got, len(tc.names))
			}
		})
	}
}

// A link finishes, but its completion record never reaches the device, and another node renames the new name before
// replay. Replay must not add the old name back: three names on two links would let unlinking free the file.
func TestLinkReplayAfterPeerRename(t *testing.T) {
	ctx := context.Background()
	fsys, path, offset, size := journaledFS(t)
	peer := openFormattedFS(t, path)
	a := createFile(t, fsys, "a")
	crashed := crashAfterCommit(t, fsys, "link: named")
	go func() { _, _ = fsys.addLink(ctx, inode.InodeRoot, a, "b") }()
	<-crashed
	if errno := peer.Rename(ctx, "b", peer, "c", 0); errno != 0 {
		t.Fatalf("peer rename b to c: %v", errno)
	}

	after := remountAndReplay(t, path, offset, size)
	if got := rootNames(t, after, a.inodeNum); !slices.Equal(got, []string{"a", "c"}) {
		t.Errorf("names after replay: %v, want [a c]", got)
	}
	if got := linkCount(t, after, a.inodeNum); got != 2 {
		t.Errorf("link count after replay: %d, want 2", got)
	}
	expectCleanFsck(t, after)
}

// A peer frees the source and a new file takes its slot before the link reads it: the link fails rather than give the
// new file a name.
func TestLinkRefusesReusedSlot(t *testing.T) {
	ctx := context.Background()
	fsys, path, _, _ := journaledFS(t)
	peer := openFormattedFS(t, path)
	a := createFile(t, fsys, "a")
	if errno := peer.RootDir().Unlink(ctx, "a"); errno != 0 {
		t.Fatalf("peer unlink a: %v", errno)
	}
	other := createFile(t, peer, "other")
	if other.inodeNum != a.inodeNum {
		t.Fatalf("the peer's new file took slot %d, want a's slot %d", other.inodeNum, a.inodeNum)
	}
	if _, errno := fsys.addLink(ctx, inode.InodeRoot, a, "b"); errno != syscall.ENOENT {
		t.Errorf("link to a file whose slot another file took: %v, want ENOENT", errno)
	}
	if got := linkCount(t, peer, other.inodeNum); got != 1 {
		t.Errorf("the new file has %d links, want 1", got)
	}
}

// A name without its link is the dangerous mismatch: unlinking the other name would free the file under it.
func TestFsckRepairsTooFewLinks(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	a := createFile(t, fsys, "a")
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.dirMgr.Add(ctx, root, "uncounted", uint64(a.inodeNum), dir.DT_REG); err != nil {
		t.Fatal(err)
	}

	r := runFsck(t, fsys, fsck.DefaultOptions())
	if !strings.Contains(problemList(r), "has 1 links but 2 names: unlinking can free it while a name remains") {
		t.Errorf("fsck should report the missing link:%s", problemList(r))
	}
	expectRepairedClean(t, fsys)
	if got := linkCount(t, fsys, a.inodeNum); got != 2 {
		t.Errorf("link count after repair: %d, want 2", got)
	}
}

// Power fails during a link: the file keeps its data under its first name, the new name is either gone or reads the
// same file, and fsck finds at most an extra link.
func TestPowerCutDuringLink(t *testing.T) {
	data := pattern(4, BlockSize+10)
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			f = createFile(t, fsys, "f")
			writeAt(t, f, data, 0)
		},
		op: func(fsys *FS, _ *power) {
			_, _ = fsys.addLink(context.Background(), inode.InodeRoot, f, "g")
		},
		check: func(t *testing.T, fsys *FS) {
			checkFile(t, lookupFile(t, fsys, "f"), data)
			root, err := fsys.inodeTbl.Read(inode.InodeRoot)
			if err != nil {
				t.Fatal(err)
			}
			if num, _, err := fsys.dirMgr.Lookup(context.Background(), root, "g"); err == nil {
				if uint32(num) != f.inodeNum {
					t.Fatalf("g names inode %d, want %d", num, f.inodeNum)
				}
				checkFile(t, lookupFile(t, fsys, "g"), data)
			}
		},
	})
}

// Power fails while one name of a linked file is removed: the other name keeps the file and its data.
func TestPowerCutDuringUnlinkOfLinkedFile(t *testing.T) {
	data := pattern(5, 2*BlockSize)
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			f = createFile(t, fsys, "f")
			writeAt(t, f, data, 0)
			mustLink(t, fsys, inode.InodeRoot, f, "g")
		},
		op: func(fsys *FS, _ *power) {
			_ = fsys.RootDir().Unlink(context.Background(), "f")
		},
		check: func(t *testing.T, fsys *FS) {
			checkFile(t, lookupFile(t, fsys, "g"), data)
		},
	})
}
