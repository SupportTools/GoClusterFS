package integration

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func nlinkOf(t *testing.T, path string) (ino uint64, nlink uint64) {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return st.Ino, uint64(st.Nlink)
}

// Hard links through FUSE: every name reads and writes the one file, an unlink drops one name, a descriptor outlives
// every name, renames move one name, and a snapshot taken through one name shows through another.
func TestHardLinks(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount: %v", err)
	}
	if err := mfs.WriteFile("a", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := mfs.Mkdir("sub"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"b", "sub/c"} {
		if err := os.Link(mfs.Path("a"), mfs.Path(name)); err != nil {
			t.Fatalf("link a to %s: %v", name, err)
		}
	}
	ino, nlink := nlinkOf(t, mfs.Path("a"))
	if nlink != 3 {
		t.Errorf("a has %d links after two links, want 3", nlink)
	}
	if got, _ := nlinkOf(t, mfs.Path("sub/c")); got != ino {
		t.Errorf("sub/c is inode %d, want %d", got, ino)
	}
	if err := mfs.WriteFile("b", []byte("second")); err != nil {
		t.Fatal(err)
	}
	if data, err := mfs.ReadFile("sub/c"); err != nil || string(data) != "second" {
		t.Errorf("sub/c after a write through b: %q, %v", data, err)
	}

	if err := os.Link(mfs.Path("sub"), mfs.Path("dirlink")); !errors.Is(err, syscall.EPERM) {
		t.Errorf("link to a directory: %v, want EPERM", err)
	}
	if err := os.Link(mfs.Path("a"), mfs.Path("b")); !errors.Is(err, syscall.EEXIST) {
		t.Errorf("link over b: %v, want EEXIST", err)
	}

	if err := unix.Setxattr(mfs.Path("a"), "user.snapshot", []byte("through-a"), 0); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	if n, err := unix.Getxattr(mfs.Path("sub/c"), "user.snapshots", buf); err != nil || !strings.Contains(string(buf[:n]), "through-a") {
		t.Errorf("snapshots of sub/c: %q, %v; want the one taken through a", buf[:n], err)
	}

	if err := os.Remove(mfs.Path("a")); err != nil {
		t.Fatal(err)
	}
	if _, nlink := nlinkOf(t, mfs.Path("b")); nlink != 2 {
		t.Errorf("b has %d links after a was removed, want 2", nlink)
	}
	if err := os.Rename(mfs.Path("b"), mfs.Path("sub/c")); err != nil {
		t.Errorf("rename between two names of one file: %v", err)
	}
	for _, name := range []string{"b", "sub/c"} {
		if !mfs.Exists(name) {
			t.Errorf("%s is gone after a rename between two names of one file, which changes nothing", name)
		}
	}
	if err := os.Rename(mfs.Path("b"), mfs.Path("moved")); err != nil {
		t.Fatal(err)
	}
	if got, nlink := nlinkOf(t, mfs.Path("moved")); got != ino || nlink != 2 {
		t.Errorf("moved: inode %d with %d links, want inode %d with 2", got, nlink, ino)
	}

	f, err := os.Open(mfs.Path("moved"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	for _, name := range []string{"moved", "sub/c"} {
		if err := os.Remove(mfs.Path(name)); err != nil {
			t.Fatal(err)
		}
	}
	data := make([]byte, 16)
	if n, err := f.ReadAt(data, 0); string(data[:n]) != "second" {
		t.Errorf("read through a descriptor after every name went: %q, %v", data[:n], err)
	}
}

// A link made on one node is seen on the other, which can read the file through it and remove a name.
func TestMultiNodeHardLinks(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node 0: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("start node 1: %v", err)
	}
	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatal(err)
	}
	maker, other := cluster.Node(0), cluster.Node(1)
	if err := maker.WriteFile("x", []byte("shared")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(maker.Path("x"), maker.Path("y")); err != nil {
		t.Fatal(err)
	}
	if data, err := other.ReadFile("y"); err != nil || string(data) != "shared" {
		t.Errorf("node 1 reading y: %q, %v", data, err)
	}
	if _, nlink := nlinkOf(t, other.Path("y")); nlink != 2 {
		t.Errorf("node 1 sees %d links, want 2", nlink)
	}
	if err := os.Remove(other.Path("x")); err != nil {
		t.Fatal(err)
	}
	if _, ok := waitFor(10*time.Second, func() bool {
		_, nlink := nlinkOf(t, maker.Path("y"))
		return nlink == 1
	}); !ok {
		t.Error("node 0 never saw y drop to one link")
	}
	if data, err := maker.ReadFile("y"); err != nil || string(data) != "shared" {
		t.Errorf("node 0 reading y after node 1 removed x: %q, %v", data, err)
	}
}

// One node links a file while the other removes its only name, with the link starting 0 to 19 ms later. Either the
// link wins and the new name reads the file with one link, or the unlink wins and the link fails; the volume stays
// consistent.
func TestMultiNodeLinkRacesLastUnlink(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node 0: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("start node 1: %v", err)
	}
	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatal(err)
	}
	linker, remover := cluster.Node(0), cluster.Node(1)
	won := 0
	for i := range 20 {
		name, link := fmt.Sprintf("r%d", i), fmt.Sprintf("r%d-link", i)
		if err := linker.WriteFile(name, []byte(name)); err != nil {
			t.Fatal(err)
		}
		var linkErr, rmErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(i) * time.Millisecond)
			linkErr = os.Link(linker.Path(name), linker.Path(link))
		}()
		go func() { defer wg.Done(); rmErr = os.Remove(remover.Path(name)) }()
		wg.Wait()
		if rmErr != nil {
			t.Fatalf("remove %s: %v", name, rmErr)
		}
		if linkErr != nil {
			if !errors.Is(linkErr, syscall.ENOENT) {
				t.Errorf("link %s lost the race with %v, want ENOENT", name, linkErr)
			}
			continue
		}
		won++
		if data, err := remover.ReadFile(link); err != nil || string(data) != name {
			t.Errorf("%s after the race: %q, %v", link, data, err)
		}
		// The linker's kernel may keep the count it cached before the other node's unlink for -attr-timeout.
		if _, ok := waitFor(10*time.Second, func() bool {
			_, nlink := nlinkOf(t, linker.Path(link))
			return nlink == 1
		}); !ok {
			_, nlink := nlinkOf(t, linker.Path(link))
			t.Errorf("%s has %d links, want 1", link, nlink)
		}
	}
	t.Logf("the link won %d of 20 races", won)

	cluster.StopAllNodes()
	if code, out := runFsck(t, cluster.ImagePath); code != 0 {
		t.Errorf("fsck after the races exited %d:\n%s", code, strings.Join(problemLines(out), "\n"))
	}
}
