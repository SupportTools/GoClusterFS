package integration

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// TestRenameOfDirAnotherNodeMoved has node 0 look a directory up, node 1 move it to another directory, and node 0
// move it again by its new name. Node 0's kernel still holds the old name, and it used to fail that rename with
// ESTALE when it could not move the old dentry inside a cross-directory rename.
func TestRenameOfDirAnotherNodeMoved(t *testing.T) {
	cluster := startRenamePair(t)
	n0, n1 := cluster.Node(0), cluster.Node(1)
	mkdirs(t, n0, "a", "b", "c")

	cases := []struct{ name, dir, dst string }{
		{"back into the old parent", "back", "a"},
		{"into a third directory", "third", "c"},
		{"within the new parent", "within", "b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			old, moved, final := "a/"+tc.dir, "b/"+tc.dir+"-moved", tc.dst+"/"+tc.dir+"-final"
			mkdirs(t, n0, old)
			if _, err := os.Stat(n0.Path(old)); err != nil {
				t.Fatal(err)
			}
			untilFound(t, "node 1 moves "+old, func() error { return os.Rename(n1.Path(old), n1.Path(moved)) })
			untilFound(t, "node 0 moves "+moved, func() error { return os.Rename(n0.Path(moved), n0.Path(final)) })
			settle(t, cluster, "the moves", isDirAndGone(final, old, moved))
		})
	}

	t.Run("a subdirectory of the moved directory", func(t *testing.T) {
		mkdirs(t, n0, "a/outer", "a/outer/inner")
		if _, err := os.Stat(n0.Path("a/outer/inner")); err != nil {
			t.Fatal(err)
		}
		untilFound(t, "node 1 moves a/outer", func() error { return os.Rename(n1.Path("a/outer"), n1.Path("b/outer")) })
		untilFound(t, "node 0 moves b/outer/inner", func() error {
			return os.Rename(n0.Path("b/outer/inner"), n0.Path("c/inner"))
		})
		settle(t, cluster, "the moves", isDirAndGone("c/inner", "a/outer", "b/outer/inner"))
	})

	expectCleanFsckAfterStop(t, cluster)
}

// TestMovedDirOldHandleAndCwdKeepWorking keeps a directory open and a thread's working directory in it on node 0 while
// the directory gets a new node there. The handle and the working directory stay on the old node and keep working.
func TestMovedDirOldHandleAndCwdKeepWorking(t *testing.T) {
	cluster := startRenamePair(t)
	n0, n1 := cluster.Node(0), cluster.Node(1)
	mkdirs(t, n0, "a", "b", "a/d")
	if err := n0.WriteFile("a/d/before", []byte("x")); err != nil {
		t.Fatal(err)
	}
	h, err := os.Open(n0.Path("a/d"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	fd := int(h.Fd())

	untilFound(t, "node 1 moves a/d", func() error { return os.Rename(n1.Path("a/d"), n1.Path("b/d")) })
	untilFound(t, "node 0 moves b/d", func() error { return os.Rename(n0.Path("b/d"), n0.Path("a/e")) })

	names, err := h.Readdirnames(-1)
	if err != nil || !slices.Contains(names, "before") {
		t.Fatalf("readdir of the old handle: %v, %v", names, err)
	}
	created, err := unix.Openat(fd, "via-handle", unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("create through the old handle: %v", err)
	}
	if err := unix.Close(created); err != nil {
		t.Fatal(err)
	}
	if err := inDir(fd, func() error {
		f, err := os.Open(".")
		if err != nil {
			return err
		}
		names, err := f.Readdirnames(-1)
		err = errors.Join(err, f.Close())
		if err != nil || !slices.Contains(names, "before") || !slices.Contains(names, "via-handle") {
			return fmt.Errorf("readdir of the working directory: %v, %v", names, err)
		}
		return os.WriteFile("via-cwd", []byte("y"), 0o644)
	}); err != nil {
		t.Fatal(err)
	}

	settle(t, cluster, "the files", func(n *MountedFS) error {
		for _, name := range []string{"before", "via-handle", "via-cwd"} {
			if _, err := os.Stat(n.Path("a/e/" + name)); err != nil {
				return err
			}
		}
		return nil
	})
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	expectCleanFsckAfterStop(t, cluster)
}

func mkdirs(t *testing.T, n *MountedFS, names ...string) {
	t.Helper()
	for _, name := range names {
		if err := os.Mkdir(n.Path(name), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

// untilFound retries op while its node does not yet see another node's change, which can take -cache-ttl.
func untilFound(t *testing.T, what string, op func() error) {
	t.Helper()
	var err error
	if _, ok := waitFor(propagationTimeout, func() bool {
		err = op()
		return !errors.Is(err, syscall.ENOENT)
	}); !ok || err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func isDirAndGone(dir string, gone ...string) func(*MountedFS) error {
	return func(n *MountedFS) error {
		if st, err := os.Stat(n.Path(dir)); err != nil || !st.IsDir() {
			return fmt.Errorf("%s: %v, want a directory", dir, err)
		}
		for _, p := range gone {
			if _, err := os.Lstat(n.Path(p)); !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%s: %v, want it gone", p, err)
			}
		}
		return nil
	}
}

// inDir runs f on a thread whose working directory is fd, as after fchdir(fd); the thread ends with f.
func inDir(fd int, f func() error) error {
	errc := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		if err := unix.Unshare(unix.CLONE_FS); err != nil {
			errc <- err
			return
		}
		if err := unix.Fchdir(fd); err != nil {
			errc <- fmt.Errorf("fchdir: %w", err)
			return
		}
		errc <- f()
	}()
	return <-errc
}
