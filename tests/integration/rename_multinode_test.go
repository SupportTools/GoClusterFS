package integration

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestMultiNodeRenamesOntoOneName has two nodes rename their own file onto one new name at once. Without
// RENAME_NOREPLACE both succeed and the name ends with one of the two files; with it exactly one succeeds.
func TestMultiNodeRenamesOntoOneName(t *testing.T) {
	cluster := startRenamePair(t)
	seen := outcomes{}
	defer seen.log(t)
	for _, noReplace := range []bool{false, true} {
		for r := 0; r < renameRounds(); r++ {
			src := [2]string{fmt.Sprintf("a-%v-%d", noReplace, r), fmt.Sprintf("b-%v-%d", noReplace, r)}
			dst := fmt.Sprintf("t-%v-%d", noReplace, r)
			for i, name := range src {
				if err := cluster.Node(i).WriteFile(name, []byte(name)); err != nil {
					t.Fatal(err)
				}
			}
			move := func(i int) func(*MountedFS) error {
				return func(n *MountedFS) error { return rename(n, src[i], dst, noReplace) }
			}
			e0, e1 := race(cluster, move(0), move(1))

			switch {
			case !noReplace && e0 == nil && e1 == nil:
				seen.add(t, "both renamed", e0, e1)
				var got []byte
				settle(t, cluster, "both renamed", func(n *MountedFS) error {
					if err := errors.Join(absent(n, src[0]), absent(n, src[1])); err != nil {
						return err
					}
					data, err := n.ReadFile(dst)
					if err == nil && got == nil && (string(data) == src[0] || string(data) == src[1]) {
						got = data
					}
					return errors.Join(err, holds(n, dst, got))
				})
			case noReplace && (e0 == nil) != (e1 == nil) && errors.Is(errors.Join(e0, e1), unix.EEXIST):
				won := map[bool]int{true: 0, false: 1}[e0 == nil]
				seen.add(t, fmt.Sprintf("node %d won the NOREPLACE rename", won), e0, e1)
				settle(t, cluster, "one renamed", func(n *MountedFS) error {
					return errors.Join(absent(n, src[won]), holds(n, dst, []byte(src[won])), holds(n, src[1-won], []byte(src[1-won])))
				})
			default:
				t.Fatalf("NOREPLACE %v, round %d: the renames returned %v and %v", noReplace, r, e0, e1)
			}
		}
	}
	expectCleanFsckAfterStop(t, cluster)
}

// TestMultiNodeRenameRacesUnlinkOfItsSource has one node rename a file while another unlinks it. Exactly one
// succeeds; the other fails with ENOENT.
func TestMultiNodeRenameRacesUnlinkOfItsSource(t *testing.T) {
	cluster := startRenamePair(t)
	seen := outcomes{}
	defer seen.log(t)
	for r := 0; r < renameRounds(); r++ {
		src, dst := fmt.Sprintf("s-%d", r), fmt.Sprintf("d-%d", r)
		data := []byte(src)
		if err := cluster.Node(0).WriteFile(src, data); err != nil {
			t.Fatal(err)
		}
		settle(t, cluster, "source created", func(n *MountedFS) error { return holds(n, src, data) })
		renameErr, unlinkErr := race(cluster,
			func(n *MountedFS) error { return rename(n, src, dst, false) },
			func(n *MountedFS) error { return n.Remove(src) })

		switch {
		case renameErr == nil && errors.Is(unlinkErr, fs.ErrNotExist):
			seen.add(t, "rename won", renameErr, unlinkErr)
			settle(t, cluster, "renamed", func(n *MountedFS) error { return errors.Join(absent(n, src), holds(n, dst, data)) })
		case unlinkErr == nil && errors.Is(renameErr, fs.ErrNotExist):
			seen.add(t, "unlink won", renameErr, unlinkErr)
			settle(t, cluster, "unlinked", func(n *MountedFS) error { return errors.Join(absent(n, src), absent(n, dst)) })
		default:
			t.Fatalf("round %d: rename returned %v and unlink %v; want one to succeed and the other ENOENT", r, renameErr, unlinkErr)
		}
	}
	expectCleanFsckAfterStop(t, cluster)
}

// TestMultiNodeRenameRacesCreateOfItsTarget has one node rename a file onto a name another node creates with
// O_EXCL. A plain rename always ends as the target, replacing the new file if the create came first. With
// RENAME_NOREPLACE exactly one of the two succeeds.
func TestMultiNodeRenameRacesCreateOfItsTarget(t *testing.T) {
	cluster := startRenamePair(t)
	seen := outcomes{}
	defer seen.log(t)
	for _, noReplace := range []bool{false, true} {
		for r := 0; r < renameRounds(); r++ {
			src, dst := fmt.Sprintf("s-%v-%d", noReplace, r), fmt.Sprintf("t-%v-%d", noReplace, r)
			moved, created := []byte("moved "+src), []byte("created "+dst)
			if err := cluster.Node(0).WriteFile(src, moved); err != nil {
				t.Fatal(err)
			}
			renameErr, createErr := race(cluster,
				func(n *MountedFS) error {
					// A create takes longer than a rename; without this the rename always wins.
					time.Sleep(rand.N(15 * time.Millisecond))
					return rename(n, src, dst, noReplace)
				},
				func(n *MountedFS) error { return createExclusive(n, dst, created) })

			switch {
			case renameErr == nil && (createErr == nil && !noReplace || errors.Is(createErr, fs.ErrExist)):
				seen.add(t, fmt.Sprintf("NOREPLACE %v: renamed", noReplace), renameErr, createErr)
				settle(t, cluster, "renamed", func(n *MountedFS) error { return errors.Join(absent(n, src), holds(n, dst, moved)) })
			case noReplace && createErr == nil && errors.Is(renameErr, unix.EEXIST):
				seen.add(t, "NOREPLACE true: created", renameErr, createErr)
				settle(t, cluster, "created", func(n *MountedFS) error { return errors.Join(holds(n, src, moved), holds(n, dst, created)) })
			default:
				t.Fatalf("NOREPLACE %v, round %d: rename returned %v and create %v", noReplace, r, renameErr, createErr)
			}
		}
	}
	expectCleanFsckAfterStop(t, cluster)
}

// TestMultiNodeCrossMovesCannotFormALoop has one node move directory x into y while another moves y into x.
// One move succeeds. The other fails with EINVAL, or with ENOENT once its node sees x gone from the root.
func TestMultiNodeCrossMovesCannotFormALoop(t *testing.T) {
	cluster := startRenamePair(t)
	seen := outcomes{}
	defer seen.log(t)
	for r := 0; r < renameRounds(); r++ {
		x, y := fmt.Sprintf("x-%d", r), fmt.Sprintf("y-%d", r)
		for _, d := range []string{x, y} {
			if err := cluster.Node(0).Mkdir(d); err != nil {
				t.Fatal(err)
			}
		}
		settle(t, cluster, "directories created", func(n *MountedFS) error { return errors.Join(isDir(n, x), isDir(n, y)) })
		e0, e1 := race(cluster,
			func(n *MountedFS) error { return rename(n, x, y+"/x", false) },
			func(n *MountedFS) error { return rename(n, y, x+"/y", false) })

		refused := func(err error) bool { return errors.Is(err, syscall.EINVAL) || errors.Is(err, fs.ErrNotExist) }
		switch {
		case e0 == nil && refused(e1):
			seen.add(t, "node 0 moved", e0, e1)
			settle(t, cluster, "x moved into y", func(n *MountedFS) error { return errors.Join(absent(n, x), isDir(n, y+"/x")) })
		case e1 == nil && refused(e0):
			seen.add(t, "node 1 moved", e0, e1)
			settle(t, cluster, "y moved into x", func(n *MountedFS) error { return errors.Join(absent(n, y), isDir(n, x+"/y")) })
		default:
			t.Fatalf("round %d: the moves returned %v and %v; want one to succeed and the other EINVAL or ENOENT", r, e0, e1)
		}
	}
	expectCleanFsckAfterStop(t, cluster)
}

// outcomes counts how each race ended, so the log shows which orders the rounds hit.
type outcomes map[string]int

func (o outcomes) add(t *testing.T, what string, errs ...error) {
	t.Helper()
	for _, err := range errs {
		if errno := syscall.Errno(0); errors.As(err, &errno) {
			what += ", " + errno.Error()
		}
	}
	o[what]++
}

func (o outcomes) log(t *testing.T) {
	t.Helper()
	keys := make([]string, 0, len(o))
	for k := range o {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("%3d × %s", o[k], k)
	}
}

func renameRounds() int {
	if testing.Short() {
		return 4
	}
	return 20
}

func startRenamePair(t *testing.T) *MultiNodeCluster {
	t.Helper()
	cluster := NewMultiNodeCluster(t, 2, 64)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node 0: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("start node 1: %v", err)
	}
	return cluster
}

// race runs a on node 0 and b on node 1, released together, each after a random delay so either can go first.
func race(cluster *MultiNodeCluster, a, b func(*MountedFS) error) (error, error) {
	barrier := NewSyncBarrier(2)
	errs := cluster.RunConcurrent([]int{0, 1}, func(idx int, n *MountedFS) error {
		barrier.Wait()
		time.Sleep(rand.N(3 * time.Millisecond))
		return [2]func(*MountedFS) error{a, b}[idx](n)
	})
	return errs[0], errs[1]
}

// settle waits until check passes on every node; a node's kernel may show the old names for -attr-timeout.
func settle(t *testing.T, cluster *MultiNodeCluster, what string, check func(*MountedFS) error) {
	t.Helper()
	for idx := 0; idx < cluster.NodeCount(); idx++ {
		var err error
		if _, ok := waitFor(propagationTimeout, func() bool { err = check(cluster.Node(idx)); return err == nil }); !ok {
			t.Fatalf("%s: node %d: %v", what, idx, err)
		}
	}
}

func expectCleanFsckAfterStop(t *testing.T, cluster *MultiNodeCluster) {
	t.Helper()
	cluster.StopAllNodes()
	if code, out := runFsck(t, cluster.ImagePath); code != 0 {
		t.Errorf("fsck: exit %d:\n%s", code, strings.Join(problemLines(out), "\n"))
	}
}

func rename(n *MountedFS, from, to string, noReplace bool) error {
	if noReplace {
		return unix.Renameat2(unix.AT_FDCWD, n.Path(from), unix.AT_FDCWD, n.Path(to), unix.RENAME_NOREPLACE)
	}
	return os.Rename(n.Path(from), n.Path(to))
}

func createExclusive(n *MountedFS, name string, data []byte) error {
	f, err := os.OpenFile(n.Path(name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return errors.Join(err, f.Close())
}

func holds(n *MountedFS, name string, want []byte) error {
	got, err := n.ReadFile(name)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("%s holds %q, want %q", name, got, want)
	}
	return nil
}

func absent(n *MountedFS, name string) error {
	_, err := os.Lstat(n.Path(name))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err == nil:
		return fmt.Errorf("%s exists", name)
	}
	return err
}

func isDir(n *MountedFS, name string) error {
	fi, err := os.Lstat(n.Path(name))
	if err == nil && !fi.IsDir() {
		err = fmt.Errorf("%s is not a directory", name)
	}
	return err
}
