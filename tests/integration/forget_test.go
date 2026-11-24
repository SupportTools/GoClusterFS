package integration

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// kernelForget returns a func that makes the kernel drop the dentries and inodes no process holds, which sends the
// mounts a FORGET for each. It needs root for drop_caches, or memory.reclaim on a cgroup this user owns, such as a
// systemd user scope (systemd-run --user --scope); then the mounts start in scopes of their own, so the reclaim
// covers only this process's lookups. Without either the test skips.
func kernelForget(t *testing.T) func() {
	t.Helper()
	dropCaches := func(cmd ...string) func() {
		return func() {
			t.Helper()
			if out, err := exec.Command(cmd[0], cmd[1:]...).CombinedOutput(); err != nil {
				t.Fatalf("drop_caches: %v: %s", err, out)
			}
		}
	}
	switch {
	case os.Geteuid() == 0:
		return dropCaches("sh", "-c", "echo 2 > /proc/sys/vm/drop_caches")
	case exec.Command("sudo", "-n", "true").Run() == nil:
		return dropCaches("sudo", "-n", "sh", "-c", "echo 2 > /proc/sys/vm/drop_caches")
	}
	data, err := os.ReadFile("/proc/self/cgroup")
	if err == nil {
		for line := range strings.Lines(string(data)) {
			path, ok := strings.CutPrefix(strings.TrimSpace(line), "0::")
			reclaim := filepath.Join("/sys/fs/cgroup", path, "memory.reclaim")
			if !ok || unix.Access(reclaim, unix.W_OK) != nil {
				continue
			}
			mountsInOwnScope = true
			stat := filepath.Join(filepath.Dir(reclaim), "memory.stat")
			return func() {
				t.Helper()
				for idle := 0; idle < 3; {
					before := slabBytes(t, stat)
					if err := reclaimFrom(reclaim, "256M"); err != nil {
						t.Errorf("memory.reclaim: %v", err)
						return
					}
					after := slabBytes(t, stat)
					if after < 1<<20 {
						return
					}
					if after+1<<20 > before {
						idle++
					}
				}
			}
		}
	}
	t.Skip("making the kernel forget needs root for drop_caches, or a user-owned cgroup with memory.reclaim: run under systemd-run --user --scope")
	return nil
}

// reclaimFrom writes amount to a cgroup's memory.reclaim. It writes through the raw file descriptor: os.File would put
// the pollable cgroup file in the poller and wait forever on the EAGAIN that memory.reclaim returns when it reclaims
// less than asked.
func reclaimFrom(reclaim, amount string) error {
	fd, err := unix.Open(reclaim, unix.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fd) }()
	for {
		_, err := unix.Write(fd, []byte(amount))
		switch {
		case errors.Is(err, syscall.EINTR):
		case err == nil, errors.Is(err, syscall.EAGAIN):
			return nil
		default:
			return err
		}
	}
}

// slabBytes reads a cgroup's reclaimable slab, where the kernel's dentries and inodes are, from its memory.stat.
func slabBytes(t *testing.T, stat string) uint64 {
	t.Helper()
	data, err := os.ReadFile(stat)
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(string(data)) {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "slab_reclaimable "); ok {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatalf("no slab_reclaimable in %s", stat)
	return 0
}

// heapBytes reads the mount's live heap from /metrics; the mount runs a collection every GOCLUSTERFS_TEST_GC_EVERY.
func heapBytes(t *testing.T, addr string) uint64 {
	t.Helper()
	time.Sleep(time.Second)
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer mustClose(t, resp.Body)
	lines := bufio.NewScanner(resp.Body)
	for lines.Scan() {
		if v, ok := strings.CutPrefix(lines.Text(), "go_memstats_heap_alloc_bytes "); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatal(err)
			}
			return uint64(f)
		}
	}
	t.Fatal("no go_memstats_heap_alloc_bytes in /metrics")
	return 0
}

// makeFiles creates dirs directories of perDir empty files under root, with 16 writers.
func makeFiles(t *testing.T, root string, dirs, perDir int) {
	t.Helper()
	var wg sync.WaitGroup
	next := make(chan int)
	errs := make(chan error, dirs)
	for range 16 {
		wg.Go(func() {
			for d := range next {
				dir := filepath.Join(root, fmt.Sprintf("d%04d", d))
				if err := os.Mkdir(dir, 0o755); err != nil {
					errs <- err
					continue
				}
				for i := range perDir {
					if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%04d", i)), nil, 0o644); err != nil {
						errs <- err
						break
					}
				}
			}
		})
	}
	for d := range dirs {
		next <- d
	}
	close(next)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func statAll(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(root, func(p string, _ os.DirEntry, err error) error {
		if err == nil {
			_, err = os.Lstat(p)
			n++
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestKernelForgetFreesNodes stats every file of a tree on a fresh mount, then has the kernel forget them. The mount
// frees its node for each name, so its heap falls back to within half of what the walk added: the directory index and
// inode cache keep a bounded share. GOCLUSTERFS_TEST_FORGET_NAMES sets how many names (20,000 by default).
func TestKernelForgetFreesNodes(t *testing.T) {
	forget := kernelForget(t)
	names := 20000
	if n, err := strconv.Atoi(os.Getenv("GOCLUSTERFS_TEST_FORGET_NAMES")); err == nil && n > 0 {
		names = n
	}
	perDir := 1000
	dirs := (names + perDir - 1) / perDir
	m := NewMountedFS(t, 64+names/40)
	addr := freeLoopbackAddr(t)
	m.Env = append(m.Env, "GOCLUSTERFS_TEST_GC_EVERY=200ms")
	m.ExtraArgs = append(m.ExtraArgs, "-api-addr", addr, "-bytes-per-inode", "16384")
	if err := m.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount: %v", err)
	}
	makeFiles(t, m.MountPoint, dirs, perDir)
	if err := m.Unmount(); err != nil {
		t.Fatal(err)
	}
	if err := m.Mount(false); err != nil {
		t.Fatal(err)
	}

	before := heapBytes(t, addr)
	walked := statAll(t, m.MountPoint)
	walkedHeap := heapBytes(t, addr)
	grew := int64(walkedHeap) - int64(before)
	if grew < int64(walked)*150 {
		t.Fatalf("stat of %d names grew the heap by %d bytes, from %d; want the mount's nodes, about 400 bytes each", walked, grew, before)
	}
	forget()
	after := walkedHeap
	for range 60 {
		now := heapBytes(t, addr)
		if now >= after && int64(after)-int64(before) < grew/2 {
			break
		}
		after = min(after, now)
	}
	if int64(after)-int64(before) >= grew/2 {
		t.Errorf("after the kernel forgot %d names the heap is %d bytes, %d over the %d before the walk; the walk added %d, and the nodes should go with the names",
			walked, after, int64(after)-int64(before), before, grew)
	}
	t.Logf("%d names: heap %d before the walk, %d after it, %d once the kernel forgot them", walked, before, walkedHeap, after)
}

// TestForgottenNodesKeepHandlesCwdAndMoves has the kernel forget everything it does not hold, on both nodes, around
// work that needs held nodes to stay: an open file, an open directory and a working directory in it, a move by the
// other node and a rename of the moved directory, and a file unlinked while open whose inode slot a new file takes.
func TestForgottenNodesKeepHandlesCwdAndMoves(t *testing.T) {
	forget := kernelForget(t)
	cluster := startRenamePair(t)
	n0, n1 := cluster.Node(0), cluster.Node(1)
	mkdirs(t, n0, "a", "b", "c", "a/d")
	if err := n0.WriteFile("a/d/kept", []byte("kept data")); err != nil {
		t.Fatal(err)
	}
	if err := n0.WriteFile("a/d/plain", []byte("plain")); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(n0.Path("a/d/kept"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	dirHandle, err := os.Open(n0.Path("a/d"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dirHandle.Close() })
	fd := int(dirHandle.Fd())

	forget()
	untilFound(t, "node 1 moves a/d", func() error { return os.Rename(n1.Path("a/d"), n1.Path("b/d")) })
	forget()
	untilFound(t, "node 0 moves b/d", func() error { return os.Rename(n0.Path("b/d"), n0.Path("c/d")) })
	forget()

	if got, err := os.ReadFile(n0.Path("c/d/plain")); err != nil || string(got) != "plain" {
		t.Errorf("c/d/plain after the forgets: %q, %v", got, err)
	}
	if err := os.Remove(n0.Path("c/d/kept")); err != nil {
		t.Fatal(err)
	}
	if err := n0.WriteFile("c/d/new", []byte("new data")); err != nil {
		t.Fatal(err)
	}
	forget()
	buf := make([]byte, 64)
	if n, err := f.ReadAt(buf, 0); string(buf[:n]) != "kept data" || err != nil && !errors.Is(err, io.EOF) {
		t.Errorf("the open file, unlinked and forgotten around, reads %q, %v; want its own data", buf[:n], err)
	}
	if got, err := os.ReadFile(n0.Path("c/d/new")); err != nil || string(got) != "new data" {
		t.Errorf("the new file reads %q, %v", got, err)
	}
	names, err := dirHandle.Readdirnames(-1)
	if err != nil || !slices.Contains(names, "plain") || !slices.Contains(names, "new") || slices.Contains(names, "kept") {
		t.Errorf("readdir of the open directory: %v, %v; want plain and new", names, err)
	}
	if err := inDir(fd, func() error {
		forget()
		if got, err := os.ReadFile("plain"); err != nil || string(got) != "plain" {
			return fmt.Errorf("plain through the working directory: %q, %v", got, err)
		}
		return os.WriteFile("via-cwd", []byte("cwd"), 0o644)
	}); err != nil {
		t.Fatal(err)
	}

	settle(t, cluster, "the files", func(n *MountedFS) error {
		for _, name := range []string{"plain", "new", "via-cwd"} {
			if _, err := os.Stat(n.Path("c/d/" + name)); err != nil {
				return err
			}
		}
		if _, err := os.Lstat(n.Path("a/d")); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("a/d: %v, want it gone", err)
		}
		return nil
	})
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := dirHandle.Close(); err != nil {
		t.Fatal(err)
	}
	expectCleanFsckAfterStop(t, cluster)
}

// TestRenamesUnderForgetPressure moves directories back and forth between three parents from both nodes while the
// kernel forgets everything it does not hold every 100 ms. Every move must succeed once the node sees the last one,
// and the files inside must follow. GOCLUSTERFS_TEST_SOAK sets how long it runs (10 s by default).
func TestRenamesUnderForgetPressure(t *testing.T) {
	forget := kernelForget(t)
	length := 10 * time.Second
	if d, err := time.ParseDuration(os.Getenv("GOCLUSTERFS_TEST_SOAK")); err == nil && d > 0 {
		length = d
	}
	cluster := startRenamePair(t)
	nodes := []*MountedFS{cluster.Node(0), cluster.Node(1)}
	parents := []string{"p0", "p1", "p2"}
	mkdirs(t, nodes[0], parents...)
	const movers = 4
	for i := range movers {
		mkdirs(t, nodes[0], fmt.Sprintf("p0/m%d", i), fmt.Sprintf("p0/m%d/sub", i))
		if err := nodes[0].WriteFile(fmt.Sprintf("p0/m%d/sub/data", i), []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var pressure sync.WaitGroup
	pressure.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
				forget()
			}
		}
	})
	moves := 0
	where := make([]int, movers)
	for deadline := time.Now().Add(length); time.Now().Before(deadline); moves++ {
		i := moves % movers
		n := nodes[moves%2]
		from, to := where[i], (where[i]+1+moves%2)%len(parents)
		src, dst := fmt.Sprintf("%s/m%d", parents[from], i), fmt.Sprintf("%s/m%d", parents[to], i)
		untilFound(t, fmt.Sprintf("move %d: node %d renames %s to %s", moves, moves%2, src, dst), func() error {
			return os.Rename(n.Path(src), n.Path(dst))
		})
		where[i] = to
		if got, err := os.ReadFile(n.Path(dst + "/sub/data")); err != nil || string(got) != strconv.Itoa(i) {
			t.Fatalf("move %d: %s/sub/data on node %d reads %q, %v", moves, dst, moves%2, got, err)
		}
	}
	close(stop)
	pressure.Wait()
	t.Logf("%d moves under forget pressure in %v", moves, length)
	settle(t, cluster, "the movers", func(n *MountedFS) error {
		for i, p := range where {
			if got, err := os.ReadFile(n.Path(fmt.Sprintf("%s/m%d/sub/data", parents[p], i))); err != nil || string(got) != strconv.Itoa(i) {
				return fmt.Errorf("m%d in %s: %q, %v", i, parents[p], got, err)
			}
		}
		return nil
	})
	expectCleanFsckAfterStop(t, cluster)
}
