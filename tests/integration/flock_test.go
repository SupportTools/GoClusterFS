package integration

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func openForFlock(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func requireFlock(t *testing.T, f *os.File, how int, want error, what string) {
	t.Helper()
	if err := syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB); !errors.Is(err, want) {
		t.Fatalf("%s: flock = %v, want %v", what, err, want)
	}
}

// procLocks returns the /proc/locks lines of the inode ino.
func procLocks(t *testing.T, ino uint64) []string {
	t.Helper()
	f, err := os.Open("/proc/locks")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var lines []string
	suffix := fmt.Sprintf(":%d", ino)
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		for _, field := range fields {
			if strings.HasSuffix(field, suffix) && strings.Count(field, ":") == 2 {
				lines = append(lines, scan.Text())
			}
		}
	}
	return lines
}

func TestFlockExcludesOpenFilesOnOneMount(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}
	if err := mfs.WriteFile("lockfile", []byte("x")); err != nil {
		t.Fatal(err)
	}
	path := mfs.Path("lockfile")
	a, b := openForFlock(t, path), openForFlock(t, path)

	requireFlock(t, a, syscall.LOCK_EX, nil, "first exclusive")
	requireFlock(t, b, syscall.LOCK_EX, syscall.EWOULDBLOCK, "second exclusive")
	requireFlock(t, b, syscall.LOCK_SH, syscall.EWOULDBLOCK, "shared next to exclusive")

	dup, err := syscall.Dup(int(a.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	requireFlock(t, b, syscall.LOCK_EX, syscall.EWOULDBLOCK, "exclusive while a dup of the holder stays open")

	waited := make(chan error, 1)
	go func() { waited <- syscall.Flock(int(b.Fd()), syscall.LOCK_EX) }()
	select {
	case err := <-waited:
		t.Fatalf("blocking flock returned %v while the holder's dup is open", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := syscall.Close(dup); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("blocking flock after the holder's last close = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocking flock still waits after the holder's last close")
	}

	c := openForFlock(t, path)
	requireFlock(t, c, syscall.LOCK_SH, syscall.EWOULDBLOCK, "shared next to the waiter that got exclusive")
	requireFlock(t, b, syscall.LOCK_SH, nil, "holder converts to shared")
	requireFlock(t, c, syscall.LOCK_SH, nil, "shared next to shared")
	requireFlock(t, b, syscall.LOCK_UN, nil, "unlock")
	requireFlock(t, c, syscall.LOCK_UN, nil, "unlock")
	requireFlock(t, c, syscall.LOCK_EX, nil, "exclusive after both unlocked")
}

func TestFlockOnADirectory(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}
	if err := mfs.Mkdir("dir"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"dir", ""} {
		a, b := openForFlock(t, mfs.Path(name)), openForFlock(t, mfs.Path(name))
		requireFlock(t, a, syscall.LOCK_EX, nil, "exclusive on directory "+name)
		requireFlock(t, b, syscall.LOCK_EX, syscall.EWOULDBLOCK, "second exclusive on directory "+name)
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		requireFlock(t, b, syscall.LOCK_EX, nil, "exclusive after the holder closed directory "+name)
	}
}

// The kernel keeps fcntl locks, so /proc/locks lists them, and sends flocks to the mount, so it does not list those.
func TestFcntlLocksStayInTheKernel(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}
	if err := mfs.WriteFile("db", make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(mfs.Path("db"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil {
		t.Fatal(err)
	}

	lk := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: 0, Len: 100}
	if err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lk); err != nil {
		t.Fatalf("fcntl F_SETLK = %v", err)
	}
	lines := procLocks(t, st.Ino)
	pid := fmt.Sprint(os.Getpid())
	found := false
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 5 && fields[1] == "POSIX" && fields[3] == "WRITE" && fields[4] == pid {
			found = true
		}
	}
	if !found {
		t.Fatalf("/proc/locks has no POSIX write lock of pid %s on inode %d; its lines for the inode: %q", pid, st.Ino, lines)
	}

	g := openForFlock(t, mfs.Path("db"))
	requireFlock(t, g, syscall.LOCK_EX, nil, "flock next to an fcntl lock")
	for _, line := range procLocks(t, st.Ino) {
		if strings.Contains(line, "FLOCK") {
			t.Fatalf("/proc/locks lists a flock on inode %d, so the kernel kept it: %q", st.Ino, line)
		}
	}
}

// A process waiting for a flock ends at once when it is killed.
func TestKilledFlockWaiterExits(t *testing.T) {
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("flock(1) is not installed")
	}
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}
	if err := mfs.WriteFile("lockfile", []byte("x")); err != nil {
		t.Fatal(err)
	}
	holder := openForFlock(t, mfs.Path("lockfile"))
	requireFlock(t, holder, syscall.LOCK_EX, nil, "holder")

	waiter := exec.Command("flock", "-x", mfs.Path("lockfile"), "true")
	if err := waiter.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- waiter.Wait() }()
	select {
	case err := <-exited:
		t.Fatalf("flock(1) exited with %v while another open file holds the lock", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := waiter.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err == nil {
			t.Fatal("flock(1) exited cleanly after SIGKILL")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a killed flock waiter has not exited after 5s")
	}
	requireFlock(t, openForFlock(t, mfs.Path("lockfile")), syscall.LOCK_SH, syscall.EWOULDBLOCK, "the holder still holds after the waiter died")
}

func startFlockCluster(t *testing.T) (*MountedFS, *MountedFS) {
	t.Helper()
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
	return cluster.Node(0), cluster.Node(1)
}

// flockSoon retries a non-blocking flock for d, as a release reaches the mount only after close returns.
func flockSoon(t *testing.T, f *os.File, how int, d time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		err := syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB)
		if err == nil {
			return
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) || time.Now().After(deadline) {
			t.Fatalf("%s: flock = %v after %v", what, err, d)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestFlockExcludesAcrossNodes(t *testing.T) {
	a, b := startFlockCluster(t)
	if err := a.WriteFile("lockfile", []byte("x")); err != nil {
		t.Fatal(err)
	}
	onA, onB := openForFlock(t, a.Path("lockfile")), openForFlock(t, b.Path("lockfile"))

	requireFlock(t, onA, syscall.LOCK_EX, nil, "exclusive on node 0")
	requireFlock(t, onB, syscall.LOCK_EX, syscall.EWOULDBLOCK, "exclusive on node 1")
	requireFlock(t, onB, syscall.LOCK_SH, syscall.EWOULDBLOCK, "shared on node 1")
	if err := onA.Close(); err != nil {
		t.Fatal(err)
	}
	flockSoon(t, onB, syscall.LOCK_SH, 2*time.Second, "shared on node 1 after node 0 closed")
	onA = openForFlock(t, a.Path("lockfile"))
	requireFlock(t, onA, syscall.LOCK_SH, nil, "shared on both nodes")
	requireFlock(t, onA, syscall.LOCK_EX, syscall.EWOULDBLOCK, "node 0 converting to exclusive next to node 1's shared")
}

func TestFlockWaiterOnAnotherNodeWakesAtRelease(t *testing.T) {
	a, b := startFlockCluster(t)
	if err := a.WriteFile("lockfile", []byte("x")); err != nil {
		t.Fatal(err)
	}
	onA, onB := openForFlock(t, a.Path("lockfile")), openForFlock(t, b.Path("lockfile"))
	requireFlock(t, onA, syscall.LOCK_EX, nil, "exclusive on node 0")

	waited := make(chan error, 1)
	go func() { waited <- syscall.Flock(int(onB.Fd()), syscall.LOCK_EX) }()
	select {
	case err := <-waited:
		t.Fatalf("node 1's blocking flock returned %v while node 0 holds it", err)
	case <-time.After(500 * time.Millisecond):
	}
	released := time.Now()
	requireFlock(t, onA, syscall.LOCK_UN, nil, "unlock on node 0")
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("node 1's blocking flock = %v", err)
		}
		t.Logf("node 1 got the flock %v after node 0 released it", time.Since(released).Round(time.Millisecond))
	case <-time.After(5 * time.Second):
		t.Fatal("node 1 still waits 5s after node 0 released the flock")
	}
	requireFlock(t, onA, syscall.LOCK_SH, syscall.EWOULDBLOCK, "shared on node 0 after node 1 got exclusive")
}

func TestKilledFlockWaiterOnAnotherNodeExits(t *testing.T) {
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("flock(1) is not installed")
	}
	a, b := startFlockCluster(t)
	if err := a.WriteFile("lockfile", []byte("x")); err != nil {
		t.Fatal(err)
	}
	requireFlock(t, openForFlock(t, a.Path("lockfile")), syscall.LOCK_EX, nil, "exclusive on node 0")

	waiter := exec.Command("flock", "-x", b.Path("lockfile"), "true")
	if err := waiter.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- waiter.Wait() }()
	select {
	case err := <-exited:
		t.Fatalf("flock(1) on node 1 exited with %v while node 0 holds the lock", err)
	case <-time.After(500 * time.Millisecond):
	}
	if err := waiter.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("a killed flock waiter on node 1 has not exited after 5s")
	}
	onB := openForFlock(t, b.Path("lockfile"))
	requireFlock(t, onB, syscall.LOCK_SH, syscall.EWOULDBLOCK, "node 0 still holds after node 1's waiter died")
}

// A crashed node's flock moves to a waiter on a live node once the live node takes the crashed one over.
func TestCrashedNodesFlockMovesToAWaiter(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a takeover, about 30s")
	}
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
	a, b := cluster.Node(0), cluster.Node(1)
	if err := a.WriteFile("lockfile", []byte("x")); err != nil {
		t.Fatal(err)
	}
	onA, err := os.Open(a.Path("lockfile"))
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(onA.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	onB := openForFlock(t, b.Path("lockfile"))
	waited := make(chan error, 1)
	go func() { waited <- syscall.Flock(int(onB.Fd()), syscall.LOCK_EX) }()
	select {
	case err := <-waited:
		t.Fatalf("node 1's blocking flock returned %v while node 0 holds it", err)
	case <-time.After(500 * time.Millisecond):
	}

	killed := time.Now()
	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("kill node 0: %v", err)
	}
	_ = onA.Close()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("node 1's blocking flock = %v", err)
		}
		t.Logf("node 1 got the flock %v after node 0 died", time.Since(killed).Round(time.Second))
	case <-time.After(90 * time.Second):
		t.Fatal("node 1 still waits 90s after node 0 died holding the flock")
	}
}
