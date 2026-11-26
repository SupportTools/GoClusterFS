package goclusterfs

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/lock"
)

type fakeFlockCluster struct {
	mu     sync.Mutex
	calls  []string
	refuse bool
	// gate, when set, holds waiting Flock calls until it closes.
	gate chan struct{}
}

func (c *fakeFlockCluster) Flock(ctx context.Context, key lock.FlockKey, mode lock.LockMode, wait bool) error {
	c.mu.Lock()
	c.calls = append(c.calls, fmt.Sprintf("flock %d mode %d", key.Inode, mode))
	refuse, gate := c.refuse, c.gate
	c.mu.Unlock()
	if refuse {
		return lock.ErrWouldBlock
	}
	if gate != nil && wait {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (c *fakeFlockCluster) Unflock(key lock.FlockKey) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, fmt.Sprintf("unflock %d", key.Inode))
	return nil
}

func (c *fakeFlockCluster) takeCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	calls := c.calls
	c.calls = nil
	return calls
}

var flockFile1 = lock.FlockKey{Inode: 7, Birth: 1}

func requireFlockErrno(t *testing.T, got, want syscall.Errno, what string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func lockInBackground(tbl *flockTable, ctx context.Context, owner uint64, mode lock.LockMode) <-chan syscall.Errno {
	result := make(chan syscall.Errno, 1)
	go func() { result <- tbl.lock(ctx, flockFile1, owner, mode, true) }()
	return result
}

func requireStillWaiting(t *testing.T, result <-chan syscall.Errno) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("flock returned %v while it should wait", err)
	case <-time.After(100 * time.Millisecond):
	}
}

func requireDone(t *testing.T, result <-chan syscall.Errno, want syscall.Errno) {
	t.Helper()
	select {
	case err := <-result:
		requireFlockErrno(t, err, want, "waiting flock")
	case <-time.After(2 * time.Second):
		t.Fatal("flock still waits")
	}
}

func TestFlocksOfOwnersOnOneNodeExcludeEachOther(t *testing.T) {
	tbl := newFlockTable(nil)
	ctx := context.Background()
	requireFlockErrno(t, tbl.lock(ctx, flockFile1, 1, lock.LockModeExclusive, false), 0, "first exclusive flock")
	requireFlockErrno(t, tbl.lock(ctx, flockFile1, 2, lock.LockModeExclusive, false), syscall.EWOULDBLOCK, "second exclusive flock")
	requireFlockErrno(t, tbl.lock(ctx, flockFile1, 2, lock.LockModeShared, false), syscall.EWOULDBLOCK, "shared flock next to an exclusive one")
	requireFlockErrno(t, tbl.lock(ctx, lock.FlockKey{Inode: 8, Birth: 1}, 3, lock.LockModeExclusive, false), 0, "flock on another file")

	waiter := lockInBackground(tbl, ctx, 2, lock.LockModeExclusive)
	requireStillWaiting(t, waiter)
	tbl.unlock(1)
	requireDone(t, waiter, 0)
	requireFlockErrno(t, tbl.lock(ctx, flockFile1, 4, lock.LockModeShared, false), syscall.EWOULDBLOCK, "shared flock next to the waiter that got exclusive")
}

func TestSharedFlocksOfOwnersOnOneNodeHoldTogether(t *testing.T) {
	tbl := newFlockTable(nil)
	ctx := context.Background()
	requireFlockErrno(t, tbl.lock(ctx, flockFile1, 1, lock.LockModeShared, false), 0, "first shared flock")
	requireFlockErrno(t, tbl.lock(ctx, flockFile1, 2, lock.LockModeShared, false), 0, "second shared flock")
	waiter := lockInBackground(tbl, ctx, 3, lock.LockModeExclusive)
	tbl.unlock(1)
	requireStillWaiting(t, waiter)
	tbl.unlock(2)
	requireDone(t, waiter, 0)
}

func TestFlockConversionDropsTheOldModeFirst(t *testing.T) {
	tbl := newFlockTable(nil)
	ctx := context.Background()
	requireFlockErrno(t, tbl.lock(ctx, flockFile1, 1, lock.LockModeShared, false), 0, "owner 1 shared")
	requireFlockErrno(t, tbl.lock(ctx, flockFile1, 2, lock.LockModeShared, false), 0, "owner 2 shared")
	requireFlockErrno(t, tbl.lock(ctx, flockFile1, 1, lock.LockModeExclusive, false), syscall.EWOULDBLOCK, "owner 1 converting to exclusive")
	tbl.unlock(2)
	requireFlockErrno(t, tbl.lock(ctx, flockFile1, 3, lock.LockModeExclusive, false), 0,
		"exclusive flock after a failed conversion dropped owner 1's shared one")
	if len(tbl.owners) != 1 {
		t.Fatalf("owners %v, want only owner 3", tbl.owners)
	}
}

func TestEndedFlockWaitReturnsEINTR(t *testing.T) {
	tbl := newFlockTable(nil)
	requireFlockErrno(t, tbl.lock(context.Background(), flockFile1, 1, lock.LockModeExclusive, false), 0, "holder")
	ctx, cancel := context.WithCancel(context.Background())
	waiter := lockInBackground(tbl, ctx, 2, lock.LockModeShared)
	requireStillWaiting(t, waiter)
	cancel()
	requireDone(t, waiter, syscall.EINTR)
	tbl.unlock(1)
	if len(tbl.files) != 0 || len(tbl.owners) != 0 {
		t.Fatalf("after the last unlock the table keeps files %v and owners %v", tbl.files, tbl.owners)
	}
}

func TestClusterFlockFollowsTheStrongestLocalHold(t *testing.T) {
	cluster := &fakeFlockCluster{}
	tbl := newFlockTable(cluster)
	ctx := context.Background()
	steps := []struct {
		what  string
		do    func() syscall.Errno
		calls []string
	}{
		{"owner 1 exclusive", func() syscall.Errno { return tbl.lock(ctx, flockFile1, 1, lock.LockModeExclusive, true) }, []string{"flock 7 mode 2"}},
		{"owner 1 converts to shared", func() syscall.Errno { return tbl.lock(ctx, flockFile1, 1, lock.LockModeShared, true) }, []string{"flock 7 mode 1"}},
		{"owner 2 shared", func() syscall.Errno { return tbl.lock(ctx, flockFile1, 2, lock.LockModeShared, true) }, nil},
		{"owner 1 unlocks", func() syscall.Errno { tbl.unlock(1); return 0 }, nil},
		{"owner 2 unlocks", func() syscall.Errno { tbl.unlock(2); return 0 }, []string{"unflock 7"}},
		{"owner 3 shared", func() syscall.Errno { return tbl.lock(ctx, flockFile1, 3, lock.LockModeShared, true) }, []string{"flock 7 mode 1"}},
		{"owner 3 converts to exclusive", func() syscall.Errno { return tbl.lock(ctx, flockFile1, 3, lock.LockModeExclusive, true) }, []string{"flock 7 mode 2"}},
	}
	for _, step := range steps {
		requireFlockErrno(t, step.do(), 0, step.what)
		if calls := cluster.takeCalls(); !slices.Equal(calls, step.calls) {
			t.Fatalf("%s: cluster calls %v, want %v", step.what, calls, step.calls)
		}
	}
}

func TestClusterRefusalReturnsEWOULDBLOCK(t *testing.T) {
	cluster := &fakeFlockCluster{refuse: true}
	tbl := newFlockTable(cluster)
	requireFlockErrno(t, tbl.lock(context.Background(), flockFile1, 1, lock.LockModeExclusive, false), syscall.EWOULDBLOCK, "flock another node holds")
	if len(tbl.files) != 0 || len(tbl.owners) != 0 {
		t.Fatalf("a refused flock left files %v and owners %v", tbl.files, tbl.owners)
	}
}

func TestOwnersWaitBehindAClusterWait(t *testing.T) {
	cluster := &fakeFlockCluster{gate: make(chan struct{})}
	tbl := newFlockTable(cluster)
	ctx := context.Background()
	first := lockInBackground(tbl, ctx, 1, lock.LockModeExclusive)
	requireStillWaiting(t, first)
	requireFlockErrno(t, tbl.lock(ctx, flockFile1, 2, lock.LockModeShared, false), syscall.EWOULDBLOCK, "non-blocking flock behind a cluster wait")
	second := lockInBackground(tbl, ctx, 2, lock.LockModeShared)
	requireStillWaiting(t, second)

	close(cluster.gate)
	requireDone(t, first, 0)
	requireStillWaiting(t, second)
	tbl.unlock(1)
	requireDone(t, second, 0)
}

func TestInterruptedClusterWaitReturnsEINTR(t *testing.T) {
	cluster := &fakeFlockCluster{gate: make(chan struct{})}
	tbl := newFlockTable(cluster)
	ctx, cancel := context.WithCancel(context.Background())
	waiter := lockInBackground(tbl, ctx, 1, lock.LockModeExclusive)
	requireStillWaiting(t, waiter)
	cancel()
	requireDone(t, waiter, syscall.EINTR)
	if len(tbl.files) != 0 || len(tbl.owners) != 0 {
		t.Fatalf("an interrupted flock left files %v and owners %v", tbl.files, tbl.owners)
	}
}

// flockRaw stands in for go-fuse's node file system: it takes an exclusive flock on the inode NodeId names.
type flockRaw struct {
	fuse.RawFileSystem
	tbl *flockTable
}

func (r *flockRaw) SetLk(_ <-chan struct{}, in *fuse.LkIn) fuse.Status {
	return fuse.Status(r.tbl.lock(context.Background(), lock.FlockKey{Inode: uint32(in.NodeId)}, in.Owner, lock.LockModeExclusive, false))
}

func (r *flockRaw) Release(<-chan struct{}, *fuse.ReleaseIn) {}

// The kernel names a flock's owner by its struct file's address, which a new open can reuse before the old open's
// RELEASE arrives; that RELEASE must not drop the new open's flock.
func TestLateReleaseKeepsANewerOpenFilesFlock(t *testing.T) {
	tbl := newFlockTable(nil)
	raw := &flockFS{RawFileSystem: &flockRaw{RawFileSystem: fuse.NewDefaultRawFileSystem(), tbl: tbl}, flocks: tbl}
	const reused = 0xabc
	setlk := func(fh, nodeID uint64) fuse.Status {
		return raw.SetLk(nil, &fuse.LkIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: fh, Owner: reused, LkFlags: fuse.FUSE_LK_FLOCK})
	}
	if st := setlk(5, 7); !st.Ok() {
		t.Fatalf("flock through handle 5 = %v", st)
	}
	if st := setlk(7, 8); !st.Ok() {
		t.Fatalf("flock through handle 7, from an open that reused the kernel's lock owner = %v", st)
	}
	raw.Release(nil, &fuse.ReleaseIn{Fh: 5, LockOwner: reused, ReleaseFlags: fuse.FUSE_RELEASE_FLOCK_UNLOCK})

	requireFlockErrno(t, tbl.lock(context.Background(), lock.FlockKey{Inode: 8}, 99, lock.LockModeExclusive, false), syscall.EWOULDBLOCK,
		"flock on handle 7's inode after handle 5's late release")
	requireFlockErrno(t, tbl.lock(context.Background(), lock.FlockKey{Inode: 7}, 99, lock.LockModeExclusive, false), 0,
		"flock on handle 5's inode after its release")
}
