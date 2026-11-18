package goclusterfs

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// A peer died holding the inode table sector of the root directory, as one did mid-unlink in the release soak. Each
// operation below needs that sector while it holds another lock, such as the root's inode lock, which the peer's
// replay may need too. It lets go of every lock, waits out the replay, and then succeeds, with no EIO.
func TestOperationsWaitOutTheReplayOfALockHolder(t *testing.T) {
	type op struct {
		name  string
		setup func(t *testing.T, fsys *FS) uint32
		run   func(fsys *FS, num uint32) syscall.Errno
		check func(t *testing.T, fsys *FS, num uint32)
	}
	ctx := context.Background()
	named0 := func(name string) func(*testing.T, *FS, uint32) {
		return func(t *testing.T, fsys *FS, _ uint32) {
			if got := named(t, fsys, inode.InodeRoot, name); got != 0 {
				t.Errorf("%s still names inode %d", name, got)
			}
		}
	}
	namedNum := func(name string) func(*testing.T, *FS, uint32) {
		return func(t *testing.T, fsys *FS, num uint32) {
			if got := named(t, fsys, inode.InodeRoot, name); got == 0 || num != 0 && got != num {
				t.Errorf("%s names inode %d, want %d", name, got, num)
			}
		}
	}
	file := func(t *testing.T, fsys *FS) uint32 { return fileIn(t, fsys, inode.InodeRoot, "f").inodeNum }
	none := func(*testing.T, *FS) uint32 { return 0 }
	for _, c := range []op{
		{"unlink", file, func(fsys *FS, _ uint32) syscall.Errno { return fsys.Unlink(ctx, "f") }, named0("f")},
		{"rmdir", func(t *testing.T, fsys *FS) uint32 { return newSubdir(t, fsys, "d").inodeNum },
			func(fsys *FS, _ uint32) syscall.Errno { return fsys.Rmdir(ctx, "d") }, named0("d")},
		{"create", none, func(fsys *FS, _ uint32) syscall.Errno {
			_, _, _, errno := fsys.Create(ctx, "g", 0, 0o644, &fuse.EntryOut{})
			return errno
		}, namedNum("g")},
		{"mkdir", none, func(fsys *FS, _ uint32) syscall.Errno {
			_, errno := fsys.Mkdir(ctx, "e", 0o755, &fuse.EntryOut{})
			return errno
		}, namedNum("e")},
		{"rename", file, func(fsys *FS, _ uint32) syscall.Errno { return fsys.Rename(ctx, "f", fsys, "f2", 0) }, namedNum("f2")},
		{"link", file, func(fsys *FS, num uint32) syscall.Errno {
			_, errno := fsys.Link(ctx, fileNode(fsys, num), "f3", &fuse.EntryOut{})
			return errno
		}, namedNum("f3")},
		{"symlink", none, func(fsys *FS, _ uint32) syscall.Errno {
			_, errno := fsys.Symlink(ctx, "target", "s", &fuse.EntryOut{})
			return errno
		}, namedNum("s")},
		{"chmod", file, func(fsys *FS, num uint32) syscall.Errno {
			in := &fuse.SetAttrIn{}
			in.Valid, in.Mode = fuse.FATTR_MODE, 0o600
			return fileNode(fsys, num).Setattr(ctx, nil, in, &fuse.AttrOut{})
		}, func(t *testing.T, fsys *FS, num uint32) {
			if mode := readInode(t, fsys, num).Mode & 0o777; mode != 0o600 {
				t.Errorf("mode %o after chmod, want 600", mode)
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			path, lockPath := newTreeFSImage(t), newLockImage(t)
			fsys := withBridge(openLockedTreeFSNode(t, path, lockPath, 1))
			fsys.SetSymlinks(true)
			if err := fsys.InitRoot(ctx); err != nil {
				t.Fatal(err)
			}
			num := c.setup(t, fsys)
			fsys.lockMgr.ReleaseCached()

			lockDev, err := storage.Open(lockPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lockDev.Close() }()
			var replayed atomic.Bool
			fsys.lockMgr.SetOwnerView(func(slot int, fresh bool) ([16]byte, lock.OwnerState) {
				switch {
				case slot == 3 && !replayed.Load():
					return [16]byte{3}, lock.OwnerFailing
				case slot == 1:
					return [16]byte{1}, lock.OwnerAlive
				}
				return [16]byte{}, lock.OwnerReleased
			})
			dead := lock.NewManager(lockDev, 0, testLockRegion, [16]byte{3}, 3)
			sector := uint64(inode.InodeRoot) / inode.InodesPerSector
			if err := dead.Acquire(ctx, lock.LockTypeInodeSector, sector, lock.LockModeExclusive); err != nil {
				t.Fatal(err)
			}

			done := make(chan syscall.Errno, 1)
			go func() { done <- c.run(fsys, num) }()
			deadline := time.Now().Add(5 * time.Second)
			for st := fsys.lockMgr.Stats(); st.Waiting != 1 || st.Held != 0; st = fsys.lockMgr.Stats() {
				if time.Now().After(deadline) {
					t.Fatalf("%s holds %d locks while it waits for the failed node's sector, want 0", c.name, st.Held)
				}
				time.Sleep(10 * time.Millisecond)
			}

			if err := dead.Release(lock.LockTypeInodeSector, sector); err != nil {
				t.Errorf("the failed node could not release inode table sector %d: %v", sector, err)
			}
			dead.ReleaseCached()
			replayed.Store(true)
			select {
			case errno := <-done:
				if errno != 0 {
					t.Fatalf("%s after the replay: %v", c.name, errno)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s still waits 5 s after the replay ended", c.name)
			}
			c.check(t, fsys, num)
		})
	}
}

// deadPeer gives fsys a peer that holds locks and fails until replayed is set, and a live peer that replays it, both
// on fsys's lock region, in slots other than fsys's own.
func deadPeer(t *testing.T, fsys *FS, replayed *atomic.Bool) (dead, replayer *lock.Manager) {
	t.Helper()
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	self := int(fsys.nodeUUID[15])
	deadSlot, replayerSlot := (self+1)%64, (self+2)%64
	deadUUID, replayerUUID := [16]byte{byte(deadSlot), 'd'}, [16]byte{byte(replayerSlot), 'r'}
	view := func(slot int, fresh bool) ([16]byte, lock.OwnerState) {
		switch {
		case slot == deadSlot && !replayed.Load():
			return deadUUID, lock.OwnerFailing
		case slot == self:
			return fsys.nodeUUID, lock.OwnerAlive
		case slot == replayerSlot:
			return replayerUUID, lock.OwnerAlive
		}
		return [16]byte{}, lock.OwnerReleased
	}
	fsys.lockMgr.SetOwnerView(view)
	dead = lock.NewManager(fsys.dev, sb.LockBitmapOffset, sb.LockBitmapSize, deadUUID, deadSlot)
	replayer = lock.NewManager(fsys.dev, sb.LockBitmapOffset, sb.LockBitmapSize, replayerUUID, replayerSlot)
	replayer.SetOwnerView(view)
	return dead, replayer
}

// waitReleasedAll waits until fsys waits for one lock and holds none, while the operation that reports to done runs.
func waitReleasedAll(t *testing.T, fsys *FS, what string, done chan syscall.Errno) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for st := fsys.lockMgr.Stats(); st.Waiting != 1 || st.Held != 0; st = fsys.lockMgr.Stats() {
		select {
		case errno := <-done:
			t.Fatalf("%s returned %v without waiting for the failed node's lock", what, errno)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s holds %d locks and waits for %d while a failed node holds the lock it needs, want 0 and 1", what, st.Held, st.Waiting)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The release soak's failure: a peer dies holding the inode table sector of a directory while a live node, past its
// unlink's journal commit, is inside RemoveIfAndUpdate on that directory, and the peer's replay needs the directory's
// inode lock. The unlink lets the replay have it, then finishes with no EIO and no fail-stop.
func TestUnlinkPastItsCommitOutlastsADeadPeersReplay(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	ctx := context.Background()
	fsys.SetFailStop(func(err error) { t.Errorf("the node stopped: %v", err) })
	d := newSubdir(t, fsys, "d")
	fileIn(t, fsys, d.inodeNum, "f")
	fsys.lockMgr.ReleaseCached()
	var replayed atomic.Bool
	dead, replayer := deadPeer(t, fsys, &replayed)
	sector := uint64(d.inodeNum) / inode.InodesPerSector
	if err := dead.Acquire(ctx, lock.LockTypeInodeSector, sector, lock.LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	committed := make(chan struct{})
	afterCommit = func(label string) {
		if label == "unlink" {
			close(committed)
		}
	}
	t.Cleanup(func() { afterCommit = nil })

	done := make(chan syscall.Errno, 1)
	go func() { done <- d.Unlink(ctx, "f") }()
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("the unlink never committed")
	}
	waitReleasedAll(t, fsys, "the unlink", done)

	replayCtx, cancel := context.WithTimeout(lock.ForReplay(ctx), 5*time.Second)
	defer cancel()
	if err := replayer.Acquire(replayCtx, lock.LockTypeInode, uint64(d.inodeNum), lock.LockModeExclusive); err != nil {
		t.Fatalf("the replay could not take the directory's inode lock: %v", err)
	}
	if err := replayer.Release(lock.LockTypeInode, uint64(d.inodeNum)); err != nil {
		t.Errorf("the replay could not release the directory's inode lock: %v", err)
	}
	replayer.ReleaseCached()
	if err := dead.Release(lock.LockTypeInodeSector, sector); err != nil {
		t.Errorf("the failed node could not release inode table sector %d: %v", sector, err)
	}
	dead.ReleaseCached()
	replayed.Store(true)

	select {
	case errno := <-done:
		if errno != 0 {
			t.Fatalf("unlink after the replay: %v", errno)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the unlink still waits 5 s after the replay ended")
	}
	if got := named(t, fsys, d.inodeNum, "f"); got != 0 {
		t.Errorf("d/f still names inode %d", got)
	}
	expectCleanFsck(t, fsys)
}

// A rename takes the inode table sectors of the directories it writes before its journal commit. With a dead peer
// holding the new parent's sector, the rename lets go of its locks and waits before it commits, then finishes after
// the replay with exactly one name and no fail-stop.
func TestRenameWaitsForItsSectorsBeforeItsCommit(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	ctx := context.Background()
	fsys.SetFailStop(func(err error) { t.Errorf("the node stopped: %v", err) })
	p1, p2 := newSubdir(t, fsys, "p1"), newSubdir(t, fsys, "p2")
	a := fileIn(t, fsys, p1.inodeNum, "a").inodeNum
	fsys.lockMgr.ReleaseCached()
	var replayed atomic.Bool
	dead, _ := deadPeer(t, fsys, &replayed)
	sector := uint64(p2.inodeNum) / inode.InodesPerSector
	if err := dead.Acquire(ctx, lock.LockTypeInodeSector, sector, lock.LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	var commits atomic.Int32
	afterCommit = func(label string) {
		if label == "rename" {
			commits.Add(1)
		}
	}
	t.Cleanup(func() { afterCommit = nil })

	done := make(chan syscall.Errno, 1)
	go func() { done <- p1.Rename(ctx, "a", p2, "b", 0) }()
	waitReleasedAll(t, fsys, "the rename", done)
	if n := commits.Load(); n != 0 {
		t.Fatalf("the rename committed %d times before it had its sectors", n)
	}

	if err := dead.Release(lock.LockTypeInodeSector, sector); err != nil {
		t.Errorf("the failed node could not release inode table sector %d: %v", sector, err)
	}
	dead.ReleaseCached()
	replayed.Store(true)
	select {
	case errno := <-done:
		if errno != 0 {
			t.Fatalf("rename after the replay: %v", errno)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the rename still waits 5 s after the replay ended")
	}
	if n := commits.Load(); n != 1 {
		t.Errorf("the rename committed %d times, want 1", n)
	}
	expectOneName(t, fsys, p1.inodeNum, "a", p2.inodeNum, "b", a)
	expectDirLinks(t, fsys, p1.inodeNum)
	expectDirLinks(t, fsys, p2.inodeNum)
	expectCleanFsck(t, fsys)
}
