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
