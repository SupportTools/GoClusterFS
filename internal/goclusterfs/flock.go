package goclusterfs

import (
	"context"
	"errors"
	"log"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
)

var (
	_ fs.NodeSetlker  = (*File)(nil)
	_ fs.NodeSetlkwer = (*File)(nil)
	_ fs.NodeSetlker  = (*Dir)(nil)
	_ fs.NodeSetlkwer = (*Dir)(nil)
	_ fs.NodeSetlker  = (*FS)(nil)
	_ fs.NodeSetlkwer = (*FS)(nil)
)

// RawFS is the go-fuse file system to mount for f. The kernel sends it only
// flock(2) locks, with fuse.MountOptions.EnableFlockLocks, and it drops a
// flock at the last close of the open file that holds it.
func (f *FS) RawFS(opts *fs.Options) fuse.RawFileSystem {
	return &flockFS{RawFileSystem: fs.NewNodeFS(f, opts), flocks: f.flocks}
}

// flockFS names a flock's owner by the open file's handle, not by the kernel's
// lock owner. That owner is the address of the kernel's struct file, which a
// new open can reuse before the old one's RELEASE, sent in the background,
// arrives. go-fuse frees a handle only in Release, after its flock is dropped.
type flockFS struct {
	fuse.RawFileSystem
	flocks *flockTable
}

func (r *flockFS) SetLk(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	in.Owner = in.Fh
	return r.RawFileSystem.SetLk(cancel, in)
}

func (r *flockFS) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	in.Owner = in.Fh
	return r.RawFileSystem.SetLkw(cancel, in)
}

func (r *flockFS) Release(cancel <-chan struct{}, in *fuse.ReleaseIn) {
	r.flocks.unlock(in.Fh)
	r.RawFileSystem.Release(cancel, in)
}

func (r *flockFS) ReleaseDir(in *fuse.ReleaseIn) {
	r.flocks.unlock(in.Fh)
	r.RawFileSystem.ReleaseDir(in)
}

func (f *File) Setlk(ctx context.Context, _ fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return f.fs.setFlock(ctx, lock.FlockKey{Inode: f.inodeNum, Birth: f.birth}, owner, lk, flags, false)
}

func (f *File) Setlkw(ctx context.Context, _ fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return f.fs.setFlock(ctx, lock.FlockKey{Inode: f.inodeNum, Birth: f.birth}, owner, lk, flags, true)
}

func (d *Dir) Setlk(ctx context.Context, _ fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return d.fs.setDirFlock(ctx, d.inodeNum, owner, lk, flags, false)
}

func (d *Dir) Setlkw(ctx context.Context, _ fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return d.fs.setDirFlock(ctx, d.inodeNum, owner, lk, flags, true)
}

func (f *FS) Setlk(ctx context.Context, _ fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return f.setDirFlock(ctx, inode.InodeRoot, owner, lk, flags, false)
}

func (f *FS) Setlkw(ctx context.Context, _ fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return f.setDirFlock(ctx, inode.InodeRoot, owner, lk, flags, true)
}

// setDirFlock reads the directory's birth generation, which Dir does not keep,
// unless the request only unlocks.
func (f *FS) setDirFlock(ctx context.Context, num uint32, owner uint64, lk *fuse.FileLock, flags uint32, wait bool) syscall.Errno {
	key := lock.FlockKey{Inode: num}
	if lk.Typ != syscall.F_UNLCK {
		ino, err := f.inodeTbl.Read(num)
		if err != nil {
			return syscall.EIO
		}
		key.Birth = ino.BirthGeneration
	}
	return f.setFlock(ctx, key, owner, lk, flags, wait)
}

// setFlock serves a flock(2) call, which the kernel sends as a lock on the
// whole file with FUSE_LK_FLOCK set. owner is the open file's handle.
func (f *FS) setFlock(ctx context.Context, key lock.FlockKey, owner uint64, lk *fuse.FileLock, flags uint32, wait bool) syscall.Errno {
	if flags&fuse.FUSE_LK_FLOCK == 0 {
		return syscall.ENOTSUP
	}
	switch lk.Typ {
	case syscall.F_UNLCK:
		f.flocks.unlock(owner)
		return 0
	case syscall.F_RDLCK:
		return f.flocks.lock(ctx, key, owner, lock.LockModeShared, wait)
	case syscall.F_WRLCK:
		return f.flocks.lock(ctx, key, owner, lock.LockModeExclusive, wait)
	}
	return syscall.EINVAL
}

// clusterFlocks takes this node's flocks across the cluster, as lock.Manager does.
type clusterFlocks interface {
	Flock(ctx context.Context, key lock.FlockKey, mode lock.LockMode, wait bool) error
	Unflock(key lock.FlockKey) error
}

// flockTable serves flock(2) on this node. Each open file, named by its FUSE
// handle, holds one mode on one inode. Owners on this
// node wait here for each other, and the node's cluster flock on an inode
// follows the strongest mode its owners hold. Without a cluster, flocks hold
// only on this node.
type flockTable struct {
	cluster clusterFlocks

	mu     sync.Mutex
	files  map[lock.FlockKey]*flockFile
	owners map[uint64]lock.FlockKey
}

type flockFile struct {
	holders map[uint64]lock.LockMode
	// cluster is the mode this node holds across the cluster.
	cluster lock.LockMode
	// busy is set while a cluster call for the file runs; others wait for changed.
	// waiting is set when that call waits for other nodes.
	busy, waiting bool
	changed       chan struct{}
}

// clusterOf returns lockMgr as the cluster for flocks, or nil when there is no lock manager.
func clusterOf(lockMgr *lock.Manager) clusterFlocks {
	if lockMgr == nil {
		return nil
	}
	return lockMgr
}

func newFlockTable(cluster clusterFlocks) *flockTable {
	return &flockTable{cluster: cluster, files: map[lock.FlockKey]*flockFile{}, owners: map[uint64]lock.FlockKey{}}
}

// lock gives owner a flock on key in mode, replacing the one it holds: like
// flock(2), it drops the old mode before it waits for the new one. It returns
// EWOULDBLOCK instead of waiting if wait is false, and EINTR if ctx ends. While
// one owner waits for other nodes, the file's other owners here wait behind it.
func (t *flockTable) lock(ctx context.Context, key lock.FlockKey, owner uint64, mode lock.LockMode, wait bool) syscall.Errno {
	t.mu.Lock()
	defer t.mu.Unlock()
	if held, ok := t.owners[owner]; ok {
		if held == key && t.files[key].holders[owner] == mode {
			return 0
		}
		t.dropLocked(owner)
		if held != key {
			t.settleLocked(held)
		}
	}
	defer t.settleLocked(key)
	for {
		f := t.fileLocked(key)
		switch {
		case conflictsLocally(f, owner, mode) || f.busy:
			if !wait && (!f.busy || f.waiting) {
				return syscall.EWOULDBLOCK
			}
			if err := t.waitLocked(ctx, f); err != 0 {
				return err
			}
			continue
		case t.cluster != nil && f.cluster != lock.LockModeExclusive && f.cluster != mode:
			f.waiting = wait
			if err := t.clusterLocked(f, func() error { return t.cluster.Flock(ctx, key, mode, wait) }); err != nil {
				f.cluster = lock.LockModeNone
				return flockErrno(err)
			}
			f.cluster = mode
			continue
		}
		f.holders[owner] = mode
		t.owners[owner] = key
		notifyLocked(f)
		return 0
	}
}

// unlock drops owner's flock, if it holds one.
func (t *flockTable) unlock(owner uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key, ok := t.owners[owner]
	if !ok {
		return
	}
	t.dropLocked(owner)
	t.settleLocked(key)
}

func conflictsLocally(f *flockFile, owner uint64, mode lock.LockMode) bool {
	for other, held := range f.holders {
		if other != owner && (mode == lock.LockModeExclusive || held == lock.LockModeExclusive) {
			return true
		}
	}
	return false
}

func (t *flockTable) fileLocked(key lock.FlockKey) *flockFile {
	f, ok := t.files[key]
	if !ok {
		f = &flockFile{holders: map[uint64]lock.LockMode{}, changed: make(chan struct{})}
		t.files[key] = f
	}
	return f
}

func (t *flockTable) dropLocked(owner uint64) {
	key := t.owners[owner]
	delete(t.owners, owner)
	if f, ok := t.files[key]; ok {
		delete(f.holders, owner)
		notifyLocked(f)
	}
}

func notifyLocked(f *flockFile) {
	close(f.changed)
	f.changed = make(chan struct{})
}

// waitLocked waits for f to change, or returns EINTR when ctx ends.
func (t *flockTable) waitLocked(ctx context.Context, f *flockFile) syscall.Errno {
	changed := f.changed
	t.mu.Unlock()
	defer t.mu.Lock()
	select {
	case <-changed:
		return 0
	case <-ctx.Done():
		return syscall.EINTR
	}
}

// clusterLocked runs call, a cluster call for f, without t.mu, while other
// callers for f wait.
func (t *flockTable) clusterLocked(f *flockFile, call func() error) error {
	f.busy = true
	t.mu.Unlock()
	err := call()
	t.mu.Lock()
	f.busy, f.waiting = false, false
	notifyLocked(f)
	return err
}

// settleLocked brings this node's cluster flock on key down to the strongest
// mode its owners hold, and forgets the file once nothing holds it.
func (t *flockTable) settleLocked(key lock.FlockKey) {
	f, ok := t.files[key]
	if !ok {
		return
	}
	for t.cluster != nil && !f.busy {
		want := lock.LockModeNone
		for _, held := range f.holders {
			want = max(want, held)
		}
		if f.cluster == want || f.cluster == lock.LockModeNone {
			break
		}
		var err error
		if want == lock.LockModeNone {
			err = t.clusterLocked(f, func() error { return t.cluster.Unflock(key) })
		} else {
			err = t.clusterLocked(f, func() error { return t.cluster.Flock(context.Background(), key, want, false) })
		}
		if err != nil {
			log.Printf("[WARN] flock: inode %d: lowering this node's flock to mode %d: %v", key.Inode, want, err)
		}
		f.cluster = want
	}
	if len(f.holders) == 0 && !f.busy && f.cluster == lock.LockModeNone {
		delete(t.files, key)
	}
}

func flockErrno(err error) syscall.Errno {
	switch {
	case errors.Is(err, lock.ErrWouldBlock):
		return syscall.EWOULDBLOCK
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return syscall.EINTR
	case errors.Is(err, lock.ErrFlockFull):
		return syscall.ENOLCK
	}
	log.Printf("[WARN] flock: %v", err)
	return syscall.EIO
}
