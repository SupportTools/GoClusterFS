package goclusterfs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/quota"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
)

// The HTTP API calls these instead of setxattr on its own mount, which can deadlock the daemon with the kernel.

// notifySlow is how long a kernel cache notification may take before it is logged.
const notifySlow = 5 * time.Second

// MarkMounted tells the FS that a FUSE server now serves it, so API changes expire the kernel's caches.
func (f *FS) MarkMounted() {
	f.mounted.Store(true)
}

// SnapshotsEnabled reports whether the snapshot registry is on (-snapshots).
func (f *FS) SnapshotsEnabled() bool {
	return f.snapshotReg != nil
}

// SnapshotFile does what setfattr -n user.snapshot -v name does on the file at p.
func (f *FS) SnapshotFile(ctx context.Context, p, name string) error {
	return f.setxattrAt(ctx, p, "user.snapshot", name, "")
}

// DeleteFileSnapshot does what setfattr -n user.snapshot.delete -v name does on the file at p.
func (f *FS) DeleteFileSnapshot(ctx context.Context, p, name string) error {
	return f.setxattrAt(ctx, p, "user.snapshot.delete", name, "")
}

// CloneFileSnapshot does what setfattr -n user.snapshot.clone -v name:dest does on the file at p.
func (f *FS) CloneFileSnapshot(ctx context.Context, p, name, dest string) error {
	return f.setxattrAt(ctx, p, "user.snapshot.clone", name+":"+dest, dest)
}

// CloneFile does what setfattr -n user.clone -v dest does on the file at p.
func (f *FS) CloneFile(ctx context.Context, p, dest string) error {
	return f.setxattrAt(ctx, p, "user.clone", dest, dest)
}

// SnapshotsAt returns the snapshots of the file at p.
func (f *FS) SnapshotsAt(ctx context.Context, p string) ([]*snapshot.Descriptor, error) {
	file, err := f.lookupFile(ctx, p)
	if err != nil {
		return nil, err
	}
	return f.FileSnapshots(ctx, file.inodeNum)
}

func (f *FS) setxattrAt(ctx context.Context, p, attr, value, newPath string) error {
	file, err := f.lookupFile(ctx, p)
	if err != nil {
		return err
	}
	if errno := file.setxattr(ctx, attr, []byte(value)); errno != 0 {
		return fmt.Errorf("%s: %w", p, errno)
	}
	if attr != "user.snapshot.delete" {
		go f.notifyKernel(p, file.inodeNum, newPath)
	}
	return nil
}

// lookupFile finds the regular file at the absolute, clean path p, refusing a symlink anywhere on the way.
func (f *FS) lookupFile(ctx context.Context, p string) (*File, error) {
	if p == "/" {
		return nil, fmt.Errorf("%q is not an absolute, clean path to a file: %w", p, syscall.EINVAL)
	}
	ino, err := f.lookupPath(ctx, p)
	switch {
	case err != nil:
		return nil, err
	case ino.IsDir():
		return nil, fmt.Errorf("%s: %w", p, syscall.EISDIR)
	case !ino.IsFile():
		return nil, fmt.Errorf("%s is not a regular file: %w", p, syscall.EINVAL)
	}
	return newFile(f, ino), nil
}

// lookupDir finds the directory at the absolute, clean path p, refusing a symlink anywhere on the way.
func (f *FS) lookupDir(ctx context.Context, p string) (*inode.Inode, error) {
	ino, err := f.lookupPath(ctx, p)
	if err == nil && !ino.IsDir() {
		return nil, fmt.Errorf("%s: %w", p, syscall.ENOTDIR)
	}
	return ino, err
}

// lookupPath finds the inode at the absolute, clean path p, refusing a symlink anywhere on the way.
func (f *FS) lookupPath(ctx context.Context, p string) (*inode.Inode, error) {
	if !strings.HasPrefix(p, "/") || path.Clean(p) != p || strings.ContainsRune(p, '\x00') {
		return nil, fmt.Errorf("%q is not an absolute, clean path: %w", p, syscall.EINVAL)
	}
	if p == "/" {
		return f.inodeTbl.Read(inode.InodeRoot)
	}
	names := strings.Split(p[1:], "/")
	parent, err := f.getInode(inode.InodeRoot)
	if err != nil {
		return nil, err
	}
	for _, name := range names[:len(names)-1] {
		num, err := f.lookupName(ctx, parent, name, p)
		if err != nil {
			return nil, err
		}
		if parent, err = f.getInode(num); err != nil {
			return nil, err
		}
		switch {
		case parent.IsSymlink():
			return nil, fmt.Errorf("%s goes through a symlink: %w", p, syscall.ELOOP)
		case !parent.IsDir():
			return nil, fmt.Errorf("%s: %w", p, syscall.ENOTDIR)
		}
	}
	num, err := f.lookupName(ctx, parent, names[len(names)-1], p)
	if err != nil {
		return nil, err
	}
	ino, err := f.inodeTbl.Read(num)
	switch {
	case err != nil:
		return nil, err
	case ino.IsSymlink():
		return nil, fmt.Errorf("%s is a symlink: %w", p, syscall.ELOOP)
	}
	return ino, nil
}

// ProjectQuota returns what reading trusted.goclusterfs.quota on the directory at p returns.
func (f *FS) ProjectQuota(ctx context.Context, p string) (*quota.Status, error) {
	dirIno, err := f.lookupDir(ctx, p)
	if err != nil {
		return nil, err
	}
	q, errno := f.projectQuota(dirIno.InodeNum)
	if errno != 0 {
		return nil, fmt.Errorf("%s: %w", p, errno)
	}
	return q, nil
}

// SetProjectQuota does what setting trusted.goclusterfs.quota to "bytes=N inodes=N" on the directory at p does, a nil
// limit standing for a key left out, and returns the project's limits and usage after it. A caller that gives up does
// not stop it halfway, which would leave a record no directory names.
func (f *FS) SetProjectQuota(ctx context.Context, p string, bytes, inodes *uint64) (*quota.Status, error) {
	ctx = detach(ctx)
	dirIno, err := f.lookupDir(ctx, p)
	if err != nil {
		return nil, err
	}
	if errno := f.setLimits(ctx, dirIno.InodeNum, quotaSetting{bytes: bytes, inodes: inodes}); errno != 0 {
		return nil, fmt.Errorf("%s: %w", p, errno)
	}
	return f.ProjectQuota(ctx, p)
}

func (f *FS) lookupName(ctx context.Context, parent *inode.Inode, name, p string) (uint32, error) {
	num, _, err := f.dirMgr.Lookup(ctx, parent, name)
	if errors.Is(err, dir.ErrNotFound) {
		return 0, fmt.Errorf("%s: %w", p, syscall.ENOENT)
	}
	if err != nil {
		return 0, err
	}
	if errno := f.childInRange(parent.InodeNum, name, num); errno != 0 {
		return 0, fmt.Errorf("%s: %w", p, errno)
	}
	return uint32(num), nil
}

// notifyKernel expires this node's cached attributes of p and a cached miss for newPath; it runs off the request and holds no lock.
func (f *FS) notifyKernel(p string, num uint32, newPath string) {
	if !f.mounted.Load() {
		return
	}
	slow := time.AfterFunc(notifySlow, func() {
		log.Printf("[WARN] telling the kernel about an API change to %s has taken over %v", p, notifySlow)
	})
	defer slow.Stop()
	if node := f.kernelNode(p); node != nil && node.StableAttr().Ino == uint64(num) {
		if errno := node.NotifyContent(-1, 0); errno != 0 && errno != syscall.ENOENT && errno != syscall.ENOSYS {
			log.Printf("[WARN] expiring the kernel's attributes of %s: %v", p, errno)
		}
	}
	if newPath != "" {
		if errno := f.NotifyEntry(strings.TrimPrefix(newPath, "/")); errno != 0 && errno != syscall.ENOENT && errno != syscall.ENOSYS {
			log.Printf("[WARN] expiring the kernel's entry for %s: %v", newPath, errno)
		}
	}
}

// kernelNode returns the node the kernel knows at p, or nil if it has not looked p up.
func (f *FS) kernelNode(p string) *fs.Inode {
	node := f.EmbeddedInode()
	for _, name := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if node = node.GetChild(name); node == nil {
			return nil
		}
	}
	return node
}
