//go:build linux

// Package fileops runs the API's snapshot and clone requests through the
// mounted filesystem with setxattr, so they take the same path as setfattr.
package fileops

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/supporttools/GoClusterFS/internal/api"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
)

// Ops implements api.FileService for the filesystem mounted at a directory.
type Ops struct {
	root  string
	snaps Snapshots
	// checkRoot refuses a root that is not the mounted filesystem.
	checkRoot func(fd int) error
}

// Snapshots is the filesystem's snapshot registry, which knows the file each snapshot was taken of.
type Snapshots interface {
	AllSnapshots(ctx context.Context) ([]snapshot.Entry, error)
	FileSnapshots(ctx context.Context, inodeNum uint32) ([]*snapshot.Descriptor, error)
	DeleteOrphanSnapshot(ctx context.Context, name string) error
}

// New returns Ops for the mount at root, which must already be mounted.
// snaps is nil when snapshots are off.
func New(root string, snaps Snapshots) *Ops {
	var st unix.Stat_t
	dev := uint64(0)
	if unix.Stat(root, &st) == nil {
		dev = uint64(st.Dev)
	}
	return &Ops{root: root, snaps: snaps, checkRoot: func(fd int) error { return isOurMount(fd, dev) }}
}

// isOurMount refuses a root that is no longer this FUSE mount, for example a
// plain directory after FUSE has exited: setxattr there would set a host
// xattr and report success. dev is the mount's device, recorded by New.
func isOurMount(fd int, dev uint64) error {
	var sfs unix.Statfs_t
	if err := unix.Fstatfs(fd, &sfs); err != nil {
		return err
	}
	if sfs.Type != unix.FUSE_SUPER_MAGIC {
		return fmt.Errorf("the filesystem is %w", api.ErrNotMounted)
	}
	return sameDevice(fd, dev)
}

func sameDevice(fd int, dev uint64) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return err
	}
	if dev != 0 && uint64(st.Dev) != dev {
		return fmt.Errorf("the filesystem is %w: another mount replaced it", api.ErrNotMounted)
	}
	return nil
}

var _ api.FileService = (*Ops)(nil)

// ListSnapshots returns every snapshot, marking those whose file is gone, or only those of the file at p.
func (o *Ops) ListSnapshots(ctx context.Context, p string) ([]api.SnapshotInfo, error) {
	if o.snaps == nil {
		return nil, fmt.Errorf("snapshots are %w (-snapshots=false)", api.ErrDisabled)
	}
	var entries []snapshot.Entry
	if p == "" {
		var err error
		if entries, err = o.snaps.AllSnapshots(ctx); err != nil {
			return nil, err
		}
	} else {
		st, err := o.stat(p)
		if err != nil {
			return nil, err
		}
		descs, err := o.snaps.FileSnapshots(ctx, uint32(st.Ino))
		if err != nil {
			return nil, err
		}
		for _, d := range descs {
			entries = append(entries, snapshot.Entry{Descriptor: d})
		}
	}
	out := make([]api.SnapshotInfo, 0, len(entries))
	for _, e := range entries {
		d := e.Descriptor
		info := api.SnapshotInfo{
			ID:        d.SnapshotID,
			Name:      d.GetName(),
			Inode:     d.TargetInode,
			State:     d.State.String(),
			SizeBytes: d.TotalSize,
			Blocks:    d.TotalBlocks,
			Orphan:    e.Orphan,
		}
		if d.CreateTime > 0 {
			info.CreatedAt = time.Unix(d.CreateTime, 0).UTC().Format(time.RFC3339)
		}
		out = append(out, info)
	}
	return out, nil
}

// CreateSnapshot does what setfattr -n user.snapshot -v name does.
func (o *Ops) CreateSnapshot(_ context.Context, p, name string) error {
	if err := checkName(name); err != nil {
		return err
	}
	return o.setxattr(p, "user.snapshot", name)
}

// DeleteSnapshot does what setfattr -n user.snapshot.delete -v name does. Without p it deletes a snapshot whose file
// is gone.
func (o *Ops) DeleteSnapshot(ctx context.Context, p, name string) error {
	if name == "" || strings.ContainsRune(name, '\x00') {
		return fmt.Errorf("%w: snapshot name %q", api.ErrInvalidPath, name)
	}
	if p == "" {
		if o.snaps == nil {
			return fmt.Errorf("snapshots are %w (-snapshots=false)", api.ErrDisabled)
		}
		return o.snaps.DeleteOrphanSnapshot(ctx, name)
	}
	return o.setxattr(p, "user.snapshot.delete", name)
}

// CloneSnapshot does what setfattr -n user.snapshot.clone -v name:dest does.
func (o *Ops) CloneSnapshot(_ context.Context, p, name, dest string) error {
	if err := checkName(name); err != nil {
		return err
	}
	if err := checkDest(dest); err != nil {
		return err
	}
	return o.setxattr(p, "user.snapshot.clone", name+":"+dest)
}

// CloneFile does what setfattr -n user.clone -v dest does.
func (o *Ops) CloneFile(_ context.Context, p, dest string) error {
	if err := checkDest(dest); err != nil {
		return err
	}
	return o.setxattr(p, "user.clone", dest)
}

// checkName refuses names the xattr interface cannot carry: user.snapshot.clone
// splits its value at the first colon.
func checkName(name string) error {
	if name == "" || strings.ContainsAny(name, ":/\x00") {
		return fmt.Errorf("%w: snapshot name %q must be non-empty without ':', '/' or NUL", api.ErrInvalidPath, name)
	}
	return nil
}

// checkDest accepts /name only: clones can only go in the root directory.
func checkDest(dest string) error {
	if !strings.HasPrefix(dest, "/") || path.Clean(dest) != dest || strings.Count(dest, "/") != 1 || len(dest) < 2 || strings.ContainsAny(dest, ":\x00") {
		return fmt.Errorf("%w: clone destination %q must be /name in the root directory", api.ErrInvalidPath, dest)
	}
	return nil
}

// relative turns an absolute, clean path within the mount into one relative to the root.
func relative(p string) (string, error) {
	if !strings.HasPrefix(p, "/") || path.Clean(p) != p || p == "/" || strings.ContainsRune(p, '\x00') {
		return "", fmt.Errorf("%w: %q must be an absolute, clean path to a file in the mount", api.ErrInvalidPath, p)
	}
	return p[1:], nil
}

// open opens p read-only without following symlinks or leaving the mount.
// Closing a read-only descriptor reports nothing a caller could act on.
// The root is opened per call: a descriptor held open would make unmount fail with EBUSY.
func (o *Ops) open(p string) (*os.File, error) {
	rel, err := relative(p)
	if err != nil {
		return nil, err
	}
	rootFD, err := unix.Open(o.root, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(rootFD) }()
	if o.checkRoot != nil {
		if err := o.checkRoot(rootFD); err != nil {
			return nil, err
		}
	}
	// O_NONBLOCK keeps a FIFO from blocking the open.
	fd, err := unix.Openat2(rootFD, rel, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NONBLOCK | unix.O_NOCTTY,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV,
	})
	switch err {
	case nil:
		return os.NewFile(uintptr(fd), p), nil
	case unix.EXDEV, unix.ELOOP:
		return nil, fmt.Errorf("%w: %q leaves the mount or goes through a symlink", api.ErrInvalidPath, p)
	default:
		return nil, err
	}
}

func (o *Ops) stat(p string) (unix.Stat_t, error) {
	var st unix.Stat_t
	f, err := o.open(p)
	if err != nil {
		return st, err
	}
	defer func() { _ = f.Close() }()
	err = unix.Fstat(int(f.Fd()), &st)
	return st, err
}

func (o *Ops) setxattr(p, attr, value string) error {
	f, err := o.open(p)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("%w: %q is not a regular file", api.ErrInvalidPath, p)
	}
	return unix.Fsetxattr(int(f.Fd()), attr, []byte(value), 0)
}
