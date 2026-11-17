// Package fileops runs the API's snapshot and clone requests in the mount daemon. They reach the same code as
// setfattr, but never through the daemon's own FUSE mount: a daemon that waits on its own mount can deadlock.
package fileops

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/supporttools/GoClusterFS/internal/api"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
)

// FS is the mounted filesystem's snapshot and clone code; paths are absolute within the mount.
type FS interface {
	SnapshotsEnabled() bool
	AllSnapshots(ctx context.Context) ([]snapshot.Entry, error)
	SnapshotsAt(ctx context.Context, p string) ([]*snapshot.Descriptor, error)
	DeleteOrphanSnapshot(ctx context.Context, name string) error
	SnapshotFile(ctx context.Context, p, name string) error
	DeleteFileSnapshot(ctx context.Context, p, name string) error
	CloneFileSnapshot(ctx context.Context, p, name, dest string) error
	CloneFile(ctx context.Context, p, dest string) error
}

// Ops implements api.FileService on a mounted filesystem.
type Ops struct {
	fs FS
}

// New returns Ops for fsys.
func New(fsys FS) *Ops {
	return &Ops{fs: fsys}
}

var _ api.FileService = (*Ops)(nil)

// ListSnapshots returns every snapshot, marking those whose file is gone, or only those of the file at p.
func (o *Ops) ListSnapshots(ctx context.Context, p string) ([]api.SnapshotInfo, error) {
	if !o.fs.SnapshotsEnabled() {
		return nil, fmt.Errorf("snapshots are %w (-snapshots=false)", api.ErrDisabled)
	}
	var entries []snapshot.Entry
	if p == "" {
		var err error
		if entries, err = o.fs.AllSnapshots(ctx); err != nil {
			return nil, err
		}
	} else {
		if err := checkPath(p); err != nil {
			return nil, err
		}
		descs, err := o.fs.SnapshotsAt(ctx, p)
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
func (o *Ops) CreateSnapshot(ctx context.Context, p, name string) error {
	if err := checkPath(p); err != nil {
		return err
	}
	if err := checkName(name); err != nil {
		return err
	}
	return o.fs.SnapshotFile(ctx, p, name)
}

// DeleteSnapshot does what setfattr -n user.snapshot.delete -v name does. Without p it deletes a snapshot whose file
// is gone.
func (o *Ops) DeleteSnapshot(ctx context.Context, p, name string) error {
	if name == "" || strings.ContainsRune(name, '\x00') {
		return fmt.Errorf("%w: snapshot name %q", api.ErrInvalidPath, name)
	}
	if p == "" {
		if !o.fs.SnapshotsEnabled() {
			return fmt.Errorf("snapshots are %w (-snapshots=false)", api.ErrDisabled)
		}
		return o.fs.DeleteOrphanSnapshot(ctx, name)
	}
	if err := checkPath(p); err != nil {
		return err
	}
	return o.fs.DeleteFileSnapshot(ctx, p, name)
}

// CloneSnapshot does what setfattr -n user.snapshot.clone -v name:dest does.
func (o *Ops) CloneSnapshot(ctx context.Context, p, name, dest string) error {
	if err := checkPath(p); err != nil {
		return err
	}
	if err := checkName(name); err != nil {
		return err
	}
	if err := checkDest(dest); err != nil {
		return err
	}
	return o.fs.CloneFileSnapshot(ctx, p, name, dest)
}

// CloneFile does what setfattr -n user.clone -v dest does.
func (o *Ops) CloneFile(ctx context.Context, p, dest string) error {
	if err := checkPath(p); err != nil {
		return err
	}
	if err := checkDest(dest); err != nil {
		return err
	}
	return o.fs.CloneFile(ctx, p, dest)
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

// checkPath accepts an absolute, clean path to a file in the mount.
func checkPath(p string) error {
	if !strings.HasPrefix(p, "/") || path.Clean(p) != p || p == "/" || strings.ContainsRune(p, '\x00') {
		return fmt.Errorf("%w: %q must be an absolute, clean path to a file in the mount", api.ErrInvalidPath, p)
	}
	return nil
}
