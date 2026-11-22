package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/quota"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// enableQuotas creates the quota table on a volume formatted without one. It refuses while fsck sees a node running,
// since a running node of this build would allocate without counting.
func enableQuotas(out io.Writer, dev *storage.Device, devicePath string) error {
	slots, err := fsck.New(dev, fsck.Options{Output: io.Discard}).Slots()
	if err != nil {
		return fmt.Errorf("enabling quotas refused: fsck cannot tell whether a node runs (%v)", err)
	}
	var running []string
	for _, s := range slots {
		if s.Running {
			running = append(running, s.Name())
		}
	}
	if len(running) > 0 {
		return fmt.Errorf("enabling quotas refused: the filesystem is mounted by %s; unmount it on every node first", strings.Join(running, ", "))
	}
	sb, err := superblock.Read(dev)
	if err != nil {
		return err
	}
	if sb.RequiredFlags&superblock.RequiredProjectQuotas != 0 {
		_, err := fmt.Fprintf(out, "%s has project quotas already, in inode %d.\n", devicePath, sb.QuotaInode)
		return err
	}
	_, release, err := fsckFencing(dev, devicePath, true)
	if err != nil {
		return err
	}
	defer release()
	inodes := inode.NewTable(dev, sb.InodeTableOffset, sb.InodeTableSize, nil, [16]byte{})
	blocks := bitmap.NewRegion(dev, sb.BlockBitmapOffset, sb.TotalBlocks, nil, [16]byte{})
	if err := quota.Enable(context.Background(), dev, sb, inodes, blocks); err != nil && !errors.Is(err, quota.ErrEnabled) {
		return fmt.Errorf("creating the quota table on %s: %w", devicePath, err)
	}
	_, err = fmt.Fprintf(out, "Created the quota table of %s in inode %d. Directories can now get project quotas; builds without them refuse the volume.\n",
		devicePath, sb.QuotaInode)
	return err
}
