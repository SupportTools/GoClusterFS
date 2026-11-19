package goclusterfs

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/sfb"
)

func inodeSector(num uint32) int64 {
	return treeFSInodeOffset + int64(num/inode.InodesPerSector)*inode.SectorSize
}

func failInodeReadsWhile(f *File, armed func() bool) {
	sector := inodeSector(f.inodeNum)
	f.fs.dev.SetReadFault(func(off int64) error {
		if off == sector && armed() {
			return syscall.EIO
		}
		return nil
	})
}

// The write must fail cleanly, leave the file writable, and not panic in its cleanup.
func expectRereadFailsWithEIO(t *testing.T, f *File) {
	t.Helper()
	data := pattern(9, 4096)
	if _, errno := f.Write(context.Background(), nil, data, 0); errno != syscall.EIO {
		t.Fatalf("write that cannot re-read its inode: errno %v, want EIO", errno)
	}
	f.fs.dev.SetReadFault(func(int64) error { return nil })
	writeAt(t, f, data, 0)
	checkAt(t, f, data, 0)
}

func TestWriteThatCannotRereadItsInodeFailsWithEIO(t *testing.T) {
	t.Run("under the inode lock", func(t *testing.T) {
		fsys := openLockedTreeFS(t, newTreeFSImage(t))
		f := newRegularFile(t, fsys)
		failInodeReadsWhile(f, func() bool {
			return !fsys.lockMgr.HeldSince(lock.LockTypeInode, uint64(f.inodeNum)).IsZero()
		})
		expectRereadFailsWithEIO(t, f)
	})

	t.Run("after SFB migration", func(t *testing.T) {
		fsys := openTreeFS(t, newTreeFSImage(t))
		f := newSFBFile(t, fsys, newSFBContainer(t, fsys, sfb.Tier16KB), sfb.Tier16KB, 1, pattern(5, 10000))
		var migrated atomic.Bool
		sector := inodeSector(f.inodeNum)
		fsys.dev.SetWriteObserver(func(off int64, n int) {
			if off <= sector && sector < off+int64(n) {
				migrated.Store(true)
			}
		})
		failInodeReadsWhile(f, migrated.Load)
		expectRereadFailsWithEIO(t, f)
	})
}
