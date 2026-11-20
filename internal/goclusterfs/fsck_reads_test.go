package goclusterfs

import (
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

func readSuperblock(t *testing.T, fsys *FS) *superblock.Superblock {
	t.Helper()
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	return sb
}

// Over a SAN every read is a round trip, so fsck reads the inode table a megabyte at a time.
func TestFsckReadsTheInodeTableInChunks(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImageOfSize(t, 5<<30))
	for i := range 40 {
		createFile(t, fsys, fmt.Sprintf("f%d", i))
	}
	sb := readSuperblock(t, fsys)
	var reads atomic.Int64
	fsys.dev.SetReadFault(func(off int64) error {
		if uint64(off) >= sb.InodeTableOffset && uint64(off) < sb.InodeTableOffset+sb.InodeTableSize {
			reads.Add(1)
		}
		return nil
	})

	if r := runFsck(t, fsys, fsck.DefaultOptions()); len(r.Problems) > 0 {
		t.Fatalf("fsck:%s", problemList(r))
	}
	chunks := (sb.InodeTableSize + 1<<20 - 1) >> 20
	if chunks < 2 {
		t.Fatalf("the inode table is %d bytes, want more than one chunk", sb.InodeTableSize)
	}
	// Phases 3, 4 and 6 scan the table, and phase 8 rereads the files for their link counts.
	if got, want := reads.Load(), int64(4*chunks); got > want {
		t.Errorf("fsck made %d reads in the inode table, want at most %d", got, want)
	}
}

// A chunk that cannot be read in one piece is read sector by sector, so fsck reports only the bad sector.
func TestFsckReportsOnlyTheUnreadableInodeSector(t *testing.T) {
	fsys := openFormattedFS(t, newFormattedImage(t))
	for i := range 40 {
		createFile(t, fsys, fmt.Sprintf("f%d", i))
	}
	bad := int64(readSuperblock(t, fsys).InodeTableOffset)
	fsys.dev.SetReadFault(func(off int64) error {
		if off == bad {
			return syscall.EIO
		}
		return nil
	})

	r := runFsck(t, fsys, fsck.DefaultOptions())
	var unreadable []string
	for _, p := range r.Problems {
		if p.Phase == fsck.PhaseInodes && strings.HasPrefix(p.Location, "sector ") {
			unreadable = append(unreadable, p.Location)
		}
	}
	if !slices.Equal(unreadable, []string{"sector 0"}) {
		t.Errorf("unreadable inode table sectors %v, want [sector 0]", unreadable)
	}
	if r.UsedInodes < 40-fsck.InodesPerSector {
		t.Errorf("fsck found %d inodes in use, want the files outside sector 0", r.UsedInodes)
	}
}
