package goclusterfs

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/reserve"
)

// expectEmptyOr requires a file written once after it was created to be empty
// or to hold exactly what was written.
func expectEmptyOr(t *testing.T, f *File, data []byte) {
	t.Helper()
	if got := readWhole(t, f); len(got) != 0 && !bytes.Equal(got, data) {
		t.Fatalf("inode %d: %d bytes that are neither empty nor what was written", f.inodeNum, len(got))
	}
}

// expectNoLeaks requires that replay gave back every reserved block nothing used.
func expectNoLeaks(t *testing.T, fsys *FS) {
	t.Helper()
	r := runFsck(t, fsys, fsck.DefaultOptions())
	if strings.Contains(problemList(r), "leaked") {
		t.Errorf("replay left reserved blocks leaked:%s", problemList(r))
	}
}

// Power fails while a write takes the node's first reserve and maps a block from it.
func TestPowerCutDuringReserveRefill(t *testing.T) {
	data := pattern(3, 4096)
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			withReserve(t, fsys, 8, time.Hour)
			f = createFile(t, fsys, "f")
		},
		op: func(fsys *FS, _ *power) {
			_, _ = f.Write(context.Background(), nil, data, 0)
		},
		afterMount: expectNoLeaks,
		check: func(t *testing.T, fsys *FS) {
			expectEmptyOr(t, lookupFile(t, fsys, "f"), data)
		},
	})
}

// Power fails while a write maps a block the node already holds in reserve.
func TestPowerCutDuringWriteFromReserve(t *testing.T) {
	first, second := pattern(4, 4096), pattern(5, 4096)
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			withReserve(t, fsys, 8, time.Hour)
			writeAt(t, createFile(t, fsys, "first"), first, 0)
			f = createFile(t, fsys, "second")
		},
		op: func(fsys *FS, _ *power) {
			_, _ = f.Write(context.Background(), nil, second, 0)
		},
		afterMount: expectNoLeaks,
		check: func(t *testing.T, fsys *FS) {
			checkFile(t, lookupFile(t, fsys, "first"), first)
			expectEmptyOr(t, lookupFile(t, fsys, "second"), second)
		},
	})
}

// Power fails during a write that maps two reserved blocks. The inode is
// written between the two, so the first block's use is logged before that.
func TestPowerCutDuringTwoBlockWriteFromReserve(t *testing.T) {
	data := pattern(6, 8192)
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			withReserve(t, fsys, 8, time.Hour)
			f = createFile(t, fsys, "f")
		},
		op: func(fsys *FS, _ *power) {
			_, _ = f.Write(context.Background(), nil, data, BlockSize-4096)
		},
		afterMount: expectNoLeaks,
		check: func(t *testing.T, fsys *FS) {
			got := readWhole(t, lookupFile(t, fsys, "f"))
			want := append(make([]byte, BlockSize-4096), data...)
			if len(got) != 0 && !bytes.Equal(got, want) {
				t.Fatalf("%d bytes that are neither empty nor what was written", len(got))
			}
		},
	})
}

// Power fails while the node gives its reserve back. Files keep their data,
// and no block is both free and mapped; a cut between ending the record and
// freeing the blocks leaks them, which repair reclaims.
func TestPowerCutDuringReserveReturn(t *testing.T) {
	data := pattern(7, 4096)
	var res *reserve.Manager
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			res = withReserve(t, fsys, 8, time.Hour)
			writeAt(t, createFile(t, fsys, "f"), data, 0)
		},
		op: func(fsys *FS, _ *power) {
			res.ReturnAll(context.Background())
		},
		check: func(t *testing.T, fsys *FS) {
			checkFile(t, lookupFile(t, fsys, "f"), data)
		},
	})
}

// Power fails during a write that maps a reserved block into a hole and then
// copies a block a snapshot shares. The copy writes the inode first, so the
// reserved block's use is logged before that write. A cut between the two
// leaks the reserved block, which repair reclaims.
func TestPowerCutDuringReservedWriteThatCopies(t *testing.T) {
	shared := pattern(8, 4096)
	update := pattern(9, 8192)
	off := int64(BlockSize - 4096)
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			withReserve(t, fsys, 8, time.Hour)
			f = createFile(t, fsys, "f")
			writeAt(t, f, shared, BlockSize)
			setxattr(t, f, "user.snapshot", "s1")
		},
		op: func(fsys *FS, _ *power) {
			_, _ = f.Write(context.Background(), nil, update, off)
		},
		check: func(t *testing.T, fsys *FS) {
			old := append(make([]byte, BlockSize), shared...)
			updated := overwritten(old, int(off), update)
			expectEachBlock(t, lookupFile(t, fsys, "f"), old, updated)
			if !expectSnapshot(t, fsys, "s1", old) {
				t.Fatal("snapshot s1 is gone")
			}
		},
	})
}
