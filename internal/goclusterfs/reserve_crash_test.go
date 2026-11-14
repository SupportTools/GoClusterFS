package goclusterfs

import (
	"context"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
)

// crashWithReserve makes a node reserve three blocks, map one into a file after
// logging it used, and stop without returning the other two, as a crash would.
func crashWithReserve(t *testing.T) (path string, offset, size uint64, used uint64, unused []uint64) {
	t.Helper()
	fsys, path, offset, size := journaledFS(t)
	ctx := context.Background()
	blocks, err := fsys.bitmapMgr.Alloc(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	commit := func(log func(*journal.Transaction) error) {
		txn := fsys.journal.Begin()
		if err := log(txn); err != nil {
			t.Fatal(err)
		}
		if _, err := fsys.journal.Commit(txn); err != nil {
			t.Fatal(err)
		}
	}
	commit(func(txn *journal.Transaction) error { return txn.LogBlockReserve(blocks) })
	commit(func(txn *journal.Transaction) error { return txn.LogBlockUse(blocks[:1]) })
	f := createFile(t, fsys, "used")
	if _, err := fsys.updateInode(ctx, f.inodeNum, func(ino *inode.Inode) error {
		ino.Size = BlockSize
		return fsys.extentMgr.Insert(ctx, ino, &extent.Extent{LogicalBlock: 0, PhysicalBlock: blocks[0], Length: 1, Flags: extent.FlagAllocated})
	}); err != nil {
		t.Fatal(err)
	}
	fsys.snapshotWkr.Stop()
	return path, offset, size, blocks[0], blocks[1:]
}

func allocated(t *testing.T, fsys *FS, b uint64) bool {
	t.Helper()
	ok, err := fsys.bitmapMgr.IsAllocated(b)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// fsck reports the blocks a crashed node reserved and never used as reserved,
// not as leaks, and finds nothing wrong.
func TestFsckReportsCrashedNodesReserve(t *testing.T) {
	path, _, _, _, unused := crashWithReserve(t)
	r := runFsck(t, openFormattedFS(t, path), fsck.DefaultOptions())
	if r.ExitCode() != fsck.ExitClean || strings.Contains(problemList(r), "leaked") ||
		!strings.Contains(problemList(r), "2 blocks reserved by node 0 and never used") {
		t.Fatalf("fsck exit %d, want the 2 unused reserved blocks %v reported as reserved:%s", r.ExitCode(), unused, problemList(r))
	}
}

// Replaying a crashed node's journal frees what it reserved and never used, and
// keeps the reserved block it used.
func TestReplayFreesCrashedNodesUnusedReserve(t *testing.T) {
	path, offset, size, used, unused := crashWithReserve(t)
	other := openFormattedFS(t, path)
	replayCrashedJournal(t, other, offset, size)
	for _, b := range unused {
		if allocated(t, other, b) {
			t.Errorf("unused reserved block %d still allocated after the replay", b)
		}
	}
	if !allocated(t, other, used) {
		t.Errorf("reserved block %d the file maps was freed by the replay", used)
	}
	expectCleanFsck(t, other)
}

// fsck -repair frees a crashed node's unused reserve and marks it done in the
// node's journal, so a later replay leaves the blocks alone even after another
// node allocated them.
func TestFsckRepairFreesCrashedNodesReserveOnce(t *testing.T) {
	path, offset, size, used, unused := crashWithReserve(t)
	repair := fsck.DefaultOptions()
	repair.Mode, repair.AllowUncleanNodes = fsck.Repair, true
	if r := runFsck(t, openFormattedFS(t, path), repair); r.RepairedCount == 0 {
		t.Fatalf("repair freed nothing:%s", problemList(r))
	}
	other := openFormattedFS(t, path)
	for _, b := range unused {
		if allocated(t, other, b) {
			t.Fatalf("unused reserved block %d still allocated after repair", b)
		}
	}
	taken, err := other.bitmapMgr.Alloc(context.Background(), len(unused))
	if err != nil {
		t.Fatal(err)
	}
	replayCrashedJournal(t, other, offset, size)
	for _, b := range append(taken, used) {
		if !allocated(t, other, b) {
			t.Errorf("block %d was freed by the replay after repair handled the reserve", b)
		}
	}
}
