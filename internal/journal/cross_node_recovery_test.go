// Package journal provides journaling support for GoClusterFS.
// This file contains tests for the cross-node recovery module.
package journal

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// testRecoveryContext creates a RecoveryContext with real components for testing.
func testRecoveryContext(t testing.TB) (*RecoveryContext, func()) {
	t.Helper()

	// Create a temp file with size 10MB
	f, err := os.CreateTemp("", "recovery-test-*.img")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	testFile := f.Name()
	if err := f.Truncate(10 * 1024 * 1024); err != nil {
		_ = f.Close()
		_ = os.Remove(testFile)
		t.Fatalf("Failed to truncate: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Layout: 0-1MB reserved, 1-2MB bitmap, 2-3MB inodes, 3MB+ data
	const (
		bitmapOffset = 1 * 1024 * 1024
		inodeOffset  = 2 * 1024 * 1024
		dataOffset   = 3 * 1024 * 1024
		totalBlocks  = 7 // 7MB of data blocks
		inodeCount   = 100
	)

	dev, err := storage.Open(testFile)
	if err != nil {
		_ = os.Remove(testFile)
		t.Skipf("O_DIRECT not supported: %v", err)
	}

	nodeUUID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	bm := bitmap.NewRegion(dev, bitmapOffset, totalBlocks, nil, nodeUUID)
	inodeTbl := inode.NewTable(dev, inodeOffset, inodeCount*inode.InodeSize, nil, nodeUUID)
	extentMgr := extent.NewManager(dev, bm, nil, dataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	dirMgr := dir.NewManager(dev, inodeTbl, extentMgr, nil, dataOffset)

	rctx := &RecoveryContext{
		Dev:       dev,
		InodeTbl:  inodeTbl,
		BitmapMgr: bm,
		DirMgr:    dirMgr,
		ExtentMgr: extentMgr,
	}

	cleanup := func() {
		_ = dev.Close()
		_ = os.Remove(testFile)
	}

	return rctx, cleanup
}

// TestApplyInodeCreate tests creating an inode during recovery.
func TestApplyInodeCreate(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	r := newTxnReplay()

	// Create a payload for inode creation
	// Note: Use inode number < 100 since test context only creates 100 inodes
	payload := &InodeCreatePayload{
		InodeNum: 50,
		Mode:     0644,
		UID:      1000,
		GID:      1000,
	}

	// Serialize and apply
	serialized := payload.Serialize()
	applied, err := applyInodeCreate(ctx, rctx, r, serialized)
	if err != nil {
		t.Fatalf("applyInodeCreate failed: %v", err)
	}
	if !applied {
		t.Error("applyInodeCreate returned false, expected true for new inode")
	}

	// Verify the inode was created
	ino, err := rctx.InodeTbl.Read(uint32(payload.InodeNum))
	if err != nil {
		t.Fatalf("Failed to read created inode: %v", err)
	}
	if ino.Magic != inode.InodeMagic {
		t.Error("Created inode has invalid magic")
	}
	if ino.Mode != uint16(payload.Mode) {
		t.Errorf("Inode mode mismatch: got %o, want %o", ino.Mode, payload.Mode)
	}
	if ino.UID != payload.UID {
		t.Errorf("Inode UID mismatch: got %d, want %d", ino.UID, payload.UID)
	}

	// Test idempotency - applying same operation should skip
	applied, err = applyInodeCreate(ctx, rctx, r, serialized)
	if err != nil {
		t.Fatalf("applyInodeCreate (idempotent) failed: %v", err)
	}
	if applied {
		t.Error("applyInodeCreate returned true on second call, expected false (idempotent)")
	}
}

// TestApplyInodeUpdate tests updating an inode during recovery.
// Note: InodeUpdatePayload only contains header (InodeNum, UpdateMask), not actual values.
// The implementation skips these as the values should already be written to disk.
func TestApplyInodeUpdate(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	r := newTxnReplay()

	// First, create an inode to update
	// Note: Use inode number < 100 since test context only creates 100 inodes
	createPayload := &InodeCreatePayload{
		InodeNum: 51,
		Mode:     0644,
		UID:      1000,
		GID:      1000,
	}
	if _, err := applyInodeCreate(ctx, rctx, r, createPayload.Serialize()); err != nil {
		t.Fatalf("Setup: applyInodeCreate failed: %v", err)
	}

	// InodeUpdatePayload only contains header, not actual values
	updatePayload := &InodeUpdatePayload{
		InodeNum:   51,
		UpdateMask: InodeUpdateMode | InodeUpdateSize, // Example update mask
	}

	// The implementation skips InodeUpdate since payload lacks actual values
	applied, err := applyInodeUpdate(ctx, rctx, updatePayload.Serialize())
	if err != nil {
		t.Fatalf("applyInodeUpdate failed: %v", err)
	}
	// Expected to return false since implementation skips header-only updates
	if applied {
		t.Error("applyInodeUpdate returned true, expected false (header-only update)")
	}
}

// TestApplyInodeDelete tests deleting an inode during recovery.
func TestApplyInodeDelete(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	r := newTxnReplay()

	// First, create an inode to delete
	// Note: Use inode number < 100 since test context only creates 100 inodes
	createPayload := &InodeCreatePayload{
		InodeNum: 52,
		Mode:     0644,
		UID:      1000,
		GID:      1000,
	}
	if _, err := applyInodeCreate(ctx, rctx, r, createPayload.Serialize()); err != nil {
		t.Fatalf("Setup: applyInodeCreate failed: %v", err)
	}

	created, err := rctx.InodeTbl.Read(52)
	if err != nil {
		t.Fatal(err)
	}
	deletePayload := &InodeDeletePayload{InodeNum: 52, MetaGeneration: created.MetaGeneration}

	applied, err := applyInodeDelete(ctx, rctx, r, deletePayload.Serialize())
	if err != nil {
		t.Fatalf("applyInodeDelete failed: %v", err)
	}
	if !applied {
		t.Error("applyInodeDelete returned false, expected true")
	}

	// Verify the inode was deleted (magic should be cleared)
	ino, err := rctx.InodeTbl.Read(uint32(deletePayload.InodeNum))
	if err != nil {
		t.Fatalf("Failed to read deleted inode: %v", err)
	}
	if ino.Magic == inode.InodeMagic {
		t.Error("Deleted inode still has valid magic")
	}

	// Test idempotency - deleting again should skip
	applied, err = applyInodeDelete(ctx, rctx, r, deletePayload.Serialize())
	if err != nil {
		t.Fatalf("applyInodeDelete (idempotent) failed: %v", err)
	}
	if applied {
		t.Error("applyInodeDelete returned true on second call, expected false (idempotent)")
	}
}

// TestApplyBlockAlloc tests allocating a block during recovery.
func TestApplyBlockAlloc(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()

	// Allocate block 5
	payload := &BlockAllocPayload{
		InodeNum:   100,
		StartBlock: 5,
		Count:      1,
		Purpose:    BlockPurposeData,
	}

	applied, err := applyBlockAlloc(ctx, rctx, payload.Serialize())
	if err != nil {
		t.Fatalf("applyBlockAlloc failed: %v", err)
	}
	if !applied {
		t.Error("applyBlockAlloc returned false, expected true")
	}

	// Verify the block is marked as allocated
	isAlloc, err := rctx.BitmapMgr.IsAllocated(payload.StartBlock)
	if err != nil {
		t.Fatalf("Failed to check block allocation: %v", err)
	}
	if !isAlloc {
		t.Error("Block should be marked as allocated")
	}

	// Test idempotency - allocating again should skip
	applied, err = applyBlockAlloc(ctx, rctx, payload.Serialize())
	if err != nil {
		t.Fatalf("applyBlockAlloc (idempotent) failed: %v", err)
	}
	if applied {
		t.Error("applyBlockAlloc returned true on second call, expected false (idempotent)")
	}
}

// Replay of a free leaves the block allocated: the payload names a file block, and that number may be another file's.
func TestApplyBlockFreeLeavesBlocksAllocated(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()

	allocPayload := &BlockAllocPayload{InodeNum: 100, StartBlock: 6, Count: 1, Purpose: BlockPurposeData}
	if _, err := applyBlockAlloc(ctx, rctx, allocPayload.Serialize()); err != nil {
		t.Fatalf("Setup: applyBlockAlloc failed: %v", err)
	}
	freePayload := &BlockFreePayload{InodeNum: 100, StartBlock: 6, Count: 1}
	if applied, err := applyBlockFree(ctx, rctx, freePayload.Serialize()); err != nil || applied {
		t.Fatalf("applyBlockFree = %v, %v; want false, nil", applied, err)
	}
	if isAlloc, err := rctx.BitmapMgr.IsAllocated(6); err != nil || !isAlloc {
		t.Errorf("block 6 allocated = %v, %v; want it left allocated", isAlloc, err)
	}
}

// TestApplyDirAddEntry tests adding a directory entry during recovery.
func TestApplyDirAddEntry(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	r := newTxnReplay()

	// Create a parent directory inode
	parentPayload := &InodeCreatePayload{
		InodeNum: uint64(inode.InodeRoot),
		Mode:     uint32(inode.S_IFDIR | 0755),
		UID:      0,
		GID:      0,
	}
	if _, err := applyInodeCreate(ctx, rctx, r, parentPayload.Serialize()); err != nil {
		t.Fatalf("Setup: failed to create parent inode: %v", err)
	}

	// Update link count for parent
	parentIno, err := rctx.InodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatalf("Setup: failed to read parent inode: %v", err)
	}
	parentIno.LinkCount = 2
	if err := rctx.InodeTbl.Write(ctx, parentIno); err != nil {
		t.Fatalf("Setup: failed to write parent inode: %v", err)
	}

	// Initialize the directory structure
	parentIno, _ = rctx.InodeTbl.Read(inode.InodeRoot)
	if err := rctx.DirMgr.InitDirectory(ctx, parentIno, uint64(inode.InodeRoot)); err != nil {
		t.Fatalf("Setup: failed to init directory: %v", err)
	}

	// Create a child inode
	childPayload := &InodeCreatePayload{
		InodeNum: 10,
		Mode:     0644,
		UID:      1000,
		GID:      1000,
	}
	if _, err := applyInodeCreate(ctx, rctx, r, childPayload.Serialize()); err != nil {
		t.Fatalf("Setup: failed to create child inode: %v", err)
	}

	// Add directory entry
	dirPayload := &DirAddEntryPayload{
		ParentInodeNum: uint64(inode.InodeRoot),
		ChildInodeNum:  10,
		EntryType:      DirEntryRegular,
		Name:           "testfile.txt",
	}

	applied, err := applyDirAddEntry(ctx, rctx, r, dirPayload.Serialize())
	if err != nil {
		t.Fatalf("applyDirAddEntry failed: %v", err)
	}
	if !applied {
		t.Error("applyDirAddEntry returned false, expected true")
	}

	// Verify the entry was added
	parentIno, _ = rctx.InodeTbl.Read(inode.InodeRoot)
	childNum, _, err := rctx.DirMgr.Lookup(ctx, parentIno, "testfile.txt")
	if err != nil {
		t.Fatalf("Failed to lookup added entry: %v", err)
	}
	if childNum != 10 {
		t.Errorf("Entry inode mismatch: got %d, want 10", childNum)
	}

	// Test idempotency - adding again should skip
	applied, err = applyDirAddEntry(ctx, rctx, r, dirPayload.Serialize())
	if err != nil {
		t.Fatalf("applyDirAddEntry (idempotent) failed: %v", err)
	}
	if applied {
		t.Error("applyDirAddEntry returned true on second call, expected false (idempotent)")
	}
}

// TestApplyDirRemoveEntry tests removing a directory entry during recovery.
func TestApplyDirRemoveEntry(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	r := newTxnReplay()

	// Setup: Create parent dir, child inode, and add entry
	parentPayload := &InodeCreatePayload{
		InodeNum: uint64(inode.InodeRoot),
		Mode:     uint32(inode.S_IFDIR | 0755),
	}
	if _, err := applyInodeCreate(ctx, rctx, r, parentPayload.Serialize()); err != nil {
		t.Fatalf("Setup: failed to create parent inode: %v", err)
	}
	parentIno, _ := rctx.InodeTbl.Read(inode.InodeRoot)
	parentIno.LinkCount = 2
	if err := rctx.InodeTbl.Write(ctx, parentIno); err != nil {
		t.Fatal(err)
	}
	parentIno, _ = rctx.InodeTbl.Read(inode.InodeRoot)
	if err := rctx.DirMgr.InitDirectory(ctx, parentIno, uint64(inode.InodeRoot)); err != nil {
		t.Fatalf("Setup: failed to init directory: %v", err)
	}

	childPayload := &InodeCreatePayload{InodeNum: 20, Mode: 0644}
	if _, err := applyInodeCreate(ctx, rctx, r, childPayload.Serialize()); err != nil {
		t.Fatalf("Setup: failed to create child inode: %v", err)
	}

	addPayload := &DirAddEntryPayload{
		ParentInodeNum: uint64(inode.InodeRoot),
		ChildInodeNum:  20,
		EntryType:      DirEntryRegular,
		Name:           "toremove.txt",
	}
	if _, err := applyDirAddEntry(ctx, rctx, r, addPayload.Serialize()); err != nil {
		t.Fatalf("Setup: failed to add entry: %v", err)
	}

	// Now remove the entry
	child, err := rctx.InodeTbl.Read(20)
	if err != nil {
		t.Fatal(err)
	}
	removePayload := &DirRemoveEntryPayload{
		ParentInodeNum:      uint64(inode.InodeRoot),
		ChildInodeNum:       20,
		Name:                "toremove.txt",
		ChildMetaGeneration: child.MetaGeneration,
	}

	applied, err := applyDirRemoveEntry(ctx, rctx, removePayload.Serialize())
	if err != nil {
		t.Fatalf("applyDirRemoveEntry failed: %v", err)
	}
	if !applied {
		t.Error("applyDirRemoveEntry returned false, expected true")
	}

	// Verify the entry was removed
	parentIno, _ = rctx.InodeTbl.Read(inode.InodeRoot)
	_, _, err = rctx.DirMgr.Lookup(ctx, parentIno, "toremove.txt")
	if err != dir.ErrNotFound {
		t.Errorf("Entry should be removed, but Lookup returned err=%v", err)
	}

	// Test idempotency - removing again should skip
	applied, err = applyDirRemoveEntry(ctx, rctx, removePayload.Serialize())
	if err != nil {
		t.Fatalf("applyDirRemoveEntry (idempotent) failed: %v", err)
	}
	if applied {
		t.Error("applyDirRemoveEntry returned true on second call, expected false (idempotent)")
	}
}

// TestApplyExtentAdd tests adding an extent during recovery.
func TestApplyExtentAdd(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	r := newTxnReplay()

	// Create an inode to attach the extent to
	inodePayload := &InodeCreatePayload{
		InodeNum: 50,
		Mode:     0644,
	}
	if _, err := applyInodeCreate(ctx, rctx, r, inodePayload.Serialize()); err != nil {
		t.Fatalf("Setup: failed to create inode: %v", err)
	}

	// First allocate the block that will be used by the extent
	allocPayload := &BlockAllocPayload{
		InodeNum:   50,
		StartBlock: 1,
		Count:      1,
		Purpose:    BlockPurposeData,
	}
	if _, err := applyBlockAlloc(ctx, rctx, allocPayload.Serialize()); err != nil {
		t.Fatalf("Setup: failed to allocate block: %v", err)
	}

	created, err := rctx.InodeTbl.Read(50)
	if err != nil {
		t.Fatal(err)
	}
	extentPayload := &ExtentAddPayload{
		InodeNum:       50,
		FileBlock:      0,
		DiskBlock:      1,
		Length:         1,
		MetaGeneration: created.MetaGeneration,
		Size:           4096,
	}

	applied, err := applyExtentAdd(ctx, rctx, r, extentPayload.Serialize())
	if err != nil {
		t.Fatalf("applyExtentAdd failed: %v", err)
	}
	if !applied {
		t.Error("applyExtentAdd returned false, expected true")
	}

	// Verify the extent was added
	ino, _ := rctx.InodeTbl.Read(50)
	ext, _, err := rctx.ExtentMgr.Lookup(ino, 0)
	if err != nil {
		t.Fatalf("Failed to lookup extent: %v", err)
	}
	if ext.PhysicalBlock != 1 {
		t.Errorf("Extent physical block mismatch: got %d, want 1", ext.PhysicalBlock)
	}

	if ino.Size != 4096 {
		t.Errorf("size after replay = %d, want 4096", ino.Size)
	}

	// A second replay finds the inode written since the write was logged.
	applied, err = applyExtentAdd(ctx, rctx, newTxnReplay(), extentPayload.Serialize())
	if err != nil {
		t.Fatalf("applyExtentAdd (idempotent) failed: %v", err)
	}
	if applied {
		t.Error("applyExtentAdd returned true on second call, expected false (idempotent)")
	}
}

// TestCreateRecoveryHandler tests the RecoveryHandler creation.
// TestApplyExtentAddIntoTree replays an ExtentAdd whose inode write was lost in
// a crash, for a file whose extents already live in an extent tree.
func TestApplyExtentAddIntoTree(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	r := newTxnReplay()

	if _, err := applyInodeCreate(ctx, rctx, r, (&InodeCreatePayload{InodeNum: 51, Mode: 0644}).Serialize()); err != nil {
		t.Fatalf("Setup: failed to create inode: %v", err)
	}
	ino, err := rctx.InodeTbl.Read(51)
	if err != nil {
		t.Fatal(err)
	}
	for _, lb := range []uint64{0, 2, 4} {
		if _, err := rctx.ExtentMgr.AllocateBlocks(ctx, ino, lb, 1); err != nil {
			t.Fatalf("Setup: allocate block %d: %v", lb, err)
		}
	}
	if err := rctx.InodeTbl.Write(ctx, ino); err != nil {
		t.Fatalf("Setup: write inode: %v", err)
	}
	if !ino.HasExtentTree() {
		t.Fatal("Setup: inode should use an extent tree")
	}

	// The crashed write had allocated and journaled this block but never wrote the inode.
	lost, err := rctx.ExtentMgr.AllocatePhysicalBlock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload := (&ExtentAddPayload{InodeNum: 51, FileBlock: 6, DiskBlock: lost, Length: 1, MetaGeneration: ino.MetaGeneration}).Serialize()
	for attempt, wantApplied := range []bool{true, false} {
		applied, err := applyExtentAdd(ctx, rctx, newTxnReplay(), payload)
		if err != nil {
			t.Fatalf("replay %d: %v", attempt, err)
		}
		if applied != wantApplied {
			t.Fatalf("replay %d: applied=%v, want %v", attempt, applied, wantApplied)
		}
	}

	// Read back through a fresh extent manager, as after a remount.
	mgr := extent.NewManager(rctx.Dev, rctx.BitmapMgr, nil, 3<<20)
	ino, err = rctx.InodeTbl.Read(51)
	if err != nil {
		t.Fatal(err)
	}
	if ino.ExtentCount != 4 || !ino.HasExtentTree() {
		t.Fatalf("want 4 extents in a tree, got count=%d tree=%v", ino.ExtentCount, ino.HasExtentTree())
	}
	if phys, err := mgr.LookupPhysical(ino, 6); err != nil || phys != lost {
		t.Fatalf("replayed block: got %d, %v; want %d", phys, err, lost)
	}
	for _, lb := range []uint64{0, 2, 4} {
		if _, _, err := mgr.Lookup(ino, lb); err != nil {
			t.Fatalf("block %d lost after replay: %v", lb, err)
		}
	}
}

func TestCreateRecoveryHandler(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()

	handler := CreateRecoveryHandler(rctx)
	if handler == nil {
		t.Fatal("CreateRecoveryHandler returned nil")
	}
}

// TestRecoveryResult tests the RecoveryResult structure.
func TestRecoveryResult(t *testing.T) {
	result := &RecoveryResult{
		TxnReplayed:     5,
		EntriesReplayed: 10,
		EntriesSkipped:  3,
	}

	if result.TxnReplayed != 5 {
		t.Errorf("TxnReplayed mismatch: got %d, want 5", result.TxnReplayed)
	}
	if result.EntriesReplayed != 10 {
		t.Errorf("EntriesReplayed mismatch: got %d, want 10", result.EntriesReplayed)
	}
	if result.EntriesSkipped != 3 {
		t.Errorf("EntriesSkipped mismatch: got %d, want 3", result.EntriesSkipped)
	}
}

func TestReplayJournalNeverInitializedAndDamaged(t *testing.T) {
	dev, cleanup := createTestDevice(t, 2*DefaultJournalSlotSize)
	defer cleanup()
	ctx := context.Background()
	rctx := &RecoveryContext{Dev: dev}

	fresh := &node.FailedNodeInfo{JournalOffset: 0, JournalSize: DefaultJournalSlotSize}
	if _, err := ReplayJournal(ctx, rctx, fresh); err != nil {
		t.Errorf("ReplayJournal on a never-initialized slot: %v", err)
	}

	damaged := &node.FailedNodeInfo{JournalOffset: DefaultJournalSlotSize, JournalSize: DefaultJournalSlotSize}
	if _, err := InitRegion(dev, damaged.JournalOffset, damaged.JournalSize, [16]byte{2}, 1); err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}
	damageJournalHeader(t, dev, int64(damaged.JournalOffset), func(h []byte) { h[0] = 'X' })
	if _, err := ReplayJournal(ctx, rctx, damaged); !errors.Is(err, ErrInvalidJournalMagic) {
		t.Errorf("ReplayJournal on a slot with a damaged header: error %v, want %v", err, ErrInvalidJournalMagic)
	}
}

// newDataInode claims slot num for a file that maps file block i to disk block blocks[i], and returns it.
func newDataInode(t *testing.T, rctx *RecoveryContext, num uint32, blocks ...uint64) *inode.Inode {
	t.Helper()
	ctx := context.Background()
	ino, err := rctx.InodeTbl.ClaimSlot(ctx, num)
	if err != nil {
		t.Fatal(err)
	}
	ino.Mode = inode.S_IFREG | 0644
	for i, b := range blocks {
		if err := rctx.BitmapMgr.MarkAllocated(ctx, b); err != nil {
			t.Fatal(err)
		}
		if err := rctx.ExtentMgr.AddExtentForRecovery(ctx, ino, &extent.Extent{LogicalBlock: uint64(i), PhysicalBlock: b, Length: 1, Flags: extent.FlagAllocated}); err != nil {
			t.Fatal(err)
		}
	}
	ino.Size = uint64(len(blocks)) << 20
	if err := rctx.InodeTbl.Write(ctx, ino); err != nil {
		t.Fatal(err)
	}
	return ino
}

// Replay cuts the inode and frees its blocks past the logged size only while the inode is unwritten since the log.
func TestApplyExtentFreeCutsUnchangedInode(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	ino := newDataInode(t, rctx, 50, 1, 2, 3)

	stale := &ExtentFreePayload{InodeNum: 50, MetaGeneration: ino.MetaGeneration - 1, Size: 1 << 20}
	if applied, err := applyExtentFree(ctx, rctx, newTxnReplay(), stale.Serialize()); err != nil || applied {
		t.Fatalf("applyExtentFree logged before a later inode write = %v, %v; want false, nil", applied, err)
	}
	free := (&ExtentFreePayload{InodeNum: 50, MetaGeneration: ino.MetaGeneration, Size: 1 << 20}).Serialize()
	if applied, err := applyExtentFree(ctx, rctx, newTxnReplay(), free); err != nil || !applied {
		t.Fatalf("applyExtentFree = %v, %v; want true, nil", applied, err)
	}
	if applied, err := applyExtentFree(ctx, rctx, newTxnReplay(), free); err != nil || applied {
		t.Fatalf("second applyExtentFree = %v, %v; want false, nil", applied, err)
	}

	ino, err := rctx.InodeTbl.Read(50)
	if err != nil {
		t.Fatal(err)
	}
	if ino.Size != 1<<20 {
		t.Errorf("size after the replayed free = %d, want %d", ino.Size, 1<<20)
	}
	if got, err := rctx.ExtentMgr.LookupPhysical(ino, 0); err != nil || got != 1 {
		t.Errorf("file block 0 maps %d (err %v), want disk block 1", got, err)
	}
	for disk, want := range map[uint64]bool{1: true, 2: false, 3: false} {
		if got, err := rctx.BitmapMgr.IsAllocated(disk); err != nil || got != want {
			t.Errorf("disk block %d allocated = %v (err %v), want %v", disk, got, err, want)
		}
	}
}

func TestExtentFreePayloadRoundTrip(t *testing.T) {
	p := &ExtentFreePayload{InodeNum: 7, MetaGeneration: 99, Size: 4096}
	got, err := DeserializeExtentFree(p.Serialize())
	if err != nil || *got != *p {
		t.Fatalf("round trip = %+v, %v; want %+v", got, err, p)
	}
	listed := append(p.Serialize(), make([]byte, 48)...)
	if got, err := DeserializeExtentFree(listed); err != nil || *got != *p {
		t.Fatalf("record listing extents = %+v, %v; want %+v", got, err, p)
	}
}

// Records written before this build carry no generations; DeserializeX reads them as generation 0.
func TestPayloadsWithoutGenerations(t *testing.T) {
	create := (&InodeCreatePayload{InodeNum: 9, MetaGeneration: 5}).Serialize()
	if p, err := DeserializeInodeCreate(create[:40]); err != nil || p.InodeNum != 9 || p.MetaGeneration != 0 {
		t.Errorf("old InodeCreate = %+v, %v", p, err)
	}
	del := (&InodeDeletePayload{InodeNum: 9, MetaGeneration: 5}).Serialize()
	if p, err := DeserializeInodeDelete(del[:8]); err != nil || p.InodeNum != 9 || p.MetaGeneration != 0 {
		t.Errorf("old InodeDelete = %+v, %v", p, err)
	}
	remove := (&DirRemoveEntryPayload{ChildInodeNum: 9, Name: "abcde", ChildMetaGeneration: 5}).Serialize()
	if p, err := DeserializeDirRemoveEntry(remove); err != nil || p.Name != "abcde" || p.ChildMetaGeneration != 5 {
		t.Errorf("DirRemoveEntry round trip = %+v, %v", p, err)
	}
	if p, err := DeserializeDirRemoveEntry(remove[:len(remove)-8]); err != nil || p.Name != "abcde" || p.ChildMetaGeneration != 0 {
		t.Errorf("old DirRemoveEntry = %+v, %v", p, err)
	}
	add := (&ExtentAddPayload{InodeNum: 9, Length: 1, MetaGeneration: 5, Size: 7}).Serialize()
	if p, err := DeserializeExtentAdd(add); err != nil || p.MetaGeneration != 5 || p.Size != 7 {
		t.Errorf("ExtentAdd round trip = %+v, %v", p, err)
	}
	if p, err := DeserializeExtentAdd(add[:32]); err != nil || p.Length != 1 || p.MetaGeneration != 0 || p.Size != 0 {
		t.Errorf("old ExtentAdd = %+v, %v", p, err)
	}
}

func recoveredTxn(records ...interface {
	Serialize() []byte
}) *RecoveredTxn {
	txn := &RecoveredTxn{}
	for _, rec := range records {
		var op TxnOpType
		switch rec.(type) {
		case *InodeCreatePayload:
			op = TxnOpInodeCreate
		case *InodeDeletePayload:
			op = TxnOpInodeDelete
		case *DirAddEntryPayload:
			op = TxnOpDirAddEntry
		case *DirRemoveEntryPayload:
			op = TxnOpDirRemoveEntry
		case *ExtentAddPayload:
			op = TxnOpExtentAdd
		case *ExtentFreePayload:
			op = TxnOpExtentFree
		}
		txn.Entries = append(txn.Entries, &RecoveredEntry{Entry: &TxnEntry{OpType: op}, Payload: rec.Serialize()})
	}
	return txn
}

// newRootDir makes inode 2 an empty directory.
func newRootDir(t *testing.T, rctx *RecoveryContext) {
	t.Helper()
	ctx := context.Background()
	if _, err := applyInodeCreate(ctx, rctx, newTxnReplay(), (&InodeCreatePayload{InodeNum: uint64(inode.InodeRoot), Mode: uint32(inode.S_IFDIR | 0755)}).Serialize()); err != nil {
		t.Fatal(err)
	}
	root, err := rctx.InodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := rctx.DirMgr.InitDirectory(ctx, root, uint64(inode.InodeRoot)); err != nil {
		t.Fatal(err)
	}
}

func lookupRoot(t *testing.T, rctx *RecoveryContext, name string) (uint64, error) {
	t.Helper()
	root, err := rctx.InodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	num, _, err := rctx.DirMgr.Lookup(context.Background(), root, name)
	return num, err
}

// An unlink crashes before freeing its inode, and another node frees the slot and creates a file there: replay must
// not free the new file, though the unlink's records all name its inode number.
func TestReplayedUnlinkKeepsReusedInode(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	newRootDir(t, rctx)
	doomed := newDataInode(t, rctx, 50, 1, 2)
	root, _ := rctx.InodeTbl.Read(inode.InodeRoot)
	if err := rctx.DirMgr.Add(ctx, root, "doomed", 50, dir.DT_REG); err != nil {
		t.Fatal(err)
	}
	unlink := recoveredTxn(
		&DirRemoveEntryPayload{ParentInodeNum: uint64(inode.InodeRoot), ChildInodeNum: 50, Name: "doomed", ChildMetaGeneration: doomed.MetaGeneration},
		&ExtentFreePayload{InodeNum: 50, MetaGeneration: doomed.MetaGeneration},
		&InodeDeletePayload{InodeNum: 50, MetaGeneration: doomed.MetaGeneration},
	)

	// The other node finishes the unlink and creates "reused" in the same slot.
	root, _ = rctx.InodeTbl.Read(inode.InodeRoot)
	if err := rctx.DirMgr.Remove(ctx, root, "doomed"); err != nil {
		t.Fatal(err)
	}
	if err := rctx.InodeTbl.Free(ctx, 50); err != nil {
		t.Fatal(err)
	}
	reused := newDataInode(t, rctx, 50, 3, 4)
	root, _ = rctx.InodeTbl.Read(inode.InodeRoot)
	if err := rctx.DirMgr.Add(ctx, root, "doomed", 50, dir.DT_REG); err != nil {
		t.Fatal(err)
	}

	if _, err := applyTransaction(ctx, rctx, unlink); err != nil {
		t.Fatal(err)
	}
	ino, err := rctx.InodeTbl.Read(50)
	if err != nil || ino.Magic != inode.InodeMagic || ino.MetaGeneration != reused.MetaGeneration || ino.Size != reused.Size {
		t.Fatalf("reused inode after replay: %+v, %v; want it unchanged", ino, err)
	}
	if num, err := lookupRoot(t, rctx, "doomed"); err != nil || num != 50 {
		t.Errorf("entry of the reused inode after replay: %d, %v", num, err)
	}
	for _, b := range []uint64{3, 4} {
		if got, err := rctx.BitmapMgr.IsAllocated(b); err != nil || !got {
			t.Errorf("block %d of the reused inode freed by replay (err %v)", b, err)
		}
	}
}

// An unlink crashes after its commit, and replay applies all of it: each record after the first accepts the inode
// generation the replay itself wrote.
func TestReplayedUnlinkFreesUnchangedInode(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	newRootDir(t, rctx)
	doomed := newDataInode(t, rctx, 50, 1, 2)
	root, _ := rctx.InodeTbl.Read(inode.InodeRoot)
	if err := rctx.DirMgr.Add(ctx, root, "doomed", 50, dir.DT_REG); err != nil {
		t.Fatal(err)
	}
	unlink := recoveredTxn(
		&DirRemoveEntryPayload{ParentInodeNum: uint64(inode.InodeRoot), ChildInodeNum: 50, Name: "doomed", ChildMetaGeneration: doomed.MetaGeneration},
		&ExtentFreePayload{InodeNum: 50, MetaGeneration: doomed.MetaGeneration},
		&InodeDeletePayload{InodeNum: 50, MetaGeneration: doomed.MetaGeneration},
	)

	result, err := applyTransaction(ctx, rctx, unlink)
	if err != nil || result.applied != 3 {
		t.Fatalf("replay = %+v, %v; want 3 records applied", result, err)
	}
	if ino, err := rctx.InodeTbl.Read(50); err != nil || ino.Magic != 0 {
		t.Errorf("inode 50 after replay: magic %#x, err %v; want free", ino.Magic, err)
	}
	if _, err := lookupRoot(t, rctx, "doomed"); !errors.Is(err, dir.ErrNotFound) {
		t.Errorf("lookup after replay: %v, want ErrNotFound", err)
	}
	for _, b := range []uint64{1, 2} {
		if got, err := rctx.BitmapMgr.IsAllocated(b); err != nil || got {
			t.Errorf("block %d still allocated after replay (err %v)", b, err)
		}
	}
}

// A removed entry's name is reused for another file before replay: replay must leave the new entry.
func TestApplyDirRemoveEntryKeepsReusedName(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	newRootDir(t, rctx)
	old := newDataInode(t, rctx, 50)
	remove := (&DirRemoveEntryPayload{ParentInodeNum: uint64(inode.InodeRoot), ChildInodeNum: 50, Name: "name", ChildMetaGeneration: old.MetaGeneration}).Serialize()

	newDataInode(t, rctx, 51)
	root, _ := rctx.InodeTbl.Read(inode.InodeRoot)
	if err := rctx.DirMgr.Add(ctx, root, "name", 51, dir.DT_REG); err != nil {
		t.Fatal(err)
	}
	if applied, err := applyDirRemoveEntry(ctx, rctx, remove); err != nil || applied {
		t.Errorf("replay over another inode's entry = %v, %v; want false, nil", applied, err)
	}

	// The same inode number, reused since.
	root, _ = rctx.InodeTbl.Read(inode.InodeRoot)
	if err := rctx.DirMgr.Remove(ctx, root, "name"); err != nil {
		t.Fatal(err)
	}
	if err := rctx.InodeTbl.Free(ctx, 50); err != nil {
		t.Fatal(err)
	}
	newDataInode(t, rctx, 50)
	root, _ = rctx.InodeTbl.Read(inode.InodeRoot)
	if err := rctx.DirMgr.Add(ctx, root, "name", 50, dir.DT_REG); err != nil {
		t.Fatal(err)
	}
	if applied, err := applyDirRemoveEntry(ctx, rctx, remove); err != nil || applied {
		t.Errorf("replay over the reused inode's entry = %v, %v; want false, nil", applied, err)
	}
	if num, err := lookupRoot(t, rctx, "name"); err != nil || num != 50 {
		t.Errorf("entry after replay: %d, %v", num, err)
	}
}

// A create crashes before claiming its slot, and another node uses the slot: replay must neither create the inode
// nor add an entry for it.
func TestReplayedCreateSkipsUsedSlot(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	newRootDir(t, rctx)
	free, err := rctx.InodeTbl.Read(50)
	if err != nil {
		t.Fatal(err)
	}
	create := func(name string) *RecoveredTxn {
		return recoveredTxn(
			&InodeCreatePayload{InodeNum: 50, Mode: uint32(inode.S_IFREG | 0644), ParentInodeNum: uint64(inode.InodeRoot), MetaGeneration: free.MetaGeneration},
			&DirAddEntryPayload{ParentInodeNum: uint64(inode.InodeRoot), ChildInodeNum: 50, EntryType: DirEntryRegular, Name: name},
		)
	}

	other := newDataInode(t, rctx, 50, 1)
	if _, err := applyTransaction(ctx, rctx, create("lost")); err != nil {
		t.Fatal(err)
	}
	if ino, err := rctx.InodeTbl.Read(50); err != nil || ino.MetaGeneration != other.MetaGeneration {
		t.Errorf("slot 50 after replay: %+v, %v; want the other node's inode unchanged", ino, err)
	}
	if _, err := lookupRoot(t, rctx, "lost"); !errors.Is(err, dir.ErrNotFound) {
		t.Errorf("replay added an entry for another node's inode: %v", err)
	}

	// The other node's file is gone again: the slot is free, but not as the create saw it.
	if err := rctx.InodeTbl.Free(ctx, 50); err != nil {
		t.Fatal(err)
	}
	if _, err := applyTransaction(ctx, rctx, create("lost")); err != nil {
		t.Fatal(err)
	}
	if ino, err := rctx.InodeTbl.Read(50); err != nil || ino.Magic != 0 {
		t.Errorf("replay recreated inode 50 in a slot used since (magic %#x, err %v)", ino.Magic, err)
	}
	if _, err := lookupRoot(t, rctx, "lost"); !errors.Is(err, dir.ErrNotFound) {
		t.Errorf("replay added an entry for a slot used since: %v", err)
	}
}

func TestReplayedCreateFinishesUnclaimedSlot(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	newRootDir(t, rctx)
	free, err := rctx.InodeTbl.Read(50)
	if err != nil {
		t.Fatal(err)
	}
	create := recoveredTxn(
		&InodeCreatePayload{InodeNum: 50, Mode: uint32(inode.S_IFREG | 0644), ParentInodeNum: uint64(inode.InodeRoot), MetaGeneration: free.MetaGeneration},
		&DirAddEntryPayload{ParentInodeNum: uint64(inode.InodeRoot), ChildInodeNum: 50, EntryType: DirEntryRegular, Name: "made"},
	)
	if result, err := applyTransaction(ctx, rctx, create); err != nil || result.applied != 2 {
		t.Fatalf("replay = %+v, %v; want 2 records applied", result, err)
	}
	if num, err := lookupRoot(t, rctx, "made"); err != nil || num != 50 {
		t.Errorf("entry after replay: %d, %v", num, err)
	}
	if result, err := applyTransaction(ctx, rctx, create); err != nil || result.applied != 0 {
		t.Errorf("second replay = %+v, %v; want nothing applied", result, err)
	}
}

// A write crashes before its inode write, and another node truncates the file: replay must not map the write's
// blocks again, since the truncate may have let another file take them.
func TestApplyExtentAddSkipsChangedInode(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	ino := newDataInode(t, rctx, 50, 1)
	if err := rctx.BitmapMgr.MarkAllocated(ctx, 5); err != nil {
		t.Fatal(err)
	}
	write := (&ExtentAddPayload{InodeNum: 50, FileBlock: 1, DiskBlock: 5, Length: 1, MetaGeneration: ino.MetaGeneration, Size: 2 << 20}).Serialize()

	if err := rctx.ExtentMgr.Truncate(ctx, ino, 0); err != nil {
		t.Fatal(err)
	}
	if err := rctx.InodeTbl.Write(ctx, ino); err != nil {
		t.Fatal(err)
	}
	if applied, err := applyExtentAdd(ctx, rctx, newTxnReplay(), write); err != nil || applied {
		t.Fatalf("replay after a truncate = %v, %v; want false, nil", applied, err)
	}
	if ino, err := rctx.InodeTbl.Read(50); err != nil || ino.Size != 0 || ino.ExtentCount != 0 {
		t.Errorf("inode after replay: size %d, %d extents (err %v); want empty", ino.Size, ino.ExtentCount, err)
	}
}

// A parent directory removed since the record was logged makes replay skip the record, not fail.
func TestApplyDirEntrySkipsRemovedParent(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()
	r := newTxnReplay()
	free, err := rctx.InodeTbl.Read(51)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyInodeCreate(ctx, rctx, r, (&InodeCreatePayload{InodeNum: 51, Mode: 0644, MetaGeneration: free.MetaGeneration}).Serialize()); err != nil {
		t.Fatal(err)
	}
	add := (&DirAddEntryPayload{ParentInodeNum: 40, ChildInodeNum: 51, Name: "x"}).Serialize()
	if applied, err := applyDirAddEntry(ctx, rctx, r, add); err != nil || applied {
		t.Errorf("DirAddEntry into a free slot = %v, %v; want false, nil", applied, err)
	}
	remove := (&DirRemoveEntryPayload{ParentInodeNum: 40, ChildInodeNum: 51, Name: "x"}).Serialize()
	if applied, err := applyDirRemoveEntry(ctx, rctx, remove); err != nil || applied {
		t.Errorf("DirRemoveEntry from a free slot = %v, %v; want false, nil", applied, err)
	}
}
