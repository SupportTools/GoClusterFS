// Package journal provides journaling support for GoClusterFS.
// This file contains tests for the cross-node recovery module.
package journal

import (
	"context"
	"os"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
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
	applied, err := applyInodeCreate(ctx, rctx, serialized)
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
	applied, err = applyInodeCreate(ctx, rctx, serialized)
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

	// First, create an inode to update
	// Note: Use inode number < 100 since test context only creates 100 inodes
	createPayload := &InodeCreatePayload{
		InodeNum: 51,
		Mode:     0644,
		UID:      1000,
		GID:      1000,
	}
	if _, err := applyInodeCreate(ctx, rctx, createPayload.Serialize()); err != nil {
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

	// First, create an inode to delete
	// Note: Use inode number < 100 since test context only creates 100 inodes
	createPayload := &InodeCreatePayload{
		InodeNum: 52,
		Mode:     0644,
		UID:      1000,
		GID:      1000,
	}
	if _, err := applyInodeCreate(ctx, rctx, createPayload.Serialize()); err != nil {
		t.Fatalf("Setup: applyInodeCreate failed: %v", err)
	}

	// Delete the inode
	deletePayload := &InodeDeletePayload{
		InodeNum: 52,
	}

	applied, err := applyInodeDelete(ctx, rctx, deletePayload.Serialize())
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
	applied, err = applyInodeDelete(ctx, rctx, deletePayload.Serialize())
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

// TestApplyBlockFree tests freeing a block during recovery.
func TestApplyBlockFree(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()

	// First allocate a block
	allocPayload := &BlockAllocPayload{
		InodeNum:   100,
		StartBlock: 6,
		Count:      1,
		Purpose:    BlockPurposeData,
	}
	if _, err := applyBlockAlloc(ctx, rctx, allocPayload.Serialize()); err != nil {
		t.Fatalf("Setup: applyBlockAlloc failed: %v", err)
	}

	// Free the block
	freePayload := &BlockFreePayload{
		InodeNum:   100,
		StartBlock: 6,
		Count:      1,
	}
	applied, err := applyBlockFree(ctx, rctx, freePayload.Serialize())
	if err != nil {
		t.Fatalf("applyBlockFree failed: %v", err)
	}
	if !applied {
		t.Error("applyBlockFree returned false, expected true")
	}

	// Verify the block is free
	isAlloc, err := rctx.BitmapMgr.IsAllocated(freePayload.StartBlock)
	if err != nil {
		t.Fatalf("Failed to check block allocation: %v", err)
	}
	if isAlloc {
		t.Error("Block should be marked as free")
	}

	// Test idempotency - freeing again should skip
	applied, err = applyBlockFree(ctx, rctx, freePayload.Serialize())
	if err != nil {
		t.Fatalf("applyBlockFree (idempotent) failed: %v", err)
	}
	if applied {
		t.Error("applyBlockFree returned true on second call, expected false (idempotent)")
	}
}

// TestApplyDirAddEntry tests adding a directory entry during recovery.
func TestApplyDirAddEntry(t *testing.T) {
	rctx, cleanup := testRecoveryContext(t)
	defer cleanup()
	ctx := context.Background()

	// Create a parent directory inode
	parentPayload := &InodeCreatePayload{
		InodeNum: uint64(inode.InodeRoot),
		Mode:     uint32(inode.S_IFDIR | 0755),
		UID:      0,
		GID:      0,
	}
	if _, err := applyInodeCreate(ctx, rctx, parentPayload.Serialize()); err != nil {
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
	if _, err := applyInodeCreate(ctx, rctx, childPayload.Serialize()); err != nil {
		t.Fatalf("Setup: failed to create child inode: %v", err)
	}

	// Add directory entry
	dirPayload := &DirAddEntryPayload{
		ParentInodeNum: uint64(inode.InodeRoot),
		ChildInodeNum:  10,
		EntryType:      DirEntryRegular,
		Name:           "testfile.txt",
	}

	applied, err := applyDirAddEntry(ctx, rctx, dirPayload.Serialize())
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
	applied, err = applyDirAddEntry(ctx, rctx, dirPayload.Serialize())
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

	// Setup: Create parent dir, child inode, and add entry
	parentPayload := &InodeCreatePayload{
		InodeNum: uint64(inode.InodeRoot),
		Mode:     uint32(inode.S_IFDIR | 0755),
	}
	if _, err := applyInodeCreate(ctx, rctx, parentPayload.Serialize()); err != nil {
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
	if _, err := applyInodeCreate(ctx, rctx, childPayload.Serialize()); err != nil {
		t.Fatalf("Setup: failed to create child inode: %v", err)
	}

	addPayload := &DirAddEntryPayload{
		ParentInodeNum: uint64(inode.InodeRoot),
		ChildInodeNum:  20,
		EntryType:      DirEntryRegular,
		Name:           "toremove.txt",
	}
	if _, err := applyDirAddEntry(ctx, rctx, addPayload.Serialize()); err != nil {
		t.Fatalf("Setup: failed to add entry: %v", err)
	}

	// Now remove the entry
	removePayload := &DirRemoveEntryPayload{
		ParentInodeNum: uint64(inode.InodeRoot),
		Name:           "toremove.txt",
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

	// Create an inode to attach the extent to
	inodePayload := &InodeCreatePayload{
		InodeNum: 50,
		Mode:     0644,
	}
	if _, err := applyInodeCreate(ctx, rctx, inodePayload.Serialize()); err != nil {
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

	// Add an extent
	extentPayload := &ExtentAddPayload{
		InodeNum:  50,
		FileBlock: 0,
		DiskBlock: 1,
		Length:    1,
	}

	applied, err := applyExtentAdd(ctx, rctx, extentPayload.Serialize())
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

	// Test idempotency - adding again should skip
	applied, err = applyExtentAdd(ctx, rctx, extentPayload.Serialize())
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

	if _, err := applyInodeCreate(ctx, rctx, (&InodeCreatePayload{InodeNum: 51, Mode: 0644}).Serialize()); err != nil {
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
	payload := (&ExtentAddPayload{InodeNum: 51, FileBlock: 6, DiskBlock: lost, Length: 1}).Serialize()
	for attempt, wantApplied := range []bool{true, false} {
		applied, err := applyExtentAdd(ctx, rctx, payload)
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
