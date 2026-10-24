package inode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// createTestDevice creates a test file for O_DIRECT testing.
func createTestDevice(tb testing.TB, size int64) (*storage.Device, func()) {
	tb.Helper()

	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_inodes.bin")

	f, err := os.Create(testFile)
	if err != nil {
		tb.Skipf("Cannot create test file: %v", err)
	}

	if err := f.Truncate(size); err != nil {
		f.Close()
		os.Remove(testFile)
		tb.Skipf("Cannot truncate test file: %v", err)
	}
	f.Close()

	dev, err := storage.Open(testFile)
	if err != nil {
		os.Remove(testFile)
		tb.Skipf("O_DIRECT not supported: %v", err)
	}

	cleanup := func() {
		dev.Close()
		os.Remove(testFile)
	}

	return dev, cleanup
}

func TestConstants(t *testing.T) {
	// Verify inode size is exactly 256 bytes
	if InodeSize != 256 {
		t.Errorf("InodeSize = %d, want 256", InodeSize)
	}

	// Verify 16 inodes per sector
	if InodesPerSector != 16 {
		t.Errorf("InodesPerSector = %d, want 16", InodesPerSector)
	}

	// Verify sector size
	if SectorSize != 4096 {
		t.Errorf("SectorSize = %d, want 4096", SectorSize)
	}

	// Verify InodesPerSector calculation
	if SectorSize/InodeSize != InodesPerSector {
		t.Errorf("SectorSize/InodeSize = %d, want %d", SectorSize/InodeSize, InodesPerSector)
	}
}

func TestNewTable(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20) // 10MB
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	table := NewTable(dev, 0, 10<<20, nil, uuid)

	if table == nil {
		t.Fatal("NewTable returned nil")
	}

	expectedInodes := uint64(10 << 20 / InodeSize)
	if table.TotalInodes() != expectedInodes {
		t.Errorf("TotalInodes = %d, want %d", table.TotalInodes(), expectedInodes)
	}
}

func TestSerializeDeserialize(t *testing.T) {
	now := time.Now().Unix()

	inode := &Inode{
		Magic:           InodeMagic,
		InodeNum:        42,
		Mode:            S_IFREG | 0o644,
		Flags:           FlagHasExtentTree,
		UID:             1000,
		GID:             1000,
		LinkCount:       2,
		Size:            12345678,
		AllocatedBlocks: 12,
		WrittenBlocks:   10, // Include WrittenBlocks in serialization test
		Atime:           now,
		Mtime:           now - 100,
		Ctime:           now - 200,
		DataGeneration:  5,
		MetaGeneration:  10,
		ExtentTreeRoot:  1000,
		ExtentCount:     3,
		ExtentDepth:     1,
	}
	copy(inode.LastModifierUUID[:], "0123456789abcdef")

	inode.InlineExtents[0] = InlineExtent{
		LogicalBlock:  0,
		PhysicalBlock: 100,
		Length:        5,
		Flags:         1,
	}
	inode.InlineExtents[1] = InlineExtent{
		LogicalBlock:  5,
		PhysicalBlock: 200,
		Length:        3,
		Flags:         2,
	}

	// Serialize
	data := serializeInode(inode)

	if len(data) != InodeSize {
		t.Fatalf("serialized size = %d, want %d", len(data), InodeSize)
	}

	// Deserialize
	restored, err := deserializeInode(data)
	if err != nil {
		t.Fatalf("deserializeInode failed: %v", err)
	}

	// Verify all fields
	if restored.Magic != inode.Magic {
		t.Errorf("Magic = %x, want %x", restored.Magic, inode.Magic)
	}
	if restored.InodeNum != inode.InodeNum {
		t.Errorf("InodeNum = %d, want %d", restored.InodeNum, inode.InodeNum)
	}
	if restored.Mode != inode.Mode {
		t.Errorf("Mode = %o, want %o", restored.Mode, inode.Mode)
	}
	if restored.Flags != inode.Flags {
		t.Errorf("Flags = %d, want %d", restored.Flags, inode.Flags)
	}
	if restored.UID != inode.UID {
		t.Errorf("UID = %d, want %d", restored.UID, inode.UID)
	}
	if restored.GID != inode.GID {
		t.Errorf("GID = %d, want %d", restored.GID, inode.GID)
	}
	if restored.LinkCount != inode.LinkCount {
		t.Errorf("LinkCount = %d, want %d", restored.LinkCount, inode.LinkCount)
	}
	if restored.Size != inode.Size {
		t.Errorf("Size = %d, want %d", restored.Size, inode.Size)
	}
	if restored.AllocatedBlocks != inode.AllocatedBlocks {
		t.Errorf("AllocatedBlocks = %d, want %d", restored.AllocatedBlocks, inode.AllocatedBlocks)
	}
	if restored.WrittenBlocks != inode.WrittenBlocks {
		t.Errorf("WrittenBlocks = %d, want %d", restored.WrittenBlocks, inode.WrittenBlocks)
	}
	if restored.Atime != inode.Atime {
		t.Errorf("Atime = %d, want %d", restored.Atime, inode.Atime)
	}
	if restored.Mtime != inode.Mtime {
		t.Errorf("Mtime = %d, want %d", restored.Mtime, inode.Mtime)
	}
	if restored.Ctime != inode.Ctime {
		t.Errorf("Ctime = %d, want %d", restored.Ctime, inode.Ctime)
	}
	if restored.DataGeneration != inode.DataGeneration {
		t.Errorf("DataGeneration = %d, want %d", restored.DataGeneration, inode.DataGeneration)
	}
	if restored.MetaGeneration != inode.MetaGeneration {
		t.Errorf("MetaGeneration = %d, want %d", restored.MetaGeneration, inode.MetaGeneration)
	}
	if restored.LastModifierUUID != inode.LastModifierUUID {
		t.Error("LastModifierUUID mismatch")
	}
	if restored.ExtentTreeRoot != inode.ExtentTreeRoot {
		t.Errorf("ExtentTreeRoot = %d, want %d", restored.ExtentTreeRoot, inode.ExtentTreeRoot)
	}
	if restored.ExtentCount != inode.ExtentCount {
		t.Errorf("ExtentCount = %d, want %d", restored.ExtentCount, inode.ExtentCount)
	}
	if restored.ExtentDepth != inode.ExtentDepth {
		t.Errorf("ExtentDepth = %d, want %d", restored.ExtentDepth, inode.ExtentDepth)
	}

	// Verify inline extents
	for i := 0; i < MaxInlineExtents; i++ {
		if restored.InlineExtents[i] != inode.InlineExtents[i] {
			t.Errorf("InlineExtents[%d] mismatch: got %+v, want %+v",
				i, restored.InlineExtents[i], inode.InlineExtents[i])
		}
	}
}

func TestDeserializeEmpty(t *testing.T) {
	// Empty (zero) inode should deserialize with Magic=0
	data := make([]byte, InodeSize)
	inode, err := deserializeInode(data)
	if err != nil {
		t.Fatalf("deserializeInode empty failed: %v", err)
	}
	if inode.Magic != 0 {
		t.Errorf("empty inode Magic = %x, want 0", inode.Magic)
	}
}

func TestDeserializeCorrupted(t *testing.T) {
	// Create valid inode
	inode := &Inode{
		Magic:    InodeMagic,
		InodeNum: 42,
		Mode:     S_IFREG | 0o644,
	}
	data := serializeInode(inode)

	// Corrupt the data
	data[50] ^= 0xFF

	// Should fail checksum
	_, err := deserializeInode(data)
	if err != ErrChecksumMismatch {
		t.Errorf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestAllocInode(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Allocate first inode
	inode, err := table.Alloc(ctx)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	if inode.InodeNum != InodeFirstUsable {
		t.Errorf("First allocated inode = %d, want %d", inode.InodeNum, InodeFirstUsable)
	}

	if inode.Magic != InodeMagic {
		t.Errorf("Allocated inode Magic = %x, want %x", inode.Magic, InodeMagic)
	}

	// Allocate second inode
	inode2, err := table.Alloc(ctx)
	if err != nil {
		t.Fatalf("Second Alloc failed: %v", err)
	}

	if inode2.InodeNum != InodeFirstUsable+1 {
		t.Errorf("Second allocated inode = %d, want %d", inode2.InodeNum, InodeFirstUsable+1)
	}
}

func TestFreeInode(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Allocate an inode
	inode, err := table.Alloc(ctx)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	// Free it
	err = table.Free(ctx, inode.InodeNum)
	if err != nil {
		t.Fatalf("Free failed: %v", err)
	}

	// Read it back - should be empty (Magic = 0)
	freed, err := table.Read(inode.InodeNum)
	if err != nil {
		t.Fatalf("Read freed inode failed: %v", err)
	}

	if freed.Magic != 0 {
		t.Errorf("Freed inode Magic = %x, want 0", freed.Magic)
	}

	// Allocate again - should get same number
	inode2, err := table.Alloc(ctx)
	if err != nil {
		t.Fatalf("Re-Alloc failed: %v", err)
	}

	if inode2.InodeNum != inode.InodeNum {
		t.Errorf("Re-allocated inode = %d, want %d", inode2.InodeNum, inode.InodeNum)
	}
}

func TestFreeReservedInode(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Try to free reserved inodes
	for _, num := range []uint32{InodeInvalid, InodeBadBlocks, InodeRoot} {
		err := table.Free(ctx, num)
		if err == nil {
			t.Errorf("Free(reserved inode %d) should fail", num)
		}
	}
}

func TestFreeNotInUse(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Try to free an inode that was never allocated
	err := table.Free(ctx, InodeFirstUsable)
	if err != ErrInodeNotInUse {
		t.Errorf("Free unallocated inode: got %v, want ErrInodeNotInUse", err)
	}
}

func TestWriteRead(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Allocate an inode
	inode, err := table.Alloc(ctx)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	// Modify it
	inode.Mode = S_IFREG | 0o755
	inode.Size = 1000
	inode.UID = 500
	inode.GID = 500

	// Write it
	err = table.Write(ctx, inode)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read it back
	read, err := table.Read(inode.InodeNum)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if read.Size != 1000 {
		t.Errorf("Read Size = %d, want 1000", read.Size)
	}
	if read.Mode != S_IFREG|0o755 {
		t.Errorf("Read Mode = %o, want %o", read.Mode, S_IFREG|0o755)
	}
	if read.UID != 500 {
		t.Errorf("Read UID = %d, want 500", read.UID)
	}
}

func TestInitRootInode(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Initialize root
	err := table.InitRootInode(ctx)
	if err != nil {
		t.Fatalf("InitRootInode failed: %v", err)
	}

	// Read root inode
	root, err := table.Read(InodeRoot)
	if err != nil {
		t.Fatalf("Read root inode failed: %v", err)
	}

	if root.Magic != InodeMagic {
		t.Errorf("Root Magic = %x, want %x", root.Magic, InodeMagic)
	}

	if !root.IsDir() {
		t.Error("Root inode should be a directory")
	}

	if root.LinkCount != 2 {
		t.Errorf("Root LinkCount = %d, want 2", root.LinkCount)
	}

	// Init again should be idempotent
	err = table.InitRootInode(ctx)
	if err != nil {
		t.Fatalf("Second InitRootInode failed: %v", err)
	}
}

func TestInodeHelpers(t *testing.T) {
	testCases := []struct {
		name   string
		mode   uint16
		isDir  bool
		isFile bool
		isLink bool
	}{
		{"directory", S_IFDIR | 0o755, true, false, false},
		{"regular file", S_IFREG | 0o644, false, true, false},
		{"symlink", S_IFLNK | 0o777, false, false, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inode := &Inode{Mode: tc.mode}

			if inode.IsDir() != tc.isDir {
				t.Errorf("IsDir = %v, want %v", inode.IsDir(), tc.isDir)
			}
			if inode.IsFile() != tc.isFile {
				t.Errorf("IsFile = %v, want %v", inode.IsFile(), tc.isFile)
			}
			if inode.IsSymlink() != tc.isLink {
				t.Errorf("IsSymlink = %v, want %v", inode.IsSymlink(), tc.isLink)
			}
		})
	}
}

func TestPermissions(t *testing.T) {
	inode := &Inode{Mode: S_IFREG | 0o755}

	if inode.Permissions() != 0o755 {
		t.Errorf("Permissions = %o, want 0755", inode.Permissions())
	}

	inode.Mode = S_IFDIR | 0o4755 // setuid
	if inode.Permissions() != 0o4755 {
		t.Errorf("Permissions with setuid = %o, want 04755", inode.Permissions())
	}
}

func TestHasExtentTree(t *testing.T) {
	inode := &Inode{Flags: 0}

	if inode.HasExtentTree() {
		t.Error("Empty flags should not have extent tree")
	}

	inode.Flags = FlagHasExtentTree
	if !inode.HasExtentTree() {
		t.Error("Should have extent tree")
	}

	inode.Flags = FlagHasExtentTree | FlagHasSnapshot
	if !inode.HasExtentTree() {
		t.Error("Should have extent tree with multiple flags")
	}
}

func TestSizeForInodes(t *testing.T) {
	testCases := []struct {
		count    uint64
		expected uint64
	}{
		{1, SectorSize},                    // 1 inode = 1 sector
		{16, SectorSize},                   // 16 inodes = 1 sector
		{17, 2 * SectorSize},               // 17 inodes = 2 sectors
		{32, 2 * SectorSize},               // 32 inodes = 2 sectors
		{100, 7 * SectorSize},              // 100/16 = 6.25 → 7 sectors
		{1000, (1000/16 + 1) * SectorSize}, // 62.5 → 63 sectors
	}

	for _, tc := range testCases {
		result := SizeForInodes(tc.count)
		if result != tc.expected {
			t.Errorf("SizeForInodes(%d) = %d, want %d", tc.count, result, tc.expected)
		}
	}
}

func TestReadOutOfRange(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	// Create table with limited size
	table := NewTable(dev, 0, 256*100, nil, uuid) // 100 inodes

	_, err := table.Read(101) // Out of range
	if err != ErrInodeOutOfRange {
		t.Errorf("Read out of range: got %v, want ErrInodeOutOfRange", err)
	}
}

func TestPersistence(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Allocate and write some inodes
	for i := 0; i < 5; i++ {
		inode, err := table.Alloc(ctx)
		if err != nil {
			t.Fatalf("Alloc %d failed: %v", i, err)
		}
		inode.Size = uint64(i * 1000)
		err = table.Write(ctx, inode)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	// Create new table instance (simulates remount)
	table2 := NewTable(dev, 0, 10<<20, nil, uuid)

	// Verify inodes are persisted
	for i := 0; i < 5; i++ {
		inode, err := table2.Read(InodeFirstUsable + uint32(i))
		if err != nil {
			t.Fatalf("Read %d from new table failed: %v", i, err)
		}
		expectedSize := uint64(i * 1000)
		if inode.Size != expectedSize {
			t.Errorf("Inode %d Size = %d, want %d", i, inode.Size, expectedSize)
		}
	}
}

func TestMultipleSectors(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Allocate inodes across multiple sectors
	// 16 inodes per sector, so 50 inodes spans 4 sectors
	inodes := make([]*Inode, 50)
	for i := 0; i < 50; i++ {
		inode, err := table.Alloc(ctx)
		if err != nil {
			t.Fatalf("Alloc %d failed: %v", i, err)
		}
		inode.Size = uint64(i * 100)
		err = table.Write(ctx, inode)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
		inodes[i] = inode
	}

	// Verify all can be read back
	for i, expected := range inodes {
		read, err := table.Read(expected.InodeNum)
		if err != nil {
			t.Fatalf("Read inode %d failed: %v", expected.InodeNum, err)
		}
		if read.Size != expected.Size {
			t.Errorf("Inode %d Size = %d, want %d", i, read.Size, expected.Size)
		}
	}
}

func TestMetaGenerationIncrement(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Allocate an inode
	inode, err := table.Alloc(ctx)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	initialGen := inode.MetaGeneration

	// Write multiple times
	for i := 0; i < 5; i++ {
		inode.Size = uint64(i * 100)
		err = table.Write(ctx, inode)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	// Read back
	read, err := table.Read(inode.InodeNum)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// MetaGeneration should have incremented 5 times
	expectedGen := initialGen + 5
	if read.MetaGeneration != expectedGen {
		t.Errorf("MetaGeneration = %d, want %d", read.MetaGeneration, expectedGen)
	}
}

func BenchmarkAllocInode(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, 100<<20, nil, uuid)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.Alloc(ctx)
	}
}

func BenchmarkReadInode(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, 100<<20, nil, uuid)
	ctx := context.Background()

	// Allocate one inode to read
	inode, _ := table.Alloc(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.Read(inode.InodeNum)
	}
}

func BenchmarkWriteInode(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, 100<<20, nil, uuid)
	ctx := context.Background()

	// Allocate one inode to write
	inode, _ := table.Alloc(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inode.Size = uint64(i)
		table.Write(ctx, inode)
	}
}

// =============================================================================
// Two-Phase Allocation Tests (FindFreeSlot + ClaimSlot)
// =============================================================================

func TestFindFreeSlot(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")
	table := NewTable(dev, 0, 10<<20, nil, uuid)

	// Find first free slot
	slot, err := table.FindFreeSlot()
	if err != nil {
		t.Fatalf("FindFreeSlot failed: %v", err)
	}

	// Should return first usable inode
	if slot != InodeFirstUsable {
		t.Errorf("FindFreeSlot returned %d, want %d", slot, InodeFirstUsable)
	}

	// FindFreeSlot is read-only, calling again should return same slot
	slot2, err := table.FindFreeSlot()
	if err != nil {
		t.Fatalf("Second FindFreeSlot failed: %v", err)
	}
	if slot2 != slot {
		t.Errorf("Second FindFreeSlot returned %d, want %d", slot2, slot)
	}
}

func TestClaimSlot(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")
	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Find a free slot
	slot, err := table.FindFreeSlot()
	if err != nil {
		t.Fatalf("FindFreeSlot failed: %v", err)
	}

	// Claim the slot
	inode, err := table.ClaimSlot(ctx, slot)
	if err != nil {
		t.Fatalf("ClaimSlot failed: %v", err)
	}

	if inode.InodeNum != slot {
		t.Errorf("Claimed inode num = %d, want %d", inode.InodeNum, slot)
	}
	if inode.Magic != InodeMagic {
		t.Errorf("Claimed inode Magic = %x, want %x", inode.Magic, InodeMagic)
	}

	// Verify it's persisted
	read, err := table.Read(slot)
	if err != nil {
		t.Fatalf("Read after ClaimSlot failed: %v", err)
	}
	if read.Magic != InodeMagic {
		t.Errorf("Read inode Magic = %x, want %x", read.Magic, InodeMagic)
	}
}

func TestClaimSlotAlreadyInUse(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")
	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Find a free slot
	slot, err := table.FindFreeSlot()
	if err != nil {
		t.Fatalf("FindFreeSlot failed: %v", err)
	}

	// Claim it first time
	_, err = table.ClaimSlot(ctx, slot)
	if err != nil {
		t.Fatalf("First ClaimSlot failed: %v", err)
	}

	// Try to claim it again - should fail with ErrInodeInUse
	_, err = table.ClaimSlot(ctx, slot)
	if err != ErrInodeInUse {
		t.Errorf("Second ClaimSlot error = %v, want ErrInodeInUse", err)
	}
}

func TestTwoPhaseAllocation(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")
	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Simulate the two-phase allocation pattern:
	// 1. Find slot (no locks)
	// 2. Do other work (e.g., allocate blocks - would hold Level 6 lock)
	// 3. Claim slot (acquires Level 8 lock)

	slot1, err := table.FindFreeSlot()
	if err != nil {
		t.Fatalf("FindFreeSlot 1 failed: %v", err)
	}

	// Simulate "other work" between find and claim
	// In real code, this is where BlockBitmap lock would be acquired

	slot2, err := table.FindFreeSlot()
	if err != nil {
		t.Fatalf("FindFreeSlot 2 failed: %v", err)
	}

	// Both finds should return the same slot since neither claimed yet
	if slot1 != slot2 {
		t.Errorf("Two FindFreeSlot calls returned different slots: %d vs %d", slot1, slot2)
	}

	// Now claim the first slot
	inode1, err := table.ClaimSlot(ctx, slot1)
	if err != nil {
		t.Fatalf("ClaimSlot 1 failed: %v", err)
	}

	// After claiming, FindFreeSlot should return a different slot
	slot3, err := table.FindFreeSlot()
	if err != nil {
		t.Fatalf("FindFreeSlot 3 failed: %v", err)
	}

	if slot3 == slot1 {
		t.Errorf("FindFreeSlot returned same slot %d after it was claimed", slot3)
	}
	if slot3 != slot1+1 {
		t.Errorf("FindFreeSlot returned %d, expected next slot %d", slot3, slot1+1)
	}

	// Claim second slot
	inode2, err := table.ClaimSlot(ctx, slot3)
	if err != nil {
		t.Fatalf("ClaimSlot 2 failed: %v", err)
	}

	if inode1.InodeNum == inode2.InodeNum {
		t.Error("Two claimed inodes have same number")
	}
}

func TestClaimSlotOutOfRange(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, 10<<20, nil, uuid)
	ctx := context.Background()

	// Try to claim an out-of-range slot
	totalInodes := table.TotalInodes()
	_, err := table.ClaimSlot(ctx, uint32(totalInodes+100))
	if err != ErrInodeOutOfRange {
		t.Errorf("ClaimSlot out of range error = %v, want ErrInodeOutOfRange", err)
	}
}

func TestFindFreeSlotWhenFull(t *testing.T) {
	// Create a small table that we can fill
	dev, cleanup := createTestDevice(t, SectorSize) // Just one sector = 16 inodes
	defer cleanup()

	var uuid [16]byte
	table := NewTable(dev, 0, SectorSize, nil, uuid)
	ctx := context.Background()

	// Fill all usable slots (inodes 3-15, since 0,1,2 are reserved)
	usableCount := InodesPerSector - int(InodeFirstUsable) // 16 - 3 = 13

	for i := 0; i < usableCount; i++ {
		slot, err := table.FindFreeSlot()
		if err != nil {
			t.Fatalf("FindFreeSlot %d failed: %v", i, err)
		}
		_, err = table.ClaimSlot(ctx, slot)
		if err != nil {
			t.Fatalf("ClaimSlot %d failed: %v", i, err)
		}
	}

	// Now FindFreeSlot should return ErrInodeFull
	_, err := table.FindFreeSlot()
	if err != ErrInodeFull {
		t.Errorf("FindFreeSlot on full table error = %v, want ErrInodeFull", err)
	}
}

type recordingFlusher struct{ events *[]string }

func (f recordingFlusher) FlushTree(ino, onDisk *Inode) (func(), error) {
	*f.events = append(*f.events, "flush")
	return func() { *f.events = append(*f.events, "release") }, nil
}

// A tree commit must reach the disk before released blocks are freed or a
// later commit overwrites the half the previous inode used.
func TestTreeCommitIsSyncedBeforeRelease(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	var uuid [16]byte
	tbl := NewTable(dev, 0, 16*InodeSize, nil, uuid)
	var events []string
	tbl.SetTreeFlusher(recordingFlusher{&events})
	syncErr := error(nil)
	tbl.syncDevice = func() error {
		onDisk, err := tbl.readInodeUnlocked(5)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, fmt.Sprintf("sync size=%d", onDisk.Size))
		return syncErr
	}

	ctx := context.Background()
	ino := &Inode{Magic: InodeMagic, InodeNum: 5, Size: 77, PendingTree: struct{}{}}
	if err := tbl.Write(ctx, ino); err != nil {
		t.Fatal(err)
	}
	if want := []string{"flush", "sync size=77", "release"}; !slices.Equal(events, want) {
		t.Fatalf("events %v, want %v", events, want)
	}

	events, syncErr = nil, errors.New("sync failed")
	ino.Size, ino.PendingTree = 78, struct{}{}
	if err := tbl.Write(ctx, ino); err == nil {
		t.Fatal("Write should fail when the commit cannot be synced")
	}
	if want := []string{"flush", "sync size=78"}; !slices.Equal(events, want) {
		t.Fatalf("a failed sync must not release blocks: events %v, want %v", events, want)
	}

	events = nil
	ino.Size = 79
	if err := tbl.Write(ctx, ino); err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("a write without tree changes should not flush or sync: %v", events)
	}
}

// A copy with no extent edits must not wipe or roll back the tree on disk.
func TestStaleCopyKeepsOnDiskExtentTree(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	var uuid [16]byte
	tbl := NewTable(dev, 0, 16*InodeSize, nil, uuid)
	ctx := context.Background()

	stale := &Inode{Magic: InodeMagic, InodeNum: 6, LinkCount: 1}
	stale.InlineExtents[0] = InlineExtent{LogicalBlock: 0, PhysicalBlock: 9, Length: 1, Flags: 1}
	if err := tbl.Write(ctx, stale); err != nil {
		t.Fatal(err)
	}

	tree := &Inode{Magic: InodeMagic, InodeNum: 6, LinkCount: 1, Flags: FlagHasExtentTree,
		ExtentTreeRoot: 40, ExtentTreeGen: 7, ExtentDepth: 1, ExtentCount: 3}
	if err := tbl.writeInodeUnlocked(tree); err != nil {
		t.Fatal(err)
	}

	stale.Size = 4096
	if err := tbl.Write(ctx, stale); err != nil {
		t.Fatal(err)
	}
	got, err := tbl.Read(6)
	if err != nil {
		t.Fatal(err)
	}
	if !got.HasExtentTree() || got.ExtentTreeRoot != 40 || got.ExtentTreeGen != 7 || got.InlineExtents != [MaxInlineExtents]InlineExtent{} {
		t.Fatalf("stale inline copy replaced the on-disk tree: %+v", got)
	}
	if got.Size != 4096 {
		t.Fatalf("Size = %d, want the stale writer's 4096", got.Size)
	}
}
