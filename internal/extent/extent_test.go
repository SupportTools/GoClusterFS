package extent

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// createTestDevice creates a test file for O_DIRECT testing.
func createTestDevice(tb testing.TB, size int64) (*storage.Device, func()) {
	tb.Helper()

	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_extents.bin")

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

func createTestSetup(tb testing.TB) (*Manager, *inode.Inode, func()) {
	tb.Helper()

	dev, cleanup := createTestDevice(tb, 100<<20) // 100MB

	var uuid [16]byte
	bm := bitmap.NewRegion(dev, 0, 1000, nil, uuid) // 1000 blocks
	mgr := NewManager(dev, bm, nil, 1<<20)

	ino := &inode.Inode{
		Magic:    inode.InodeMagic,
		InodeNum: 10,
		Mode:     inode.S_IFREG | 0o644,
	}

	return mgr, ino, cleanup
}

func TestConstants(t *testing.T) {
	if ExtentSize != 32 {
		t.Errorf("ExtentSize = %d, want 32", ExtentSize)
	}

	if BlockSize != 1<<20 {
		t.Errorf("BlockSize = %d, want %d", BlockSize, 1<<20)
	}

	if MaxInlineExtents != 2 {
		t.Errorf("MaxInlineExtents = %d, want 2", MaxInlineExtents)
	}
}

func TestSerializeDeserialize(t *testing.T) {
	ext := &Extent{
		LogicalBlock:  100,
		PhysicalBlock: 500,
		Length:        10,
		Flags:         FlagAllocated | FlagUnwritten,
	}

	data := SerializeExtent(ext)
	if len(data) != ExtentSize {
		t.Fatalf("serialized size = %d, want %d", len(data), ExtentSize)
	}

	restored, err := DeserializeExtent(data)
	if err != nil {
		t.Fatalf("DeserializeExtent failed: %v", err)
	}

	if restored.LogicalBlock != ext.LogicalBlock {
		t.Errorf("LogicalBlock = %d, want %d", restored.LogicalBlock, ext.LogicalBlock)
	}
	if restored.PhysicalBlock != ext.PhysicalBlock {
		t.Errorf("PhysicalBlock = %d, want %d", restored.PhysicalBlock, ext.PhysicalBlock)
	}
	if restored.Length != ext.Length {
		t.Errorf("Length = %d, want %d", restored.Length, ext.Length)
	}
	if restored.Flags != ext.Flags {
		t.Errorf("Flags = %d, want %d", restored.Flags, ext.Flags)
	}
}

func TestDeserializeInvalid(t *testing.T) {
	_, err := DeserializeExtent(make([]byte, 10))
	if err != ErrInvalidExtent {
		t.Errorf("expected ErrInvalidExtent, got %v", err)
	}
}

func TestExtentFlags(t *testing.T) {
	ext := &Extent{Flags: 0}

	if ext.IsAllocated() {
		t.Error("should not be allocated")
	}
	if ext.IsUnwritten() {
		t.Error("should not be unwritten")
	}
	if ext.IsShared() {
		t.Error("should not be shared")
	}

	ext.Flags = FlagAllocated
	if !ext.IsAllocated() {
		t.Error("should be allocated")
	}

	ext.Flags = FlagUnwritten
	if !ext.IsUnwritten() {
		t.Error("should be unwritten")
	}

	ext.Flags = FlagShared
	if !ext.IsShared() {
		t.Error("should be shared")
	}

	ext.Flags = FlagAllocated | FlagShared
	if !ext.IsAllocated() || !ext.IsShared() {
		t.Error("should be allocated and shared")
	}
}

func TestBlockHelpers(t *testing.T) {
	testCases := []struct {
		size   uint64
		blocks uint64
	}{
		{0, 0},
		{1, 1},
		{BlockSize - 1, 1},
		{BlockSize, 1},
		{BlockSize + 1, 2},
		{BlockSize * 5, 5},
		{BlockSize*5 + 1, 6},
	}

	for _, tc := range testCases {
		result := BlocksForSize(tc.size)
		if result != tc.blocks {
			t.Errorf("BlocksForSize(%d) = %d, want %d", tc.size, result, tc.blocks)
		}
	}
}

func TestOffsetBlockConversion(t *testing.T) {
	testCases := []struct {
		offset uint64
		block  uint64
	}{
		{0, 0},
		{BlockSize - 1, 0},
		{BlockSize, 1},
		{BlockSize + 1, 1},
		{BlockSize * 100, 100},
	}

	for _, tc := range testCases {
		result := OffsetToBlock(tc.offset)
		if result != tc.block {
			t.Errorf("OffsetToBlock(%d) = %d, want %d", tc.offset, result, tc.block)
		}

		// Verify BlockToOffset for whole blocks
		if tc.offset%BlockSize == 0 {
			back := BlockToOffset(tc.block)
			if back != tc.offset {
				t.Errorf("BlockToOffset(%d) = %d, want %d", tc.block, back, tc.offset)
			}
		}
	}
}

func TestInsertExtent(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert first extent
	ext1 := &Extent{
		LogicalBlock:  0,
		PhysicalBlock: 100,
		Length:        5,
		Flags:         FlagAllocated,
	}

	err := mgr.Insert(ctx, ino, ext1)
	if err != nil {
		t.Fatalf("Insert first extent failed: %v", err)
	}

	if ino.ExtentCount != 1 {
		t.Errorf("ExtentCount = %d, want 1", ino.ExtentCount)
	}

	// Insert second extent
	ext2 := &Extent{
		LogicalBlock:  10,
		PhysicalBlock: 200,
		Length:        3,
		Flags:         FlagAllocated,
	}

	err = mgr.Insert(ctx, ino, ext2)
	if err != nil {
		t.Fatalf("Insert second extent failed: %v", err)
	}

	if ino.ExtentCount != 2 {
		t.Errorf("ExtentCount = %d, want 2", ino.ExtentCount)
	}
}

func TestInsertBeyondInlineMovesToTree(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	for i := uint64(0); i < 3; i++ {
		ext := &Extent{LogicalBlock: i * 10, PhysicalBlock: 100 + i*10, Length: 5, Flags: FlagAllocated}
		if err := mgr.Insert(ctx, ino, ext); err != nil {
			t.Fatalf("Insert extent %d failed: %v", i, err)
		}
	}

	if !ino.HasExtentTree() {
		t.Fatal("third extent should move the map into an extent tree")
	}
	if ino.ExtentCount != 3 || ino.ExtentDepth != 1 {
		t.Errorf("ExtentCount=%d ExtentDepth=%d, want 3 and 1", ino.ExtentCount, ino.ExtentDepth)
	}
	if ino.InlineExtents != [inode.MaxInlineExtents]inode.InlineExtent{} {
		t.Errorf("inline slots should be empty once the tree is used: %+v", ino.InlineExtents)
	}
	for i := uint64(0); i < 3; i++ {
		phys, err := mgr.LookupPhysical(ino, i*10+2)
		if err != nil || phys != 102+i*10 {
			t.Errorf("LookupPhysical(%d) = %d, %v; want %d", i*10+2, phys, err, 102+i*10)
		}
	}
}

func TestInsertOverlap(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert first extent (blocks 0-4)
	ext1 := &Extent{
		LogicalBlock:  0,
		PhysicalBlock: 100,
		Length:        5,
		Flags:         FlagAllocated,
	}
	mgr.Insert(ctx, ino, ext1)

	// Try to insert overlapping extent (blocks 3-7)
	ext2 := &Extent{
		LogicalBlock:  3,
		PhysicalBlock: 200,
		Length:        5,
		Flags:         FlagAllocated,
	}

	err := mgr.Insert(ctx, ino, ext2)
	if err != ErrExtentOverlap {
		t.Errorf("expected ErrExtentOverlap, got %v", err)
	}
}

func TestInsertInvalidExtent(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Zero-length extent should fail
	ext := &Extent{
		LogicalBlock:  0,
		PhysicalBlock: 100,
		Length:        0,
		Flags:         FlagAllocated,
	}

	err := mgr.Insert(ctx, ino, ext)
	if err != ErrInvalidExtent {
		t.Errorf("expected ErrInvalidExtent, got %v", err)
	}
}

func TestLookup(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert extent (blocks 10-19)
	ext := &Extent{
		LogicalBlock:  10,
		PhysicalBlock: 100,
		Length:        10,
		Flags:         FlagAllocated,
	}
	mgr.Insert(ctx, ino, ext)

	// Lookup within extent
	found, offset, err := mgr.Lookup(ino, 15)
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}

	if found.LogicalBlock != 10 {
		t.Errorf("Found LogicalBlock = %d, want 10", found.LogicalBlock)
	}
	if offset != 5 {
		t.Errorf("Offset = %d, want 5", offset)
	}

	// Lookup at start
	found, offset, err = mgr.Lookup(ino, 10)
	if err != nil {
		t.Fatalf("Lookup at start failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("Offset at start = %d, want 0", offset)
	}

	// Lookup at end
	found, offset, err = mgr.Lookup(ino, 19)
	if err != nil {
		t.Fatalf("Lookup at end failed: %v", err)
	}
	if offset != 9 {
		t.Errorf("Offset at end = %d, want 9", offset)
	}
}

func TestLookupNotFound(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert extent (blocks 10-19)
	ext := &Extent{
		LogicalBlock:  10,
		PhysicalBlock: 100,
		Length:        10,
		Flags:         FlagAllocated,
	}
	mgr.Insert(ctx, ino, ext)

	// Lookup before extent
	_, _, err := mgr.Lookup(ino, 5)
	if err != ErrExtentNotFound {
		t.Errorf("Lookup before: expected ErrExtentNotFound, got %v", err)
	}

	// Lookup after extent
	_, _, err = mgr.Lookup(ino, 25)
	if err != ErrExtentNotFound {
		t.Errorf("Lookup after: expected ErrExtentNotFound, got %v", err)
	}
}

func TestLookupPhysical(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert extent
	ext := &Extent{
		LogicalBlock:  10,
		PhysicalBlock: 100,
		Length:        10,
		Flags:         FlagAllocated,
	}
	mgr.Insert(ctx, ino, ext)

	// Lookup physical block
	phys, err := mgr.LookupPhysical(ino, 15)
	if err != nil {
		t.Fatalf("LookupPhysical failed: %v", err)
	}

	expected := uint64(105) // 100 + (15-10)
	if phys != expected {
		t.Errorf("Physical = %d, want %d", phys, expected)
	}

	// Lookup hole (not allocated) should return 0
	phys, err = mgr.LookupPhysical(ino, 5)
	if err != nil {
		t.Fatalf("LookupPhysical hole failed: %v", err)
	}
	if phys != 0 {
		t.Errorf("Physical for hole = %d, want 0", phys)
	}
}

func TestAllocateBlocks(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Allocate 5 blocks at logical block 0
	ext, err := mgr.AllocateBlocks(ctx, ino, 0, 5)
	if err != nil {
		t.Fatalf("AllocateBlocks failed: %v", err)
	}

	if ext.LogicalBlock != 0 {
		t.Errorf("Allocated LogicalBlock = %d, want 0", ext.LogicalBlock)
	}
	if ext.Length != 5 {
		t.Errorf("Allocated Length = %d, want 5", ext.Length)
	}
	if !ext.IsAllocated() {
		t.Error("Extent should be allocated")
	}

	// Verify inode stats updated
	if ino.AllocatedBlocks != 5 {
		t.Errorf("AllocatedBlocks = %d, want 5", ino.AllocatedBlocks)
	}
}

func TestAllocateAlreadyExists(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// First allocation
	ext1, err := mgr.AllocateBlocks(ctx, ino, 0, 5)
	if err != nil {
		t.Fatalf("First AllocateBlocks failed: %v", err)
	}

	// Same allocation again should return existing
	ext2, err := mgr.AllocateBlocks(ctx, ino, 0, 5)
	if err != nil {
		t.Fatalf("Second AllocateBlocks failed: %v", err)
	}

	if ext2.PhysicalBlock != ext1.PhysicalBlock {
		t.Errorf("Second allocation returned different physical block: %d vs %d",
			ext2.PhysicalBlock, ext1.PhysicalBlock)
	}
}

func TestTruncate(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert extents
	ext1 := &Extent{
		LogicalBlock:  0,
		PhysicalBlock: 100,
		Length:        5,
		Flags:         FlagAllocated,
	}
	ext2 := &Extent{
		LogicalBlock:  10,
		PhysicalBlock: 200,
		Length:        5,
		Flags:         FlagAllocated,
	}

	mgr.Insert(ctx, ino, ext1)
	mgr.Insert(ctx, ino, ext2)
	ino.AllocatedBlocks = 10
	ino.Size = 15 * BlockSize

	// Truncate to remove second extent entirely
	err := mgr.Truncate(ctx, ino, 5*BlockSize)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify second extent removed
	extents := listExtents(t, mgr, ino)
	if len(extents) != 1 {
		t.Errorf("After truncate: %d extents, want 1", len(extents))
	}

	if ino.Size != 5*BlockSize {
		t.Errorf("Size after truncate = %d, want %d", ino.Size, 5*BlockSize)
	}
}

func TestTruncatePartial(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert extent covering blocks 0-9
	ext := &Extent{
		LogicalBlock:  0,
		PhysicalBlock: 100,
		Length:        10,
		Flags:         FlagAllocated,
	}
	mgr.Insert(ctx, ino, ext)
	ino.AllocatedBlocks = 10
	ino.Size = 10 * BlockSize

	// Truncate to 5 blocks
	err := mgr.Truncate(ctx, ino, 5*BlockSize)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify extent was shortened
	extents := listExtents(t, mgr, ino)
	if len(extents) != 1 {
		t.Fatalf("After truncate: %d extents, want 1", len(extents))
	}

	if extents[0].Length != 5 {
		t.Errorf("Extent length after truncate = %d, want 5", extents[0].Length)
	}

	if ino.AllocatedBlocks != 5 {
		t.Errorf("AllocatedBlocks = %d, want 5", ino.AllocatedBlocks)
	}
}

func TestTruncateToZero(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert extents
	ext := &Extent{
		LogicalBlock:  0,
		PhysicalBlock: 100,
		Length:        10,
		Flags:         FlagAllocated,
	}
	mgr.Insert(ctx, ino, ext)
	ino.AllocatedBlocks = 10
	ino.Size = 10 * BlockSize

	// Truncate to 0
	err := mgr.Truncate(ctx, ino, 0)
	if err != nil {
		t.Fatalf("Truncate to 0 failed: %v", err)
	}

	extents := listExtents(t, mgr, ino)
	if len(extents) != 0 {
		t.Errorf("After truncate to 0: %d extents, want 0", len(extents))
	}

	if ino.Size != 0 {
		t.Errorf("Size after truncate = %d, want 0", ino.Size)
	}
}

func TestListExtents(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert extents out of order
	ext2 := &Extent{
		LogicalBlock:  20,
		PhysicalBlock: 200,
		Length:        5,
		Flags:         FlagAllocated,
	}
	ext1 := &Extent{
		LogicalBlock:  10,
		PhysicalBlock: 100,
		Length:        5,
		Flags:         FlagAllocated,
	}

	mgr.Insert(ctx, ino, ext2)
	mgr.Insert(ctx, ino, ext1)

	// List should return sorted
	extents := listExtents(t, mgr, ino)
	if len(extents) != 2 {
		t.Fatalf("ListExtents returned %d, want 2", len(extents))
	}

	if extents[0].LogicalBlock != 10 {
		t.Errorf("First extent LogicalBlock = %d, want 10", extents[0].LogicalBlock)
	}
	if extents[1].LogicalBlock != 20 {
		t.Errorf("Second extent LogicalBlock = %d, want 20", extents[1].LogicalBlock)
	}
}

func TestContiguousRuns(t *testing.T) {
	runs := contiguousRuns(10, []uint64{5, 6, 7, 9, 12, 13}, FlagAllocated)
	want := []Extent{
		{LogicalBlock: 10, PhysicalBlock: 5, Length: 3, Flags: FlagAllocated},
		{LogicalBlock: 13, PhysicalBlock: 9, Length: 1, Flags: FlagAllocated},
		{LogicalBlock: 14, PhysicalBlock: 12, Length: 2, Flags: FlagAllocated},
	}
	if len(runs) != len(want) {
		t.Fatalf("got %d runs, want %d: %+v", len(runs), len(want), runs)
	}
	for i := range want {
		if runs[i] != want[i] {
			t.Errorf("run %d = %+v, want %+v", i, runs[i], want[i])
		}
	}
}

func TestAllocateBlocksTracksWrittenBlocks(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Allocate 5 blocks at logical block 0
	ext, err := mgr.AllocateBlocks(ctx, ino, 0, 5)
	if err != nil {
		t.Fatalf("AllocateBlocks failed: %v", err)
	}

	if ext.LogicalBlock != 0 {
		t.Errorf("Allocated LogicalBlock = %d, want 0", ext.LogicalBlock)
	}
	if ext.Length != 5 {
		t.Errorf("Allocated Length = %d, want 5", ext.Length)
	}

	// Verify both AllocatedBlocks and WrittenBlocks are updated
	if ino.AllocatedBlocks != 5 {
		t.Errorf("AllocatedBlocks = %d, want 5", ino.AllocatedBlocks)
	}
	if ino.WrittenBlocks != 5 {
		t.Errorf("WrittenBlocks = %d, want 5", ino.WrittenBlocks)
	}
}

func TestTruncateAdjustsWrittenBlocks(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert extent covering blocks 0-9 (written, not unwritten)
	ext := &Extent{
		LogicalBlock:  0,
		PhysicalBlock: 100,
		Length:        10,
		Flags:         FlagAllocated, // Not FlagUnwritten
	}
	mgr.Insert(ctx, ino, ext)
	ino.AllocatedBlocks = 10
	ino.WrittenBlocks = 10
	ino.Size = 10 * BlockSize

	// Truncate to 5 blocks
	err := mgr.Truncate(ctx, ino, 5*BlockSize)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify both AllocatedBlocks and WrittenBlocks are decremented
	if ino.AllocatedBlocks != 5 {
		t.Errorf("AllocatedBlocks = %d, want 5", ino.AllocatedBlocks)
	}
	if ino.WrittenBlocks != 5 {
		t.Errorf("WrittenBlocks = %d, want 5", ino.WrittenBlocks)
	}
}

func TestTruncateToZeroAdjustsWrittenBlocks(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert written extent
	ext := &Extent{
		LogicalBlock:  0,
		PhysicalBlock: 100,
		Length:        10,
		Flags:         FlagAllocated,
	}
	mgr.Insert(ctx, ino, ext)
	ino.AllocatedBlocks = 10
	ino.WrittenBlocks = 10
	ino.Size = 10 * BlockSize

	// Truncate to 0
	err := mgr.Truncate(ctx, ino, 0)
	if err != nil {
		t.Fatalf("Truncate to 0 failed: %v", err)
	}

	if ino.AllocatedBlocks != 0 {
		t.Errorf("AllocatedBlocks after truncate = %d, want 0", ino.AllocatedBlocks)
	}
	if ino.WrittenBlocks != 0 {
		t.Errorf("WrittenBlocks after truncate = %d, want 0", ino.WrittenBlocks)
	}
}

func TestTruncateUnwrittenExtentDoesNotAffectWrittenBlocks(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Insert unwritten extent (lazy zero, pre-allocated but not written)
	ext := &Extent{
		LogicalBlock:  0,
		PhysicalBlock: 100,
		Length:        10,
		Flags:         FlagAllocated | FlagUnwritten,
	}
	mgr.Insert(ctx, ino, ext)
	ino.AllocatedBlocks = 10
	ino.WrittenBlocks = 0 // Unwritten extents don't count as written
	ino.Size = 10 * BlockSize

	// Truncate to 0
	err := mgr.Truncate(ctx, ino, 0)
	if err != nil {
		t.Fatalf("Truncate to 0 failed: %v", err)
	}

	// AllocatedBlocks should be decremented, but WrittenBlocks stays at 0
	if ino.AllocatedBlocks != 0 {
		t.Errorf("AllocatedBlocks after truncate = %d, want 0", ino.AllocatedBlocks)
	}
	if ino.WrittenBlocks != 0 {
		t.Errorf("WrittenBlocks should stay 0, got %d", ino.WrittenBlocks)
	}
}

func TestFreeExtentAdjustsWrittenBlocks(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Allocate blocks (which sets both AllocatedBlocks and WrittenBlocks)
	_, err := mgr.AllocateBlocks(ctx, ino, 0, 5)
	if err != nil {
		t.Fatalf("AllocateBlocks failed: %v", err)
	}

	if ino.AllocatedBlocks != 5 {
		t.Errorf("AllocatedBlocks after alloc = %d, want 5", ino.AllocatedBlocks)
	}
	if ino.WrittenBlocks != 5 {
		t.Errorf("WrittenBlocks after alloc = %d, want 5", ino.WrittenBlocks)
	}

	// Free the extent
	err = mgr.FreeExtent(ctx, ino, 0)
	if err != nil {
		t.Fatalf("FreeExtent failed: %v", err)
	}

	// Both should be decremented
	if ino.AllocatedBlocks != 0 {
		t.Errorf("AllocatedBlocks after free = %d, want 0", ino.AllocatedBlocks)
	}
	if ino.WrittenBlocks != 0 {
		t.Errorf("WrittenBlocks after free = %d, want 0", ino.WrittenBlocks)
	}
}

func BenchmarkLookup(b *testing.B) {
	mgr, ino, cleanup := createTestSetup(b)
	defer cleanup()

	ctx := context.Background()

	// Insert extents
	mgr.Insert(ctx, ino, &Extent{LogicalBlock: 0, PhysicalBlock: 100, Length: 1000, Flags: FlagAllocated})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Lookup(ino, uint64(i%1000))
	}
}

func BenchmarkInsert(b *testing.B) {
	mgr, _, cleanup := createTestSetup(b)
	defer cleanup()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ino := &inode.Inode{
			Magic:    inode.InodeMagic,
			InodeNum: uint32(i),
		}
		mgr.Insert(ctx, ino, &Extent{
			LogicalBlock:  0,
			PhysicalBlock: uint64(i * 10),
			Length:        5,
			Flags:         FlagAllocated,
		})
	}
}

// ============================================================================
// Unwritten Extent Tests (for fallocate lazy-zero support)
// ============================================================================

// TestAllocateUnwritten_CreatesUnwrittenExtent verifies that AllocateUnwritten
// creates extents with the FlagUnwritten flag set.
func TestAllocateUnwritten_CreatesUnwrittenExtent(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	var uuid [16]byte
	bm := bitmap.NewRegion(dev, 0, 100, nil, uuid)
	mgr := NewManager(dev, bm, nil, 0)
	ctx := context.Background()

	ino := &inode.Inode{
		Magic:    inode.InodeMagic,
		InodeNum: 100,
	}

	// Allocate unwritten extent
	ext, err := mgr.AllocateUnwritten(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateUnwritten failed: %v", err)
	}

	// Verify FlagUnwritten is set along with FlagAllocated
	if ext.Flags&FlagUnwritten == 0 {
		t.Errorf("Expected FlagUnwritten to be set, got flags=%d", ext.Flags)
	}
	if ext.Flags&FlagAllocated == 0 {
		t.Errorf("Expected FlagAllocated to be set, got flags=%d", ext.Flags)
	}

	// Verify extent properties
	if ext.LogicalBlock != 0 {
		t.Errorf("Expected LogicalBlock=0, got %d", ext.LogicalBlock)
	}
	if ext.Length != 1 {
		t.Errorf("Expected Length=1, got %d", ext.Length)
	}
}

// TestAllocateUnwritten_DoesNotIncrementWrittenBlocks verifies that
// AllocateUnwritten only increments AllocatedBlocks, not WrittenBlocks.
func TestAllocateUnwritten_DoesNotIncrementWrittenBlocks(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	var uuid [16]byte
	bm := bitmap.NewRegion(dev, 0, 100, nil, uuid)
	mgr := NewManager(dev, bm, nil, 0)
	ctx := context.Background()

	ino := &inode.Inode{
		Magic:           inode.InodeMagic,
		InodeNum:        100,
		AllocatedBlocks: 0,
		WrittenBlocks:   0,
	}

	// Allocate one unwritten extent (to stay within inline extent limit)
	_, err := mgr.AllocateUnwritten(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateUnwritten failed: %v", err)
	}

	// Verify AllocatedBlocks was incremented
	if ino.AllocatedBlocks != 1 {
		t.Errorf("Expected AllocatedBlocks=1, got %d", ino.AllocatedBlocks)
	}

	// Verify WrittenBlocks was NOT incremented
	if ino.WrittenBlocks != 0 {
		t.Errorf("Expected WrittenBlocks=0 for unwritten extent, got %d", ino.WrittenBlocks)
	}
}

// TestClearUnwritten_ClearsFlagAndIncrementsWritten verifies that ClearUnwritten
// properly clears the flag and increments WrittenBlocks.
func TestClearUnwritten_ClearsFlagAndIncrementsWritten(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	var uuid [16]byte
	bm := bitmap.NewRegion(dev, 0, 100, nil, uuid)
	mgr := NewManager(dev, bm, nil, 0)
	ctx := context.Background()

	ino := &inode.Inode{
		Magic:           inode.InodeMagic,
		InodeNum:        100,
		AllocatedBlocks: 0,
		WrittenBlocks:   0,
	}

	// Allocate unwritten extent
	ext, err := mgr.AllocateUnwritten(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateUnwritten failed: %v", err)
	}

	// Verify starting state
	if ino.WrittenBlocks != 0 {
		t.Fatalf("Expected WrittenBlocks=0 before clear, got %d", ino.WrittenBlocks)
	}

	// Clear unwritten flag
	cleared, err := mgr.ClearUnwritten(ino, ext)
	if err != nil {
		t.Fatalf("ClearUnwritten failed: %v", err)
	}
	if !cleared {
		t.Error("ClearUnwritten should return true for first clear")
	}

	// Verify FlagUnwritten is now cleared
	if ext.Flags&FlagUnwritten != 0 {
		t.Errorf("Expected FlagUnwritten to be cleared, got flags=%d", ext.Flags)
	}

	// FlagAllocated should still be set
	if ext.Flags&FlagAllocated == 0 {
		t.Errorf("Expected FlagAllocated to remain set, got flags=%d", ext.Flags)
	}

	// Verify WrittenBlocks was incremented
	if ino.WrittenBlocks != 1 {
		t.Errorf("Expected WrittenBlocks=1 after clear, got %d", ino.WrittenBlocks)
	}
}

// TestClearUnwritten_NoOpIfAlreadyWritten verifies that ClearUnwritten
// returns false and does nothing if the extent is already written.
func TestClearUnwritten_NoOpIfAlreadyWritten(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	var uuid [16]byte
	bm := bitmap.NewRegion(dev, 0, 100, nil, uuid)
	mgr := NewManager(dev, bm, nil, 0)
	ctx := context.Background()

	ino := &inode.Inode{
		Magic:           inode.InodeMagic,
		InodeNum:        100,
		AllocatedBlocks: 0,
		WrittenBlocks:   0,
	}

	// Use regular AllocateBlocks (which sets only FlagAllocated, not FlagUnwritten)
	ext, err := mgr.AllocateBlocks(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateBlocks failed: %v", err)
	}

	// Verify it's not unwritten
	if ext.Flags&FlagUnwritten != 0 {
		t.Fatalf("Regular AllocateBlocks should not set FlagUnwritten")
	}

	initialWrittenBlocks := ino.WrittenBlocks

	// ClearUnwritten should be no-op
	cleared, err := mgr.ClearUnwritten(ino, ext)
	if err != nil {
		t.Fatalf("ClearUnwritten failed: %v", err)
	}
	if cleared {
		t.Error("ClearUnwritten should return false for already-written extent")
	}

	// WrittenBlocks should be unchanged
	if ino.WrittenBlocks != initialWrittenBlocks {
		t.Errorf("Expected WrittenBlocks to remain %d, got %d",
			initialWrittenBlocks, ino.WrittenBlocks)
	}
}

// TestLookupPhysical_ReturnsZeroForUnwritten verifies that LookupPhysical
// returns 0 (hole) for unwritten extents.
func TestLookupPhysical_ReturnsZeroForUnwritten(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	var uuid [16]byte
	bm := bitmap.NewRegion(dev, 0, 100, nil, uuid)
	mgr := NewManager(dev, bm, nil, 0)
	ctx := context.Background()

	ino := &inode.Inode{
		Magic:    inode.InodeMagic,
		InodeNum: 100,
	}

	// Allocate unwritten extent
	ext, err := mgr.AllocateUnwritten(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateUnwritten failed: %v", err)
	}

	// LookupPhysical should return 0 (treat as hole for reads)
	physBlock, err := mgr.LookupPhysical(ino, 0)
	if err != nil {
		t.Fatalf("LookupPhysical failed: %v", err)
	}
	if physBlock != 0 {
		t.Errorf("Expected physBlock=0 for unwritten extent, got %d", physBlock)
	}

	// But Lookup should find the extent
	foundExt, _, err := mgr.Lookup(ino, 0)
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if foundExt.PhysicalBlock != ext.PhysicalBlock {
		t.Errorf("Lookup should find the extent, got physBlock=%d", foundExt.PhysicalBlock)
	}

	// Now clear unwritten and LookupPhysical should return the real block
	cleared, err := mgr.ClearUnwritten(ino, foundExt)
	if err != nil {
		t.Fatalf("ClearUnwritten failed: %v", err)
	}
	if !cleared {
		t.Error("ClearUnwritten should return true for first clear")
	}

	physBlock, err = mgr.LookupPhysical(ino, 0)
	if err != nil {
		t.Fatalf("LookupPhysical after clear failed: %v", err)
	}
	// Note: ext.PhysicalBlock could be 0 (valid block number), so we compare directly
	if physBlock != ext.PhysicalBlock {
		t.Errorf("Expected physBlock=%d after clearing unwritten, got %d", ext.PhysicalBlock, physBlock)
	}
}

// TestAllocateUnwritten_IdempotentForExisting verifies that AllocateUnwritten
// is a no-op for already allocated blocks.
func TestAllocateUnwritten_IdempotentForExisting(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	var uuid [16]byte
	bm := bitmap.NewRegion(dev, 0, 100, nil, uuid)
	mgr := NewManager(dev, bm, nil, 0)
	ctx := context.Background()

	ino := &inode.Inode{
		Magic:    inode.InodeMagic,
		InodeNum: 100,
	}

	// Allocate first time
	ext1, err := mgr.AllocateUnwritten(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("First AllocateUnwritten failed: %v", err)
	}

	allocatedBefore := ino.AllocatedBlocks

	// Allocate again at same block
	ext2, err := mgr.AllocateUnwritten(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("Second AllocateUnwritten failed: %v", err)
	}

	// Should return the existing extent
	if ext2.PhysicalBlock != ext1.PhysicalBlock {
		t.Errorf("Expected same physical block, got %d vs %d",
			ext1.PhysicalBlock, ext2.PhysicalBlock)
	}

	// AllocatedBlocks should not change
	if ino.AllocatedBlocks != allocatedBefore {
		t.Errorf("Expected AllocatedBlocks to remain %d, got %d",
			allocatedBefore, ino.AllocatedBlocks)
	}
}

// TestFreeExtent_HandlesUnwrittenCorrectly verifies that FreeExtent
// properly handles unwritten extents (doesn't decrement WrittenBlocks).
func TestFreeExtent_HandlesUnwrittenCorrectly(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	var uuid [16]byte
	bm := bitmap.NewRegion(dev, 0, 100, nil, uuid)
	mgr := NewManager(dev, bm, nil, 0)
	ctx := context.Background()

	ino := &inode.Inode{
		Magic:    inode.InodeMagic,
		InodeNum: 100,
	}

	// Allocate unwritten extent
	_, err := mgr.AllocateUnwritten(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateUnwritten failed: %v", err)
	}

	// Verify WrittenBlocks is 0 (unwritten)
	if ino.WrittenBlocks != 0 {
		t.Fatalf("Expected WrittenBlocks=0, got %d", ino.WrittenBlocks)
	}

	// Free the extent
	err = mgr.FreeExtent(ctx, ino, 0)
	if err != nil {
		t.Fatalf("FreeExtent failed: %v", err)
	}

	// AllocatedBlocks should be decremented
	if ino.AllocatedBlocks != 0 {
		t.Errorf("Expected AllocatedBlocks=0, got %d", ino.AllocatedBlocks)
	}

	// WrittenBlocks should still be 0 (wasn't written, so no decrement)
	if ino.WrittenBlocks != 0 {
		t.Errorf("Expected WrittenBlocks=0, got %d", ino.WrittenBlocks)
	}
}

// ============================================================================
// Copy-on-Write (CoW) Tests (for snapshot support)
// ============================================================================

// createTestSetupWithRefcount creates a test setup including a refcount region.
func createTestSetupWithRefcount(tb testing.TB) (*Manager, *refcount.Region, *inode.Inode, func()) {
	tb.Helper()

	// Need larger device for refcount region (at least 10MB)
	dev, cleanup := createTestDevice(tb, 100<<20) // 100MB

	var uuid [16]byte
	// Layout: 0-1MB bitmap, 1-2MB refcount, 2MB+ data blocks
	bitmapOffset := uint64(0)
	refcountOffset := uint64(1 << 20) // 1MB
	refcountSize := uint64(1 << 20)   // 1MB for refcounts

	bm := bitmap.NewRegion(dev, bitmapOffset, 1000, nil, uuid) // 1000 blocks
	refcountReg := refcount.NewRegion(dev, refcountOffset, refcountSize, nil)
	mgr := NewManager(dev, bm, nil, 2<<20)

	ino := &inode.Inode{
		Magic:    inode.InodeMagic,
		InodeNum: 10,
		Mode:     inode.S_IFREG | 0o644,
	}

	return mgr, refcountReg, ino, cleanup
}

// inlineExtentToExtent converts an inode.InlineExtent to an Extent.
func inlineExtentToExtent(ie *inode.InlineExtent) *Extent {
	return &Extent{
		LogicalBlock:  ie.LogicalBlock,
		PhysicalBlock: ie.PhysicalBlock,
		Length:        ie.Length,
		Flags:         ie.Flags,
	}
}

// TestNeedsCoW_NoRefcountRegion verifies NeedsCoW returns false when no refcount region exists.
func TestNeedsCoW_NoRefcountRegion(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Allocate block and set FlagShared
	ext, err := mgr.AllocateBlocks(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateBlocks failed: %v", err)
	}
	ext.Flags |= FlagShared

	// NeedsCoW should return false when refcountReg is nil
	needsCoW, err := mgr.NeedsCoW(ctx, ext, nil)
	if err != nil {
		t.Fatalf("NeedsCoW failed: %v", err)
	}
	if needsCoW {
		t.Error("NeedsCoW should return false when refcountReg is nil")
	}
}

// TestNeedsCoW_NotShared verifies NeedsCoW returns false for non-shared extents.
func TestNeedsCoW_NotShared(t *testing.T) {
	mgr, refcountReg, ino, cleanup := createTestSetupWithRefcount(t)
	defer cleanup()

	ctx := context.Background()

	// Allocate block without FlagShared
	ext, err := mgr.AllocateBlocks(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateBlocks failed: %v", err)
	}

	// NeedsCoW should return false for non-shared extent
	needsCoW, err := mgr.NeedsCoW(ctx, ext, refcountReg)
	if err != nil {
		t.Fatalf("NeedsCoW failed: %v", err)
	}
	if needsCoW {
		t.Error("NeedsCoW should return false for non-shared extent")
	}
}

// TestNeedsCoW_SharedWithRefCountOne verifies NeedsCoW returns false when RefCount == 1.
func TestNeedsCoW_SharedWithRefCountOne(t *testing.T) {
	mgr, refcountReg, ino, cleanup := createTestSetupWithRefcount(t)
	defer cleanup()

	ctx := context.Background()

	// Allocate block and set FlagShared
	ext, err := mgr.AllocateBlocks(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateBlocks failed: %v", err)
	}
	ext.Flags |= FlagShared

	// Set refcount to 1 (only one reference, no CoW needed)
	err = refcountReg.Set(ctx, ext.PhysicalBlock, 1, 0)
	if err != nil {
		t.Fatalf("refcount.Set failed: %v", err)
	}

	// NeedsCoW should return false for RefCount == 1
	needsCoW, err := mgr.NeedsCoW(ctx, ext, refcountReg)
	if err != nil {
		t.Fatalf("NeedsCoW failed: %v", err)
	}
	if needsCoW {
		t.Error("NeedsCoW should return false when RefCount == 1")
	}
}

// TestNeedsCoW_SharedWithRefCountTwo verifies NeedsCoW returns true when RefCount > 1.
func TestNeedsCoW_SharedWithRefCountTwo(t *testing.T) {
	mgr, refcountReg, ino, cleanup := createTestSetupWithRefcount(t)
	defer cleanup()

	ctx := context.Background()

	// Allocate block and set FlagShared
	ext, err := mgr.AllocateBlocks(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateBlocks failed: %v", err)
	}
	ext.Flags |= FlagShared

	// Set refcount to 2 (shared with snapshot)
	err = refcountReg.Set(ctx, ext.PhysicalBlock, 2, 0)
	if err != nil {
		t.Fatalf("refcount.Set failed: %v", err)
	}

	// NeedsCoW should return true for RefCount > 1
	needsCoW, err := mgr.NeedsCoW(ctx, ext, refcountReg)
	if err != nil {
		t.Fatalf("NeedsCoW failed: %v", err)
	}
	if !needsCoW {
		t.Error("NeedsCoW should return true when RefCount > 1")
	}
}

// TestMarkAllShared verifies MarkAllShared sets FlagShared on all extents.
func TestMarkAllShared(t *testing.T) {
	mgr, ino, cleanup := createTestSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Allocate two extents
	ext1, err := mgr.AllocateBlocks(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateBlocks(0) failed: %v", err)
	}
	ext2, err := mgr.AllocateBlocks(ctx, ino, 1, 1)
	if err != nil {
		t.Fatalf("AllocateBlocks(1) failed: %v", err)
	}

	// Verify neither is shared initially
	if ext1.Flags&FlagShared != 0 {
		t.Error("ext1 should not be shared initially")
	}
	if ext2.Flags&FlagShared != 0 {
		t.Error("ext2 should not be shared initially")
	}

	// Mark all shared
	if err := mgr.MarkAllShared(ino); err != nil {
		t.Fatalf("MarkAllShared failed: %v", err)
	}

	extents := listExtents(t, mgr, ino)
	if len(extents) == 0 {
		t.Fatal("expected extents after allocation")
	}
	for _, ext := range extents {
		if ext.Flags&FlagShared == 0 {
			t.Errorf("extent %+v should be shared after MarkAllShared", *ext)
		}
	}
}

// TestCopyOnWrite_Success verifies CopyOnWrite allocates a new block and copies data.
func TestCopyOnWrite_Success(t *testing.T) {
	mgr, refcountReg, ino, cleanup := createTestSetupWithRefcount(t)
	defer cleanup()

	ctx := context.Background()

	// Allocate a single block
	ext, err := mgr.AllocateBlocks(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateBlocks failed: %v", err)
	}

	originalPhysBlock := ext.PhysicalBlock

	// Set up as shared with refcount > 1
	ext.Flags |= FlagShared
	err = refcountReg.Set(ctx, ext.PhysicalBlock, 2, 0)
	if err != nil {
		t.Fatalf("refcount.Set failed: %v", err)
	}

	// Update inode's inline extent to reflect shared flag
	ino.InlineExtents[0].Flags |= FlagShared

	// Data offset for writes (2MB in our test layout)
	dataOffset := uint64(2 << 20)

	// Convert inline extent to Extent for CopyOnWrite
	extPtr := inlineExtentToExtent(&ino.InlineExtents[0])

	// Perform CoW
	result, err := mgr.CopyOnWrite(ctx, ino, extPtr, 0, dataOffset, refcountReg)
	if err != nil {
		t.Fatalf("CopyOnWrite failed: %v", err)
	}

	// Verify result
	if !result.WasShared {
		t.Error("CopyOnWrite should report WasShared=true")
	}
	if result.OldPhysBlock != originalPhysBlock {
		t.Errorf("OldPhysBlock = %d, want %d", result.OldPhysBlock, originalPhysBlock)
	}
	if result.NewPhysBlock == originalPhysBlock {
		t.Error("NewPhysBlock should be different from OldPhysBlock")
	}

	// Verify extent was updated
	if ino.InlineExtents[0].PhysicalBlock != result.NewPhysBlock {
		t.Errorf("Extent PhysicalBlock = %d, want %d",
			ino.InlineExtents[0].PhysicalBlock, result.NewPhysBlock)
	}

	// Verify FlagShared was cleared
	if ino.InlineExtents[0].Flags&FlagShared != 0 {
		t.Error("FlagShared should be cleared after CoW")
	}
}

// TestCopyOnWrite_AlreadyPrivate verifies CoW returns quickly for non-shared blocks.
func TestCopyOnWrite_AlreadyPrivate(t *testing.T) {
	mgr, refcountReg, ino, cleanup := createTestSetupWithRefcount(t)
	defer cleanup()

	ctx := context.Background()

	// Allocate a single block (NOT shared)
	ext, err := mgr.AllocateBlocks(ctx, ino, 0, 1)
	if err != nil {
		t.Fatalf("AllocateBlocks failed: %v", err)
	}

	originalPhysBlock := ext.PhysicalBlock

	// Set refcount to 1 (private)
	err = refcountReg.Set(ctx, ext.PhysicalBlock, 1, 0)
	if err != nil {
		t.Fatalf("refcount.Set failed: %v", err)
	}

	// Data offset for writes
	dataOffset := uint64(2 << 20)

	// Convert inline extent to Extent for CopyOnWrite
	extPtr := inlineExtentToExtent(&ino.InlineExtents[0])

	// Perform CoW - should be no-op
	result, err := mgr.CopyOnWrite(ctx, ino, extPtr, 0, dataOffset, refcountReg)
	if err != nil {
		t.Fatalf("CopyOnWrite failed: %v", err)
	}

	// Verify no actual CoW was performed
	if result.WasShared {
		t.Error("CopyOnWrite should report WasShared=false for private block")
	}

	// Block should remain the same (extPtr points to a copy, check inode directly)
	if ino.InlineExtents[0].PhysicalBlock != originalPhysBlock {
		t.Error("Extent PhysicalBlock should remain unchanged for private block")
	}
}

// TestCopyOnWrite_MultiBlockExtent verifies CoW of one block splits a shared
// multi-block extent and leaves the other blocks shared.
func TestCopyOnWrite_MultiBlockExtent(t *testing.T) {
	mgr, refcountReg, ino, cleanup := createTestSetupWithRefcount(t)
	defer cleanup()

	ctx := context.Background()

	ext := &Extent{LogicalBlock: 0, PhysicalBlock: 50, Length: 5, Flags: FlagAllocated | FlagShared}
	if err := mgr.Insert(ctx, ino, ext); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := refcountReg.Set(ctx, 52, 2, 0); err != nil {
		t.Fatalf("refcount.Set failed: %v", err)
	}

	result, err := mgr.CopyOnWrite(ctx, ino, ext, 2, 2<<20, refcountReg)
	if err != nil {
		t.Fatalf("CopyOnWrite failed: %v", err)
	}
	if !result.WasShared || result.OldPhysBlock != 52 || result.NewPhysBlock == 52 {
		t.Fatalf("unexpected CoW result %+v", result)
	}

	want := []Extent{
		{LogicalBlock: 0, PhysicalBlock: 50, Length: 2, Flags: FlagAllocated | FlagShared},
		{LogicalBlock: 2, PhysicalBlock: result.NewPhysBlock, Length: 1, Flags: FlagAllocated},
		{LogicalBlock: 3, PhysicalBlock: 53, Length: 2, Flags: FlagAllocated | FlagShared},
	}
	got := listExtents(t, mgr, ino)
	if len(got) != len(want) {
		t.Fatalf("got %d extents, want %d", len(got), len(want))
	}
	for i := range want {
		if *got[i] != want[i] {
			t.Errorf("extent %d = %+v, want %+v", i, *got[i], want[i])
		}
	}
}

func listExtents(tb testing.TB, mgr *Manager, ino *inode.Inode) []*Extent {
	tb.Helper()
	extents, err := mgr.ListExtents(ino)
	if err != nil {
		tb.Fatalf("ListExtents failed: %v", err)
	}
	return extents
}
