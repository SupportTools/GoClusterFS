package bitmap

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// createTestDevice creates a test file for O_DIRECT testing.
func createTestDevice(tb testing.TB, size int64) (*storage.Device, func()) {
	tb.Helper()

	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_bitmap.bin")

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
	// Verify constants are correct
	if SectorSize != storage.SectorSize {
		t.Errorf("SectorSize = %d, want %d", SectorSize, storage.SectorSize)
	}

	if BitsPerSector != (SectorSize-ChecksumSize)*8 {
		t.Errorf("BitsPerSector = %d, want %d", BitsPerSector, (SectorSize-ChecksumSize)*8)
	}

	if BytesPerSector != SectorSize-ChecksumSize {
		t.Errorf("BytesPerSector = %d, want %d", BytesPerSector, SectorSize-ChecksumSize)
	}
}

func TestNewRegion(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20) // 10MB
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	region := NewRegion(dev, 0, 1000, nil, uuid)

	if region == nil {
		t.Fatal("NewRegion returned nil")
	}

	if region.TotalBlocks() != 1000 {
		t.Errorf("TotalBlocks() = %d, want 1000", region.TotalBlocks())
	}

	// Initially all blocks should be free (cached value)
	if region.FreeCount() != 1000 {
		t.Errorf("FreeCount() = %d, want 1000", region.FreeCount())
	}
}

func TestAllocSingle(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 1000, nil, uuid)

	ctx := context.Background()

	// Allocate a single block
	blocks, err := region.Alloc(ctx, 1)
	if err != nil {
		t.Fatalf("Alloc(1) failed: %v", err)
	}

	if len(blocks) != 1 {
		t.Fatalf("Alloc(1) returned %d blocks, want 1", len(blocks))
	}

	// First allocated block should be 0
	if blocks[0] != 0 {
		t.Errorf("First allocated block = %d, want 0", blocks[0])
	}

	// Check that block is now allocated
	isFree, err := region.IsFree(blocks[0])
	if err != nil {
		t.Fatalf("IsFree failed: %v", err)
	}
	if isFree {
		t.Error("Allocated block should not be free")
	}
}

func TestAllocMultiple(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 1000, nil, uuid)

	ctx := context.Background()

	// Allocate 10 blocks
	blocks, err := region.Alloc(ctx, 10)
	if err != nil {
		t.Fatalf("Alloc(10) failed: %v", err)
	}

	if len(blocks) != 10 {
		t.Fatalf("Alloc(10) returned %d blocks, want 10", len(blocks))
	}

	// Blocks should be sequential (first-fit)
	for i, block := range blocks {
		if block != uint64(i) {
			t.Errorf("blocks[%d] = %d, want %d", i, block, i)
		}
	}

	// All should be allocated
	for _, block := range blocks {
		allocated, err := region.IsAllocated(block)
		if err != nil {
			t.Fatalf("IsAllocated(%d) failed: %v", block, err)
		}
		if !allocated {
			t.Errorf("Block %d should be allocated", block)
		}
	}
}

func TestAllocFree(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 1000, nil, uuid)

	ctx := context.Background()

	// Allocate some blocks
	blocks, err := region.Alloc(ctx, 5)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	// Free the middle one
	err = region.Free(ctx, blocks[2])
	if err != nil {
		t.Fatalf("Free failed: %v", err)
	}

	// Check it's free
	isFree, err := region.IsFree(blocks[2])
	if err != nil {
		t.Fatalf("IsFree failed: %v", err)
	}
	if !isFree {
		t.Error("Freed block should be free")
	}

	// Allocate one more - should get the freed block
	newBlocks, err := region.Alloc(ctx, 1)
	if err != nil {
		t.Fatalf("Second Alloc failed: %v", err)
	}

	if newBlocks[0] != blocks[2] {
		t.Errorf("Reallocation got block %d, want %d", newBlocks[0], blocks[2])
	}
}

func TestAllocZero(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 1000, nil, uuid)

	ctx := context.Background()

	// Allocate zero blocks should succeed
	blocks, err := region.Alloc(ctx, 0)
	if err != nil {
		t.Fatalf("Alloc(0) failed: %v", err)
	}

	if len(blocks) != 0 {
		t.Errorf("Alloc(0) returned %d blocks, want 0", len(blocks))
	}
}

func TestAllocExhaust(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	// Small region - only 10 blocks
	region := NewRegion(dev, 0, 10, nil, uuid)

	ctx := context.Background()

	// Allocate all blocks
	blocks, err := region.Alloc(ctx, 10)
	if err != nil {
		t.Fatalf("Alloc(10) failed: %v", err)
	}

	if len(blocks) != 10 {
		t.Fatalf("Alloc(10) returned %d blocks", len(blocks))
	}

	// Try to allocate one more - should fail
	_, err = region.Alloc(ctx, 1)
	if err != ErrNoFreeBlocks {
		t.Errorf("Expected ErrNoFreeBlocks, got %v", err)
	}
}

func TestFreeOutOfRange(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 100, nil, uuid)

	ctx := context.Background()

	// Free a block that's out of range
	err := region.Free(ctx, 100)
	if err != ErrBlockOutOfRange {
		t.Errorf("Expected ErrBlockOutOfRange, got %v", err)
	}
}

func TestFreeAlreadyFree(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 100, nil, uuid)

	ctx := context.Background()

	// Try to free a block that was never allocated
	err := region.Free(ctx, 0)
	if err != ErrAlreadyFree {
		t.Errorf("Expected ErrAlreadyFree, got %v", err)
	}
}

func TestIsFreeOutOfRange(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 100, nil, uuid)

	_, err := region.IsFree(100)
	if err != ErrBlockOutOfRange {
		t.Errorf("Expected ErrBlockOutOfRange, got %v", err)
	}
}

func TestScan(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 100, nil, uuid)

	ctx := context.Background()

	// Allocate 30 blocks
	_, err := region.Alloc(ctx, 30)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	// Scan and verify counts
	free, allocated, err := region.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if free != 70 {
		t.Errorf("Scan: free = %d, want 70", free)
	}
	if allocated != 30 {
		t.Errorf("Scan: allocated = %d, want 30", allocated)
	}

	// FreeCount should be updated
	if region.FreeCount() != 70 {
		t.Errorf("FreeCount() = %d, want 70", region.FreeCount())
	}
}

func TestMarkAllocated(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 100, nil, uuid)

	ctx := context.Background()

	// Mark block 50 as allocated
	err := region.MarkAllocated(ctx, 50)
	if err != nil {
		t.Fatalf("MarkAllocated failed: %v", err)
	}

	// Check it's allocated
	allocated, err := region.IsAllocated(50)
	if err != nil {
		t.Fatalf("IsAllocated failed: %v", err)
	}
	if !allocated {
		t.Error("Block 50 should be allocated")
	}

	// Try to mark again - should fail
	err = region.MarkAllocated(ctx, 50)
	if err != ErrAlreadyAllocated {
		t.Errorf("Expected ErrAlreadyAllocated, got %v", err)
	}
}

func TestMarkAllocatedOutOfRange(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 100, nil, uuid)

	ctx := context.Background()

	err := region.MarkAllocated(ctx, 100)
	if err != ErrBlockOutOfRange {
		t.Errorf("Expected ErrBlockOutOfRange, got %v", err)
	}
}

func TestSizeForBlocks(t *testing.T) {
	tests := []struct {
		totalBlocks uint64
		wantSectors uint64
	}{
		{0, 0},
		{1, 1},
		{BitsPerSector, 1},
		{BitsPerSector + 1, 2},
		{BitsPerSector * 2, 2},
		{BitsPerSector * 3, 3},
	}

	for _, tt := range tests {
		size := SizeForBlocks(tt.totalBlocks)
		wantSize := tt.wantSectors * SectorSize
		if size != wantSize {
			t.Errorf("SizeForBlocks(%d) = %d, want %d", tt.totalBlocks, size, wantSize)
		}
	}
}

func TestBlocksPerSector(t *testing.T) {
	if BlocksPerSector() != BitsPerSector {
		t.Errorf("BlocksPerSector() = %d, want %d", BlocksPerSector(), BitsPerSector)
	}
}

func TestAllocatedCount(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 100, nil, uuid)

	ctx := context.Background()

	// Initially 0 allocated (cached)
	if region.AllocatedCount() != 0 {
		t.Errorf("AllocatedCount() = %d, want 0", region.AllocatedCount())
	}

	// Allocate 25 blocks
	_, err := region.Alloc(ctx, 25)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	if region.AllocatedCount() != 25 {
		t.Errorf("AllocatedCount() = %d, want 25", region.AllocatedCount())
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_bitmap_persist.bin")
	defer os.Remove(testFile)

	// Create and allocate
	func() {
		f, err := os.Create(testFile)
		if err != nil {
			t.Skipf("Cannot create test file: %v", err)
		}
		if err := f.Truncate(10 << 20); err != nil {
			f.Close()
			t.Skipf("Cannot truncate: %v", err)
		}
		f.Close()

		dev, err := storage.Open(testFile)
		if err != nil {
			t.Skipf("O_DIRECT not supported: %v", err)
		}
		defer dev.Close()

		var uuid [16]byte
		region := NewRegion(dev, 0, 100, nil, uuid)

		ctx := context.Background()

		// Allocate blocks 0-9 and 50-59
		region.Alloc(ctx, 10)
		for i := uint64(50); i < 60; i++ {
			region.MarkAllocated(ctx, i)
		}
	}()

	// Reopen and verify
	dev, err := storage.Open(testFile)
	if err != nil {
		t.Skipf("Cannot reopen: %v", err)
	}
	defer dev.Close()

	var uuid [16]byte
	region := NewRegion(dev, 0, 100, nil, uuid)

	// Blocks 0-9 should be allocated
	for i := uint64(0); i < 10; i++ {
		allocated, err := region.IsAllocated(i)
		if err != nil {
			t.Fatalf("IsAllocated(%d) failed: %v", i, err)
		}
		if !allocated {
			t.Errorf("Block %d should be allocated", i)
		}
	}

	// Blocks 10-49 should be free
	for i := uint64(10); i < 50; i++ {
		isFree, err := region.IsFree(i)
		if err != nil {
			t.Fatalf("IsFree(%d) failed: %v", i, err)
		}
		if !isFree {
			t.Errorf("Block %d should be free", i)
		}
	}

	// Blocks 50-59 should be allocated
	for i := uint64(50); i < 60; i++ {
		allocated, err := region.IsAllocated(i)
		if err != nil {
			t.Fatalf("IsAllocated(%d) failed: %v", i, err)
		}
		if !allocated {
			t.Errorf("Block %d should be allocated", i)
		}
	}
}

func TestMultipleSectors(t *testing.T) {
	dev, cleanup := createTestDevice(t, 100<<20) // 100MB for many sectors
	defer cleanup()

	var uuid [16]byte
	// Create region with blocks spanning multiple sectors
	totalBlocks := uint64(BitsPerSector*3 + 100) // 3 full sectors + partial
	region := NewRegion(dev, 0, totalBlocks, nil, uuid)

	ctx := context.Background()

	// Allocate blocks from different sectors
	blocksToAlloc := []uint64{0, BitsPerSector, BitsPerSector * 2, totalBlocks - 1}
	for _, block := range blocksToAlloc {
		err := region.MarkAllocated(ctx, block)
		if err != nil {
			t.Fatalf("MarkAllocated(%d) failed: %v", block, err)
		}
	}

	// Verify all are allocated
	for _, block := range blocksToAlloc {
		allocated, err := region.IsAllocated(block)
		if err != nil {
			t.Fatalf("IsAllocated(%d) failed: %v", block, err)
		}
		if !allocated {
			t.Errorf("Block %d should be allocated", block)
		}
	}

	// Scan and verify
	free, allocated, err := region.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	expectedAllocated := uint64(len(blocksToAlloc))
	expectedFree := totalBlocks - expectedAllocated

	if allocated != expectedAllocated {
		t.Errorf("Scan: allocated = %d, want %d", allocated, expectedAllocated)
	}
	if free != expectedFree {
		t.Errorf("Scan: free = %d, want %d", free, expectedFree)
	}
}

func BenchmarkAlloc(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, uint64(b.N*10), nil, uuid)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region.Alloc(ctx, 1)
	}
}

func BenchmarkFree(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, uint64(b.N), nil, uuid)

	ctx := context.Background()

	// Pre-allocate all blocks
	blocks, _ := region.Alloc(ctx, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region.Free(ctx, blocks[i])
	}
}

func BenchmarkIsFree(b *testing.B) {
	dev, cleanup := createTestDevice(b, 10<<20)
	defer cleanup()

	var uuid [16]byte
	region := NewRegion(dev, 0, 10000, nil, uuid)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region.IsFree(uint64(i % 10000))
	}
}
