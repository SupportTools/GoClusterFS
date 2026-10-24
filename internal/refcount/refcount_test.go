package refcount

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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

	testFile := filepath.Join(tmpDir, "test_refcount.bin")

	f, err := os.Create(testFile)
	if err != nil {
		tb.Skipf("Cannot create test file: %v", err)
	}

	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		_ = os.Remove(testFile)
		tb.Skipf("Cannot truncate test file: %v", err)
	}
	if err := f.Close(); err != nil {
		tb.Fatal(err)
	}

	dev, err := storage.Open(testFile)
	if err != nil {
		_ = os.Remove(testFile)
		tb.Skipf("O_DIRECT not supported: %v", err)
	}

	cleanup := func() {
		_ = dev.Close()
		_ = os.Remove(testFile)
	}

	return dev, cleanup
}

func TestConstants(t *testing.T) {
	// Verify entry size is 8 bytes
	if EntrySize != 8 {
		t.Errorf("EntrySize = %d, want 8", EntrySize)
	}

	// Verify sector size matches storage
	if SectorSize != storage.SectorSize {
		t.Errorf("SectorSize = %d, want %d", SectorSize, storage.SectorSize)
	}

	// Verify entries per sector calculation
	expectedEntries := (SectorSize - ChecksumSize) / EntrySize
	if EntriesPerSector != expectedEntries {
		t.Errorf("EntriesPerSector = %d, want %d", EntriesPerSector, expectedEntries)
	}

	// Verify 511 entries per 4KB sector
	if EntriesPerSector != 511 {
		t.Errorf("EntriesPerSector = %d, want 511", EntriesPerSector)
	}

	// Verify bytes per sector
	if BytesPerSector != EntriesPerSector*EntrySize {
		t.Errorf("BytesPerSector = %d, want %d", BytesPerSector, EntriesPerSector*EntrySize)
	}
}

func TestEntryHelpers(t *testing.T) {
	tests := []struct {
		name        string
		entry       Entry
		isShared    bool
		isExclusive bool
		isFree      bool
		needsCoW    bool
	}{
		{
			name:        "free entry",
			entry:       Entry{RefCount: 0, Flags: FlagNone},
			isShared:    false,
			isExclusive: false,
			isFree:      true,
			needsCoW:    false,
		},
		{
			name:        "exclusive entry",
			entry:       Entry{RefCount: 1, Flags: FlagNone},
			isShared:    false,
			isExclusive: true,
			isFree:      false,
			needsCoW:    false,
		},
		{
			name:        "shared entry (2 refs)",
			entry:       Entry{RefCount: 2, Flags: FlagNone},
			isShared:    true,
			isExclusive: false,
			isFree:      false,
			needsCoW:    true,
		},
		{
			name:        "shared entry (many refs)",
			entry:       Entry{RefCount: 100, Flags: FlagNone},
			isShared:    true,
			isExclusive: false,
			isFree:      false,
			needsCoW:    true,
		},
		{
			name:        "exclusive with CoW pending flag",
			entry:       Entry{RefCount: 1, Flags: FlagCowPending},
			isShared:    false,
			isExclusive: true,
			isFree:      false,
			needsCoW:    true,
		},
		{
			name:        "snapshot entry",
			entry:       Entry{RefCount: 1, Flags: FlagSnapshot},
			isShared:    false,
			isExclusive: true,
			isFree:      false,
			needsCoW:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.entry.IsShared(); got != tt.isShared {
				t.Errorf("IsShared() = %v, want %v", got, tt.isShared)
			}
			if got := tt.entry.IsExclusive(); got != tt.isExclusive {
				t.Errorf("IsExclusive() = %v, want %v", got, tt.isExclusive)
			}
			if got := tt.entry.IsFree(); got != tt.isFree {
				t.Errorf("IsFree() = %v, want %v", got, tt.isFree)
			}
			if got := tt.entry.NeedsCoW(); got != tt.needsCoW {
				t.Errorf("NeedsCoW() = %v, want %v", got, tt.needsCoW)
			}
		})
	}
}

func TestNewRegion(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20) // 10MB
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)

	if region == nil {
		t.Fatal("NewRegion returned nil")
	}

	if region.TotalBlocks() != 1000 {
		t.Errorf("TotalBlocks() = %d, want 1000", region.TotalBlocks())
	}
}

func TestGetEntry(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Get entry from uninitialized region - should be zero
	entry, err := region.Get(ctx, 0)
	if err != nil {
		t.Fatalf("Get(0) failed: %v", err)
	}

	if entry.RefCount != 0 {
		t.Errorf("Initial RefCount = %d, want 0", entry.RefCount)
	}
	if entry.Flags != FlagNone {
		t.Errorf("Initial Flags = %d, want 0", entry.Flags)
	}
}

func TestGetOutOfRange(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(100), 100, nil)
	ctx := context.Background()

	_, err := region.Get(ctx, 100)
	if err != ErrBlockOutOfRange {
		t.Errorf("Expected ErrBlockOutOfRange, got %v", err)
	}

	_, err = region.Get(ctx, 1000)
	if err != ErrBlockOutOfRange {
		t.Errorf("Expected ErrBlockOutOfRange, got %v", err)
	}
}

func TestIncrement(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Increment from 0
	count, err := region.Increment(ctx, 0)
	if err != nil {
		t.Fatalf("Increment(0) failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Increment returned %d, want 1", count)
	}

	// Increment again
	count, err = region.Increment(ctx, 0)
	if err != nil {
		t.Fatalf("Second Increment(0) failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Second Increment returned %d, want 2", count)
	}

	// Verify via Get
	entry, err := region.Get(ctx, 0)
	if err != nil {
		t.Fatalf("Get(0) failed: %v", err)
	}
	if entry.RefCount != 2 {
		t.Errorf("Entry.RefCount = %d, want 2", entry.RefCount)
	}
}

func TestIncrementOverflow(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Set to max value
	err := region.Set(ctx, 0, 0xFFFFFFFF, FlagNone)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Try to increment - should overflow
	_, err = region.Increment(ctx, 0)
	if err != ErrRefCountOverflow {
		t.Errorf("Expected ErrRefCountOverflow, got %v", err)
	}
}

func TestIncrementOutOfRange(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(100), 100, nil)
	ctx := context.Background()

	_, err := region.Increment(ctx, 100)
	if err != ErrBlockOutOfRange {
		t.Errorf("Expected ErrBlockOutOfRange, got %v", err)
	}
}

func TestDecrement(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Set initial count to 5
	err := region.Set(ctx, 0, 5, FlagNone)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Decrement
	count, err := region.Decrement(ctx, 0)
	if err != nil {
		t.Fatalf("Decrement(0) failed: %v", err)
	}
	if count != 4 {
		t.Errorf("Decrement returned %d, want 4", count)
	}

	// Verify via Get
	entry, err := region.Get(ctx, 0)
	if err != nil {
		t.Fatalf("Get(0) failed: %v", err)
	}
	if entry.RefCount != 4 {
		t.Errorf("Entry.RefCount = %d, want 4", entry.RefCount)
	}
}

func TestDecrementUnderflow(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Try to decrement from 0 - should underflow
	_, err := region.Decrement(ctx, 0)
	if err != ErrRefCountUnderflow {
		t.Errorf("Expected ErrRefCountUnderflow, got %v", err)
	}
}

func TestDecrementOutOfRange(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(100), 100, nil)
	ctx := context.Background()

	_, err := region.Decrement(ctx, 100)
	if err != ErrBlockOutOfRange {
		t.Errorf("Expected ErrBlockOutOfRange, got %v", err)
	}
}

func TestSet(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Set count and flags
	err := region.Set(ctx, 50, 10, FlagSnapshot|FlagMetadata)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify
	entry, err := region.Get(ctx, 50)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if entry.RefCount != 10 {
		t.Errorf("RefCount = %d, want 10", entry.RefCount)
	}
	if entry.Flags != FlagSnapshot|FlagMetadata {
		t.Errorf("Flags = %d, want %d", entry.Flags, FlagSnapshot|FlagMetadata)
	}
}

func TestSetOutOfRange(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(100), 100, nil)
	ctx := context.Background()

	err := region.Set(ctx, 100, 1, FlagNone)
	if err != ErrBlockOutOfRange {
		t.Errorf("Expected ErrBlockOutOfRange, got %v", err)
	}
}

func TestSetFlags(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Set initial count
	err := region.Set(ctx, 0, 5, FlagNone)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Update just the flags
	err = region.SetFlags(ctx, 0, FlagCowPending)
	if err != nil {
		t.Fatalf("SetFlags failed: %v", err)
	}

	// Verify count preserved and flags updated
	entry, err := region.Get(ctx, 0)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if entry.RefCount != 5 {
		t.Errorf("RefCount = %d, want 5 (should be preserved)", entry.RefCount)
	}
	if entry.Flags != FlagCowPending {
		t.Errorf("Flags = %d, want %d", entry.Flags, FlagCowPending)
	}
}

func TestSetFlagsOutOfRange(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(100), 100, nil)
	ctx := context.Background()

	err := region.SetFlags(ctx, 100, FlagSnapshot)
	if err != ErrBlockOutOfRange {
		t.Errorf("Expected ErrBlockOutOfRange, got %v", err)
	}
}

func TestIncrementBatch(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Increment batch
	blockNums := []uint64{0, 1, 5, 100, 500}
	counts, err := region.IncrementBatch(ctx, blockNums)
	if err != nil {
		t.Fatalf("IncrementBatch failed: %v", err)
	}

	if len(counts) != len(blockNums) {
		t.Fatalf("IncrementBatch returned %d counts, want %d", len(counts), len(blockNums))
	}

	// All should be 1
	for i, count := range counts {
		if count != 1 {
			t.Errorf("counts[%d] = %d, want 1", i, count)
		}
	}

	// Increment same batch again
	counts, err = region.IncrementBatch(ctx, blockNums)
	if err != nil {
		t.Fatalf("Second IncrementBatch failed: %v", err)
	}

	// All should be 2
	for i, count := range counts {
		if count != 2 {
			t.Errorf("counts[%d] = %d, want 2", i, count)
		}
	}
}

func TestIncrementBatchEmpty(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	counts, err := region.IncrementBatch(ctx, nil)
	if err != nil {
		t.Fatalf("IncrementBatch(nil) failed: %v", err)
	}
	if counts != nil {
		t.Errorf("IncrementBatch(nil) returned non-nil: %v", counts)
	}

	counts, err = region.IncrementBatch(ctx, []uint64{})
	if err != nil {
		t.Fatalf("IncrementBatch([]) failed: %v", err)
	}
	if counts != nil {
		t.Errorf("IncrementBatch([]) returned non-nil: %v", counts)
	}
}

func TestIncrementBatchOutOfRange(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(100), 100, nil)
	ctx := context.Background()

	_, err := region.IncrementBatch(ctx, []uint64{50, 100})
	if err != ErrBlockOutOfRange {
		t.Errorf("Expected ErrBlockOutOfRange, got %v", err)
	}
}

func TestIncrementBatchOverflow(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Set one block to max
	err := region.Set(ctx, 5, 0xFFFFFFFF, FlagNone)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Batch increment including the maxed block
	_, err = region.IncrementBatch(ctx, []uint64{0, 5, 10})
	if err != ErrRefCountOverflow {
		t.Errorf("Expected ErrRefCountOverflow, got %v", err)
	}
}

func TestDecrementBatch(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Set initial counts
	blockNums := []uint64{0, 1, 5, 100, 500}
	for _, block := range blockNums {
		err := region.Set(ctx, block, 5, FlagNone)
		if err != nil {
			t.Fatalf("Set(%d) failed: %v", block, err)
		}
	}

	// Decrement batch
	counts, err := region.DecrementBatch(ctx, blockNums)
	if err != nil {
		t.Fatalf("DecrementBatch failed: %v", err)
	}

	if len(counts) != len(blockNums) {
		t.Fatalf("DecrementBatch returned %d counts, want %d", len(counts), len(blockNums))
	}

	// All should be 4
	for i, count := range counts {
		if count != 4 {
			t.Errorf("counts[%d] = %d, want 4", i, count)
		}
	}
}

func TestDecrementBatchUnderflow(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Set block 0 to 1, leave block 5 at 0
	err := region.Set(ctx, 0, 1, FlagNone)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Batch decrement including the zero block
	_, err = region.DecrementBatch(ctx, []uint64{0, 5})
	if err != ErrRefCountUnderflow {
		t.Errorf("Expected ErrRefCountUnderflow, got %v", err)
	}
}

func TestShareAndRelease(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()
	blocks := []uint64{3, 700}
	count := func(block uint64) uint32 {
		t.Helper()
		e, err := region.Get(ctx, block)
		if err != nil {
			t.Fatal(err)
		}
		return e.RefCount
	}

	if err := region.Share(ctx, blocks); err != nil {
		t.Fatal(err)
	}
	if got := count(3); got != 2 {
		t.Fatalf("first share of a new block: count %d, want 2", got)
	}
	if err := region.Share(ctx, blocks[:1]); err != nil {
		t.Fatal(err)
	}
	if got := count(3); got != 3 {
		t.Fatalf("second share: count %d, want 3", got)
	}

	for want := uint32(2); want >= 1; want-- {
		free, err := region.Release(ctx, blocks[:1])
		if err != nil {
			t.Fatal(err)
		}
		if len(free) != 0 || count(3) != want {
			t.Fatalf("release down to %d: free=%v count=%d", want, free, count(3))
		}
	}
	free, err := region.Release(ctx, blocks)
	if err != nil {
		t.Fatal(err)
	}
	if len(free) != 1 || free[0] != 3 || count(3) != 0 || count(700) != 1 {
		t.Fatalf("last owner of 3 and one of two owners of 700: free=%v counts=%d,%d", free, count(3), count(700))
	}

	free, err = region.Release(ctx, []uint64{700, 900})
	if err != nil {
		t.Fatal(err)
	}
	if len(free) != 2 || count(700) != 0 || count(900) != 0 {
		t.Fatalf("release of single-owner blocks: free=%v", free)
	}
}

func TestScan(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(100), 100, nil)
	ctx := context.Background()

	// Set up mixed refcounts:
	// - Blocks 0-9: refcount 1 (exclusive)
	// - Blocks 10-14: refcount 2 (shared)
	// - Blocks 15-19: refcount 5 with FlagSnapshot
	// - Rest: refcount 0 (free)

	for i := uint64(0); i < 10; i++ {
		if err := region.Set(ctx, i, 1, FlagNone); err != nil {
			t.Fatal(err)
		}
	}
	for i := uint64(10); i < 15; i++ {
		if err := region.Set(ctx, i, 2, FlagNone); err != nil {
			t.Fatal(err)
		}
	}
	for i := uint64(15); i < 20; i++ {
		if err := region.Set(ctx, i, 5, FlagSnapshot); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := region.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if stats.TotalBlocks != 100 {
		t.Errorf("TotalBlocks = %d, want 100", stats.TotalBlocks)
	}
	if stats.FreeBlocks != 80 {
		t.Errorf("FreeBlocks = %d, want 80", stats.FreeBlocks)
	}
	if stats.ExclusiveBlocks != 10 {
		t.Errorf("ExclusiveBlocks = %d, want 10", stats.ExclusiveBlocks)
	}
	if stats.SharedBlocks != 10 { // 5 with refcount=2, 5 with refcount=5
		t.Errorf("SharedBlocks = %d, want 10", stats.SharedBlocks)
	}
	// Total shared refs = 5*2 + 5*5 = 10 + 25 = 35
	if stats.TotalSharedRefs != 35 {
		t.Errorf("TotalSharedRefs = %d, want 35", stats.TotalSharedRefs)
	}
	if stats.SnapshotBlocks != 5 {
		t.Errorf("SnapshotBlocks = %d, want 5", stats.SnapshotBlocks)
	}
}

func TestInitialize(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// First set some values
	if err := region.Set(ctx, 0, 10, FlagSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := region.Set(ctx, 500, 5, FlagCowPending); err != nil {
		t.Fatal(err)
	}

	// Initialize should reset everything
	err := region.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// All entries should be zero
	entry0, _ := region.Get(ctx, 0)
	entry500, _ := region.Get(ctx, 500)

	if entry0.RefCount != 0 || entry0.Flags != 0 {
		t.Errorf("Block 0 not zeroed: RefCount=%d, Flags=%d", entry0.RefCount, entry0.Flags)
	}
	if entry500.RefCount != 0 || entry500.Flags != 0 {
		t.Errorf("Block 500 not zeroed: RefCount=%d, Flags=%d", entry500.RefCount, entry500.Flags)
	}
}

func TestSizeForBlocks(t *testing.T) {
	tests := []struct {
		totalBlocks uint64
		wantSectors uint64
	}{
		{0, 0},
		{1, 1},
		{EntriesPerSector, 1},
		{EntriesPerSector + 1, 2},
		{EntriesPerSector * 2, 2},
		{EntriesPerSector * 3, 3},
		{1000000, (1000000 + EntriesPerSector - 1) / EntriesPerSector}, // 1TB with 1MB blocks
	}

	for _, tt := range tests {
		size := SizeForBlocks(tt.totalBlocks)
		wantSize := tt.wantSectors * SectorSize
		if size != wantSize {
			t.Errorf("SizeForBlocks(%d) = %d, want %d", tt.totalBlocks, size, wantSize)
		}
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_refcount_persist.bin")
	defer func() { _ = os.Remove(testFile) }()

	// Create and set some values
	func() {
		f, err := os.Create(testFile)
		if err != nil {
			t.Skipf("Cannot create test file: %v", err)
		}
		if err := f.Truncate(10 << 20); err != nil {
			_ = f.Close()
			t.Skipf("Cannot truncate: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		dev, err := storage.Open(testFile)
		if err != nil {
			t.Skipf("O_DIRECT not supported: %v", err)
		}
		defer func() { _ = dev.Close() }()

		region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
		ctx := context.Background()

		// Set various refcounts and flags
		if err := region.Set(ctx, 0, 1, FlagNone); err != nil {
			t.Fatal(err)
		}
		if err := region.Set(ctx, 50, 10, FlagSnapshot); err != nil {
			t.Fatal(err)
		}
		if err := region.Set(ctx, 100, 3, FlagCowPending|FlagMetadata); err != nil {
			t.Fatal(err)
		}
		if err := region.Set(ctx, 999, 2, FlagPinned); err != nil {
			t.Fatal(err)
		}
	}()

	// Reopen and verify
	dev, err := storage.Open(testFile)
	if err != nil {
		t.Skipf("Cannot reopen: %v", err)
	}
	defer func() { _ = dev.Close() }()

	region := NewRegion(dev, 0, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	tests := []struct {
		block    uint64
		refcount uint32
		flags    EntryFlags
	}{
		{0, 1, FlagNone},
		{50, 10, FlagSnapshot},
		{100, 3, FlagCowPending | FlagMetadata},
		{999, 2, FlagPinned},
	}

	for _, tt := range tests {
		entry, err := region.Get(ctx, tt.block)
		if err != nil {
			t.Fatalf("Get(%d) failed: %v", tt.block, err)
		}
		if entry.RefCount != tt.refcount {
			t.Errorf("Block %d: RefCount = %d, want %d", tt.block, entry.RefCount, tt.refcount)
		}
		if entry.Flags != tt.flags {
			t.Errorf("Block %d: Flags = %d, want %d", tt.block, entry.Flags, tt.flags)
		}
	}
}

func TestMultipleSectors(t *testing.T) {
	dev, cleanup := createTestDevice(t, 100<<20) // 100MB for many sectors
	defer cleanup()

	// Create region spanning multiple sectors
	totalBlocks := uint64(EntriesPerSector*3 + 100) // 3 full sectors + partial
	region := NewRegion(dev, 0, SizeForBlocks(totalBlocks), totalBlocks, nil)
	ctx := context.Background()

	// Set entries in different sectors
	blocksToSet := []struct {
		block    uint64
		refcount uint32
		flags    EntryFlags
	}{
		{0, 1, FlagNone},                                        // First sector
		{EntriesPerSector - 1, 2, FlagSnapshot},                 // End of first sector
		{EntriesPerSector, 3, FlagCowPending},                   // Start of second sector
		{EntriesPerSector * 2, 4, FlagMetadata},                 // Start of third sector
		{EntriesPerSector*3 + 50, 5, FlagSnapshot | FlagPinned}, // Fourth sector
	}

	for _, b := range blocksToSet {
		err := region.Set(ctx, b.block, b.refcount, b.flags)
		if err != nil {
			t.Fatalf("Set(%d) failed: %v", b.block, err)
		}
	}

	// Verify all entries
	for _, b := range blocksToSet {
		entry, err := region.Get(ctx, b.block)
		if err != nil {
			t.Fatalf("Get(%d) failed: %v", b.block, err)
		}
		if entry.RefCount != b.refcount {
			t.Errorf("Block %d: RefCount = %d, want %d", b.block, entry.RefCount, b.refcount)
		}
		if entry.Flags != b.flags {
			t.Errorf("Block %d: Flags = %d, want %d", b.block, entry.Flags, b.flags)
		}
	}

	// Scan and verify counts
	stats, err := region.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if stats.TotalBlocks != totalBlocks {
		t.Errorf("TotalBlocks = %d, want %d", stats.TotalBlocks, totalBlocks)
	}

	expectedAllocated := uint64(len(blocksToSet))
	// Exclusive: blocks with refcount=1
	// Shared: blocks with refcount>1
	expectedExclusive := uint64(1) // Only block 0 has refcount=1
	expectedShared := expectedAllocated - expectedExclusive
	expectedFree := totalBlocks - expectedAllocated

	if stats.ExclusiveBlocks != expectedExclusive {
		t.Errorf("ExclusiveBlocks = %d, want %d", stats.ExclusiveBlocks, expectedExclusive)
	}
	if stats.SharedBlocks != expectedShared {
		t.Errorf("SharedBlocks = %d, want %d", stats.SharedBlocks, expectedShared)
	}
	if stats.FreeBlocks != expectedFree {
		t.Errorf("FreeBlocks = %d, want %d", stats.FreeBlocks, expectedFree)
	}
}

func TestBatchAcrossSectors(t *testing.T) {
	dev, cleanup := createTestDevice(t, 100<<20)
	defer cleanup()

	totalBlocks := uint64(EntriesPerSector * 3)
	region := NewRegion(dev, 0, SizeForBlocks(totalBlocks), totalBlocks, nil)
	ctx := context.Background()

	// Batch increment blocks from different sectors
	blockNums := []uint64{
		0,                      // Sector 0
		EntriesPerSector,       // Sector 1
		EntriesPerSector * 2,   // Sector 2
		EntriesPerSector / 2,   // Middle of sector 0
		EntriesPerSector + 100, // Middle of sector 1
	}

	counts, err := region.IncrementBatch(ctx, blockNums)
	if err != nil {
		t.Fatalf("IncrementBatch failed: %v", err)
	}

	for i, count := range counts {
		if count != 1 {
			t.Errorf("counts[%d] = %d, want 1", i, count)
		}
	}

	// Verify each block
	for _, block := range blockNums {
		entry, err := region.Get(ctx, block)
		if err != nil {
			t.Fatalf("Get(%d) failed: %v", block, err)
		}
		if entry.RefCount != 1 {
			t.Errorf("Block %d: RefCount = %d, want 1", block, entry.RefCount)
		}
	}
}

func TestRegionOffset(t *testing.T) {
	dev, cleanup := createTestDevice(t, 100<<20)
	defer cleanup()

	// Test region at a non-zero offset
	offset := uint64(50 * SectorSize) // 50 sectors into device
	region := NewRegion(dev, offset, SizeForBlocks(1000), 1000, nil)
	ctx := context.Background()

	// Set some values
	err := region.Set(ctx, 0, 42, FlagSnapshot)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify
	entry, err := region.Get(ctx, 0)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if entry.RefCount != 42 || entry.Flags != FlagSnapshot {
		t.Errorf("Entry = {%d, %d}, want {42, %d}", entry.RefCount, entry.Flags, FlagSnapshot)
	}
}

// A region too small for its blocks, like layout v2's 1 MB table on a 200 GiB
// volume, refuses blocks past its end and leaves the next region alone.
func TestBlocksPastRegionLeaveTheNextRegionAlone(t *testing.T) {
	ctx := context.Background()
	const size, totalBlocks = 1 << 20, 204537
	dev, cleanup := createTestDevice(t, 2*size)
	defer cleanup()
	first := BlocksForSize(size)
	last := uint64(totalBlocks - 1)

	// The next region's first sector holds data and a CRC32C, like a refcount sector.
	next := storage.NewAlignedBuffer(size)
	for i := range SectorSize - ChecksumSize {
		next.Bytes()[i] = byte(i%251 + 1)
	}
	binary.LittleEndian.PutUint32(next.Bytes()[SectorSize-ChecksumSize:], storage.CRC32C(next.Bytes()[:SectorSize-ChecksumSize]))
	if _, err := dev.WriteAt(next.Bytes(), size); err != nil {
		t.Fatal(err)
	}

	region := NewRegion(dev, 0, size, totalBlocks, nil)

	if _, err := region.Increment(ctx, first); !errors.Is(err, ErrBlockPastRegion) {
		t.Errorf("Increment(%d) = %v, want ErrBlockPastRegion", first, err)
	}
	if err := region.Share(ctx, []uint64{first - 1, last}); !errors.Is(err, ErrBlockPastRegion) {
		t.Errorf("Share(%d, %d) = %v, want ErrBlockPastRegion", first-1, last, err)
	}
	if err := region.Set(ctx, last, 2, 0); !errors.Is(err, ErrBlockPastRegion) {
		t.Errorf("Set(%d) = %v, want ErrBlockPastRegion", last, err)
	}
	if err := region.SetFlags(ctx, first, FlagSnapshot); !errors.Is(err, ErrBlockPastRegion) {
		t.Errorf("SetFlags(%d) = %v, want ErrBlockPastRegion", first, err)
	}
	if entry, err := region.Get(ctx, first); err != nil || entry != (Entry{}) {
		t.Errorf("Get(%d) = %+v, %v, want a single owner", first, entry, err)
	}
	if free, err := region.Release(ctx, []uint64{first, last}); err != nil || len(free) != 2 {
		t.Errorf("Release(%d, %d) = %v, %v, want both free", first, last, free, err)
	}
	if entry, _ := region.Get(ctx, first-1); entry.RefCount != 0 {
		t.Errorf("failed Share left block %d with count %d", first-1, entry.RefCount)
	}
	if stats, err := region.Scan(ctx); err != nil || stats.FreeBlocks != totalBlocks {
		t.Errorf("Scan = %+v, %v, want all %d blocks free", stats, err, totalBlocks)
	}

	after := storage.NewAlignedBuffer(size)
	if _, err := dev.ReadAt(after.Bytes(), size); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after.Bytes(), next.Bytes()) {
		t.Error("refcount updates for blocks past the region changed the next region")
	}
}

// Benchmarks

func BenchmarkGet(b *testing.B) {
	dev, cleanup := createTestDevice(b, 10<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(10000), 10000, nil)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := region.Get(ctx, uint64(i%10000)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIncrement(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(uint64(b.N+1)), uint64(b.N+1), nil)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := region.Increment(ctx, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecrement(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(uint64(b.N+1)), uint64(b.N+1), nil)
	ctx := context.Background()

	// Pre-set all counts to 1
	for i := 0; i < b.N; i++ {
		if err := region.Set(ctx, uint64(i), 1, FlagNone); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := region.Decrement(ctx, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIncrementBatch(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(10000), 10000, nil)
	ctx := context.Background()

	// Create batch of 100 blocks
	batch := make([]uint64, 100)
	for i := range batch {
		batch[i] = uint64(i * 10)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := region.IncrementBatch(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScan(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	region := NewRegion(dev, 0, SizeForBlocks(10000), 10000, nil)
	ctx := context.Background()

	// Set some values
	for i := uint64(0); i < 1000; i++ {
		if err := region.Set(ctx, i, uint32(i%10+1), FlagNone); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := region.Scan(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
