package journal

import (
	"bytes"
	"os"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// createTestDevice creates a temporary file for testing.
// The file is sparse and suitable for O_DIRECT operations.
func createTestDevice(t testing.TB, size int64) (*storage.Device, func()) {
	t.Helper()

	// Create temp file
	f, err := os.CreateTemp("", "journal_test_*.img")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	path := f.Name()

	// Allocate sparse file
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		t.Fatalf("Failed to truncate file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Open with O_DIRECT
	dev, err := storage.Open(path)
	if err != nil {
		_ = os.Remove(path)
		t.Fatalf("Failed to open device: %v", err)
	}

	cleanup := func() {
		_ = dev.Close()
		_ = os.Remove(path)
	}

	return dev, cleanup
}

func TestInitRegion(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024) // 10MB
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	generation := uint64(1)
	regionOffset := uint64(0)
	regionSize := uint64(DefaultJournalSlotSize) // 4MB

	region, err := InitRegion(dev, regionOffset, regionSize, nodeUUID, generation)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Verify state
	if region.State() != JournalStateClean {
		t.Errorf("State = %v, want Clean", region.State())
	}

	if region.NeedsRecovery() {
		t.Error("New region should not need recovery")
	}

	if !region.IsEmpty() {
		t.Error("New region should be empty")
	}

	// Verify superblock
	sb := region.Superblock()
	if sb.NodeUUID != nodeUUID {
		t.Error("NodeUUID mismatch")
	}
	if sb.NodeGeneration != generation {
		t.Errorf("NodeGeneration = %d, want %d", sb.NodeGeneration, generation)
	}
	if sb.DataAreaSize != uint32(regionSize)-JournalDataOffset {
		t.Errorf("DataAreaSize = %d, want %d", sb.DataAreaSize, uint32(regionSize)-JournalDataOffset)
	}
}

func TestOpenRegion(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	generation := uint64(42)
	regionOffset := uint64(0)
	regionSize := uint64(DefaultJournalSlotSize)

	// Initialize first
	_, err := InitRegion(dev, regionOffset, regionSize, nodeUUID, generation)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Now open
	region, err := OpenRegion(dev, regionOffset, regionSize, nodeUUID)
	if err != nil {
		t.Fatalf("OpenRegion failed: %v", err)
	}

	// Verify it was read correctly
	sb := region.Superblock()
	if sb.NodeUUID != nodeUUID {
		t.Error("NodeUUID mismatch after reopen")
	}
	if sb.NodeGeneration != generation {
		t.Errorf("NodeGeneration = %d, want %d after reopen", sb.NodeGeneration, generation)
	}
}

func TestOpenRegionWrongNode(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID1 := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	nodeUUID2 := [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	regionOffset := uint64(0)
	regionSize := uint64(DefaultJournalSlotSize)

	// Initialize with UUID1
	_, err := InitRegion(dev, regionOffset, regionSize, nodeUUID1, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Try to open with UUID2 - should fail
	_, err = OpenRegion(dev, regionOffset, regionSize, nodeUUID2)
	if err == nil {
		t.Error("Expected error when opening journal with wrong node UUID")
	}
}

func TestRegionWriteRead(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	regionOffset := uint64(0)
	regionSize := uint64(DefaultJournalSlotSize)

	region, err := InitRegion(dev, regionOffset, regionSize, nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Write some data
	testData := []byte("Hello, Journal World! This is a test transaction.")
	written, err := region.WriteAt(testData)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if written != uint32(len(testData)) {
		t.Errorf("Written = %d, want %d", written, len(testData))
	}

	// Sync to ensure data is on disk
	if err := region.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Read it back
	readBuf := make([]byte, len(testData))
	n, err := region.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Read %d bytes, want %d", n, len(testData))
	}
	if !bytes.Equal(readBuf, testData) {
		t.Errorf("Data mismatch: got %q, want %q", readBuf, testData)
	}
}

func TestRegionNextSeq(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Get sequence numbers
	seq1 := region.NextSeq()
	seq2 := region.NextSeq()
	seq3 := region.NextSeq()

	if seq1 != 1 {
		t.Errorf("First seq = %d, want 1", seq1)
	}
	if seq2 != 2 {
		t.Errorf("Second seq = %d, want 2", seq2)
	}
	if seq3 != 3 {
		t.Errorf("Third seq = %d, want 3", seq3)
	}
}

func TestRegionMarkDirtyClean(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Start clean
	if region.State() != JournalStateClean {
		t.Error("Should start clean")
	}
	if region.NeedsRecovery() {
		t.Error("Should not need recovery initially")
	}

	// Mark dirty
	if err := region.MarkDirty(); err != nil {
		t.Fatalf("MarkDirty failed: %v", err)
	}
	if region.State() != JournalStateDirty {
		t.Error("Should be dirty")
	}
	if !region.NeedsRecovery() {
		t.Error("Should need recovery when dirty")
	}

	// Mark clean
	if err := region.MarkClean(); err != nil {
		t.Fatalf("MarkClean failed: %v", err)
	}
	if region.State() != JournalStateClean {
		t.Error("Should be clean")
	}
	if region.NeedsRecovery() {
		t.Error("Should not need recovery when clean")
	}
}

func TestRegionStatePersistence(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	regionOffset := uint64(0)
	regionSize := uint64(DefaultJournalSlotSize)

	// Create and mark dirty
	region1, err := InitRegion(dev, regionOffset, regionSize, nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}
	if err := region1.MarkDirty(); err != nil {
		t.Fatalf("MarkDirty failed: %v", err)
	}

	// Reopen - should still be dirty
	region2, err := OpenRegion(dev, regionOffset, regionSize, nodeUUID)
	if err != nil {
		t.Fatalf("OpenRegion failed: %v", err)
	}
	if region2.State() != JournalStateDirty {
		t.Error("State should persist as dirty after reopen")
	}
	if !region2.NeedsRecovery() {
		t.Error("Should need recovery after reopening dirty journal")
	}
}

func TestRegionCheckpoint(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Write some data to advance tail
	testData := make([]byte, 1000)
	_, err = region.WriteAt(testData)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Head should still be 0
	if region.HeadOffset() != 0 {
		t.Errorf("Head = %d before checkpoint, want 0", region.HeadOffset())
	}

	// Checkpoint at position 500
	if err := region.Checkpoint(1, 500); err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	// Head should now be 500
	if region.HeadOffset() != 500 {
		t.Errorf("Head = %d after checkpoint, want 500", region.HeadOffset())
	}

	// Available space should have increased
	space := region.AvailableSpace()
	// Total data area minus (tail - new head)
	// Note: tail is sector-aligned (SectorSize) not 1000
	alignedTail := uint32(storage.SectorSize) // 1000 bytes rounds up to SectorSize
	expected := region.DataAreaSize() - (alignedTail - 500)
	if space != expected {
		t.Errorf("Available space = %d, want %d", space, expected)
	}
}

func TestRegionReset(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Simulate some activity
	region.NextSeq()
	region.NextSeq()
	if err := region.MarkDirty(); err != nil {
		t.Fatal(err)
	}
	if _, err := region.WriteAt(make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}

	// Verify we have state
	if region.State() != JournalStateDirty {
		t.Error("Should be dirty before reset")
	}
	if region.TailOffset() == 0 {
		t.Error("Tail should be non-zero before reset")
	}

	// Reset
	if err := region.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Verify reset state
	if region.State() != JournalStateClean {
		t.Error("Should be clean after reset")
	}
	if region.HeadOffset() != 0 {
		t.Errorf("Head = %d after reset, want 0", region.HeadOffset())
	}
	if region.TailOffset() != 0 {
		t.Errorf("Tail = %d after reset, want 0", region.TailOffset())
	}
}

func TestRegionAvailableSpace(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	regionSize := uint64(DefaultJournalSlotSize)
	region, err := InitRegion(dev, 0, regionSize, nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	dataAreaSize := region.DataAreaSize()
	initialSpace := region.AvailableSpace()

	// Initially should have full data area available
	if initialSpace != dataAreaSize {
		t.Errorf("Initial space = %d, want %d", initialSpace, dataAreaSize)
	}

	// Write 1000 bytes
	_, err = region.WriteAt(make([]byte, 1000))
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	// Space should decrease by sector-aligned amount (SectorSize, not 1000)
	afterWrite := region.AvailableSpace()
	alignedLen := uint32(storage.SectorSize) // 1000 bytes rounds up to SectorSize for O_DIRECT
	if afterWrite != dataAreaSize-alignedLen {
		t.Errorf("Space after write = %d, want %d", afterWrite, dataAreaSize-alignedLen)
	}
}

func TestRegionTooSmall(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	// Try to create a region that's too small
	_, err := InitRegion(dev, 0, uint64(JournalDataOffset), nodeUUID, 1)
	if err == nil {
		t.Error("Should fail with region too small")
	}
}

func TestRegionJournalFull(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	// Create a small region
	regionSize := uint64(JournalDataOffset + 8192) // Only 8KB data area
	region, err := InitRegion(dev, 0, regionSize, nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Try to write more than available
	bigData := make([]byte, 10000)
	_, err = region.WriteAt(bigData)
	if err != ErrJournalFull {
		t.Errorf("Expected ErrJournalFull, got %v", err)
	}
}
