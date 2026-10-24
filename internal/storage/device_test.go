package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDeviceOpenNonExistent verifies error handling for missing files.
func TestDeviceOpenNonExistent(t *testing.T) {
	_, err := Open("/nonexistent/path/to/device")
	if err == nil {
		t.Error("Open should fail for non-existent path")
	}
}

// TestValidateAlignment tests the alignment validation logic.
func TestValidateAlignment(t *testing.T) {
	// Create an aligned buffer
	buf := NewAlignedBuffer(SectorSize)

	tests := []struct {
		name      string
		buf       []byte
		offset    int64
		wantError bool
	}{
		{
			name:      "aligned buffer and offset",
			buf:       buf.Bytes(),
			offset:    0,
			wantError: false,
		},
		{
			name:      "aligned buffer, aligned offset",
			buf:       buf.Bytes(),
			offset:    SectorSize,
			wantError: false,
		},
		{
			name:      "aligned buffer, unaligned offset",
			buf:       buf.Bytes(),
			offset:    1,
			wantError: true,
		},
		{
			name:      "unaligned buffer size",
			buf:       make([]byte, 100), // Not sector-aligned size
			offset:    0,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAlignment(tt.buf, tt.offset)
			if (err != nil) != tt.wantError {
				t.Errorf("validateAlignment() error = %v, wantError = %v", err, tt.wantError)
			}
		})
	}
}

// TestDeviceWithFile tests device operations using a regular file.
// Note: O_DIRECT may not work on all filesystems (e.g., tmpfs).
// This test creates a file and attempts O_DIRECT; skips if unsupported.
func TestDeviceWithFile(t *testing.T) {
	// Create a temp directory (not tmpfs, which doesn't support O_DIRECT)
	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		// Try current directory
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_device.bin")

	// Create a test file with some size
	f, err := os.Create(testFile)
	if err != nil {
		t.Skipf("Cannot create test file: %v", err)
	}

	// Write 1MB of zeros
	size := int64(1 << 20)
	if err := f.Truncate(size); err != nil {
		f.Close()
		os.Remove(testFile)
		t.Skipf("Cannot truncate test file: %v", err)
	}
	f.Close()

	defer os.Remove(testFile)

	// Try to open with O_DIRECT
	dev, err := Open(testFile)
	if err != nil {
		// O_DIRECT not supported on this filesystem
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	defer dev.Close()

	// Test Size()
	if dev.Size() != size {
		t.Errorf("Size() = %d, want %d", dev.Size(), size)
	}

	// Test Path()
	if dev.Path() != testFile {
		t.Errorf("Path() = %q, want %q", dev.Path(), testFile)
	}

	// Test WriteAt
	buf := NewAlignedBuffer(SectorSize)
	testData := []byte("GoClusterFS test data")
	copy(buf.Bytes(), testData)

	n, err := dev.WriteAt(buf.Bytes(), 0)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != SectorSize {
		t.Errorf("WriteAt wrote %d bytes, want %d", n, SectorSize)
	}

	// Test Sync
	if err := dev.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}

	// Test ReadAt
	readBuf := NewAlignedBuffer(SectorSize)
	n, err = dev.ReadAt(readBuf.Bytes(), 0)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != SectorSize {
		t.Errorf("ReadAt read %d bytes, want %d", n, SectorSize)
	}

	// Verify data
	for i := 0; i < len(testData); i++ {
		if readBuf.Bytes()[i] != testData[i] {
			t.Errorf("data mismatch at byte %d: got %d, want %d",
				i, readBuf.Bytes()[i], testData[i])
			break
		}
	}
}

// TestDeviceUnalignedOperations verifies that unaligned operations are rejected.
func TestDeviceUnalignedOperations(t *testing.T) {
	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_unaligned.bin")

	f, err := os.Create(testFile)
	if err != nil {
		t.Skipf("Cannot create test file: %v", err)
	}
	f.Truncate(1 << 20)
	f.Close()
	defer os.Remove(testFile)

	dev, err := Open(testFile)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	defer dev.Close()

	// Unaligned offset should fail
	buf := NewAlignedBuffer(SectorSize)
	_, err = dev.WriteAt(buf.Bytes(), 1) // Offset 1 is not aligned
	if err == nil {
		t.Error("WriteAt with unaligned offset should fail")
	}

	_, err = dev.ReadAt(buf.Bytes(), 1)
	if err == nil {
		t.Error("ReadAt with unaligned offset should fail")
	}

	// Unaligned buffer size should fail
	unalignedBuf := make([]byte, 100)
	_, err = dev.WriteAt(unalignedBuf, 0)
	if err == nil {
		t.Error("WriteAt with unaligned buffer should fail")
	}
}

// TestShardIndex verifies shard index calculation.
func TestShardIndex(t *testing.T) {
	tests := []struct {
		offset int64
		want   int
	}{
		{0, 0},                              // First shard
		{IOShardSize - 1, 0},                // End of first shard
		{IOShardSize, 1},                    // Start of second shard
		{IOShardSize * 63, 63},              // Last shard
		{IOShardSize * 64, 0},               // Wrap around to first shard
		{IOShardSize * 65, 1},               // Wrap around to second shard
		{IOShardSize*64 + IOShardSize/2, 0}, // Mid-block in wrapped shard
	}

	for _, tt := range tests {
		got := shardIndex(tt.offset)
		if got != tt.want {
			t.Errorf("shardIndex(%d) = %d, want %d", tt.offset, got, tt.want)
		}
	}
}

// TestConcurrentShardedIO tests that operations to different shards run in parallel.
func TestConcurrentShardedIO(t *testing.T) {
	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_sharded_io.bin")

	f, err := os.Create(testFile)
	if err != nil {
		t.Skipf("Cannot create test file: %v", err)
	}

	// Create file large enough for multiple shards (4 MB = 4 shards)
	size := int64(4 * IOShardSize)
	if err := f.Truncate(size); err != nil {
		f.Close()
		os.Remove(testFile)
		t.Skipf("Cannot truncate test file: %v", err)
	}
	f.Close()
	defer os.Remove(testFile)

	dev, err := Open(testFile)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	defer dev.Close()

	// Run concurrent writes to different shards
	const numGoroutines = 4
	var wg sync.WaitGroup
	var writeCount atomic.Int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(shardNum int) {
			defer wg.Done()

			buf := NewAlignedBuffer(SectorSize)
			// Write identifier to detect data correctness
			buf.Bytes()[0] = byte(shardNum)

			offset := int64(shardNum) * IOShardSize
			_, err := dev.WriteAt(buf.Bytes(), offset)
			if err != nil {
				t.Errorf("WriteAt to shard %d failed: %v", shardNum, err)
				return
			}
			writeCount.Add(1)
		}(i)
	}

	wg.Wait()

	if writeCount.Load() != numGoroutines {
		t.Errorf("Expected %d writes, got %d", numGoroutines, writeCount.Load())
	}

	// Verify each shard has correct data
	for i := 0; i < numGoroutines; i++ {
		buf := NewAlignedBuffer(SectorSize)
		offset := int64(i) * IOShardSize
		_, err := dev.ReadAt(buf.Bytes(), offset)
		if err != nil {
			t.Errorf("ReadAt from shard %d failed: %v", i, err)
			continue
		}
		if buf.Bytes()[0] != byte(i) {
			t.Errorf("Shard %d has wrong data: got %d, want %d", i, buf.Bytes()[0], i)
		}
	}
}

// TestMultiShardIO tests operations that span multiple shards.
func TestMultiShardIO(t *testing.T) {
	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_multishard.bin")

	f, err := os.Create(testFile)
	if err != nil {
		t.Skipf("Cannot create test file: %v", err)
	}

	// Create file large enough for cross-shard test
	size := int64(3 * IOShardSize)
	if err := f.Truncate(size); err != nil {
		f.Close()
		os.Remove(testFile)
		t.Skipf("Cannot truncate test file: %v", err)
	}
	f.Close()
	defer os.Remove(testFile)

	dev, err := Open(testFile)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	defer dev.Close()

	// Write data that spans two shards
	// Start at offset IOShardSize - SectorSize and write 2 sectors
	startOffset := IOShardSize - SectorSize
	buf := NewAlignedBuffer(2 * SectorSize)
	for i := 0; i < len(buf.Bytes()); i++ {
		buf.Bytes()[i] = byte(i % 256)
	}

	n, err := dev.WriteAt(buf.Bytes(), int64(startOffset))
	if err != nil {
		t.Fatalf("Multi-shard write failed: %v", err)
	}
	if n != 2*SectorSize {
		t.Errorf("Wrote %d bytes, expected %d", n, 2*SectorSize)
	}

	// Sync and read back
	dev.Sync()

	readBuf := NewAlignedBuffer(2 * SectorSize)
	n, err = dev.ReadAt(readBuf.Bytes(), int64(startOffset))
	if err != nil {
		t.Fatalf("Multi-shard read failed: %v", err)
	}
	if n != 2*SectorSize {
		t.Errorf("Read %d bytes, expected %d", n, 2*SectorSize)
	}

	// Verify data integrity
	for i := 0; i < len(buf.Bytes()); i++ {
		if readBuf.Bytes()[i] != buf.Bytes()[i] {
			t.Errorf("Data mismatch at byte %d: got %d, want %d",
				i, readBuf.Bytes()[i], buf.Bytes()[i])
			break
		}
	}
}

// A write that waits for its I/O shard past its deadline writes nothing.
func TestWriteAtBeforeSkipsWriteAfterShardWait(t *testing.T) {
	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = t.TempDir()
	}
	testFile := filepath.Join(tmpDir, "test_write_deadline.bin")
	if err := os.WriteFile(testFile, make([]byte, IOShardSize), 0o600); err != nil {
		t.Skipf("Cannot create test file: %v", err)
	}
	defer os.Remove(testFile)
	dev, err := Open(testFile)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	defer dev.Close()

	buf := NewAlignedBuffer(SectorSize)
	buf.Bytes()[0] = 0xAB
	shard := &dev.ioShards[shardIndex(0)]
	shard.Lock()
	errc := make(chan error, 1)
	go func() {
		_, err := dev.WriteAtBefore(buf.Bytes(), 0, time.Now().Add(10*time.Millisecond))
		errc <- err
	}()
	time.Sleep(30 * time.Millisecond)
	shard.Unlock()
	if err := <-errc; !errors.Is(err, ErrWriteDeadline) {
		t.Fatalf("write after a 30ms shard wait with a 10ms deadline: got %v, want ErrWriteDeadline", err)
	}

	got := NewAlignedBuffer(SectorSize)
	if _, err := dev.ReadAt(got.Bytes(), 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if got.Bytes()[0] != 0 {
		t.Fatalf("the skipped write reached the device: first byte %#x", got.Bytes()[0])
	}
	if _, err := dev.WriteAtBefore(buf.Bytes(), 0, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("write well before its deadline: %v", err)
	}
}
