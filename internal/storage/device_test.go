package storage

import (
	"os"
	"path/filepath"
	"testing"
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
