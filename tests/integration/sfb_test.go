// Package integration provides integration tests for GoClusterFS.
// This file contains tests for Sub-File Block (SFB) containers.
package integration

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// TestSFBSmallFileAllocation verifies that small files (≤64KB) can be
// created and read back correctly when SFB is enabled.
func TestSFBSmallFileAllocation(t *testing.T) {
	mfs := NewMountedFS(t, 50) // 50MB image

	// Mount with SFB enabled
	if err := mfs.FormatAndMountWithSFB(false); err != nil {
		t.Fatalf("FormatAndMountWithSFB failed: %v", err)
	}

	// Test various small file sizes within SFB range
	sizes := []int{
		100,   // Tiny file
		1024,  // 1KB
		4096,  // 4KB (tier boundary)
		8192,  // 8KB (tier boundary)
		16384, // 16KB (tier boundary)
		32768, // 32KB
		65536, // 64KB (max SFB size)
	}

	for _, size := range sizes {
		name := fileSizeName(size)
		t.Run(name, func(t *testing.T) {
			data := make([]byte, size)
			rand.Read(data)

			// Write file
			if err := mfs.WriteFile(name, data); err != nil {
				t.Fatalf("WriteFile(%s) failed: %v", name, err)
			}

			// Read back and verify
			readData, err := mfs.ReadFile(name)
			if err != nil {
				t.Fatalf("ReadFile(%s) failed: %v", name, err)
			}

			if !bytes.Equal(data, readData) {
				t.Errorf("Data mismatch for %s: wrote %d bytes, read %d bytes",
					name, len(data), len(readData))
			}

			// Verify size via stat
			info, err := mfs.Stat(name)
			if err != nil {
				t.Fatalf("Stat(%s) failed: %v", name, err)
			}
			if info.Size() != int64(size) {
				t.Errorf("Stat size mismatch: got %d, want %d", info.Size(), size)
			}
		})
	}
}

// TestSFBTierSelection verifies that files at tier boundaries are
// handled correctly. Each tier has a specific slot size:
// - Tier4KB:  slots of 4KB  (256 per container)
// - Tier8KB:  slots of 8KB  (128 per container)
// - Tier16KB: slots of 16KB (64 per container)
// - Tier64KB: slots of 64KB (16 per container)
func TestSFBTierSelection(t *testing.T) {
	mfs := NewMountedFS(t, 50)

	if err := mfs.FormatAndMountWithSFB(false); err != nil {
		t.Fatalf("FormatAndMountWithSFB failed: %v", err)
	}

	// Test files at exact tier boundaries and just above
	testCases := []struct {
		name string
		size int
	}{
		// Tier4KB (≤4KB)
		{"tier4kb_exact", 4096},
		{"tier4kb_small", 100},

		// Tier8KB (4KB < size ≤ 8KB)
		{"tier8kb_just_above_4k", 4097},
		{"tier8kb_exact", 8192},

		// Tier16KB (8KB < size ≤ 16KB)
		{"tier16kb_just_above_8k", 8193},
		{"tier16kb_exact", 16384},

		// Tier64KB (16KB < size ≤ 64KB)
		{"tier64kb_just_above_16k", 16385},
		{"tier64kb_exact", 65536},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)
			rand.Read(data)

			if err := mfs.WriteFile(tc.name, data); err != nil {
				t.Fatalf("WriteFile failed: %v", err)
			}

			readData, err := mfs.ReadFile(tc.name)
			if err != nil {
				t.Fatalf("ReadFile failed: %v", err)
			}

			if !bytes.Equal(data, readData) {
				t.Errorf("Data mismatch: wrote %d bytes, read %d bytes",
					len(data), len(readData))
			}
		})
	}
}

// TestSFBDataIntegrity writes random data to SFB-backed files and
// verifies the data is correctly read back.
func TestSFBDataIntegrity(t *testing.T) {
	mfs := NewMountedFS(t, 50)

	if err := mfs.FormatAndMountWithSFB(false); err != nil {
		t.Fatalf("FormatAndMountWithSFB failed: %v", err)
	}

	// Create multiple files with random data
	fileCount := 20
	files := make(map[string][]byte)

	for i := 0; i < fileCount; i++ {
		name := fileSizeName(i*1000 + 100) // Varying sizes
		size := (i%16 + 1) * 1024          // 1KB to 16KB

		data := make([]byte, size)
		rand.Read(data)
		files[name] = data

		if err := mfs.WriteFile(name, data); err != nil {
			t.Fatalf("WriteFile(%s) failed: %v", name, err)
		}
	}

	// Read all files back and verify
	for name, expectedData := range files {
		readData, err := mfs.ReadFile(name)
		if err != nil {
			t.Errorf("ReadFile(%s) failed: %v", name, err)
			continue
		}

		if !bytes.Equal(expectedData, readData) {
			t.Errorf("Data mismatch for %s", name)
		}
	}
}

// TestSFBMultipleSmallFiles creates many small files to verify that
// multiple files can share SFB containers efficiently.
func TestSFBMultipleSmallFiles(t *testing.T) {
	mfs := NewMountedFS(t, 100) // 100MB to have room

	if err := mfs.FormatAndMountWithSFB(false); err != nil {
		t.Fatalf("FormatAndMountWithSFB failed: %v", err)
	}

	// Create 50 small files (1KB each)
	// Without SFB: would need 50 x 1MB = 50MB
	// With SFB: should fit in ~1 container (256 x 4KB slots per 1MB)
	fileCount := 50
	fileSize := 1024 // 1KB each

	files := make(map[string][]byte)

	for i := 0; i < fileCount; i++ {
		name := fileSizeName(i)
		data := make([]byte, fileSize)
		rand.Read(data)
		files[name] = data

		if err := mfs.WriteFile(name, data); err != nil {
			t.Fatalf("WriteFile(%s) failed: %v", name, err)
		}
	}

	// Verify all files are readable
	for name, expectedData := range files {
		readData, err := mfs.ReadFile(name)
		if err != nil {
			t.Errorf("ReadFile(%s) failed: %v", name, err)
			continue
		}

		if !bytes.Equal(expectedData, readData) {
			t.Errorf("Data mismatch for %s", name)
		}
	}

	t.Logf("Successfully created and verified %d small files", fileCount)
}

// TestSFBMigrationAt64KB verifies that when a file grows beyond 64KB,
// it is migrated from SFB to full extent allocation correctly.
func TestSFBMigrationAt64KB(t *testing.T) {
	mfs := NewMountedFS(t, 50)

	if err := mfs.FormatAndMountWithSFB(false); err != nil {
		t.Fatalf("FormatAndMountWithSFB failed: %v", err)
	}

	// Create a file at exactly 64KB (max SFB size)
	initialSize := 65536
	initialData := make([]byte, initialSize)
	rand.Read(initialData)

	if err := mfs.WriteFile("migration_test", initialData); err != nil {
		t.Fatalf("WriteFile (initial) failed: %v", err)
	}

	// Verify initial data
	readData, err := mfs.ReadFile("migration_test")
	if err != nil {
		t.Fatalf("ReadFile (initial) failed: %v", err)
	}
	if !bytes.Equal(initialData, readData) {
		t.Fatalf("Initial data mismatch")
	}

	// Now append data to exceed 64KB, triggering migration
	appendData := make([]byte, 1024) // Append 1KB
	rand.Read(appendData)

	// Create combined expected data
	expectedData := append(initialData, appendData...)

	// Open file and append
	f, err := mfs.OpenFile("migration_test")
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Seek to end and write
	if _, err := f.Seek(0, 2); err != nil { // Seek to end
		f.Close()
		t.Fatalf("Seek failed: %v", err)
	}
	if _, err := f.Write(appendData); err != nil {
		f.Close()
		t.Fatalf("Write (append) failed: %v", err)
	}
	f.Close()

	// Verify file size increased
	info, err := mfs.Stat("migration_test")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	expectedSize := int64(initialSize + len(appendData))
	if info.Size() != expectedSize {
		t.Errorf("Size after migration: got %d, want %d", info.Size(), expectedSize)
	}

	// Verify all data is intact after migration
	readData, err = mfs.ReadFile("migration_test")
	if err != nil {
		t.Fatalf("ReadFile (after migration) failed: %v", err)
	}
	if !bytes.Equal(expectedData, readData) {
		t.Errorf("Data mismatch after migration: got %d bytes, want %d bytes",
			len(readData), len(expectedData))
	}

	t.Logf("Migration test passed: file grew from %d to %d bytes", initialSize, len(expectedData))
}

// TestSFBPersistence verifies that SFB-backed files survive unmount/remount.
func TestSFBPersistence(t *testing.T) {
	mfs := NewMountedFS(t, 50)

	if err := mfs.FormatAndMountWithSFB(false); err != nil {
		t.Fatalf("FormatAndMountWithSFB failed: %v", err)
	}

	// Create files of various sizes
	files := map[string][]byte{
		"tiny.txt":   make([]byte, 100),
		"small.txt":  make([]byte, 4096),
		"medium.txt": make([]byte, 16384),
		"large.txt":  make([]byte, 65536),
	}

	for name, data := range files {
		rand.Read(data)
		if err := mfs.WriteFile(name, data); err != nil {
			t.Fatalf("WriteFile(%s) failed: %v", name, err)
		}
	}

	// Unmount
	if err := mfs.Unmount(); err != nil {
		t.Fatalf("Unmount failed: %v", err)
	}

	// Remount (without format, with SFB)
	if err := mfs.MountWithSFB(false); err != nil {
		t.Fatalf("MountWithSFB (remount) failed: %v", err)
	}

	// Verify all files persisted
	for name, expectedData := range files {
		readData, err := mfs.ReadFile(name)
		if err != nil {
			t.Errorf("ReadFile(%s) after remount failed: %v", name, err)
			continue
		}

		if !bytes.Equal(expectedData, readData) {
			t.Errorf("Data mismatch for %s after remount", name)
		}
	}

	t.Log("Persistence test passed: all files survived remount")
}

// TestSFBOverwrite verifies that overwriting SFB-backed files works correctly.
func TestSFBOverwrite(t *testing.T) {
	mfs := NewMountedFS(t, 50)

	if err := mfs.FormatAndMountWithSFB(false); err != nil {
		t.Fatalf("FormatAndMountWithSFB failed: %v", err)
	}

	// Create initial file
	initialData := make([]byte, 4096)
	rand.Read(initialData)

	if err := mfs.WriteFile("overwrite_test", initialData); err != nil {
		t.Fatalf("WriteFile (initial) failed: %v", err)
	}

	// Overwrite with different data of same size
	newData := make([]byte, 4096)
	rand.Read(newData)

	if err := mfs.WriteFile("overwrite_test", newData); err != nil {
		t.Fatalf("WriteFile (overwrite) failed: %v", err)
	}

	// Verify new data
	readData, err := mfs.ReadFile("overwrite_test")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(newData, readData) {
		t.Error("Data mismatch after overwrite")
	}

	// Overwrite with smaller data
	smallerData := make([]byte, 1024)
	rand.Read(smallerData)

	if err := mfs.WriteFile("overwrite_test", smallerData); err != nil {
		t.Fatalf("WriteFile (smaller) failed: %v", err)
	}

	readData, err = mfs.ReadFile("overwrite_test")
	if err != nil {
		t.Fatalf("ReadFile (smaller) failed: %v", err)
	}

	if !bytes.Equal(smallerData, readData) {
		t.Error("Data mismatch after smaller overwrite")
	}
}

// fileSizeName generates a filename based on size/index for test organization.
func fileSizeName(sizeOrIndex int) string {
	return "file_" + string(rune('a'+sizeOrIndex%26)) + "_" + itoa(sizeOrIndex)
}

// itoa is a simple int to string conversion.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf) - 1
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		buf[i] = byte('0' + n%10)
		n /= 10
		i--
	}
	if neg {
		buf[i] = '-'
		i--
	}
	return string(buf[i+1:])
}
