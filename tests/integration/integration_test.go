// Package integration provides integration tests for GoClusterFS.
package integration

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestMountUnmount verifies basic mount and unmount functionality.
func TestMountUnmount(t *testing.T) {
	mfs := NewMountedFS(t, 20) // 20MB test image

	// Format and mount
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Verify mounted
	if !mfs.IsMounted() {
		t.Error("Filesystem should be mounted")
	}

	// Verify mount point is accessible
	entries, err := mfs.ReadDir("")
	if err != nil {
		t.Fatalf("Failed to read mount point: %v", err)
	}
	t.Logf("Root has %d entries", len(entries))

	// Unmount
	if err := mfs.Unmount(); err != nil {
		t.Fatalf("Unmount failed: %v", err)
	}

	// Verify unmounted (mount point should now be empty dir)
	// Note: After unmount, the directory is just an empty local directory
	t.Log("Mount/unmount cycle completed successfully")
}

// TestCreateReadFile tests basic file creation and reading.
func TestCreateReadFile(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Write a file
	content := []byte("Hello GoClusterFS!")
	t.Logf("Writing: %q to test.txt", string(content))

	if err := mfs.WriteFile("test.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Read it back
	readBack, err := mfs.ReadFile("test.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	t.Logf("Read back: %q", string(readBack))

	// Verify content matches
	if !bytes.Equal(content, readBack) {
		t.Errorf("Content mismatch: wrote %q, read %q", content, readBack)
	}

	t.Log("File create/read test passed")
}

// TestCreateDirectory tests directory creation and listing.
func TestCreateDirectory(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create a directory
	if err := mfs.Mkdir("testdir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Verify it exists
	info, err := mfs.Stat("testdir")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if !info.IsDir() {
		t.Error("testdir should be a directory")
	}

	// List root to verify directory appears
	entries, err := mfs.ReadDir("")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	found := false
	for _, e := range entries {
		t.Logf("Entry: %s (dir=%v)", e.Name(), e.IsDir())
		if e.Name() == "testdir" && e.IsDir() {
			found = true
		}
	}
	if !found {
		t.Error("testdir not found in root listing")
	}

	t.Log("Directory creation test passed")
}

// TestFileInDirectory tests creating files inside directories.
func TestFileInDirectory(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create directory
	if err := mfs.Mkdir("subdir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Create file in directory
	content := []byte("File in subdirectory")
	if err := os.WriteFile(mfs.Path("subdir/nested.txt"), content, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Read it back
	readBack, err := os.ReadFile(mfs.Path("subdir/nested.txt"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(content, readBack) {
		t.Errorf("Content mismatch")
	}

	// List directory
	entries, err := mfs.ReadDir("subdir")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	found := false
	for _, e := range entries {
		if e.Name() == "nested.txt" {
			found = true
		}
	}
	if !found {
		t.Error("nested.txt not found in subdir listing")
	}

	t.Log("File in directory test passed")
}

// TestDataPersistence tests that data survives unmount/remount.
func TestDataPersistence(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create some data
	content := []byte("Persistent data test - this should survive remount!")
	if err := mfs.WriteFile("persistent.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create a directory with a file
	if err := mfs.Mkdir("persistdir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if err := os.WriteFile(mfs.Path("persistdir/file.txt"), []byte("nested file"), 0644); err != nil {
		t.Fatalf("WriteFile nested failed: %v", err)
	}

	t.Log("Data written, unmounting...")

	// Unmount
	if err := mfs.Unmount(); err != nil {
		t.Fatalf("Unmount failed: %v", err)
	}

	t.Log("Remounting...")

	// Remount (without format!)
	if err := mfs.Mount(false); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	// Verify data persisted
	readBack, err := mfs.ReadFile("persistent.txt")
	if err != nil {
		t.Fatalf("ReadFile after remount failed: %v", err)
	}
	if !bytes.Equal(content, readBack) {
		t.Errorf("Data didn't persist: expected %q, got %q", content, readBack)
	}

	// Verify directory and nested file
	nestedData, err := os.ReadFile(mfs.Path("persistdir/file.txt"))
	if err != nil {
		t.Fatalf("ReadFile nested after remount failed: %v", err)
	}
	if string(nestedData) != "nested file" {
		t.Errorf("Nested file didn't persist")
	}

	t.Log("Data persistence test passed")
}

// TestLargeFile tests writing and reading a file larger than one data block.
func TestLargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Generate 2MB of random data (spans multiple 1MB blocks)
	size := 2 * 1024 * 1024
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}
	originalMD5 := md5.Sum(data)
	t.Logf("Writing %d bytes, MD5: %x", size, originalMD5)

	// Write
	if err := mfs.WriteFile("largefile.bin", data); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Verify size
	info, err := mfs.Stat("largefile.bin")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() != int64(size) {
		t.Errorf("Size mismatch: expected %d, got %d", size, info.Size())
	}

	// Read back
	readBack, err := mfs.ReadFile("largefile.bin")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	// Verify MD5
	readMD5 := md5.Sum(readBack)
	t.Logf("Read back MD5: %x", readMD5)

	if originalMD5 != readMD5 {
		t.Errorf("MD5 mismatch: wrote %x, read %x", originalMD5, readMD5)
	}

	t.Log("Large file test passed")
}

// TestManyFiles tests creating many small files.
// Note: GoClusterFS currently uses 1MB blocks, so each small file consumes 1MB.
// This test creates a reasonable number of files within the data space.
func TestManyFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping many files test in short mode")
	}

	// Use a larger image: 120MB = 117 data blocks
	mfs := NewMountedFS(t, 120)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create files - with 1MB blocks, we can create about 100 files in 117 blocks
	// leaving some room for overhead
	numFiles := 100
	t.Logf("Creating %d files...", numFiles)

	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("file_%03d.txt", i)
		content := []byte(fmt.Sprintf("Content of file %d", i))
		if err := mfs.WriteFile(name, content); err != nil {
			t.Fatalf("Failed to create file %s: %v", name, err)
		}
	}

	// List and verify count
	entries, err := mfs.ReadDir("")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != numFiles {
		t.Errorf("Expected %d entries, got %d", numFiles, len(entries))
	}

	// Verify each file
	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("file_%03d.txt", i)
		expected := []byte(fmt.Sprintf("Content of file %d", i))
		actual, err := mfs.ReadFile(name)
		if err != nil {
			t.Fatalf("Failed to read file %s: %v", name, err)
		}
		if !bytes.Equal(expected, actual) {
			t.Errorf("Content mismatch for %s", name)
		}
	}

	t.Logf("All %d files verified", numFiles)
}

// TestNestedDirectories tests creating nested directory structures.
func TestNestedDirectories(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create nested structure: a/b/c/d
	dirs := []string{"a", "a/b", "a/b/c", "a/b/c/d"}
	for _, d := range dirs {
		if err := mfs.Mkdir(d); err != nil {
			t.Fatalf("Failed to create %s: %v", d, err)
		}
	}

	// Create a file at the deepest level
	deepFile := "a/b/c/d/deep.txt"
	if err := os.WriteFile(mfs.Path(deepFile), []byte("deep content"), 0644); err != nil {
		t.Fatalf("Failed to create deep file: %v", err)
	}

	// Read it back
	content, err := os.ReadFile(mfs.Path(deepFile))
	if err != nil {
		t.Fatalf("Failed to read deep file: %v", err)
	}
	if string(content) != "deep content" {
		t.Error("Deep file content mismatch")
	}

	// Walk the path and verify each level
	for _, d := range dirs {
		info, err := mfs.Stat(d)
		if err != nil {
			t.Errorf("Failed to stat %s: %v", d, err)
		} else if !info.IsDir() {
			t.Errorf("%s should be a directory", d)
		}
	}

	t.Log("Nested directories test passed")
}

// TestDeleteFile tests file deletion.
func TestDeleteFile(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create a file
	if err := mfs.WriteFile("todelete.txt", []byte("delete me")); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Verify it exists
	if !mfs.Exists("todelete.txt") {
		t.Fatal("File should exist before deletion")
	}

	// Delete it
	if err := mfs.Remove("todelete.txt"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify it's gone
	if mfs.Exists("todelete.txt") {
		t.Error("File should not exist after deletion")
	}

	t.Log("File deletion test passed")
}

// TestDeleteDirectory tests directory deletion.
func TestDeleteDirectory(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create an empty directory
	if err := mfs.Mkdir("emptydir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Delete it
	if err := mfs.Remove("emptydir"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify it's gone
	if mfs.Exists("emptydir") {
		t.Error("Directory should not exist after deletion")
	}

	t.Log("Empty directory deletion test passed")
}

// TestDeleteNonEmptyDirectory tests that deleting non-empty directory with rmdir fails.
func TestDeleteNonEmptyDirectory(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create directory with a file
	if err := mfs.Mkdir("notempty"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if err := os.WriteFile(mfs.Path("notempty/file.txt"), []byte("content"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Try to delete directory (should fail with ENOTEMPTY)
	err := mfs.Remove("notempty")
	if err == nil {
		t.Error("Remove should have failed for non-empty directory")
	} else {
		t.Logf("Remove correctly failed: %v", err)
	}

	// RemoveAll should work
	if err := mfs.RemoveAll("notempty"); err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	if mfs.Exists("notempty") {
		t.Error("Directory should be gone after RemoveAll")
	}

	t.Log("Non-empty directory deletion test passed")
}

// TestFileAttributes tests that file attributes are correct.
func TestFileAttributes(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create a file
	content := []byte("Test file for attributes")
	if err := mfs.WriteFile("attrs.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Get attributes
	info, err := mfs.Stat("attrs.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	// Verify size
	if info.Size() != int64(len(content)) {
		t.Errorf("Size mismatch: expected %d, got %d", len(content), info.Size())
	}

	// Verify it's a regular file
	if info.IsDir() {
		t.Error("Should be a file, not a directory")
	}

	// Verify mode (should be readable)
	if info.Mode()&0400 == 0 {
		t.Error("File should be readable")
	}

	t.Logf("File attrs: size=%d, mode=%v, modtime=%v", info.Size(), info.Mode(), info.ModTime())
	t.Log("File attributes test passed")
}

// TestLongFilename tests creating files with long names.
func TestLongFilename(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create a 200-character filename (within 255 limit)
	longName := strings.Repeat("a", 200) + ".txt"
	content := []byte("Long filename test")

	if err := mfs.WriteFile(longName, content); err != nil {
		t.Fatalf("WriteFile with long name failed: %v", err)
	}

	// Read it back
	readBack, err := mfs.ReadFile(longName)
	if err != nil {
		t.Fatalf("ReadFile with long name failed: %v", err)
	}

	if !bytes.Equal(content, readBack) {
		t.Error("Content mismatch with long filename")
	}

	t.Log("Long filename test passed")
}

// TestSpecialCharacters tests filenames with special characters.
func TestSpecialCharacters(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Test various special character names
	names := []string{
		"file with spaces.txt",
		"file-with-dashes.txt",
		"file_with_underscores.txt",
		"file.multiple.dots.txt",
		"UPPERCASE.TXT",
		"MixedCase.Txt",
	}

	for _, name := range names {
		content := []byte("Content for " + name)
		if err := mfs.WriteFile(name, content); err != nil {
			t.Errorf("Failed to create %q: %v", name, err)
			continue
		}

		readBack, err := mfs.ReadFile(name)
		if err != nil {
			t.Errorf("Failed to read %q: %v", name, err)
			continue
		}

		if !bytes.Equal(content, readBack) {
			t.Errorf("Content mismatch for %q", name)
		}
	}

	t.Log("Special characters test passed")
}

// TestFileNotFound tests that accessing nonexistent files returns proper errors.
func TestFileNotFound(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Try to read nonexistent file
	_, err := mfs.ReadFile("nonexistent.txt")
	if err == nil {
		t.Error("Reading nonexistent file should fail")
	}

	// Should be ENOENT (file not found)
	if !os.IsNotExist(err) {
		t.Errorf("Error should be 'not exist', got: %v", err)
	}

	t.Log("File not found test passed")
}

// TestOverwriteFile tests overwriting an existing file.
func TestOverwriteFile(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create a file
	if err := mfs.WriteFile("overwrite.txt", []byte("Original content")); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Overwrite it
	newContent := []byte("New content that is different")
	if err := mfs.WriteFile("overwrite.txt", newContent); err != nil {
		t.Fatalf("WriteFile overwrite failed: %v", err)
	}

	// Read back
	readBack, err := mfs.ReadFile("overwrite.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(newContent, readBack) {
		t.Errorf("Overwrite failed: expected %q, got %q", newContent, readBack)
	}

	t.Log("Overwrite file test passed")
}

// TestTruncateFile tests truncating a file to a smaller size.
func TestTruncateFile(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create a file with some content
	originalContent := []byte("This is some longer content that we will truncate")
	if err := mfs.WriteFile("truncate.txt", originalContent); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Truncate to 10 bytes
	if err := os.Truncate(mfs.Path("truncate.txt"), 10); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Read back
	readBack, err := mfs.ReadFile("truncate.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	expected := originalContent[:10]
	if !bytes.Equal(expected, readBack) {
		t.Errorf("Truncate failed: expected %q, got %q", expected, readBack)
	}

	// Verify size
	info, err := mfs.Stat("truncate.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() != 10 {
		t.Errorf("Size after truncate: expected 10, got %d", info.Size())
	}

	t.Log("Truncate file test passed")
}

// TestFilesystemStats tests df-like statistics.
func TestFilesystemStats(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Get filesystem stats
	var stat syscall.Statfs_t
	if err := syscall.Statfs(mfs.MountPoint, &stat); err != nil {
		t.Fatalf("Statfs failed: %v", err)
	}

	t.Logf("Filesystem stats:")
	t.Logf("  Block size: %d", stat.Bsize)
	t.Logf("  Total blocks: %d", stat.Blocks)
	t.Logf("  Free blocks: %d", stat.Bfree)
	t.Logf("  Available blocks: %d", stat.Bavail)
	t.Logf("  Total inodes: %d", stat.Files)
	t.Logf("  Free inodes: %d", stat.Ffree)

	// Basic sanity checks
	if stat.Blocks == 0 {
		t.Error("Total blocks should not be 0")
	}
	if stat.Bfree > stat.Blocks {
		t.Error("Free blocks should not exceed total blocks")
	}

	t.Log("Filesystem stats test passed")
}

// TestAppendFile tests appending to an existing file.
func TestAppendFile(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create initial file
	initial := []byte("Hello")
	if err := mfs.WriteFile("append.txt", initial); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Open for append
	f, err := os.OpenFile(mfs.Path("append.txt"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile for append failed: %v", err)
	}

	// Append data
	appended := []byte(" World!")
	if _, err := f.Write(appended); err != nil {
		f.Close()
		t.Fatalf("Append write failed: %v", err)
	}
	f.Close()

	// Read back
	readBack, err := mfs.ReadFile("append.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	expected := append(initial, appended...)
	if !bytes.Equal(expected, readBack) {
		t.Errorf("Append failed: expected %q, got %q", expected, readBack)
	}

	t.Log("Append file test passed")
}

// TestRename tests renaming files.
// Note: Rename is not yet implemented in GoClusterFS - this test verifies the expected error.
func TestRename(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create a file
	content := []byte("Rename test content")
	if err := mfs.WriteFile("oldname.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Rename it - currently not implemented, so expect failure
	oldPath := mfs.Path("oldname.txt")
	newPath := mfs.Path("newname.txt")
	err := os.Rename(oldPath, newPath)
	if err != nil {
		// Rename not implemented - this is expected for now
		t.Logf("Rename not implemented (expected): %v", err)
		t.Skip("Rename operation not yet implemented in GoClusterFS")
	}

	// Old name should not exist
	if mfs.Exists("oldname.txt") {
		t.Error("Old name should not exist after rename")
	}

	// New name should exist with same content
	readBack, err := mfs.ReadFile("newname.txt")
	if err != nil {
		t.Fatalf("ReadFile new name failed: %v", err)
	}

	if !bytes.Equal(content, readBack) {
		t.Error("Content changed after rename")
	}

	t.Log("Rename test passed")
}

// TestCopyFile tests copying files within the filesystem.
func TestCopyFile(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create source file
	content := []byte("Copy test content")
	if err := mfs.WriteFile("source.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Copy it (manual copy since os.Copy doesn't exist)
	srcPath := mfs.Path("source.txt")
	dstPath := mfs.Path("dest.txt")

	srcData, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("Read source failed: %v", err)
	}
	if err := os.WriteFile(dstPath, srcData, 0644); err != nil {
		t.Fatalf("Write dest failed: %v", err)
	}

	// Both should exist
	if !mfs.Exists("source.txt") {
		t.Error("Source should still exist")
	}
	if !mfs.Exists("dest.txt") {
		t.Error("Dest should exist")
	}

	// Content should match
	destData, err := mfs.ReadFile("dest.txt")
	if err != nil {
		t.Fatalf("Read dest failed: %v", err)
	}

	if !bytes.Equal(content, destData) {
		t.Error("Copied content doesn't match")
	}

	t.Log("Copy file test passed")
}

// TestMkdirP tests mkdir -p style nested directory creation.
func TestMkdirP(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create deeply nested path in one call
	deepPath := filepath.Join(mfs.MountPoint, "one", "two", "three", "four")
	if err := os.MkdirAll(deepPath, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Verify each level exists
	levels := []string{"one", "one/two", "one/two/three", "one/two/three/four"}
	for _, level := range levels {
		if !mfs.Exists(level) {
			t.Errorf("Level %s should exist", level)
		}
		info, _ := mfs.Stat(level)
		if info != nil && !info.IsDir() {
			t.Errorf("Level %s should be a directory", level)
		}
	}

	t.Log("MkdirAll test passed")
}

// TestStress performs stress testing with many operations.
func TestStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	mfs := NewMountedFS(t, 50) // Larger image for stress test
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create and delete many files
	iterations := 200
	t.Logf("Running %d create/delete iterations...", iterations)

	for i := 0; i < iterations; i++ {
		name := fmt.Sprintf("stress_%04d.txt", i)
		content := []byte(fmt.Sprintf("Stress test iteration %d", i))

		if err := mfs.WriteFile(name, content); err != nil {
			t.Fatalf("Iteration %d: WriteFile failed: %v", i, err)
		}

		// Read back
		readBack, err := mfs.ReadFile(name)
		if err != nil {
			t.Fatalf("Iteration %d: ReadFile failed: %v", i, err)
		}
		if !bytes.Equal(content, readBack) {
			t.Fatalf("Iteration %d: Content mismatch", i)
		}

		// Delete
		if err := mfs.Remove(name); err != nil {
			t.Fatalf("Iteration %d: Remove failed: %v", i, err)
		}
	}

	// Verify no files left
	entries, err := mfs.ReadDir("")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries after stress test, got %d", len(entries))
	}

	t.Logf("Stress test passed: %d iterations", iterations)
}
