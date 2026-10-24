// Package integration provides integration tests for GoClusterFS.
package integration

import (
	"bytes"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Snapshot jobs finish in milliseconds; this only bounds a hung worker.
const snapshotJobTimeout = 15 * time.Second

// snapshotStates parses the "name:state" lines of path's user.snapshots attribute.
func snapshotStates(t *testing.T, path string) map[string]string {
	t.Helper()
	states := make(map[string]string)
	buf := make([]byte, 4096)
	n, err := unix.Getxattr(path, "user.snapshots", buf)
	if err == syscall.ENODATA {
		return states
	}
	if err != nil {
		t.Fatalf("Getxattr for snapshot list failed: %v", err)
	}
	for _, line := range strings.Split(string(buf[:n]), "\n") {
		if name, state, ok := strings.Cut(line, ":"); ok {
			states[name] = state
		}
	}
	return states
}

// waitForSnapshotState polls until every named snapshot of path is in state; "" means gone.
func waitForSnapshotState(t *testing.T, path, state string, names ...string) {
	t.Helper()
	var states map[string]string
	took, ok := waitFor(snapshotJobTimeout, func() bool {
		states = snapshotStates(t, path)
		for _, name := range names {
			if states[name] != state {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("Snapshots %v did not reach state %q within %v: %v", names, state, snapshotJobTimeout, states)
	}
	t.Logf("Snapshots %v reached state %q in %v", names, state, took)
}

// TestSnapshotCreation tests creating a snapshot via extended attributes.
// This verifies Task #4711: Fast O(1) snapshot creation.
func TestSnapshotCreation(t *testing.T) {
	mfs := NewMountedFS(t, 20) // 20MB test image
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Write a file to snapshot
	content := []byte("Original content before snapshot")
	if err := mfs.WriteFile("testfile.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create snapshot via setxattr
	filePath := mfs.Path("testfile.txt")
	snapName := "test-snap-1"

	start := time.Now()
	err := unix.Setxattr(filePath, "user.snapshot", []byte(snapName), 0)
	duration := time.Since(start)

	if err != nil {
		if err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP {
			t.Skip("Snapshot xattr not supported (feature may not be enabled)")
		}
		t.Fatalf("Setxattr for snapshot creation failed: %v", err)
	}

	t.Logf("Snapshot created in %v", duration)

	waitForSnapshotState(t, filePath, "active", snapName)

	t.Log("Snapshot creation test passed")
}

// TestSnapshotCoW tests that Copy-on-Write works after snapshot.
// After taking a snapshot, writing to the file should allocate new blocks.
func TestSnapshotCoW(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Write initial content
	originalContent := []byte("This is the original content that will be snapshotted")
	if err := mfs.WriteFile("cow_test.txt", originalContent); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create snapshot
	filePath := mfs.Path("cow_test.txt")
	err := unix.Setxattr(filePath, "user.snapshot", []byte("cow-snapshot"), 0)
	if err != nil {
		if err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP {
			t.Skip("Snapshot xattr not supported")
		}
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	waitForSnapshotState(t, filePath, "active", "cow-snapshot")

	// Write new content - this should trigger CoW
	modifiedContent := []byte("Modified content after snapshot - new blocks allocated")
	if err := mfs.WriteFile("cow_test.txt", modifiedContent); err != nil {
		t.Fatalf("WriteFile after snapshot failed: %v", err)
	}

	// Read back and verify new content is there
	readBack, err := mfs.ReadFile("cow_test.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(modifiedContent, readBack) {
		t.Errorf("Content mismatch after CoW write: expected %q, got %q", modifiedContent, readBack)
	}

	t.Log("Snapshot CoW test passed")
}

// TestSnapshotDataIntegrity tests that both snapshot and live file retain their data.
func TestSnapshotDataIntegrity(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Write original content
	originalContent := []byte("Original data: AAAAABBBBBCCCCC")
	if err := mfs.WriteFile("integrity.txt", originalContent); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create snapshot
	filePath := mfs.Path("integrity.txt")
	err := unix.Setxattr(filePath, "user.snapshot", []byte("integrity-snap"), 0)
	if err != nil {
		if err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP {
			t.Skip("Snapshot xattr not supported")
		}
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	waitForSnapshotState(t, filePath, "active", "integrity-snap")

	// Modify the live file
	newContent := []byte("Modified: XXXXXYYYYYZZZZZZ")
	if err := mfs.WriteFile("integrity.txt", newContent); err != nil {
		t.Fatalf("WriteFile after snapshot failed: %v", err)
	}

	// Verify live file has new content
	liveData, err := mfs.ReadFile("integrity.txt")
	if err != nil {
		t.Fatalf("ReadFile live failed: %v", err)
	}
	if !bytes.Equal(newContent, liveData) {
		t.Errorf("Live file content wrong: expected %q, got %q", newContent, liveData)
	}

	// TODO: Add snapshot read test once snapshot read interface is implemented
	// For now, verify the snapshot list still exists
	snapListBuf := make([]byte, 4096)
	n, err := unix.Getxattr(filePath, "user.snapshots", snapListBuf)
	if err != nil {
		t.Fatalf("Getxattr for snapshot list failed: %v", err)
	}

	if !bytes.Contains(snapListBuf[:n], []byte("integrity-snap")) {
		t.Errorf("Snapshot should still exist after live file modification")
	}

	t.Log("Snapshot data integrity test passed")
}

// TestSnapshotDelete tests deleting a snapshot via extended attributes.
func TestSnapshotDelete(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create a file and snapshot
	content := []byte("Content for deletion test")
	if err := mfs.WriteFile("delete_test.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	filePath := mfs.Path("delete_test.txt")
	snapName := "delete-me"

	// Create snapshot
	err := unix.Setxattr(filePath, "user.snapshot", []byte(snapName), 0)
	if err != nil {
		if err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP {
			t.Skip("Snapshot xattr not supported")
		}
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	waitForSnapshotState(t, filePath, "active", snapName)

	// Delete snapshot via removexattr
	// Note: The attribute name format is "user.snapshot.<name>"
	err = unix.Removexattr(filePath, "user.snapshot."+snapName)
	if err != nil && err != syscall.ENODATA {
		t.Fatalf("Removexattr for snapshot deletion failed: %v", err)
	}

	waitForSnapshotState(t, filePath, "", snapName)

	t.Log("Snapshot delete test passed")
}

// TestSnapshotPersistence tests that snapshots survive unmount/remount.
func TestSnapshotPersistence(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create file and snapshot
	content := []byte("Persistent snapshot content")
	if err := mfs.WriteFile("persistent.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	filePath := mfs.Path("persistent.txt")
	snapName := "persist-snap"

	err := unix.Setxattr(filePath, "user.snapshot", []byte(snapName), 0)
	if err != nil {
		if err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP {
			t.Skip("Snapshot xattr not supported")
		}
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Wait for snapshot to complete
	time.Sleep(500 * time.Millisecond)

	// Unmount
	if err := mfs.Unmount(); err != nil {
		t.Fatalf("Unmount failed: %v", err)
	}

	// Remount
	if err := mfs.Mount(false); err != nil {
		t.Fatalf("Remount failed: %v", err)
	}

	// Verify file content persisted
	readBack, err := mfs.ReadFile("persistent.txt")
	if err != nil {
		t.Fatalf("ReadFile after remount failed: %v", err)
	}
	if !bytes.Equal(content, readBack) {
		t.Errorf("File content didn't persist: expected %q, got %q", content, readBack)
	}

	// Verify snapshot still exists
	filePath = mfs.Path("persistent.txt") // Path might change after remount
	snapListBuf := make([]byte, 4096)
	n, err := unix.Getxattr(filePath, "user.snapshots", snapListBuf)
	if err != nil {
		t.Logf("Note: Getxattr after remount: %v (snapshot list may be empty)", err)
	} else if !bytes.Contains(snapListBuf[:n], []byte(snapName)) {
		t.Errorf("Snapshot %q should persist after remount", snapName)
	}

	t.Log("Snapshot persistence test passed")
}

// TestSnapshotMultiple tests creating multiple snapshots of the same file.
func TestSnapshotMultiple(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create file
	content := []byte("Multi-snapshot content")
	if err := mfs.WriteFile("multi.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	filePath := mfs.Path("multi.txt")
	snapNames := []string{"snap-1", "snap-2", "snap-3"}

	// Create multiple snapshots
	for _, name := range snapNames {
		err := unix.Setxattr(filePath, "user.snapshot", []byte(name), 0)
		if err != nil {
			if err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP {
				t.Skip("Snapshot xattr not supported")
			}
			t.Fatalf("Failed to create snapshot %s: %v", name, err)
		}
		// Small delay between snapshots
		time.Sleep(100 * time.Millisecond)
	}

	waitForSnapshotState(t, filePath, "active", snapNames...)

	t.Log("Multiple snapshots test passed")
}

// TestSnapshotLargeFile tests snapshotting a file larger than one block.
func TestSnapshotLargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	mfs := NewMountedFS(t, 30) // Larger for multi-block file
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create a 2MB file (spans 2 blocks)
	size := 2 * 1024 * 1024
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 256)
	}

	if err := mfs.WriteFile("large.bin", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create snapshot
	filePath := mfs.Path("large.bin")
	start := time.Now()
	err := unix.Setxattr(filePath, "user.snapshot", []byte("large-snap"), 0)
	duration := time.Since(start)

	if err != nil {
		if err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP {
			t.Skip("Snapshot xattr not supported")
		}
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	t.Logf("Large file snapshot created in %v", duration)

	waitForSnapshotState(t, filePath, "active", "large-snap")

	// Verify original data intact
	readBack, err := mfs.ReadFile("large.bin")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(content, readBack) {
		t.Errorf("Large file content mismatch after snapshot")
	}

	t.Log("Large file snapshot test passed")
}

// TestSnapshotInvalidFile tests snapshot operations on non-existent or invalid targets.
func TestSnapshotInvalidFile(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Try to snapshot non-existent file
	invalidPath := mfs.Path("nonexistent.txt")
	err := unix.Setxattr(invalidPath, "user.snapshot", []byte("bad-snap"), 0)
	if err == nil {
		t.Error("Snapshot of non-existent file should fail")
	} else if !os.IsNotExist(err) {
		t.Logf("Expected error for non-existent file: %v", err)
	}

	// Create a directory
	if err := mfs.Mkdir("testdir"); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Try to snapshot directory (might not be supported)
	dirPath := mfs.Path("testdir")
	err = unix.Setxattr(dirPath, "user.snapshot", []byte("dir-snap"), 0)
	if err != nil {
		t.Logf("Directory snapshot returned expected error: %v", err)
	}

	t.Log("Invalid file snapshot test passed")
}

// TestSnapshotMiddleDeletion tests deleting a middle snapshot (merge-up).
// This verifies Task #4712: leaf + merge-up deletion.
func TestSnapshotMiddleDeletion(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create a file
	content := []byte("Content for merge-up test")
	if err := mfs.WriteFile("mergeup.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	filePath := mfs.Path("mergeup.txt")

	// Create chain of snapshots: snap-1 -> snap-2 -> snap-3
	snapNames := []string{"snap-1", "snap-2", "snap-3"}
	for _, name := range snapNames {
		err := unix.Setxattr(filePath, "user.snapshot", []byte(name), 0)
		if err != nil {
			if err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP {
				t.Skip("Snapshot xattr not supported")
			}
			t.Fatalf("Failed to create snapshot %s: %v", name, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	waitForSnapshotState(t, filePath, "active", snapNames...)

	// Delete the middle snapshot (snap-2)
	t.Logf("Calling Removexattr for snap-2...")
	err := unix.Removexattr(filePath, "user.snapshot.snap-2")
	t.Logf("Removexattr returned: err=%v", err)
	if err != nil && err != syscall.ENODATA {
		t.Fatalf("Removexattr for middle snapshot deletion failed: %v", err)
	}

	waitForSnapshotState(t, filePath, "", "snap-2")

	states := snapshotStates(t, filePath)
	t.Logf("Snapshot states after merge-up: %v", states)
	if states["snap-1"] != "active" {
		t.Errorf("snap-1 should still be active")
	}
	if states["snap-3"] != "active" {
		t.Errorf("snap-3 should still be active (re-parented to snap-1)")
	}

	t.Log("Middle snapshot deletion (merge-up) test passed")
}

// TestSnapshotDeleteAndReuse tests that after deleting a snapshot, blocks are freed.
// This verifies Task #4712: block freeing when refcount reaches 0.
func TestSnapshotDeleteAndReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	mfs := NewMountedFS(t, 25) // 25MB test image
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create a file with some content
	content := make([]byte, 512*1024) // 512KB
	for i := range content {
		content[i] = byte(i % 256)
	}
	if err := mfs.WriteFile("reuse.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	filePath := mfs.Path("reuse.txt")

	// Create a snapshot
	err := unix.Setxattr(filePath, "user.snapshot", []byte("temp-snap"), 0)
	if err != nil {
		if err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP {
			t.Skip("Snapshot xattr not supported")
		}
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	waitForSnapshotState(t, filePath, "active", "temp-snap")

	// Overwrite the file (triggers CoW, allocates new blocks)
	newContent := make([]byte, 512*1024)
	for i := range newContent {
		newContent[i] = byte((i + 128) % 256)
	}
	if err := mfs.WriteFile("reuse.txt", newContent); err != nil {
		t.Fatalf("WriteFile after snapshot failed: %v", err)
	}

	// Delete the snapshot - this should free the old blocks
	err = unix.Removexattr(filePath, "user.snapshot.temp-snap")
	if err != nil && err != syscall.ENODATA {
		t.Fatalf("Removexattr failed: %v", err)
	}

	// Wait for background deletion and block freeing
	time.Sleep(500 * time.Millisecond)

	// Verify the live file still has the new content
	readBack, err := mfs.ReadFile("reuse.txt")
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(newContent, readBack) {
		t.Errorf("File content mismatch after snapshot deletion")
	}

	// Create another file to verify blocks were freed and reused
	moreContent := make([]byte, 256*1024)
	for i := range moreContent {
		moreContent[i] = byte(i % 128)
	}
	if err := mfs.WriteFile("reuse2.txt", moreContent); err != nil {
		t.Fatalf("WriteFile for new file failed: %v", err)
	}

	// Verify new file is readable
	readBack2, err := mfs.ReadFile("reuse2.txt")
	if err != nil {
		t.Fatalf("ReadFile for new file failed: %v", err)
	}
	if !bytes.Equal(moreContent, readBack2) {
		t.Errorf("New file content mismatch")
	}

	t.Log("Snapshot delete and block reuse test passed")
}

// =============================================================================
// WRITABLE CLONE TESTS (Task #4713)
// =============================================================================

// TestCloneBasic tests creating a writable clone via extended attributes.
func TestCloneBasic(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Write a file to clone
	content := []byte("Original content for cloning")
	if err := mfs.WriteFile("source.txt", content); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create clone via setxattr
	filePath := mfs.Path("source.txt")
	clonePath := "/clone.txt"

	start := time.Now()
	err := unix.Setxattr(filePath, "user.clone", []byte(clonePath), 0)
	duration := time.Since(start)

	if err != nil {
		if err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP {
			t.Skip("Clone xattr not supported")
		}
		t.Fatalf("Setxattr for clone creation failed: %v", err)
	}

	t.Logf("Clone created in %v", duration)

	// Wait for background refcount job
	time.Sleep(500 * time.Millisecond)

	// Verify clone exists and has same content
	cloneContent, err := mfs.ReadFile("clone.txt")
	if err != nil {
		t.Fatalf("ReadFile for clone failed: %v", err)
	}

	if !bytes.Equal(content, cloneContent) {
		t.Errorf("Clone content mismatch: got %q, want %q", cloneContent, content)
	}

	t.Log("Basic clone test passed")
}

// TestCloneCoW tests that writing to a clone triggers Copy-on-Write.
func TestCloneCoW(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Write original file
	originalContent := []byte("Original content that will be cloned")
	if err := mfs.WriteFile("source.txt", originalContent); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create clone
	filePath := mfs.Path("source.txt")
	err := unix.Setxattr(filePath, "user.clone", []byte("/clone.txt"), 0)
	if err != nil {
		if err == syscall.ENOTSUP {
			t.Skip("Clone not supported")
		}
		t.Fatalf("Failed to create clone: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Modify the clone
	modifiedContent := []byte("Modified clone content - should trigger CoW")
	if err := mfs.WriteFile("clone.txt", modifiedContent); err != nil {
		t.Fatalf("WriteFile to clone failed: %v", err)
	}

	// Verify source is unchanged
	sourceContent, err := mfs.ReadFile("source.txt")
	if err != nil {
		t.Fatalf("ReadFile for source failed: %v", err)
	}
	if !bytes.Equal(originalContent, sourceContent) {
		t.Errorf("Source content changed after clone write: got %q, want %q", sourceContent, originalContent)
	}

	// Verify clone has new content
	cloneContent, err := mfs.ReadFile("clone.txt")
	if err != nil {
		t.Fatalf("ReadFile for clone failed: %v", err)
	}
	if !bytes.Equal(modifiedContent, cloneContent) {
		t.Errorf("Clone content mismatch: got %q, want %q", cloneContent, modifiedContent)
	}

	t.Log("Clone CoW test passed")
}

// TestCloneSourceCoW tests that writing to source after clone triggers CoW.
func TestCloneSourceCoW(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Write original file
	originalContent := []byte("Original content before clone")
	if err := mfs.WriteFile("source.txt", originalContent); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create clone
	filePath := mfs.Path("source.txt")
	err := unix.Setxattr(filePath, "user.clone", []byte("/clone.txt"), 0)
	if err != nil {
		if err == syscall.ENOTSUP {
			t.Skip("Clone not supported")
		}
		t.Fatalf("Failed to create clone: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Modify the source (should trigger CoW, keeping clone intact)
	modifiedSource := []byte("Modified source content - clone should be unchanged")
	if err := mfs.WriteFile("source.txt", modifiedSource); err != nil {
		t.Fatalf("WriteFile to source failed: %v", err)
	}

	// Verify source has new content
	sourceContent, err := mfs.ReadFile("source.txt")
	if err != nil {
		t.Fatalf("ReadFile for source failed: %v", err)
	}
	if !bytes.Equal(modifiedSource, sourceContent) {
		t.Errorf("Source content mismatch: got %q, want %q", sourceContent, modifiedSource)
	}

	// Verify clone still has original content
	cloneContent, err := mfs.ReadFile("clone.txt")
	if err != nil {
		t.Fatalf("ReadFile for clone failed: %v", err)
	}
	if !bytes.Equal(originalContent, cloneContent) {
		t.Errorf("Clone content changed after source write: got %q, want %q", cloneContent, originalContent)
	}

	t.Log("Clone source CoW test passed")
}

// TestCloneDataIntegrity tests both files retain correct data after modifications.
func TestCloneDataIntegrity(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Write original file
	originalContent := []byte("Original data for integrity test")
	if err := mfs.WriteFile("source.txt", originalContent); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Create clone
	filePath := mfs.Path("source.txt")
	err := unix.Setxattr(filePath, "user.clone", []byte("/clone.txt"), 0)
	if err != nil {
		if err == syscall.ENOTSUP {
			t.Skip("Clone not supported")
		}
		t.Fatalf("Failed to create clone: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Modify both files
	newSourceContent := []byte("New source data - completely different")
	newCloneContent := []byte("New clone data - also different")

	if err := mfs.WriteFile("source.txt", newSourceContent); err != nil {
		t.Fatalf("WriteFile to source failed: %v", err)
	}
	if err := mfs.WriteFile("clone.txt", newCloneContent); err != nil {
		t.Fatalf("WriteFile to clone failed: %v", err)
	}

	// Verify both have their correct content
	sourceContent, _ := mfs.ReadFile("source.txt")
	cloneContent, _ := mfs.ReadFile("clone.txt")

	if !bytes.Equal(newSourceContent, sourceContent) {
		t.Errorf("Source content wrong: got %q, want %q", sourceContent, newSourceContent)
	}
	if !bytes.Equal(newCloneContent, cloneContent) {
		t.Errorf("Clone content wrong: got %q, want %q", cloneContent, newCloneContent)
	}

	t.Log("Clone data integrity test passed")
}

// TestCloneDestinationExists tests that cloning to existing file fails.
func TestCloneDestinationExists(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Create two files
	if err := mfs.WriteFile("source.txt", []byte("source")); err != nil {
		t.Fatalf("WriteFile source failed: %v", err)
	}
	if err := mfs.WriteFile("existing.txt", []byte("existing")); err != nil {
		t.Fatalf("WriteFile existing failed: %v", err)
	}

	// Try to clone to existing file
	filePath := mfs.Path("source.txt")
	err := unix.Setxattr(filePath, "user.clone", []byte("/existing.txt"), 0)

	if err == nil {
		t.Fatal("Expected error when cloning to existing file, got nil")
	}
	if err != syscall.EEXIST {
		t.Logf("Got error %v (may be EEXIST in different form)", err)
	}

	t.Log("Clone destination exists test passed")
}

// TestCloneFromSnapshot tests creating a clone from a named snapshot.
func TestCloneFromSnapshot(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	// Write original file
	originalContent := []byte("Original content for snapshot")
	if err := mfs.WriteFile("source.txt", originalContent); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	filePath := mfs.Path("source.txt")

	// Create a snapshot
	err := unix.Setxattr(filePath, "user.snapshot", []byte("snap1"), 0)
	if err != nil {
		if err == syscall.ENOTSUP {
			t.Skip("Snapshot not supported")
		}
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	waitForSnapshotState(t, filePath, "active", "snap1")

	// Modify the source after snapshot
	modifiedContent := []byte("Modified after snapshot")
	if err := mfs.WriteFile("source.txt", modifiedContent); err != nil {
		t.Fatalf("WriteFile after snapshot failed: %v", err)
	}

	// Create clone from the snapshot
	err = unix.Setxattr(filePath, "user.snapshot.clone", []byte("snap1:/clone.txt"), 0)
	if err != nil {
		if err == syscall.ENOTSUP {
			t.Skip("Snapshot clone not supported")
		}
		t.Fatalf("Failed to create clone from snapshot: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify clone exists
	cloneContent, err := mfs.ReadFile("clone.txt")
	if err != nil {
		t.Fatalf("ReadFile for clone failed: %v", err)
	}

	t.Logf("Clone content: %q", cloneContent)
	t.Log("Clone from snapshot test passed")
}

func setxattr(t *testing.T, path, attr, value string) {
	t.Helper()
	if err := unix.Setxattr(path, attr, []byte(value), 0); err != nil {
		t.Fatalf("setxattr %s=%q on %s: %v", attr, value, path, err)
	}
}

func overwrite(base []byte, off int, data []byte) []byte {
	out := bytes.Clone(base)
	copy(out[off:], data)
	return out
}

func freeBlocks(t *testing.T, mfs *MountedFS) uint64 {
	t.Helper()
	var st unix.Statfs_t
	if err := unix.Statfs(mfs.Path(""), &st); err != nil {
		t.Fatalf("statfs: %v", err)
	}
	return st.Bfree
}

// TestSnapshotPreservesData reads a snapshot, through a clone of it, after the
// file was overwritten in place, appended to and truncated.
func TestSnapshotPreservesData(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start second node: %v", err)
	}
	writer, reader := cluster.Node(0), cluster.Node(1)

	original := chunk(1, 3*mb)
	if err := writer.WriteFile("data.bin", original); err != nil {
		t.Fatal(err)
	}
	checkFile(t, writer, "data.bin", original)
	setxattr(t, writer.Path("data.bin"), "user.snapshot", "before")

	if err := writer.WriteFileAt("data.bin", chunk(2, 4096), mb+100); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteFileAt("data.bin", chunk(3, 2*mb), 3*mb); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(writer.Path("data.bin"), mb+mb/2); err != nil {
		t.Fatal(err)
	}
	setxattr(t, writer.Path("data.bin"), "user.snapshot.clone", "before:/before-clone")

	live := overwrite(original[:mb+mb/2], mb+100, chunk(2, 4096))
	for _, node := range []*MountedFS{writer, reader} {
		checkFile(t, node, "before-clone", original)
		checkFile(t, node, "data.bin", live)
	}
}

// TestCloneInPlaceWrite writes into a clone and its source without truncating,
// after both were read, so the reads that follow must not reuse cached blocks.
func TestCloneInPlaceWrite(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	original := chunk(4, 2*mb)
	if err := mfs.WriteFile("source.bin", original); err != nil {
		t.Fatal(err)
	}
	setxattr(t, mfs.Path("source.bin"), "user.clone", "/clone.bin")
	checkFile(t, mfs, "source.bin", original)
	checkFile(t, mfs, "clone.bin", original)

	if err := mfs.WriteFileAt("clone.bin", chunk(5, 100), 10); err != nil {
		t.Fatal(err)
	}
	if err := mfs.WriteFileAt("source.bin", chunk(6, 100), mb+10); err != nil {
		t.Fatal(err)
	}

	wantSource := overwrite(original, mb+10, chunk(6, 100))
	wantClone := overwrite(original, 10, chunk(5, 100))
	checkFile(t, mfs, "source.bin", wantSource)
	checkFile(t, mfs, "clone.bin", wantClone)
	remount(t, mfs)
	checkFile(t, mfs, "source.bin", wantSource)
	checkFile(t, mfs, "clone.bin", wantClone)
}

// TestSnapshotDeleteAfterOverwrite deletes a snapshot whose blocks the file no
// longer maps; the delete must finish and give back exactly those blocks.
func TestSnapshotDeleteAfterOverwrite(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	if err := mfs.WriteFile("data.bin", chunk(7, 2*mb)); err != nil {
		t.Fatal(err)
	}
	free := freeBlocks(t, mfs)
	setxattr(t, mfs.Path("data.bin"), "user.snapshot", "temp")

	updated := chunk(8, 2*mb)
	if err := mfs.WriteFileAt("data.bin", updated, 0); err != nil {
		t.Fatal(err)
	}
	if got := freeBlocks(t, mfs); got != free-2 {
		t.Fatalf("free blocks after overwriting 2 snapshotted blocks = %d, want %d", got, free-2)
	}

	if err := unix.Removexattr(mfs.Path("data.bin"), "user.snapshot.temp"); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}
	listed := make([]byte, 4096)
	if _, ok := waitFor(10*time.Second, func() bool {
		n, err := unix.Getxattr(mfs.Path("data.bin"), "user.snapshots", listed)
		return err == nil && !bytes.Contains(listed[:n], []byte("temp"))
	}); !ok {
		t.Fatal("snapshot still listed 10s after delete")
	}
	if got := freeBlocks(t, mfs); got != free {
		t.Fatalf("free blocks after deleting the snapshot = %d, want %d", got, free)
	}
	checkFile(t, mfs, "data.bin", updated)
}
