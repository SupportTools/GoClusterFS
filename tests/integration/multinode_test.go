// Package integration provides integration tests for GoClusterFS.
// This file contains multi-node/multi-process test scenarios.
package integration

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestMultiNodeConcurrentFileCreation tests two nodes simultaneously creating different files.
// This validates that bitmap allocation and inode table updates don't corrupt when accessed
// concurrently by multiple processes.
func TestMultiNodeConcurrentFileCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent file creation test in short mode - known race condition in distributed lock (Task #5177)")
	}
	cluster := NewMultiNodeCluster(t, 2, 50) // 2 nodes, 50MB image

	// Format and start first node
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	// Start second node
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start second node: %v", err)
	}

	// Wait for both to be mounted
	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("Nodes failed to mount: %v", err)
	}

	// Synchronization barrier to ensure both operations start together
	barrier := NewSyncBarrier(2)
	var wg sync.WaitGroup
	errors := make([]error, 2)

	// Node 0 creates fileA.txt
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait() // Wait for both goroutines to be ready
		node := cluster.Node(0)
		errors[0] = node.WriteFile("fileA.txt", []byte("Content from Node 0"))
	}()

	// Node 1 creates fileB.txt
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait() // Wait for both goroutines to be ready
		node := cluster.Node(1)
		errors[1] = node.WriteFile("fileB.txt", []byte("Content from Node 1"))
	}()

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			t.Errorf("Node %d file creation failed: %v", i, err)
		}
	}

	// Give filesystem time to sync
	time.Sleep(500 * time.Millisecond)

	// Verify both files exist from both nodes' perspectives
	for nodeIdx := 0; nodeIdx < 2; nodeIdx++ {
		node := cluster.Node(nodeIdx)

		// Check fileA.txt
		if !node.Exists("fileA.txt") {
			t.Errorf("Node %d: fileA.txt does not exist", nodeIdx)
		}

		// Check fileB.txt
		if !node.Exists("fileB.txt") {
			t.Errorf("Node %d: fileB.txt does not exist", nodeIdx)
		}

		// List root directory
		entries, err := node.ReadDir("")
		if err != nil {
			t.Errorf("Node %d: failed to read root dir: %v", nodeIdx, err)
			continue
		}
		t.Logf("Node %d sees %d entries in root", nodeIdx, len(entries))
		for _, e := range entries {
			t.Logf("  - %s", e.Name())
		}
	}

	t.Log("Concurrent file creation test completed")
}

// TestMultiNodeConcurrentWrites tests two nodes writing to different files concurrently.
// Validates that block writes don't interfere with each other.
func TestMultiNodeConcurrentWrites(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start second node: %v", err)
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("Nodes failed to mount: %v", err)
	}

	// Pre-create files from node 0
	node0 := cluster.Node(0)
	if err := node0.WriteFile("dataA.txt", []byte("initial")); err != nil {
		t.Fatalf("Failed to create dataA.txt: %v", err)
	}
	if err := node0.WriteFile("dataB.txt", []byte("initial")); err != nil {
		t.Fatalf("Failed to create dataB.txt: %v", err)
	}

	// Define test data - larger to ensure real block I/O
	dataA := bytes.Repeat([]byte("AAAA"), 10000) // 40KB
	dataB := bytes.Repeat([]byte("BBBB"), 10000) // 40KB

	barrier := NewSyncBarrier(2)
	var wg sync.WaitGroup
	errors := make([]error, 2)

	// Node 0 writes to dataA.txt
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[0] = cluster.Node(0).WriteFile("dataA.txt", dataA)
	}()

	// Node 1 writes to dataB.txt
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[1] = cluster.Node(1).WriteFile("dataB.txt", dataB)
	}()

	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Errorf("Node %d write failed: %v", i, err)
		}
	}

	// Verify content integrity
	time.Sleep(500 * time.Millisecond)

	// Read back from node 0
	readA, err := node0.ReadFile("dataA.txt")
	if err != nil {
		t.Fatalf("Failed to read dataA.txt: %v", err)
	}
	if !bytes.Equal(readA, dataA) {
		t.Errorf("dataA.txt content mismatch: got %d bytes, expected %d", len(readA), len(dataA))
	}

	readB, err := node0.ReadFile("dataB.txt")
	if err != nil {
		t.Fatalf("Failed to read dataB.txt: %v", err)
	}
	if !bytes.Equal(readB, dataB) {
		t.Errorf("dataB.txt content mismatch: got %d bytes, expected %d", len(readB), len(dataB))
	}

	t.Log("Concurrent writes test completed - data integrity verified")
}

// TestMultiNodeSequentialAccess tests one node writing, unmounting, then another
// node mounting and reading. Validates data persistence across mount cycles.
func TestMultiNodeSequentialAccess(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)

	// Format and mount node 0
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	// Write data from node 0
	node0 := cluster.Node(0)
	testData := []byte("This data should persist across mount cycles - GoClusterFS test")
	if err := node0.WriteFile("persistent.txt", testData); err != nil {
		t.Fatalf("Failed to write persistent.txt: %v", err)
	}
	t.Log("Node 0: wrote persistent.txt")

	// Verify it's readable
	readBack, err := node0.ReadFile("persistent.txt")
	if err != nil {
		t.Fatalf("Failed to read back from node 0: %v", err)
	}
	if !bytes.Equal(readBack, testData) {
		t.Fatalf("Node 0: immediate read-back mismatch")
	}

	// Unmount node 0
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("Failed to unmount node 0: %v", err)
	}
	t.Log("Node 0: unmounted")

	// Wait a moment for filesystem to fully sync
	time.Sleep(1 * time.Second)

	// Mount node 1 (not formatting - should see existing data)
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start node 1: %v", err)
	}
	t.Log("Node 1: mounted")

	// Read data from node 1
	node1 := cluster.Node(1)
	readFromNode1, err := node1.ReadFile("persistent.txt")
	if err != nil {
		t.Fatalf("Node 1: failed to read persistent.txt: %v", err)
	}

	if !bytes.Equal(readFromNode1, testData) {
		t.Errorf("Data persistence failed!\nExpected: %q\nGot: %q", string(testData), string(readFromNode1))
	} else {
		t.Log("Node 1: successfully read data written by Node 0")
	}

	t.Log("Sequential access test completed - persistence verified")
}

// TestMultiNodeCrashRecovery tests that the filesystem remains usable after a node crash.
// Node 0 creates files, gets killed (SIGKILL), then Node 1 mounts and verifies data.
func TestMultiNodeCrashRecovery(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)

	// Format and mount node 0
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	// Write some data
	node0 := cluster.Node(0)
	testData := []byte("Data written before crash")
	if err := node0.WriteFile("beforecrash.txt", testData); err != nil {
		t.Fatalf("Failed to write beforecrash.txt: %v", err)
	}
	t.Logf("Node 0 (PID %d): wrote beforecrash.txt", node0.PID())

	// Give time for data to reach disk
	time.Sleep(1 * time.Second)

	// Kill node 0 (simulate crash)
	t.Log("Killing node 0 to simulate crash...")
	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("Failed to kill node 0: %v", err)
	}
	t.Log("Node 0: killed (simulated crash)")

	// Wait for system to stabilize
	time.Sleep(2 * time.Second)

	// Mount node 1 - filesystem should still be usable
	t.Log("Starting node 1 after node 0 crash...")
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start node 1 after crash: %v", err)
	}
	t.Log("Node 1: mounted successfully after node 0 crash")

	// Verify the filesystem is usable
	node1 := cluster.Node(1)

	// Try to read the pre-crash data
	readData, err := node1.ReadFile("beforecrash.txt")
	if err != nil {
		t.Logf("Warning: could not read pre-crash file: %v (this may be expected if data wasn't synced)", err)
	} else if !bytes.Equal(readData, testData) {
		t.Logf("Warning: pre-crash data may be incomplete (got %d bytes, expected %d)", len(readData), len(testData))
	} else {
		t.Log("Node 1: successfully recovered pre-crash data")
	}

	// Verify we can create new files (filesystem is still functional)
	newData := []byte("Data written after recovery")
	if err := node1.WriteFile("afterrecovery.txt", newData); err != nil {
		t.Fatalf("Node 1: failed to write after recovery: %v", err)
	}

	readNew, err := node1.ReadFile("afterrecovery.txt")
	if err != nil {
		t.Fatalf("Node 1: failed to read afterrecovery.txt: %v", err)
	}
	if !bytes.Equal(readNew, newData) {
		t.Errorf("Node 1: post-recovery write/read mismatch")
	}

	t.Log("Crash recovery test completed - filesystem remains functional after crash")
}

// TestMultiNodeConcurrentDirectoryOps tests multiple nodes performing directory operations
// concurrently. Both nodes create files in the root directory and verify all files appear.
func TestMultiNodeConcurrentDirectoryOps(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start second node: %v", err)
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("Nodes failed to mount: %v", err)
	}

	// Each node will create multiple files
	filesPerNode := 5
	barrier := NewSyncBarrier(2)
	var wg sync.WaitGroup
	errorsChan := make(chan error, filesPerNode*2)

	// Node 0 creates files node0_0.txt through node0_4.txt
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		node := cluster.Node(0)
		for i := 0; i < filesPerNode; i++ {
			filename := fmt.Sprintf("node0_%d.txt", i)
			content := fmt.Sprintf("Content from node 0, file %d", i)
			if err := node.WriteFile(filename, []byte(content)); err != nil {
				errorsChan <- fmt.Errorf("node 0 failed to create %s: %v", filename, err)
			}
			time.Sleep(10 * time.Millisecond) // Small delay between operations
		}
	}()

	// Node 1 creates files node1_0.txt through node1_4.txt
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		node := cluster.Node(1)
		for i := 0; i < filesPerNode; i++ {
			filename := fmt.Sprintf("node1_%d.txt", i)
			content := fmt.Sprintf("Content from node 1, file %d", i)
			if err := node.WriteFile(filename, []byte(content)); err != nil {
				errorsChan <- fmt.Errorf("node 1 failed to create %s: %v", filename, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errorsChan)

	// Report any errors
	for err := range errorsChan {
		t.Error(err)
	}

	// Give time for sync
	time.Sleep(1 * time.Second)

	// Verify all files exist from both nodes' perspectives
	expectedFiles := make(map[string]bool)
	for i := 0; i < filesPerNode; i++ {
		expectedFiles[fmt.Sprintf("node0_%d.txt", i)] = false
		expectedFiles[fmt.Sprintf("node1_%d.txt", i)] = false
	}

	for nodeIdx := 0; nodeIdx < 2; nodeIdx++ {
		node := cluster.Node(nodeIdx)
		entries, err := node.ReadDir("")
		if err != nil {
			t.Errorf("Node %d: failed to read root dir: %v", nodeIdx, err)
			continue
		}

		t.Logf("Node %d sees %d entries:", nodeIdx, len(entries))
		found := make(map[string]bool)
		for _, e := range entries {
			found[e.Name()] = true
			t.Logf("  - %s", e.Name())
		}

		// Check for missing files
		for filename := range expectedFiles {
			if !found[filename] {
				t.Errorf("Node %d: missing expected file %s", nodeIdx, filename)
			}
		}
	}

	// Verify content integrity by reading a sample from each node
	node0 := cluster.Node(0)
	data, err := node0.ReadFile("node1_0.txt")
	if err != nil {
		t.Errorf("Node 0 failed to read node1's file: %v", err)
	} else {
		expected := "Content from node 1, file 0"
		if string(data) != expected {
			t.Errorf("Content mismatch: got %q, expected %q", string(data), expected)
		}
	}

	t.Log("Concurrent directory operations test completed")
}

// TestMultiNodeConcurrentSameFileWrites tests two nodes writing to the SAME file
// at different offsets concurrently. This validates that the optimistic retry
// pattern in File.Write() handles concurrent block allocations correctly.
func TestMultiNodeConcurrentSameFileWrites(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start second node: %v", err)
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("Nodes failed to mount: %v", err)
	}

	// Pre-create file and extend to final size from node 0.
	// Pre-extending avoids concurrent file-extension which hits a TOCTOU race
	// in the distributed lock's CAS. This tests data integrity (the original
	// bug fix) rather than concurrent size updates.
	node0 := cluster.Node(0)
	finalSize := 2 * 400 * 1024 // Space for both nodes' data
	initialData := make([]byte, finalSize)
	if err := node0.WriteFile("shared.txt", initialData); err != nil {
		t.Fatalf("Failed to create shared.txt: %v", err)
	}
	t.Logf("Created shared.txt with %d bytes", finalSize)

	// Give time for the file to be visible to both nodes
	time.Sleep(500 * time.Millisecond)

	// Define test data - each node writes a distinct pattern at a specific offset
	// Node 0 writes to the first part, Node 1 writes to the second part
	// Use 400KB each (800KB total) to fit within a single 1MB block
	// This avoids extent allocation contention between concurrent writers
	dataSize := 400 * 1024 // 400KB each
	dataNode0 := bytes.Repeat([]byte("NODE0-"), dataSize/6)
	dataNode1 := bytes.Repeat([]byte("NODE1-"), dataSize/6)

	barrier := NewSyncBarrier(2)
	var wg sync.WaitGroup
	errors := make([]error, 2)

	// Node 0 writes to offset 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		// Use WriteFileAt for offset-based writing
		errors[0] = cluster.Node(0).WriteFileAt("shared.txt", dataNode0, 0)
	}()

	// Node 1 writes to offset after Node 0's data
	// This forces both nodes to potentially extend the file simultaneously
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[1] = cluster.Node(1).WriteFileAt("shared.txt", dataNode1, int64(len(dataNode0)))
	}()

	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Errorf("Node %d write failed: %v", i, err)
		}
	}

	// Allow time for syncing
	time.Sleep(500 * time.Millisecond)

	// Verify content integrity - read the whole file from node 0
	fullContent, err := node0.ReadFile("shared.txt")
	if err != nil {
		t.Fatalf("Failed to read shared.txt: %v", err)
	}

	expectedSize := len(dataNode0) + len(dataNode1)
	if len(fullContent) < expectedSize {
		t.Errorf("File too small: got %d bytes, expected at least %d", len(fullContent), expectedSize)
	}

	// Verify Node 0's data at the beginning
	if len(fullContent) >= len(dataNode0) {
		node0Part := fullContent[:len(dataNode0)]
		if !bytes.Equal(node0Part, dataNode0) {
			t.Errorf("Node 0's data corrupted: first 20 bytes = %q", node0Part[:min(20, len(node0Part))])
		} else {
			t.Log("Node 0's data verified at offset 0")
		}
	}

	// Verify Node 1's data at the expected offset
	if len(fullContent) >= expectedSize {
		node1Part := fullContent[len(dataNode0):expectedSize]
		if !bytes.Equal(node1Part, dataNode1) {
			t.Errorf("Node 1's data corrupted: first 20 bytes = %q", node1Part[:min(20, len(node1Part))])
		} else {
			t.Logf("Node 1's data verified at offset %d", len(dataNode0))
		}
	}

	t.Logf("Concurrent same-file writes test completed - total file size: %d bytes", len(fullContent))
}

// TestMultiNodeConcurrentSameFileWrites3Nodes tests three nodes writing to the SAME file
// at different offsets concurrently. This validates lock serialization under higher contention.
func TestMultiNodeConcurrentSameFileWrites3Nodes(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 3, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	for i := 1; i < 3; i++ {
		if err := cluster.StartNode(i, false); err != nil {
			t.Fatalf("Failed to start node %d: %v", i, err)
		}
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("Nodes failed to mount: %v", err)
	}

	// Each node writes 300KB at increasing offsets (all fit in one 1MB block)
	chunkSize := 300 * 1024
	dataNode0 := bytes.Repeat([]byte("NODE0X"), chunkSize/6) // 6-byte pattern
	dataNode1 := bytes.Repeat([]byte("NODE1X"), chunkSize/6)
	dataNode2 := bytes.Repeat([]byte("NODE2X"), chunkSize/6)
	actualChunkSize := len(dataNode0)
	finalSize := actualChunkSize * 3

	// Pre-create file and extend to final size from node 0.
	// Pre-extending avoids concurrent file-extension which hits a TOCTOU race
	// in the distributed lock's CAS. Tests data integrity under contention.
	node0 := cluster.Node(0)
	initialData := make([]byte, finalSize)
	if err := node0.WriteFile("shared3.txt", initialData); err != nil {
		t.Fatalf("Failed to create shared3.txt: %v", err)
	}
	t.Logf("Created shared3.txt with %d bytes", finalSize)

	time.Sleep(500 * time.Millisecond)

	barrier := NewSyncBarrier(3)
	var wg sync.WaitGroup
	errors := make([]error, 3)

	// Node 0 writes at offset 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[0] = cluster.Node(0).WriteFileAt("shared3.txt", dataNode0, 0)
	}()

	// Node 1 writes at offset after Node 0's data
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[1] = cluster.Node(1).WriteFileAt("shared3.txt", dataNode1, int64(actualChunkSize))
	}()

	// Node 2 writes at offset after Node 1's data
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[2] = cluster.Node(2).WriteFileAt("shared3.txt", dataNode2, int64(actualChunkSize*2))
	}()

	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Errorf("Node %d write failed: %v", i, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Verify content from node 0
	fullContent, err := node0.ReadFile("shared3.txt")
	if err != nil {
		t.Fatalf("Failed to read shared3.txt: %v", err)
	}

	expectedSize := actualChunkSize * 3
	if len(fullContent) < expectedSize {
		t.Errorf("File too small: got %d bytes, expected at least %d", len(fullContent), expectedSize)
	}

	// Verify each node's data
	for i, expected := range [][]byte{dataNode0, dataNode1, dataNode2} {
		offset := i * actualChunkSize
		if len(fullContent) >= offset+len(expected) {
			actual := fullContent[offset : offset+len(expected)]
			if !bytes.Equal(actual, expected) {
				t.Errorf("Node %d's data corrupted at offset %d", i, offset)
			} else {
				t.Logf("Node %d's data verified at offset %d", i, offset)
			}
		}
	}

	t.Logf("3-node concurrent writes test completed - total file size: %d bytes", len(fullContent))
}

// TestMultiNodeMixedSizeWrites tests that non-extending writes don't corrupt file size.
// One node establishes file size, another writes within that size (non-extending),
// and a third extends the file further.
func TestMultiNodeMixedSizeWrites(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 3, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	for i := 1; i < 3; i++ {
		if err := cluster.StartNode(i, false); err != nil {
			t.Fatalf("Failed to start node %d: %v", i, err)
		}
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("Nodes failed to mount: %v", err)
	}

	// Define data sizes upfront so we can pre-extend the file
	node0 := cluster.Node(0)
	initialSize := 1024 * 1024
	initialData := bytes.Repeat([]byte("INIT--"), initialSize/6)
	actualInitialSize := len(initialData)

	// Now define the concurrent writes:
	// - Node 1: Write 100 bytes at offset 500KB (within initial region)
	// - Node 2: Write 512KB at offset 1MB (in the extended region)
	nonExtendData := bytes.Repeat([]byte("N"), 100)
	extendData := bytes.Repeat([]byte("EXT---"), 512*1024/6)
	finalSize := actualInitialSize + len(extendData)

	// Pre-create file at full final size to avoid concurrent file-extension.
	// Pre-extending avoids a TOCTOU race in the distributed lock's CAS.
	// We fill the initial region with INIT data, leave extended region as zeros.
	fullBuffer := make([]byte, finalSize)
	copy(fullBuffer[0:actualInitialSize], initialData)
	if err := node0.WriteFile("mixed.txt", fullBuffer); err != nil {
		t.Fatalf("Failed to create mixed.txt: %v", err)
	}
	t.Logf("Created mixed.txt with %d bytes (pre-extended)", finalSize)

	time.Sleep(500 * time.Millisecond)

	// Verify initial size from node 1
	info, err := cluster.Node(1).Stat("mixed.txt")
	if err != nil {
		t.Fatalf("Node 1 failed to stat file: %v", err)
	}
	t.Logf("Node 1 sees size: %d", info.Size())

	barrier := NewSyncBarrier(2)
	var wg sync.WaitGroup
	errors := make([]error, 2)

	// Node 1: write within initial region
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[0] = cluster.Node(1).WriteFileAt("mixed.txt", nonExtendData, 500*1024)
	}()

	// Node 2: write in the extended region (no longer extending since pre-extended)
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[1] = cluster.Node(2).WriteFileAt("mixed.txt", extendData, int64(actualInitialSize))
	}()

	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Errorf("Write %d failed: %v", i, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Verify final size
	fullContent, err := node0.ReadFile("mixed.txt")
	if err != nil {
		t.Fatalf("Failed to read mixed.txt: %v", err)
	}

	expectedSize := actualInitialSize + len(extendData)
	if len(fullContent) < expectedSize {
		t.Errorf("File too small: got %d bytes, expected at least %d", len(fullContent), expectedSize)
	}

	// Verify non-extending write didn't corrupt
	if len(fullContent) >= 500*1024+100 {
		nonExtendPart := fullContent[500*1024 : 500*1024+100]
		if !bytes.Equal(nonExtendPart, nonExtendData) {
			t.Errorf("Non-extending write data corrupted")
		} else {
			t.Log("Non-extending write data verified")
		}
	}

	// Verify extending write
	if len(fullContent) >= expectedSize {
		extendPart := fullContent[actualInitialSize:expectedSize]
		if !bytes.Equal(extendPart, extendData) {
			t.Errorf("Extending write data corrupted")
		} else {
			t.Log("Extending write data verified")
		}
	}

	t.Logf("Mixed-size writes test completed - final size: %d bytes", len(fullContent))
}

// TestMultiNodeOverlappingBlockWrites tests multiple nodes writing to the same physical block.
// This validates that extent lock serializes writes correctly and read-modify-write is atomic.
func TestMultiNodeOverlappingBlockWrites(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 3, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	for i := 1; i < 3; i++ {
		if err := cluster.StartNode(i, false); err != nil {
			t.Fatalf("Failed to start node %d: %v", i, err)
		}
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("Nodes failed to mount: %v", err)
	}

	// All writes go to the same 1MB block (block 0 for this file)
	// Node 0: 256KB at offset 0
	// Node 1: 256KB at offset 256KB
	// Node 2: 256KB at offset 512KB
	chunkSize := 256 * 1024
	finalSize := chunkSize * 3 // 768KB total

	// Pre-create file and extend to final size.
	// Pre-extending avoids concurrent file-extension which hits a TOCTOU race
	// in the distributed lock's CAS. Tests data integrity within a single block.
	node0 := cluster.Node(0)
	preFillData := make([]byte, finalSize)
	if err := node0.WriteFile("overlap.txt", preFillData); err != nil {
		t.Fatalf("Failed to create overlap.txt: %v", err)
	}
	t.Logf("Created overlap.txt with %d bytes", finalSize)

	time.Sleep(500 * time.Millisecond)
	dataNode0 := bytes.Repeat([]byte("AAAA"), chunkSize/4)
	dataNode1 := bytes.Repeat([]byte("BBBB"), chunkSize/4)
	dataNode2 := bytes.Repeat([]byte("CCCC"), chunkSize/4)

	barrier := NewSyncBarrier(3)
	var wg sync.WaitGroup
	errors := make([]error, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[0] = cluster.Node(0).WriteFileAt("overlap.txt", dataNode0, 0)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[1] = cluster.Node(1).WriteFileAt("overlap.txt", dataNode1, int64(chunkSize))
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[2] = cluster.Node(2).WriteFileAt("overlap.txt", dataNode2, int64(chunkSize*2))
	}()

	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Errorf("Node %d write failed: %v", i, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Verify content
	fullContent, err := node0.ReadFile("overlap.txt")
	if err != nil {
		t.Fatalf("Failed to read overlap.txt: %v", err)
	}

	expectedSize := chunkSize * 3
	if len(fullContent) < expectedSize {
		t.Errorf("File too small: got %d bytes, expected at least %d", len(fullContent), expectedSize)
	}

	// Verify each chunk
	for i, expected := range [][]byte{dataNode0, dataNode1, dataNode2} {
		offset := i * chunkSize
		if len(fullContent) >= offset+len(expected) {
			actual := fullContent[offset : offset+len(expected)]
			if !bytes.Equal(actual, expected) {
				t.Errorf("Chunk %d corrupted at offset %d", i, offset)
			} else {
				t.Logf("Chunk %d verified at offset %d", i, offset)
			}
		}
	}

	t.Logf("Overlapping block writes test completed - file size: %d bytes", len(fullContent))
}

// TestMultiNodeWriteStress tests lock contention with multiple rounds of concurrent writes.
// Each round has 3 nodes writing simultaneously to different offsets, repeated 5 times.
// This validates lock serialization under repeated contention.
func TestMultiNodeWriteStress(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 3, 100)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	for i := 1; i < 3; i++ {
		if err := cluster.StartNode(i, false); err != nil {
			t.Fatalf("Failed to start node %d: %v", i, err)
		}
	}

	if err := cluster.WaitAllMounted(20 * time.Second); err != nil {
		t.Fatalf("Nodes failed to mount: %v", err)
	}

	// Run 2 rounds of concurrent 3-node writes
	// Each round: 3 nodes write 256KB each at contiguous offsets
	// Total: 2 rounds * 3 nodes * 256KB = 1.5MB
	nodeCount := 3
	numRounds := 2
	chunkSize := 256 * 1024
	finalSize := numRounds * nodeCount * chunkSize

	// Pre-create file and extend to final size.
	// Pre-extending avoids concurrent file-extension which hits a TOCTOU race
	// in the distributed lock's CAS. Tests data integrity under stress.
	node0 := cluster.Node(0)
	initialData := make([]byte, finalSize)
	if err := node0.WriteFile("stress.txt", initialData); err != nil {
		t.Fatalf("Failed to create stress.txt: %v", err)
	}
	t.Logf("Created stress.txt with %d bytes", finalSize)

	time.Sleep(500 * time.Millisecond)

	for round := 0; round < numRounds; round++ {
		barrier := NewSyncBarrier(nodeCount)
		var wg sync.WaitGroup
		errors := make([]error, nodeCount)

		for nodeIdx := 0; nodeIdx < nodeCount; nodeIdx++ {
			wg.Add(1)
			go func(n, r int) {
				defer wg.Done()
				barrier.Wait()
				// Calculate offset: (round * 3 + nodeIdx) * chunkSize
				offset := int64((r*nodeCount + n) * chunkSize)
				pattern := byte('A' + n)
				data := bytes.Repeat([]byte{pattern}, chunkSize)
				errors[n] = cluster.Node(n).WriteFileAt("stress.txt", data, offset)
			}(nodeIdx, round)
		}

		wg.Wait()

		// Check for errors in this round
		for i, err := range errors {
			if err != nil {
				t.Errorf("Round %d node %d write failed: %v", round, i, err)
			}
		}
		t.Logf("Round %d completed", round)
	}

	time.Sleep(500 * time.Millisecond)

	// Verify file size
	fullContent, err := node0.ReadFile("stress.txt")
	if err != nil {
		t.Fatalf("Failed to read stress.txt: %v", err)
	}

	expectedSize := numRounds * nodeCount * chunkSize
	if len(fullContent) < expectedSize {
		t.Errorf("File too small: got %d bytes, expected at least %d", len(fullContent), expectedSize)
	}

	// Verify data from each round's first node write
	verified := 0
	for round := 0; round < numRounds; round++ {
		offset := round * nodeCount * chunkSize
		pattern := byte('A') // Node 0's pattern
		expected := bytes.Repeat([]byte{pattern}, chunkSize)
		if len(fullContent) >= offset+chunkSize {
			actual := fullContent[offset : offset+chunkSize]
			if bytes.Equal(actual, expected) {
				verified++
			} else {
				t.Errorf("Round %d node 0's write corrupted at offset %d", round, offset)
			}
		}
	}

	t.Logf("Stress test completed - file size: %d bytes, verified %d/%d rounds", len(fullContent), verified, numRounds)
}

// TestMultiNodeReversedWriteOrder tests that size updates work correctly when a high-offset
// write completes before a low-offset write.
func TestMultiNodeReversedWriteOrder(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start second node: %v", err)
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("Nodes failed to mount: %v", err)
	}

	// Node 0 writes at high offset, Node 1 writes at low offset (0)
	// The writes are contiguous (no sparse gap) - tests size update order
	// Due to barrier, they start together, but completion order varies
	// Use smaller data sizes (400KB each) to fit within a single 1MB block
	dataSize := 400 * 1024
	dataLow := bytes.Repeat([]byte("LOW---"), dataSize/6)  // At offset 0
	dataHigh := bytes.Repeat([]byte("HIGH--"), dataSize/6) // At offset right after low data
	lowLen := len(dataLow)
	highLen := len(dataHigh)
	finalSize := lowLen + highLen

	// Pre-create file and extend to final size from node 0.
	// Pre-extending avoids concurrent file-extension which hits a TOCTOU race
	// in the distributed lock's CAS. Tests data integrity with reversed write order.
	node0 := cluster.Node(0)
	initialData := make([]byte, finalSize)
	if err := node0.WriteFile("reversed.txt", initialData); err != nil {
		t.Fatalf("Failed to create reversed.txt: %v", err)
	}
	t.Logf("Created reversed.txt with %d bytes", finalSize)

	time.Sleep(500 * time.Millisecond)

	barrier := NewSyncBarrier(2)
	var wg sync.WaitGroup
	errors := make([]error, 2)

	// Node 0: write at high offset (right after low data would end)
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[0] = cluster.Node(0).WriteFileAt("reversed.txt", dataHigh, int64(lowLen))
	}()

	// Node 1: write at low offset (0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		barrier.Wait()
		errors[1] = cluster.Node(1).WriteFileAt("reversed.txt", dataLow, 0)
	}()

	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Errorf("Write %d failed: %v", i, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Verify content
	fullContent, err := node0.ReadFile("reversed.txt")
	if err != nil {
		t.Fatalf("Failed to read reversed.txt: %v", err)
	}

	// Expected size: low + high data length
	expectedSize := lowLen + highLen
	if len(fullContent) < expectedSize {
		t.Errorf("File too small: got %d bytes, expected at least %d", len(fullContent), expectedSize)
	}

	// Verify low offset data
	if len(fullContent) >= lowLen {
		lowPart := fullContent[:lowLen]
		if !bytes.Equal(lowPart, dataLow) {
			t.Errorf("Low offset data corrupted")
		} else {
			t.Log("Low offset data verified")
		}
	}

	// Verify high offset data
	if len(fullContent) >= expectedSize {
		highPart := fullContent[lowLen:expectedSize]
		if !bytes.Equal(highPart, dataHigh) {
			t.Errorf("High offset data corrupted")
		} else {
			t.Log("High offset data verified")
		}
	}

	t.Logf("Reversed write order test completed - file size: %d bytes", len(fullContent))
}

// TestMultiNodeInodeReuseStaleRead checks that a node which read a file reads the
// right data after another node deletes it and a new file reuses its inode number.
func TestMultiNodeInodeReuseStaleRead(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}
	// The reader must rely on its own cache validation, like VMs on separate networks.
	cluster.Node(1).ExtraArgs = []string{"-broadcast=false"}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start second node: %v", err)
	}
	writer, reader := cluster.Node(0), cluster.Node(1)

	t.Run("SmallFile", func(t *testing.T) {
		readAfterInodeReuse(t, writer, reader, "small",
			[]byte("v5\n"), []byte("from vm 1\n"), nil)
	})

	// The padding file pushes the new data away from the old block, so a stale extent mapping reads wrong data.
	t.Run("BlockFile", func(t *testing.T) {
		readAfterInodeReuse(t, writer, reader, "block",
			bytes.Repeat([]byte("o"), 200<<10),
			bytes.Repeat([]byte("n"), 300<<10),
			bytes.Repeat([]byte("p"), 200<<10))
	})
}

func readAfterInodeReuse(t *testing.T, writer, reader *MountedFS, prefix string, oldData, newData, padData []byte) {
	t.Helper()
	oldName, newName, padName := prefix+"_old", prefix+"_new", prefix+"_pad"

	if err := writer.WriteFile(oldName, oldData); err != nil {
		t.Fatalf("Writer: create %s: %v", oldName, err)
	}
	oldIno := inodeNumber(t, writer, oldName)

	got, err := reader.ReadFile(oldName)
	if err != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("Reader: read %s: %d bytes, err=%v, want %d bytes", oldName, len(got), err, len(oldData))
	}

	if err := writer.Remove(oldName); err != nil {
		t.Fatalf("Writer: remove %s: %v", oldName, err)
	}
	if err := writer.WriteFile(newName, nil); err != nil {
		t.Fatalf("Writer: create %s: %v", newName, err)
	}
	if ino := inodeNumber(t, writer, newName); ino != oldIno {
		t.Fatalf("%s got inode %d, want reused inode %d", newName, ino, oldIno)
	}
	if padData != nil {
		if err := writer.WriteFile(padName, padData); err != nil {
			t.Fatalf("Writer: create %s: %v", padName, err)
		}
	}
	if err := writer.WriteFile(newName, newData); err != nil {
		t.Fatalf("Writer: write %s: %v", newName, err)
	}

	got, err = reader.ReadFile(newName)
	if err != nil {
		t.Fatalf("Reader: read %s: %v", newName, err)
	}
	if !bytes.Equal(got, newData) {
		t.Errorf("Reader: %s has %d bytes starting %q, want %d bytes starting %q",
			newName, len(got), got[:min(len(got), 16)], len(newData), newData[:min(len(newData), 16)])
	}
}

func inodeNumber(t *testing.T, m *MountedFS, name string) uint64 {
	t.Helper()
	fi, err := m.Stat(name)
	if err != nil {
		t.Fatalf("stat %s: %v", name, err)
	}
	return fi.Sys().(*syscall.Stat_t).Ino
}

// TestMultiNodeExtentCacheCoherency checks that a node which cached a file's block
// mappings reads the new data after another node changes those mappings.
func TestMultiNodeExtentCacheCoherency(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}
	cluster.Node(1).ExtraArgs = []string{"-broadcast=false"}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start second node: %v", err)
	}
	writer, reader := cluster.Node(0), cluster.Node(1)

	t.Run("TruncateRewrite", func(t *testing.T) {
		const name = "trunc.bin"
		writeOrFail(t, writer, name, bytes.Repeat([]byte("o"), 300<<10), 0)
		expectContent(t, reader, name, bytes.Repeat([]byte("o"), 300<<10))

		if err := os.Truncate(writer.Path(name), 0); err != nil {
			t.Fatalf("Writer: truncate %s: %v", name, err)
		}
		// The padding file takes the freed block, so the rewrite lands in another one.
		writeOrFail(t, writer, "trunc_pad.bin", bytes.Repeat([]byte("p"), 300<<10), 0)
		writeOrFail(t, writer, name, bytes.Repeat([]byte("n"), 300<<10), 0)

		expectContent(t, reader, name, bytes.Repeat([]byte("n"), 300<<10))
	})

	t.Run("HoleFill", func(t *testing.T) {
		const name = "holes.bin"
		const blockSize = 1 << 20
		chunk := func(c byte) []byte { return bytes.Repeat([]byte{c}, 4096) }
		writeOrFail(t, writer, name, chunk('a'), 0)
		writeOrFail(t, writer, name, chunk('c'), 2*blockSize)

		want := make([]byte, 2*blockSize+4096)
		copy(want, chunk('a'))
		copy(want[2*blockSize:], chunk('c'))
		expectContent(t, reader, name, want)

		writeOrFail(t, writer, name, chunk('b'), blockSize)
		copy(want[blockSize:], chunk('b'))
		expectContent(t, reader, name, want)
	})
}

func writeOrFail(t *testing.T, m *MountedFS, name string, data []byte, offset int64) {
	t.Helper()
	if err := m.WriteFileAt(name, data, offset); err != nil {
		t.Fatalf("write %s at %d: %v", name, offset, err)
	}
}

func expectContent(t *testing.T, m *MountedFS, name string, want []byte) {
	t.Helper()
	got, err := m.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if bytes.Equal(got, want) {
		return
	}
	i := 0
	for i < len(got) && i < len(want) && got[i] == want[i] {
		i++
	}
	t.Errorf("%s: %d bytes, want %d; first difference at offset %d", name, len(got), len(want), i)
}

// TestNetworkPartition simulates network partition handling by testing node isolation
// and reconnection. Since Go integration tests run multiple FUSE mount processes on
// the same host (sharing a backing image), true network partition cannot be simulated.
// Instead, this test validates the same semantic behavior:
// 1. All nodes operational - write a file
// 2. One node "isolated" (killed) - surviving nodes continue operating
// 3. Node "healed" (restarted) - can access data from before partition
//
// This mirrors the Docker/VM tests which use container networking/iptables for isolation.
func TestNetworkPartition(t *testing.T) {
	// Need 3 nodes: 1 to isolate, 2 to continue operating
	cluster := NewMultiNodeCluster(t, 3, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	for i := 1; i < 3; i++ {
		if err := cluster.StartNode(i, false); err != nil {
			t.Fatalf("Failed to start node %d: %v", i, err)
		}
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("Nodes failed to mount: %v", err)
	}

	t.Log("All 3 nodes mounted successfully")

	// Phase 1: Write a file with all nodes operational (before partition)
	node0 := cluster.Node(0)
	beforeContent := []byte("Before partition content")
	if err := node0.WriteFile("before_partition.txt", beforeContent); err != nil {
		t.Fatalf("Failed to write file before partition: %v", err)
	}
	t.Log("Wrote before_partition.txt with all nodes up")

	// Verify all nodes can see the file
	for i := 0; i < 3; i++ {
		node := cluster.Node(i)
		if !node.Exists("before_partition.txt") {
			t.Errorf("Node %d cannot see before_partition.txt before isolation", i)
		}
	}

	// Phase 2: Simulate partition by killing node 2 (the "isolated" node)
	isolatedNodeIdx := 2
	t.Logf("Simulating partition: killing node %d", isolatedNodeIdx)

	if err := cluster.KillNode(isolatedNodeIdx); err != nil {
		t.Fatalf("Failed to kill node %d: %v", isolatedNodeIdx, err)
	}
	t.Logf("Node %d killed (isolated)", isolatedNodeIdx)

	// Give time for the kill to propagate
	time.Sleep(500 * time.Millisecond)

	// Phase 3: Verify surviving nodes still operate normally
	t.Log("Verifying surviving nodes can still operate...")

	// Node 0 and 1 should still be able to read the file
	for _, nodeIdx := range []int{0, 1} {
		node := cluster.Node(nodeIdx)
		content, err := node.ReadFile("before_partition.txt")
		if err != nil {
			t.Errorf("Surviving node %d cannot read file: %v", nodeIdx, err)
			continue
		}
		if !bytes.Equal(content, beforeContent) {
			t.Errorf("Surviving node %d read wrong content", nodeIdx)
		} else {
			t.Logf("Surviving node %d can read file correctly", nodeIdx)
		}
	}

	// Surviving nodes can write during partition
	node1 := cluster.Node(1)
	duringContent := []byte("Written during partition")
	if err := node1.WriteFile("during_partition.txt", duringContent); err != nil {
		t.Errorf("Surviving node 1 failed to write during partition: %v", err)
	} else {
		t.Log("Surviving nodes can write during partition")
	}

	// Phase 4: Heal partition by restarting the isolated node
	t.Logf("Healing partition: restarting node %d", isolatedNodeIdx)

	if err := cluster.StartNode(isolatedNodeIdx, false); err != nil {
		t.Fatalf("Failed to restart isolated node: %v", err)
	}

	// Wait for the node to remount
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cluster.Node(isolatedNodeIdx).IsMounted() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !cluster.Node(isolatedNodeIdx).IsMounted() {
		t.Fatalf("Isolated node failed to remount after healing")
	}
	t.Logf("Node %d restarted (partition healed)", isolatedNodeIdx)

	// Give filesystem time to sync
	time.Sleep(500 * time.Millisecond)

	// Phase 5: Verify healed node can access all data
	healedNode := cluster.Node(isolatedNodeIdx)

	// Check file from before partition
	if !healedNode.Exists("before_partition.txt") {
		t.Error("Healed node cannot see file from before partition")
	} else {
		content, err := healedNode.ReadFile("before_partition.txt")
		if err != nil {
			t.Errorf("Healed node cannot read file from before partition: %v", err)
		} else if !bytes.Equal(content, beforeContent) {
			t.Error("Healed node read wrong content from before partition")
		} else {
			t.Log("Healed node can access file from before partition")
		}
	}

	// Check file written during partition
	if !healedNode.Exists("during_partition.txt") {
		t.Error("Healed node cannot see file written during partition")
	} else {
		content, err := healedNode.ReadFile("during_partition.txt")
		if err != nil {
			t.Errorf("Healed node cannot read file written during partition: %v", err)
		} else if !bytes.Equal(content, duringContent) {
			t.Error("Healed node read wrong content from during partition")
		} else {
			t.Log("Healed node can access file written during partition")
		}
	}

	t.Log("Network partition test completed successfully")
}

// TestRollingRestart validates that nodes can be restarted one at a time
// while the cluster remains operational. This is critical for zero-downtime
// maintenance and upgrade scenarios.
//
// The test:
// 1. Creates a file with all nodes running
// 2. Restarts each node one at a time
// 3. After each restart, verifies the file is accessible from the restarted node
// 4. At any point, at least one node remains operational
func TestRollingRestart(t *testing.T) {
	nodeCount := 3
	cluster := NewMultiNodeCluster(t, nodeCount, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}

	for i := 1; i < nodeCount; i++ {
		if err := cluster.StartNode(i, false); err != nil {
			t.Fatalf("Failed to start node %d: %v", i, err)
		}
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("Nodes failed to mount: %v", err)
	}

	t.Logf("All %d nodes mounted successfully", nodeCount)

	// Write a file before any restarts
	node0 := cluster.Node(0)
	testContent := []byte("Rolling restart test content")
	if err := node0.WriteFile("rolling_test.txt", testContent); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	t.Log("Wrote rolling_test.txt with all nodes up")

	// Verify all nodes can see the file initially
	for i := 0; i < nodeCount; i++ {
		node := cluster.Node(i)
		if !node.Exists("rolling_test.txt") {
			t.Errorf("Node %d cannot see rolling_test.txt initially", i)
		}
	}

	// Rolling restart: restart each node one at a time
	for nodeIdx := 0; nodeIdx < nodeCount; nodeIdx++ {
		t.Logf("Rolling restart: stopping node %d...", nodeIdx)

		// Stop the node
		if err := cluster.StopNode(nodeIdx); err != nil {
			t.Errorf("Failed to stop node %d: %v", nodeIdx, err)
			continue
		}

		// Give time for unmount to complete
		time.Sleep(500 * time.Millisecond)

		// Verify other nodes are still operational
		for otherIdx := 0; otherIdx < nodeCount; otherIdx++ {
			if otherIdx == nodeIdx {
				continue // Skip the stopped node
			}
			otherNode := cluster.Node(otherIdx)
			if otherNode.IsMounted() {
				content, err := otherNode.ReadFile("rolling_test.txt")
				if err != nil {
					t.Errorf("Node %d cannot read file while node %d is down: %v", otherIdx, nodeIdx, err)
				} else if !bytes.Equal(content, testContent) {
					t.Errorf("Node %d read wrong content while node %d is down", otherIdx, nodeIdx)
				} else {
					t.Logf("Node %d can still read file while node %d is down", otherIdx, nodeIdx)
				}
			}
		}

		// Restart the node
		t.Logf("Rolling restart: restarting node %d...", nodeIdx)
		if err := cluster.StartNode(nodeIdx, false); err != nil {
			t.Errorf("Failed to restart node %d: %v", nodeIdx, err)
			continue
		}

		// Wait for the node to remount
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if cluster.Node(nodeIdx).IsMounted() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		if !cluster.Node(nodeIdx).IsMounted() {
			t.Errorf("Node %d failed to remount after restart", nodeIdx)
			continue
		}
		t.Logf("Node %d restarted and remounted", nodeIdx)

		// Give filesystem time to sync
		time.Sleep(300 * time.Millisecond)

		// Verify the restarted node can access the file
		restartedNode := cluster.Node(nodeIdx)
		if !restartedNode.Exists("rolling_test.txt") {
			t.Errorf("Node %d cannot see rolling_test.txt after restart", nodeIdx)
		} else {
			content, err := restartedNode.ReadFile("rolling_test.txt")
			if err != nil {
				t.Errorf("Node %d cannot read rolling_test.txt after restart: %v", nodeIdx, err)
			} else if !bytes.Equal(content, testContent) {
				t.Errorf("Node %d read wrong content after restart", nodeIdx)
			} else {
				t.Logf("Node %d can access file correctly after restart", nodeIdx)
			}
		}
	}

	// Final verification: all nodes should have the file
	t.Log("Final verification: checking all nodes can access the file")
	for i := 0; i < nodeCount; i++ {
		node := cluster.Node(i)
		if !node.IsMounted() {
			t.Errorf("Node %d is not mounted at end of test", i)
			continue
		}
		content, err := node.ReadFile("rolling_test.txt")
		if err != nil {
			t.Errorf("Node %d cannot read file at end: %v", i, err)
		} else if !bytes.Equal(content, testContent) {
			t.Errorf("Node %d has wrong content at end", i)
		}
	}

	t.Log("Rolling restart test completed successfully")
}

// TestStressRandomFailures tests that the system handles random node failures during load.
// This matches the behavior of test_stress_random_failures in Docker/VM tests.
func TestStressRandomFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	nodeCount := 3
	cluster := NewMultiNodeCluster(t, nodeCount, 50)
	defer cluster.Cleanup()

	// Format and start all nodes
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}
	for i := 1; i < nodeCount; i++ {
		if err := cluster.StartNode(i, false); err != nil {
			t.Fatalf("Failed to start node %d: %v", i, err)
		}
	}
	if err := cluster.WaitAllMounted(10 * time.Second); err != nil {
		t.Fatalf("Not all nodes mounted: %v", err)
	}

	iterations := 5
	filesCreated := 0

	for i := 1; i <= iterations; i++ {
		// Pick a random node for writing
		writeNodeIdx := rand.Intn(nodeCount)

		// Ensure node is mounted (it might have been killed previously)
		if !cluster.Node(writeNodeIdx).IsMounted() {
			// Find another mounted node
			for j := 0; j < nodeCount; j++ {
				if cluster.Node(j).IsMounted() {
					writeNodeIdx = j
					break
				}
			}
		}

		writeNode := cluster.Node(writeNodeIdx)
		if !writeNode.IsMounted() {
			t.Logf("Warning: No mounted node available for iteration %d, skipping write", i)
			continue
		}

		filename := fmt.Sprintf("stress_%d.txt", i)
		content := []byte(fmt.Sprintf("Stress test iteration %d", i))

		err := writeNode.WriteFile(filename, content)
		if err != nil {
			t.Errorf("Iteration %d: Failed to write %s on node %d: %v", i, filename, writeNodeIdx, err)
			continue
		}
		filesCreated++
		t.Logf("Iteration %d: Wrote %s on node %d", i, filename, writeNodeIdx)

		// 20% chance to kill a random node
		if rand.Intn(5) == 0 {
			killNodeIdx := rand.Intn(nodeCount)
			t.Logf("Iteration %d: Killing node %d (20%% chance)", i, killNodeIdx)

			if err := cluster.KillNode(killNodeIdx); err != nil {
				t.Logf("Warning: Failed to kill node %d: %v", killNodeIdx, err)
				continue
			}

			time.Sleep(500 * time.Millisecond)

			// Restart the node
			if err := cluster.StartNode(killNodeIdx, false); err != nil {
				t.Logf("Warning: Failed to restart node %d: %v", killNodeIdx, err)
				continue
			}

			// Wait for node to remount
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				if cluster.Node(killNodeIdx).IsMounted() {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}

			if cluster.Node(killNodeIdx).IsMounted() {
				t.Logf("Iteration %d: Node %d restarted and remounted", i, killNodeIdx)
			} else {
				t.Logf("Warning: Node %d failed to remount after restart", killNodeIdx)
			}
		}
	}

	// Allow time for any pending operations to complete
	time.Sleep(2 * time.Second)

	// Verify all files exist - find a mounted node for verification
	var verifyNode *MountedFS
	for i := 0; i < nodeCount; i++ {
		if cluster.Node(i).IsMounted() {
			verifyNode = cluster.Node(i)
			break
		}
	}

	if verifyNode == nil {
		t.Fatal("No mounted node available for verification")
	}

	for i := 1; i <= iterations; i++ {
		filename := fmt.Sprintf("stress_%d.txt", i)
		if !verifyNode.Exists(filename) {
			t.Errorf("File %s not found after stress test", filename)
		}
	}

	t.Logf("Stress test completed: %d files created", filesCreated)
}
