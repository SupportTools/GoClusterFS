// Package integration provides integration tests for GoClusterFS.
// This file contains multi-node/multi-process test scenarios.
package integration

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestMultiNodeConcurrentFileCreation tests two nodes simultaneously creating different files.
// This validates that bitmap allocation and inode table updates don't corrupt when accessed
// concurrently by multiple processes.
func TestMultiNodeConcurrentFileCreation(t *testing.T) {
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

	// Pre-create an empty file from node 0
	node0 := cluster.Node(0)
	if err := node0.WriteFile("shared.txt", []byte{}); err != nil {
		t.Fatalf("Failed to create shared.txt: %v", err)
	}
	t.Log("Created shared.txt")

	// Give time for the file to be visible to both nodes
	time.Sleep(500 * time.Millisecond)

	// Define test data - each node writes a distinct pattern at a specific offset
	// Node 0 writes to the first part, Node 1 writes to the second part
	// Using 1MB+ sizes to ensure block allocation is required
	dataSize := 512 * 1024 // 512KB each
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
