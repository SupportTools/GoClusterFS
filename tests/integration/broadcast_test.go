// Package integration provides integration tests for GoClusterFS.
// This file tests network broadcast cache invalidation across nodes.
package integration

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"
)

// TestBroadcastInvalidationSpeed verifies that network broadcast enables
// near-instant cache invalidation between nodes. When Node A modifies a file,
// Node B should see the change within ~100ms (via broadcast), not the 5s TTL.
//
// This test creates a 2-node cluster where both nodes mount the same image
// with broadcast enabled (default). It then measures propagation time:
// - Node A writes a file
// - Node B polls until it sees the change
// - Propagation should complete in <500ms (generous threshold)
func TestBroadcastInvalidationSpeed(t *testing.T) {
	// Create a 2-node cluster (both have broadcast enabled by default)
	cluster := NewMultiNodeCluster(t, 2, 50) // 2 nodes, 50MB image

	// Format and start first node
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("FormatAndStartFirst failed: %v", err)
	}

	// Start second node
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("StartNode(1) failed: %v", err)
	}

	// Wait for both nodes to be fully mounted
	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("WaitAllMounted failed: %v", err)
	}

	// Give broadcast a moment to discover peers via heartbeat
	time.Sleep(500 * time.Millisecond)

	node0 := cluster.Node(0)
	node1 := cluster.Node(1)

	// Create initial file on Node 0
	initialData := make([]byte, 4096)
	rand.Read(initialData)

	if err := node0.WriteFile("broadcast_test.txt", initialData); err != nil {
		t.Fatalf("Node0 WriteFile failed: %v", err)
	}

	// Verify Node 1 can read it (may take a moment due to initial discovery)
	var readData []byte
	var err error
	for i := 0; i < 50; i++ { // 50 x 100ms = 5s max
		readData, err = node1.ReadFile("broadcast_test.txt")
		if err == nil && bytes.Equal(initialData, readData) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Node1 initial read failed: %v", err)
	}
	if !bytes.Equal(initialData, readData) {
		t.Fatalf("Initial data mismatch")
	}

	// Now test broadcast speed: modify file on Node 0, measure how fast Node 1 sees it
	updatedData := make([]byte, 4096)
	rand.Read(updatedData)

	// Record start time
	start := time.Now()

	// Node 0 writes updated content
	if err := node0.WriteFile("broadcast_test.txt", updatedData); err != nil {
		t.Fatalf("Node0 update write failed: %v", err)
	}

	// Node 1 polls until it sees the change
	// With broadcast, this should be <100ms
	// With TTL only (5s), this would take up to 5s
	var propagationTime time.Duration
	maxWait := 2 * time.Second // Generous timeout for test stability

	for time.Since(start) < maxWait {
		readData, err = node1.ReadFile("broadcast_test.txt")
		if err == nil && bytes.Equal(updatedData, readData) {
			propagationTime = time.Since(start)
			break
		}
		time.Sleep(10 * time.Millisecond) // Fast polling
	}

	if !bytes.Equal(updatedData, readData) {
		t.Errorf("Node1 did not see update within %v (broadcast may not be working)", maxWait)
		return
	}

	// Log the propagation time
	t.Logf("Broadcast propagation time: %v", propagationTime)

	// Assert broadcast is working: propagation should be <500ms
	// (We use 500ms instead of 100ms to account for test environment variance)
	if propagationTime > 500*time.Millisecond {
		t.Errorf("Propagation too slow: %v (expected <500ms with broadcast)", propagationTime)
	}
}

// TestBroadcastDirectoryInvalidation verifies that directory changes
// (new file creation) are broadcast between nodes.
func TestBroadcastDirectoryInvalidation(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("FormatAndStartFirst failed: %v", err)
	}

	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("StartNode(1) failed: %v", err)
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("WaitAllMounted failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond) // Allow peer discovery

	node0 := cluster.Node(0)
	node1 := cluster.Node(1)

	// Node 0 creates a new file
	start := time.Now()
	testData := []byte("broadcast directory test")
	if err := node0.WriteFile("new_file.txt", testData); err != nil {
		t.Fatalf("Node0 WriteFile failed: %v", err)
	}

	// Node 1 should see the new file quickly
	maxWait := 2 * time.Second
	var found bool
	for time.Since(start) < maxWait {
		if node1.Exists("new_file.txt") {
			found = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	propagationTime := time.Since(start)
	t.Logf("Directory invalidation time: %v", propagationTime)

	if !found {
		t.Errorf("Node1 did not see new file within %v", maxWait)
		return
	}

	// Verify the content is correct
	readData, err := node1.ReadFile("new_file.txt")
	if err != nil {
		t.Fatalf("Node1 ReadFile failed: %v", err)
	}
	if !bytes.Equal(testData, readData) {
		t.Errorf("Content mismatch: got %q, want %q", readData, testData)
	}

	if propagationTime > 500*time.Millisecond {
		t.Errorf("Propagation too slow: %v (expected <500ms)", propagationTime)
	}
}

// TestBroadcastMultipleUpdates verifies that rapid successive updates
// are all broadcast correctly.
func TestBroadcastMultipleUpdates(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("FormatAndStartFirst failed: %v", err)
	}

	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("StartNode(1) failed: %v", err)
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("WaitAllMounted failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	node0 := cluster.Node(0)
	node1 := cluster.Node(1)

	// Create initial file
	if err := node0.WriteFile("rapid_updates.txt", []byte("v0")); err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Wait for initial propagation
	time.Sleep(200 * time.Millisecond)

	// Perform rapid updates on Node 0
	updateCount := 5
	for i := 1; i <= updateCount; i++ {
		content := []byte("v" + string(rune('0'+i)))
		if err := node0.WriteFile("rapid_updates.txt", content); err != nil {
			t.Fatalf("Update %d failed: %v", i, err)
		}
		time.Sleep(50 * time.Millisecond) // Small gap between updates
	}

	// Allow final broadcast to propagate
	time.Sleep(300 * time.Millisecond)

	// Node 1 should see the final version
	readData, err := node1.ReadFile("rapid_updates.txt")
	if err != nil {
		t.Fatalf("Node1 ReadFile failed: %v", err)
	}

	expectedFinal := []byte("v" + string(rune('0'+updateCount)))
	if !bytes.Equal(readData, expectedFinal) {
		t.Errorf("Final content mismatch: got %q, want %q", readData, expectedFinal)
	}

	t.Log("Multiple rapid updates correctly propagated via broadcast")
}

// TestBroadcastBidirectional verifies that invalidations work in both
// directions between nodes.
func TestBroadcastBidirectional(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("FormatAndStartFirst failed: %v", err)
	}

	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("StartNode(1) failed: %v", err)
	}

	if err := cluster.WaitAllMounted(15 * time.Second); err != nil {
		t.Fatalf("WaitAllMounted failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	node0 := cluster.Node(0)
	node1 := cluster.Node(1)

	// Node 0 creates file A
	if err := node0.WriteFile("file_a.txt", []byte("from node 0")); err != nil {
		t.Fatalf("Node0 create file_a failed: %v", err)
	}

	// Node 1 creates file B
	if err := node1.WriteFile("file_b.txt", []byte("from node 1")); err != nil {
		t.Fatalf("Node1 create file_b failed: %v", err)
	}

	// Allow broadcast propagation
	time.Sleep(300 * time.Millisecond)

	// Node 1 should see file A from Node 0
	dataA, err := node1.ReadFile("file_a.txt")
	if err != nil {
		t.Fatalf("Node1 ReadFile(file_a) failed: %v", err)
	}
	if string(dataA) != "from node 0" {
		t.Errorf("file_a content wrong: got %q", dataA)
	}

	// Node 0 should see file B from Node 1
	dataB, err := node0.ReadFile("file_b.txt")
	if err != nil {
		t.Fatalf("Node0 ReadFile(file_b) failed: %v", err)
	}
	if string(dataB) != "from node 1" {
		t.Errorf("file_b content wrong: got %q", dataB)
	}

	t.Log("Bidirectional broadcast invalidation working correctly")
}
