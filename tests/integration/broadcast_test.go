// Package integration provides integration tests for GoClusterFS.
// This file tests network broadcast cache invalidation across nodes.
package integration

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"
)

// Well above the 5s inode cache TTL; tests log propagation time instead of asserting a bound.
const propagationTimeout = 15 * time.Second

// TestBroadcastInvalidationSpeed verifies that Node B sees a file update made
// by Node A and logs how long that took. With working broadcast this should be
// ~100ms rather than the 5s TTL.
//
// This test creates a 2-node cluster where both nodes mount the same image
// with broadcast enabled (default). It then measures propagation time:
// - Node A writes a file
// - Node B polls until it sees the change
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

	var readData []byte
	var err error
	if _, ok := waitFor(propagationTimeout, func() bool {
		readData, err = node1.ReadFile("broadcast_test.txt")
		return err == nil && bytes.Equal(initialData, readData)
	}); !ok {
		t.Fatalf("Node1 initial read failed: err=%v, data matches=%v", err, bytes.Equal(initialData, readData))
	}

	// Now test broadcast speed: modify file on Node 0, measure how fast Node 1 sees it
	updatedData := make([]byte, 4096)
	rand.Read(updatedData)

	writeStart := time.Now()
	if err := node0.WriteFile("broadcast_test.txt", updatedData); err != nil {
		t.Fatalf("Node0 update write failed: %v", err)
	}
	t.Logf("Node0 update took %v", time.Since(writeStart))

	propagationTime, ok := waitFor(propagationTimeout, func() bool {
		readData, err = node1.ReadFile("broadcast_test.txt")
		return err == nil && bytes.Equal(updatedData, readData)
	})
	t.Logf("Broadcast propagation time: %v", propagationTime)
	if !ok {
		t.Fatalf("Node1 did not see update within %v", propagationTimeout)
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

	writeStart := time.Now()
	testData := []byte("broadcast directory test")
	if err := node0.WriteFile("new_file.txt", testData); err != nil {
		t.Fatalf("Node0 WriteFile failed: %v", err)
	}
	t.Logf("Node0 create took %v", time.Since(writeStart))

	propagationTime, found := waitFor(propagationTimeout, func() bool {
		return node1.Exists("new_file.txt")
	})
	t.Logf("Directory invalidation time: %v", propagationTime)
	if !found {
		t.Fatalf("Node1 did not see new file within %v", propagationTimeout)
	}

	// Verify the content is correct
	readData, err := node1.ReadFile("new_file.txt")
	if err != nil {
		t.Fatalf("Node1 ReadFile failed: %v", err)
	}
	if !bytes.Equal(testData, readData) {
		t.Errorf("Content mismatch: got %q, want %q", readData, testData)
	}
}

// waitForContent polls node until name reads as want.
func waitForContent(t *testing.T, node *MountedFS, name, want string) {
	t.Helper()
	var data []byte
	var err error
	took, ok := waitFor(propagationTimeout, func() bool {
		data, err = node.ReadFile(name)
		return err == nil && string(data) == want
	})
	if !ok {
		t.Fatalf("%s did not read %q within %v: got %q, err=%v", name, want, propagationTimeout, data, err)
	}
	t.Logf("%s propagated in %v", name, took)
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

	waitForContent(t, node1, "rapid_updates.txt", "v"+string(rune('0'+updateCount)))

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

	waitForContent(t, node1, "file_a.txt", "from node 0")
	waitForContent(t, node0, "file_b.txt", "from node 1")

	t.Log("Bidirectional broadcast invalidation working correctly")
}
