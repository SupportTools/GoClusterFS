// Package integration provides integration tests for GoClusterFS.
// This file contains tests for cross-node journal recovery functionality.
package integration

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

// TestMultiNodeJournalRecovery tests that when a node crashes, another node
// can detect the failure and replay the crashed node's journal to recover
// uncommitted transactions.
//
// Test scenario:
// 1. Node A: Format filesystem and create a file (generates journal entries)
// 2. Node A: Gets killed (simulating a crash)
// 3. Wait for heartbeat to become stale
// 4. Node B: Starts, detects failed node via heartbeat timeout
// 5. Node B: Recovery handler replays Node A's journal
// 6. Verify: File created by Node A is visible on Node B
//
// Note: This test takes ~25 seconds due to heartbeat timeout requirements.
// The HeartbeatTimeout is 10s, and failure detection uses 2x timeout (20s)
// for consensus before triggering recovery.
func TestMultiNodeJournalRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multi-node journal recovery test in short mode (requires ~25s)")
	}

	cluster := NewMultiNodeCluster(t, 2, 50) // 2 nodes, 50MB image

	// Phase 1: Node A formats filesystem and creates a file
	t.Log("Phase 1: Starting Node A and creating test file...")
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start Node A: %v", err)
	}

	nodeA := cluster.Node(0)
	testFileName := "recovery-test.txt"
	testContent := []byte("This file should survive node crash and be recovered by Node B")

	if err := nodeA.WriteFile(testFileName, testContent); err != nil {
		t.Fatalf("Node A: Failed to create test file: %v", err)
	}
	t.Logf("Node A (PID %d): Created %s with %d bytes", nodeA.PID(), testFileName, len(testContent))

	// Verify the file is readable on Node A
	readBack, err := nodeA.ReadFile(testFileName)
	if err != nil {
		t.Fatalf("Node A: Failed to read back test file: %v", err)
	}
	if !bytes.Equal(readBack, testContent) {
		t.Fatalf("Node A: Read-back content mismatch")
	}
	t.Log("Node A: File verified readable before crash")

	// Give time for data to be written to disk
	time.Sleep(1 * time.Second)

	// Phase 2: Kill Node A to simulate crash
	t.Log("Phase 2: Killing Node A to simulate crash...")
	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("Failed to kill Node A: %v", err)
	}
	t.Log("Node A: Killed (crash simulated)")

	// Phase 3: Wait for heartbeat to become stale
	// HeartbeatTimeout is 10s, failure detection uses 2x (20s) for consensus
	// Adding buffer for safety
	t.Log("Phase 3: Waiting for Node A's heartbeat to become stale (25s)...")
	time.Sleep(25 * time.Second)

	// Phase 4: Start Node B - it should detect the failed node and trigger recovery
	t.Log("Phase 4: Starting Node B...")
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start Node B: %v", err)
	}
	t.Log("Node B: Started, failure detection should be active")

	// Wait for Node B to complete any recovery operations
	// Give time for failure detection loop to run and recovery to complete
	t.Log("Phase 5: Waiting for failure detection and recovery (10s)...")
	time.Sleep(10 * time.Second)

	// Phase 6: Verify the file exists on Node B
	t.Log("Phase 6: Verifying file recovery on Node B...")
	nodeB := cluster.Node(1)

	// Check file exists
	if !nodeB.Exists(testFileName) {
		// List what files we can see for debugging
		entries, err := nodeB.ReadDir("")
		if err != nil {
			t.Errorf("Node B: Cannot read root directory: %v", err)
		} else {
			t.Logf("Node B: Root directory contains %d entries:", len(entries))
			for _, e := range entries {
				t.Logf("  - %s", e.Name())
			}
		}
		t.Fatalf("Node B: Test file %q does not exist after recovery", testFileName)
	}

	// Verify content
	recoveredContent, err := nodeB.ReadFile(testFileName)
	if err != nil {
		t.Fatalf("Node B: Failed to read recovered file: %v", err)
	}

	if !bytes.Equal(recoveredContent, testContent) {
		t.Errorf("Node B: Recovered content mismatch.\nExpected: %q\nGot: %q",
			string(testContent), string(recoveredContent))
	} else {
		t.Logf("Node B: Successfully recovered file %s with correct content (%d bytes)",
			testFileName, len(recoveredContent))
	}

	// Additional verification: Node B can write new files
	newFile := "post-recovery.txt"
	newContent := []byte("File created after recovery")
	if err := nodeB.WriteFile(newFile, newContent); err != nil {
		t.Errorf("Node B: Failed to write new file after recovery: %v", err)
	} else {
		t.Log("Node B: Successfully created new file after recovery")
	}

	t.Log("Cross-node journal recovery test PASSED")
}

// TestMultiNodeRecoveryWithMultipleFiles tests recovery when the crashed node
// had created multiple files before crashing.
func TestMultiNodeRecoveryWithMultipleFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multi-node recovery test in short mode (requires ~30s)")
	}

	cluster := NewMultiNodeCluster(t, 2, 50)

	// Node A creates multiple files
	t.Log("Phase 1: Node A creating multiple files...")
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start Node A: %v", err)
	}

	nodeA := cluster.Node(0)
	fileCount := 5
	files := make(map[string][]byte)

	for i := 0; i < fileCount; i++ {
		filename := fmt.Sprintf("multifile_%d.txt", i)
		content := []byte(fmt.Sprintf("Content of file %d - should survive crash", i))
		files[filename] = content

		if err := nodeA.WriteFile(filename, content); err != nil {
			t.Fatalf("Node A: Failed to create %s: %v", filename, err)
		}
		t.Logf("Node A: Created %s", filename)
	}

	// Let data sync
	time.Sleep(1 * time.Second)

	// Kill Node A
	t.Log("Phase 2: Killing Node A...")
	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("Failed to kill Node A: %v", err)
	}

	// Wait for heartbeat timeout
	t.Log("Phase 3: Waiting for heartbeat timeout (25s)...")
	time.Sleep(25 * time.Second)

	// Start Node B
	t.Log("Phase 4: Starting Node B...")
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start Node B: %v", err)
	}

	// Wait for recovery
	t.Log("Phase 5: Waiting for recovery (10s)...")
	time.Sleep(10 * time.Second)

	// Verify all files
	t.Log("Phase 6: Verifying all files on Node B...")
	nodeB := cluster.Node(1)
	recoveredCount := 0

	for filename, expectedContent := range files {
		if !nodeB.Exists(filename) {
			t.Errorf("Node B: File %q not found after recovery", filename)
			continue
		}

		content, err := nodeB.ReadFile(filename)
		if err != nil {
			t.Errorf("Node B: Failed to read %s: %v", filename, err)
			continue
		}

		if !bytes.Equal(content, expectedContent) {
			t.Errorf("Node B: Content mismatch for %s", filename)
			continue
		}

		recoveredCount++
		t.Logf("Node B: Verified %s", filename)
	}

	t.Logf("Phase 6: Recovered %d/%d files", recoveredCount, fileCount)

	if recoveredCount != fileCount {
		t.Errorf("Not all files were recovered: got %d, want %d", recoveredCount, fileCount)
	} else {
		t.Log("Multi-file recovery test PASSED")
	}
}

// TestMultiNodeRecoveryWithLargeFile tests recovery when the crashed node
// had created a large file (multiple blocks).
func TestMultiNodeRecoveryWithLargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file recovery test in short mode (requires ~30s)")
	}

	cluster := NewMultiNodeCluster(t, 2, 100) // Need more space for large file

	t.Log("Phase 1: Node A creating large file...")
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start Node A: %v", err)
	}

	nodeA := cluster.Node(0)
	filename := "large-recovery.txt"
	// Create 500KB file to span multiple blocks
	content := bytes.Repeat([]byte("RECOVERY-TEST-DATA-"), 25000)

	if err := nodeA.WriteFile(filename, content); err != nil {
		t.Fatalf("Node A: Failed to create large file: %v", err)
	}
	t.Logf("Node A: Created %s (%d KB)", filename, len(content)/1024)

	// Verify read-back
	readBack, err := nodeA.ReadFile(filename)
	if err != nil {
		t.Fatalf("Node A: Failed to read back large file: %v", err)
	}
	if !bytes.Equal(readBack, content) {
		t.Fatalf("Node A: Large file read-back mismatch")
	}

	// Let data sync
	time.Sleep(2 * time.Second)

	// Kill Node A
	t.Log("Phase 2: Killing Node A...")
	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("Failed to kill Node A: %v", err)
	}

	// Wait for heartbeat timeout
	t.Log("Phase 3: Waiting for heartbeat timeout (25s)...")
	time.Sleep(25 * time.Second)

	// Start Node B
	t.Log("Phase 4: Starting Node B...")
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start Node B: %v", err)
	}

	// Wait for recovery
	t.Log("Phase 5: Waiting for recovery (10s)...")
	time.Sleep(10 * time.Second)

	// Verify large file
	t.Log("Phase 6: Verifying large file on Node B...")
	nodeB := cluster.Node(1)

	if !nodeB.Exists(filename) {
		t.Fatalf("Node B: Large file not found after recovery")
	}

	recoveredContent, err := nodeB.ReadFile(filename)
	if err != nil {
		t.Fatalf("Node B: Failed to read recovered large file: %v", err)
	}

	if len(recoveredContent) != len(content) {
		t.Errorf("Node B: Large file size mismatch. Got %d bytes, expected %d bytes",
			len(recoveredContent), len(content))
	} else if !bytes.Equal(recoveredContent, content) {
		t.Errorf("Node B: Large file content mismatch")
	} else {
		t.Logf("Node B: Successfully recovered large file (%d KB)", len(recoveredContent)/1024)
	}

	t.Log("Large file recovery test PASSED")
}

// TestMultiNodeRecoveryIdempotency tests that recovery is idempotent -
// replaying the same journal multiple times should produce the same result.
func TestMultiNodeRecoveryIdempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping idempotency test in short mode (requires ~45s)")
	}

	cluster := NewMultiNodeCluster(t, 3, 50) // 3 nodes for this test

	// Node A creates files
	t.Log("Phase 1: Node A creating files...")
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start Node A: %v", err)
	}

	nodeA := cluster.Node(0)
	if err := nodeA.WriteFile("idempotent.txt", []byte("Idempotency test")); err != nil {
		t.Fatalf("Node A: Failed to create file: %v", err)
	}

	// Let data sync
	time.Sleep(1 * time.Second)

	// Kill Node A
	t.Log("Phase 2: Killing Node A...")
	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("Failed to kill Node A: %v", err)
	}

	// Wait for heartbeat timeout
	t.Log("Phase 3: Waiting for heartbeat timeout (25s)...")
	time.Sleep(25 * time.Second)

	// Start Node B - first recovery attempt
	t.Log("Phase 4: Starting Node B (first recovery)...")
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start Node B: %v", err)
	}

	// Wait for recovery
	t.Log("Phase 5: Waiting for first recovery (10s)...")
	time.Sleep(10 * time.Second)

	// Verify file exists on Node B
	nodeB := cluster.Node(1)
	if !nodeB.Exists("idempotent.txt") {
		t.Fatalf("Node B: File not found after first recovery")
	}
	t.Log("Node B: First recovery successful")

	// Stop Node B
	if err := cluster.StopNode(1); err != nil {
		t.Fatalf("Failed to stop Node B: %v", err)
	}

	// Wait briefly
	time.Sleep(2 * time.Second)

	// Start Node C - this could potentially try to recover again
	// The journal should have been cleared by Node B's recovery,
	// but if not, recovery should still be idempotent
	t.Log("Phase 6: Starting Node C (checking idempotency)...")
	if err := cluster.StartNode(2, false); err != nil {
		t.Fatalf("Failed to start Node C: %v", err)
	}

	// Wait for any potential recovery
	time.Sleep(5 * time.Second)

	// Verify file still exists and is correct
	t.Log("Phase 7: Verifying file on Node C...")
	nodeC := cluster.Node(2)

	if !nodeC.Exists("idempotent.txt") {
		t.Fatalf("Node C: File not found (idempotency violated)")
	}

	content, err := nodeC.ReadFile("idempotent.txt")
	if err != nil {
		t.Fatalf("Node C: Failed to read file: %v", err)
	}

	if string(content) != "Idempotency test" {
		t.Errorf("Node C: Content corrupted after multiple recovery attempts")
	} else {
		t.Log("Node C: File content verified after multiple nodes")
	}

	// Verify we can still create new files
	if err := nodeC.WriteFile("new-file.txt", []byte("Post-recovery file")); err != nil {
		t.Errorf("Node C: Cannot create new files: %v", err)
	}

	t.Log("Recovery idempotency test PASSED")
}

