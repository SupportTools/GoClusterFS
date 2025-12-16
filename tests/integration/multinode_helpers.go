// Package integration provides integration tests for GoClusterFS.
// This file contains helpers for multi-node/multi-process testing scenarios.
package integration

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// MultiNodeCluster manages multiple GoClusterFS mount instances sharing a single
// backing image file. This simulates a multi-node cluster on a single host for
// testing concurrent access, failure scenarios, and recovery.
type MultiNodeCluster struct {
	ImagePath string       // Shared backing image file
	Nodes     []*MountedFS // Individual mount instances (one per "node")
	t         *testing.T
	sizeMB    int
	mu        sync.Mutex // Protects Nodes slice
}

// NewMultiNodeCluster creates a new test cluster with the specified number of nodes.
// All nodes share the same backing image file.
// The cluster is automatically cleaned up when the test completes.
func NewMultiNodeCluster(t *testing.T, nodeCount, sizeMB int) *MultiNodeCluster {
	t.Helper()

	if nodeCount < 1 {
		t.Fatalf("nodeCount must be at least 1, got %d", nodeCount)
	}
	if sizeMB < 10 {
		t.Fatalf("sizeMB must be at least 10, got %d", sizeMB)
	}

	// Create shared image file
	imgFile, err := os.CreateTemp("", "goclusterfs_multinode_*.img")
	if err != nil {
		t.Fatalf("Failed to create temp image: %v", err)
	}
	imagePath := imgFile.Name()
	imgFile.Close()

	// Allocate the image file
	if err := allocateImage(imagePath, sizeMB); err != nil {
		os.Remove(imagePath)
		t.Fatalf("Failed to allocate image: %v", err)
	}
	t.Logf("Created shared %dMB image at %s for %d nodes", sizeMB, imagePath, nodeCount)

	cluster := &MultiNodeCluster{
		ImagePath: imagePath,
		Nodes:     make([]*MountedFS, nodeCount),
		t:         t,
		sizeMB:    sizeMB,
	}

	// Create mount instances (but don't mount yet)
	for i := 0; i < nodeCount; i++ {
		cluster.Nodes[i] = NewMountedFSWithImage(t, imagePath)
	}

	// Register cleanup
	t.Cleanup(func() {
		cluster.Cleanup()
	})

	return cluster
}

// FormatAndStartFirst formats the filesystem using the first node and leaves it mounted.
// This must be called before starting other nodes.
func (c *MultiNodeCluster) FormatAndStartFirst(debug bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.Nodes) == 0 {
		return fmt.Errorf("no nodes in cluster")
	}

	return c.Nodes[0].FormatAndMount(debug)
}

// StartNode starts a specific node (mounts the shared filesystem).
// The first node should be started with FormatAndStartFirst to format the filesystem.
func (c *MultiNodeCluster) StartNode(idx int, debug bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if idx < 0 || idx >= len(c.Nodes) {
		return fmt.Errorf("node index %d out of range [0, %d)", idx, len(c.Nodes))
	}

	return c.Nodes[idx].Mount(debug)
}

// StopNode gracefully unmounts a specific node.
func (c *MultiNodeCluster) StopNode(idx int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if idx < 0 || idx >= len(c.Nodes) {
		return fmt.Errorf("node index %d out of range [0, %d)", idx, len(c.Nodes))
	}

	return c.Nodes[idx].Unmount()
}

// KillNode sends SIGKILL to a node's mount process to simulate a crash.
// This is harsher than StopNode and tests recovery from unclean shutdown.
func (c *MultiNodeCluster) KillNode(idx int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if idx < 0 || idx >= len(c.Nodes) {
		return fmt.Errorf("node index %d out of range [0, %d)", idx, len(c.Nodes))
	}

	if err := c.Nodes[idx].Kill(); err != nil {
		return err
	}

	// Wait briefly for process to die and unmount to propagate
	time.Sleep(500 * time.Millisecond)

	// Force unmount to clean up mount point
	c.Nodes[idx].Unmount()

	return nil
}

// Node returns the MountedFS for a specific node index.
func (c *MultiNodeCluster) Node(idx int) *MountedFS {
	c.mu.Lock()
	defer c.mu.Unlock()

	if idx < 0 || idx >= len(c.Nodes) {
		c.t.Fatalf("node index %d out of range [0, %d)", idx, len(c.Nodes))
	}
	return c.Nodes[idx]
}

// NodeCount returns the number of nodes in the cluster.
func (c *MultiNodeCluster) NodeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.Nodes)
}

// WaitAllMounted waits until all nodes report they are mounted.
func (c *MultiNodeCluster) WaitAllMounted(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		allMounted := true
		c.mu.Lock()
		for i, node := range c.Nodes {
			if !node.IsMounted() {
				c.t.Logf("Node %d not yet mounted", i)
				allMounted = false
				break
			}
		}
		c.mu.Unlock()

		if allMounted {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for all nodes to mount after %v", timeout)
}

// StartAllNodes starts all nodes (except first which should use FormatAndStartFirst).
func (c *MultiNodeCluster) StartAllNodes(debug bool) error {
	for i := 1; i < len(c.Nodes); i++ {
		if err := c.StartNode(i, debug); err != nil {
			return fmt.Errorf("failed to start node %d: %v", i, err)
		}
	}
	return nil
}

// StopAllNodes gracefully stops all nodes.
func (c *MultiNodeCluster) StopAllNodes() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, node := range c.Nodes {
		if node.IsMounted() {
			if err := node.Unmount(); err != nil {
				c.t.Logf("Warning: failed to unmount node %d: %v", i, err)
			}
		}
	}
}

// Cleanup stops all nodes and removes the shared image file.
func (c *MultiNodeCluster) Cleanup() {
	c.StopAllNodes()

	// Remove shared image file
	if c.ImagePath != "" {
		if err := os.Remove(c.ImagePath); err != nil {
			c.t.Logf("Warning: failed to remove image %s: %v", c.ImagePath, err)
		}
	}
}

// RunConcurrent runs a function on multiple nodes concurrently and waits for all to complete.
// The function receives the node index and the MountedFS instance.
// Returns a slice of errors (nil entries indicate success).
func (c *MultiNodeCluster) RunConcurrent(nodeIndices []int, fn func(idx int, node *MountedFS) error) []error {
	var wg sync.WaitGroup
	errors := make([]error, len(nodeIndices))

	for i, nodeIdx := range nodeIndices {
		wg.Add(1)
		go func(i, nodeIdx int) {
			defer wg.Done()
			node := c.Node(nodeIdx)
			errors[i] = fn(nodeIdx, node)
		}(i, nodeIdx)
	}

	wg.Wait()
	return errors
}

// SyncBarrier provides a synchronization point for concurrent operations.
// All goroutines calling Wait() will block until the expected count is reached.
type SyncBarrier struct {
	count    int
	expected int
	mu       sync.Mutex
	cond     *sync.Cond
}

// NewSyncBarrier creates a new barrier that releases when 'expected' goroutines are waiting.
func NewSyncBarrier(expected int) *SyncBarrier {
	b := &SyncBarrier{expected: expected}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Wait blocks until the expected number of goroutines have called Wait.
func (b *SyncBarrier) Wait() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.count++
	if b.count >= b.expected {
		// Last one in - release everyone
		b.cond.Broadcast()
	} else {
		// Wait for others
		b.cond.Wait()
	}
}

// Reset resets the barrier for reuse.
func (b *SyncBarrier) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.count = 0
}
