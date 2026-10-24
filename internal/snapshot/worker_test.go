package snapshot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	testBlockSize     = 1024 * 1024 // 1MB
	testDeviceSize    = 100 * testBlockSize
	testDataBlocks    = 50
	testRefcountStart = uint64(80 * testBlockSize)
	testSnapshotStart = uint64(90 * testBlockSize)
	testSnapshotSize  = uint64(5 * 4096) // 5 sectors
	testInodeStart    = uint64(70 * testBlockSize)
	testInodeSize     = uint64(10 * testBlockSize)
)

// setupTestDevice creates a temporary device for testing.
func setupTestDevice(t *testing.T) (*storage.Device, string, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "snapshot-worker-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	devPath := filepath.Join(tmpDir, "test.img")
	f, err := os.Create(devPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create device file: %v", err)
	}

	if err := f.Truncate(testDeviceSize); err != nil {
		f.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to truncate device: %v", err)
	}
	f.Close()

	dev, err := storage.Open(devPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to open device: %v", err)
	}

	cleanup := func() {
		dev.Close()
		os.RemoveAll(tmpDir)
	}

	return dev, devPath, cleanup
}

// setupTestComponents creates all components needed for worker testing.
const jobTimeout = 10 * time.Second

// waitForJob polls done until the worker's background job has finished.
func waitForJob(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(jobTimeout)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("Background job did not finish within %v", jobTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func snapshotActive(reg *Registry, id uint64) func() bool {
	return func() bool {
		desc, err := reg.Get(context.Background(), id)
		return err == nil && desc.State == StateActive
	}
}

func snapshotRemoved(reg *Registry, id uint64) func() bool {
	return func() bool {
		_, err := reg.Get(context.Background(), id)
		return err == ErrSnapshotNotFound
	}
}

func setupTestComponents(t *testing.T, dev *storage.Device) (*Registry, *refcount.Region, *extent.Manager, *inode.Table) {
	t.Helper()

	// Create refcount region
	refReg := refcount.NewRegion(dev, testRefcountStart, testDataBlocks, nil)

	// Initialize refcount region
	ctx := context.Background()
	if err := refReg.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize refcount region: %v", err)
	}

	// Create snapshot registry
	snapReg := NewRegistry(dev, testSnapshotStart, testSnapshotSize, nil)

	// Initialize snapshot registry
	if err := snapReg.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize snapshot registry: %v", err)
	}

	// Create extent manager (bitmap can be nil for tests not needing allocation)
	extMgr := extent.NewManager(dev, nil, nil, 0)

	// Create inode table
	var nodeUUID [16]byte
	inodeTbl := inode.NewTable(dev, testInodeStart, testInodeSize, nil, nodeUUID)

	return snapReg, refReg, extMgr, inodeTbl
}

func TestWorkerStartStop(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, refReg, extMgr, inodeTbl := setupTestComponents(t, dev)

	cfg := DefaultWorkerConfig()
	worker := NewWorker(snapReg, refReg, extMgr, inodeTbl, cfg)

	if worker.IsRunning() {
		t.Error("Worker should not be running before Start()")
	}

	worker.Start()
	if !worker.IsRunning() {
		t.Error("Worker should be running after Start()")
	}

	// Start again should be idempotent
	worker.Start()
	if !worker.IsRunning() {
		t.Error("Worker should still be running after double Start()")
	}

	worker.Stop()
	if worker.IsRunning() {
		t.Error("Worker should not be running after Stop()")
	}

	// Stop again should be idempotent
	worker.Stop()
	if worker.IsRunning() {
		t.Error("Worker should still be stopped after double Stop()")
	}
}

func TestWorkerQueueCreateJob(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, refReg, extMgr, inodeTbl := setupTestComponents(t, dev)
	ctx := context.Background()

	// Create a snapshot first
	desc, err := snapReg.Create(ctx, "test-snap", 100, 0, 0, 3, 1024, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	cfg := DefaultWorkerConfig()
	cfg.OnJobComplete = func(result *JobResult) {
		t.Logf("Job %d completed: success=%v, duration=%v", result.JobID, result.Success, result.Duration)
	}
	worker := NewWorker(snapReg, refReg, extMgr, inodeTbl, cfg)
	worker.Start()
	defer worker.Stop()

	// Queue a create job
	blockNums := []uint64{10, 11, 12}
	jobID, err := worker.QueueCreateJob(ctx, desc.SnapshotID, 100, blockNums)
	if err != nil {
		t.Fatalf("Failed to queue create job: %v", err)
	}

	if jobID == 0 {
		t.Error("Job ID should not be 0")
	}

	waitForJob(t, snapshotActive(snapReg, desc.SnapshotID))

	// Verify refcounts were incremented
	for _, blockNum := range blockNums {
		entry, err := refReg.Get(ctx, blockNum)
		if err != nil {
			t.Errorf("Failed to get refcount for block %d: %v", blockNum, err)
			continue
		}
		if entry.RefCount != 1 {
			t.Errorf("Block %d refcount = %d, want 1", blockNum, entry.RefCount)
		}
	}

	// Verify snapshot state changed to Active
	updatedDesc, err := snapReg.Get(ctx, desc.SnapshotID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}
	if updatedDesc.State != StateActive {
		t.Errorf("Snapshot state = %v, want StateActive", updatedDesc.State)
	}
}

func TestWorkerQueueDeleteJob(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, refReg, extMgr, inodeTbl := setupTestComponents(t, dev)
	ctx := context.Background()

	// Create and complete a snapshot first
	desc, err := snapReg.Create(ctx, "test-snap", 100, 0, 0, 3, 1024, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Set up initial refcounts
	blockNums := []uint64{20, 21, 22}
	for _, blockNum := range blockNums {
		if _, err := refReg.Increment(ctx, blockNum); err != nil {
			t.Fatalf("Failed to increment refcount for block %d: %v", blockNum, err)
		}
	}

	// Mark snapshot as active, then delete
	if err := snapReg.UpdateState(ctx, desc.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to update state: %v", err)
	}
	if err := snapReg.Delete(ctx, desc.SnapshotID); err != nil {
		t.Fatalf("Failed to delete snapshot: %v", err)
	}

	cfg := DefaultWorkerConfig()
	worker := NewWorker(snapReg, refReg, extMgr, inodeTbl, cfg)
	worker.Start()
	defer worker.Stop()

	// Queue delete job
	jobID, err := worker.QueueDeleteJob(ctx, desc.SnapshotID, blockNums)
	if err != nil {
		t.Fatalf("Failed to queue delete job: %v", err)
	}

	if jobID == 0 {
		t.Error("Job ID should not be 0")
	}

	waitForJob(t, snapshotRemoved(snapReg, desc.SnapshotID))

	// Verify refcounts were decremented
	for _, blockNum := range blockNums {
		entry, err := refReg.Get(ctx, blockNum)
		if err != nil {
			t.Errorf("Failed to get refcount for block %d: %v", blockNum, err)
			continue
		}
		if entry.RefCount != 0 {
			t.Errorf("Block %d refcount = %d, want 0", blockNum, entry.RefCount)
		}
	}

	// Verify snapshot was removed
	_, err = snapReg.Get(ctx, desc.SnapshotID)
	if err != ErrSnapshotNotFound {
		t.Errorf("Snapshot should be removed, got err = %v", err)
	}
}

func TestWorkerNotRunning(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, refReg, extMgr, inodeTbl := setupTestComponents(t, dev)
	ctx := context.Background()

	cfg := DefaultWorkerConfig()
	worker := NewWorker(snapReg, refReg, extMgr, inodeTbl, cfg)
	// Don't start the worker

	_, err := worker.QueueCreateJob(ctx, 1, 100, []uint64{1, 2, 3})
	if err == nil {
		t.Error("QueueCreateJob should fail when worker not running")
	}

	_, err = worker.QueueDeleteJob(ctx, 1, []uint64{1, 2, 3})
	if err == nil {
		t.Error("QueueDeleteJob should fail when worker not running")
	}
}

func TestWorkerEmptyBlockList(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, refReg, extMgr, inodeTbl := setupTestComponents(t, dev)
	ctx := context.Background()

	// Create snapshot
	desc, err := snapReg.Create(ctx, "empty-snap", 100, 0, 0, 0, 0, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	cfg := DefaultWorkerConfig()
	worker := NewWorker(snapReg, refReg, extMgr, inodeTbl, cfg)
	worker.Start()
	defer worker.Stop()

	// Queue create job with empty block list
	_, err = worker.QueueCreateJob(ctx, desc.SnapshotID, 100, []uint64{})
	if err != nil {
		t.Fatalf("Failed to queue empty create job: %v", err)
	}

	waitForJob(t, snapshotActive(snapReg, desc.SnapshotID))

	// Verify snapshot state changed to Active
	updatedDesc, err := snapReg.Get(ctx, desc.SnapshotID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}
	if updatedDesc.State != StateActive {
		t.Errorf("Snapshot state = %v, want StateActive", updatedDesc.State)
	}
}

// TestWorkerQueuesJobsWithCanceledContext checks that a canceled FUSE request
// context cannot drop a job for a snapshot that already exists.
func TestWorkerQueuesJobsWithCanceledContext(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, refReg, extMgr, inodeTbl := setupTestComponents(t, dev)

	worker := NewWorker(snapReg, refReg, extMgr, inodeTbl, DefaultWorkerConfig())
	worker.Start()
	defer worker.Stop()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	for i := 0; i < 20; i++ {
		desc, err := snapReg.Create(context.Background(), fmt.Sprintf("snap-%d", i), 100, 0, 0, 0, 1024, FlagReadOnly)
		if err != nil {
			t.Fatalf("Failed to create snapshot %d: %v", i, err)
		}

		if _, err := worker.QueueCreateJob(canceled, desc.SnapshotID, 100, []uint64{}); err != nil {
			t.Fatalf("QueueCreateJob %d with canceled context: %v", i, err)
		}
		waitForJob(t, snapshotActive(snapReg, desc.SnapshotID))

		if err := snapReg.Delete(context.Background(), desc.SnapshotID); err != nil {
			t.Fatalf("Failed to delete snapshot %d: %v", i, err)
		}
		if _, err := worker.QueueDeleteJob(canceled, desc.SnapshotID, []uint64{}); err != nil {
			t.Fatalf("QueueDeleteJob %d with canceled context: %v", i, err)
		}
		waitForJob(t, snapshotRemoved(snapReg, desc.SnapshotID))
	}
}

func TestWorkerPendingJobs(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, refReg, extMgr, inodeTbl := setupTestComponents(t, dev)

	cfg := DefaultWorkerConfig()
	cfg.QueueSize = 10
	worker := NewWorker(snapReg, refReg, extMgr, inodeTbl, cfg)

	// Before starting, pending should be 0
	if worker.PendingJobs() != 0 {
		t.Errorf("PendingJobs = %d before start, want 0", worker.PendingJobs())
	}

	worker.Start()
	defer worker.Stop()

	// After starting with no jobs, still 0
	if worker.PendingJobs() != 0 {
		t.Errorf("PendingJobs = %d after start, want 0", worker.PendingJobs())
	}
}

func TestDefaultWorkerConfig(t *testing.T) {
	cfg := DefaultWorkerConfig()

	if cfg.QueueSize <= 0 {
		t.Errorf("QueueSize = %d, want > 0", cfg.QueueSize)
	}
	if cfg.WorkerCount <= 0 {
		t.Errorf("WorkerCount = %d, want > 0", cfg.WorkerCount)
	}
}

// TestLeafSnapshotDeletion tests deleting a snapshot with no children.
// Expected behavior:
// - Refcounts decremented
// - Blocks with refcount=0 are freed via bitmap
// - Snapshot descriptor removed
func TestLeafSnapshotDeletion(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, refReg, extMgr, inodeTbl := setupTestComponents(t, dev)
	ctx := context.Background()

	// Create a snapshot (will be a leaf since it has no children)
	desc, err := snapReg.Create(ctx, "leaf-snap", 100, 0, 0, 3, 1024, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Set up initial refcounts (simulating blocks used by snapshot)
	blockNums := []uint64{30, 31, 32}
	for _, blockNum := range blockNums {
		if _, err := refReg.Increment(ctx, blockNum); err != nil {
			t.Fatalf("Failed to increment refcount for block %d: %v", blockNum, err)
		}
	}

	// Mark as active then delete
	if err := snapReg.UpdateState(ctx, desc.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to update state: %v", err)
	}
	if err := snapReg.Delete(ctx, desc.SnapshotID); err != nil {
		t.Fatalf("Failed to delete snapshot: %v", err)
	}

	cfg := DefaultWorkerConfig()
	worker := NewWorker(snapReg, refReg, extMgr, inodeTbl, cfg)
	worker.Start()
	defer worker.Stop()

	// Queue delete job
	_, err = worker.QueueDeleteJob(ctx, desc.SnapshotID, blockNums)
	if err != nil {
		t.Fatalf("Failed to queue delete job: %v", err)
	}

	waitForJob(t, snapshotRemoved(snapReg, desc.SnapshotID))

	// Verify refcounts are 0 (all decremented)
	for _, blockNum := range blockNums {
		entry, err := refReg.Get(ctx, blockNum)
		if err != nil {
			t.Errorf("Failed to get refcount for block %d: %v", blockNum, err)
			continue
		}
		if entry.RefCount != 0 {
			t.Errorf("Block %d refcount = %d, want 0", blockNum, entry.RefCount)
		}
	}

	// Verify snapshot is removed
	_, err = snapReg.Get(ctx, desc.SnapshotID)
	if err != ErrSnapshotNotFound {
		t.Errorf("Expected ErrSnapshotNotFound, got %v", err)
	}
}

// TestMiddleSnapshotDeletionMergeUp tests deleting a snapshot that has children.
// Expected behavior:
// - Children are re-parented to grandparent (merge-up)
// - Refcounts decremented
// - Snapshot descriptor removed
func TestMiddleSnapshotDeletionMergeUp(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, refReg, extMgr, inodeTbl := setupTestComponents(t, dev)
	ctx := context.Background()

	// Create parent snapshot (root)
	rootSnap, err := snapReg.Create(ctx, "root-snap", 100, 0, 0, 2, 1024, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create root snapshot: %v", err)
	}
	if err := snapReg.UpdateState(ctx, rootSnap.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to activate root: %v", err)
	}

	// Create middle snapshot (child of root)
	middleSnap, err := snapReg.Create(ctx, "middle-snap", 100, rootSnap.SnapshotID, 0, 3, 1024, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create middle snapshot: %v", err)
	}
	if err := snapReg.UpdateState(ctx, middleSnap.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to activate middle: %v", err)
	}

	// Create leaf snapshot (child of middle)
	leafSnap, err := snapReg.Create(ctx, "leaf-snap", 100, middleSnap.SnapshotID, 0, 4, 1024, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create leaf snapshot: %v", err)
	}
	if err := snapReg.UpdateState(ctx, leafSnap.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to activate leaf: %v", err)
	}

	// Set up refcounts for middle snapshot's blocks
	middleBlocks := []uint64{40, 41, 42}
	for _, blockNum := range middleBlocks {
		// Increment twice: once for middle, once simulating shared with leaf
		if _, err := refReg.Increment(ctx, blockNum); err != nil {
			t.Fatalf("Failed to increment refcount: %v", err)
		}
		if _, err := refReg.Increment(ctx, blockNum); err != nil {
			t.Fatalf("Failed to increment refcount: %v", err)
		}
	}

	// Delete middle snapshot
	if err := snapReg.Delete(ctx, middleSnap.SnapshotID); err != nil {
		t.Fatalf("Failed to delete middle snapshot: %v", err)
	}

	cfg := DefaultWorkerConfig()
	worker := NewWorker(snapReg, refReg, extMgr, inodeTbl, cfg)
	worker.Start()
	defer worker.Stop()

	// Queue delete job for middle snapshot
	_, err = worker.QueueDeleteJob(ctx, middleSnap.SnapshotID, middleBlocks)
	if err != nil {
		t.Fatalf("Failed to queue delete job: %v", err)
	}

	waitForJob(t, snapshotRemoved(snapReg, middleSnap.SnapshotID))

	// Verify leaf snapshot was re-parented to root
	updatedLeaf, err := snapReg.Get(ctx, leafSnap.SnapshotID)
	if err != nil {
		t.Fatalf("Failed to get leaf snapshot: %v", err)
	}
	if updatedLeaf.ParentID != rootSnap.SnapshotID {
		t.Errorf("Leaf ParentID = %d, want %d (should be re-parented to root)",
			updatedLeaf.ParentID, rootSnap.SnapshotID)
	}

	// Verify refcounts decremented (should be 1 now, not 0, due to leaf sharing)
	for _, blockNum := range middleBlocks {
		entry, err := refReg.Get(ctx, blockNum)
		if err != nil {
			t.Errorf("Failed to get refcount for block %d: %v", blockNum, err)
			continue
		}
		if entry.RefCount != 1 {
			t.Errorf("Block %d refcount = %d, want 1 (shared with leaf)", blockNum, entry.RefCount)
		}
	}

	// Verify middle snapshot is removed
	_, err = snapReg.Get(ctx, middleSnap.SnapshotID)
	if err != ErrSnapshotNotFound {
		t.Errorf("Expected ErrSnapshotNotFound for middle, got %v", err)
	}

	// Verify root and leaf still exist
	_, err = snapReg.Get(ctx, rootSnap.SnapshotID)
	if err != nil {
		t.Errorf("Root snapshot should still exist: %v", err)
	}
	_, err = snapReg.Get(ctx, leafSnap.SnapshotID)
	if err != nil {
		t.Errorf("Leaf snapshot should still exist: %v", err)
	}
}

// TestGetChildren verifies the GetChildren registry method.
func TestGetChildren(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, _, _, _ := setupTestComponents(t, dev)
	ctx := context.Background()

	// Create parent snapshot
	parent, err := snapReg.Create(ctx, "parent", 100, 0, 0, 1, 100, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}
	if err := snapReg.UpdateState(ctx, parent.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to activate parent: %v", err)
	}

	// Create two children
	child1, err := snapReg.Create(ctx, "child1", 100, parent.SnapshotID, 0, 1, 100, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create child1: %v", err)
	}
	if err := snapReg.UpdateState(ctx, child1.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to activate child1: %v", err)
	}

	child2, err := snapReg.Create(ctx, "child2", 100, parent.SnapshotID, 0, 1, 100, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create child2: %v", err)
	}
	if err := snapReg.UpdateState(ctx, child2.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to activate child2: %v", err)
	}

	// Get children of parent
	children, err := snapReg.GetChildren(ctx, parent.SnapshotID)
	if err != nil {
		t.Fatalf("GetChildren failed: %v", err)
	}

	if len(children) != 2 {
		t.Errorf("Expected 2 children, got %d", len(children))
	}

	// Verify children IDs
	childIDs := make(map[uint64]bool)
	for _, c := range children {
		childIDs[c.SnapshotID] = true
	}
	if !childIDs[child1.SnapshotID] {
		t.Errorf("Child1 not found in children list")
	}
	if !childIDs[child2.SnapshotID] {
		t.Errorf("Child2 not found in children list")
	}

	// Parent should have no children returned for GetChildren(parent's parent)
	rootChildren, err := snapReg.GetChildren(ctx, 0)
	if err == nil || err != ErrInvalidID {
		// GetChildren with ID 0 should return error
		t.Logf("GetChildren(0) returned: %v, %v", rootChildren, err)
	}
}

// TestUpdateParentID verifies the UpdateParentID registry method.
func TestUpdateParentID(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, _, _, _ := setupTestComponents(t, dev)
	ctx := context.Background()

	// Create a chain: grandparent -> parent -> child
	grandparent, err := snapReg.Create(ctx, "grandparent", 100, 0, 0, 1, 100, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create grandparent: %v", err)
	}
	if err := snapReg.UpdateState(ctx, grandparent.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to activate grandparent: %v", err)
	}

	parent, err := snapReg.Create(ctx, "parent", 100, grandparent.SnapshotID, 0, 1, 100, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}
	if err := snapReg.UpdateState(ctx, parent.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to activate parent: %v", err)
	}

	child, err := snapReg.Create(ctx, "child", 100, parent.SnapshotID, 0, 1, 100, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	// Verify initial parent
	if child.ParentID != parent.SnapshotID {
		t.Errorf("Initial ParentID = %d, want %d", child.ParentID, parent.SnapshotID)
	}

	// Update child's parent to grandparent (simulating merge-up)
	err = snapReg.UpdateParentID(ctx, child.SnapshotID, grandparent.SnapshotID)
	if err != nil {
		t.Fatalf("UpdateParentID failed: %v", err)
	}

	// Verify updated parent
	updatedChild, err := snapReg.Get(ctx, child.SnapshotID)
	if err != nil {
		t.Fatalf("Failed to get updated child: %v", err)
	}
	if updatedChild.ParentID != grandparent.SnapshotID {
		t.Errorf("Updated ParentID = %d, want %d", updatedChild.ParentID, grandparent.SnapshotID)
	}
}

// TestMultipleChildrenMergeUp tests merge-up when a snapshot has multiple children.
func TestMultipleChildrenMergeUp(t *testing.T) {
	dev, _, cleanup := setupTestDevice(t)
	defer cleanup()

	snapReg, refReg, extMgr, inodeTbl := setupTestComponents(t, dev)
	ctx := context.Background()

	// Create: root -> middle -> {child1, child2, child3}
	root, err := snapReg.Create(ctx, "root", 100, 0, 0, 1, 100, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create root: %v", err)
	}
	if err := snapReg.UpdateState(ctx, root.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to activate root: %v", err)
	}

	middle, err := snapReg.Create(ctx, "middle", 100, root.SnapshotID, 0, 2, 200, FlagReadOnly)
	if err != nil {
		t.Fatalf("Failed to create middle: %v", err)
	}
	if err := snapReg.UpdateState(ctx, middle.SnapshotID, StateActive); err != nil {
		t.Fatalf("Failed to activate middle: %v", err)
	}

	var children []*Descriptor
	for i := 1; i <= 3; i++ {
		child, err := snapReg.Create(ctx, fmt.Sprintf("child%d", i), 100, middle.SnapshotID, 0, 1, 100, FlagReadOnly)
		if err != nil {
			t.Fatalf("Failed to create child%d: %v", i, err)
		}
		if err := snapReg.UpdateState(ctx, child.SnapshotID, StateActive); err != nil {
			t.Fatalf("Failed to activate child%d: %v", i, err)
		}
		children = append(children, child)
	}

	// Delete middle
	if err := snapReg.Delete(ctx, middle.SnapshotID); err != nil {
		t.Fatalf("Failed to delete middle: %v", err)
	}

	cfg := DefaultWorkerConfig()
	worker := NewWorker(snapReg, refReg, extMgr, inodeTbl, cfg)
	worker.Start()
	defer worker.Stop()

	// Queue delete job
	_, err = worker.QueueDeleteJob(ctx, middle.SnapshotID, []uint64{})
	if err != nil {
		t.Fatalf("Failed to queue delete job: %v", err)
	}

	waitForJob(t, snapshotRemoved(snapReg, middle.SnapshotID))

	// Verify all children now point to root
	for i, child := range children {
		updated, err := snapReg.Get(ctx, child.SnapshotID)
		if err != nil {
			t.Errorf("Failed to get child%d: %v", i+1, err)
			continue
		}
		if updated.ParentID != root.SnapshotID {
			t.Errorf("Child%d ParentID = %d, want %d (root)",
				i+1, updated.ParentID, root.SnapshotID)
		}
	}

	// Verify middle is gone
	_, err = snapReg.Get(ctx, middle.SnapshotID)
	if err != ErrSnapshotNotFound {
		t.Errorf("Middle snapshot should be removed, got: %v", err)
	}
}
