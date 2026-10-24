package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	testBlockSize     = 1024 * 1024 // 1MB
	testDeviceSize    = 100 * testBlockSize
	testDataBlocks    = 50
	testBitmapStart   = uint64(60 * testBlockSize)
	testInodeStart    = uint64(70 * testBlockSize)
	testInodeSize     = uint64(10 * testBlockSize)
	testRefcountStart = uint64(80 * testBlockSize)
	testSnapshotStart = uint64(90 * testBlockSize)
	testSnapshotSize  = uint64(5 * 4096) // 5 sectors
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

// workerEnv holds a small filesystem: data blocks start at device offset 0.
type workerEnv struct {
	t    *testing.T
	ctx  context.Context
	reg  *Registry
	refs *refcount.Region
	bm   *bitmap.Region
	tbl  *inode.Table
	ext  *extent.Manager
	w    *Worker
	done chan *JobResult
}

func newWorkerEnv(t *testing.T) *workerEnv {
	t.Helper()
	dev, _, cleanup := setupTestDevice(t)
	t.Cleanup(cleanup)

	var nodeUUID [16]byte
	env := &workerEnv{
		t:    t,
		ctx:  context.Background(),
		reg:  NewRegistry(dev, testSnapshotStart, testSnapshotSize, nil),
		refs: refcount.NewRegion(dev, testRefcountStart, testDataBlocks, nil),
		bm:   bitmap.NewRegion(dev, testBitmapStart, testDataBlocks, nil, nodeUUID),
		tbl:  inode.NewTable(dev, testInodeStart, testInodeSize, nil, nodeUUID),
		done: make(chan *JobResult, 64),
	}
	if err := env.reg.Initialize(env.ctx); err != nil {
		t.Fatalf("initialize registry: %v", err)
	}
	env.ext = extent.NewManager(dev, env.bm, nil, 0)
	env.ext.SetRefcounts(env.refs)
	env.tbl.SetTreeFlusher(env.ext)

	cfg := DefaultWorkerConfig()
	cfg.OnJobComplete = func(r *JobResult) { env.done <- r }
	env.w = NewWorker(env.reg, env.ext, env.tbl, cfg)
	return env
}

func (env *workerEnv) start() {
	env.w.Start()
	env.t.Cleanup(env.w.Stop)
}

func (env *workerEnv) claim() *inode.Inode {
	env.t.Helper()
	num, err := env.tbl.FindFreeSlot()
	if err != nil {
		env.t.Fatal(err)
	}
	ino, err := env.tbl.ClaimSlot(env.ctx, num)
	if err != nil {
		env.t.Fatal(err)
	}
	ino.Mode = inode.S_IFREG | 0o644
	return ino
}

func (env *workerEnv) write(ino *inode.Inode) {
	env.t.Helper()
	if err := env.tbl.Write(env.ctx, ino); err != nil {
		env.t.Fatalf("write inode %d: %v", ino.InodeNum, err)
	}
}

func (env *workerEnv) file(blocks uint64) (*inode.Inode, []uint64) {
	env.t.Helper()
	ino := env.claim()
	if _, err := env.ext.AllocateBlocks(env.ctx, ino, 0, blocks); err != nil {
		env.t.Fatal(err)
	}
	env.write(ino)
	data, err := env.ext.DataBlocks(ino)
	if err != nil {
		env.t.Fatal(err)
	}
	return ino, data
}

// snapshot captures file the way createSnapshot does and leaves it in state.
func (env *workerEnv) snapshot(file *inode.Inode, name string, state State) *Descriptor {
	env.t.Helper()
	data := env.claim()
	data.Flags |= inode.FlagSnapshotData
	env.write(data)
	desc, err := env.reg.Create(env.ctx, name, file.InodeNum, 0, data.InodeNum, file.AllocatedBlocks, file.Size, FlagReadOnly)
	if err != nil {
		env.t.Fatal(err)
	}
	blocks, err := env.ext.DataBlocks(file)
	if err != nil {
		env.t.Fatal(err)
	}
	if err := env.refs.Share(env.ctx, blocks); err != nil {
		env.t.Fatal(err)
	}
	if err := env.ext.CopyExtentsForClone(env.ctx, file, data); err != nil {
		env.t.Fatal(err)
	}
	env.write(data)
	if err := env.ext.MarkAllShared(file); err != nil {
		env.t.Fatal(err)
	}
	env.write(file)
	if state != StateCreating {
		if err := env.reg.UpdateState(env.ctx, desc.SnapshotID, state); err != nil {
			env.t.Fatal(err)
		}
	}
	return desc
}

// remove marks a snapshot deleting, runs its delete job, and waits for it.
func (env *workerEnv) remove(id uint64) {
	env.t.Helper()
	if err := env.reg.Delete(env.ctx, id); err != nil {
		env.t.Fatal(err)
	}
	if _, err := env.w.QueueDeleteJob(env.ctx, id); err != nil {
		env.t.Fatal(err)
	}
	env.wait(1)
}

func (env *workerEnv) wait(jobs int) {
	env.t.Helper()
	timeout := time.After(10 * time.Second)
	for range jobs {
		select {
		case r := <-env.done:
			if !r.Success {
				env.t.Fatalf("job %d for snapshot %d failed: %v", r.JobID, r.SnapshotID, r.Error)
			}
		case <-timeout:
			env.t.Fatalf("timed out waiting for %d snapshot job(s)", jobs)
		}
	}
}

func (env *workerEnv) allocated(block uint64) bool {
	env.t.Helper()
	ok, err := env.bm.IsAllocated(block)
	if err != nil {
		env.t.Fatal(err)
	}
	return ok
}

func (env *workerEnv) checkGone(desc *Descriptor) {
	env.t.Helper()
	if _, err := env.reg.Get(env.ctx, desc.SnapshotID); !errors.Is(err, ErrSnapshotNotFound) {
		env.t.Errorf("snapshot %q still listed: %v", desc.GetName(), err)
	}
	if ino, err := env.tbl.Read(desc.DataInode); err != nil || ino.Magic != 0 {
		env.t.Errorf("data inode %d of snapshot %q not freed (err=%v)", desc.DataInode, desc.GetName(), err)
	}
}

func TestWorkerStartStop(t *testing.T) {
	env := newWorkerEnv(t)
	worker := env.w

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

func TestWorkerQueueDeleteJob(t *testing.T) {
	env := newWorkerEnv(t)
	env.start()
	initial := env.bm.FreeCount()

	file, blocks := env.file(3)
	snap := env.snapshot(file, "test-snap", StateActive)

	// The file lets go of its last two blocks; only the snapshot holds them now.
	if err := env.ext.Truncate(env.ctx, file, extent.BlockSize); err != nil {
		t.Fatal(err)
	}
	env.write(file)
	if !env.allocated(blocks[1]) || !env.allocated(blocks[2]) {
		t.Fatal("truncating the file freed blocks the snapshot holds")
	}

	env.remove(snap.SnapshotID)
	env.checkGone(snap)
	if env.allocated(blocks[1]) || env.allocated(blocks[2]) {
		t.Error("blocks only the snapshot held stay allocated")
	}
	if !env.allocated(blocks[0]) {
		t.Error("deleting the snapshot freed a block the file still maps")
	}
	if got := env.bm.FreeCount(); got != initial-1 {
		t.Errorf("free blocks = %d, want %d", got, initial-1)
	}
}

// TestWorkerQueuesJobsWithCanceledContext checks that a canceled FUSE request
// context cannot drop a job for a snapshot that already exists.
func TestWorkerQueuesJobsWithCanceledContext(t *testing.T) {
	env := newWorkerEnv(t)
	file, _ := env.file(1)
	var snaps []*Descriptor
	for i := range 20 {
		snaps = append(snaps, env.snapshot(file, fmt.Sprintf("snap%d", i), StateDeleting))
	}
	env.start()

	ctx, cancel := context.WithCancel(env.ctx)
	cancel()
	for _, snap := range snaps {
		if _, err := env.w.QueueDeleteJob(ctx, snap.SnapshotID); err != nil {
			t.Fatalf("queue delete of %q: %v", snap.GetName(), err)
		}
	}
	env.wait(len(snaps))
	for _, snap := range snaps {
		env.checkGone(snap)
	}
}

func TestWorkerNotRunning(t *testing.T) {
	env := newWorkerEnv(t)
	if _, err := env.w.QueueDeleteJob(env.ctx, 1); !errors.Is(err, ErrWorkerStopped) {
		t.Errorf("QueueDeleteJob on a stopped worker: got %v, want ErrWorkerStopped", err)
	}
}

func TestWorkerPendingJobs(t *testing.T) {
	env := newWorkerEnv(t)
	if env.w.PendingJobs() != 0 {
		t.Errorf("PendingJobs = %d before start, want 0", env.w.PendingJobs())
	}
	env.start()
	if env.w.PendingJobs() != 0 {
		t.Errorf("PendingJobs = %d after start, want 0", env.w.PendingJobs())
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

// TestLeafSnapshotDeletion deletes the only owner left after the file is gone.
func TestLeafSnapshotDeletion(t *testing.T) {
	env := newWorkerEnv(t)
	env.start()
	initial := env.bm.FreeCount()

	file, blocks := env.file(2)
	snap := env.snapshot(file, "leaf-snap", StateActive)
	if err := env.ext.Delete(env.ctx, file); err != nil {
		t.Fatal(err)
	}
	if err := env.tbl.Free(env.ctx, file.InodeNum); err != nil {
		t.Fatal(err)
	}
	for _, b := range blocks {
		if !env.allocated(b) {
			t.Fatalf("deleting the file freed block %d that the snapshot holds", b)
		}
	}

	env.remove(snap.SnapshotID)
	env.checkGone(snap)
	if got := env.bm.FreeCount(); got != initial {
		t.Errorf("free blocks = %d, want %d", got, initial)
	}
}

func TestRecoverIncompleteSnapshots(t *testing.T) {
	env := newWorkerEnv(t)
	file, blocks := env.file(2)
	active := env.snapshot(file, "active", StateActive)
	creating := env.snapshot(file, "creating", StateCreating)
	deleting := env.snapshot(file, "deleting", StateDeleting)
	env.start()

	if err := env.w.RecoverIncompleteJobs(env.ctx); err != nil {
		t.Fatal(err)
	}
	env.wait(2)

	env.checkGone(creating)
	env.checkGone(deleting)
	if got, err := env.reg.Get(env.ctx, active.SnapshotID); err != nil || got.State != StateActive {
		t.Fatalf("active snapshot after recovery: %+v, %v", got, err)
	}
	for _, b := range blocks {
		e, err := env.refs.Get(env.ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		if !env.allocated(b) || e.RefCount != 2 {
			t.Errorf("block %d: allocated=%v refcount=%d, want held by the file and one snapshot", b, env.allocated(b), e.RefCount)
		}
	}
}

// TestMiddleSnapshotDeletionMergeUp tests deleting a snapshot that has children.
// Expected behavior:
// - Children are re-parented to grandparent (merge-up)
// - Snapshot descriptor removed
func TestMiddleSnapshotDeletionMergeUp(t *testing.T) {
	env := newWorkerEnv(t)
	snapReg, ctx := env.reg, env.ctx
	env.start()

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

	env.remove(middleSnap.SnapshotID)

	// Verify leaf snapshot was re-parented to root
	updatedLeaf, err := snapReg.Get(ctx, leafSnap.SnapshotID)
	if err != nil {
		t.Fatalf("Failed to get leaf snapshot: %v", err)
	}
	if updatedLeaf.ParentID != rootSnap.SnapshotID {
		t.Errorf("Leaf ParentID = %d, want %d (should be re-parented to root)",
			updatedLeaf.ParentID, rootSnap.SnapshotID)
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
	env := newWorkerEnv(t)
	snapReg, ctx := env.reg, env.ctx

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
	env := newWorkerEnv(t)
	snapReg, ctx := env.reg, env.ctx

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
	env := newWorkerEnv(t)
	snapReg, ctx := env.reg, env.ctx
	env.start()

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

	env.remove(middle.SnapshotID)

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
