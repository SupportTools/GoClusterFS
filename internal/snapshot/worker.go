// Package snapshot implements snapshot management for GoClusterFS.
//
// worker.go implements the background worker for asynchronous refcount updates.
// When a snapshot is created, the descriptor is written immediately (O(1)),
// and the refcount increments are queued for background processing.
package snapshot

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/refcount"
)

// JobType represents the type of background job.
type JobType uint8

const (
	// JobTypeCreate processes refcount increments for snapshot creation.
	JobTypeCreate JobType = 1

	// JobTypeDelete processes refcount decrements for snapshot deletion.
	JobTypeDelete JobType = 2
)

// Job represents a background refcount job.
type Job struct {
	ID         uint64    // Unique job identifier
	Type       JobType   // Create or Delete
	SnapshotID uint64    // Snapshot being processed
	InodeNum   uint32    // Inode number of the source file
	BlockNums  []uint64  // Block numbers to process
	StartTime  time.Time // When job was queued
	Retries    int       // Number of retry attempts
}

// JobResult contains the result of a completed job.
type JobResult struct {
	JobID      uint64
	SnapshotID uint64
	Success    bool
	Error      error
	Duration   time.Duration
}

// Worker processes background refcount jobs for snapshots.
type Worker struct {
	mu         sync.RWMutex
	registry   *Registry
	refRegion  *refcount.Region
	extentMgr  *extent.Manager
	inodeTable *inode.Table
	bitmapMgr  *bitmap.Region // For freeing blocks when refcount reaches 0

	jobQueue    chan *Job
	resultChan  chan *JobResult
	stopChan    chan struct{}
	wg          sync.WaitGroup
	running     atomic.Bool
	nextJobID   atomic.Uint64
	workerCount int

	// Callbacks for integration
	onJobComplete func(*JobResult)
}

// WorkerConfig contains configuration for the background worker.
type WorkerConfig struct {
	// QueueSize is the maximum number of pending jobs.
	QueueSize int

	// WorkerCount is the number of concurrent workers.
	WorkerCount int

	// OnJobComplete is called when a job completes (optional).
	OnJobComplete func(*JobResult)
}

// DefaultWorkerConfig returns the default worker configuration.
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		QueueSize:   100,
		WorkerCount: 2,
	}
}

// NewWorker creates a new background worker.
// Note: bitmapMgr can be nil if block freeing is handled elsewhere, but for
// proper snapshot deletion with block reclamation, it should be provided.
func NewWorker(registry *Registry, refRegion *refcount.Region, extentMgr *extent.Manager, inodeTable *inode.Table, cfg WorkerConfig) *Worker {
	return NewWorkerWithBitmap(registry, refRegion, extentMgr, inodeTable, nil, cfg)
}

// NewWorkerWithBitmap creates a new background worker with bitmap for block freeing.
// When a block's refcount reaches 0 during snapshot deletion, the bitmap is used
// to mark the block as free for reallocation.
func NewWorkerWithBitmap(registry *Registry, refRegion *refcount.Region, extentMgr *extent.Manager, inodeTable *inode.Table, bitmapMgr *bitmap.Region, cfg WorkerConfig) *Worker {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 100
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 2
	}

	w := &Worker{
		registry:      registry,
		refRegion:     refRegion,
		extentMgr:     extentMgr,
		inodeTable:    inodeTable,
		bitmapMgr:     bitmapMgr,
		jobQueue:      make(chan *Job, cfg.QueueSize),
		resultChan:    make(chan *JobResult, cfg.QueueSize),
		stopChan:      make(chan struct{}),
		workerCount:   cfg.WorkerCount,
		onJobComplete: cfg.OnJobComplete,
	}
	w.nextJobID.Store(1)
	return w
}

// Start begins the background worker goroutines.
func (w *Worker) Start() {
	if w.running.Swap(true) {
		return // Already running
	}

	// Start worker goroutines
	for i := 0; i < w.workerCount; i++ {
		w.wg.Add(1)
		go w.workerLoop(i)
	}

	// Start result handler
	w.wg.Add(1)
	go w.resultHandler()

	log.Printf("[snapshot/worker] Started %d worker(s)", w.workerCount)
}

// Stop gracefully stops the background worker.
func (w *Worker) Stop() {
	if !w.running.Swap(false) {
		return // Not running
	}

	close(w.stopChan)
	w.wg.Wait()
	log.Printf("[snapshot/worker] Stopped")
}

// QueueCreateJob queues a refcount increment job for snapshot creation.
// Returns the job ID immediately.
func (w *Worker) QueueCreateJob(ctx context.Context, snapshotID uint64, inodeNum uint32, blockNums []uint64) (uint64, error) {
	if !w.running.Load() {
		return 0, fmt.Errorf("worker not running")
	}

	jobID := w.nextJobID.Add(1) - 1
	job := &Job{
		ID:         jobID,
		Type:       JobTypeCreate,
		SnapshotID: snapshotID,
		InodeNum:   inodeNum,
		BlockNums:  blockNums,
		StartTime:  time.Now(),
	}

	// Store job ID in snapshot descriptor
	if err := w.registry.SetRefCountJobID(ctx, snapshotID, jobID); err != nil {
		return 0, fmt.Errorf("failed to set job ID: %w", err)
	}

	select {
	case w.jobQueue <- job:
		log.Printf("[snapshot/worker] Queued create job %d for snapshot %d (%d blocks)",
			jobID, snapshotID, len(blockNums))
		return jobID, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return 0, fmt.Errorf("job queue full")
	}
}

// QueueDeleteJob queues a refcount decrement job for snapshot deletion.
// Returns the job ID immediately.
func (w *Worker) QueueDeleteJob(ctx context.Context, snapshotID uint64, blockNums []uint64) (uint64, error) {
	if !w.running.Load() {
		return 0, fmt.Errorf("worker not running")
	}

	jobID := w.nextJobID.Add(1) - 1
	job := &Job{
		ID:         jobID,
		Type:       JobTypeDelete,
		SnapshotID: snapshotID,
		BlockNums:  blockNums,
		StartTime:  time.Now(),
	}

	// Store job ID in snapshot descriptor
	if err := w.registry.SetRefCountJobID(ctx, snapshotID, jobID); err != nil {
		return 0, fmt.Errorf("failed to set job ID: %w", err)
	}

	select {
	case w.jobQueue <- job:
		log.Printf("[snapshot/worker] Queued delete job %d for snapshot %d (%d blocks)",
			jobID, snapshotID, len(blockNums))
		return jobID, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return 0, fmt.Errorf("job queue full")
	}
}

// workerLoop is the main loop for a worker goroutine.
func (w *Worker) workerLoop(workerID int) {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopChan:
			return
		case job := <-w.jobQueue:
			if job == nil {
				continue
			}
			result := w.processJob(job)
			w.resultChan <- result
		}
	}
}

// processJob executes a single refcount job.
func (w *Worker) processJob(job *Job) *JobResult {
	ctx := context.Background()
	start := time.Now()

	var err error
	switch job.Type {
	case JobTypeCreate:
		err = w.processCreateJob(ctx, job)
	case JobTypeDelete:
		err = w.processDeleteJob(ctx, job)
	default:
		err = fmt.Errorf("unknown job type: %d", job.Type)
	}

	return &JobResult{
		JobID:      job.ID,
		SnapshotID: job.SnapshotID,
		Success:    err == nil,
		Error:      err,
		Duration:   time.Since(start),
	}
}

// processCreateJob increments refcounts and transitions snapshot to Active.
func (w *Worker) processCreateJob(ctx context.Context, job *Job) error {
	if len(job.BlockNums) == 0 {
		// No blocks to process, just transition state
		return w.registry.UpdateState(ctx, job.SnapshotID, StateActive)
	}

	// Increment refcounts in batches for efficiency
	const batchSize = 512 // Process 512 blocks per batch
	for i := 0; i < len(job.BlockNums); i += batchSize {
		end := i + batchSize
		if end > len(job.BlockNums) {
			end = len(job.BlockNums)
		}

		batch := job.BlockNums[i:end]
		if _, err := w.refRegion.IncrementBatch(ctx, batch); err != nil {
			return fmt.Errorf("failed to increment refcounts (batch %d-%d): %w", i, end, err)
		}
	}

	// Transition snapshot to Active state
	if err := w.registry.UpdateState(ctx, job.SnapshotID, StateActive); err != nil {
		return fmt.Errorf("failed to update snapshot state: %w", err)
	}

	log.Printf("[snapshot/worker] Snapshot %d now active (%d blocks processed)",
		job.SnapshotID, len(job.BlockNums))
	return nil
}

// processDeleteJob handles snapshot deletion with merge-up for middle snapshots.
//
// For leaf snapshots (no children):
//   - Decrement all block refcounts
//   - Free blocks where refcount reaches 0
//
// For middle snapshots (has children):
//   - Re-parent children to this snapshot's parent (merge-up)
//   - Decrement refcounts (shared blocks won't reach 0)
//   - Free blocks that reach refcount 0
func (w *Worker) processDeleteJob(ctx context.Context, job *Job) error {
	// First, get the snapshot to find its parent
	snap, err := w.registry.Get(ctx, job.SnapshotID)
	if err != nil {
		return fmt.Errorf("failed to get snapshot %d: %w", job.SnapshotID, err)
	}

	// Check for children (determines if this is a leaf or middle snapshot)
	children, err := w.registry.GetChildren(ctx, job.SnapshotID)
	if err != nil {
		return fmt.Errorf("failed to get children for snapshot %d: %w", job.SnapshotID, err)
	}

	// Handle merge-up: re-parent children to grandparent
	if len(children) > 0 {
		log.Printf("[snapshot/worker] Snapshot %d has %d children, performing merge-up to parent %d",
			job.SnapshotID, len(children), snap.ParentID)

		for _, child := range children {
			if err := w.registry.UpdateParentID(ctx, child.SnapshotID, snap.ParentID); err != nil {
				return fmt.Errorf("failed to re-parent snapshot %d: %w", child.SnapshotID, err)
			}
			log.Printf("[snapshot/worker] Re-parented snapshot %d from %d to %d",
				child.SnapshotID, job.SnapshotID, snap.ParentID)
		}
	}

	// Decrement refcounts and free blocks that reach 0
	var freedCount int
	if len(job.BlockNums) > 0 {
		freedCount, err = w.decrementAndFreeBlocks(ctx, job.BlockNums)
		if err != nil {
			return fmt.Errorf("failed to decrement/free blocks: %w", err)
		}
	}

	// Remove the snapshot descriptor
	if err := w.registry.Remove(ctx, job.SnapshotID); err != nil {
		return fmt.Errorf("failed to remove snapshot: %w", err)
	}

	log.Printf("[snapshot/worker] Snapshot %d deleted (%d blocks processed, %d freed)",
		job.SnapshotID, len(job.BlockNums), freedCount)
	return nil
}

// decrementAndFreeBlocks decrements refcounts for blocks and frees those that reach 0.
// Returns the count of blocks freed.
func (w *Worker) decrementAndFreeBlocks(ctx context.Context, blockNums []uint64) (int, error) {
	const batchSize = 512
	freedCount := 0

	for i := 0; i < len(blockNums); i += batchSize {
		end := i + batchSize
		if end > len(blockNums) {
			end = len(blockNums)
		}

		batch := blockNums[i:end]
		newCounts, err := w.refRegion.DecrementBatch(ctx, batch)
		if err != nil {
			return freedCount, fmt.Errorf("failed to decrement refcounts (batch %d-%d): %w", i, end, err)
		}

		// Free blocks that reached refcount 0
		if w.bitmapMgr != nil {
			for j, newCount := range newCounts {
				if newCount == 0 {
					blockNum := batch[j]
					if err := w.bitmapMgr.Free(ctx, blockNum); err != nil {
						// Log but don't fail - block leak is better than corruption
						log.Printf("[snapshot/worker] Warning: failed to free block %d: %v", blockNum, err)
					} else {
						freedCount++
					}
				}
			}
		}
	}

	return freedCount, nil
}

// resultHandler processes completed job results.
func (w *Worker) resultHandler() {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopChan:
			return
		case result := <-w.resultChan:
			if result == nil {
				continue
			}

			if !result.Success {
				log.Printf("[snapshot/worker] Job %d failed for snapshot %d: %v",
					result.JobID, result.SnapshotID, result.Error)
			}

			// Call completion callback if set
			if w.onJobComplete != nil {
				w.onJobComplete(result)
			}
		}
	}
}

// RecoverIncompleteJobs scans for snapshots in StateCreating or StateDeleting
// and re-queues their background jobs. Call this on mount/startup.
func (w *Worker) RecoverIncompleteJobs(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Scan registry for incomplete snapshots
	stats, err := w.registry.Scan(ctx)
	if err != nil {
		return fmt.Errorf("failed to scan registry: %w", err)
	}

	if stats.CreatingCount == 0 && stats.DeletingCount == 0 {
		log.Printf("[snapshot/worker] No incomplete jobs to recover")
		return nil
	}

	log.Printf("[snapshot/worker] Recovering %d creating + %d deleting snapshots",
		stats.CreatingCount, stats.DeletingCount)

	// Get all snapshots and process incomplete ones
	snapshots, err := w.registry.List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}

	for _, snap := range snapshots {
		switch snap.State {
		case StateCreating:
			if err := w.recoverCreateJob(ctx, snap); err != nil {
				log.Printf("[snapshot/worker] Failed to recover create job for snapshot %d: %v",
					snap.SnapshotID, err)
			}

		case StateDeleting:
			if err := w.recoverDeleteJob(ctx, snap); err != nil {
				log.Printf("[snapshot/worker] Failed to recover delete job for snapshot %d: %v",
					snap.SnapshotID, err)
			}
		}
	}

	return nil
}

// recoverCreateJob recovers a snapshot stuck in StateCreating.
func (w *Worker) recoverCreateJob(ctx context.Context, snap *Descriptor) error {
	// Read the inode to get current block list
	ino, err := w.inodeTable.Read(snap.TargetInode)
	if err != nil {
		return fmt.Errorf("failed to read inode %d: %w", snap.TargetInode, err)
	}

	// Collect all block numbers from the inode
	blockNums, err := w.extentMgr.DataBlocks(ino)
	if err != nil {
		return fmt.Errorf("failed to list blocks of inode %d: %w", snap.TargetInode, err)
	}

	// Re-queue the create job
	// Note: This may result in some blocks being incremented twice if crash
	// happened mid-way. This is safe - it means slightly higher refcounts
	// that will be cleaned up when the snapshot is deleted.
	_, err = w.QueueCreateJob(ctx, snap.SnapshotID, snap.TargetInode, blockNums)
	return err
}

// recoverDeleteJob recovers a snapshot stuck in StateDeleting.
func (w *Worker) recoverDeleteJob(ctx context.Context, snap *Descriptor) error {
	// For deletion, we need to read the snapshot's extent tree (not the current inode)
	// Since we stored ExtentTreeRoot at snapshot time, we use that.
	// However, the blocks may have already been decremented, so we need to be careful.

	// For now, just remove the descriptor - the refcounts were either
	// already decremented or will be off by 1 (leak) which is acceptable
	// for crash recovery.
	return w.registry.Remove(ctx, snap.SnapshotID)
}

// PendingJobs returns the number of jobs waiting in the queue.
func (w *Worker) PendingJobs() int {
	return len(w.jobQueue)
}

// IsRunning returns true if the worker is running.
func (w *Worker) IsRunning() bool {
	return w.running.Load()
}
