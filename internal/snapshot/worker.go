// Package snapshot implements snapshot management for GoClusterFS.
//
// worker.go implements the background worker that deletes snapshots and
// cleans up snapshots a crash left half created or half deleted.
package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

// ErrWorkerStopped is returned when a job is queued on a stopped worker.
var ErrWorkerStopped = errors.New("snapshot worker stopped")

// Job is a queued snapshot deletion.
type Job struct {
	ID         uint64    // Unique job identifier
	SnapshotID uint64    // Snapshot being deleted
	StartTime  time.Time // When job was queued
}

// JobResult contains the result of a completed job.
type JobResult struct {
	JobID      uint64
	SnapshotID uint64
	Success    bool
	Error      error
	Duration   time.Duration
}

// Worker deletes snapshots in the background.
type Worker struct {
	mu         sync.RWMutex
	registry   *Registry
	extentMgr  *extent.Manager
	inodeTable *inode.Table

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

// NewWorker creates a new background worker. Blocks released by a deleted
// snapshot are freed through extentMgr, which must have refcounts set.
func NewWorker(registry *Registry, extentMgr *extent.Manager, inodeTable *inode.Table, cfg WorkerConfig) *Worker {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 100
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 2
	}

	w := &Worker{
		registry:      registry,
		extentMgr:     extentMgr,
		inodeTable:    inodeTable,
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

// QueueDeleteJob queues the deletion of a snapshot that is already marked
// deleting. The job outlives the request that queued it, so ctx only carries
// values; a full queue makes the caller wait.
func (w *Worker) QueueDeleteJob(ctx context.Context, snapshotID uint64) (uint64, error) {
	if !w.running.Load() {
		return 0, ErrWorkerStopped
	}
	ctx = context.WithoutCancel(ctx)

	jobID := w.nextJobID.Add(1) - 1
	job := &Job{
		ID:         jobID,
		SnapshotID: snapshotID,
		StartTime:  time.Now(),
	}

	if err := w.registry.SetRefCountJobID(ctx, snapshotID, jobID); err != nil {
		return 0, fmt.Errorf("failed to set job ID: %w", err)
	}

	select {
	case w.jobQueue <- job:
		log.Printf("[snapshot/worker] Queued delete job %d for snapshot %d", jobID, snapshotID)
		return jobID, nil
	case <-w.stopChan:
		return 0, ErrWorkerStopped
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

// processJob executes a single job.
func (w *Worker) processJob(job *Job) *JobResult {
	start := time.Now()
	err := w.processDeleteJob(context.Background(), job)
	return &JobResult{
		JobID:      job.ID,
		SnapshotID: job.SnapshotID,
		Success:    err == nil,
		Error:      err,
		Duration:   time.Since(start),
	}
}

// processDeleteJob re-parents the snapshot's children to its parent, gives
// back the blocks it captured, and removes its descriptor.
func (w *Worker) processDeleteJob(ctx context.Context, job *Job) error {
	snap, err := w.registry.Get(ctx, job.SnapshotID)
	if errors.Is(err, ErrSnapshotNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get snapshot %d: %w", job.SnapshotID, err)
	}

	children, err := w.registry.GetChildren(ctx, job.SnapshotID)
	if err != nil {
		return fmt.Errorf("failed to get children for snapshot %d: %w", job.SnapshotID, err)
	}
	for _, child := range children {
		if err := w.registry.UpdateParentID(ctx, child.SnapshotID, snap.ParentID); err != nil {
			return fmt.Errorf("failed to re-parent snapshot %d: %w", child.SnapshotID, err)
		}
		log.Printf("[snapshot/worker] Re-parented snapshot %d from %d to %d",
			child.SnapshotID, job.SnapshotID, snap.ParentID)
	}

	if snap.HasData() {
		if err := w.ReleaseData(ctx, snap.DataInode); err != nil {
			return fmt.Errorf("failed to release data of snapshot %d: %w", job.SnapshotID, err)
		}
	}

	if err := w.registry.Remove(ctx, job.SnapshotID); err != nil {
		return fmt.Errorf("failed to remove snapshot: %w", err)
	}

	// Freed only once no descriptor names it, so a reused slot is never released again.
	if snap.HasData() {
		if err := w.inodeTable.Free(ctx, snap.DataInode); err != nil {
			log.Printf("[snapshot/worker] Failed to free data inode %d of snapshot %d: %v",
				snap.DataInode, job.SnapshotID, err)
		}
	}

	log.Printf("[snapshot/worker] Snapshot %d deleted", job.SnapshotID)
	return nil
}

// ReleaseData gives back the blocks held by a snapshot's data inode. The empty
// extent map is committed before any block is released, so a crash leaks
// blocks at worst and calling it again is safe.
func (w *Worker) ReleaseData(ctx context.Context, num uint32) error {
	const attempts = 5
	for attempt := 1; ; attempt++ {
		ino, err := w.inodeTable.Read(num)
		if err != nil {
			return err
		}
		if ino.Magic == 0 || ino.Flags&inode.FlagSnapshotData == 0 {
			return nil
		}
		if err := w.extentMgr.Truncate(ctx, ino, 0); err != nil {
			w.extentMgr.DiscardPending(ctx, ino)
			return err
		}
		err = w.inodeTable.Write(ctx, ino)
		if err == nil {
			return nil
		}
		w.extentMgr.DiscardPending(ctx, ino)
		if !errors.Is(err, inode.ErrTreeConflict) || attempt == attempts {
			return err
		}
	}
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

// RecoverIncompleteJobs deletes snapshots that a crash left creating or
// deleting. A snapshot still creating never became visible as active, so it
// is rolled back. Call this on mount, after Start.
func (w *Worker) RecoverIncompleteJobs(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	snapshots, err := w.registry.List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}

	var recovered int
	for _, snap := range snapshots {
		switch snap.State {
		case StateCreating:
			if err := w.registry.Delete(ctx, snap.SnapshotID); err != nil {
				log.Printf("[snapshot/worker] Failed to roll back snapshot %d: %v", snap.SnapshotID, err)
				continue
			}
		case StateDeleting:
		default:
			continue
		}
		if _, err := w.QueueDeleteJob(ctx, snap.SnapshotID); err != nil {
			log.Printf("[snapshot/worker] Failed to queue cleanup of snapshot %d: %v", snap.SnapshotID, err)
			continue
		}
		recovered++
	}

	if recovered == 0 {
		log.Printf("[snapshot/worker] No incomplete jobs to recover")
	} else {
		log.Printf("[snapshot/worker] Cleaning up %d incomplete snapshot(s)", recovered)
	}
	return nil
}

// PendingJobs returns the number of jobs waiting in the queue.
func (w *Worker) PendingJobs() int {
	return len(w.jobQueue)
}

// IsRunning returns true if the worker is running.
func (w *Worker) IsRunning() bool {
	return w.running.Load()
}
