package journal

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// Config holds journal configuration options.
type Config struct {
	// SlotSize is the size of each node's journal area.
	// Default: 4MB (DefaultJournalSlotSize)
	SlotSize uint64

	// CommitMode controls when journal writes are synced.
	// Sync mode (default) fsyncs after every commit for maximum safety.
	// Async mode skips syncs entirely (use only for testing).
	// Batch mode groups syncs for better performance with durability.
	CommitMode CommitMode

	// BatchConfig controls batch commit behavior when CommitMode is CommitBatch.
	BatchConfig BatchConfig

	// CheckpointInterval is how often to checkpoint completed transactions.
	// Default: 5 seconds
	CheckpointInterval time.Duration
}

// CommitMode controls journal sync behavior.
type CommitMode int

const (
	// CommitSync fsyncs journal after every transaction commit.
	// Safest mode - guarantees durability but highest latency.
	CommitSync CommitMode = iota

	// CommitAsync skips journal syncs entirely for maximum performance.
	// WARNING: Transactions may be lost on crash. Use only for testing
	// or when durability is not required.
	CommitAsync

	// CommitBatch groups journal syncs for better performance while
	// still providing durability guarantees within a time window.
	// Syncs occur when MaxBatchTransactions is reached OR MaxBatchAge
	// expires, whichever comes first.
	CommitBatch
)

// BatchConfig controls batch commit behavior when CommitMode is CommitBatch.
type BatchConfig struct {
	// MaxBatchTransactions triggers a sync after this many uncommitted transactions.
	// Default: 32
	MaxBatchTransactions int

	// MaxBatchAge triggers a sync after this duration since first uncommitted transaction.
	// Default: 100ms
	MaxBatchAge time.Duration
}

// DefaultBatchConfig returns the default batch commit configuration.
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		MaxBatchTransactions: 32,
		MaxBatchAge:          100 * time.Millisecond,
	}
}

// DefaultConfig returns the default journal configuration.
func DefaultConfig() Config {
	return Config{
		SlotSize:           DefaultJournalSlotSize,
		CommitMode:         CommitSync,
		BatchConfig:        DefaultBatchConfig(),
		CheckpointInterval: 5 * time.Second,
	}
}

// Journal manages write-ahead logging for a single node.
// It provides the Begin/Commit interface for transactions and
// handles writing to the underlying ring buffer.
type Journal struct {
	mu sync.Mutex

	region     *Region
	nodeUUID   [16]byte
	generation uint64
	config     Config

	activeTxns     map[*Transaction]*ActiveTxn
	pendingIntents map[uint64]uint32 // committed but not yet completed, by seq, with where each starts in the ring
	checkpointHook atomic.Pointer[func()]
	hookRunning    atomic.Bool

	// Checkpoint tracking
	lastCheckpointSeq  uint64
	lastCheckpointTime time.Time

	// Background checkpoint goroutine
	checkpointStop chan struct{}
	checkpointWg   sync.WaitGroup
	stopOnce       sync.Once // Ensures Stop() is idempotent

	// Batch commit tracking (for CommitBatch mode)
	batchMu           sync.Mutex   // Protects batch state
	batchPendingCount int          // Number of commits since last sync
	batchFirstCommit  time.Time    // Time of first commit in current batch
	batchSyncCond     *sync.Cond   // Signals waiting goroutines when batch syncs
	batchSyncTimer    *time.Timer  // Timer for MaxBatchAge
	batchSyncWaiters  []chan error // Each gets the result of the batch sync it waits for

	// Metrics
	commitCount  uint64
	bytesWritten uint64
	batchSyncs   uint64 // Number of batch syncs performed
}

// ActiveTxn tracks a transaction between Begin and Commit or Abort.
type ActiveTxn struct {
	startTime time.Time
}

// Open opens an existing journal for the given node.
func Open(dev *storage.Device, regionOffset uint64, regionSize uint64, nodeUUID [16]byte, config Config) (*Journal, error) {
	region, err := OpenRegion(dev, regionOffset, regionSize, nodeUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to open journal region: %w", err)
	}

	j := &Journal{
		region:            region,
		nodeUUID:          nodeUUID,
		generation:        region.Superblock().NodeGeneration,
		config:            config,
		activeTxns:        make(map[*Transaction]*ActiveTxn),
		pendingIntents:    make(map[uint64]uint32),
		checkpointStop:    make(chan struct{}),
		lastCheckpointSeq: region.Superblock().LastCheckpointSeq,
	}
	j.batchSyncCond = sync.NewCond(&j.batchMu)

	return j, nil
}

// Init initializes a new journal for the given node.
func Init(dev *storage.Device, regionOffset uint64, regionSize uint64, nodeUUID [16]byte, generation uint64, config Config) (*Journal, error) {
	region, err := InitRegion(dev, regionOffset, regionSize, nodeUUID, generation)
	if err != nil {
		return nil, fmt.Errorf("failed to init journal region: %w", err)
	}

	j := &Journal{
		region:            region,
		nodeUUID:          nodeUUID,
		generation:        generation,
		config:            config,
		activeTxns:        make(map[*Transaction]*ActiveTxn),
		pendingIntents:    make(map[uint64]uint32),
		checkpointStop:    make(chan struct{}),
		lastCheckpointSeq: 0,
	}
	j.batchSyncCond = sync.NewCond(&j.batchMu)

	return j, nil
}

// Start begins background checkpoint processing.
func (j *Journal) Start() {
	j.checkpointWg.Add(1)
	go j.checkpointLoop()
}

// Stop stops background checkpoint processing and closes the journal.
// This function is idempotent - calling it multiple times is safe.
func (j *Journal) Stop() error {
	var stopErr error
	j.stopOnce.Do(func() {
		// Flush any pending batch commits before stopping
		var flushErr error
		if j.config.CommitMode == CommitBatch {
			flushErr = j.flushBatch()
		}

		close(j.checkpointStop)
		j.checkpointWg.Wait()

		j.mu.Lock()
		defer j.mu.Unlock()

		var cleanErr error
		switch {
		case flushErr != nil:
			// Leave the journal dirty so the next mount replays what may not have synced.
			cleanErr = fmt.Errorf("failed to flush batched commits: %w", flushErr)
		case len(j.activeTxns) == 0 && len(j.pendingIntents) == 0:
			// Mark journal clean on orderly shutdown
			if err := j.region.MarkClean(); err != nil {
				cleanErr = fmt.Errorf("failed to mark journal clean: %w", err)
			}
		}
		stopErr = errors.Join(cleanErr, j.region.Close())
	})
	return stopErr
}

// checkpointLoop periodically checkpoints completed transactions.
func (j *Journal) checkpointLoop() {
	defer j.checkpointWg.Done()

	ticker := time.NewTicker(j.config.CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-j.checkpointStop:
			return
		case <-ticker.C:
			j.periodicCheckpoint()
		}
	}
}

// periodicCheckpoint advances the head pointer for completed transactions.
// A failed checkpoint is retried on the next tick.
func (j *Journal) periodicCheckpoint() {
	if hook := j.checkpointHook.Load(); hook != nil {
		(*hook)()
	}
	_, _ = j.Checkpoint()
}

// SetCheckpointHook also runs fn early when the oldest intent is half a ring behind, since bursts fill the ring.
func (j *Journal) SetCheckpointHook(fn func()) {
	j.checkpointHook.Store(&fn)
}

func (j *Journal) runHookAsync() {
	hook := j.checkpointHook.Load()
	if hook == nil || !j.hookRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer j.hookRunning.Store(false)
		(*hook)()
	}()
}

func (j *Journal) oldestIntentLagLocked() uint32 {
	size, tail := j.region.DataAreaSize(), j.region.TailOffset()
	var oldest uint32
	for _, start := range j.pendingIntents {
		oldest = max(oldest, (tail+size-start)%size)
	}
	return oldest
}

// IntentLag reports in bytes how far intent seq starts behind the tail, and the ring size.
func (j *Journal) IntentLag(seq uint64) (lag, size uint32, ok bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	start, ok := j.pendingIntents[seq]
	if !ok {
		return 0, 0, false
	}
	size = j.region.DataAreaSize()
	return (j.region.TailOffset() + size - start) % size, size, true
}

// Checkpoint advances the head now instead of at the next interval. It reports false when the head could not move.
func (j *Journal) Checkpoint() (bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.advanceHead()
}

// advanceHead moves the head up to the oldest transaction that is still active or whose operation has not completed.
// It syncs the device first, so the changes of every transaction it passes outlive their records. Must hold j.mu.
func (j *Journal) advanceHead() (bool, error) {
	head, size := j.region.HeadOffset(), j.region.DataAreaSize()
	after := func(pos uint32) uint32 {
		if pos >= head {
			return pos - head
		}
		return pos + size - head
	}
	target, seq := j.region.TailOffset(), j.region.Superblock().NextSeq-1
	block := func(blockSeq uint64, pos uint32) {
		if after(pos) < after(target) {
			target = pos
		}
		seq = min(seq, blockSeq-1)
	}
	for blockSeq, pos := range j.pendingIntents {
		block(blockSeq, pos)
	}
	if target == head {
		return false, nil
	}
	if err := j.region.Sync(); err != nil {
		return false, err
	}
	if err := j.region.Checkpoint(seq, target); err != nil {
		return false, err
	}
	j.lastCheckpointSeq = seq
	j.lastCheckpointTime = time.Now()
	return true, nil
}

// Begin starts a new transaction. Commit assigns its sequence number and persists it.
func (j *Journal) Begin() *Transaction {
	j.mu.Lock()
	defer j.mu.Unlock()

	txn := NewTransaction(0, j.nodeUUID, j.generation)
	j.activeTxns[txn] = &ActiveTxn{startTime: time.Now()}
	return txn
}

// Commit writes the transaction to the journal and syncs to disk.
// This is the COMMIT POINT - after this returns successfully, the
// transaction is guaranteed to be replayed on recovery.
//
// The WAL protocol:
// 1. Serialize transaction to bytes
// 2. Write to journal ring buffer (advances tail in memory)
// 3. Flush superblock with updated tail offset to disk
// 4. Fsync journal (COMMIT POINT - barrier ensuring durability)
// 5. Mark transaction as committed in memory
//
// Returns the transaction sequence number for use with CompleteIntent().
func (j *Journal) Commit(txn *Transaction) (uint64, error) {
	j.mu.Lock()

	if _, ok := j.activeTxns[txn]; !ok {
		j.mu.Unlock()
		return 0, errors.New("transaction not found in active set")
	}

	// Check space for the sector-aligned record WriteAt writes
	need := sectorAligned(txn.TotalLength())
	available := j.region.AvailableSpace()
	if need > available {
		// Try to force checkpoint to free space
		if err := j.forceCheckpoint(); err != nil {
			j.mu.Unlock()
			return 0, ErrJournalFull
		}
		available = j.region.AvailableSpace()
		if need > available {
			j.mu.Unlock()
			return 0, ErrJournalFull
		}
	}

	// Mark journal dirty before writing
	if j.region.State() == JournalStateClean {
		if err := j.region.MarkDirty(); err != nil {
			j.mu.Unlock()
			return 0, fmt.Errorf("failed to mark journal dirty: %w", err)
		}
	}

	seq, start, written, err := j.region.Append(func(seq uint64) ([]byte, error) {
		txn.setSeq(seq)
		return txn.Serialize()
	})
	if err != nil {
		j.mu.Unlock()
		return 0, fmt.Errorf("failed to write transaction: %w", err)
	}

	// Flush superblock with updated tail offset to disk
	// This is CRITICAL for crash consistency - the tail offset must be
	// persisted so recovery can find the transaction data.
	if err := j.region.FlushSuperblock(); err != nil {
		j.mu.Unlock()
		return 0, fmt.Errorf("failed to flush superblock: %w", err)
	}

	// Update metrics and state BEFORE releasing lock
	// This is safe because the journal data is already written
	j.commitCount++
	j.bytesWritten += uint64(written)

	// The operation runs after Commit, so the head must not pass this transaction until CompleteIntent.
	delete(j.activeTxns, txn)
	j.pendingIntents[seq] = start
	pressed := j.oldestIntentLagLocked() > j.region.DataAreaSize()/2

	// Release lock BEFORE sync operations to avoid deadlock
	// The sync operations (CommitSync/CommitBatch) may need to acquire
	// other locks, so we must not hold j.mu during sync.
	j.mu.Unlock()
	if pressed {
		j.runHookAsync()
	}

	// COMMIT POINT - sync to disk
	// After this barrier, the transaction is guaranteed durable.
	switch j.config.CommitMode {
	case CommitSync:
		// Sync immediately for maximum durability
		if err := j.region.Sync(); err != nil {
			j.dropIntent(seq)
			return 0, fmt.Errorf("failed to sync journal: %w", err)
		}
	case CommitBatch:
		// Batch mode: wait for periodic sync, reducing fsync frequency
		// This amortizes fsync cost across multiple commits
		if err := j.waitForBatchSync(); err != nil {
			j.dropIntent(seq)
			return 0, fmt.Errorf("failed batch sync: %w", err)
		}
	case CommitAsync:
		// No sync - fastest but no durability guarantee
	}

	return seq, nil
}

func sectorAligned(n uint32) uint32 {
	return (n + storage.SectorSize - 1) / storage.SectorSize * storage.SectorSize
}

// dropIntent forgets an intent whose Commit failed, since its caller never runs the operation.
func (j *Journal) dropIntent(seq uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.pendingIntents, seq)
}

// Abort abandons a transaction without committing.
func (j *Journal) Abort(txn *Transaction) {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.activeTxns, txn)
}

// waitForBatchSync adds the caller to the batch and waits for sync.
// Called by Commit() when in CommitBatch mode.
// NOTE: Must NOT hold j.mu when calling this (releases j.mu before calling).
func (j *Journal) waitForBatchSync() error {
	j.batchMu.Lock()

	// Track this commit in the batch
	j.batchPendingCount++
	isFirstInBatch := j.batchPendingCount == 1

	// Start timer on first commit in batch
	if isFirstInBatch {
		j.batchFirstCommit = time.Now()
		j.scheduleBatchTimer()
	}

	// Check if we should trigger immediate sync (hit transaction limit)
	shouldSync := j.batchPendingCount >= j.config.BatchConfig.MaxBatchTransactions

	if shouldSync {
		// We hit the limit - perform sync now
		j.batchMu.Unlock()
		return j.flushBatch()
	}

	// Create a channel to wait on
	waitCh := make(chan error, 1)
	j.batchSyncWaiters = append(j.batchSyncWaiters, waitCh)
	j.batchMu.Unlock()

	// Wait for batch sync to complete
	return <-waitCh
}

// scheduleBatchTimer starts a timer to trigger batch sync after MaxBatchAge.
// Must be called with batchMu held.
func (j *Journal) scheduleBatchTimer() {
	if j.batchSyncTimer != nil {
		j.batchSyncTimer.Stop()
	}

	j.batchSyncTimer = time.AfterFunc(j.config.BatchConfig.MaxBatchAge, func() {
		if err := j.flushBatch(); err != nil {
			log.Printf("[ERROR] journal: batch sync failed: %v", err)
		}
	})
}

// flushBatch performs the actual sync and notifies all waiting goroutines.
// This is called either by the timer (MaxBatchAge) or when hitting MaxBatchTransactions.
func (j *Journal) flushBatch() error {
	j.batchMu.Lock()

	// Nothing to flush
	if j.batchPendingCount == 0 {
		j.batchMu.Unlock()
		return nil
	}

	// Stop timer if running
	if j.batchSyncTimer != nil {
		j.batchSyncTimer.Stop()
		j.batchSyncTimer = nil
	}

	// Capture waiters and reset batch state
	waiters := j.batchSyncWaiters
	j.batchSyncWaiters = nil
	j.batchPendingCount = 0
	j.batchFirstCommit = time.Time{}

	j.batchMu.Unlock()

	// Perform the actual sync (outside of lock)
	err := j.region.Sync()

	// Update metrics atomically (avoid acquiring j.mu which could cause contention)
	atomic.AddUint64(&j.batchSyncs, 1)

	// Notify all waiters
	for _, ch := range waiters {
		ch <- err
	}

	return err
}

// CompleteIntent writes an intent completion record to the journal.
// This marks that the operations from a previously committed transaction
// have been fully executed and do not need to be replayed on recovery.
//
// The intent logging protocol:
// 1. Begin transaction and log operations (intent)
// 2. Commit transaction (INTENT POINT - operations are now guaranteed to happen)
// 3. Execute operations idempotently
// 4. CompleteIntent (COMPLETION POINT - operations are done, skip on recovery)
//
// If the system crashes between steps 2 and 4, recovery will replay the
// operations. The idempotent execution ensures correctness regardless of
// how many times operations are replayed.
//
// It is idempotent: once an intent is complete, later calls do nothing.
func (j *Journal) CompleteIntent(intentSeq uint64) error {
	j.mu.Lock()
	_, pending := j.pendingIntents[intentSeq]
	delete(j.pendingIntents, intentSeq)
	j.mu.Unlock()
	if !pending {
		return nil
	}

	// The operation's writes must be durable before a record that says it finished, or a power cut could keep the
	// record and lose the writes. The record itself is made durable by the next sync.
	if err := j.syncBeforeCompletion(); err != nil {
		return fmt.Errorf("failed to sync before completing intent %d: %w", intentSeq, err)
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	need := sectorAligned(completionLength)
	if need > j.region.AvailableSpace() {
		if err := j.forceCheckpoint(); err != nil || need > j.region.AvailableSpace() {
			return ErrJournalFull
		}
	}
	if j.region.State() == JournalStateClean {
		if err := j.region.MarkDirty(); err != nil {
			return fmt.Errorf("failed to mark journal dirty: %w", err)
		}
	}

	_, _, written, err := j.region.Append(func(seq uint64) ([]byte, error) {
		return completionRecord(seq, j.nodeUUID, j.generation, intentSeq)
	})
	if err != nil {
		return fmt.Errorf("failed to write intent completion: %w", err)
	}
	if err := j.region.FlushSuperblock(); err != nil {
		return fmt.Errorf("failed to flush superblock: %w", err)
	}
	j.bytesWritten += uint64(written)
	return nil
}

func (j *Journal) syncBeforeCompletion() error {
	switch j.config.CommitMode {
	case CommitSync:
		return j.region.Sync()
	case CommitBatch:
		return j.waitForBatchSync()
	}
	return nil
}

// forceCheckpoint tries to checkpoint to free journal space.
// Must be called with lock held.
func (j *Journal) forceCheckpoint() error {
	_, err := j.advanceHead()
	return err
}

// NeedsRecovery returns true if the journal has uncommitted transactions.
func (j *Journal) NeedsRecovery() bool {
	return j.region.NeedsRecovery()
}

// State returns the current journal state.
func (j *Journal) State() JournalState {
	return j.region.State()
}

// AvailableSpace returns bytes available for new transactions.
func (j *Journal) AvailableSpace() uint32 {
	return j.region.AvailableSpace()
}

// Stats returns journal statistics.
func (j *Journal) Stats() JournalStats {
	j.mu.Lock()
	defer j.mu.Unlock()

	return JournalStats{
		State:             j.region.State(),
		HeadOffset:        j.region.HeadOffset(),
		TailOffset:        j.region.TailOffset(),
		AvailableSpace:    j.region.AvailableSpace(),
		ActiveTxnCount:    len(j.activeTxns),
		CommitCount:       j.commitCount,
		BytesWritten:      j.bytesWritten,
		LastCheckpointSeq: j.lastCheckpointSeq,
		BatchSyncCount:    atomic.LoadUint64(&j.batchSyncs),
	}
}

// JournalStats contains journal statistics.
type JournalStats struct {
	State             JournalState
	HeadOffset        uint32
	TailOffset        uint32
	AvailableSpace    uint32
	ActiveTxnCount    int
	CommitCount       uint64
	BytesWritten      uint64
	LastCheckpointSeq uint64
	BatchSyncCount    uint64 // Number of batch syncs (CommitBatch mode only)
}

// Region returns the underlying journal region.
// This is exposed for recovery operations.
func (j *Journal) Region() *Region {
	return j.region
}

// ScanAndRecover scans a journal region for uncommitted transactions and
// replays them. This is called during mount AFTER managers are created
// but BEFORE creating the new journal, to recover any dirty journals
// left by crashed nodes.
//
// This function:
// 1. Opens the journal region without UUID validation (for recovery)
// 2. Checks if recovery is needed (journal is dirty)
// 3. Replays committed transactions via the handler
// 4. Resets the journal to clean state
//
// Returns:
// - nil, nil if the journal was never initialized or is clean
// - result, nil if recovery succeeded
// - nil, error if the journal header is damaged or recovery failed
func ScanAndRecover(dev *storage.Device, regionOffset, regionSize uint64, handler RecoveryHandler, release func([]uint64) error) (*ScanRecoveryResult, error) {
	region, err := OpenRegionForRecovery(dev, regionOffset, regionSize)
	if errors.Is(err, ErrJournalUninitialized) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("journal at offset %d: %w", regionOffset, err)
	}

	// Check if recovery is needed
	if !region.NeedsRecovery() {
		return nil, nil // Journal is clean, nothing to do
	}

	// Get info about the crashed node's journal
	sb := region.Superblock()
	result := &ScanRecoveryResult{
		WasNeeded:  true,
		NodeUUID:   sb.NodeUUID,
		Generation: sb.NodeGeneration,
	}

	// Perform recovery
	stats, err := RecoverReleasing(region, handler, nil, release)
	if err != nil {
		return nil, fmt.Errorf("journal recovery failed: %w", err)
	}

	result.Stats = *stats
	return result, nil
}

// ScanRecoveryResult contains information about a self-recovery operation
// performed during mount. This is distinct from RecoveryResult (in
// cross_node_recovery.go) which is for recovering other nodes' journals.
type ScanRecoveryResult struct {
	WasNeeded  bool          // True if recovery was performed
	NodeUUID   [16]byte      // UUID of the crashed node whose journal was recovered
	Generation uint64        // Generation of the crashed node's journal
	Stats      RecoveryStats // Detailed recovery statistics
}
