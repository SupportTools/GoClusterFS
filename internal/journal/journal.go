package journal

import (
	"fmt"
	"sync"
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
	// Async mode batches syncs for better performance.
	CommitMode CommitMode

	// CheckpointInterval is how often to checkpoint completed transactions.
	// Default: 5 seconds
	CheckpointInterval time.Duration

	// FullThreshold triggers forced checkpoint when journal is this % full.
	// Default: 75%
	FullThreshold int
}

// CommitMode controls journal sync behavior.
type CommitMode int

const (
	// CommitSync fsyncs journal after every transaction commit.
	// Safest mode - guarantees durability.
	CommitSync CommitMode = iota

	// CommitAsync batches journal syncs for better performance.
	// Transactions may be lost on crash between syncs.
	CommitAsync
)

// DefaultConfig returns the default journal configuration.
func DefaultConfig() Config {
	return Config{
		SlotSize:           DefaultJournalSlotSize,
		CommitMode:         CommitSync,
		CheckpointInterval: 5 * time.Second,
		FullThreshold:      75,
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

	// Active transaction tracking
	activeTxns      map[uint64]*ActiveTxn
	oldestActiveSeq uint64

	// Checkpoint tracking
	lastCheckpointSeq  uint64
	lastCheckpointTime time.Time

	// Background checkpoint goroutine
	checkpointStop chan struct{}
	checkpointWg   sync.WaitGroup

	// Metrics
	commitCount  uint64
	bytesWritten uint64
}

// ActiveTxn tracks an in-progress transaction.
type ActiveTxn struct {
	txn       *Transaction
	startTime time.Time
	tailPos   uint32 // Position in journal where this txn starts
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
		activeTxns:        make(map[uint64]*ActiveTxn),
		checkpointStop:    make(chan struct{}),
		lastCheckpointSeq: region.Superblock().LastCheckpointSeq,
	}

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
		activeTxns:        make(map[uint64]*ActiveTxn),
		checkpointStop:    make(chan struct{}),
		lastCheckpointSeq: 0,
	}

	return j, nil
}

// Start begins background checkpoint processing.
func (j *Journal) Start() {
	j.checkpointWg.Add(1)
	go j.checkpointLoop()
}

// Stop stops background checkpoint processing and closes the journal.
func (j *Journal) Stop() error {
	close(j.checkpointStop)
	j.checkpointWg.Wait()

	// Mark journal clean on orderly shutdown
	j.mu.Lock()
	defer j.mu.Unlock()

	if len(j.activeTxns) == 0 {
		if err := j.region.MarkClean(); err != nil {
			return fmt.Errorf("failed to mark journal clean: %w", err)
		}
	}

	return j.region.Close()
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
func (j *Journal) periodicCheckpoint() {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Find the oldest active transaction
	if len(j.activeTxns) > 0 {
		// Cannot advance past any active transaction
		return
	}

	// All transactions are complete - can checkpoint to current tail
	tail := j.region.TailOffset()
	if tail != j.region.HeadOffset() {
		sb := j.region.Superblock()
		if err := j.region.Checkpoint(sb.NextSeq-1, tail); err != nil {
			// Log error but don't fail - will retry
			return
		}
		j.lastCheckpointSeq = sb.NextSeq - 1
		j.lastCheckpointTime = time.Now()
	}
}

// Begin starts a new transaction.
// The transaction will be assigned a sequence number and can have entries added.
// Call Commit() on the transaction to persist it.
func (j *Journal) Begin() *Transaction {
	j.mu.Lock()
	defer j.mu.Unlock()

	seq := j.region.NextSeq()
	txn := NewTransaction(seq, j.nodeUUID, j.generation)

	j.activeTxns[seq] = &ActiveTxn{
		txn:       txn,
		startTime: time.Now(),
		tailPos:   j.region.TailOffset(),
	}

	if j.oldestActiveSeq == 0 || seq < j.oldestActiveSeq {
		j.oldestActiveSeq = seq
	}

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
func (j *Journal) Commit(txn *Transaction) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	seq := txn.Seq()

	// Verify this transaction belongs to us
	active, ok := j.activeTxns[seq]
	if !ok {
		return fmt.Errorf("transaction %d not found in active set", seq)
	}
	if active.txn != txn {
		return fmt.Errorf("transaction %d mismatch", seq)
	}

	// Serialize the transaction
	data, err := txn.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize transaction: %w", err)
	}

	// Check space
	available := j.region.AvailableSpace()
	if uint32(len(data)) > available {
		// Try to force checkpoint to free space
		if err := j.forceCheckpoint(); err != nil {
			return ErrJournalFull
		}
		available = j.region.AvailableSpace()
		if uint32(len(data)) > available {
			return ErrJournalFull
		}
	}

	// Mark journal dirty before writing
	if j.region.State() == JournalStateClean {
		if err := j.region.MarkDirty(); err != nil {
			return fmt.Errorf("failed to mark journal dirty: %w", err)
		}
	}

	// Write to ring buffer
	written, err := j.region.WriteAt(data)
	if err != nil {
		return fmt.Errorf("failed to write transaction: %w", err)
	}
	if written != uint32(len(data)) {
		return fmt.Errorf("short write: %d < %d", written, len(data))
	}

	// Flush superblock with updated tail offset to disk
	// This is CRITICAL for crash consistency - the tail offset must be
	// persisted so recovery can find the transaction data.
	if err := j.region.FlushSuperblock(); err != nil {
		return fmt.Errorf("failed to flush superblock: %w", err)
	}

	// COMMIT POINT - sync to disk
	// After this barrier, the transaction is guaranteed durable.
	if j.config.CommitMode == CommitSync {
		if err := j.region.Sync(); err != nil {
			return fmt.Errorf("failed to sync journal: %w", err)
		}
	}

	// Update metrics
	j.commitCount++
	j.bytesWritten += uint64(written)

	// Remove from active set
	delete(j.activeTxns, seq)

	// Update oldest active
	if seq == j.oldestActiveSeq {
		j.updateOldestActive()
	}

	return nil
}

// Abort abandons a transaction without committing.
func (j *Journal) Abort(txn *Transaction) {
	j.mu.Lock()
	defer j.mu.Unlock()

	seq := txn.Seq()
	delete(j.activeTxns, seq)

	if seq == j.oldestActiveSeq {
		j.updateOldestActive()
	}
}

// forceCheckpoint tries to checkpoint to free journal space.
// Must be called with lock held.
func (j *Journal) forceCheckpoint() error {
	// Can only checkpoint past committed transactions
	if len(j.activeTxns) == 0 {
		// No active txns - checkpoint everything
		tail := j.region.TailOffset()
		sb := j.region.Superblock()
		return j.region.Checkpoint(sb.NextSeq-1, tail)
	}

	// Find oldest active transaction position
	var oldestPos uint32
	for _, active := range j.activeTxns {
		if oldestPos == 0 || active.tailPos < oldestPos {
			oldestPos = active.tailPos
		}
	}

	// Can only checkpoint up to just before oldest active
	if oldestPos > 0 && oldestPos != j.region.HeadOffset() {
		return j.region.Checkpoint(j.oldestActiveSeq-1, oldestPos)
	}

	return nil
}

// updateOldestActive recalculates the oldest active sequence.
// Must be called with lock held.
func (j *Journal) updateOldestActive() {
	j.oldestActiveSeq = 0
	for seq := range j.activeTxns {
		if j.oldestActiveSeq == 0 || seq < j.oldestActiveSeq {
			j.oldestActiveSeq = seq
		}
	}
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
// - nil, nil if no recovery was needed
// - result, nil if recovery succeeded
// - nil, error if recovery failed
func ScanAndRecover(dev *storage.Device, regionOffset, regionSize uint64, handler RecoveryHandler) (*ScanRecoveryResult, error) {
	// Try to open the journal region for recovery (no UUID validation)
	region, err := OpenRegionForRecovery(dev, regionOffset, regionSize)
	if err != nil {
		// Journal doesn't exist or is corrupt - no recovery needed
		// This is normal for fresh filesystems
		return nil, nil
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
	stats, err := Recover(region, handler)
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
