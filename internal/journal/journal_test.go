package journal

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// testInodePayload creates a simple inode create payload for testing.
func testInodePayload(inodeNum uint64) *InodeCreatePayload {
	return &InodeCreatePayload{
		InodeNum:       inodeNum,
		Mode:           0755 | 0100000, // regular file
		UID:            1000,
		GID:            1000,
		ParentInodeNum: 1,
	}
}

// testDirAddPayload creates a simple directory entry payload for testing.
func testDirAddPayload(parentInode, childInode uint64, name string) *DirAddEntryPayload {
	return &DirAddEntryPayload{
		ParentInodeNum: parentInode,
		ChildInodeNum:  childInode,
		Name:           name,
	}
}

// createTestJournal creates a temporary journal for testing.
func createTestJournal(t *testing.T, config Config) (*Journal, func()) {
	t.Helper()

	dev, cleanup := createTestDevice(t, 10*1024*1024) // 10MB

	nodeUUID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	generation := uint64(1)
	regionOffset := uint64(0)
	regionSize := uint64(DefaultJournalSlotSize) // 4MB

	j, err := Init(dev, regionOffset, regionSize, nodeUUID, generation, config)
	if err != nil {
		cleanup()
		t.Fatalf("Init failed: %v", err)
	}

	return j, func() {
		_ = j.Stop()
		cleanup()
	}
}

func TestJournalInit(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Verify initial state
	if j.State() != JournalStateClean {
		t.Errorf("State = %v, want Clean", j.State())
	}

	if j.NeedsRecovery() {
		t.Error("New journal should not need recovery")
	}

	stats := j.Stats()
	if stats.ActiveTxnCount != 0 {
		t.Errorf("ActiveTxnCount = %d, want 0", stats.ActiveTxnCount)
	}
	if stats.CommitCount != 0 {
		t.Errorf("CommitCount = %d, want 0", stats.CommitCount)
	}
}

func TestJournalBeginCommit(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Begin a transaction
	txn := j.Begin()
	if txn == nil {
		t.Fatal("Begin returned nil")
	}

	// Check sequence number
	seq := txn.Seq()
	if seq != 1 {
		t.Errorf("First txn seq = %d, want 1", seq)
	}

	// Check active transaction tracking
	stats := j.Stats()
	if stats.ActiveTxnCount != 1 {
		t.Errorf("ActiveTxnCount = %d, want 1", stats.ActiveTxnCount)
	}

	// Add an operation
	if err := txn.LogInodeCreate(testInodePayload(100)); err != nil {
		t.Fatal(err)
	}

	// Commit
	if _, err := j.Commit(txn); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Check state after commit
	stats = j.Stats()
	if stats.ActiveTxnCount != 0 {
		t.Errorf("ActiveTxnCount after commit = %d, want 0", stats.ActiveTxnCount)
	}
	if stats.CommitCount != 1 {
		t.Errorf("CommitCount = %d, want 1", stats.CommitCount)
	}
	if stats.BytesWritten == 0 {
		t.Error("BytesWritten should be > 0")
	}
}

func TestJournalBeginAbort(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Begin and abort
	txn := j.Begin()
	if err := txn.LogInodeCreate(testInodePayload(100)); err != nil {
		t.Fatal(err)
	}

	j.Abort(txn)

	// Check state
	stats := j.Stats()
	if stats.ActiveTxnCount != 0 {
		t.Errorf("ActiveTxnCount after abort = %d, want 0", stats.ActiveTxnCount)
	}
	if stats.CommitCount != 0 {
		t.Errorf("CommitCount after abort = %d, want 0", stats.CommitCount)
	}
}

func TestJournalMultipleTransactions(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Commit several transactions
	for i := uint64(1); i <= 10; i++ {
		txn := j.Begin()
		if txn.Seq() != i {
			t.Errorf("Transaction %d seq = %d, want %d", i, txn.Seq(), i)
		}
		if err := txn.LogInodeCreate(testInodePayload(100 + i)); err != nil {
			t.Fatal(err)
		}
		if _, err := j.Commit(txn); err != nil {
			t.Fatalf("Commit %d failed: %v", i, err)
		}
	}

	stats := j.Stats()
	if stats.CommitCount != 10 {
		t.Errorf("CommitCount = %d, want 10", stats.CommitCount)
	}
}

func TestJournalConcurrentTransactions(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Begin multiple transactions before committing any
	txn1 := j.Begin()
	txn2 := j.Begin()
	txn3 := j.Begin()

	// Check tracking
	stats := j.Stats()
	if stats.ActiveTxnCount != 3 {
		t.Errorf("ActiveTxnCount = %d, want 3", stats.ActiveTxnCount)
	}

	// Add operations
	if err := txn1.LogInodeCreate(testInodePayload(100)); err != nil {
		t.Fatal(err)
	}
	if err := txn2.LogInodeCreate(testInodePayload(101)); err != nil {
		t.Fatal(err)
	}
	if err := txn3.LogInodeCreate(testInodePayload(102)); err != nil {
		t.Fatal(err)
	}

	// Commit out of order
	if _, err := j.Commit(txn2); err != nil {
		t.Fatalf("Commit txn2 failed: %v", err)
	}
	if _, err := j.Commit(txn1); err != nil {
		t.Fatalf("Commit txn1 failed: %v", err)
	}
	if _, err := j.Commit(txn3); err != nil {
		t.Fatalf("Commit txn3 failed: %v", err)
	}

	stats = j.Stats()
	if stats.ActiveTxnCount != 0 {
		t.Errorf("ActiveTxnCount = %d, want 0", stats.ActiveTxnCount)
	}
	if stats.CommitCount != 3 {
		t.Errorf("CommitCount = %d, want 3", stats.CommitCount)
	}
}

func TestJournalCommitWrongTxn(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Create a transaction not from this journal
	fakeTxn := NewTransaction(999, [16]byte{}, 1)

	// Should fail
	_, err := j.Commit(fakeTxn)
	if err == nil {
		t.Error("Expected error when committing unknown transaction")
	}
}

func TestJournalMarksDirtyOnCommit(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Initially clean
	if j.State() != JournalStateClean {
		t.Error("Should start clean")
	}

	// Commit a transaction
	txn := j.Begin()
	if err := txn.LogInodeCreate(testInodePayload(100)); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Commit(txn); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Should be dirty now
	if j.State() != JournalStateDirty {
		t.Error("Should be dirty after commit")
	}
}

func TestJournalCleanOnStop(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	regionOffset := uint64(0)
	regionSize := uint64(DefaultJournalSlotSize)

	// Create, commit, and stop
	j1, err := Init(dev, regionOffset, regionSize, nodeUUID, 1, DefaultConfig())
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	txn := j1.Begin()
	if err := txn.LogInodeCreate(testInodePayload(100)); err != nil {
		t.Fatal(err)
	}
	if _, err := j1.Commit(txn); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Stop should mark clean
	if err := j1.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Reopen and verify clean
	j2, err := Open(dev, regionOffset, regionSize, nodeUUID, DefaultConfig())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = j2.Stop() }()

	if j2.NeedsRecovery() {
		t.Error("Should not need recovery after clean shutdown")
	}
}

func TestJournalStartStop(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Start background checkpoint
	j.Start()

	// Do some operations
	for i := 0; i < 5; i++ {
		txn := j.Begin()
		if err := txn.LogInodeCreate(testInodePayload(uint64(100 + i))); err != nil {
			t.Fatal(err)
		}
		if _, err := j.Commit(txn); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}

	// Stop should work without hanging
	// (implicitly tested by cleanup, but let's be explicit)
}

func TestJournalAsyncCommitMode(t *testing.T) {
	config := DefaultConfig()
	config.CommitMode = CommitAsync

	j, cleanup := createTestJournal(t, config)
	defer cleanup()

	// Commit should work in async mode
	txn := j.Begin()
	if err := txn.LogInodeCreate(testInodePayload(100)); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Commit(txn); err != nil {
		t.Fatalf("Commit in async mode failed: %v", err)
	}

	stats := j.Stats()
	if stats.CommitCount != 1 {
		t.Errorf("CommitCount = %d, want 1", stats.CommitCount)
	}
}

func TestJournalRegionAccess(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Region should be accessible for recovery operations
	region := j.Region()
	if region == nil {
		t.Error("Region() returned nil")
	}
}

func TestJournalAvailableSpace(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	initialSpace := j.AvailableSpace()
	if initialSpace == 0 {
		t.Error("Initial available space should be > 0")
	}

	// After commit, space should decrease
	txn := j.Begin()
	if err := txn.LogInodeCreate(testInodePayload(100)); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Commit(txn); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	afterSpace := j.AvailableSpace()
	if afterSpace >= initialSpace {
		t.Errorf("Space should decrease after commit: %d >= %d", afterSpace, initialSpace)
	}
}

func TestJournalLargeTransaction(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Create a transaction with many entries
	txn := j.Begin()

	for i := uint64(0); i < 100; i++ {
		if err := txn.LogInodeCreate(testInodePayload(100 + i)); err != nil {
			t.Fatal(err)
		}
		if err := txn.LogDirAddEntry(testDirAddPayload(1, 100+i, "file"+string(rune('a'+int(i%26))))); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := j.Commit(txn); err != nil {
		t.Fatalf("Commit large transaction failed: %v", err)
	}

	if txn.EntryCount() != 200 {
		t.Errorf("EntryCount = %d, want 200", txn.EntryCount())
	}
}

func TestJournalStats(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Initial stats
	stats := j.Stats()
	if stats.State != JournalStateClean {
		t.Error("Initial state should be Clean")
	}
	if stats.HeadOffset != 0 {
		t.Errorf("HeadOffset = %d, want 0", stats.HeadOffset)
	}
	if stats.TailOffset != 0 {
		t.Errorf("TailOffset = %d, want 0", stats.TailOffset)
	}

	// After commit
	txn := j.Begin()
	if err := txn.LogInodeCreate(testInodePayload(100)); err != nil {
		t.Fatal(err)
	}
	j.Commit(txn) //nolint:errcheck

	stats = j.Stats()
	if stats.TailOffset == 0 {
		t.Error("TailOffset should be > 0 after commit")
	}
	if stats.BytesWritten == 0 {
		t.Error("BytesWritten should be > 0")
	}
}

func TestJournalConfigDefaults(t *testing.T) {
	config := DefaultConfig()

	if config.SlotSize != DefaultJournalSlotSize {
		t.Errorf("SlotSize = %d, want %d", config.SlotSize, DefaultJournalSlotSize)
	}
	if config.CommitMode != CommitSync {
		t.Errorf("CommitMode = %v, want CommitSync", config.CommitMode)
	}
	if config.CheckpointInterval != 5*time.Second {
		t.Errorf("CheckpointInterval = %v, want 5s", config.CheckpointInterval)
	}
	if config.FullThreshold != 75 {
		t.Errorf("FullThreshold = %d, want 75", config.FullThreshold)
	}
}

func TestJournalOpen(t *testing.T) {
	// Create a temp file
	f, err := os.CreateTemp("", "journal_open_test_*.img")
	if err != nil {
		t.Fatalf("CreateTemp failed: %v", err)
	}
	path := f.Name()
	if err := f.Truncate(10 * 1024 * 1024); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(path) }()

	// Need to import storage for this test
	// Instead, let's use the helper
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	regionOffset := uint64(0)
	regionSize := uint64(DefaultJournalSlotSize)
	config := DefaultConfig()

	// Init first
	j1, err := Init(dev, regionOffset, regionSize, nodeUUID, 1, config)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Commit something
	txn := j1.Begin()
	if err := txn.LogInodeCreate(testInodePayload(100)); err != nil {
		t.Fatal(err)
	}
	j1.Commit(txn) //nolint:errcheck
	if err := j1.Stop(); err != nil {
		t.Fatal(err)
	}

	// Open
	j2, err := Open(dev, regionOffset, regionSize, nodeUUID, config)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = j2.Stop() }()

	// Should be able to continue
	txn2 := j2.Begin()
	if txn2.Seq() != 2 {
		t.Errorf("After reopen, seq = %d, want 2", txn2.Seq())
	}
}

func TestJournalBatchCommitMode(t *testing.T) {
	config := DefaultConfig()
	config.CommitMode = CommitBatch
	config.BatchConfig = BatchConfig{
		MaxBatchTransactions: 5,
		MaxBatchAge:          50 * time.Millisecond,
	}

	j, cleanup := createTestJournal(t, config)
	defer cleanup()

	// Single commit should work (will wait for timer)
	start := time.Now()
	txn := j.Begin()
	if err := txn.LogInodeCreate(testInodePayload(100)); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Commit(txn); err != nil {
		t.Fatalf("Commit in batch mode failed: %v", err)
	}
	elapsed := time.Since(start)

	// Should have waited approximately MaxBatchAge (timer triggered)
	// Allow some slack for timing
	if elapsed < 40*time.Millisecond {
		t.Errorf("Commit returned too quickly (%v), expected to wait for batch timer", elapsed)
	}

	stats := j.Stats()
	if stats.CommitCount != 1 {
		t.Errorf("CommitCount = %d, want 1", stats.CommitCount)
	}
	if stats.BatchSyncCount != 1 {
		t.Errorf("BatchSyncCount = %d, want 1", stats.BatchSyncCount)
	}
}

func TestJournalBatchTriggerByCount(t *testing.T) {
	config := DefaultConfig()
	config.CommitMode = CommitBatch
	config.BatchConfig = BatchConfig{
		MaxBatchTransactions: 3,
		MaxBatchAge:          5 * time.Second, // Long timer - should not trigger
	}

	j, cleanup := createTestJournal(t, config)
	defer cleanup()

	// Commit MaxBatchTransactions transactions concurrently
	// When the 3rd commit arrives, it should trigger immediate sync.
	// NOTE: Batch mode is designed for concurrent commits from multiple goroutines.
	// Sequential commits from the same goroutine would block on the first commit.
	var wg sync.WaitGroup
	errors := make(chan error, 3)

	start := time.Now()
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			txn := j.Begin()
			if err := txn.LogInodeCreate(testInodePayload(uint64(100 + idx))); err != nil {
				errors <- err
				return
			}
			if _, err := j.Commit(txn); err != nil {
				errors <- fmt.Errorf("commit %d failed: %v", idx, err)
			}
		}(i)
	}
	wg.Wait()
	close(errors)
	elapsed := time.Since(start)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Should have completed quickly (transaction count trigger, not timer)
	if elapsed > 1*time.Second {
		t.Errorf("Commits took too long (%v), expected quick sync on count trigger", elapsed)
	}

	stats := j.Stats()
	if stats.CommitCount != 3 {
		t.Errorf("CommitCount = %d, want 3", stats.CommitCount)
	}
	// At least one batch sync should have occurred
	if stats.BatchSyncCount == 0 {
		t.Error("BatchSyncCount should be > 0")
	}
}

func TestJournalBatchConcurrent(t *testing.T) {
	config := DefaultConfig()
	config.CommitMode = CommitBatch
	config.BatchConfig = BatchConfig{
		MaxBatchTransactions: 10,
		MaxBatchAge:          100 * time.Millisecond,
	}

	j, cleanup := createTestJournal(t, config)
	defer cleanup()

	// Launch multiple goroutines that commit concurrently
	// They should share batch syncs
	const numGoroutines = 5
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			txn := j.Begin()
			if err := txn.LogInodeCreate(testInodePayload(uint64(100 + id))); err != nil {
				errors <- err
				return
			}
			if _, err := j.Commit(txn); err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent commit failed: %v", err)
	}

	stats := j.Stats()
	if stats.CommitCount != numGoroutines {
		t.Errorf("CommitCount = %d, want %d", stats.CommitCount, numGoroutines)
	}

	// Batch syncs should be much less than commit count
	// (multiple commits share the same sync)
	if stats.BatchSyncCount >= uint64(numGoroutines) {
		t.Logf("Note: BatchSyncCount (%d) >= CommitCount (%d), batching might not be working optimally",
			stats.BatchSyncCount, numGoroutines)
	}
}

func TestJournalBatchConfigDefaults(t *testing.T) {
	config := DefaultBatchConfig()

	if config.MaxBatchTransactions != 32 {
		t.Errorf("MaxBatchTransactions = %d, want 32", config.MaxBatchTransactions)
	}
	if config.MaxBatchAge != 100*time.Millisecond {
		t.Errorf("MaxBatchAge = %v, want 100ms", config.MaxBatchAge)
	}
}

// A batched commit must report a failed batch sync, not success.
func TestBatchCommitReturnsSyncError(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()
	config := DefaultConfig()
	config.CommitMode = CommitBatch
	config.BatchConfig = BatchConfig{MaxBatchTransactions: 100, MaxBatchAge: 500 * time.Millisecond}
	j, err := Init(dev, 0, DefaultJournalSlotSize, [16]byte{1}, 1, config)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	defer func() { _ = j.Stop() }()

	txn := j.Begin()
	if err := txn.LogInodeCreate(testInodePayload(100)); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := j.Commit(txn)
		result <- err
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		j.batchMu.Lock()
		pending := j.batchPendingCount
		j.batchMu.Unlock()
		if pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("commit never joined a batch")
		}
		time.Sleep(time.Millisecond)
	}
	if err := dev.Close(); err != nil {
		t.Fatal(err)
	}

	if err := <-result; err == nil {
		t.Fatal("Commit returned nil after its batch sync failed")
	}
}
