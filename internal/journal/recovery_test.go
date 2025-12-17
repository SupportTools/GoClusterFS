package journal

import (
	"errors"
	"testing"
)

func TestRecoveryCleanJournal(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Clean journal should not need recovery
	if region.NeedsRecovery() {
		t.Error("New journal should not need recovery")
	}

	// Recovery should return immediately with empty stats
	stats, err := Recover(region, nil)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if stats.TxnScanned != 0 {
		t.Errorf("TxnScanned = %d, want 0", stats.TxnScanned)
	}
}

func TestRecoveryDirtyJournalNoTransactions(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Mark dirty but don't write any transactions
	if err := region.MarkDirty(); err != nil {
		t.Fatalf("MarkDirty failed: %v", err)
	}

	if !region.NeedsRecovery() {
		t.Error("Dirty journal should need recovery")
	}

	// Recovery should succeed with no transactions
	stats, err := Recover(region, nil)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if stats.TxnScanned != 0 {
		t.Errorf("TxnScanned = %d, want 0", stats.TxnScanned)
	}

	// Journal should be clean after recovery
	if region.NeedsRecovery() {
		t.Error("Journal should not need recovery after Recover")
	}
	if region.State() != JournalStateClean {
		t.Errorf("State = %v, want Clean", region.State())
	}
}

func TestRecoveryOneTransaction(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Create and write a transaction
	seq := region.NextSeq()
	txn := NewTransaction(seq, nodeUUID, 1)
	txn.LogInodeCreate(&InodeCreatePayload{
		InodeNum:       100,
		Mode:           0755,
		UID:            1000,
		GID:            1000,
		ParentInodeNum: 1,
	})

	data, err := txn.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Mark dirty and write
	if err := region.MarkDirty(); err != nil {
		t.Fatalf("MarkDirty failed: %v", err)
	}
	if _, err := region.WriteAt(data); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := region.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Recovery should find the transaction
	var recovered []*RecoveredTxn
	handler := func(txn *RecoveredTxn) error {
		recovered = append(recovered, txn)
		return nil
	}

	stats, err := Recover(region, handler)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if stats.TxnScanned != 1 {
		t.Errorf("TxnScanned = %d, want 1", stats.TxnScanned)
	}
	if stats.TxnCommitted != 1 {
		t.Errorf("TxnCommitted = %d, want 1", stats.TxnCommitted)
	}
	if len(recovered) != 1 {
		t.Errorf("len(recovered) = %d, want 1", len(recovered))
	}
	if stats.EntriesReplayed != 1 {
		t.Errorf("EntriesReplayed = %d, want 1", stats.EntriesReplayed)
	}

	// Verify recovered transaction
	if len(recovered) > 0 {
		r := recovered[0]
		if r.Header.Seq != seq {
			t.Errorf("Recovered seq = %d, want %d", r.Header.Seq, seq)
		}
		if len(r.Entries) != 1 {
			t.Errorf("Recovered entries = %d, want 1", len(r.Entries))
		}
		if len(r.Entries) > 0 && r.Entries[0].Entry.OpType != TxnOpInodeCreate {
			t.Errorf("OpType = %v, want TxnOpInodeCreate", r.Entries[0].Entry.OpType)
		}
	}
}

func TestRecoveryMultipleTransactions(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Mark dirty
	if err := region.MarkDirty(); err != nil {
		t.Fatalf("MarkDirty failed: %v", err)
	}

	// Write 5 transactions
	for i := 0; i < 5; i++ {
		seq := region.NextSeq()
		txn := NewTransaction(seq, nodeUUID, 1)
		txn.LogInodeCreate(&InodeCreatePayload{
			InodeNum:       uint64(100 + i),
			Mode:           0755,
			UID:            1000,
			GID:            1000,
			ParentInodeNum: 1,
		})
		txn.LogDirAddEntry(&DirAddEntryPayload{
			ParentInodeNum: 1,
			ChildInodeNum:  uint64(100 + i),
			Name:           "file",
		})

		data, _ := txn.Serialize()
		if _, err := region.WriteAt(data); err != nil {
			t.Fatalf("WriteAt %d failed: %v", i, err)
		}
	}
	if err := region.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Recovery should find all transactions
	var seqs []uint64
	handler := func(txn *RecoveredTxn) error {
		seqs = append(seqs, txn.Header.Seq)
		return nil
	}

	stats, err := Recover(region, handler)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if stats.TxnCommitted != 5 {
		t.Errorf("TxnCommitted = %d, want 5", stats.TxnCommitted)
	}
	if len(seqs) != 5 {
		t.Errorf("len(seqs) = %d, want 5", len(seqs))
	}
	// Entries: 2 per transaction (InodeCreate + DirAddEntry) × 5 = 10
	if stats.EntriesReplayed != 10 {
		t.Errorf("EntriesReplayed = %d, want 10", stats.EntriesReplayed)
	}

	// Verify sequences are in order
	for i, seq := range seqs {
		expected := uint64(i + 1)
		if seq != expected {
			t.Errorf("seqs[%d] = %d, want %d", i, seq, expected)
		}
	}
}

func TestRecoveryHandlerError(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Write a transaction
	if err := region.MarkDirty(); err != nil {
		t.Fatalf("MarkDirty failed: %v", err)
	}

	seq := region.NextSeq()
	txn := NewTransaction(seq, nodeUUID, 1)
	txn.LogInodeCreate(&InodeCreatePayload{InodeNum: 100})
	data, _ := txn.Serialize()
	region.WriteAt(data)
	region.Sync()

	// Handler that returns an error
	handlerErr := errors.New("handler failed")
	handler := func(txn *RecoveredTxn) error {
		return handlerErr
	}

	stats, err := Recover(region, handler)
	if err == nil {
		t.Error("Expected error from recovery")
	}
	if stats.Errors != 1 {
		t.Errorf("Errors = %d, want 1", stats.Errors)
	}

	// Journal should still be in recovering state (not reset)
	if region.State() != JournalStateRecovering {
		t.Errorf("State = %v, want Recovering", region.State())
	}
}

func TestDryRunRecover(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Write transactions
	if err := region.MarkDirty(); err != nil {
		t.Fatalf("MarkDirty failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		seq := region.NextSeq()
		txn := NewTransaction(seq, nodeUUID, 1)
		txn.LogInodeCreate(&InodeCreatePayload{InodeNum: uint64(100 + i)})
		data, _ := txn.Serialize()
		region.WriteAt(data)
	}
	region.Sync()

	// Dry run should not modify state
	initialState := region.State()

	txns, stats, err := DryRunRecover(region)
	if err != nil {
		t.Fatalf("DryRunRecover failed: %v", err)
	}

	if len(txns) != 3 {
		t.Errorf("len(txns) = %d, want 3", len(txns))
	}
	if stats.TxnCommitted != 3 {
		t.Errorf("TxnCommitted = %d, want 3", stats.TxnCommitted)
	}

	// State should not have changed
	if region.State() != initialState {
		t.Errorf("State changed from %v to %v during dry run", initialState, region.State())
	}

	// Journal should still need recovery
	if !region.NeedsRecovery() {
		t.Error("Journal should still need recovery after dry run")
	}
}

func TestRecoveryCleanState(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Simulate a crash after dirty but before commit
	if err := region.MarkDirty(); err != nil {
		t.Fatalf("MarkDirty failed: %v", err)
	}

	// Write partial (uncommitted) transaction - just raw bytes that won't parse
	garbage := make([]byte, 100)
	region.WriteAt(garbage)
	region.Sync()

	// Recover should handle this gracefully
	stats, err := Recover(region, nil)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Should have found incomplete transaction (invalid magic)
	if stats.TxnIncomplete != 1 {
		t.Errorf("TxnIncomplete = %d, want 1", stats.TxnIncomplete)
	}

	// Journal should be clean after recovery
	if region.NeedsRecovery() {
		t.Error("Journal should not need recovery after Recover")
	}
}

func TestRecoveryVerifiesChecksum(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4}
	region, err := InitRegion(dev, 0, uint64(DefaultJournalSlotSize), nodeUUID, 1)
	if err != nil {
		t.Fatalf("InitRegion failed: %v", err)
	}

	// Write a valid transaction
	if err := region.MarkDirty(); err != nil {
		t.Fatalf("MarkDirty failed: %v", err)
	}

	seq := region.NextSeq()
	txn := NewTransaction(seq, nodeUUID, 1)
	txn.LogInodeCreate(&InodeCreatePayload{InodeNum: 100})
	data, _ := txn.Serialize()

	// Corrupt the data (flip a bit in the middle)
	data[len(data)/2] ^= 0xFF

	region.WriteAt(data)
	region.Sync()

	// Recovery should detect checksum mismatch and treat as incomplete
	stats, err := Recover(region, nil)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Checksum mismatch should be treated as incomplete
	if stats.TxnIncomplete != 1 {
		t.Errorf("TxnIncomplete = %d, want 1", stats.TxnIncomplete)
	}
	if stats.TxnCommitted != 0 {
		t.Errorf("TxnCommitted = %d, want 0", stats.TxnCommitted)
	}
}
