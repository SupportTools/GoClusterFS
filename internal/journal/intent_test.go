package journal

import (
	"testing"
	"time"
)

func TestIntentCompletionSerializeDeserialize(t *testing.T) {
	// Create an intent completion record
	original := NewIntentCompletion(42)

	// Serialize it
	data := original.Serialize()

	// Verify size
	if len(data) != IntentCompletionSize {
		t.Errorf("Serialized size = %d, want %d", len(data), IntentCompletionSize)
	}

	// Deserialize it
	recovered, err := DeserializeIntentCompletion(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Verify fields
	if recovered.Magic != IntentCompletionMagic {
		t.Errorf("Magic = 0x%X, want 0x%X", recovered.Magic, IntentCompletionMagic)
	}
	if recovered.IntentSeq != 42 {
		t.Errorf("IntentSeq = %d, want 42", recovered.IntentSeq)
	}
	if recovered.Timestamp != original.Timestamp {
		t.Errorf("Timestamp = %d, want %d", recovered.Timestamp, original.Timestamp)
	}
	if recovered.Checksum != original.Checksum {
		t.Errorf("Checksum = 0x%X, want 0x%X", recovered.Checksum, original.Checksum)
	}
}

func TestIntentCompletionBadMagic(t *testing.T) {
	data := make([]byte, IntentCompletionSize)
	// Wrong magic number
	data[0] = 0xFF
	data[1] = 0xFF
	data[2] = 0xFF
	data[3] = 0xFF

	_, err := DeserializeIntentCompletion(data)
	if err == nil {
		t.Error("Expected error for invalid magic")
	}
}

func TestIntentCompletionBadChecksum(t *testing.T) {
	ic := NewIntentCompletion(99)
	data := ic.Serialize()

	// Corrupt the intent sequence field
	data[4] = 0xFF

	_, err := DeserializeIntentCompletion(data)
	if err == nil {
		t.Error("Expected error for checksum mismatch")
	}
}

func TestIntentCompletionTooShort(t *testing.T) {
	data := make([]byte, IntentCompletionSize-1)

	_, err := DeserializeIntentCompletion(data)
	if err == nil {
		t.Error("Expected error for short buffer")
	}
}

func TestJournalCompleteIntent(t *testing.T) {
	j, cleanup := createTestJournal(t, DefaultConfig())
	defer cleanup()

	// Begin and commit a transaction
	txn := j.Begin()
	txn.LogInodeCreate(testInodePayload(100))

	intentSeq, err := j.Commit(txn)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if intentSeq != 1 {
		t.Errorf("First intent seq = %d, want 1", intentSeq)
	}

	// Mark the intent as complete
	if err := j.CompleteIntent(intentSeq); err != nil {
		t.Fatalf("CompleteIntent failed: %v", err)
	}

	// Journal should still be in dirty state (has uncommitted data)
	if j.State() != JournalStateDirty {
		t.Errorf("State after CompleteIntent = %v, want Dirty", j.State())
	}

	// Stats should show bytes written increased
	stats := j.Stats()
	if stats.BytesWritten == 0 {
		t.Error("BytesWritten should be > 0 after CompleteIntent")
	}
}

func TestRecoveryWithIntentCompletion(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10*1024*1024)
	defer cleanup()

	nodeUUID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	generation := uint64(1)
	regionOffset := uint64(0)
	regionSize := uint64(DefaultJournalSlotSize)
	config := DefaultConfig()

	// Create journal and commit transactions
	j1, err := Init(dev, regionOffset, regionSize, nodeUUID, generation, config)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Transaction 1: Commit and complete
	txn1 := j1.Begin()
	txn1.LogInodeCreate(testInodePayload(100))
	seq1, err := j1.Commit(txn1)
	if err != nil {
		t.Fatalf("Commit txn1 failed: %v", err)
	}
	if err := j1.CompleteIntent(seq1); err != nil {
		t.Fatalf("CompleteIntent seq1 failed: %v", err)
	}

	// Transaction 2: Commit but DON'T complete (simulates crash during execution)
	txn2 := j1.Begin()
	txn2.LogInodeCreate(testInodePayload(101))
	seq2, err := j1.Commit(txn2)
	if err != nil {
		t.Fatalf("Commit txn2 failed: %v", err)
	}
	// Intentionally NOT calling CompleteIntent for seq2

	// Transaction 3: Commit and complete
	txn3 := j1.Begin()
	txn3.LogInodeCreate(testInodePayload(102))
	seq3, err := j1.Commit(txn3)
	if err != nil {
		t.Fatalf("Commit txn3 failed: %v", err)
	}
	if err := j1.CompleteIntent(seq3); err != nil {
		t.Fatalf("CompleteIntent seq3 failed: %v", err)
	}

	// Simulate crash by closing without clean shutdown
	// Don't call j1.Stop() - leave it dirty

	// Open region directly for recovery (bypass normal Open which validates)
	region, err := OpenRegionForRecovery(dev, regionOffset, regionSize)
	if err != nil {
		t.Fatalf("OpenRegionForRecovery failed: %v", err)
	}

	// Verify recovery is needed
	if !region.NeedsRecovery() {
		t.Error("Journal should need recovery")
	}

	// Track which transactions are replayed
	var replayedSeqs []uint64
	handler := func(txn *RecoveredTxn) error {
		replayedSeqs = append(replayedSeqs, txn.Header.Seq)
		return nil
	}

	// Perform recovery
	stats, err := Recover(region, handler)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Verify only transaction 2 was replayed (1 and 3 were completed)
	if len(replayedSeqs) != 1 {
		t.Errorf("Replayed %d transactions, want 1", len(replayedSeqs))
	}
	if len(replayedSeqs) > 0 && replayedSeqs[0] != seq2 {
		t.Errorf("Replayed seq %d, want seq %d (the incomplete one)", replayedSeqs[0], seq2)
	}

	// Verify recovery stats
	if stats.TxnCommitted != 3 {
		t.Errorf("TxnCommitted = %d, want 3", stats.TxnCommitted)
	}
	if stats.IntentCompletions != 2 {
		t.Errorf("IntentCompletions = %d, want 2", stats.IntentCompletions)
	}
	if stats.IntentsSkipped != 2 {
		t.Errorf("IntentsSkipped = %d, want 2", stats.IntentsSkipped)
	}
	if stats.EntriesReplayed != 1 {
		t.Errorf("EntriesReplayed = %d, want 1", stats.EntriesReplayed)
	}
}

func TestIntentCompletionMagic(t *testing.T) {
	// Verify magic is "INTC" in little-endian
	// I = 0x49, N = 0x4E, T = 0x54, C = 0x43
	// Little-endian: 0x43544E49
	if IntentCompletionMagic != 0x43544E49 {
		t.Errorf("IntentCompletionMagic = 0x%X, want 0x43544E49", IntentCompletionMagic)
	}
}

func TestIntentCompletionTimestamp(t *testing.T) {
	before := time.Now().UnixNano()
	ic := NewIntentCompletion(123)
	after := time.Now().UnixNano()

	if ic.Timestamp < before || ic.Timestamp > after {
		t.Errorf("Timestamp %d not in range [%d, %d]", ic.Timestamp, before, after)
	}
}
