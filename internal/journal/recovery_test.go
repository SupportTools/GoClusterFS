package journal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
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
	if err := txn.LogInodeCreate(&InodeCreatePayload{
		InodeNum:       100,
		Mode:           0755,
		UID:            1000,
		GID:            1000,
		ParentInodeNum: 1,
	}); err != nil {
		t.Fatal(err)
	}

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
		if err := txn.LogInodeCreate(&InodeCreatePayload{
			InodeNum:       uint64(100 + i),
			Mode:           0755,
			UID:            1000,
			GID:            1000,
			ParentInodeNum: 1,
		}); err != nil {
			t.Fatal(err)
		}
		if err := txn.LogDirAddEntry(&DirAddEntryPayload{
			ParentInodeNum: 1,
			ChildInodeNum:  uint64(100 + i),
			Name:           "file",
		}); err != nil {
			t.Fatal(err)
		}

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
	if err := txn.LogInodeCreate(&InodeCreatePayload{InodeNum: 100}); err != nil {
		t.Fatal(err)
	}
	data, _ := txn.Serialize()
	if _, err := region.WriteAt(data); err != nil {
		t.Fatal(err)
	}
	if err := region.Sync(); err != nil {
		t.Fatal(err)
	}

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
		if err := txn.LogInodeCreate(&InodeCreatePayload{InodeNum: uint64(100 + i)}); err != nil {
			t.Fatal(err)
		}
		data, _ := txn.Serialize()
		if _, err := region.WriteAt(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := region.Sync(); err != nil {
		t.Fatal(err)
	}

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
	if _, err := region.WriteAt(garbage); err != nil {
		t.Fatal(err)
	}
	if err := region.Sync(); err != nil {
		t.Fatal(err)
	}

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
	if err := txn.LogInodeCreate(&InodeCreatePayload{InodeNum: 100}); err != nil {
		t.Fatal(err)
	}
	data, _ := txn.Serialize()

	// Corrupt the data (flip a bit in the middle)
	data[len(data)/2] ^= 0xFF

	if _, err := region.WriteAt(data); err != nil {
		t.Fatal(err)
	}
	if err := region.Sync(); err != nil {
		t.Fatal(err)
	}

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

func TestScanAndRecoverNeverInitialized(t *testing.T) {
	dev, cleanup := createTestDevice(t, DefaultJournalSlotSize)
	defer cleanup()

	result, err := ScanAndRecover(dev, 0, DefaultJournalSlotSize, nil)
	if result != nil || err != nil {
		t.Fatalf("ScanAndRecover on a zeroed region = %+v, %v; want nil, nil", result, err)
	}
}

func TestScanAndRecoverDamagedHeader(t *testing.T) {
	tests := []struct {
		name   string
		damage func(header []byte)
		want   error
	}{
		{"bad checksum", func(h []byte) { h[100] ^= 0xFF }, ErrJournalChecksumMismatch},
		{"bad magic", func(h []byte) { h[0] = 'X' }, ErrInvalidJournalMagic},
		{"bad geometry", func(h []byte) {
			binary.LittleEndian.PutUint32(h[92:96], 0)
			binary.LittleEndian.PutUint32(h[JournalSuperblockSize-4:], storage.CRC32C(h[:JournalSuperblockSize-4]))
		}, ErrJournalCorrupt},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dev, cleanup := createTestDevice(t, DefaultJournalSlotSize)
			defer cleanup()
			if _, err := InitRegion(dev, 0, DefaultJournalSlotSize, [16]byte{1}, 1); err != nil {
				t.Fatalf("InitRegion failed: %v", err)
			}
			damaged := damageJournalHeader(t, dev, 0, tt.damage)

			replayed := false
			_, err := ScanAndRecover(dev, 0, DefaultJournalSlotSize, func(*RecoveredTxn) error {
				replayed = true
				return nil
			})
			if !errors.Is(err, tt.want) {
				t.Fatalf("ScanAndRecover error = %v, want %v", err, tt.want)
			}
			if replayed {
				t.Error("ScanAndRecover replayed transactions from a damaged journal")
			}
			if !bytes.Equal(readJournalHeader(t, dev, 0), damaged) {
				t.Error("ScanAndRecover rewrote the damaged journal header")
			}
		})
	}
}

func readJournalHeader(t *testing.T, dev *storage.Device, offset int64) []byte {
	t.Helper()
	buf := storage.NewAlignedBuffer(storage.SectorSize)
	if _, err := dev.ReadAt(buf.Bytes(), offset); err != nil {
		t.Fatalf("read journal header: %v", err)
	}
	return buf.Bytes()
}

func damageJournalHeader(t *testing.T, dev *storage.Device, offset int64, damage func(header []byte)) []byte {
	t.Helper()
	header := readJournalHeader(t, dev, offset)
	damage(header)
	if _, err := dev.WriteAt(header, offset); err != nil {
		t.Fatalf("write journal header: %v", err)
	}
	return bytes.Clone(header)
}
