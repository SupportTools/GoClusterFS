package journal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/node"
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
	seq := nextSeq(region)
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
		seq := nextSeq(region)
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

	seq := nextSeq(region)
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
		seq := nextSeq(region)
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

func TestPendingCommitted(t *testing.T) {
	const size = uint64(DefaultJournalSlotSize)
	dev, cleanup := createTestDevice(t, int64(6*size))
	defer cleanup()
	uuid := [16]byte{1}
	journalAt := func(i int) uint64 { return uint64(i) * size }
	initAt := func(i int) *Region {
		t.Helper()
		region, err := InitRegion(dev, journalAt(i), size, uuid, 1)
		if err != nil {
			t.Fatal(err)
		}
		return region
	}
	write := func(region *Region, data []byte) {
		t.Helper()
		if _, err := region.WriteAt(data); err != nil {
			t.Fatal(err)
		}
		if err := region.Sync(); err != nil {
			t.Fatal(err)
		}
	}

	initAt(1)
	uncommitted := initAt(2)
	if err := uncommitted.MarkDirty(); err != nil {
		t.Fatal(err)
	}
	write(uncommitted, make([]byte, 100))
	j, err := Init(dev, journalAt(3), size, uuid, 1, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		txn := j.Begin()
		if err := txn.LogInodeCreate(&InodeCreatePayload{InodeNum: uint64(100 + i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := j.Commit(txn); err != nil {
			t.Fatal(err)
		}
	}
	initAt(4)
	damageJournalHeader(t, dev, int64(journalAt(4)), func(h []byte) { h[100] ^= 0xff })
	initAt(5)
	damageJournalHeader(t, dev, int64(journalAt(5)), func(h []byte) {
		binary.LittleEndian.PutUint32(h[40:44], uint32(JournalStateCorrupt))
		binary.LittleEndian.PutUint32(h[JournalSuperblockSize-4:], storage.CRC32C(h[:JournalSuperblockSize-4]))
	})

	for i, want := range []struct {
		name    string
		pending int
		err     error
	}{
		{"never initialized", 0, nil},
		{"clean", 0, nil},
		{"only an uncommitted transaction", 0, nil},
		{"two committed transactions", 2, nil},
		{"damaged header", 0, ErrJournalChecksumMismatch},
		{"corrupt state", 0, ErrJournalCorrupt},
	} {
		pending, err := PendingCommitted(dev, journalAt(i), size)
		if pending != want.pending || !errors.Is(err, want.err) || (want.err == nil) != (err == nil) {
			t.Errorf("%s: PendingCommitted = %d, %v; want %d, %v", want.name, pending, err, want.pending, want.err)
		}
	}
}

// A transaction with an op type this build does not define, as a newer build logs, stops replay before any of its
// entries apply; skipping it would leave that operation half applied.
func TestReplayRefusesUnknownOp(t *testing.T) {
	const size = uint64(DefaultJournalSlotSize)
	const bitmapAt = 2 * size
	ctx := context.Background()
	alloc := func(b uint64) func(*Transaction) error {
		return func(txn *Transaction) error {
			return txn.LogBlockAlloc(&BlockAllocPayload{InodeNum: 9, StartBlock: b, Count: 1})
		}
	}
	unknown := func(txn *Transaction) error { return txn.addEntry(TxnOpType(0x7777), []byte{1, 2, 3}) }
	newJournal := func(t *testing.T) (*storage.Device, *bitmap.Region) {
		t.Helper()
		dev, cleanup := createTestDevice(t, int64(bitmapAt+1<<20))
		t.Cleanup(cleanup)
		j, err := Init(dev, 0, size, [16]byte{1}, 1, DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		for _, logs := range [][]func(*Transaction) error{{alloc(10)}, {alloc(20), unknown}, {alloc(30)}} {
			txn := j.Begin()
			for _, log := range logs {
				if err := log(txn); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := j.Commit(txn); err != nil {
				t.Fatal(err)
			}
		}
		return dev, bitmap.NewRegion(dev, bitmapAt, 64, nil, [16]byte{})
	}
	allocated := func(t *testing.T, bm *bitmap.Region) string {
		t.Helper()
		var got []string
		for _, b := range []uint64{10, 20, 30} {
			on, err := bm.IsAllocated(b)
			if err != nil {
				t.Fatal(err)
			}
			if on {
				got = append(got, fmt.Sprint(b))
			}
		}
		return strings.Join(got, ",")
	}

	for name, replay := range map[string]func(*storage.Device, *bitmap.Region) error{
		"peer": func(dev *storage.Device, bm *bitmap.Region) error {
			info := &node.FailedNodeInfo{JournalOffset: 0, JournalSize: size}
			_, err := ReplayJournal(ctx, &RecoveryContext{Dev: dev, BitmapMgr: bm}, info)
			return err
		},
		"own": func(dev *storage.Device, bm *bitmap.Region) error {
			_, err := ScanAndRecover(dev, 0, size, CreateTxnRecoveryHandler(&RecoveryContext{Dev: dev, BitmapMgr: bm}), nil)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			dev, bm := newJournal(t)
			for attempt := range 2 {
				if err := replay(dev, bm); !errors.Is(err, ErrUnknownOp) || !strings.Contains(err.Error(), "unknown op 0x7777") {
					t.Fatalf("replay %d: %v, want ErrUnknownOp naming 0x7777", attempt, err)
				}
			}
			if got := allocated(t, bm); got != "10" {
				t.Errorf("blocks allocated after the refused replay: %q, want only the first transaction's 10", got)
			}
			region, err := OpenRegionForRecovery(dev, 0, size)
			if err != nil || !region.NeedsRecovery() {
				t.Errorf("journal after the refused replay: %v; want it still in recovery", err)
			}
		})
	}

	dev, _ := newJournal(t)
	region, err := OpenRegionForRecovery(dev, 0, size)
	if err != nil {
		t.Fatal(err)
	}
	if txns, _, err := DryRunRecover(region); len(txns) != 2 || !errors.Is(err, ErrUnknownOp) {
		t.Errorf("dry run: %d transactions, %v; want 2 and ErrUnknownOp", len(txns), err)
	}
	if _, err := EntryTouches(TxnOpType(0x7777), nil); !errors.Is(err, ErrUnknownOp) {
		t.Errorf("EntryTouches of an unknown op: %v, want ErrUnknownOp", err)
	}

	damageJournalHeader(t, dev, 0, func(h []byte) {
		binary.LittleEndian.PutUint32(h[12:16], JournalVersion+1)
		binary.LittleEndian.PutUint32(h[JournalSuperblockSize-4:], storage.CRC32C(h[:JournalSuperblockSize-4]))
	})
	info := &node.FailedNodeInfo{JournalOffset: 0, JournalSize: size}
	if _, err := ReplayJournal(ctx, &RecoveryContext{Dev: dev}, info); !errors.Is(err, ErrJournalTooNew) {
		t.Errorf("replay of a newer journal format: %v, want ErrJournalTooNew", err)
	}
	if region, err = OpenRegionForRecovery(dev, 0, size); err != nil {
		t.Fatal(err)
	}
	if _, _, err := DryRunRecover(region); !errors.Is(err, ErrJournalTooNew) {
		t.Errorf("dry run of a newer journal format: %v, want ErrJournalTooNew", err)
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

	seq := nextSeq(region)
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

	result, err := ScanAndRecover(dev, 0, DefaultJournalSlotSize, nil, nil)
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
			}, nil)
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

func TestRecoveryRefusesUnusableJournalState(t *testing.T) {
	for _, state := range []JournalState{JournalStateCorrupt, JournalState(9)} {
		t.Run(state.String(), func(t *testing.T) {
			dev, cleanup := createTestDevice(t, DefaultJournalSlotSize)
			defer cleanup()
			if _, err := InitRegion(dev, 0, DefaultJournalSlotSize, [16]byte{1}, 1); err != nil {
				t.Fatalf("InitRegion failed: %v", err)
			}
			marked := damageJournalHeader(t, dev, 0, func(h []byte) {
				binary.LittleEndian.PutUint32(h[40:44], uint32(state))
				binary.LittleEndian.PutUint32(h[JournalSuperblockSize-4:], storage.CRC32C(h[:JournalSuperblockSize-4]))
			})
			if _, err := ScanAndRecover(dev, 0, DefaultJournalSlotSize, func(*RecoveredTxn) error { return nil }, nil); !errors.Is(err, ErrJournalCorrupt) {
				t.Errorf("ScanAndRecover = %v, want ErrJournalCorrupt", err)
			}
			info := &node.FailedNodeInfo{JournalOffset: 0, JournalSize: DefaultJournalSlotSize}
			if _, err := ReplayJournal(context.Background(), &RecoveryContext{Dev: dev}, info); !errors.Is(err, ErrJournalCorrupt) {
				t.Errorf("ReplayJournal = %v, want ErrJournalCorrupt", err)
			}
			if !bytes.Equal(readJournalHeader(t, dev, 0), marked) {
				t.Error("recovery rewrote a journal it refused")
			}
		})
	}
}
