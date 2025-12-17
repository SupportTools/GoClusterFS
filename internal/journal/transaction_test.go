package journal

import (
	"testing"
)

func TestTxnTypeString(t *testing.T) {
	tests := []struct {
		txnType TxnType
		want    string
	}{
		{TxnTypeBegin, "Begin"},
		{TxnTypeCommit, "Commit"},
		{TxnTypeAbort, "Abort"},
		{TxnTypeCheckpoint, "Checkpoint"},
		{TxnType(99), "Unknown"},
	}

	for _, tt := range tests {
		got := tt.txnType.String()
		if got != tt.want {
			t.Errorf("TxnType(%d).String() = %q, want %q", tt.txnType, got, tt.want)
		}
	}
}

func TestTxnOpTypeString(t *testing.T) {
	tests := []struct {
		opType TxnOpType
		want   string
	}{
		{TxnOpInodeCreate, "InodeCreate"},
		{TxnOpInodeUpdate, "InodeUpdate"},
		{TxnOpInodeDelete, "InodeDelete"},
		{TxnOpDirAddEntry, "DirAddEntry"},
		{TxnOpDirRemoveEntry, "DirRemoveEntry"},
		{TxnOpDirRename, "DirRename"},
		{TxnOpBlockAlloc, "BlockAlloc"},
		{TxnOpBlockFree, "BlockFree"},
		{TxnOpExtentAdd, "ExtentAdd"},
		{TxnOpExtentRemove, "ExtentRemove"},
		{TxnOpSnapshotCreate, "SnapshotCreate"},
		{TxnOpSnapshotDelete, "SnapshotDelete"},
		{TxnOpRefCountUpdate, "RefCountUpdate"},
		{TxnOpType(0xFFFF), "Unknown"},
	}

	for _, tt := range tests {
		got := tt.opType.String()
		if got != tt.want {
			t.Errorf("TxnOpType(0x%04x).String() = %q, want %q", tt.opType, got, tt.want)
		}
	}
}

func TestNewTransaction(t *testing.T) {
	nodeUUID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	generation := uint64(42)
	seq := uint64(100)

	txn := NewTransaction(seq, nodeUUID, generation)

	if txn.Seq() != seq {
		t.Errorf("Seq() = %d, want %d", txn.Seq(), seq)
	}

	if txn.EntryCount() != 0 {
		t.Errorf("EntryCount() = %d, want 0 for new txn", txn.EntryCount())
	}

	// Header size only for empty transaction
	if txn.TotalLength() != TxnHeaderSize {
		t.Errorf("TotalLength() = %d, want %d", txn.TotalLength(), TxnHeaderSize)
	}
}

func TestTransactionAddEntries(t *testing.T) {
	nodeUUID := [16]byte{1, 2, 3, 4}
	txn := NewTransaction(1, nodeUUID, 1)

	// Add an inode create entry
	err := txn.LogInodeCreate(&InodeCreatePayload{
		InodeNum:       100,
		Mode:           0644,
		UID:            1000,
		GID:            1000,
		ParentInodeNum: 2,
		NameHash:       0xDEADBEEF,
	})
	if err != nil {
		t.Fatalf("LogInodeCreate failed: %v", err)
	}

	if txn.EntryCount() != 1 {
		t.Errorf("EntryCount() = %d, want 1", txn.EntryCount())
	}

	// Add a directory entry
	err = txn.LogDirAddEntry(&DirAddEntryPayload{
		ParentInodeNum: 2,
		ChildInodeNum:  100,
		EntryType:      DirEntryRegular,
		Name:           "test.txt",
	})
	if err != nil {
		t.Fatalf("LogDirAddEntry failed: %v", err)
	}

	if txn.EntryCount() != 2 {
		t.Errorf("EntryCount() = %d, want 2", txn.EntryCount())
	}

	// Total length should have increased
	// Header (64) + Entry1 (8+40) + Entry2 (8+24+8) = 64 + 48 + 40 = 152
	// Note: "test.txt" is 8 bytes, already 4-byte aligned
	expectedLen := uint32(TxnHeaderSize + TxnEntryHeaderSize + 40 + TxnEntryHeaderSize + 24 + 8)
	if txn.TotalLength() != expectedLen {
		t.Errorf("TotalLength() = %d, want %d", txn.TotalLength(), expectedLen)
	}
}

func TestTransactionSerializeRoundTrip(t *testing.T) {
	nodeUUID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	generation := uint64(42)
	seq := uint64(100)

	// Create transaction with entries
	txn := NewTransaction(seq, nodeUUID, generation)

	err := txn.LogInodeCreate(&InodeCreatePayload{
		InodeNum:       123,
		Mode:           0755,
		UID:            500,
		GID:            500,
		ParentInodeNum: 2,
		NameHash:       0x12345678,
	})
	if err != nil {
		t.Fatalf("LogInodeCreate failed: %v", err)
	}

	err = txn.LogBlockAlloc(&BlockAllocPayload{
		InodeNum:   123,
		StartBlock: 1000,
		Count:      5,
		Purpose:    BlockPurposeData,
	})
	if err != nil {
		t.Fatalf("LogBlockAlloc failed: %v", err)
	}

	// Serialize
	data, err := txn.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Verify checksum
	if err := ValidateTxnChecksum(data); err != nil {
		t.Errorf("ValidateTxnChecksum failed: %v", err)
	}

	// Deserialize header
	header, err := DeserializeTxnHeader(data)
	if err != nil {
		t.Fatalf("DeserializeTxnHeader failed: %v", err)
	}

	// Verify header fields
	if header.Magic != TxnMagic {
		t.Errorf("Magic = 0x%08x, want 0x%08x", header.Magic, TxnMagic)
	}
	if header.Type != TxnTypeCommit {
		t.Errorf("Type = %v, want Commit", header.Type)
	}
	if header.Seq != seq {
		t.Errorf("Seq = %d, want %d", header.Seq, seq)
	}
	if header.EntryCount != 2 {
		t.Errorf("EntryCount = %d, want 2", header.EntryCount)
	}
	if header.OwnerNodeUUID != nodeUUID {
		t.Error("OwnerNodeUUID mismatch")
	}
	if header.OwnerGeneration != generation {
		t.Errorf("OwnerGeneration = %d, want %d", header.OwnerGeneration, generation)
	}
	if header.TotalLength != uint32(len(data)) {
		t.Errorf("TotalLength = %d, want %d", header.TotalLength, len(data))
	}
}

func TestTransactionIterateEntries(t *testing.T) {
	nodeUUID := [16]byte{1, 2, 3, 4}
	txn := NewTransaction(1, nodeUUID, 1)

	// Add multiple entries
	txn.LogInodeCreate(&InodeCreatePayload{InodeNum: 100})
	txn.LogDirAddEntry(&DirAddEntryPayload{ParentInodeNum: 2, ChildInodeNum: 100, Name: "file"})
	txn.LogBlockAlloc(&BlockAllocPayload{InodeNum: 100, StartBlock: 50, Count: 1})

	data, err := txn.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Iterate and collect entries
	var ops []TxnOpType
	err = IterateEntries(data, func(entry *TxnEntry, payload []byte) error {
		ops = append(ops, entry.OpType)
		return nil
	})
	if err != nil {
		t.Fatalf("IterateEntries failed: %v", err)
	}

	expected := []TxnOpType{TxnOpInodeCreate, TxnOpDirAddEntry, TxnOpBlockAlloc}
	if len(ops) != len(expected) {
		t.Fatalf("Got %d entries, want %d", len(ops), len(expected))
	}
	for i, op := range ops {
		if op != expected[i] {
			t.Errorf("Entry %d: got %v, want %v", i, op, expected[i])
		}
	}
}

func TestTransactionChecksumValidation(t *testing.T) {
	nodeUUID := [16]byte{1, 2, 3, 4}
	txn := NewTransaction(1, nodeUUID, 1)
	txn.LogInodeDelete(&InodeDeletePayload{InodeNum: 50})

	data, err := txn.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Corrupt a byte
	data[40] ^= 0xFF

	err = ValidateTxnChecksum(data)
	if err != ErrTxnChecksumMismatch {
		t.Errorf("Expected ErrTxnChecksumMismatch, got %v", err)
	}
}

func TestTransactionInvalidMagic(t *testing.T) {
	data := make([]byte, TxnHeaderSize)
	// No magic set

	_, err := DeserializeTxnHeader(data)
	if err != ErrInvalidTxnMagic {
		t.Errorf("Expected ErrInvalidTxnMagic, got %v", err)
	}
}

func TestTransactionTooSmall(t *testing.T) {
	data := make([]byte, 10) // Too small

	_, err := DeserializeTxnHeader(data)
	if err != ErrTxnTooSmall {
		t.Errorf("Expected ErrTxnTooSmall, got %v", err)
	}
}

func TestTransactionAlreadyCommitted(t *testing.T) {
	nodeUUID := [16]byte{1, 2, 3, 4}
	txn := NewTransaction(1, nodeUUID, 1)

	// Commit it
	_, err := txn.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Try to add more entries after commit
	err = txn.LogInodeDelete(&InodeDeletePayload{InodeNum: 1})
	if err != ErrTxnAlreadyCommitted {
		t.Errorf("Expected ErrTxnAlreadyCommitted, got %v", err)
	}
}

// ============================================================================
// Payload Round-Trip Tests
// ============================================================================

func TestInodeCreatePayloadRoundTrip(t *testing.T) {
	original := &InodeCreatePayload{
		InodeNum:       12345,
		Mode:           0755,
		UID:            1000,
		GID:            1001,
		ParentInodeNum: 2,
		NameHash:       0xDEADBEEFCAFEBABE,
	}

	data := original.Serialize()
	restored, err := DeserializeInodeCreate(data)
	if err != nil {
		t.Fatalf("DeserializeInodeCreate failed: %v", err)
	}

	if restored.InodeNum != original.InodeNum {
		t.Errorf("InodeNum = %d, want %d", restored.InodeNum, original.InodeNum)
	}
	if restored.Mode != original.Mode {
		t.Errorf("Mode = 0%o, want 0%o", restored.Mode, original.Mode)
	}
	if restored.UID != original.UID {
		t.Errorf("UID = %d, want %d", restored.UID, original.UID)
	}
	if restored.GID != original.GID {
		t.Errorf("GID = %d, want %d", restored.GID, original.GID)
	}
	if restored.ParentInodeNum != original.ParentInodeNum {
		t.Errorf("ParentInodeNum = %d, want %d", restored.ParentInodeNum, original.ParentInodeNum)
	}
	if restored.NameHash != original.NameHash {
		t.Errorf("NameHash = 0x%x, want 0x%x", restored.NameHash, original.NameHash)
	}
}

func TestDirAddEntryPayloadRoundTrip(t *testing.T) {
	original := &DirAddEntryPayload{
		ParentInodeNum: 100,
		ChildInodeNum:  200,
		EntryType:      DirEntryRegular,
		Name:           "testfile.txt",
	}

	data := original.Serialize()
	restored, err := DeserializeDirAddEntry(data)
	if err != nil {
		t.Fatalf("DeserializeDirAddEntry failed: %v", err)
	}

	if restored.ParentInodeNum != original.ParentInodeNum {
		t.Errorf("ParentInodeNum = %d, want %d", restored.ParentInodeNum, original.ParentInodeNum)
	}
	if restored.ChildInodeNum != original.ChildInodeNum {
		t.Errorf("ChildInodeNum = %d, want %d", restored.ChildInodeNum, original.ChildInodeNum)
	}
	if restored.EntryType != original.EntryType {
		t.Errorf("EntryType = %d, want %d", restored.EntryType, original.EntryType)
	}
	if restored.Name != original.Name {
		t.Errorf("Name = %q, want %q", restored.Name, original.Name)
	}
}

func TestDirRenamePayloadRoundTrip(t *testing.T) {
	original := &DirRenamePayload{
		OldParentInodeNum: 10,
		NewParentInodeNum: 20,
		InodeNum:          100,
		EntryType:         DirEntryDirectory,
		OldName:           "oldname",
		NewName:           "newname",
	}

	data := original.Serialize()
	restored, err := DeserializeDirRename(data)
	if err != nil {
		t.Fatalf("DeserializeDirRename failed: %v", err)
	}

	if restored.OldParentInodeNum != original.OldParentInodeNum {
		t.Errorf("OldParentInodeNum = %d, want %d", restored.OldParentInodeNum, original.OldParentInodeNum)
	}
	if restored.NewParentInodeNum != original.NewParentInodeNum {
		t.Errorf("NewParentInodeNum = %d, want %d", restored.NewParentInodeNum, original.NewParentInodeNum)
	}
	if restored.InodeNum != original.InodeNum {
		t.Errorf("InodeNum = %d, want %d", restored.InodeNum, original.InodeNum)
	}
	if restored.OldName != original.OldName {
		t.Errorf("OldName = %q, want %q", restored.OldName, original.OldName)
	}
	if restored.NewName != original.NewName {
		t.Errorf("NewName = %q, want %q", restored.NewName, original.NewName)
	}
}

func TestBlockAllocPayloadRoundTrip(t *testing.T) {
	original := &BlockAllocPayload{
		InodeNum:   500,
		StartBlock: 1000000,
		Count:      16,
		Purpose:    BlockPurposeData,
	}

	data := original.Serialize()
	restored, err := DeserializeBlockAlloc(data)
	if err != nil {
		t.Fatalf("DeserializeBlockAlloc failed: %v", err)
	}

	if restored.InodeNum != original.InodeNum {
		t.Errorf("InodeNum = %d, want %d", restored.InodeNum, original.InodeNum)
	}
	if restored.StartBlock != original.StartBlock {
		t.Errorf("StartBlock = %d, want %d", restored.StartBlock, original.StartBlock)
	}
	if restored.Count != original.Count {
		t.Errorf("Count = %d, want %d", restored.Count, original.Count)
	}
	if restored.Purpose != original.Purpose {
		t.Errorf("Purpose = %d, want %d", restored.Purpose, original.Purpose)
	}
}

func TestExtentAddPayloadRoundTrip(t *testing.T) {
	original := &ExtentAddPayload{
		InodeNum:  300,
		FileBlock: 0,
		DiskBlock: 50000,
		Length:    128,
		Flags:     ExtentFlagWritten | ExtentFlagShared,
	}

	data := original.Serialize()
	restored, err := DeserializeExtentAdd(data)
	if err != nil {
		t.Fatalf("DeserializeExtentAdd failed: %v", err)
	}

	if restored.InodeNum != original.InodeNum {
		t.Errorf("InodeNum = %d, want %d", restored.InodeNum, original.InodeNum)
	}
	if restored.FileBlock != original.FileBlock {
		t.Errorf("FileBlock = %d, want %d", restored.FileBlock, original.FileBlock)
	}
	if restored.DiskBlock != original.DiskBlock {
		t.Errorf("DiskBlock = %d, want %d", restored.DiskBlock, original.DiskBlock)
	}
	if restored.Length != original.Length {
		t.Errorf("Length = %d, want %d", restored.Length, original.Length)
	}
	if restored.Flags != original.Flags {
		t.Errorf("Flags = 0x%x, want 0x%x", restored.Flags, original.Flags)
	}
}

func TestSnapshotCreatePayloadRoundTrip(t *testing.T) {
	original := &SnapshotCreatePayload{
		SnapshotID:   5,
		ParentID:     3,
		RootInodeNum: 2,
		Timestamp:    1700000000,
	}

	data := original.Serialize()
	restored, err := DeserializeSnapshotCreate(data)
	if err != nil {
		t.Fatalf("DeserializeSnapshotCreate failed: %v", err)
	}

	if restored.SnapshotID != original.SnapshotID {
		t.Errorf("SnapshotID = %d, want %d", restored.SnapshotID, original.SnapshotID)
	}
	if restored.ParentID != original.ParentID {
		t.Errorf("ParentID = %d, want %d", restored.ParentID, original.ParentID)
	}
	if restored.RootInodeNum != original.RootInodeNum {
		t.Errorf("RootInodeNum = %d, want %d", restored.RootInodeNum, original.RootInodeNum)
	}
	if restored.Timestamp != original.Timestamp {
		t.Errorf("Timestamp = %d, want %d", restored.Timestamp, original.Timestamp)
	}
}

func TestRefCountUpdatePayloadRoundTrip(t *testing.T) {
	original := &RefCountUpdatePayload{
		BlockNum: 999999,
		OldCount: 1,
		NewCount: 2,
	}

	data := original.Serialize()
	restored, err := DeserializeRefCountUpdate(data)
	if err != nil {
		t.Fatalf("DeserializeRefCountUpdate failed: %v", err)
	}

	if restored.BlockNum != original.BlockNum {
		t.Errorf("BlockNum = %d, want %d", restored.BlockNum, original.BlockNum)
	}
	if restored.OldCount != original.OldCount {
		t.Errorf("OldCount = %d, want %d", restored.OldCount, original.OldCount)
	}
	if restored.NewCount != original.NewCount {
		t.Errorf("NewCount = %d, want %d", restored.NewCount, original.NewCount)
	}
}

// ============================================================================
// Edge Cases
// ============================================================================

func TestDirEntryLongName(t *testing.T) {
	// Test with max-length name (255 bytes)
	longName := make([]byte, 255)
	for i := range longName {
		longName[i] = 'a' + byte(i%26)
	}

	original := &DirAddEntryPayload{
		ParentInodeNum: 1,
		ChildInodeNum:  2,
		EntryType:      DirEntryRegular,
		Name:           string(longName),
	}

	data := original.Serialize()
	restored, err := DeserializeDirAddEntry(data)
	if err != nil {
		t.Fatalf("DeserializeDirAddEntry failed: %v", err)
	}

	if restored.Name != original.Name {
		t.Errorf("Long name not preserved correctly")
	}
}

func TestEmptyTransaction(t *testing.T) {
	nodeUUID := [16]byte{1, 2, 3, 4}
	txn := NewTransaction(1, nodeUUID, 1)

	data, err := txn.Serialize()
	if err != nil {
		t.Fatalf("Serialize empty txn failed: %v", err)
	}

	if len(data) != TxnHeaderSize {
		t.Errorf("Empty txn size = %d, want %d", len(data), TxnHeaderSize)
	}

	header, err := DeserializeTxnHeader(data)
	if err != nil {
		t.Fatalf("DeserializeTxnHeader failed: %v", err)
	}

	if header.EntryCount != 0 {
		t.Errorf("EntryCount = %d, want 0", header.EntryCount)
	}
}

func TestMultipleOperationsInTransaction(t *testing.T) {
	nodeUUID := [16]byte{1, 2, 3, 4}
	txn := NewTransaction(1, nodeUUID, 1)

	// Simulate file creation: inode create + dir entry + block alloc + extent add
	txn.LogInodeCreate(&InodeCreatePayload{InodeNum: 100, Mode: 0644, ParentInodeNum: 2})
	txn.LogDirAddEntry(&DirAddEntryPayload{ParentInodeNum: 2, ChildInodeNum: 100, Name: "newfile"})
	txn.LogBlockAlloc(&BlockAllocPayload{InodeNum: 100, StartBlock: 500, Count: 1})
	txn.LogExtentAdd(&ExtentAddPayload{InodeNum: 100, FileBlock: 0, DiskBlock: 500, Length: 1})

	if txn.EntryCount() != 4 {
		t.Errorf("EntryCount = %d, want 4", txn.EntryCount())
	}

	data, err := txn.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Verify checksum
	if err := ValidateTxnChecksum(data); err != nil {
		t.Fatalf("Checksum validation failed: %v", err)
	}

	// Parse entries and verify
	count := 0
	err = IterateEntries(data, func(entry *TxnEntry, payload []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("IterateEntries failed: %v", err)
	}
	if count != 4 {
		t.Errorf("Iterated %d entries, want 4", count)
	}
}
