package journal

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// TxnMagic identifies a valid transaction record.
	// "TXNR" in little-endian: 0x524E5854
	TxnMagic uint32 = 0x524E5854

	// TxnHeaderSize is the fixed size of the transaction header (64 bytes).
	TxnHeaderSize = 64

	// TxnEntryHeaderSize is the size of the entry header (8 bytes).
	TxnEntryHeaderSize = 8
)

// TxnType represents the type of transaction record.
type TxnType uint32

const (
	// TxnTypeBegin marks the start of a transaction.
	TxnTypeBegin TxnType = 1

	// TxnTypeCommit marks a committed transaction.
	TxnTypeCommit TxnType = 2

	// TxnTypeAbort marks an aborted transaction.
	TxnTypeAbort TxnType = 3

	// TxnTypeCheckpoint marks a checkpoint record.
	TxnTypeCheckpoint TxnType = 4
)

// String returns a human-readable representation of the transaction type.
func (t TxnType) String() string {
	switch t {
	case TxnTypeBegin:
		return "Begin"
	case TxnTypeCommit:
		return "Commit"
	case TxnTypeAbort:
		return "Abort"
	case TxnTypeCheckpoint:
		return "Checkpoint"
	default:
		return "Unknown"
	}
}

// Transaction errors
var (
	ErrInvalidTxnMagic     = errors.New("invalid transaction magic")
	ErrTxnChecksumMismatch = errors.New("transaction checksum mismatch")
	ErrTxnTooSmall         = errors.New("buffer too small for transaction")
	ErrTxnTooLarge         = errors.New("transaction exceeds maximum size")
	ErrTxnAlreadyCommitted = errors.New("transaction already committed")
)

// TxnHeader is the on-disk header for each transaction record.
// Total size: 64 bytes.
//
// Layout:
//
//	Offset  Size  Field
//	------  ----  -----
//	0       4     Magic (0x524E5854 "TXNR")
//	4       4     Type (TxnType enum)
//	8       8     Seq (sequence number)
//	16      4     TotalLength (including header)
//	20      4     EntryCount
//	24      8     Timestamp (Unix nanoseconds)
//	32      16    OwnerNodeUUID
//	48      8     OwnerGeneration
//	56      4     Checksum (CRC32C of entire transaction)
//	60      4     Reserved
type TxnHeader struct {
	Magic           uint32
	Type            TxnType
	Seq             uint64
	TotalLength     uint32
	EntryCount      uint32
	Timestamp       int64
	OwnerNodeUUID   [16]byte
	OwnerGeneration uint64
	Checksum        uint32
	Reserved        uint32
}

// TxnEntry is the header for each operation entry within a transaction.
// The payload immediately follows this header.
//
// Layout:
//
//	Offset  Size  Field
//	------  ----  -----
//	0       4     OpType
//	4       4     Length (including this header)
type TxnEntry struct {
	OpType TxnOpType
	Length uint32
	// Payload follows immediately after
}

// Transaction is an in-memory representation of a transaction being built.
// Use NewTransaction to create one, then add entries and commit.
type Transaction struct {
	header  TxnHeader
	entries []TxnEntry
	data    []byte // Serialized entries including payloads

	committed bool
}

// NewTransaction creates a new transaction builder.
func NewTransaction(seq uint64, nodeUUID [16]byte, generation uint64) *Transaction {
	return &Transaction{
		header: TxnHeader{
			Magic:           TxnMagic,
			Type:            TxnTypeBegin,
			Seq:             seq,
			Timestamp:       time.Now().UnixNano(),
			OwnerNodeUUID:   nodeUUID,
			OwnerGeneration: generation,
		},
		entries: make([]TxnEntry, 0, 8),
		data:    make([]byte, 0, 256),
	}
}

// Seq returns the transaction's sequence number.
func (txn *Transaction) Seq() uint64 {
	return txn.header.Seq
}

// EntryCount returns the number of entries in the transaction.
func (txn *Transaction) EntryCount() uint32 {
	return uint32(len(txn.entries))
}

// addEntry adds a serialized entry to the transaction.
// The payload should already be serialized.
func (txn *Transaction) addEntry(opType TxnOpType, payload []byte) error {
	if txn.committed {
		return ErrTxnAlreadyCommitted
	}

	entryLen := uint32(TxnEntryHeaderSize + len(payload))

	entry := TxnEntry{
		OpType: opType,
		Length: entryLen,
	}
	txn.entries = append(txn.entries, entry)

	// Serialize entry header
	entryHeader := make([]byte, TxnEntryHeaderSize)
	binary.LittleEndian.PutUint32(entryHeader[0:4], uint32(opType))
	binary.LittleEndian.PutUint32(entryHeader[4:8], entryLen)

	txn.data = append(txn.data, entryHeader...)
	txn.data = append(txn.data, payload...)

	return nil
}

// TotalLength returns the total serialized size of the transaction.
func (txn *Transaction) TotalLength() uint32 {
	return uint32(TxnHeaderSize + len(txn.data))
}

// Serialize converts the transaction to bytes for writing to disk.
// Sets the type to TxnTypeCommit and computes checksum.
func (txn *Transaction) Serialize() ([]byte, error) {
	// Update header
	txn.header.Type = TxnTypeCommit
	txn.header.EntryCount = uint32(len(txn.entries))
	txn.header.TotalLength = txn.TotalLength()

	// Allocate buffer
	buf := make([]byte, txn.header.TotalLength)

	// Serialize header
	serializeTxnHeader(&txn.header, buf[:TxnHeaderSize])

	// Copy entry data
	copy(buf[TxnHeaderSize:], txn.data)

	// Compute and store checksum (over everything except checksum field)
	checksum := computeTxnChecksum(buf)
	binary.LittleEndian.PutUint32(buf[56:60], checksum)
	txn.header.Checksum = checksum

	txn.committed = true
	return buf, nil
}

// serializeTxnHeader writes the header to buf (must be at least TxnHeaderSize bytes).
func serializeTxnHeader(h *TxnHeader, buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], h.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(h.Type))
	binary.LittleEndian.PutUint64(buf[8:16], h.Seq)
	binary.LittleEndian.PutUint32(buf[16:20], h.TotalLength)
	binary.LittleEndian.PutUint32(buf[20:24], h.EntryCount)
	binary.LittleEndian.PutUint64(buf[24:32], uint64(h.Timestamp))
	copy(buf[32:48], h.OwnerNodeUUID[:])
	binary.LittleEndian.PutUint64(buf[48:56], h.OwnerGeneration)
	// Checksum at 56:60 is set later
	// Reserved at 60:64 is zero
}

// computeTxnChecksum computes CRC32C of transaction, excluding checksum field.
func computeTxnChecksum(buf []byte) uint32 {
	// Checksum is at offset 56-60, so we checksum [0:56] + [60:end]
	crc := storage.CRC32C(buf[:56])
	if len(buf) > 60 {
		// Continue CRC with data after checksum field
		crc = storage.CRC32CWithSeed(crc, buf[60:])
	}
	return crc
}

// DeserializeTxnHeader parses a transaction header from raw bytes.
// Does NOT verify checksum - caller must do that after reading full transaction.
func DeserializeTxnHeader(buf []byte) (*TxnHeader, error) {
	if len(buf) < TxnHeaderSize {
		return nil, ErrTxnTooSmall
	}

	h := &TxnHeader{
		Magic:           binary.LittleEndian.Uint32(buf[0:4]),
		Type:            TxnType(binary.LittleEndian.Uint32(buf[4:8])),
		Seq:             binary.LittleEndian.Uint64(buf[8:16]),
		TotalLength:     binary.LittleEndian.Uint32(buf[16:20]),
		EntryCount:      binary.LittleEndian.Uint32(buf[20:24]),
		Timestamp:       int64(binary.LittleEndian.Uint64(buf[24:32])),
		OwnerGeneration: binary.LittleEndian.Uint64(buf[48:56]),
		Checksum:        binary.LittleEndian.Uint32(buf[56:60]),
	}
	copy(h.OwnerNodeUUID[:], buf[32:48])

	if h.Magic != TxnMagic {
		return nil, ErrInvalidTxnMagic
	}

	return h, nil
}

// DeserializeTxnEntry parses a transaction entry header from raw bytes.
func DeserializeTxnEntry(buf []byte) (*TxnEntry, error) {
	if len(buf) < TxnEntryHeaderSize {
		return nil, ErrTxnTooSmall
	}

	return &TxnEntry{
		OpType: TxnOpType(binary.LittleEndian.Uint32(buf[0:4])),
		Length: binary.LittleEndian.Uint32(buf[4:8]),
	}, nil
}

// ValidateTxnChecksum verifies the checksum of a complete transaction buffer.
func ValidateTxnChecksum(buf []byte) error {
	if len(buf) < TxnHeaderSize {
		return ErrTxnTooSmall
	}

	storedChecksum := binary.LittleEndian.Uint32(buf[56:60])
	computedChecksum := computeTxnChecksum(buf)

	if storedChecksum != computedChecksum {
		return ErrTxnChecksumMismatch
	}
	return nil
}

// IterateEntries calls fn for each entry in the transaction.
// buf should be the complete serialized transaction.
func IterateEntries(buf []byte, fn func(entry *TxnEntry, payload []byte) error) error {
	if len(buf) < TxnHeaderSize {
		return ErrTxnTooSmall
	}

	header, err := DeserializeTxnHeader(buf)
	if err != nil {
		return err
	}

	offset := TxnHeaderSize
	for i := uint32(0); i < header.EntryCount; i++ {
		if offset+TxnEntryHeaderSize > len(buf) {
			return ErrTxnTooSmall
		}

		entry, err := DeserializeTxnEntry(buf[offset:])
		if err != nil {
			return err
		}

		if offset+int(entry.Length) > len(buf) {
			return ErrTxnTooSmall
		}

		payloadStart := offset + TxnEntryHeaderSize
		payloadEnd := offset + int(entry.Length)

		if err := fn(entry, buf[payloadStart:payloadEnd]); err != nil {
			return err
		}

		offset += int(entry.Length)
	}

	return nil
}
