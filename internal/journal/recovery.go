package journal

import (
	"errors"
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// Recovery errors
var (
	ErrRecoveryFailed     = errors.New("journal recovery failed")
	ErrCorruptTxn         = errors.New("corrupt transaction found")
	ErrUnexpectedEnd      = errors.New("unexpected end of journal data")
	ErrRecoveryInProgress = errors.New("another recovery already in progress")
)

// RecoveredTxn represents a transaction found during recovery.
type RecoveredTxn struct {
	Header  *TxnHeader
	Entries []*RecoveredEntry
	RawData []byte
}

// RecoveredEntry represents an entry from a recovered transaction.
type RecoveredEntry struct {
	Entry   *TxnEntry
	Payload []byte
}

// RecoveryStats tracks statistics about the recovery process.
type RecoveryStats struct {
	TxnScanned      int
	TxnCommitted    int
	TxnIncomplete   int
	EntriesReplayed int
	BytesScanned    uint32
	Errors          int
}

// RecoveryHandler is called for each committed transaction during replay.
// Implementations should apply the transaction idempotently.
// Return nil to continue, or an error to abort recovery.
type RecoveryHandler func(txn *RecoveredTxn) error

// Recover performs journal recovery on a region.
// It scans from head to tail and calls the handler for each committed transaction.
//
// The recovery protocol:
// 1. Check if recovery is needed (State != Clean)
// 2. Mark state as Recovering
// 3. Scan from head to tail
// 4. For each committed transaction, call handler
// 5. On success, mark state as Clean and reset pointers
func Recover(region *Region, handler RecoveryHandler) (*RecoveryStats, error) {
	if !region.NeedsRecovery() {
		return &RecoveryStats{}, nil
	}

	// Prevent concurrent recovery
	state := region.State()
	if state == JournalStateRecovering {
		return nil, ErrRecoveryInProgress
	}

	// Mark as recovering
	if err := region.MarkRecovering(); err != nil {
		return nil, fmt.Errorf("failed to mark journal as recovering: %w", err)
	}

	stats := &RecoveryStats{}

	// Scan and recover
	err := scanJournal(region, handler, stats)
	if err != nil {
		// Recovery failed - leave in recovering state for manual intervention
		return stats, fmt.Errorf("recovery failed: %w", err)
	}

	// Success - reset the journal
	if err := region.Reset(); err != nil {
		return stats, fmt.Errorf("failed to reset journal after recovery: %w", err)
	}

	return stats, nil
}

// scanJournal scans the journal from head to tail, recovering transactions.
func scanJournal(region *Region, handler RecoveryHandler, stats *RecoveryStats) error {
	sb := region.Superblock()
	head := sb.HeadOffset
	tail := sb.TailOffset
	dataAreaSize := sb.DataAreaSize

	// Handle empty journal
	if head == tail {
		return nil
	}

	pos := head
	for {
		// Check if we've reached the tail
		if pos == tail {
			break
		}

		// Handle wrap-around
		if pos >= dataAreaSize {
			pos = 0
		}

		// Calculate bytes remaining to scan
		var remaining uint32
		if tail > pos {
			remaining = tail - pos
		} else {
			remaining = dataAreaSize - pos + tail
		}

		if remaining < TxnHeaderSize {
			// Not enough data for a transaction header
			stats.TxnIncomplete++
			break
		}

		// Read at least one sector to get the header
		// (pos is sector-aligned because we advance by alignedLen)
		initialReadSize := storage.SectorSize
		if remaining < uint32(initialReadSize) {
			initialReadSize = int(remaining)
		}
		initialBuf := make([]byte, initialReadSize)
		n, err := region.ReadAt(initialBuf, pos)
		if err != nil {
			stats.Errors++
			return fmt.Errorf("failed to read txn header at %d: %w", pos, err)
		}
		if n < TxnHeaderSize {
			stats.TxnIncomplete++
			break
		}

		// Parse header
		txnHeader, err := DeserializeTxnHeader(initialBuf[:TxnHeaderSize])
		if err != nil {
			// Invalid header - could be garbage or partial write
			// In a production system, we might try to scan forward for next valid header
			stats.TxnIncomplete++
			break
		}

		stats.TxnScanned++

		// Validate transaction length
		if txnHeader.TotalLength < TxnHeaderSize || txnHeader.TotalLength > remaining {
			stats.TxnIncomplete++
			break
		}

		// Read full transaction if it spans multiple sectors
		var txnBuf []byte
		if txnHeader.TotalLength <= uint32(initialReadSize) {
			// Transaction fits in initial read
			txnBuf = initialBuf[:txnHeader.TotalLength]
		} else {
			// Need to read more - do one aligned read from pos for the whole txn
			alignedTxnLen := ((txnHeader.TotalLength + storage.SectorSize - 1) / storage.SectorSize) * storage.SectorSize
			txnBuf = make([]byte, alignedTxnLen)
			n, err = region.ReadAt(txnBuf, pos)
			if err != nil || uint32(n) < txnHeader.TotalLength {
				stats.Errors++
				return fmt.Errorf("failed to read full txn at %d: %w", pos, err)
			}
			txnBuf = txnBuf[:txnHeader.TotalLength]
		}

		// Validate checksum
		if err := ValidateTxnChecksum(txnBuf); err != nil {
			// Checksum mismatch - transaction was not fully committed
			stats.TxnIncomplete++
			break
		}

		// Only replay committed transactions
		if txnHeader.Type == TxnTypeCommit {
			stats.TxnCommitted++

			// Parse entries
			recoveredTxn, err := parseTxnEntries(txnHeader, txnBuf)
			if err != nil {
				stats.Errors++
				return fmt.Errorf("failed to parse txn entries: %w", err)
			}

			// Call recovery handler
			if handler != nil {
				if err := handler(recoveredTxn); err != nil {
					stats.Errors++
					return fmt.Errorf("handler failed for txn %d: %w", txnHeader.Seq, err)
				}
			}

			stats.EntriesReplayed += len(recoveredTxn.Entries)
		}

		// Advance position (account for sector alignment)
		alignedLen := ((txnHeader.TotalLength + storage.SectorSize - 1) / storage.SectorSize) * storage.SectorSize
		pos = pos + alignedLen
		stats.BytesScanned += alignedLen

		// Handle wrap-around
		if pos >= dataAreaSize {
			pos = pos - dataAreaSize
		}
	}

	return nil
}

// parseTxnEntries parses all entries from a transaction buffer.
func parseTxnEntries(header *TxnHeader, buf []byte) (*RecoveredTxn, error) {
	txn := &RecoveredTxn{
		Header:  header,
		Entries: make([]*RecoveredEntry, 0, header.EntryCount),
		RawData: buf,
	}

	offset := TxnHeaderSize
	for i := uint32(0); i < header.EntryCount; i++ {
		if offset+TxnEntryHeaderSize > len(buf) {
			return nil, ErrCorruptTxn
		}

		entry, err := DeserializeTxnEntry(buf[offset:])
		if err != nil {
			return nil, err
		}

		if offset+int(entry.Length) > len(buf) {
			return nil, ErrCorruptTxn
		}

		payloadStart := offset + TxnEntryHeaderSize
		payloadEnd := offset + int(entry.Length)

		txn.Entries = append(txn.Entries, &RecoveredEntry{
			Entry:   entry,
			Payload: buf[payloadStart:payloadEnd],
		})

		offset += int(entry.Length)
	}

	return txn, nil
}

// MarkRecovering sets the journal state to Recovering.
// This is exposed on Region for the recovery protocol.
func (r *Region) MarkRecovering() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.superblock.State == JournalStateRecovering {
		return nil // Already recovering
	}

	r.superblock.State = JournalStateRecovering
	return r.writeSuperblock() // Use unlocked helper since we hold the lock
}

// RecoverPrint is a simple handler that prints recovered transactions.
// Useful for debugging and testing.
func RecoverPrint(txn *RecoveredTxn) error {
	fmt.Printf("Recovered txn seq=%d entries=%d\n", txn.Header.Seq, len(txn.Entries))
	for i, entry := range txn.Entries {
		fmt.Printf("  [%d] op=%s len=%d\n", i, entry.Entry.OpType, entry.Entry.Length)
	}
	return nil
}

// DryRunRecover scans the journal without modifying state.
// Returns the list of transactions that would be replayed.
func DryRunRecover(region *Region) ([]*RecoveredTxn, *RecoveryStats, error) {
	if !region.NeedsRecovery() {
		return nil, &RecoveryStats{}, nil
	}

	var txns []*RecoveredTxn
	stats := &RecoveryStats{}

	handler := func(txn *RecoveredTxn) error {
		txns = append(txns, txn)
		return nil
	}

	// Don't mark as recovering for dry run
	if err := scanJournal(region, handler, stats); err != nil {
		return nil, stats, err
	}

	return txns, stats, nil
}
