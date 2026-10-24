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
	TxnScanned        int
	TxnCommitted      int
	TxnIncomplete     int
	EntriesReplayed   int
	BytesScanned      uint32
	Errors            int
	IntentCompletions int // Number of intent completion records found
	IntentsSkipped    int // Number of intents skipped (already completed)
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

// scanForCompletions scans the journal for IntentCompletion records.
// This is Phase 1 of the two-phase recovery protocol.
// Records are identified by IntentCompletionMagic (0x43544E49 "INTC").
func scanForCompletions(region *Region, head, tail, dataAreaSize uint32, completed map[uint64]bool, stats *RecoveryStats) error {
	pos := head
	for pos != tail {
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

		if remaining < 4 {
			// Not enough data for even a magic number
			break
		}

		// Read one sector to examine the magic number
		sectorBuf := make([]byte, storage.SectorSize)
		n, err := region.ReadAt(sectorBuf, pos)
		if err != nil || n < 4 {
			break
		}

		// Check magic number to determine record type
		magic := uint32(sectorBuf[0]) | uint32(sectorBuf[1])<<8 | uint32(sectorBuf[2])<<16 | uint32(sectorBuf[3])<<24

		if magic == IntentCompletionMagic {
			// This is an IntentCompletion record
			ic, err := DeserializeIntentCompletion(sectorBuf)
			if err == nil {
				completed[ic.IntentSeq] = true
				stats.IntentCompletions++
			}
			// Advance by one aligned sector (IntentCompletion is padded to 512 bytes)
			pos += storage.SectorSize
		} else if magic == TxnMagic {
			// This is a transaction - skip it (we'll process in phase 2)
			header, err := DeserializeTxnHeader(sectorBuf[:TxnHeaderSize])
			if err != nil || header.TotalLength < TxnHeaderSize || header.TotalLength > remaining {
				break
			}
			// Advance by aligned transaction length
			alignedLen := ((header.TotalLength + storage.SectorSize - 1) / storage.SectorSize) * storage.SectorSize
			pos += alignedLen
		} else {
			// Unknown record type - corruption or end of valid data
			break
		}

		// Handle wrap-around
		if pos >= dataAreaSize {
			pos = pos - dataAreaSize
		}
	}

	return nil
}

// scanJournal scans the journal from head to tail, recovering transactions.
// It implements the two-phase intent logging recovery protocol:
// Phase 1: Scan for IntentCompletion records to find completed intents
// Phase 2: Replay only transactions without matching completion records
func scanJournal(region *Region, handler RecoveryHandler, stats *RecoveryStats) error {
	sb := region.Superblock()
	head := sb.HeadOffset
	tail := sb.TailOffset
	dataAreaSize := sb.DataAreaSize

	// Handle empty journal
	if head == tail {
		return nil
	}

	// Phase 1: Scan for intent completion records
	completedIntents := make(map[uint64]bool)
	if err := scanForCompletions(region, head, tail, dataAreaSize, completedIntents, stats); err != nil {
		return fmt.Errorf("phase 1 completion scan failed: %w", err)
	}

	// Phase 2: Replay transactions that are not completed
	pos := head
	for pos != tail {
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

		// Check magic number to determine record type
		magic := uint32(initialBuf[0]) | uint32(initialBuf[1])<<8 | uint32(initialBuf[2])<<16 | uint32(initialBuf[3])<<24

		// Skip IntentCompletion records (already processed in Phase 1)
		if magic == IntentCompletionMagic {
			pos += storage.SectorSize
			if pos >= dataAreaSize {
				pos = pos - dataAreaSize
			}
			continue
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

			// Check if this intent has already been completed
			// (has a matching IntentCompletion record)
			if completedIntents[txnHeader.Seq] {
				stats.IntentsSkipped++
				// Skip to next record without replaying
				alignedLen := ((txnHeader.TotalLength + storage.SectorSize - 1) / storage.SectorSize) * storage.SectorSize
				pos = pos + alignedLen
				stats.BytesScanned += alignedLen
				if pos >= dataAreaSize {
					pos = pos - dataAreaSize
				}
				continue
			}

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
		Entries: make([]*RecoveredEntry, 0, min(header.EntryCount, uint32(len(buf)/TxnEntryHeaderSize))),
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

		if entry.Length < TxnEntryHeaderSize || offset+int(entry.Length) > len(buf) {
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
