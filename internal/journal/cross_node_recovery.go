// Package journal cross-node recovery implementation.
//
// This file implements journal replay for failed nodes in a cluster.
// When a node fails, another node can replay its journal to recover
// any committed but not yet checkpointed transactions.
package journal

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// RecoveryContext holds the dependencies needed for journal replay.
type RecoveryContext struct {
	Dev       *storage.Device
	InodeTbl  *inode.Table
	BitmapMgr *bitmap.Region
	DirMgr    *dir.Manager
	ExtentMgr *extent.Manager
}

// RecoveryResult contains statistics about the recovery operation.
type RecoveryResult struct {
	TxnReplayed     int
	EntriesReplayed int
	EntriesSkipped  int // Already applied (idempotent check)
	Errors          []error
	Duration        time.Duration
}

// ReplayJournal replays the journal of a failed node.
//
// This function opens the failed node's journal region, scans for committed
// transactions, and applies each operation idempotently. The idempotency
// ensures that replaying the same journal multiple times is safe.
//
// Returns RecoveryResult with statistics about the recovery operation.
func ReplayJournal(ctx context.Context, rctx *RecoveryContext, info *node.FailedNodeInfo) (*RecoveryResult, error) {
	if rctx == nil {
		return nil, fmt.Errorf("recovery context is nil")
	}
	if info == nil {
		return nil, fmt.Errorf("failed node info is nil")
	}
	if info.JournalSize == 0 {
		// Node has no journal region - nothing to replay
		return &RecoveryResult{}, nil
	}

	start := time.Now()
	result := &RecoveryResult{}

	log.Printf("[Recovery] Starting journal replay for node %x (slot %d, hostname: %s)",
		info.UUID[:4], info.SlotIndex, info.Hostname)
	log.Printf("[Recovery] Journal region: offset=%d, size=%d",
		info.JournalOffset, info.JournalSize)

	// Open the failed node's journal region
	region, err := OpenRegion(rctx.Dev, info.JournalOffset, info.JournalSize, info.UUID)
	if err != nil {
		return nil, fmt.Errorf("failed to open journal region: %w", err)
	}
	defer region.Close()

	// Check if recovery is needed
	if !region.NeedsRecovery() {
		log.Printf("[Recovery] Journal is clean, no recovery needed")
		result.Duration = time.Since(start)
		return result, nil
	}

	// Create handler that applies transactions idempotently
	handler := func(txn *RecoveredTxn) error {
		result.TxnReplayed++
		txnResult, err := applyTransaction(ctx, rctx, txn)
		if err != nil {
			result.Errors = append(result.Errors, err)
			return err
		}
		result.EntriesReplayed += txnResult.applied
		result.EntriesSkipped += txnResult.skipped
		return nil
	}

	// Perform recovery
	stats, err := Recover(region, handler)
	if err != nil {
		return result, fmt.Errorf("journal recovery failed: %w", err)
	}

	result.Duration = time.Since(start)

	log.Printf("[Recovery] Completed: txn_scanned=%d, txn_committed=%d, entries_replayed=%d, skipped=%d, duration=%v",
		stats.TxnScanned, stats.TxnCommitted, result.EntriesReplayed, result.EntriesSkipped, result.Duration)

	return result, nil
}

// txnApplyResult tracks how many entries were applied vs skipped.
type txnApplyResult struct {
	applied int
	skipped int
}

// applyTransaction applies a single recovered transaction idempotently.
func applyTransaction(ctx context.Context, rctx *RecoveryContext, txn *RecoveredTxn) (*txnApplyResult, error) {
	result := &txnApplyResult{}

	for _, entry := range txn.Entries {
		applied, err := applyEntry(ctx, rctx, entry)
		if err != nil {
			return result, fmt.Errorf("failed to apply entry op=%s: %w", entry.Entry.OpType, err)
		}
		if applied {
			result.applied++
		} else {
			result.skipped++
		}
	}

	return result, nil
}

// applyEntry applies a single journal entry idempotently.
// Returns true if the operation was applied, false if it was already applied.
func applyEntry(ctx context.Context, rctx *RecoveryContext, entry *RecoveredEntry) (bool, error) {
	switch entry.Entry.OpType {
	case TxnOpInodeCreate:
		return applyInodeCreate(ctx, rctx, entry.Payload)
	case TxnOpInodeUpdate:
		return applyInodeUpdate(ctx, rctx, entry.Payload)
	case TxnOpInodeDelete:
		return applyInodeDelete(ctx, rctx, entry.Payload)
	case TxnOpDirAddEntry:
		return applyDirAddEntry(ctx, rctx, entry.Payload)
	case TxnOpDirRemoveEntry:
		return applyDirRemoveEntry(ctx, rctx, entry.Payload)
	case TxnOpBlockAlloc:
		return applyBlockAlloc(ctx, rctx, entry.Payload)
	case TxnOpBlockFree:
		return applyBlockFree(ctx, rctx, entry.Payload)
	case TxnOpExtentAdd:
		return applyExtentAdd(ctx, rctx, entry.Payload)
	case TxnOpExtentRemove:
		return applyExtentRemove(ctx, rctx, entry.Payload)
	default:
		// Unhandled operation types are logged but not an error
		log.Printf("[Recovery] Skipping unhandled operation type: %s", entry.Entry.OpType)
		return false, nil
	}
}

// applyInodeCreate creates an inode if it doesn't already exist.
func applyInodeCreate(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeInodeCreate(payload)
	if err != nil {
		return false, err
	}

	// Check if inode already exists
	existing, err := rctx.InodeTbl.Read(uint32(p.InodeNum))
	if err == nil && existing.Magic == inode.InodeMagic {
		// Inode already exists - idempotent skip
		log.Printf("[Recovery] InodeCreate: inode %d already exists, skipping", p.InodeNum)
		return false, nil
	}

	// Create the inode
	ino := &inode.Inode{
		Magic:     inode.InodeMagic,
		InodeNum:  uint32(p.InodeNum),
		Mode:      uint16(p.Mode),
		UID:       p.UID,
		GID:       p.GID,
		LinkCount: 1,
	}
	if err == nil {
		ino.DataGeneration = existing.DataGeneration + 1
		ino.MetaGeneration = existing.MetaGeneration + 1
	}

	now := time.Now().UnixNano()
	ino.Atime = now
	ino.Mtime = now
	ino.Ctime = now

	if err := rctx.InodeTbl.WriteWithLockHeld(ino); err != nil {
		return false, fmt.Errorf("failed to write inode %d: %w", p.InodeNum, err)
	}

	log.Printf("[Recovery] InodeCreate: created inode %d", p.InodeNum)
	return true, nil
}

// applyInodeUpdate updates an inode's metadata.
// For recovery, we re-read and update to ensure consistency.
func applyInodeUpdate(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeInodeUpdate(payload)
	if err != nil {
		return false, err
	}

	// Read current inode
	ino, err := rctx.InodeTbl.Read(uint32(p.InodeNum))
	if err != nil {
		// Inode doesn't exist - this is an error during recovery
		return false, fmt.Errorf("inode %d not found for update", p.InodeNum)
	}

	// For InodeUpdate, we don't have the actual values in the header-only payload
	// This is a limitation - we would need the full payload with values
	// For now, log and skip (the values should have been written to disk already)
	log.Printf("[Recovery] InodeUpdate: inode %d update mask=%x (values in full payload)", p.InodeNum, p.UpdateMask)
	_ = ino // Use the read inode to satisfy unused variable

	return false, nil
}

// applyInodeDelete marks an inode as deleted.
func applyInodeDelete(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeInodeDelete(payload)
	if err != nil {
		return false, err
	}

	// Check if inode exists
	ino, err := rctx.InodeTbl.Read(uint32(p.InodeNum))
	if err != nil || ino.Magic != inode.InodeMagic {
		// Already deleted or doesn't exist - idempotent skip
		log.Printf("[Recovery] InodeDelete: inode %d already deleted, skipping", p.InodeNum)
		return false, nil
	}

	// Free the inode
	if err := rctx.InodeTbl.Free(ctx, uint32(p.InodeNum)); err != nil {
		return false, fmt.Errorf("failed to free inode %d: %w", p.InodeNum, err)
	}

	log.Printf("[Recovery] InodeDelete: deleted inode %d", p.InodeNum)
	return true, nil
}

// applyDirAddEntry adds a directory entry if it doesn't exist.
func applyDirAddEntry(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeDirAddEntry(payload)
	if err != nil {
		return false, err
	}

	// Read parent directory inode
	parentIno, err := rctx.InodeTbl.Read(uint32(p.ParentInodeNum))
	if err != nil {
		return false, fmt.Errorf("parent inode %d not found", p.ParentInodeNum)
	}

	// Check if entry already exists
	_, _, err = rctx.DirMgr.Lookup(ctx, parentIno, p.Name)
	if err == nil {
		// Entry already exists - idempotent skip
		log.Printf("[Recovery] DirAddEntry: entry %q already exists in parent %d, skipping",
			p.Name, p.ParentInodeNum)
		return false, nil
	}
	if err != dir.ErrNotFound {
		return false, fmt.Errorf("lookup failed: %w", err)
	}

	// Convert entry type
	var fileType uint8
	switch p.EntryType {
	case DirEntryRegular:
		fileType = dir.DT_REG
	case DirEntryDirectory:
		fileType = dir.DT_DIR
	case DirEntrySymlink:
		fileType = dir.DT_LNK
	default:
		fileType = dir.DT_UNKNOWN
	}

	// Add the entry
	if err := rctx.DirMgr.Add(ctx, parentIno, p.Name, p.ChildInodeNum, fileType); err != nil {
		// Check if it's a race condition where another recovery added it
		if err == dir.ErrExists {
			log.Printf("[Recovery] DirAddEntry: entry %q added by concurrent recovery, skipping", p.Name)
			return false, nil
		}
		return false, fmt.Errorf("failed to add entry %q: %w", p.Name, err)
	}

	log.Printf("[Recovery] DirAddEntry: added entry %q -> inode %d in parent %d",
		p.Name, p.ChildInodeNum, p.ParentInodeNum)
	return true, nil
}

// applyDirRemoveEntry removes a directory entry if it exists.
func applyDirRemoveEntry(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeDirRemoveEntry(payload)
	if err != nil {
		return false, err
	}

	// Read parent directory inode
	parentIno, err := rctx.InodeTbl.Read(uint32(p.ParentInodeNum))
	if err != nil {
		return false, fmt.Errorf("parent inode %d not found", p.ParentInodeNum)
	}

	// Check if entry exists
	_, _, err = rctx.DirMgr.Lookup(ctx, parentIno, p.Name)
	if err == dir.ErrNotFound {
		// Already removed - idempotent skip
		log.Printf("[Recovery] DirRemoveEntry: entry %q already removed from parent %d, skipping",
			p.Name, p.ParentInodeNum)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lookup failed: %w", err)
	}

	// Remove the entry
	if err := rctx.DirMgr.Remove(ctx, parentIno, p.Name); err != nil {
		// Entry may have been removed by concurrent recovery
		if err == dir.ErrNotFound {
			log.Printf("[Recovery] DirRemoveEntry: entry %q removed by concurrent recovery, skipping", p.Name)
			return false, nil
		}
		return false, fmt.Errorf("failed to remove entry %q: %w", p.Name, err)
	}

	log.Printf("[Recovery] DirRemoveEntry: removed entry %q from parent %d", p.Name, p.ParentInodeNum)
	return true, nil
}

// applyBlockAlloc marks blocks as allocated if not already.
func applyBlockAlloc(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeBlockAlloc(payload)
	if err != nil {
		return false, err
	}

	applied := false
	for i := uint32(0); i < p.Count; i++ {
		blockNum := p.StartBlock + uint64(i)

		// Check if already allocated
		isAllocated, err := rctx.BitmapMgr.IsAllocated(blockNum)
		if err != nil {
			return applied, fmt.Errorf("failed to check block %d: %w", blockNum, err)
		}

		if isAllocated {
			// Already allocated - idempotent skip for this block
			continue
		}

		// Mark as allocated
		if err := rctx.BitmapMgr.MarkAllocated(ctx, blockNum); err != nil {
			return applied, fmt.Errorf("failed to mark block %d allocated: %w", blockNum, err)
		}
		applied = true
	}

	if applied {
		log.Printf("[Recovery] BlockAlloc: marked blocks %d-%d allocated for inode %d",
			p.StartBlock, p.StartBlock+uint64(p.Count)-1, p.InodeNum)
	} else {
		log.Printf("[Recovery] BlockAlloc: blocks %d-%d already allocated, skipping",
			p.StartBlock, p.StartBlock+uint64(p.Count)-1)
	}

	return applied, nil
}

// applyBlockFree marks blocks as free if currently allocated.
func applyBlockFree(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeBlockFree(payload)
	if err != nil {
		return false, err
	}

	applied := false
	for i := uint32(0); i < p.Count; i++ {
		blockNum := p.StartBlock + uint64(i)

		// Check if allocated
		isAllocated, err := rctx.BitmapMgr.IsAllocated(blockNum)
		if err != nil {
			return applied, fmt.Errorf("failed to check block %d: %w", blockNum, err)
		}

		if !isAllocated {
			// Already free - idempotent skip for this block
			continue
		}

		// Free the block
		if err := rctx.BitmapMgr.Free(ctx, blockNum); err != nil {
			return applied, fmt.Errorf("failed to free block %d: %w", blockNum, err)
		}
		applied = true
	}

	if applied {
		log.Printf("[Recovery] BlockFree: freed blocks %d-%d for inode %d",
			p.StartBlock, p.StartBlock+uint64(p.Count)-1, p.InodeNum)
	} else {
		log.Printf("[Recovery] BlockFree: blocks %d-%d already free, skipping",
			p.StartBlock, p.StartBlock+uint64(p.Count)-1)
	}

	return applied, nil
}

// applyExtentAdd adds an extent to an inode if not already present.
func applyExtentAdd(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeExtentAdd(payload)
	if err != nil {
		return false, err
	}

	// Read the inode
	ino, err := rctx.InodeTbl.Read(uint32(p.InodeNum))
	if err != nil {
		return false, fmt.Errorf("inode %d not found", p.InodeNum)
	}

	// Check if extent already exists at this file block
	_, _, err = rctx.ExtentMgr.Lookup(ino, p.FileBlock)
	if err == nil {
		// Extent already exists at this position - idempotent skip
		log.Printf("[Recovery] ExtentAdd: extent at file block %d already exists for inode %d, skipping",
			p.FileBlock, p.InodeNum)
		return false, nil
	}

	// Add the extent using the extent manager's recovery-friendly method
	// Note: Journal payload uses FileBlock/DiskBlock, extent uses LogicalBlock/PhysicalBlock
	ext := &extent.Extent{
		LogicalBlock:  p.FileBlock,
		PhysicalBlock: p.DiskBlock,
		Length:        uint64(p.Length),
		Flags:         extent.FlagAllocated,
	}

	if err := rctx.ExtentMgr.AddExtentForRecovery(ctx, ino, ext); err != nil {
		// Check for overlap (concurrent recovery may have added it)
		if err == extent.ErrExtentOverlap {
			log.Printf("[Recovery] ExtentAdd: extent overlaps, likely concurrent recovery, skipping")
			return false, nil
		}
		return false, fmt.Errorf("failed to add extent: %w", err)
	}

	// Write the modified inode back to disk (extent was added in-memory)
	if err := rctx.InodeTbl.Write(ctx, ino); err != nil {
		return false, fmt.Errorf("failed to write inode after extent add: %w", err)
	}

	log.Printf("[Recovery] ExtentAdd: added extent file=%d disk=%d len=%d to inode %d",
		p.FileBlock, p.DiskBlock, p.Length, p.InodeNum)
	return true, nil
}

// applyExtentRemove removes an extent from an inode if present.
func applyExtentRemove(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeExtentRemove(payload)
	if err != nil {
		return false, err
	}

	// Read the inode
	ino, err := rctx.InodeTbl.Read(uint32(p.InodeNum))
	if err != nil {
		return false, fmt.Errorf("inode %d not found", p.InodeNum)
	}

	// Check if extent exists
	_, _, err = rctx.ExtentMgr.Lookup(ino, p.FileBlock)
	if err != nil {
		// Extent doesn't exist - already removed, idempotent skip
		log.Printf("[Recovery] ExtentRemove: extent at file block %d already removed from inode %d, skipping",
			p.FileBlock, p.InodeNum)
		return false, nil
	}

	// Remove the extent using truncate to the file block position
	// This removes extents from FileBlock onwards for Length blocks
	// Note: Journal uses uint64 for FileBlock, but extent manager uses uint32
	if err := rctx.ExtentMgr.RemoveExtentForRecovery(ctx, ino, uint32(p.FileBlock), p.Length); err != nil {
		return false, fmt.Errorf("failed to remove extent: %w", err)
	}

	// Write the modified inode back to disk (extent was removed in-memory)
	if err := rctx.InodeTbl.Write(ctx, ino); err != nil {
		return false, fmt.Errorf("failed to write inode after extent remove: %w", err)
	}

	log.Printf("[Recovery] ExtentRemove: removed extent file=%d len=%d from inode %d",
		p.FileBlock, p.Length, p.InodeNum)
	return true, nil
}

// CreateRecoveryHandler creates a node.RecoveryHandler that uses the given context.
// This is the function that should be called to wire up recovery in fs.go.
func CreateRecoveryHandler(rctx *RecoveryContext) node.RecoveryHandler {
	return func(info *node.FailedNodeInfo) error {
		ctx := context.Background()
		result, err := ReplayJournal(ctx, rctx, info)
		if err != nil {
			return err
		}

		// Log any errors that occurred during recovery
		for _, e := range result.Errors {
			log.Printf("[Recovery] Error during replay: %v", e)
		}

		return nil
	}
}

// CreateTxnRecoveryHandler creates a per-transaction RecoveryHandler.
// This is used for self-recovery during mount when we need to replay
// transactions directly without the node.FailedNodeInfo wrapper.
func CreateTxnRecoveryHandler(rctx *RecoveryContext) RecoveryHandler {
	ctx := context.Background()
	return func(txn *RecoveredTxn) error {
		result, err := applyTransaction(ctx, rctx, txn)
		if err != nil {
			return err
		}
		log.Printf("[Recovery] Replayed txn seq=%d: applied=%d, skipped=%d",
			txn.Header.Seq, result.applied, result.skipped)
		return nil
	}
}
