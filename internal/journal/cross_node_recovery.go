// Package journal cross-node recovery implementation.
//
// This file implements journal replay for failed nodes in a cluster.
// When a node fails, another node can replay its journal to recover
// any committed but not yet checkpointed transactions.
package journal

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
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

	// The region belongs to the slot and may hold a dirty journal from the slot's previous owner.
	region, err := OpenRegionForRecovery(rctx.Dev, info.JournalOffset, info.JournalSize)
	if errors.Is(err, ErrJournalUninitialized) {
		log.Printf("[Recovery] Journal was never initialized, nothing to replay")
		result.Duration = time.Since(start)
		return result, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open journal region: %w", err)
	}
	defer func() {
		if err := region.Close(); err != nil {
			log.Printf("[Recovery] Closing journal region failed: %v", err)
		}
	}()
	if owner := region.Superblock().NodeUUID; owner != info.UUID {
		log.Printf("[Recovery] Journal was written by earlier slot owner %x", owner[:4])
	}

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

	stats, err := RecoverChecked(region, handler, info.CheckLease)
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
	r := newTxnReplay()

	for _, entry := range txn.Entries {
		applied, err := applyEntry(ctx, rctx, r, entry)
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

// txnReplay is what replaying one transaction changed, so that its later records can tell those changes from another
// node's. A peer can take a dead node's locks before its journal is replayed, so each record checks that what it
// changes is still as the operation left it.
type txnReplay struct {
	created map[uint64]bool
	wrote   map[uint32]uint64
}

func newTxnReplay() *txnReplay {
	return &txnReplay{created: make(map[uint64]bool), wrote: make(map[uint32]uint64)}
}

// unchanged reports whether ino is in use and was last written before a record logged its MetaGeneration as gen, or
// by this replay.
func (r *txnReplay) unchanged(ino *inode.Inode, gen uint64) bool {
	if ino.Magic != inode.InodeMagic {
		return false
	}
	wrote, ok := r.wrote[ino.InodeNum]
	return ino.MetaGeneration == gen || ok && ino.MetaGeneration == wrote
}

// applyEntry applies a single journal entry idempotently.
// Returns true if the operation was applied, false if it was already applied or must not be.
func applyEntry(ctx context.Context, rctx *RecoveryContext, r *txnReplay, entry *RecoveredEntry) (bool, error) {
	switch entry.Entry.OpType {
	case TxnOpInodeCreate:
		return applyInodeCreate(ctx, rctx, r, entry.Payload)
	case TxnOpInodeUpdate:
		return applyInodeUpdate(ctx, rctx, entry.Payload)
	case TxnOpInodeDelete:
		return applyInodeDelete(ctx, rctx, r, entry.Payload)
	case TxnOpDirAddEntry:
		return applyDirAddEntry(ctx, rctx, r, entry.Payload)
	case TxnOpDirRemoveEntry:
		return applyDirRemoveEntry(ctx, rctx, entry.Payload)
	case TxnOpBlockAlloc:
		return applyBlockAlloc(ctx, rctx, entry.Payload)
	case TxnOpBlockFree:
		return applyBlockFree(ctx, rctx, entry.Payload)
	case TxnOpExtentAdd:
		return applyExtentAdd(ctx, rctx, r, entry.Payload)
	case TxnOpExtentFree:
		return applyExtentFree(ctx, rctx, r, entry.Payload)
	default:
		// Unhandled operation types are logged but not an error
		log.Printf("[Recovery] Skipping unhandled operation type: %s", entry.Entry.OpType)
		return false, nil
	}
}

// applyInodeCreate claims the inode's slot if it is still free and nobody used it since the create was logged.
func applyInodeCreate(ctx context.Context, rctx *RecoveryContext, r *txnReplay, payload []byte) (bool, error) {
	p, err := DeserializeInodeCreate(payload)
	if err != nil {
		return false, err
	}

	ino, err := rctx.InodeTbl.ClaimSlotIf(ctx, uint32(p.InodeNum), func(free *inode.Inode) bool {
		return free.MetaGeneration == p.MetaGeneration
	})
	if errors.Is(err, inode.ErrInodeInUse) {
		log.Printf("[Recovery] InodeCreate: slot %d is in use or was used since, skipping", p.InodeNum)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to claim inode %d: %w", p.InodeNum, err)
	}

	ino.Mode = uint16(p.Mode)
	ino.UID = p.UID
	ino.GID = p.GID
	now := time.Now().UnixNano()
	ino.Atime = now
	ino.Mtime = now
	ino.Ctime = now
	if err := rctx.InodeTbl.Write(ctx, ino); err != nil {
		return false, fmt.Errorf("failed to write inode %d: %w", p.InodeNum, err)
	}
	r.created[p.InodeNum] = true
	r.wrote[ino.InodeNum] = ino.MetaGeneration

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

// applyInodeDelete frees the inode if it is unchanged since the unlink was logged.
func applyInodeDelete(ctx context.Context, rctx *RecoveryContext, r *txnReplay, payload []byte) (bool, error) {
	p, err := DeserializeInodeDelete(payload)
	if err != nil {
		return false, err
	}

	freed, err := rctx.InodeTbl.FreeIf(ctx, uint32(p.InodeNum), func(ino *inode.Inode) bool {
		return r.unchanged(ino, p.MetaGeneration)
	})
	if errors.Is(err, inode.ErrInodeNotInUse) || err == nil && !freed {
		log.Printf("[Recovery] InodeDelete: inode %d is free or was written since, skipping", p.InodeNum)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to free inode %d: %w", p.InodeNum, err)
	}

	log.Printf("[Recovery] InodeDelete: deleted inode %d", p.InodeNum)
	return true, nil
}

// applyDirAddEntry adds the entry for an inode this transaction's replay created.
func applyDirAddEntry(ctx context.Context, rctx *RecoveryContext, r *txnReplay, payload []byte) (bool, error) {
	p, err := DeserializeDirAddEntry(payload)
	if err != nil {
		return false, err
	}
	if !r.created[p.ChildInodeNum] {
		log.Printf("[Recovery] DirAddEntry: replay did not create inode %d, skipping %q", p.ChildInodeNum, p.Name)
		return false, nil
	}

	parentIno, ok, err := liveDir(rctx, p.ParentInodeNum)
	if !ok {
		return false, err
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

// liveDir reads the inode num and reports whether it is a directory in use. Another node may have removed it since
// the record was logged.
func liveDir(rctx *RecoveryContext, num uint64) (*inode.Inode, bool, error) {
	ino, err := rctx.InodeTbl.Read(uint32(num))
	if err != nil {
		return nil, false, fmt.Errorf("parent inode %d not found", num)
	}
	if ino.Magic != inode.InodeMagic || !ino.IsDir() {
		log.Printf("[Recovery] inode %d is no longer a directory, skipping", num)
		return nil, false, nil
	}
	return ino, true, nil
}

// applyDirRemoveEntry removes the entry if it still names the child the unlink logged, unchanged since.
func applyDirRemoveEntry(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeDirRemoveEntry(payload)
	if err != nil {
		return false, err
	}

	parentIno, ok, err := liveDir(rctx, p.ParentInodeNum)
	if !ok {
		return false, err
	}

	childNum, _, err := rctx.DirMgr.Lookup(ctx, parentIno, p.Name)
	if err == dir.ErrNotFound {
		// Already removed - idempotent skip
		log.Printf("[Recovery] DirRemoveEntry: entry %q already removed from parent %d, skipping",
			p.Name, p.ParentInodeNum)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lookup failed: %w", err)
	}
	if childNum != p.ChildInodeNum {
		log.Printf("[Recovery] DirRemoveEntry: %q now names inode %d, not %d, skipping", p.Name, childNum, p.ChildInodeNum)
		return false, nil
	}
	child, err := rctx.InodeTbl.Read(uint32(childNum))
	if err != nil {
		return false, fmt.Errorf("child inode %d not found: %w", childNum, err)
	}
	if child.MetaGeneration != p.ChildMetaGeneration {
		log.Printf("[Recovery] DirRemoveEntry: inode %d of %q changed since the unlink was logged, skipping", childNum, p.Name)
		return false, nil
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

// applyBlockFree frees nothing: older builds logged logical file blocks here, and freeing those numbers as physical
// blocks would free blocks other files map. Truncate and unlink now log ExtentFree.
func applyBlockFree(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeBlockFree(payload)
	if err != nil {
		return false, err
	}
	log.Printf("[Recovery] BlockFree: leaving file blocks %d-%d of inode %d allocated",
		p.StartBlock, p.StartBlock+uint64(p.Count)-1, p.InodeNum)
	return false, nil
}

// applyExtentAdd maps the logged blocks where the inode has a hole and grows the inode to the write's size, if the
// inode is unchanged since the write was logged.
func applyExtentAdd(ctx context.Context, rctx *RecoveryContext, r *txnReplay, payload []byte) (bool, error) {
	p, err := DeserializeExtentAdd(payload)
	if err != nil {
		return false, err
	}

	return updateExtents(ctx, rctx, r, uint32(p.InodeNum), func(ino *inode.Inode) (bool, error) {
		if !r.unchanged(ino, p.MetaGeneration) {
			log.Printf("[Recovery] ExtentAdd: inode %d is free or was written since, skipping", p.InodeNum)
			return false, nil
		}
		grown := ino.Size < p.Size
		ino.Size = max(ino.Size, p.Size)
		if _, _, err := rctx.ExtentMgr.Lookup(ino, p.FileBlock); err == nil {
			log.Printf("[Recovery] ExtentAdd: extent at file block %d already exists for inode %d, skipping",
				p.FileBlock, p.InodeNum)
			return grown, nil
		}

		// Note: Journal payload uses FileBlock/DiskBlock, extent uses LogicalBlock/PhysicalBlock
		ext := &extent.Extent{
			LogicalBlock:  p.FileBlock,
			PhysicalBlock: p.DiskBlock,
			Length:        uint64(p.Length),
			Flags:         extent.FlagAllocated,
		}
		if err := rctx.ExtentMgr.AddExtentForRecovery(ctx, ino, ext); err != nil {
			if err == extent.ErrExtentOverlap {
				log.Printf("[Recovery] ExtentAdd: extent overlaps, likely concurrent recovery, skipping")
				return grown, nil
			}
			return false, fmt.Errorf("failed to add extent: %w", err)
		}
		log.Printf("[Recovery] ExtentAdd: added extent file=%d disk=%d len=%d to inode %d",
			p.FileBlock, p.DiskBlock, p.Length, p.InodeNum)
		return true, nil
	})
}

// updateExtents reads an inode, applies change, and writes it back when change
// reports an update, starting over if another node changed the inode's extent
// tree in between.
func updateExtents(ctx context.Context, rctx *RecoveryContext, r *txnReplay, inodeNum uint32, change func(*inode.Inode) (bool, error)) (bool, error) {
	const attempts = 5
	for attempt := 1; ; attempt++ {
		ino, err := rctx.InodeTbl.Read(inodeNum)
		if err != nil {
			return false, fmt.Errorf("inode %d not found", inodeNum)
		}
		changed, err := change(ino)
		if err != nil || !changed {
			rctx.ExtentMgr.DiscardPending(ctx, ino)
			return false, err
		}
		err = rctx.InodeTbl.Write(ctx, ino)
		if err == nil {
			r.wrote[inodeNum] = ino.MetaGeneration
			return true, nil
		}
		rctx.ExtentMgr.DiscardPending(ctx, ino)
		if !errors.Is(err, extent.ErrTreeConflict) || attempt == attempts {
			return false, fmt.Errorf("failed to write inode %d after extent change: %w", inodeNum, err)
		}
		time.Sleep(time.Duration(attempt) * time.Millisecond)
	}
}

// CreateRecoveryHandler creates a node.RecoveryHandler that uses the given context.
// This is the function that should be called to wire up recovery in fs.go.
func CreateRecoveryHandler(rctx *RecoveryContext) node.RecoveryHandler {
	return func(info *node.FailedNodeInfo) error {
		ctx := lock.ForReplay(context.Background())
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
	ctx := lock.ForReplay(context.Background())
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

// applyExtentFree cuts the inode to the logged size and frees its blocks past it, through refcounts for shared ones,
// if the inode is unchanged since the free was logged. Otherwise the operation's inode write landed, or another node
// changed the inode after the crash, and the blocks may be in use again.
func applyExtentFree(ctx context.Context, rctx *RecoveryContext, r *txnReplay, payload []byte) (bool, error) {
	p, err := DeserializeExtentFree(payload)
	if err != nil {
		return false, err
	}
	return updateExtents(ctx, rctx, r, uint32(p.InodeNum), func(ino *inode.Inode) (bool, error) {
		if !r.unchanged(ino, p.MetaGeneration) {
			log.Printf("[Recovery] ExtentFree: inode %d is free or was written since, skipping", p.InodeNum)
			return false, nil
		}
		if err := rctx.ExtentMgr.Truncate(ctx, ino, min(ino.Size, p.Size)); err != nil {
			return false, fmt.Errorf("failed to free the blocks of inode %d past %d bytes: %w", p.InodeNum, p.Size, err)
		}
		log.Printf("[Recovery] ExtentFree: cut inode %d to %d bytes", p.InodeNum, ino.Size)
		return true, nil
	})
}
