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
	"math"
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

	// OrphanHolders returns the slots that may hold open a file replay leaves at zero links.
	OrphanHolders func(num uint32) (uint64, error)
	AddOrphan     func(num uint32, birth uint64) error
	FreeSFBSlot   func(ctx context.Context, packed uint64)
}

// ReleaseReserved returns a crashed node's unused reserved blocks. They have no
// count, and freeing skips any already free.
func (rctx *RecoveryContext) ReleaseReserved(ctx context.Context) func([]uint64) error {
	return func(blocks []uint64) error {
		log.Printf("[Recovery] Freeing %d reserved blocks the node never used", len(blocks))
		rctx.ExtentMgr.ReleaseBlocks(ctx, blocks)
		return nil
	}
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

	if err := region.Superblock().checkState(); err != nil {
		return nil, err
	}
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

	stats, err := RecoverReleasing(region, handler, info.CheckLease, rctx.ReleaseReserved(ctx))
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
	if err := unknownOpIn(txn); err != nil {
		return result, err
	}
	r := newTxnReplay(txn.Header.OwnerNodeUUID)

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
	owner   [16]byte
	created map[uint64]bool
	wrote   map[uint32]uint64
	keep    map[uint64]bool // targets a replayed rename left named, so its records must not drop or free them
	linked  map[uint64]bool // files this transaction gave a link, which its DirAddEntry names
}

func newTxnReplay(owner [16]byte) *txnReplay {
	return &txnReplay{owner: owner, created: make(map[uint64]bool), wrote: make(map[uint32]uint64), keep: make(map[uint64]bool),
		linked: make(map[uint64]bool)}
}

// kept reports whether a rename earlier in this transaction left inode num named, and logs why op skips it.
func (r *txnReplay) kept(op string, num uint64) bool {
	if r.keep[num] {
		log.Printf("[Recovery] %s: the rename did not replace inode %d, which keeps its name, skipping", op, num)
	}
	return r.keep[num]
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
	if err := inodesInTable(rctx, entry); err != nil {
		return false, err
	}
	switch entry.Entry.OpType {
	case TxnOpInodeCreate:
		return applyInodeCreate(ctx, rctx, r, entry.Payload)
	case TxnOpInodeUpdate:
		return applyInodeUpdate(ctx, rctx, entry.Payload)
	case TxnOpInodeDelete:
		return applyInodeDelete(ctx, rctx, r, entry.Payload)
	case TxnOpLinkDrop:
		return applyLinkDrop(ctx, rctx, r, entry.Payload)
	case TxnOpLinkAdd:
		return applyLinkAdd(ctx, rctx, r, entry.Payload)
	case TxnOpSymlinkTarget:
		return applySymlinkTarget(ctx, rctx, r, entry.Payload)
	case TxnOpDirAddEntry:
		return applyDirAddEntry(ctx, rctx, r, entry.Payload)
	case TxnOpDirRename:
		return applyDirRename(ctx, rctx, r, entry.Payload)
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
	case TxnOpBlockReserve, TxnOpBlockUse:
		// Read before replay to free unused reserved blocks; nothing to apply.
		return false, nil
	case TxnOpQuotaDelta, TxnOpQuotaFlush, TxnOpQuotaFold:
		// Folded into the quota table as a whole before replay: see QuotaReplay.
		return false, nil
	default:
		log.Printf("[Recovery] Skipping %s, which this build no longer logs and replay does not need", entry.Entry.OpType)
		return false, nil
	}
}

// applyInodeCreate claims the inode's slot if it is still free and nobody used it since the create was logged, or
// takes over the create's own claim if it landed before the crash.
func applyInodeCreate(ctx context.Context, rctx *RecoveryContext, r *txnReplay, payload []byte) (bool, error) {
	p, err := DeserializeInodeCreate(payload)
	if err != nil {
		return false, err
	}

	num, err := tableInode(p.InodeNum)
	if err != nil {
		return false, err
	}
	ino, err := rctx.InodeTbl.ClaimSlotIf(ctx, num, func(free *inode.Inode) bool {
		return free.MetaGeneration == p.MetaGeneration
	})
	if errors.Is(err, inode.ErrInodeInUse) {
		ino, err = r.ownClaim(rctx, p)
		if ino == nil && err == nil {
			log.Printf("[Recovery] InodeCreate: slot %d is in use or was used since, skipping", p.InodeNum)
			return false, nil
		}
	}
	if err != nil {
		return false, fmt.Errorf("failed to claim inode %d: %w", p.InodeNum, err)
	}

	if ino.Mode == 0 {
		if err := initCreated(ctx, rctx, ino, p); err != nil {
			return false, fmt.Errorf("failed to set up inode %d: %w", p.InodeNum, err)
		}
	}
	r.created[p.InodeNum] = true
	r.wrote[ino.InodeNum] = ino.MetaGeneration

	log.Printf("[Recovery] InodeCreate: created inode %d", p.InodeNum)
	return true, nil
}

// ownClaim returns the slot's inode if the create's own claim landed and nothing wrote the inode since: the logging
// node wrote it last, and at most as often as a create does before adding the name.
func (r *txnReplay) ownClaim(rctx *RecoveryContext, p *InodeCreatePayload) (*inode.Inode, error) {
	num, err := tableInode(p.InodeNum)
	if err != nil {
		return nil, err
	}
	ino, err := rctx.InodeTbl.Read(num)
	if err != nil {
		return nil, err
	}
	writes := ino.MetaGeneration - p.MetaGeneration
	maxWrites := uint64(2) // the claim, then the attributes
	if uint16(p.Mode)&inode.S_IFMT == inode.S_IFDIR {
		maxWrites = 3 // the claim, "." and "..", then the attributes
	}
	if ino.Magic != inode.InodeMagic || ino.LastModifierUUID != r.owner || writes < 1 || writes > maxWrites ||
		ino.Mode != 0 && ino.Mode != uint16(p.Mode) {
		return nil, nil
	}
	return ino, nil
}

// initCreated gives a newly claimed inode the logged attributes, and a directory its "." and ".." entries.
func initCreated(ctx context.Context, rctx *RecoveryContext, ino *inode.Inode, p *InodeCreatePayload) error {
	ino.Mode = uint16(p.Mode)
	ino.UID = p.UID
	ino.GID = p.GID
	ino.ProjectID = p.ProjectID
	if ino.IsDir() && p.ProjectID != 0 {
		ino.Flags |= inode.FlagProjInherit
	}
	now := time.Now().UnixNano()
	ino.Atime = now
	ino.Mtime = now
	ino.Ctime = now
	if !ino.IsDir() {
		return rctx.InodeTbl.Write(ctx, ino)
	}
	ino.LinkCount = 2
	return rctx.DirMgr.InitDirectory(ctx, ino, p.ParentInodeNum)
}

var errLinkAddSkipped = errors.New("the file changed since the link was logged")

// applyLinkAdd gives a file the link a hard link logged, only if nothing changed the file since; only then may the
// transaction's DirAddEntry name it. A changed file may already have the link and its name, which another node may
// have renamed since, so replay leaves it alone. A link that a crash cut off between its count and its name stays an
// extra link, which fsck repairs.
func applyLinkAdd(ctx context.Context, rctx *RecoveryContext, r *txnReplay, payload []byte) (bool, error) {
	p, err := DeserializeLinkDrop(payload)
	if err != nil {
		return false, err
	}
	num, err := tableInode(p.InodeNum)
	if err != nil {
		return false, err
	}
	err = rctx.InodeTbl.Update(ctx, num, func(ino *inode.Inode) error {
		if !r.unchanged(ino, p.MetaGeneration) || !ino.IsFile() || ino.LinkCount == 0 {
			return errLinkAddSkipped
		}
		ino.LinkCount++
		return nil
	})
	switch {
	case errors.Is(err, errLinkAddSkipped):
		log.Printf("[Recovery] LinkAdd: inode %d changed since the link was logged, skipping", num)
		return false, nil
	case err != nil:
		return false, fmt.Errorf("failed to add a link to inode %d: %w", num, err)
	}
	after, err := rctx.InodeTbl.Read(num)
	if err != nil {
		return true, err
	}
	r.wrote[num] = after.MetaGeneration
	r.linked[p.InodeNum] = true
	return true, nil
}

// applySymlinkTarget gives a symlink its target, only when replay created it in this transaction and nothing wrote it since.
func applySymlinkTarget(ctx context.Context, rctx *RecoveryContext, r *txnReplay, payload []byte) (bool, error) {
	p, err := DeserializeSymlinkTarget(payload)
	if err != nil {
		return false, err
	}
	if !r.created[p.InodeNum] {
		log.Printf("[Recovery] SymlinkTarget: replay did not create inode %d, skipping", p.InodeNum)
		return false, nil
	}
	return updateExtents(ctx, rctx, r, p.InodeNum, func(ino *inode.Inode) (bool, error) {
		if !ino.IsSymlink() || !r.unchanged(ino, 0) || ino.HasInlineTarget() {
			return false, nil
		}
		if p.DiskBlock == 0 {
			ino.SetInlineTarget(p.Target)
			return true, nil
		}
		if _, _, err := rctx.ExtentMgr.Lookup(ino, 0); err == nil {
			return false, nil
		}
		ext := &extent.Extent{PhysicalBlock: p.DiskBlock, Length: 1, Flags: extent.FlagAllocated}
		if err := rctx.ExtentMgr.AddExtentForRecovery(ctx, ino, ext); err != nil {
			return false, fmt.Errorf("failed to map symlink block: %w", err)
		}
		ino.Size = uint64(len(p.Target))
		ino.AllocatedBlocks, ino.WrittenBlocks = 1, 1
		return true, nil
	})
}

// applyInodeUpdate updates an inode's metadata.
// For recovery, we re-read and update to ensure consistency.
func applyInodeUpdate(ctx context.Context, rctx *RecoveryContext, payload []byte) (bool, error) {
	p, err := DeserializeInodeUpdate(payload)
	if err != nil {
		return false, err
	}

	num, err := tableInode(p.InodeNum)
	if err != nil {
		return false, err
	}
	ino, err := rctx.InodeTbl.Read(num)
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
	if r.kept("InodeDelete", uint64(p.InodeNum)) {
		return false, nil
	}

	num, err := tableInode(p.InodeNum)
	if err != nil {
		return false, err
	}
	freed, err := rctx.InodeTbl.FreeIf(ctx, num, func(ino *inode.Inode) bool {
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

var (
	errLinkDropDone  = errors.New("link drop already applied or overtaken")
	errParentChanged = errors.New("the parent changed since the rename logged it")
)

// applyLinkDrop redoes a lost link drop; at zero links the file is listed as an orphan, or freed if nobody holds it.
func applyLinkDrop(ctx context.Context, rctx *RecoveryContext, r *txnReplay, payload []byte) (bool, error) {
	p, err := DeserializeLinkDrop(payload)
	if err != nil {
		return false, err
	}
	if r.kept("LinkDrop", uint64(p.InodeNum)) {
		return false, nil
	}
	num, err := tableInode(p.InodeNum)
	if err != nil {
		return false, err
	}
	var holders uint64
	if rctx.OrphanHolders != nil {
		if holders, err = rctx.OrphanHolders(num); err != nil {
			return false, fmt.Errorf("reading the live slots for inode %d: %w", num, err)
		}
	}
	err = rctx.InodeTbl.Update(ctx, num, func(ino *inode.Inode) error {
		if !r.unchanged(ino, p.MetaGeneration) || ino.LinkCount == 0 {
			return errLinkDropDone
		}
		ino.LinkCount--
		if ino.LinkCount == 0 {
			ino.Holders = holders
		}
		return nil
	})
	switch {
	case errors.Is(err, errLinkDropDone):
		// The drop landed before the crash; the orphan may still be unlisted or unheld.
		ino, err := rctx.InodeTbl.Read(num)
		switch {
		case err != nil || ino.Magic != inode.InodeMagic || ino.LinkCount != 0:
			log.Printf("[Recovery] LinkDrop: inode %d changed since the drop was logged, skipping", num)
			return false, nil
		case ino.Holders != 0 && rctx.AddOrphan != nil:
			return false, rctx.AddOrphan(num, ino.BirthGeneration)
		}
		// Without AddOrphan this node is alone, and the crash ended every open.
		return false, freeOrphan(ctx, rctx, num, ino.MetaGeneration)
	case err != nil:
		return false, fmt.Errorf("failed to drop a link of inode %d: %w", num, err)
	}
	after, err := rctx.InodeTbl.Read(num)
	if err != nil {
		return true, err
	}
	r.wrote[num] = after.MetaGeneration
	switch {
	case after.LinkCount > 0:
	case after.Holders != 0 && rctx.AddOrphan != nil:
		return true, rctx.AddOrphan(num, after.BirthGeneration)
	default:
		return true, freeOrphan(ctx, rctx, num, after.MetaGeneration)
	}
	log.Printf("[Recovery] LinkDrop: inode %d has %d links left", num, after.LinkCount)
	return true, nil
}

func freeOrphan(ctx context.Context, rctx *RecoveryContext, num uint32, gen uint64) error {
	var freed *inode.Inode
	ok, err := rctx.InodeTbl.FreeIf(ctx, num, func(ino *inode.Inode) bool {
		freed = ino
		return ino.MetaGeneration == gen && ino.LinkCount == 0
	})
	if err != nil || !ok {
		return err
	}
	log.Printf("[Recovery] LinkDrop: freed inode %d", num)
	sfbBacked, sfbSlot := freed.IsSFBBacked(), freed.InlineExtents[0].PhysicalBlock
	if err := rctx.ExtentMgr.Delete(ctx, freed); err != nil {
		return err
	}
	if sfbBacked && rctx.FreeSFBSlot != nil {
		rctx.FreeSFBSlot(ctx, sfbSlot)
	}
	return nil
}

// applyDirAddEntry adds the entry for an inode this transaction's replay created.
func applyDirAddEntry(ctx context.Context, rctx *RecoveryContext, r *txnReplay, payload []byte) (bool, error) {
	p, err := DeserializeDirAddEntry(payload)
	if err != nil {
		return false, err
	}
	added, err := addLoggedEntry(ctx, rctx, r, p)
	if err != nil || p.EntryType != DirEntryDirectory {
		return added, err
	}
	return added, recountLinks(ctx, rctx, p.ParentInodeNum)
}

func addLoggedEntry(ctx context.Context, rctx *RecoveryContext, r *txnReplay, p *DirAddEntryPayload) (bool, error) {
	if !r.created[p.ChildInodeNum] && !r.linked[p.ChildInodeNum] {
		log.Printf("[Recovery] DirAddEntry: replay did not create or link inode %d, skipping %q", p.ChildInodeNum, p.Name)
		return false, nil
	}

	parentIno, ok, err := liveDir(rctx, p.ParentInodeNum)
	if !ok {
		return false, err
	}

	// Add checks that the parent is live and the name is free under the directory lock.
	err = rctx.DirMgr.Add(ctx, parentIno, p.Name, p.ChildInodeNum, fileTypeOf(p.EntryType))
	switch {
	case errors.Is(err, dir.ErrExists):
		log.Printf("[Recovery] DirAddEntry: entry %q already exists in parent %d, skipping", p.Name, p.ParentInodeNum)
		return false, nil
	case errors.Is(err, dir.ErrRemoved):
		log.Printf("[Recovery] DirAddEntry: parent %d of %q was removed, skipping", p.ParentInodeNum, p.Name)
		return false, nil
	case err != nil:
		return false, fmt.Errorf("failed to add entry %q: %w", p.Name, err)
	}
	log.Printf("[Recovery] DirAddEntry: added entry %q -> inode %d in parent %d",
		p.Name, p.ChildInodeNum, p.ParentInodeNum)
	return true, nil
}

// recountLinks sets directory num's link count from its subdirectories, after replay of a mkdir or rmdir that may
// have been cut between the parent's inode write and its chunk write.
func recountLinks(ctx context.Context, rctx *RecoveryContext, num uint64) error {
	parent, ok, err := liveDir(rctx, num)
	if !ok {
		return err
	}
	fixed, err := rctx.DirMgr.RecountLinks(ctx, parent)
	switch {
	case errors.Is(err, dir.ErrRemoved):
		return nil
	case err != nil:
		return fmt.Errorf("failed to recount the links of directory %d: %w", num, err)
	case fixed:
		log.Printf("[Recovery] directory %d: link count set from its subdirectories", num)
	}
	return nil
}

func fileTypeOf(t DirEntryType) uint8 {
	switch t {
	case DirEntryRegular:
		return dir.DT_REG
	case DirEntryDirectory:
		return dir.DT_DIR
	case DirEntrySymlink:
		return dir.DT_LNK
	}
	return dir.DT_UNKNOWN
}

// applyDirRename finishes a committed rename: the new name ends up naming the child and the old name is gone, unless
// another node took the new name since, in which case the old name stays and the target keeps its name.
func applyDirRename(ctx context.Context, rctx *RecoveryContext, r *txnReplay, payload []byte) (bool, error) {
	p, err := DeserializeDirRename(payload)
	if err != nil {
		return false, err
	}
	if done, err := renameOvertaken(rctx, p); err != nil || done {
		return false, err
	}
	placed, err := placeRenamed(ctx, rctx, r, p)
	if err != nil || !placed {
		if p.TargetInodeNum != 0 {
			r.keep[p.TargetInodeNum] = true
		}
		return false, err
	}
	if oldParent, ok, err := liveDir(rctx, p.OldParentInodeNum); err != nil {
		return true, err
	} else if ok {
		if _, err := rctx.DirMgr.RemoveIf(ctx, oldParent, p.OldName, func(cur uint64) bool { return cur == p.InodeNum }); err != nil && !errors.Is(err, dir.ErrNotFound) && !errors.Is(err, dir.ErrRemoved) {
			return true, fmt.Errorf("failed to remove the old name %q: %w", p.OldName, err)
		}
	}
	if p.EntryType == DirEntryDirectory && p.OldParentInodeNum != p.NewParentInodeNum {
		if child, ok, err := liveDir(rctx, p.InodeNum); err != nil {
			return true, err
		} else if ok {
			if _, err := rctx.DirMgr.PointIf(ctx, child, "..", p.NewParentInodeNum, dir.DT_DIR, func(cur uint64) bool { return cur == p.OldParentInodeNum }); err != nil && !errors.Is(err, dir.ErrNotFound) && !errors.Is(err, dir.ErrRemoved) {
				return true, fmt.Errorf("failed to point the \"..\" of directory %d at %d: %w", p.InodeNum, p.NewParentInodeNum, err)
			}
		}
	}
	log.Printf("[Recovery] DirRename: %q in %d is %q in %d", p.OldName, p.OldParentInodeNum, p.NewName, p.NewParentInodeNum)
	return true, nil
}

// renameOvertaken reports whether an inode the rename writes once under its locks is freed or more than one write past
// the generation it logged. Another operation took the lock after the rename finished, so its names must stay.
func renameOvertaken(rctx *RecoveryContext, p *DirRenamePayload) (bool, error) {
	logged := map[uint64]uint64{p.OldParentInodeNum: p.OldParentGen, p.NewParentInodeNum: p.NewParentGen}
	if p.EntryType == DirEntryDirectory && p.OldParentInodeNum != p.NewParentInodeNum {
		logged[p.InodeNum] = p.MetaGeneration
	}
	for logNum, gen := range logged {
		num, err := tableInode(logNum)
		if err != nil {
			return false, err
		}
		ino, err := rctx.InodeTbl.Read(num)
		if err != nil {
			return false, fmt.Errorf("failed to read inode %d: %w", num, err)
		}
		if ino.Magic != inode.InodeMagic || ino.MetaGeneration > gen+1 {
			log.Printf("[Recovery] DirRename: inode %d changed after the rename of %q to %q finished, skipping", num, p.OldName, p.NewName)
			return true, nil
		}
	}
	return false, nil
}

// placeRenamed makes the new name name the child, if it is free, names the child, or still names the logged target.
// Each parent's link count changes first, and only while the parent is as the rename logged it.
func placeRenamed(ctx context.Context, rctx *RecoveryContext, r *txnReplay, p *DirRenamePayload) (bool, error) {
	newParent, ok, err := liveDir(rctx, p.NewParentInodeNum)
	if !ok {
		return false, err
	}
	cur, _, err := rctx.DirMgr.Lookup(ctx, newParent, p.NewName)
	switch {
	case err == nil && cur == p.InodeNum:
		return true, nil
	case err == nil && (p.TargetInodeNum == 0 || cur != p.TargetInodeNum):
		log.Printf("[Recovery] DirRename: %q in %d names inode %d since, skipping", p.NewName, p.NewParentInodeNum, cur)
		return false, nil
	case err != nil && !errors.Is(err, dir.ErrNotFound):
		return false, fmt.Errorf("failed to look up %q in %d: %w", p.NewName, p.NewParentInodeNum, err)
	}

	movesDir := p.EntryType == DirEntryDirectory && p.OldParentInodeNum != p.NewParentInodeNum
	links := map[uint64]int{}
	gens := map[uint64]uint64{p.OldParentInodeNum: p.OldParentGen, p.NewParentInodeNum: p.NewParentGen}
	if movesDir {
		links[p.OldParentInodeNum]--
		links[p.NewParentInodeNum]++
	}
	if p.TargetInodeNum != 0 && p.TargetType == DirEntryDirectory {
		links[p.NewParentInodeNum]--
	}
	for logNum, delta := range links {
		if delta == 0 {
			continue
		}
		num, err := tableInode(logNum)
		if err != nil {
			return false, err
		}
		var wrote bool
		if err := rctx.InodeTbl.Update(ctx, num, func(parent *inode.Inode) error {
			if wrote = r.unchanged(parent, gens[logNum]); !wrote {
				return errParentChanged
			}
			parent.LinkCount = uint32(int64(parent.LinkCount) + int64(delta))
			return nil
		}); err != nil && !errors.Is(err, errParentChanged) {
			return false, fmt.Errorf("failed to update the link count of directory %d: %w", num, err)
		}
		if wrote {
			if parent, err := rctx.InodeTbl.Read(num); err == nil {
				r.wrote[num] = parent.MetaGeneration
			}
		}
	}

	if errors.Is(err, dir.ErrNotFound) {
		err = rctx.DirMgr.Add(ctx, newParent, p.NewName, p.InodeNum, fileTypeOf(p.EntryType))
		if errors.Is(err, dir.ErrExists) || errors.Is(err, dir.ErrRemoved) {
			log.Printf("[Recovery] DirRename: %q in %d was taken or removed meanwhile, skipping", p.NewName, p.NewParentInodeNum)
			return false, nil
		}
		return err == nil, err
	}
	pointed, err := rctx.DirMgr.PointIf(ctx, newParent, p.NewName, p.InodeNum, fileTypeOf(p.EntryType), func(cur uint64) bool { return cur == p.TargetInodeNum })
	if errors.Is(err, dir.ErrNotFound) || errors.Is(err, dir.ErrRemoved) {
		return false, nil
	}
	return pointed, err
}

// inodesInTable fails for an entry that names an inode at or past the end of
// the inode table, which only a damaged record does. Replay would otherwise
// write that number into a directory entry, or narrow it onto a real inode.
// An entry that does not decode is left for its apply function to report.
func inodesInTable(rctx *RecoveryContext, entry *RecoveredEntry) error {
	if rctx.InodeTbl == nil {
		return nil
	}
	touched, err := EntryTouches(entry.Entry.OpType, entry.Payload)
	if err != nil {
		return nil
	}
	total := rctx.InodeTbl.TotalInodes()
	for _, num := range touched.Inodes {
		if num >= total {
			return fmt.Errorf("%s entry names inode %d, past the inode table's %d inodes: %w", entry.Entry.OpType, num, total, inode.ErrInodeOutOfRange)
		}
	}
	return nil
}

// tableInode narrows a logged inode number to the inode table's uint32. Only a
// damaged record holds 2^32 or more, and narrowing that would wrap onto a real
// inode, so it fails as a number past the table does.
func tableInode(num uint64) (uint32, error) {
	if num > math.MaxUint32 {
		return 0, fmt.Errorf("inode %d: %w", num, inode.ErrInodeOutOfRange)
	}
	return uint32(num), nil
}

// liveDir reads the inode num and reports whether it is a directory in use. Another node may have removed it since
// the record was logged.
func liveDir(rctx *RecoveryContext, num uint64) (*inode.Inode, bool, error) {
	tableNum, err := tableInode(num)
	if err != nil {
		return nil, false, err
	}
	ino, err := rctx.InodeTbl.Read(tableNum)
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
	removed, err := removeLoggedEntry(ctx, rctx, p)
	if err != nil || p.EntryType != DirEntryDirectory {
		return removed, err
	}
	return removed, recountLinks(ctx, rctx, p.ParentInodeNum)
}

func removeLoggedEntry(ctx context.Context, rctx *RecoveryContext, p *DirRemoveEntryPayload) (bool, error) {
	parentIno, ok, err := liveDir(rctx, p.ParentInodeNum)
	if !ok {
		return false, err
	}

	var readErr error
	removed, err := rctx.DirMgr.RemoveIf(ctx, parentIno, p.Name, func(childNum uint64) bool {
		if childNum != p.ChildInodeNum {
			log.Printf("[Recovery] DirRemoveEntry: %q now names inode %d, not %d, skipping", p.Name, childNum, p.ChildInodeNum)
			return false
		}
		tableNum, err := tableInode(childNum)
		if err != nil {
			readErr = err
			return false
		}
		child, err := rctx.InodeTbl.Read(tableNum)
		if err != nil {
			readErr = fmt.Errorf("child inode %d not found: %w", childNum, err)
			return false
		}
		if child.MetaGeneration != p.ChildMetaGeneration {
			log.Printf("[Recovery] DirRemoveEntry: inode %d of %q changed since the unlink was logged, skipping", childNum, p.Name)
			return false
		}
		return true
	})
	switch {
	case readErr != nil:
		return false, readErr
	case errors.Is(err, dir.ErrNotFound), errors.Is(err, dir.ErrRemoved):
		log.Printf("[Recovery] DirRemoveEntry: %q is not in parent %d, skipping", p.Name, p.ParentInodeNum)
		return false, nil
	case err != nil:
		return false, fmt.Errorf("failed to remove entry %q: %w", p.Name, err)
	case !removed:
		return false, nil
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

	return updateExtents(ctx, rctx, r, p.InodeNum, func(ino *inode.Inode) (bool, error) {
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

// beforeReplayWrite, when a test sets it, runs between a replay's read of an inode and its write.
var beforeReplayWrite func(inodeNum uint32)

// updateExtents reads an inode, applies change, and writes it back when change
// reports an update. If any write landed since the read, it starts over.
func updateExtents(ctx context.Context, rctx *RecoveryContext, r *txnReplay, logNum uint64, change func(*inode.Inode) (bool, error)) (bool, error) {
	inodeNum, err := tableInode(logNum)
	if err != nil {
		return false, err
	}
	const attempts = 5
	for attempt := 1; ; attempt++ {
		ino, err := rctx.InodeTbl.Read(inodeNum)
		if err != nil {
			return false, fmt.Errorf("inode %d not found", inodeNum)
		}
		readGen := ino.MetaGeneration
		changed, err := change(ino)
		if err != nil || !changed {
			rctx.ExtentMgr.DiscardPending(ctx, ino)
			return false, err
		}
		if beforeReplayWrite != nil {
			beforeReplayWrite(inodeNum)
		}
		err = rctx.InodeTbl.WriteIfUnchanged(ctx, ino, readGen)
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
	if r.kept("ExtentFree", uint64(p.InodeNum)) {
		return false, nil
	}
	return updateExtents(ctx, rctx, r, p.InodeNum, func(ino *inode.Inode) (bool, error) {
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
