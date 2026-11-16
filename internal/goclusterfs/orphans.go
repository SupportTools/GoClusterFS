package goclusterfs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/node"
)

// An orphan is a file with no links that some node may still have open: one Holders bit per such
// registry slot. Each node clears its bit at its last close, and whoever clears the last bit frees it.
type orphanRegistry interface {
	LocalSlot() int
	LiveSlots() (uint64, error)
	AddOrphan(node.Orphan) error
	DropOrphans(func(node.Orphan) bool) error
	AllOrphans() ([]node.Orphan, error)
}

type openFiles struct {
	mu      sync.Mutex
	count   map[uint32]int
	cleared map[node.Orphan]bool
	waitMu  sync.Mutex
}

var orphanDrainWait = 2 * node.FailureCheckInterval

var errNotHeld = errors.New("orphan is not held by this node")

// SetOrphanRegistry connects orphan handling to the node registry; without one this node is alone in slot 0.
func (f *FS) SetOrphanRegistry(r orphanRegistry) {
	f.orphanReg = r
}

func (f *FS) addOpen(num uint32, d int) int {
	f.opens.mu.Lock()
	defer f.opens.mu.Unlock()
	if f.opens.count == nil {
		f.opens.count = map[uint32]int{}
	}
	n := f.opens.count[num] + d
	if n <= 0 {
		delete(f.opens.count, num)
		return 0
	}
	f.opens.count[num] = n
	return n
}

func (f *FS) HasOpen(num uint32) bool {
	return f.opened(num)
}

func (f *FS) opened(num uint32) bool {
	f.opens.mu.Lock()
	defer f.opens.mu.Unlock()
	return f.opens.count[num] > 0
}

func (f *FS) slotBit() uint64 {
	if f.orphanReg == nil || f.orphanReg.LocalSlot() < 0 {
		return 1
	}
	return 1 << f.orphanReg.LocalSlot()
}

func (f *FS) liveHolders() (uint64, error) {
	if f.orphanReg == nil {
		return f.slotBit(), nil
	}
	live, err := f.orphanReg.LiveSlots()
	return live | f.slotBit(), err
}

func orphanOf(ino *inode.Inode) node.Orphan {
	return node.Orphan{Inode: ino.InodeNum, Birth: uint32(ino.BirthGeneration)}
}

// reserveOrphan lists ino before its last link goes. A full list waits while peers free its
// orphans, and fails the unlink with EBUSY, before it changes anything, once they stop.
func (f *FS) reserveOrphan(ino *inode.Inode) (uint64, error) {
	live, err := f.liveHolders()
	if err != nil || f.orphanReg == nil || live == f.slotBit() {
		return live, err
	}
	if err = f.orphanReg.AddOrphan(orphanOf(ino)); !errors.Is(err, node.ErrOrphansFull) {
		return live, err
	}
	f.opens.waitMu.Lock()
	defer f.opens.waitMu.Unlock()
	for progress := time.Now(); ; {
		if err = f.orphanReg.AddOrphan(orphanOf(ino)); !errors.Is(err, node.ErrOrphansFull) {
			return live, err
		}
		f.notifyOrphans()
		switch {
		case f.pruneOwnOrphans() > 0:
			progress = time.Now()
		case time.Since(progress) > orphanDrainWait:
			return 0, fmt.Errorf("%w: %v", errOrphansFull, err)
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (f *FS) notifyOrphans() {
	if f.cacheMgr != nil {
		f.cacheMgr.BroadcastOrphans()
	}
}

var errOrphansFull = errors.New("too many unlinked files are still open")

// dropLink removes one link; at zero the slots in live, from reserveOrphan, hold the file.
func (f *FS) dropLink(ctx context.Context, num uint32, live uint64) error {
	unlock := f.lockInode(num)
	ino, err := f.updateUnderInodeLock(ctx, num, func(ino *inode.Inode) error {
		if ino.LinkCount == 0 {
			return fmt.Errorf("inode %d has no link to drop", num)
		}
		ino.LinkCount--
		if ino.LinkCount == 0 {
			ino.Holders = live | f.slotBit()
			if !f.opened(num) {
				ino.Holders &^= f.slotBit()
			}
		}
		return nil
	})
	unlock()
	if err != nil {
		return err
	}
	f.cacheInode(ino)
	switch {
	case ino.LinkCount != 0:
	case ino.Holders == 0:
		return f.freeOrphan(ctx, num, ino.MetaGeneration)
	case ino.Holders != f.slotBit():
		f.notifyOrphans()
	}
	return nil
}

// clearHolder clears the bits of dead slots in gone, and this node's bit unless it has o open.
func (f *FS) clearHolder(ctx context.Context, o node.Orphan, gone uint64) error {
	unlock := f.lockInode(o.Inode)
	clear := gone
	if !f.opened(o.Inode) {
		clear |= f.slotBit()
	}
	ino, err := f.updateUnderInodeLock(ctx, o.Inode, func(ino *inode.Inode) error {
		if ino.Magic != inode.InodeMagic || ino.LinkCount != 0 || orphanOf(ino) != o || ino.Holders&clear == 0 {
			return errNotHeld
		}
		ino.Holders &^= clear
		return nil
	})
	unlock()
	switch {
	case errors.Is(err, errNotHeld):
		return nil
	case err != nil:
		return err
	case ino.Holders == 0:
		if err := f.freeOrphan(ctx, o.Inode, ino.MetaGeneration); err != nil {
			return err
		}
		f.notifyOrphans()
	}
	return nil
}

func (f *FS) freeOrphan(ctx context.Context, num uint32, gen uint64) error {
	ino, err := f.inodeTbl.Read(num)
	if err != nil {
		return err
	}
	if f.journal != nil {
		txn := f.journal.Begin()
		if err := logExtentFree(txn, ino, 0); err != nil {
			f.journal.Abort(txn)
			return err
		}
		if err := txn.LogInodeDelete(&journal.InodeDeletePayload{InodeNum: uint64(num), MetaGeneration: gen}); err != nil {
			f.journal.Abort(txn)
			return err
		}
		seq, err := f.journal.Commit(txn)
		if err != nil {
			return err
		}
		defer f.closeIntent(seq)
	}
	var freed *inode.Inode
	ok, err := f.inodeTbl.FreeIf(ctx, num, func(ino *inode.Inode) bool {
		freed = ino
		return ino.MetaGeneration == gen && ino.LinkCount == 0 && ino.Holders == 0
	})
	if err != nil || !ok {
		return err
	}
	if afterCommit != nil {
		afterCommit("unlink: inode freed")
	}
	f.invalidateInode(num)
	sfbBacked, sfbSlot := freed.IsSFBBacked(), freed.InlineExtents[0].PhysicalBlock
	if err := f.extentMgr.Delete(ctx, freed); err != nil {
		log.Printf("[WARN] orphan inode %d: freeing its blocks failed, some may leak: %v", num, err)
	}
	if sfbBacked {
		f.freeSFBSlot(ctx, sfbSlot)
	}
	if afterCommit != nil {
		afterCommit("unlink: blocks freed")
	}
	if f.orphanReg != nil {
		done := orphanOf(freed)
		if err := f.orphanReg.DropOrphans(func(o node.Orphan) bool { return o == done }); err != nil && !errors.Is(err, node.ErrNotRegistered) {
			log.Printf("[WARN] orphan inode %d: removing it from this node's list failed: %v", num, err)
		}
	}
	return nil
}

// LearnOrphans lets go of each listed orphan this node does not have open.
func (f *FS) LearnOrphans(slot int, list []node.Orphan) {
	ctx := context.Background()
	for _, o := range list {
		if f.opened(o.Inode) || f.alreadyCleared(o) {
			continue
		}
		if err := f.clearHolder(ctx, o, 0); err != nil {
			log.Printf("[WARN] orphan inode %d: letting go of it failed: %v", o.Inode, err)
			continue
		}
		f.markCleared(o)
	}
	if f.orphanReg != nil && slot == f.orphanReg.LocalSlot() {
		f.pruneOwnOrphans()
	}
}

// ForgetSlot adopts the orphan list of dead slot k and clears bit k on every listed orphan.
func (f *FS) ForgetSlot(ctx context.Context, k int, dead []node.Orphan) error {
	if f.orphanReg == nil {
		return nil
	}
	for _, o := range dead {
		if err := f.orphanReg.AddOrphan(o); err != nil {
			log.Printf("[WARN] orphan inode %d of dead slot %d: listing it here failed, fsck frees it: %v", o.Inode, k, err)
		}
	}
	all, err := f.orphanReg.AllOrphans()
	if err != nil {
		return err
	}
	for _, o := range append(all, dead...) {
		if err := f.clearHolder(ctx, o, 1<<k); err != nil {
			return fmt.Errorf("orphan inode %d: clearing dead slot %d: %w", o.Inode, k, err)
		}
	}
	return nil
}

func (f *FS) pruneOwnOrphans() (dropped int) {
	err := f.orphanReg.DropOrphans(func(o node.Orphan) bool {
		ino, err := f.inodeTbl.Read(o.Inode)
		done := err == nil && (ino.Magic != inode.InodeMagic || ino.LinkCount != 0 || orphanOf(ino) != o)
		if done {
			dropped++
		}
		return done
	})
	if err != nil && !errors.Is(err, node.ErrNotRegistered) {
		log.Printf("[WARN] pruning this node's orphan list failed: %v", err)
		return 0
	}
	return dropped
}

func (f *FS) alreadyCleared(o node.Orphan) bool {
	f.opens.mu.Lock()
	defer f.opens.mu.Unlock()
	return f.opens.cleared[o]
}

func (f *FS) markCleared(o node.Orphan) {
	f.opens.mu.Lock()
	defer f.opens.mu.Unlock()
	if f.opens.cleared == nil {
		f.opens.cleared = map[node.Orphan]bool{}
	}
	if len(f.opens.cleared) > 4*node.MaxOrphans {
		clear(f.opens.cleared)
	}
	f.opens.cleared[o] = true
}
