package quota

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
)

// Guard is the quota lock, which every write of usage to the table holds. The lock manager lets every goroutine of a
// node in once the node holds a key, so a process mutex keeps this node's flushes and folds apart too.
type Guard struct {
	mu    sync.Mutex
	locks *lock.Manager
}

// NewGuard returns the quota lock of a node with lock manager locks, which may be nil on a single node.
func NewGuard(locks *lock.Manager) *Guard {
	return &Guard{locks: locks}
}

// Lock takes the quota lock and returns the func that releases it.
func (g *Guard) Lock(ctx context.Context) (func(), error) {
	g.mu.Lock()
	if g.locks == nil {
		return g.mu.Unlock, nil
	}
	owner := lock.NewOwner()
	if err := g.locks.AcquireAs(ctx, owner, lock.LockTypeQuota, 0, lock.LockModeExclusive); err != nil {
		g.mu.Unlock()
		return nil, err
	}
	return func() {
		if err := g.locks.ReleaseAs(owner, lock.LockTypeQuota, 0); err != nil {
			log.Printf("[WARN] quota: releasing the quota lock failed: %v", err)
		}
		g.mu.Unlock()
	}, nil
}

// Adopted reports whether this node keeps the quota lock of a dead node for its replay. This node's own acquires
// pass over that claim, so a flush must not write until the replay ends: its fold may not have run yet.
func (g *Guard) Adopted() bool {
	return g.locks != nil && g.locks.Adopted(lock.LockTypeQuota, 0)
}

// Fold folds what the dirty journal in region owes the table, before replay begins. slot and owner are the journal's
// registry slot and that slot's node; ownSlot is this node's. Under the quota lock Fold appends the fold to the
// journal, writes the usage and completes the fold. For another node's slot it keeps a claim on the lock in that
// slot meanwhile: if this node dies halfway, peers stay off the table until a replay of the slot redoes the fold.
func (g *Guard) Fold(ctx context.Context, open func() (Table, error), slot, ownSlot int, owner [16]byte, region *journal.Region) error {
	q, err := journal.ReadQuotaReplay(region)
	if err != nil {
		return err
	}
	if !q.Owes() {
		return nil
	}
	unlock, err := g.Lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	tbl, err := open()
	if err != nil {
		return err
	}
	folds, err := q.Fold(func(project uint32) (uint64, uint64, error) {
		r, err := tbl.Record(project)
		if err != nil {
			return 0, 0, err
		}
		return r.UsedBlocks, r.UsedInodes, nil
	})
	if err != nil {
		return err
	}
	seq := q.FoldSeq
	if !q.Folded && len(folds) == 0 {
		return nil
	}
	if slot != ownSlot && g.locks != nil {
		release, err := g.locks.KeepFor(lock.LockTypeQuota, 0, owner, slot)
		if err != nil {
			return fmt.Errorf("keeping the quota lock in slot %d: %w", slot, err)
		}
		defer func() {
			if err := release(); err != nil {
				log.Printf("[WARN] quota: clearing the quota lock kept in slot %d failed: %v", slot, err)
			}
		}()
	}
	if !q.Folded {
		if seq, err = journal.AppendQuotaFold(region, folds); err != nil {
			return err
		}
	}
	for _, f := range folds {
		if err := tbl.SetUsage(f.Project, f.NewBlocks, f.NewInodes); err != nil {
			return fmt.Errorf("project %d: %w", f.Project, err)
		}
	}
	if err := journal.CompleteQuotaFold(region, seq); err != nil {
		return err
	}
	log.Printf("quota: folded the usage of %d projects from the journal of slot %d", len(folds), slot)
	return nil
}
