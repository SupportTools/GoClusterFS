package goclusterfs

import (
	"context"
	"fmt"
	"log"
	"maps"
	"slices"
	"time"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/quota"
)

// lastQuotaFlushTimeout bounds the flush at unmount. A journal left with usage no flush covered stays dirty, and a
// replay folds it.
const lastQuotaFlushTimeout = 30 * time.Second

// StartQuotaFlush moves this node's quota ledger into the table every interval, and early once the journal's holds
// press on its space, until Close.
func (f *FS) StartQuotaFlush(interval time.Duration) {
	if f.quotaInode == 0 || f.journal == nil {
		return
	}
	kick := make(chan struct{}, 1)
	f.quotaStop, f.quotaDone = make(chan struct{}), make(chan struct{})
	f.journal.SetQuotaPressureHook(func() {
		select {
		case kick <- struct{}{}:
		default:
		}
	})
	go func() {
		defer close(f.quotaDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-f.quotaStop:
				return
			case <-ticker.C:
			case <-kick:
			}
			if err := f.flushQuota(context.Background()); err != nil {
				log.Printf("[WARN] quota: flushing usage into the table failed: %v", err)
			}
		}
	}()
}

func (f *FS) stopQuotaFlush() {
	if f.quotaStop == nil {
		return
	}
	close(f.quotaStop)
	<-f.quotaDone
	ctx, cancel := context.WithTimeout(context.Background(), lastQuotaFlushTimeout)
	defer cancel()
	if err := f.flushQuota(ctx); err != nil {
		log.Printf("[WARN] quota: the last flush failed: %v; the journal stays for a replay to fold", err)
	}
}

// flushQuota moves this node's quota ledger into the table. Under the quota lock it reads each pending project's
// usage, commits one QuotaFlush per project for its whole ledger, writes the usage, and completes the commit durably
// before the lock goes: a replay writes an unfinished flush again, over whatever usage is newer. Changes of a project
// whose record is free or holds another project now are dropped. A project left with no usage and no root directory
// gives its record up.
func (f *FS) flushQuota(ctx context.Context) error {
	if f.quotaInode == 0 || f.journal == nil || f.quotaStuck.Load() {
		return nil
	}
	pending := f.journal.QuotaPending()
	if len(pending) == 0 {
		return nil
	}
	unlock, err := f.quotaGuard.Lock(ctx)
	if err != nil {
		return err
	}
	keepLock := false
	defer func() {
		if !keepLock {
			unlock()
		}
	}()
	if f.quotaGuard.Adopted() {
		return nil
	}
	tbl, err := f.quotaTable()
	if err != nil {
		return err
	}
	projects := slices.Sorted(maps.Keys(pending))
	used := make(map[uint32]*quota.Record, len(projects))
	for _, p := range projects {
		if used[p], err = tbl.Record(p); err != nil {
			return fmt.Errorf("project %d: %w", p, err)
		}
	}

	var flushed []journal.QuotaUpdatePayload
	txn := f.journal.Begin()
	seq, err := f.journal.CommitQuota(txn, func(ledger func(uint32) journal.QuotaDeltaPayload) error {
		for _, p := range projects {
			d, r := ledger(p), used[p]
			u := journal.QuotaUpdatePayload{Project: p, CoveredBlocks: d.Blocks, CoveredInodes: d.Inodes}
			if r.Owns(p) {
				u.OldBlocks, u.OldInodes = r.UsedBlocks, r.UsedInodes
				u.NewBlocks = journal.AddUsage(r.UsedBlocks, d.Blocks, p, "blocks")
				u.NewInodes = journal.AddUsage(r.UsedInodes, d.Inodes, p, "inodes")
			}
			if err := txn.LogQuotaFlush(&u); err != nil {
				return err
			}
			flushed = append(flushed, u)
		}
		return nil
	})
	if err != nil {
		f.journal.Abort(txn)
		return err
	}
	for _, u := range flushed {
		rec := *used[u.Project]
		if !rec.Owns(u.Project) {
			f.noteRecord(u.Project, &rec)
			continue
		}
		rec.UsedBlocks, rec.UsedInodes = u.NewBlocks, u.NewInodes
		if rec.UsedBlocks == 0 && rec.UsedInodes == 0 && !f.rootCarries(&rec, u.Project) {
			rec = quota.Record{ID: u.Project}
			log.Printf("quota: project %d has no root directory and nothing left in it; its record is free", u.Project)
		}
		if err := tbl.PutRecord(u.Project, &rec); err != nil {
			keepLock = true
			return f.quotaFlushFailed(fmt.Errorf("writing the usage of project %d: %w", u.Project, err))
		}
		f.noteRecord(u.Project, &rec)
	}
	if err := f.journal.CompleteIntentDurable(seq); err != nil {
		keepLock = true
		return f.quotaFlushFailed(fmt.Errorf("completing the flush: %w", err))
	}
	return nil
}

// rootCarries reports whether the root directory rec names is still in project id; it says yes when it cannot tell.
func (f *FS) rootCarries(rec *quota.Record, id uint32) bool {
	root, err := f.inodeTbl.Read(rec.RootInode)
	if err != nil {
		return true
	}
	return root.Magic == inode.InodeMagic && root.BirthGeneration == rec.RootBirth && root.ProjectID == id
}

// quotaFlushFailed handles a flush that failed after its commit. A replay of this node's journal writes the flush
// again over whatever usage the table holds then, so this node keeps the quota lock, which keeps every node off the
// table, stops flushing, and stops the node when the mount set how.
func (f *FS) quotaFlushFailed(err error) error {
	f.quotaStuck.Store(true)
	reason := fmt.Sprintf("quota: %v after the flush was logged; the quota lock stays held until a replay of this node's journal writes the flush again", err)
	log.Printf("[ERROR] %s", reason)
	if f.stopNode != nil {
		f.stopNode(reason)
	}
	return err
}

// SetStopNode sets how this node stops when it cannot go on safely, such as after a quota flush failed halfway.
func (f *FS) SetStopNode(stop func(reason string)) {
	f.stopNode = stop
}
