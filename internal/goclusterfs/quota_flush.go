package goclusterfs

import (
	"context"
	"fmt"
	"log"
	"maps"
	"slices"
	"time"

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
// before the lock goes: a replay writes an unfinished flush again, over whatever usage is newer.
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
	defer unlock()
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
			u := journal.QuotaUpdatePayload{Project: p, CoveredBlocks: d.Blocks, CoveredInodes: d.Inodes,
				OldBlocks: r.UsedBlocks, OldInodes: r.UsedInodes,
				NewBlocks: journal.AddUsage(r.UsedBlocks, d.Blocks, p, "blocks"),
				NewInodes: journal.AddUsage(r.UsedInodes, d.Inodes, p, "inodes")}
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
		if err := tbl.SetUsage(u.Project, u.NewBlocks, u.NewInodes); err != nil {
			f.quotaStuck.Store(true)
			log.Printf("[ERROR] quota: writing the usage of project %d failed: %v. This node stops flushing; its journal keeps the flush for the next replay", u.Project, err)
			return err
		}
	}
	if err := f.journal.CompleteIntentDurable(seq); err != nil {
		f.quotaStuck.Store(true)
		log.Printf("[ERROR] quota: completing a flush failed: %v. This node stops flushing; its journal keeps the flush for the next replay", err)
		return err
	}
	return nil
}
