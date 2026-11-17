package fsck

import (
	"context"
	"errors"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/refcount"
)

// Salvage says how far a salvaging release replayed a slot's journal.
type Salvage struct {
	Replayed  map[uint64]bool // the sequence numbers of the transactions it applied
	FailedSeq uint64          // the transaction that failed to apply; 0 when none did
	Err       error           // why the replay stopped before the end; nil when it reached the end
}

// salvage replays slot i's journal until a transaction fails to apply or a
// record cannot be read. It returns an error only when renew fails, since
// another node may then hold the slot.
func (c *Checker) salvage(i int, renew func() error) (*Salvage, error) {
	s := &Salvage{Replayed: make(map[uint64]bool)}
	region, err := c.slotJournal(i)
	switch {
	case errors.Is(err, journal.ErrJournalUninitialized):
		return s, nil
	case err != nil:
		s.Err = rootCause(err)
		return s, nil
	}
	if !region.NeedsRecovery() {
		return s, nil
	}
	rctx := c.replayContext()
	apply := journal.CreateTxnRecoveryHandler(rctx)
	handler := func(txn *journal.RecoveredTxn) error {
		if err := apply(txn); err != nil {
			s.FailedSeq = txn.Header.Seq
			return err
		}
		s.Replayed[txn.Header.Seq] = true
		return nil
	}
	var leaseErr error
	check := func() error {
		leaseErr = renew()
		return leaseErr
	}
	_, s.Err = journal.RecoverReleasing(region, handler, check, rctx.ReleaseReserved(context.Background()))
	if leaseErr != nil {
		return nil, leaseErr
	}
	return s, nil
}

// replayContext builds the managers a replay needs, without locks: fsck replays only while no node runs.
func (c *Checker) replayContext() *journal.RecoveryContext {
	sb := c.sbData
	var uuid [16]byte
	bm := bitmap.NewRegion(c.dev, sb.BlockBitmapOffset, sb.TotalBlocks, nil, uuid)
	tbl := inode.NewTable(c.dev, sb.InodeTableOffset, sb.InodeTableSize, nil, uuid)
	mgr := extent.NewManager(c.dev, bm, nil, sb.DataRegionOffset)
	mgr.SetRefcounts(refcount.NewRegion(c.dev, sb.RefCountTableOffset, sb.RefCountTableSize, sb.TotalBlocks, nil))
	tbl.SetTreeFlusher(mgr)
	return &journal.RecoveryContext{Dev: c.dev, InodeTbl: tbl, BitmapMgr: bm, DirMgr: dir.NewManager(c.dev, tbl, mgr, nil, sb.DataRegionOffset), ExtentMgr: mgr}
}
