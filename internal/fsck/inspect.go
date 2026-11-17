package fsck

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/supporttools/GoClusterFS/internal/journal"
)

// SlotInspection is what -inspect-slot reports about one node registry slot. Gathering it writes nothing.
type SlotInspection struct {
	Status        SlotStatus
	JournalOffset uint64
	JournalSize   uint64
	// Header is nil when the journal was never initialized or HeaderErr says why it cannot be read.
	Header    *journal.JournalSuperblock
	HeaderErr error
	// Pending are the committed transactions a replay would apply, in order; ScanErr says why the scan stopped early.
	Pending []PendingTxn
	Stats   journal.RecoveryStats
	ScanErr error
}

// PendingTxn is one committed transaction a replay would apply.
type PendingTxn struct {
	Seq       uint64
	Time      time.Time
	Ops       []string
	Inodes    []uint64
	Blocks    []journal.BlockRun
	DecodeErr error // an entry a replay would fail to decode
}

// InspectSlot describes slot i, its journal and what replaying the journal would change.
func (c *Checker) InspectSlot(i int) (*SlotInspection, error) {
	slots, err := c.Slots()
	if err != nil {
		return nil, err
	}
	if i < 0 || i >= len(slots) {
		return nil, fmt.Errorf("%w: slot %d; slots are 0 to %d", ErrNoSuchSlot, i, len(slots)-1)
	}
	return c.inspect(slots[i]), nil
}

// inspect reads the journal of slot s and what replaying it would change.
func (c *Checker) inspect(s SlotStatus) *SlotInspection {
	in := &SlotInspection{Status: s}
	in.JournalOffset, in.JournalSize = c.journalArea(s.Index)
	region, err := c.slotJournal(s.Index)
	switch {
	case errors.Is(err, journal.ErrJournalUninitialized):
		return in
	case err != nil:
		in.HeaderErr = rootCause(err)
		return in
	}
	header := region.Superblock()
	in.Header = &header
	txns, stats, err := journal.DryRunRecover(region)
	if stats != nil {
		in.Stats = *stats
	}
	in.ScanErr = err
	for _, txn := range txns {
		in.Pending = append(in.Pending, pendingTxn(txn))
	}
	return in
}

func pendingTxn(txn *journal.RecoveredTxn) PendingTxn {
	p := PendingTxn{Seq: txn.Header.Seq, Time: time.Unix(0, txn.Header.Timestamp).UTC()}
	for _, e := range txn.Entries {
		p.Ops = append(p.Ops, e.Entry.OpType.String())
		touched, err := journal.EntryTouches(e.Entry.OpType, e.Payload)
		if err != nil {
			p.DecodeErr = cmp.Or(p.DecodeErr, err)
			continue
		}
		p.Inodes = append(p.Inodes, touched.Inodes...)
		p.Blocks = append(p.Blocks, touched.Blocks...)
	}
	slices.Sort(p.Inodes)
	p.Inodes = slices.Compact(p.Inodes)
	slices.SortFunc(p.Blocks, func(a, b journal.BlockRun) int { return cmp.Compare(a.Start, b.Start) })
	return p
}
