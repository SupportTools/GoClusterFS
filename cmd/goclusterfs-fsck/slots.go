package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/node"
)

const slotsHelp = `
Node registry slots:
  Every mounted node holds a registry slot with its own journal. When a node
  crashes, a peer replays its journal and frees the slot, or a mount on any
  host takes the slot and replays the journal first. A slot whose journal
  cannot be replayed, for example because its header is damaged, stays
  taken. -list-slots shows each slot that is in use or whose journal needs
  attention.

  -release-slot N checks that slot N can be freed and prints what freeing it
  would discard. It refuses while any node has the filesystem mounted, until
  a minute after the slot's lease ended, and, without -force, while a mount
  could still replay the journal. Add -discard-journal to reset the slot's
  journal and free the slot; fsck then exits 1.
`

// printSlots lists the slots that are in use or whose journal was written
// and is not clean.
func printSlots(out io.Writer, checker *fsck.Checker) error {
	slots, err := checker.Slots()
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	p := &printer{w: w}
	p.printf("SLOT\tSTATE\tHOST\tNODE\tLEASE\tJOURNAL\n")
	shown := 0
	for _, s := range slots {
		if !s.InUse() && s.JournalFresh {
			continue
		}
		state, host, uuid := "unreadable: "+fmt.Sprint(s.Err), "-", "-"
		if s.Slot != nil {
			state = s.Slot.State.String()
			host = strings.TrimRight(string(s.Slot.Hostname[:]), "\x00")
			uuid = fmt.Sprintf("%x", s.Slot.UUID[:8])
		}
		p.printf("%d\t%s\t%s\t%s\t%s\t%s\n", s.Index, state, host, uuid, lease(s), s.Journal)
		shown++
	}
	if p.err != nil {
		return p.err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "\n%d of %d slots shown; the others are free, with a journal that is clean or was never used.\n", shown, len(slots))
	return err
}

// releaseSlot checks slot i and prints what releasing it discards. With
// discard set it also releases the slot.
func releaseSlot(out io.Writer, checker *fsck.Checker, i int, discard, force bool) error {
	plan, err := checker.PlanRelease(i, force)
	if err != nil {
		return err
	}
	s := plan.Slot
	p := &printer{w: out}
	p.printf("Slot %d: %s, %s, lease %s\n", s.Index, s.Name(), s.Slot.State, lease(s))
	p.printf("Journal: %s\n", s.Journal)
	p.printf("Releasing discards %s.\n", plan.Discards)
	if !discard {
		p.printf("Nothing was written. Run again with -discard-journal to reset the journal and free the slot.\n")
		return p.err
	}
	// Discard nothing the user could not be shown.
	if p.err != nil {
		return p.err
	}
	if err := checker.Release(plan); err != nil {
		return err
	}
	p.printf("Released slot %d: its journal is reset and a node can register in it.\n", i)
	return p.err
}

// printer keeps the first write error and skips writes after it.
type printer struct {
	w   io.Writer
	err error
}

func (p *printer) printf(format string, args ...any) {
	if p.err == nil {
		_, p.err = fmt.Fprintf(p.w, format, args...)
	}
}

func lease(s fsck.SlotStatus) string {
	switch {
	case s.Running:
		return "held (node running)"
	case s.Slot == nil || s.Slot.State == node.NodeStateEmpty:
		return "-"
	}
	expiry := s.Slot.LeaseExpiry()
	if left := time.Until(expiry); left > 0 {
		return "ends in " + left.Round(time.Second).String()
	}
	return "ended " + time.Since(expiry).Round(time.Second).String() + " ago"
}
