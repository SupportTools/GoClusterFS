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
  Every mounted node holds a registry slot with its own journal. A crashed
  node's slot frees once another node replays its journal, or when the same
  host mounts again. -list-slots shows each slot that is in use or whose
  journal needs attention.

  -release-slot N frees the slot of a node that will not return, for example
  one whose journal header is damaged. It refuses while any node has the
  filesystem mounted, or until a minute after the slot's lease ended, and
  prints what releasing would discard. Add -discard-journal to reset the
  slot's journal and free the slot.
`

// printSlots lists the slots that are in use or whose journal was written
// and is not clean.
func printSlots(out io.Writer, checker *fsck.Checker) error {
	slots, err := checker.Slots()
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SLOT\tSTATE\tHOST\tNODE\tLEASE\tJOURNAL")
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
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\n", s.Index, state, host, uuid, lease(s), s.Journal)
		shown++
	}
	if err := w.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(out, "\n%d of %d slots shown; the others are free, with a journal that is clean or was never used.\n", shown, len(slots))
	return nil
}

// releaseSlot checks slot i and prints what releasing it discards. With
// discard set it also releases the slot.
func releaseSlot(out io.Writer, checker *fsck.Checker, i int, discard bool) error {
	plan, err := checker.PlanRelease(i)
	if err != nil {
		return err
	}
	s := plan.Slot
	fmt.Fprintf(out, "Slot %d: %s, %s, lease %s\n", s.Index, s.Name(), s.Slot.State, lease(s))
	fmt.Fprintf(out, "Journal: %s\n", s.Journal)
	fmt.Fprintf(out, "Releasing discards %s.\n", plan.Discards)
	if !discard {
		fmt.Fprintln(out, "Nothing was written. Run again with -discard-journal to reset the journal and free the slot.")
		return nil
	}
	if err := checker.Release(plan); err != nil {
		return err
	}
	fmt.Fprintf(out, "Released slot %d: its journal is reset and a node can register in it.\n", i)
	return nil
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
