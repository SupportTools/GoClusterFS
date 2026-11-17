package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/journal"
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

  -inspect-slot N describes slot N: its node, lease and orphans, its journal
  header, and every committed transaction a replay would apply, with the
  inodes and blocks each changes. It writes nothing, so it runs while the
  filesystem is mounted.

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

// inspectSlot prints what fsck.InspectSlot finds about slot i.
func inspectSlot(out io.Writer, checker *fsck.Checker, i int) error {
	in, err := checker.InspectSlot(i)
	if err != nil {
		return err
	}
	p := &printer{w: out}
	s := in.Status
	if s.Slot == nil {
		p.printf("Slot %d: unreadable: %v\n", s.Index, s.Err)
	} else {
		sl := s.Slot
		p.printf("Slot %d: %s, %s, node %x, lease %s\n", s.Index, s.Name(), sl.State, sl.UUID[:8], lease(s))
		if sl.State != node.NodeStateEmpty {
			p.printf("  mounted %s, last heartbeat %s, boot %x, pid %d\n", unixTime(sl.MountTime), unixTime(sl.HeartbeatTime), sl.BootID[:8], sl.PID)
		}
		if sl.RecoveryOwner != ([16]byte{}) {
			p.printf("  journal replay lease held by node %x, last renewed %s\n", sl.RecoveryOwner[:8], unixTime(sl.RecoveryStart))
		}
		if len(sl.Orphans) > 0 {
			p.printf("  %d unlinked files it may still have open, inodes %s\n", len(sl.Orphans), orphanInodes(sl.Orphans))
		}
	}
	p.printf("Journal at offset %d, %d MB: ", in.JournalOffset, in.JournalSize>>20)
	switch {
	case in.HeaderErr != nil:
		p.printf("header damaged: %v. A replay fails here; nothing in the journal can be read.\n", in.HeaderErr)
	case in.Header == nil:
		p.printf("never initialized.\n")
	default:
		printPending(p, in)
	}
	p.printf("Nothing was written.\n")
	return p.err
}

func printPending(p *printer, in *fsck.SlotInspection) {
	h := in.Header
	p.printf("%s, written by node %x, generation %d.\n", strings.ToLower(h.State.String()), h.NodeUUID[:8], h.NodeGeneration)
	p.printf("  %d committed transactions to replay, %d incomplete at the end (a replay drops those).\n", len(in.Pending), in.Stats.TxnIncomplete)
	for _, t := range in.Pending {
		p.printf("  txn %d at %s: %s", t.Seq, t.Time.Format(time.DateTime), strings.Join(t.Ops, ", "))
		if len(t.Inodes) > 0 {
			p.printf("; inodes %s", joinNumbers(t.Inodes))
		}
		if len(t.Blocks) > 0 {
			p.printf("; blocks %s", blockRuns(t.Blocks))
		}
		p.printf("\n")
		if t.DecodeErr != nil {
			p.printf("    a replay cannot decode it: %v\n", t.DecodeErr)
		}
	}
	if in.ScanErr != nil {
		p.printf("  the scan stopped after txn %s: %v. A replay fails here too.\n", lastSeq(in.Pending), in.ScanErr)
	}
}

func unixTime(sec int64) string {
	if sec == 0 {
		return "never"
	}
	t := time.Unix(sec, 0)
	return fmt.Sprintf("%s (%s ago)", t.UTC().Format(time.DateTime), time.Since(t).Round(time.Second))
}

func orphanInodes(orphans []node.Orphan) string {
	nums := make([]uint64, len(orphans))
	for i, o := range orphans {
		nums[i] = uint64(o.Inode)
	}
	return joinNumbers(nums)
}

func joinNumbers(nums []uint64) string {
	parts := make([]string, len(nums))
	for i, n := range nums {
		parts[i] = fmt.Sprint(n)
	}
	return strings.Join(parts, ", ")
}

func blockRuns(runs []journal.BlockRun) string {
	parts := make([]string, len(runs))
	for i, r := range runs {
		if r.Count == 1 {
			parts[i] = fmt.Sprint(r.Start)
		} else {
			parts[i] = fmt.Sprintf("%d-%d", r.Start, r.Start+r.Count-1)
		}
	}
	return strings.Join(parts, ", ")
}

func lastSeq(pending []fsck.PendingTxn) string {
	if len(pending) == 0 {
		return "none"
	}
	return fmt.Sprint(pending[len(pending)-1].Seq)
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
