package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
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

  -release-slot N checks that slot N can be freed and lists the transactions
  in its journal. With no node mounted, it refuses until a minute after the
  slot's lease ended. While nodes have the filesystem mounted, it watches the
  slot unchanged for 40 seconds (70 for a slot a peer was replaying) instead,
  and refuses a journal that holds a committed transaction or that it cannot
  read: stop the cluster for those. It writes nothing until you pick one of:
    -salvage          replay the journal until a transaction fails to apply
                      or cannot be read, then discard the rest
    -discard-journal  discard the whole journal unread; while a mount could
                      still replay it, this also needs -force
  Either resets the journal and frees the slot, and fsck then exits 1.
  -report FILE also writes what happened to each transaction to FILE. Run
  fsck afterwards: a discarded or failed transaction may be partly applied.
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

// releaseSlot checks slot i and prints what releasing it replays and
// discards. With salvage or discard set it releases the slot, and with a
// report path it also writes that text to the file.
func releaseSlot(out io.Writer, checker *fsck.Checker, i int, salvage, discard, force bool, reportPath string) (err error) {
	plan, err := checker.PlanRelease(i, force || !discard)
	if err != nil {
		return err
	}
	var record bytes.Buffer
	if reportPath != "" {
		fmt.Fprintf(&record, "goclusterfs-fsck -release-slot %d%s at %s UTC\n", i, releaseFlags(salvage, discard, force), time.Now().UTC().Format(time.DateTime))
	}
	p := &printer{w: io.MultiWriter(out, &record)}
	s := plan.Slot
	p.printf("Slot %d: %s, %s, lease %s\n", s.Index, s.Name(), s.Slot.State, lease(s))
	p.printf("Journal: %s\n", s.Journal)
	if plan.Online != "" {
		p.printf("Online: %s has the filesystem mounted. The journal holds no committed transaction, so releasing the slot "+
			"loses nothing; fsck replays nothing, and the node's unused block reserve stays allocated until goclusterfs-fsck -repair.\n", plan.Online)
	}
	if reportPath != "" {
		// Discard nothing the report could not record.
		if err := os.WriteFile(reportPath, record.Bytes(), 0o644); err != nil {
			return fmt.Errorf("write the report: %w; nothing was written to the device", err)
		}
		defer func() {
			if err != nil {
				fmt.Fprintf(&record, "Error: %v\n", err)
			}
			if werr := os.WriteFile(reportPath, record.Bytes(), 0o644); werr != nil && err == nil {
				err = fmt.Errorf("the release finished, but writing the report failed: %w", werr)
			}
		}()
	}

	if !salvage && !discard {
		p.printf("-discard-journal discards %s.\n", plan.Discards)
		printTxns(p, plan.Journal, func(fsck.PendingTxn) string { return "" })
		if plan.Replayable != "" {
			p.printf("Replay: %s. Mount a node to replay it, or use -salvage; -discard-journal needs -force.\n", plan.Replayable)
		}
		p.printf("Nothing was written. Run again with -salvage to replay the transactions that apply and discard the rest, " +
			"or with -discard-journal to discard the whole journal; either frees the slot.\n")
		return p.err
	}
	if p.err != nil {
		return p.err
	}

	salvaged, err := checker.Release(plan, salvage)
	switch {
	case salvage && salvaged != nil:
		p.printf("Salvage replayed %d of %d committed transactions:\n", len(salvaged.Replayed), len(plan.Journal.Pending))
		printTxns(p, plan.Journal, func(t fsck.PendingTxn) string {
			switch {
			case salvaged.Replayed[t.Seq]:
				return "replayed"
			case t.Seq == salvaged.FailedSeq:
				return "failed"
			}
			return "discarded"
		})
		switch {
		case salvaged.FailedSeq != 0:
			p.printf("Txn %d failed: %v. It may be partly applied; the transactions after it were discarded.\n", salvaged.FailedSeq, salvaged.Err)
		case salvaged.Err != nil:
			p.printf("Salvage stopped: %v. What follows was discarded.\n", salvaged.Err)
		}
	case salvage && plan.Online != "" && err == nil:
		p.printf("Nothing to replay: the journal holds no committed transaction.\n")
	case discard && err == nil:
		p.printf("Discarded %s:\n", plan.Discards)
		printTxns(p, plan.Journal, func(fsck.PendingTxn) string { return "discarded" })
	}
	if err != nil {
		return err
	}
	p.printf("Released slot %d: its journal is reset and a node can register in it. Run goclusterfs-fsck now: "+
		"discarded or failed transactions may be partly applied.\n", i)
	return p.err
}

func releaseFlags(salvage, discard, force bool) string {
	var flags string
	if salvage {
		flags += " -salvage"
	}
	if discard {
		flags += " -discard-journal"
	}
	if force {
		flags += " -force"
	}
	return flags
}

// printTxns lists the journal's pending transactions, each after what mark says the release does with it.
func printTxns(p *printer, in *fsck.SlotInspection, mark func(fsck.PendingTxn) string) {
	for _, t := range in.Pending {
		prefix := mark(t)
		if prefix != "" {
			prefix = fmt.Sprintf("%-9s ", prefix)
		}
		p.printf("  %s%s\n", prefix, txnLine(t))
		if t.DecodeErr != nil {
			p.printf("    a replay fails here: %v\n", t.DecodeErr)
		}
	}
	if in.ScanErr != nil {
		p.printf("  the scan stopped after txn %s: %v. A replay stops there too.\n", lastSeq(in.Pending), in.ScanErr)
	}
}

func txnLine(t fsck.PendingTxn) string {
	line := fmt.Sprintf("txn %d at %s: %s", t.Seq, t.Time.Format(time.DateTime), strings.Join(t.Ops, ", "))
	if len(t.Inodes) > 0 {
		line += "; inodes " + joinNumbers(t.Inodes)
	}
	if len(t.Blocks) > 0 {
		line += "; blocks " + blockRuns(t.Blocks)
	}
	return line
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
	printTxns(p, in, func(fsck.PendingTxn) string { return "" })
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
