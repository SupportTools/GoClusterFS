package fsck

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// ReleaseMargin is how long a slot's lease must have been over before fsck
// releases the slot, so a node that is only slow, or whose clock lags, keeps it.
const ReleaseMargin = time.Minute

// ErrNoSuchSlot means a release named a slot the registry does not have.
var ErrNoSuchSlot = errors.New("no such node registry slot")

// Test hooks: beforeReleaseWrites runs after Release checks the registry, and
// whileReleaseHoldsLease runs once Release holds the slot's recovery lease.
var beforeReleaseWrites, whileReleaseHoldsLease func()

// SlotStatus describes a node registry slot and its journal.
type SlotStatus struct {
	Index   int
	Slot    *node.Slot // nil when the slot is unreadable
	Err     error      // why the slot is unreadable
	Running bool       // the slot changed while fsck watched the registry
	Journal string     // the journal's state, or why it cannot be read
	// JournalFresh means the journal was never initialized or is clean.
	JournalFresh bool

	raw []byte
}

// InUse reports whether the slot belongs to a node or is unreadable.
func (s SlotStatus) InUse() bool {
	return s.Slot == nil || s.Slot.State != node.NodeStateEmpty
}

// Name names the slot's node.
func (s SlotStatus) Name() string {
	return slotName(s.Index, s.Slot)
}

// Slots reads the superblock and describes every node registry slot. It first
// watches the registry for running nodes, for up to 11 seconds when a
// heartbeat is recent.
func (c *Checker) Slots() ([]SlotStatus, error) {
	if err := c.checkSuperblock(); err != nil {
		return nil, fmt.Errorf("superblock: %w", err)
	}
	slots, _ := node.NewRegistry(c.dev, c.sbData.NodeRegistryOffset).Slots()
	running, err := c.watchRegistry(slots)
	if err != nil {
		return nil, fmt.Errorf("read the node registry: %w", err)
	}

	status := make([]SlotStatus, min(len(c.registry)/node.SlotSize, int(c.sbData.MaxNodes)))
	for i := range status {
		raw := c.registry[i*node.SlotSize : (i+1)*node.SlotSize]
		s := SlotStatus{Index: i, Running: running[i], raw: raw}
		s.Slot, s.Err = node.ParseSlot(raw)
		region, err := c.slotJournal(i)
		s.Journal = journalState(region, err)
		s.JournalFresh = errors.Is(err, journal.ErrJournalUninitialized) ||
			err == nil && region.State() == journal.JournalStateClean
		status[i] = s
	}
	return status, nil
}

// ReleasePlan is a checked request to release one node registry slot.
type ReleasePlan struct {
	Slot     SlotStatus
	Discards string // what resetting the slot's journal throws away
	// Replayable says why a mount could still replay the journal; empty when it could not.
	Replayable string
	Journal    *SlotInspection
	// Online names a node that has the filesystem mounted; empty when none does.
	Online string
}

// How long a release with nodes running watches the slot unchanged before it
// counts the slot's node as gone: twice as long as peers wait before they
// replay a node, or past a peer's recovery lease for a dead slot.
var (
	onlineActiveWatch = 4 * node.HeartbeatTimeout
	onlineDeadWatch   = time.Minute + node.HeartbeatTimeout
)

// PlanRelease checks that slot i can be released: mount would accept the
// superblock, the slot is readable, and its node is gone. With no node
// running, the slot's lease must have ended ReleaseMargin ago, and unless
// force is set, a mount must not be able to replay its journal instead. With
// nodes running, fsck watches the slot unchanged instead, and refuses a
// journal that holds a committed transaction or that it cannot read, since
// nothing may be lost while nodes use the filesystem. It writes nothing.
func (c *Checker) PlanRelease(i int, force bool) (*ReleasePlan, error) {
	if _, err := superblock.Read(c.dev); err != nil {
		return nil, fmt.Errorf("release refused: mount would reject the superblock (%v); run fsck first", err)
	}
	slots, err := c.Slots()
	if err != nil {
		return nil, err
	}
	if i < 0 || i >= len(slots) {
		return nil, fmt.Errorf("%w: slot %d; slots are 0 to %d", ErrNoSuchSlot, i, len(slots)-1)
	}
	online := ""
	for _, s := range slots {
		switch {
		case s.Running && s.Index == i:
			return nil, fmt.Errorf("release refused: the filesystem is mounted by %s, the node in that slot", s.Name())
		case s.Running && online == "":
			online = s.Name()
		}
	}
	s := slots[i]
	if s.Err != nil {
		return nil, fmt.Errorf("release refused: slot %d is unreadable (%v)", i, s.Err)
	}
	if online != "" {
		return c.planOnline(s, online)
	}
	if after := s.Slot.LeaseExpiry().Add(ReleaseMargin); time.Now().Before(after) {
		return nil, fmt.Errorf("release refused: %s may still be alive; run again after %s",
			s.Name(), after.Format(time.DateTime))
	}
	discards, keep := journalContents(c.slotJournal(i))
	if keep != "" && !force {
		return nil, fmt.Errorf("release refused: slot %d's journal holds %s, and %s; "+
			"mount a node to replay it, add -salvage to replay it here, or add -force to discard it", i, discards, keep)
	}
	return &ReleasePlan{Slot: s, Discards: discards, Replayable: keep, Journal: c.inspect(s)}, nil
}

// planOnline checks slot s for a release while the node online names has the
// filesystem mounted: the journal holds no committed transaction, and the slot
// stays unchanged while fsck watches it on its own clock.
func (c *Checker) planOnline(s SlotStatus, online string) (*ReleasePlan, error) {
	offset, size := c.journalArea(s.Index)
	pending, err := journal.PendingCommitted(c.dev, offset, size)
	loss := ""
	switch {
	case err != nil:
		loss = fmt.Sprintf("fsck cannot tell what slot %d's journal holds (%v)", s.Index, rootCause(err))
	case pending == 1:
		loss = fmt.Sprintf("slot %d's journal holds 1 committed transaction that releasing would drop", s.Index)
	case pending > 1:
		loss = fmt.Sprintf("slot %d's journal holds %d committed transactions that releasing would drop", s.Index, pending)
	}
	if loss != "" {
		return nil, fmt.Errorf("release refused: %s has the filesystem mounted, and %s. Stop the cluster: unmount every node, "+
			"release the slot with -salvage or -discard-journal, then run goclusterfs-fsck -repair before mounting again", online, loss)
	}
	if err := c.watchSlot(s, online); err != nil {
		return nil, err
	}
	discards, _ := journalContents(c.slotJournal(s.Index))
	return &ReleasePlan{Slot: s, Discards: discards, Journal: c.inspect(s), Online: online}, nil
}

// watchSlot fails if slot s changes within the online watch for its state, since then its node is running or a peer
// is replaying it.
func (c *Checker) watchSlot(s SlotStatus, online string) error {
	watch := onlineActiveWatch
	switch s.Slot.State {
	case node.NodeStateEmpty:
		return nil
	case node.NodeStateDead:
		watch = onlineDeadWatch
	}
	_, _ = fmt.Fprintf(c.opts.Output, "%s has the filesystem mounted, so fsck watches slot %d for %s to prove its node is gone...\n",
		online, s.Index, watch)
	offset := c.sbData.NodeRegistryOffset + uint64(s.Index)*node.SlotSize
	buf := storage.NewAlignedBuffer(node.SlotSize).Bytes()
	for deadline := time.Now().Add(watch); time.Now().Before(deadline); {
		time.Sleep(node.HeartbeatInterval / 2)
		if err := c.readSector(buf, offset); err != nil {
			return fmt.Errorf("read node registry slot %d: %w", s.Index, err)
		}
		if !bytes.Equal(buf, s.raw) {
			return fmt.Errorf("release refused: slot %d changed while fsck watched it: its node is running, "+
				"or another node is replaying its journal; check -list-slots again", s.Index)
		}
	}
	return nil
}

// Release takes the plan's slot under a recovery lease, resets its journal
// and empties the slot, so a node can register there with a new journal. With
// salvage set and no node running, it first replays the journal until a
// transaction fails to apply or a record cannot be read, and returns how far
// it got. It writes nothing if a registry slot changed since PlanRelease
// checked it: any slot with no node running, or else the plan's slot.
func (c *Checker) Release(plan *ReleasePlan, salvage bool) (*Salvage, error) {
	i := plan.Slot.Index
	now, err := c.readRegistry()
	if err != nil {
		return nil, fmt.Errorf("read the node registry: %w", err)
	}
	changed := changedSlots(c.registry, now)
	if plan.Online != "" {
		// Running nodes change their slots all the time, and replay without cluster locks would race them.
		changed = slices.DeleteFunc(changed, func(slot int) bool { return slot != i })
		salvage = false
	}
	if len(changed) > 0 {
		return nil, fmt.Errorf("release stopped: node registry slot %d changed since fsck checked it; nothing was written", changed[0])
	}
	if beforeReleaseWrites != nil {
		beforeReleaseWrites()
	}

	reset := false
	var salvaged *Salvage
	resetJournal := func(renew func() error) error {
		if whileReleaseHoldsLease != nil {
			whileReleaseHoldsLease()
		}
		if salvage {
			s, err := c.salvage(i, renew)
			if err != nil {
				return err
			}
			salvaged = s
		}
		offset, _ := c.journalArea(i)
		if _, err := c.dev.WriteAt(storage.NewAlignedBuffer(storage.SectorSize).Bytes(), int64(offset)); err != nil {
			return fmt.Errorf("reset the journal: %w", err)
		}
		reset = true
		if err := c.dev.Sync(); err != nil {
			return err
		}
		if _, err := lock.ClearSlotClaims(c.dev, c.sbData.LockBitmapOffset, c.sbData.LockBitmapSize, i); err != nil {
			return fmt.Errorf("clear the slot's lock claims: %w", err)
		}
		return c.dev.Sync()
	}
	reg := node.NewRegistry(c.dev, c.sbData.NodeRegistryOffset)
	reg.SetPreempter(func(owner [16]byte, _ bool) error { return c.opts.Preempt(owner) })
	released, err := reg.ReleaseSlot(i, plan.Slot.raw, resetJournal)
	switch {
	case err != nil && reset:
		return salvaged, fmt.Errorf("release slot %d: %w; its journal is reset but the slot is not free, run again", i, err)
	case err != nil:
		return salvaged, fmt.Errorf("release slot %d: %w", i, err)
	case !released:
		return nil, fmt.Errorf("release stopped: another node took slot %d since fsck checked it; its journal was not touched", i)
	}
	return salvaged, c.dev.Sync()
}

// journalArea returns where slot i's journal starts and how long it is.
func (c *Checker) journalArea(i int) (offset, size uint64) {
	size = c.sbData.JournalRegionSize / uint64(c.sbData.MaxNodes)
	return c.sbData.JournalRegionOffset + uint64(i)*size, size
}

// slotJournal opens slot i's journal. A journal that was never initialized
// returns journal.ErrJournalUninitialized.
func (c *Checker) slotJournal(i int) (*journal.Region, error) {
	offset, size := c.journalArea(i)
	return journal.OpenRegionForRecovery(c.dev, offset, size)
}

func journalState(region *journal.Region, err error) string {
	switch {
	case errors.Is(err, journal.ErrJournalUninitialized):
		return "never initialized"
	case err != nil:
		return "damaged: " + rootCause(err).Error()
	}
	return strings.ToLower(region.State().String())
}

// journalContents says what resetting a journal throws away and, when a mount
// might replay it instead, why.
func journalContents(region *journal.Region, err error) (discards, keep string) {
	switch {
	case errors.Is(err, journal.ErrJournalUninitialized):
		return "nothing: the journal was never initialized", ""
	case err != nil:
		return "the whole journal, unread: " + rootCause(err).Error(), ""
	case region.State() == journal.JournalStateClean:
		return "nothing: the journal is clean", ""
	case !region.NeedsRecovery():
		return "the whole journal, unread: its state is " + strings.ToLower(region.State().String()), ""
	}
	_, stats, err := journal.DryRunRecover(region)
	pending := stats.TxnCommitted - stats.IntentsSkipped
	discards = fmt.Sprintf("%d committed transactions that were never replayed", pending)
	if pending == 1 {
		discards = "1 committed transaction that was never replayed"
	}
	switch {
	case errors.Is(err, journal.ErrUnknownOp):
		return discards + ", from the first one this build cannot replay on",
			fmt.Sprintf("this build cannot replay all of it (%v), but a newer build may", rootCause(err))
	case err != nil:
		return discards + ", and whatever follows a record fsck cannot read",
			fmt.Sprintf("fsck could not read all of it (%v), so a mount may still replay it", err)
	case pending > 0:
		return discards, "a mount can replay it"
	}
	return discards, ""
}

func rootCause(err error) error {
	if inner := errors.Unwrap(err); inner != nil {
		return inner
	}
	return err
}
