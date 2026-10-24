package fsck

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// ReleaseMargin is how long a slot's lease must have been over before fsck
// releases the slot, so a node that is only slow, or whose clock lags, keeps it.
const ReleaseMargin = time.Minute

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
}

// PlanRelease checks that slot i can be released: no node runs, the slot is
// readable, and its lease ended ReleaseMargin ago. It writes nothing.
func (c *Checker) PlanRelease(i int) (*ReleasePlan, error) {
	slots, err := c.Slots()
	if err != nil {
		return nil, err
	}
	if i < 0 || i >= len(slots) {
		return nil, fmt.Errorf("slot %d does not exist; slots are 0 to %d", i, len(slots)-1)
	}
	for _, s := range slots {
		if s.Running {
			return nil, fmt.Errorf("release refused: the filesystem is mounted by %s; unmount it on every node first", s.Name())
		}
	}
	s := slots[i]
	if s.Err != nil {
		return nil, fmt.Errorf("release refused: slot %d is unreadable (%v)", i, s.Err)
	}
	if after := s.Slot.LeaseExpiry().Add(ReleaseMargin); time.Now().Before(after) {
		return nil, fmt.Errorf("release refused: %s may still be alive; run again after %s",
			s.Name(), after.Format(time.DateTime))
	}
	return &ReleasePlan{Slot: s, Discards: discards(c.slotJournal(i))}, nil
}

// Release resets the plan's journal and empties its registry slot, so a node
// can register there with a new journal. It stops if any registry slot
// changed since PlanRelease watched the registry.
func (c *Checker) Release(plan *ReleasePlan) error {
	i := plan.Slot.Index
	now, err := c.readRegistry()
	if err != nil {
		return fmt.Errorf("read the node registry: %w", err)
	}
	if changed := changedSlots(c.registry, now); len(changed) > 0 {
		return fmt.Errorf("release stopped: node registry slot %d changed since fsck checked it; run again", changed[0])
	}

	offset, _ := c.journalArea(i)
	if _, err := c.dev.WriteAt(storage.NewAlignedBuffer(storage.SectorSize).Bytes(), int64(offset)); err != nil {
		return fmt.Errorf("reset the journal of slot %d: %w", i, err)
	}
	if err := c.dev.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	released, err := node.NewRegistry(c.dev, c.sbData.NodeRegistryOffset).ReleaseSlot(i, plan.Slot.raw)
	if err != nil {
		return fmt.Errorf("release slot %d: %w", i, err)
	}
	if !released {
		return fmt.Errorf("slot %d changed while fsck released it: its journal is reset but the slot is not; run again", i)
	}
	return c.dev.Sync()
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

// discards says what resetting a journal throws away.
func discards(region *journal.Region, err error) string {
	switch {
	case errors.Is(err, journal.ErrJournalUninitialized):
		return "nothing: the journal was never initialized"
	case err != nil:
		return "the whole journal, unread: " + rootCause(err).Error()
	case region.State() == journal.JournalStateClean:
		return "nothing: the journal is clean"
	case !region.NeedsRecovery():
		return "the whole journal, unread: its state is " + strings.ToLower(region.State().String())
	}
	txns, _, err := journal.DryRunRecover(region)
	what := fmt.Sprintf("%d committed transactions that were never replayed", len(txns))
	if err != nil {
		what += fmt.Sprintf(", and whatever follows a record fsck cannot read (%v)", err)
	}
	return what
}

func rootCause(err error) error {
	if inner := errors.Unwrap(err); inner != nil {
		return inner
	}
	return err
}
