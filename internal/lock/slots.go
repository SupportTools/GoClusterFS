package lock

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// Every lock record is a row of one claim slot per registry slot, each in its
// own sector, and a node writes only its own slot. A claim writes the node's
// slot, then reads the whole row in one I/O, and wins if no other live slot
// conflicts. Of two conflicting claimants, the second to finish its write reads
// the row after the first's write completed, so it sees that slot and loses.
// This holds whatever the I/O delays.
//
// When claims collide, the claimant in the lower slot keeps its slot and waits;
// any claimant that sees a conflict in a lower slot clears its own and retries.
// Waits only point from lower to higher slots, so they never form a cycle. A
// claimant that cleared its slot reads the row before writing again, and waits
// without writing while the lower slot is still there, so it does not get in
// the way of that slot's claim.
const (
	SlotsPerRecord = 64
	SlotSize       = storage.SectorSize
	RowSize        = SlotsPerRecord * SlotSize
	slotMagic      = 0x534C4F54
)

var (
	errTornSlot = errors.New("lock slot fails its checksum")
	emptySlot   = make([]byte, SlotSize)
)

type claimSlot struct {
	mode        LockMode
	lockType    LockType
	adopted     bool // kept by AdoptOwner for a journal replay; every claimant defers to it
	resourceID  uint64
	owner       [16]byte
	acquireTime int64 // Unix nanoseconds
}

type adoptedSlot struct {
	slot  int
	owner [16]byte
}

type slotRef struct {
	record uint64
	slot   int
}

type tornSighting struct {
	since time.Time
	sum   uint64
}

func encodeSlot(s *claimSlot) []byte {
	buf := make([]byte, SlotSize)
	if s == nil {
		return buf
	}
	binary.LittleEndian.PutUint32(buf[0:4], slotMagic)
	buf[4] = byte(s.mode)
	buf[5] = byte(s.lockType)
	if s.adopted {
		buf[6] = 1
	}
	binary.LittleEndian.PutUint64(buf[8:16], s.resourceID)
	copy(buf[16:32], s.owner[:])
	binary.LittleEndian.PutUint64(buf[32:40], uint64(s.acquireTime))
	binary.LittleEndian.PutUint32(buf[SlotSize-4:], storage.CRC32C(buf[:SlotSize-4]))
	return buf
}

// decodeSlot returns nil for an empty slot and errTornSlot for one whose
// checksum fails, which callers treat as a claim.
func decodeSlot(buf []byte) (*claimSlot, error) {
	if isZeroSlot(buf) {
		return nil, nil
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != slotMagic ||
		binary.LittleEndian.Uint32(buf[SlotSize-4:]) != storage.CRC32C(buf[:SlotSize-4]) {
		return nil, errTornSlot
	}
	s := &claimSlot{
		mode:        LockMode(buf[4]),
		lockType:    LockType(buf[5]),
		adopted:     buf[6] == 1,
		resourceID:  binary.LittleEndian.Uint64(buf[8:16]),
		acquireTime: int64(binary.LittleEndian.Uint64(buf[32:40])),
	}
	copy(s.owner[:], buf[16:32])
	return s, nil
}

func isZeroSlot(buf []byte) bool {
	return bytes.Equal(buf, emptySlot)
}

func (s *claimSlot) stale() bool {
	return time.Since(time.Unix(0, s.acquireTime)) > StaleThreshold
}

func rowSlot(row []byte, slot int) []byte {
	return row[slot*SlotSize : (slot+1)*SlotSize]
}

func (m *Manager) slotOffset(recordID uint64, slot int) int64 {
	return int64(m.offset) + int64(recordID)*RowSize + int64(slot)*SlotSize
}

// writeSlot writes slot of recordID; a nil s clears it. Callers hold the record's gate.
func (m *Manager) writeSlot(recordID uint64, slot int, s *claimSlot) error {
	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), encodeSlot(s))
	m.slotWrites.Add(1)
	write := m.dev.WriteAtUnsharded
	if m.slotWrite != nil {
		write = m.slotWrite
	}
	_, err := write(buf.Bytes(), m.slotOffset(recordID, slot))
	return err
}

func (m *Manager) readRow(recordID uint64) ([]byte, error) {
	buf := storage.GetBuffer(RowSize)
	defer storage.PutBuffer(buf)
	m.rowReads.Add(1)
	if _, err := m.dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(recordID, 0)); err != nil {
		return nil, err
	}
	return append([]byte(nil), buf.Bytes()...), nil
}

// rowConflicts reports whether a slot other than this node's and ignore
// conflicts with a claim in mode, whether one of those is in a lower slot or
// adopted, which this node must defer to, and what stands in the way. A write
// claim conflicts with every live slot, a read claim with live write slots, and
// a torn slot counts as a write claim. With an owner view a slot stays live
// until its registry slot is empty. Without one, it stays live until it is older
// than StaleThreshold, or for a torn slot, until it stays torn for tornExpiry.
//
// ignore is the slot of a dead node whose claim this node adopted for a replay.
// Another replay's adopted claim then counts as lower only in a lower slot and
// not at all in a higher one, so two replays sharing a record take turns.
func (m *Manager) rowConflicts(recordID uint64, row []byte, mode LockMode, ignore int) (conflict, lower bool, by blocker) {
	m.mu.Lock()
	view := m.ownerView
	m.mu.Unlock()
	for slot := range SlotsPerRecord {
		if slot == m.nodeSlot || slot == ignore {
			continue
		}
		s, err := decodeSlot(rowSlot(row, slot))
		var torn []byte
		if err != nil {
			torn = rowSlot(row, slot)
		}
		tornTooLong := m.tornTooLong(slotRef{recordID, slot}, torn)
		if err == nil && (s == nil || (mode != LockModeExclusive && s.mode != LockModeExclusive)) {
			continue
		}
		adopted := s != nil && s.adopted
		if adopted && ignore >= 0 && slot > ignore {
			continue
		}
		b := blocker{slot: slot}
		if view == nil {
			if tornTooLong || (s != nil && s.stale()) {
				continue
			}
		} else if b = m.slotState(view, slot, s); b.state == OwnerReleased {
			continue
		}
		if conflict {
			b = worse(by, b)
		}
		conflict, by = true, b
		if slot < m.nodeSlot || adopted {
			lower = true
		}
	}
	return conflict, lower, by
}

// tornTooLong reports whether ref has read torn, with the same bytes, for
// tornExpiry; torn is nil when the slot reads whole. Changed bytes mean a node
// is writing the slot, so they start the wait again.
func (m *Manager) tornTooLong(ref slotRef, torn []byte) bool {
	if torn == nil && m.tornCount.Load() == 0 {
		return false
	}
	m.tornMu.Lock()
	defer m.tornMu.Unlock()
	defer func() { m.tornCount.Store(int32(len(m.tornSince))) }()
	if torn == nil {
		delete(m.tornSince, ref)
		return false
	}
	// Not CRC32C: a slot holds the CRC of its own contents, so every slot torn
	// the same way has the same CRC32C whatever it says.
	h := fnv.New64a()
	h.Write(torn)
	sum := h.Sum64()
	seen, ok := m.tornSince[ref]
	if !ok || seen.sum != sum {
		m.tornSince[ref] = tornSighting{since: time.Now(), sum: sum}
		return false
	}
	return age(seen.since) > tornExpiry
}

// ownAdoptedLocked reports whether AdoptOwner kept recordID in this node's own
// slot, for the predecessor that last used it.
func (m *Manager) ownAdoptedLocked(recordID uint64) bool {
	a, ok := m.adopted[recordID]
	return ok && a.slot == m.nodeSlot
}

// ownClaim is what this node writes in its slot to claim recordID in mode. A
// record adopted in this slot stays an adopted write claim until ReleaseAdopted.
func (m *Manager) ownClaim(recordID uint64, key lockKey, mode LockMode, at time.Time) *claimSlot {
	s := &claimSlot{mode: mode, lockType: key.Type, resourceID: key.ResourceID, owner: m.nodeUUID, acquireTime: at.UnixNano()}
	m.mu.Lock()
	if m.ownAdoptedLocked(recordID) {
		s.mode, s.adopted = LockModeExclusive, true
	}
	m.mu.Unlock()
	return s
}

func (m *Manager) addClaimWaiterLocked(recordID uint64) {
	c, ok := m.claims[recordID]
	if !ok {
		c = &pendingClaim{}
		m.claims[recordID] = c
	}
	c.waiters++
}

// dropClaimWaiter ends one Acquire call's wait. The last waiter to leave
// clears a slot this node wrote for the record but never won.
func (m *Manager) dropClaimWaiter(recordID uint64) {
	m.mu.Lock()
	c := m.claims[recordID]
	c.waiters--
	if c.waiters > 0 {
		m.mu.Unlock()
		return
	}
	if c.written.IsZero() {
		delete(m.claims, recordID)
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()
	m.mu.Lock()
	c, ok := m.claims[recordID]
	if !ok || c.waiters > 0 {
		m.mu.Unlock()
		return
	}
	delete(m.claims, recordID)
	m.mu.Unlock()
	if !c.written.IsZero() {
		_ = m.releaseRecord(recordID)
	}
}

// claim tries to win recordID in mode through this node's slot, as described
// at the top of this file, and returns when the slot it relies on was written.
// Callers hold the record's gate and have a waiter registered for it.
func (m *Manager) claim(recordID uint64, key lockKey, mode LockMode) (written time.Time, won bool, by blocker, err error) {
	m.mu.Lock()
	c := m.claims[recordID]
	rewrite := c.written.IsZero() || c.mode != mode || age(c.written) > renewAfter
	ignore := -1
	if a, ok := m.adopted[recordID]; ok {
		ignore = a.slot
	}
	behindLower := c.behindLower
	m.mu.Unlock()

	if behindLower {
		row, err := m.readRow(recordID)
		if err != nil {
			return time.Time{}, false, blocker{}, err
		}
		if _, lower, by := m.rowConflicts(recordID, row, mode, ignore); lower {
			m.deferred.Add(1)
			return time.Time{}, false, by, nil
		}
		m.mu.Lock()
		c.behindLower = false
		m.mu.Unlock()
	}

	start := time.Now()
	if rewrite {
		err := m.writeSlot(recordID, m.nodeSlot, m.ownClaim(recordID, key, mode, start))
		m.mu.Lock()
		c.written, c.mode = start, mode
		m.mu.Unlock()
		if err != nil {
			return time.Time{}, false, blocker{}, err
		}
	}
	row, err := m.readRow(recordID)
	if err != nil {
		return time.Time{}, false, blocker{}, err
	}
	if rewrite {
		m.recordClaimTime(time.Since(start))
	}

	conflict, lower, by := m.rowConflicts(recordID, row, mode, ignore)
	switch {
	case !conflict:
		m.mu.Lock()
		written = c.written
		c.written = time.Time{}
		m.mu.Unlock()
		return written, true, blocker{}, nil
	case lower:
		m.conflicts.Add(1)
		if err := m.releaseRecord(recordID); err != nil {
			return time.Time{}, false, by, err
		}
		m.mu.Lock()
		c.written, c.behindLower = time.Time{}, true
		m.mu.Unlock()
		return time.Time{}, false, by, nil
	default:
		m.waits.Add(1)
		return time.Time{}, false, by, nil
	}
}

// renewRecord writes this node's slot of a held record again so other nodes
// do not see it as stale. It runs under the record gate.
func (m *Manager) renewRecord(recordID uint64, key lockKey, rec *heldRecord) error {
	now := time.Now()
	if err := m.writeSlot(recordID, m.nodeSlot, m.ownClaim(recordID, key, rec.mode, now)); err != nil {
		return err
	}
	m.mu.Lock()
	rec.renewed = now
	m.mu.Unlock()
	return nil
}

// releaseRecord clears this node's slot of a record it held or claimed, or
// keeps an adoption there until ReleaseAdopted. It runs under the record gate.
func (m *Manager) releaseRecord(recordID uint64) error {
	m.mu.Lock()
	keep := m.ownAdoptedLocked(recordID)
	m.mu.Unlock()
	if keep {
		return m.writeSlot(recordID, m.nodeSlot, m.ownClaim(recordID, lockKey{}, LockModeExclusive, time.Now()))
	}
	return m.writeSlot(recordID, m.nodeSlot, nil)
}

// describeHolder says which slots claim a record, for logs.
func (m *Manager) describeHolder(recordID uint64) string {
	row, err := m.readRow(recordID)
	if err != nil {
		return fmt.Sprintf("could not be read: %v", err)
	}
	var holders []string
	for slot := range SlotsPerRecord {
		s, err := decodeSlot(rowSlot(row, slot))
		switch {
		case err != nil:
			holders = append(holders, fmt.Sprintf("slot %d (torn)", slot))
		case s != nil:
			adopted := ""
			if s.adopted {
				adopted = " (adopted)"
			}
			holders = append(holders, fmt.Sprintf("slot %d node %x mode %d type %d res %d%s, written %v ago",
				slot, s.owner[:8], s.mode, s.lockType, s.resourceID, adopted, time.Since(time.Unix(0, s.acquireTime)).Round(time.Millisecond)))
		}
	}
	if len(holders) == 0 {
		return "is free"
	}
	return "is claimed by " + strings.Join(holders, ", ")
}
