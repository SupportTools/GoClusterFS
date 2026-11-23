package lock

import (
	"log"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// AdoptOwner keeps the write claims in ownerSlot, the registry slot of owner, a
// node proven dead, in place while the slot's journal is replayed, and returns
// their records. It writes each write slot there, and any torn one, again with
// the current time, so peers stay off those records until ReleaseAdopted while
// this node's own acquires ignore them. Read slots there are cleared at once.
// That covers claims an earlier node of the slot left, such as a node killed
// before a mount that registered in its slot stopped before its replay. Call it
// before the replay and pass the result to ReleaseAdopted after it.
func (m *Manager) AdoptOwner(owner [16]byte, ownerSlot int) ([]Adoption, error) {
	var adopted []Adoption
	for recordID := uint64(0); recordID < m.size/RowSize; recordID++ {
		kept, err := m.adoptRecord(recordID, owner, ownerSlot)
		if err != nil {
			return adopted, err
		}
		if kept {
			adopted = append(adopted, Adoption{Record: recordID, Slot: ownerSlot})
		}
	}
	if len(adopted) > 0 {
		log.Printf("lock: took over %d lock records of dead node %x", len(adopted), owner[:8])
	}
	return adopted, nil
}

// Adoption is a record AdoptOwner kept in a dead node's slot.
type Adoption struct {
	Record uint64
	Slot   int
}

func (m *Manager) adoptRecord(recordID uint64, owner [16]byte, ownerSlot int) (bool, error) {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	if _, inUse := m.ownSlotUse(recordID); ownerSlot == m.nodeSlot && inUse {
		return false, nil
	}
	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	if _, err := m.dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(recordID, ownerSlot)); err != nil {
		return false, err
	}
	dead, err := decodeSlot(buf.Bytes())
	switch {
	case err == nil && (dead == nil || dead.owner == m.nodeUUID):
		return false, nil
	case err == nil && dead.mode != LockModeExclusive:
		return false, m.writeSlot(recordID, ownerSlot, nil)
	case err != nil:
		dead = &claimSlot{owner: owner}
	}
	dead.mode, dead.adopted = LockModeExclusive, true
	dead.acquireTime = time.Now().UnixNano()
	if err := m.writeSlot(recordID, ownerSlot, dead); err != nil {
		return false, err
	}
	m.mu.Lock()
	if m.adopted[recordID] == nil {
		m.adopted[recordID] = map[int][16]byte{}
	}
	m.adopted[recordID][ownerSlot] = dead.owner
	m.mu.Unlock()
	return true, nil
}

// ownSlotUse reports whether this node holds recordID or has a claim written
// for it, and in which mode.
func (m *Manager) ownSlotUse(recordID uint64) (LockMode, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rec, ok := m.records[recordID]; ok {
		return rec.mode, true
	}
	if c, ok := m.claims[recordID]; ok && !c.written.IsZero() {
		return c.mode, true
	}
	return 0, false
}

// KeepFor writes an exclusive claim on the record of lockType and resourceID into the slot ownerSlot of owner, a node
// proven dead, as AdoptOwner keeps one the node held. Peers then stay off the lock until the returned func clears the
// claim, or, if this node dies first, until the slot's next replay, which adopts the claim and releases it at its
// end. A claim the node held already stays as it is. The caller holds the lock.
func (m *Manager) KeepFor(lockType LockType, resourceID uint64, owner [16]byte, ownerSlot int) (func() error, error) {
	recordID := m.hashToRecord(lockType, resourceID)
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	if _, err := m.dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(recordID, ownerSlot)); err != nil {
		return nil, err
	}
	if s, err := decodeSlot(buf.Bytes()); err == nil && s != nil && s.owner == owner && s.mode == LockModeExclusive {
		return func() error { return nil }, nil
	}
	kept := &claimSlot{mode: LockModeExclusive, lockType: lockType, resourceID: resourceID, adopted: true, owner: owner,
		acquireTime: time.Now().UnixNano()}
	if err := m.writeSlot(recordID, ownerSlot, kept); err != nil {
		return nil, err
	}
	return func() error {
		gate.Lock()
		defer gate.Unlock()
		return m.writeSlot(recordID, ownerSlot, nil)
	}, nil
}

// Adopted reports whether this node keeps a dead node's claim on the record of lockType and resourceID for a replay.
// This node's own acquires pass over such a claim.
func (m *Manager) Adopted(lockType LockType, resourceID uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.adopted[m.hashToRecord(lockType, resourceID)]) > 0
}

// ReleaseAdopted clears the dead node's slots that AdoptOwner kept. A slot
// that is this node's own and in use again is left to its own release, and a
// slot a new node has written since is left alone.
func (m *Manager) ReleaseAdopted(adopted []Adoption) error {
	for _, a := range adopted {
		if err := m.releaseAdopted(a); err != nil {
			return err
		}
	}
	return nil
}

// ClearForeignClaims clears the claims in this node's slot that it did not
// write: a torn claim, or one an earlier occupant of the slot left that no
// replay adopted, such as after fsck released the slot. Under an owner view they
// would count as this node's. Call it after the mount's replay.
func (m *Manager) ClearForeignClaims() error {
	for recordID := uint64(0); recordID < m.size/RowSize; recordID++ {
		if err := m.clearForeignClaim(recordID); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) clearForeignClaim(recordID uint64) error {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	if _, inUse := m.ownSlotUse(recordID); inUse {
		return nil
	}
	m.mu.Lock()
	adopted := len(m.adopted[recordID]) > 0
	m.mu.Unlock()
	if adopted {
		return nil
	}
	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	if _, err := m.dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(recordID, m.nodeSlot)); err != nil {
		return err
	}
	if s, err := decodeSlot(buf.Bytes()); err == nil && (s == nil || s.owner == m.nodeUUID) {
		return nil
	}
	return m.writeSlot(recordID, m.nodeSlot, nil)
}

// ClearSlotClaims clears every claim and waiting mark in registry slot slot's
// column of the lock records at offset, so a node that registers in the slot
// later does not inherit them. Only the slot's node, or a peer holding the
// slot's recovery lease, writes that column, so call it only under that lease,
// as a release does. It returns how many it cleared.
func ClearSlotClaims(dev *storage.Device, offset, size uint64, slot int) (int, error) {
	m := &Manager{dev: dev, offset: offset, size: size}
	buf := storage.NewAlignedBuffer(SlotSize)
	cleared := 0
	for recordID := uint64(0); recordID < size/RowSize; recordID++ {
		if _, err := dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(recordID, slot)); err != nil {
			return cleared, err
		}
		if isZeroSlot(buf.Bytes()) {
			continue
		}
		if err := m.writeSlot(recordID, slot, nil); err != nil {
			return cleared, err
		}
		cleared++
	}
	return cleared, nil
}

// ForgetAdopted drops this node's own record of the adoptions in records and
// leaves their claims on the device, after a replay that failed. The dead
// node's locks then stay held until another replay adopts and releases them,
// and this node no longer passes over them.
func (m *Manager) ForgetAdopted(adopted []Adoption) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range adopted {
		m.forgetLocked(a)
	}
}

func (m *Manager) forgetLocked(a Adoption) (owner [16]byte, ok bool) {
	owner, ok = m.adopted[a.Record][a.Slot]
	delete(m.adopted[a.Record], a.Slot)
	if len(m.adopted[a.Record]) == 0 {
		delete(m.adopted, a.Record)
	}
	return owner, ok
}

func (m *Manager) releaseAdopted(a Adoption) error {
	recordID := a.Record
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	m.mu.Lock()
	owner, ok := m.forgetLocked(a)
	m.mu.Unlock()
	if !ok {
		return nil
	}
	if a.Slot == m.nodeSlot {
		if mode, inUse := m.ownSlotUse(recordID); inUse {
			return m.writeSlot(recordID, a.Slot, m.ownClaim(recordID, lockKey{}, mode, time.Now()))
		}
		return m.writeSlot(recordID, a.Slot, nil)
	}
	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	if _, err := m.dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(recordID, a.Slot)); err != nil {
		return err
	}
	if s, err := decodeSlot(buf.Bytes()); err != nil || s == nil || !s.adopted || s.owner != owner {
		return nil
	}
	return m.writeSlot(recordID, a.Slot, nil)
}
