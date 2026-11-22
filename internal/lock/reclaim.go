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
func (m *Manager) AdoptOwner(owner [16]byte, ownerSlot int) ([]uint64, error) {
	var adopted []uint64
	for recordID := uint64(0); recordID < m.size/RowSize; recordID++ {
		kept, err := m.adoptRecord(recordID, owner, ownerSlot)
		if err != nil {
			return adopted, err
		}
		if kept {
			adopted = append(adopted, recordID)
		}
	}
	if len(adopted) > 0 {
		log.Printf("lock: took over %d lock records of dead node %x", len(adopted), owner[:8])
	}
	return adopted, nil
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
	m.adopted[recordID] = adoptedSlot{slot: ownerSlot, owner: dead.owner}
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

// ReleaseAdopted clears the dead node's slots that AdoptOwner kept. A slot
// that is this node's own and in use again is left to its own release, and a
// slot a new node has written since is left alone.
func (m *Manager) ReleaseAdopted(records []uint64) error {
	for _, recordID := range records {
		if err := m.releaseAdopted(recordID); err != nil {
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
	_, adopted := m.adopted[recordID]
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
func (m *Manager) ForgetAdopted(records []uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, recordID := range records {
		delete(m.adopted, recordID)
	}
}

func (m *Manager) releaseAdopted(recordID uint64) error {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	m.mu.Lock()
	a, ok := m.adopted[recordID]
	delete(m.adopted, recordID)
	m.mu.Unlock()
	if !ok {
		return nil
	}
	if a.slot == m.nodeSlot {
		if mode, inUse := m.ownSlotUse(recordID); inUse {
			return m.writeSlot(recordID, a.slot, m.ownClaim(recordID, lockKey{}, mode, time.Now()))
		}
		return m.writeSlot(recordID, a.slot, nil)
	}
	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	if _, err := m.dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(recordID, a.slot)); err != nil {
		return err
	}
	if s, err := decodeSlot(buf.Bytes()); err != nil || s == nil || !s.adopted || s.owner != a.owner {
		return nil
	}
	return m.writeSlot(recordID, a.slot, nil)
}
