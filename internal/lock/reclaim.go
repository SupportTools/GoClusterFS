package lock

import (
	"log"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// AdoptOwner keeps the write claims of owner, a node proven dead whose
// registry slot is ownerSlot, in place while owner's journal is replayed, and
// returns their records. It writes each of owner's write slots, and any torn
// slot in ownerSlot, again with the current time, so peers stay off those
// records until ReleaseAdopted while this node's own acquires ignore them.
// owner's read slots are cleared at once. Call it before the replay and pass
// the result to ReleaseAdopted after it.
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

	if ownerSlot == m.nodeSlot && m.usesOwnSlot(recordID) {
		return false, nil
	}
	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	if _, err := m.dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(recordID, ownerSlot)); err != nil {
		return false, err
	}
	dead, err := decodeSlot(buf.Bytes())
	switch {
	case err == nil && (dead == nil || dead.owner != owner):
		return false, nil
	case err == nil && dead.mode != LockModeExclusive:
		return false, m.writeSlot(recordID, ownerSlot, nil)
	case err != nil:
		dead = &claimSlot{owner: owner}
	}
	dead.mode = LockModeExclusive
	dead.acquireTime = time.Now().UnixNano()
	if err := m.writeSlot(recordID, ownerSlot, dead); err != nil {
		return false, err
	}
	m.mu.Lock()
	m.adopted[recordID] = ownerSlot
	m.mu.Unlock()
	return true, nil
}

// usesOwnSlot reports whether this node holds recordID or has a claim written for it.
func (m *Manager) usesOwnSlot(recordID uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, held := m.records[recordID]
	c, claiming := m.claims[recordID]
	return held || (claiming && !c.written.IsZero())
}

// ReleaseAdopted clears the dead node's slots that AdoptOwner kept. A slot
// that is this node's own and in use again is left to its own release.
func (m *Manager) ReleaseAdopted(records []uint64) error {
	for _, recordID := range records {
		if err := m.releaseAdopted(recordID); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) releaseAdopted(recordID uint64) error {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	m.mu.Lock()
	slot, ok := m.adopted[recordID]
	delete(m.adopted, recordID)
	m.mu.Unlock()
	if !ok || (slot == m.nodeSlot && m.usesOwnSlot(recordID)) {
		return nil
	}
	return m.writeSlot(recordID, slot, nil)
}
