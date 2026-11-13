package lock

import (
	"log"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// Start runs a renewer that rewrites this node's held slots and the slots it
// adopted for a replay, so peers never see them as stale however long an
// operation or a replay runs. Stop ends it.
func (m *Manager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopRenew != nil {
		return
	}
	m.stopRenew, m.renewDone = make(chan struct{}), make(chan struct{})
	go m.renewLoop(m.stopRenew, m.renewDone)
}

// Stop ends the renewer and waits for it to finish.
func (m *Manager) Stop() {
	m.mu.Lock()
	stop, done := m.stopRenew, m.renewDone
	m.stopRenew, m.renewDone = nil, nil
	m.mu.Unlock()
	if stop != nil {
		close(stop)
		<-done
	}
}

func (m *Manager) renewLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	tick := time.NewTicker(renewAfter / 4)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			m.renewDue()
		}
	}
}

// renewDue rewrites the slots this node wrote more than renewAfter/2 ago, so
// they stay younger than renewAfter, which also spares joins an inline renewal.
func (m *Manager) renewDue() {
	m.mu.Lock()
	var held, adopted []uint64
	for recordID, rec := range m.records {
		if age(rec.renewed) > renewAfter/2 {
			held = append(held, recordID)
		}
	}
	for recordID, a := range m.adopted {
		if age(a.renewed) > renewAfter/2 {
			adopted = append(adopted, recordID)
		}
	}
	m.mu.Unlock()

	for _, recordID := range held {
		if err := m.renewHeld(recordID); err != nil {
			log.Printf("[WARN] lock: renewing record %d: %v", recordID, err)
		}
	}
	for _, recordID := range adopted {
		if err := m.renewAdopted(recordID); err != nil {
			log.Printf("[WARN] lock: renewing adopted record %d: %v", recordID, err)
		}
	}
}

func (m *Manager) renewHeld(recordID uint64) error {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	m.mu.Lock()
	rec, ok := m.records[recordID]
	due := ok && age(rec.renewed) > renewAfter/2
	m.mu.Unlock()
	if !due {
		return nil
	}
	return m.renewRecord(recordID, rec.key, rec)
}

// renewAdopted rewrites a slot AdoptOwner kept, unless it no longer holds that
// adoption. An adoption in this node's own slot that a hold or claim uses is
// renewed with that hold or claim.
func (m *Manager) renewAdopted(recordID uint64) error {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	m.mu.Lock()
	a, ok := m.adopted[recordID]
	m.mu.Unlock()
	if !ok || age(a.renewed) <= renewAfter/2 {
		return nil
	}
	now := time.Now()
	if a.slot == m.nodeSlot {
		if _, inUse := m.ownSlotUse(recordID); inUse {
			return nil
		}
		if err := m.writeSlot(recordID, a.slot, m.ownClaim(recordID, lockKey{}, LockModeExclusive, now)); err != nil {
			return err
		}
	} else {
		buf := storage.GetBuffer(SlotSize)
		defer storage.PutBuffer(buf)
		if _, err := m.dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(recordID, a.slot)); err != nil {
			return err
		}
		s, err := decodeSlot(buf.Bytes())
		if err != nil || s == nil || !s.adopted || s.owner != a.owner {
			return nil
		}
		s.acquireTime = now.UnixNano()
		if err := m.writeSlot(recordID, a.slot, s); err != nil {
			return err
		}
	}
	m.mu.Lock()
	if cur, ok := m.adopted[recordID]; ok {
		cur.renewed = now
		m.adopted[recordID] = cur
	}
	m.mu.Unlock()
	return nil
}
