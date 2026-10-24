package lock

import "log"

// ReclaimOwner releases the lock records held by owner and returns how many it
// changed. Call it only once owner is proven dead (its process gone, or its
// lease expired by this node's monotonic clock plus a margin): a slow node
// that still runs would lose locks it is using.
//
// Exclusive records of owner are freed. A shared record names only its latest
// reader, so when that reader is owner its one count is dropped and the record
// is freed if it was the last; other readers' counts stay until they release
// or the record goes stale.
func (m *Manager) ReclaimOwner(owner [16]byte) (int, error) {
	reclaimed := 0
	for recordID := uint64(0); recordID < m.size/RecordSize; recordID++ {
		changed, err := m.reclaimRecord(recordID, owner)
		if err != nil {
			return reclaimed, err
		}
		if changed {
			reclaimed++
		}
	}
	if reclaimed > 0 {
		log.Printf("lock: reclaimed %d lock records of dead node %x", reclaimed, owner[:8])
	}
	return reclaimed, nil
}

func (m *Manager) reclaimRecord(recordID uint64, owner [16]byte) (bool, error) {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	for {
		record, err := m.readRecord(recordID)
		if err != nil {
			return false, err
		}
		if record.Mode == LockModeNone || record.OwnerUUID != owner {
			return false, nil
		}

		next := &Record{Sequence: record.Sequence + 1}
		if record.Mode == LockModeShared && record.SharedCount > 1 {
			kept := *record
			kept.OwnerUUID = [16]byte{}
			kept.SharedCount--
			kept.Sequence++
			next = &kept
		}

		ok, err := m.casRecord(recordID, record, next)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
}
