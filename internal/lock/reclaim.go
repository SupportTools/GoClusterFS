package lock

import (
	"errors"
	"log"
	"time"
)

// adoptSettle is how long AdoptOwner waits before re-reading the records it
// took, so a peer's racing stale takeover of one of them is noticed.
const adoptSettle = 50 * time.Millisecond

// AdoptOwner makes this node the holder of every exclusive lock record owner
// holds and returns their IDs. Call it only once owner is proven dead, before
// replaying owner's journal, and pass the result to ReleaseAdopted after the
// replay: until then peers cannot take the locks of the dead node's unfinished
// operations, while this node's own acquires reuse them.
//
// Shared records are left to the staleness rule: OwnerUUID names only the
// latest reader, who may have released already while others still read.
func (m *Manager) AdoptOwner(owner [16]byte) ([]uint64, error) {
	var taken []uint64
	for recordID := uint64(0); recordID < m.size/RecordSize; recordID++ {
		ok, err := m.adoptRecord(recordID, owner)
		if err != nil {
			return taken, err
		}
		if ok {
			taken = append(taken, recordID)
		}
	}
	if len(taken) == 0 {
		return nil, nil
	}

	time.Sleep(adoptSettle)
	var adopted []uint64
	for _, recordID := range taken {
		if m.holdsExclusive(recordID) {
			adopted = append(adopted, recordID)
		}
	}
	log.Printf("lock: took over %d lock records of dead node %x", len(adopted), owner[:8])
	return adopted, nil
}

func (m *Manager) adoptRecord(recordID uint64, owner [16]byte) (bool, error) {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	for attempt := 0; ; attempt++ {
		record, err := m.readRecord(recordID)
		if err != nil {
			return false, err
		}
		if record.Mode != LockModeExclusive || record.OwnerUUID != owner {
			return false, nil
		}
		adopted := *record
		adopted.OwnerUUID = m.nodeUUID
		adopted.OwnerSlot = uint32(m.nodeSlot)
		adopted.AcquireTime = time.Now().Unix()
		adopted.Sequence++
		ok, err := m.casRecord(recordID, record, &adopted)
		if errors.Is(err, errLateWrite) {
			m.giveUpLateClaim(recordID, &adopted)
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		if attempt == maxRecordUpdates-1 {
			log.Printf("[WARN] lock: could not take over record %d of dead node %x in %d tries; it frees itself once stale", recordID, owner[:8], maxRecordUpdates)
			return false, nil
		}
		updateBackoff(attempt)
	}
}

// ReleaseAdopted frees the records AdoptOwner returned that this node still
// holds without using them itself; a record in use is freed by its own release.
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
	_, inUse := m.records[recordID]
	m.mu.Unlock()
	if inUse {
		return nil
	}

	for attempt := 0; ; attempt++ {
		record, err := m.readRecord(recordID)
		if err != nil {
			return err
		}
		if record.Mode != LockModeExclusive || record.OwnerUUID != m.nodeUUID {
			return nil
		}
		ok, err := m.casRecord(recordID, record, &Record{Sequence: record.Sequence + 1})
		if errors.Is(err, errLateWrite) {
			return nil
		}
		if err != nil || ok {
			return err
		}
		if attempt == maxRecordUpdates-1 {
			log.Printf("[WARN] lock: could not release taken-over record %d in %d tries; it frees itself once stale", recordID, maxRecordUpdates)
			return nil
		}
		updateBackoff(attempt)
	}
}

func (m *Manager) holdsExclusive(recordID uint64) bool {
	record, err := m.readRecord(recordID)
	return err == nil && record.Mode == LockModeExclusive && record.OwnerUUID == m.nodeUUID
}
