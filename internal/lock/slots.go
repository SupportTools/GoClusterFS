package lock

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// Prototype claim protocol: every lock record is a row of one slot per registry
// slot, each in its own sector. A node writes only its own slot. A claim writes
// the node's slot, then reads the whole row in one I/O, and holds the lock if no
// other live slot conflicts; otherwise it clears its slot and retries. Of two
// claimants, the second to finish its write sees the first in its read, so two
// conflicting holders are impossible whatever the I/O delays.
const (
	SlotsPerRecord = 64
	SlotSize       = storage.SectorSize
	RowSize        = SlotsPerRecord * SlotSize
	slotMagic      = 0x534C4F54
)

var errTornSlot = errors.New("lock slot fails its checksum")

var claimConflicts atomic.Uint64

type claimSlot struct {
	mode        LockMode
	lockType    LockType
	resourceID  uint64
	owner       [16]byte
	acquireTime int64
}

func encodeSlot(s *claimSlot) []byte {
	buf := make([]byte, SlotSize)
	if s == nil {
		return buf
	}
	binary.LittleEndian.PutUint32(buf[0:4], slotMagic)
	buf[4] = byte(s.mode)
	buf[5] = byte(s.lockType)
	binary.LittleEndian.PutUint64(buf[8:16], s.resourceID)
	copy(buf[16:32], s.owner[:])
	binary.LittleEndian.PutUint64(buf[32:40], uint64(s.acquireTime))
	binary.LittleEndian.PutUint32(buf[SlotSize-4:], storage.CRC32C(buf[:SlotSize-4]))
	return buf
}

// decodeSlot returns nil for an empty slot and errTornSlot for one whose
// checksum fails, which callers treat as a claim in progress.
func decodeSlot(buf []byte) (*claimSlot, error) {
	if binary.LittleEndian.Uint32(buf[0:4]) == 0 && isZeroSlot(buf) {
		return nil, nil
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != slotMagic ||
		binary.LittleEndian.Uint32(buf[SlotSize-4:]) != storage.CRC32C(buf[:SlotSize-4]) {
		return nil, errTornSlot
	}
	s := &claimSlot{
		mode:        LockMode(buf[4]),
		lockType:    LockType(buf[5]),
		resourceID:  binary.LittleEndian.Uint64(buf[8:16]),
		acquireTime: int64(binary.LittleEndian.Uint64(buf[32:40])),
	}
	copy(s.owner[:], buf[16:32])
	return s, nil
}

func isZeroSlot(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

func (m *Manager) slotOffset(recordID uint64, slot int) int64 {
	return int64(m.offset) + int64(recordID)*RowSize + int64(slot)*SlotSize
}

func (m *Manager) writeSlotAt(recordID uint64, slot int, s *claimSlot) error {
	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), encodeSlot(s))
	m.writes.Add(1)
	start := time.Now()
	if m.casWrite != nil {
		_, err := m.casWrite(buf.Bytes(), m.slotOffset(recordID, slot), time.Time{})
		return err
	}
	_, _, err := m.dev.WriteAtBeforeUnshardedTimed(buf.Bytes(), m.slotOffset(recordID, slot), time.Time{})
	m.recordWriteGap(time.Since(start))
	return err
}

func (m *Manager) readRow(recordID uint64) ([]byte, error) {
	buf := storage.GetBuffer(RowSize)
	defer storage.PutBuffer(buf)
	if _, _, err := m.dev.ReadAtUnshardedTimed(buf.Bytes(), m.slotOffset(recordID, 0)); err != nil {
		return nil, err
	}
	return append([]byte(nil), buf.Bytes()...), nil
}

// rowConflicts reports whether any slot other than this node's and ignore
// conflicts with a claim in mode: a write claim conflicts with any live slot,
// a read claim only with live write slots. Slots older than StaleThreshold are
// ignored, as their nodes are presumed dead.
func (m *Manager) rowConflicts(row []byte, mode LockMode, ignore int) bool {
	for slot := range SlotsPerRecord {
		if slot == m.nodeSlot || slot == ignore {
			continue
		}
		s, err := decodeSlot(row[slot*SlotSize : (slot+1)*SlotSize])
		if err != nil {
			return true
		}
		if s == nil || time.Since(time.Unix(s.acquireTime, 0)) > StaleThreshold {
			continue
		}
		if mode == LockModeExclusive || s.mode == LockModeExclusive {
			return true
		}
	}
	return false
}

// slotClaim writes this node's slot and checks the row, as described above.
func (m *Manager) slotClaim(recordID uint64, lockType LockType, resourceID uint64, mode LockMode, ignore int) (bool, error) {
	mine := &claimSlot{mode: mode, lockType: lockType, resourceID: resourceID, owner: m.nodeUUID, acquireTime: time.Now().Unix()}
	if err := m.writeSlotAt(recordID, m.nodeSlot, mine); err != nil {
		return false, err
	}
	row, err := m.readRow(recordID)
	if err != nil {
		return false, err
	}
	if m.rowConflicts(row, mode, ignore) {
		claimConflicts.Add(1)
		return false, m.writeSlotAt(recordID, m.nodeSlot, nil)
	}
	m.countAcquire(lockType, func(c *acquireCounts) *atomic.Uint64 { return &c.confirmed })
	return true, nil
}

func (m *Manager) describeRow(recordID uint64) string {
	row, err := m.readRow(recordID)
	if err != nil {
		return fmt.Sprintf("could not be read: %v", err)
	}
	var holders []string
	for slot := range SlotsPerRecord {
		s, err := decodeSlot(row[slot*SlotSize : (slot+1)*SlotSize])
		switch {
		case err != nil:
			holders = append(holders, fmt.Sprintf("slot %d (torn)", slot))
		case s != nil:
			holders = append(holders, fmt.Sprintf("slot %d mode %d %v ago", slot, s.mode, time.Since(time.Unix(s.acquireTime, 0)).Round(time.Second)))
		}
	}
	if len(holders) == 0 {
		return "is free"
	}
	return "is held by " + strings.Join(holders, ", ")
}

// adoptSlots clears a proven-dead node's slots and, for each of its write
// slots, claims this node's slot in its place. It returns the claimed records.
func (m *Manager) adoptSlots(owner [16]byte, ownerSlot int) ([]uint64, error) {
	var adopted []uint64
	for recordID := uint64(0); recordID < m.size/RowSize; recordID++ {
		gate := m.recordGate(recordID)
		gate.Lock()
		row, err := m.readRow(recordID)
		if err != nil {
			gate.Unlock()
			return adopted, err
		}
		dead, err := decodeSlot(row[ownerSlot*SlotSize : (ownerSlot+1)*SlotSize])
		if err == nil && (dead == nil || dead.owner != owner) {
			gate.Unlock()
			continue
		}
		if dead != nil && dead.mode == LockModeExclusive {
			if ok, err := m.slotClaim(recordID, dead.lockType, dead.resourceID, LockModeExclusive, ownerSlot); err == nil && ok {
				adopted = append(adopted, recordID)
			}
		}
		err = m.writeSlotAt(recordID, ownerSlot, nil)
		gate.Unlock()
		if err != nil {
			return adopted, err
		}
	}
	log.Printf("lock: cleared dead node %x's slots; took over %d of its write locks", owner[:8], len(adopted))
	return adopted, nil
}
