package lock

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"maps"
	"math/rand"
	"slices"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// A flock record holds, in each node's slot, an entry for every inode the node
// holds a flock on or waits for, so files that share a record never block each
// other however long their flocks last. A claim follows the other records'
// rules for each inode: the node writes its slot, then reads the row, and wins
// if no live slot claims the inode in a conflicting mode. Of two conflicting
// claimants the lower slot keeps its claim and the higher one turns its entry
// into a waiting one, which conflicts with nothing.
//
// A waiter rereads only the slots in its way, every flockPoll, or at once when
// a peer's hint says the record changed. Entries need no renewal: under an
// owner view they stay live until their registry slot empties, and AdoptOwner
// clears a dead node's entries before its journal replay.
const (
	flockMagic     = 0x464C434B
	flockHeader    = 24
	flockEntrySize = 16
	// FlockCapacity is how many inodes one node can hold or wait for in one flock record.
	FlockCapacity = (SlotSize - flockHeader - 4) / flockEntrySize

	flockStateClaim = 1
	flockStateWait  = 2

	// FlockPoll is how often a waiter rereads the slots in its way when no hint arrives.
	FlockPoll = time.Second
)

var (
	// ErrWouldBlock means another node holds a conflicting flock.
	ErrWouldBlock = errors.New("another node holds a conflicting flock")
	// ErrFlockFull means this node holds or waits for FlockCapacity flocks in the key's record.
	ErrFlockFull = errors.New("this node holds too many flocks in one lock record")
	// ErrNoFlockRecords means the lock region is too small to have records for flocks.
	ErrNoFlockRecords = errors.New("the lock region has no flock records")
)

// FlockKey names a flock: an inode, and the birth generation that tells it from
// a later inode in the same slot.
type FlockKey struct {
	Inode uint32
	Birth uint64
}

type flockEntry struct {
	mode    LockMode
	waiting bool
}

func encodeFlockSlot(owner [16]byte, entries map[FlockKey]flockEntry) []byte {
	buf := make([]byte, SlotSize)
	if len(entries) == 0 {
		return buf
	}
	binary.LittleEndian.PutUint32(buf[0:4], flockMagic)
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(entries)))
	copy(buf[8:24], owner[:])
	keys := slices.SortedFunc(maps.Keys(entries), func(a, b FlockKey) int {
		return cmp.Or(cmp.Compare(a.Inode, b.Inode), cmp.Compare(a.Birth, b.Birth))
	})
	for i, key := range keys {
		e := buf[flockHeader+i*flockEntrySize:]
		binary.LittleEndian.PutUint32(e[0:4], key.Inode)
		e[4] = byte(entries[key].mode)
		e[5] = flockStateClaim
		if entries[key].waiting {
			e[5] = flockStateWait
		}
		binary.LittleEndian.PutUint64(e[8:16], key.Birth)
	}
	binary.LittleEndian.PutUint32(buf[SlotSize-4:], storage.CRC32C(buf[:SlotSize-4]))
	return buf
}

func decodeFlockSlot(buf []byte) (owner [16]byte, entries map[FlockKey]flockEntry, err error) {
	if isZeroSlot(buf) {
		return owner, nil, nil
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != flockMagic ||
		binary.LittleEndian.Uint32(buf[SlotSize-4:]) != storage.CRC32C(buf[:SlotSize-4]) {
		return owner, nil, errTornSlot
	}
	n := int(binary.LittleEndian.Uint16(buf[4:6]))
	if n > FlockCapacity {
		return owner, nil, errTornSlot
	}
	copy(owner[:], buf[8:24])
	entries = make(map[FlockKey]flockEntry, n)
	for i := range n {
		e := buf[flockHeader+i*flockEntrySize:]
		mode, state := LockMode(e[4]), e[5]
		if mode != LockModeShared && mode != LockModeExclusive || state != flockStateClaim && state != flockStateWait {
			return owner, nil, errTornSlot
		}
		key := FlockKey{Inode: binary.LittleEndian.Uint32(e[0:4]), Birth: binary.LittleEndian.Uint64(e[8:16])}
		entries[key] = flockEntry{mode: mode, waiting: state == flockStateWait}
	}
	return owner, entries, nil
}

// isFlockRecord reports whether recordID is one of the flock records.
func (m *Manager) isFlockRecord(recordID uint64) bool {
	if m.size/RowSize < layoutRows {
		return false
	}
	first := typeFirstRow[LockTypeFlock]
	return recordID >= first && recordID < first+typeRows[LockTypeFlock]
}

func (m *Manager) flockRecord(key FlockKey) (uint64, error) {
	if m.size/RowSize < layoutRows {
		return 0, ErrNoFlockRecords
	}
	return m.hashToRecord(LockTypeFlock, uint64(key.Inode)), nil
}

// Flock takes this node's flock on key in mode, or changes its mode. While
// another node holds a conflicting flock it waits, or returns ErrWouldBlock
// if wait is false. A wait that ctx ends returns ctx's error. When Flock fails,
// this node holds no flock on key. Calls for one key must not overlap.
func (m *Manager) Flock(ctx context.Context, key FlockKey, mode LockMode, wait bool) error {
	if mode != LockModeShared && mode != LockModeExclusive {
		return fmt.Errorf("lock: flock mode %d", mode)
	}
	recordID, err := m.flockRecord(key)
	if err != nil {
		return err
	}
	err = m.flock(ctx, recordID, key, mode, wait)
	if err != nil {
		m.dropFlock(recordID, key)
	}
	return err
}

func (m *Manager) flock(ctx context.Context, recordID uint64, key FlockKey, mode LockMode, wait bool) error {
	if held, ok := m.ownFlock(recordID, key); ok && !held.waiting {
		switch {
		case held.mode == mode:
			return nil
		case mode == LockModeShared:
			return m.setFlock(recordID, key, &flockEntry{mode: LockModeShared})
		}
	}
	for {
		won, blockers, err := m.claimFlock(recordID, key, mode, wait)
		if err != nil || won {
			return err
		}
		if err := m.awaitFlock(ctx, recordID, key, mode, blockers); err != nil {
			return err
		}
	}
}

// dropFlock removes key's entry after a failed Flock. If that write fails too,
// the watcher writes the slot without it.
func (m *Manager) dropFlock(recordID uint64, key FlockKey) {
	if _, ok := m.ownFlock(recordID, key); !ok {
		return
	}
	if err := m.setFlock(recordID, key, nil); err != nil {
		log.Printf("[WARN] lock: removing this node's flock entry for inode %d failed: %v", key.Inode, err)
	}
}

// Unflock releases this node's flock on key.
func (m *Manager) Unflock(key FlockKey) error {
	recordID, err := m.flockRecord(key)
	if err != nil {
		return err
	}
	if _, ok := m.ownFlock(recordID, key); !ok {
		return nil
	}
	return m.setFlock(recordID, key, nil)
}

// ReleaseFlocks clears every flock entry of this node, as at unmount.
func (m *Manager) ReleaseFlocks() error {
	m.mu.Lock()
	ids := slices.Collect(maps.Keys(m.flocks))
	m.mu.Unlock()
	var errs []error
	for _, recordID := range ids {
		gate := m.recordGate(recordID)
		gate.Lock()
		errs = append(errs, m.writeFlocks(recordID, nil))
		gate.Unlock()
		m.sendFlockHint(recordID)
	}
	return errors.Join(errs...)
}

// SetFlockHint sets the func that tells peers a flock record changed, so their
// waiters reread it before their next poll.
func (m *Manager) SetFlockHint(hint func(recordID uint64)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flockHint = hint
}

// FlockChanged wakes this node's waiters on a flock record, as a peer's hint does.
func (m *Manager) FlockChanged(recordID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if wake, ok := m.flockWake[recordID]; ok {
		close(wake)
		delete(m.flockWake, recordID)
	}
}

func (m *Manager) flockWaker(recordID uint64) <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	wake, ok := m.flockWake[recordID]
	if !ok {
		wake = make(chan struct{})
		m.flockWake[recordID] = wake
	}
	return wake
}

func (m *Manager) sendFlockHint(recordID uint64) {
	m.FlockChanged(recordID)
	m.mu.Lock()
	hint := m.flockHint
	m.mu.Unlock()
	if hint != nil {
		hint(recordID)
	}
}

func (m *Manager) ownFlock(recordID uint64, key FlockKey) (flockEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.flocks[recordID][key]
	return e, ok
}

func (m *Manager) ownFlocks(recordID uint64) map[FlockKey]flockEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.flocks[recordID])
}

// setFlock writes this node's slot with key's entry set to e, or removed if e
// is nil, and hints peers when that frees anything.
func (m *Manager) setFlock(recordID uint64, key FlockKey, e *flockEntry) error {
	gate := m.recordGate(recordID)
	gate.Lock()
	entries := m.ownFlocks(recordID)
	if entries == nil {
		entries = map[FlockKey]flockEntry{}
	}
	if e == nil {
		delete(entries, key)
	} else {
		entries[key] = *e
	}
	err := m.writeFlocks(recordID, entries)
	gate.Unlock()
	if err == nil {
		m.sendFlockHint(recordID)
	}
	return err
}

// writeFlocks writes entries as this node's slot of recordID and keeps them as
// what the slot holds. If the write fails the slot is unknown, so the watcher
// writes it again until it holds what this node keeps. Callers hold the gate.
func (m *Manager) writeFlocks(recordID uint64, entries map[FlockKey]flockEntry) error {
	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), encodeFlockSlot(m.nodeUUID, entries))
	m.slotWrites.Add(1)
	write := m.dev.WriteAtUnsharded
	if m.slotWrite != nil {
		write = m.slotWrite
	}
	_, err := write(buf.Bytes(), m.slotOffset(recordID, m.nodeSlot))
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(entries) == 0 {
		delete(m.flocks, recordID)
	} else {
		m.flocks[recordID] = maps.Clone(entries)
	}
	if err != nil {
		if !m.flocksUnwritten[recordID] {
			log.Printf("[WARN] lock: writing this node's flocks in record %d failed: %v; retrying every %v", recordID, err, m.timing.releaseRetry)
		}
		m.flocksUnwritten[recordID] = true
		return err
	}
	delete(m.flocksUnwritten, recordID)
	return nil
}

// retryFlockWrites writes again each flock slot whose last write failed.
func (m *Manager) retryFlockWrites() {
	m.mu.Lock()
	ids := slices.Collect(maps.Keys(m.flocksUnwritten))
	m.mu.Unlock()
	for _, recordID := range ids {
		gate := m.recordGate(recordID)
		gate.Lock()
		if m.writeFlocks(recordID, m.ownFlocks(recordID)) == nil {
			log.Printf("lock: wrote this node's flocks in record %d", recordID)
		}
		gate.Unlock()
		m.sendFlockHint(recordID)
	}
}

// claimFlock makes one claim on key and reports whether it won, or else the
// slots in its way. A claim that loses to a lower slot leaves a waiting entry;
// one that loses only to higher slots keeps its claim, so those back off.
func (m *Manager) claimFlock(recordID uint64, key FlockKey, mode LockMode, wait bool) (won bool, blockers []int, err error) {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	entries := m.ownFlocks(recordID)
	if entries == nil {
		entries = map[FlockKey]flockEntry{}
	}
	if _, ok := entries[key]; !ok && len(entries) >= FlockCapacity {
		return false, nil, ErrFlockFull
	}
	entries[key] = flockEntry{mode: mode}
	if err := m.writeFlocks(recordID, entries); err != nil {
		return false, nil, err
	}
	row, err := m.readRow(recordID)
	if err != nil {
		return false, nil, err
	}
	blockers, lower := m.flockConflicts(row, key, mode)
	if len(blockers) == 0 {
		return true, nil, nil
	}
	m.conflicts.Add(1)
	switch {
	case !wait:
		delete(entries, key)
		if err := m.writeFlocks(recordID, entries); err != nil {
			return false, nil, err
		}
		return false, nil, ErrWouldBlock
	case lower:
		entries[key] = flockEntry{mode: mode, waiting: true}
		if err := m.writeFlocks(recordID, entries); err != nil {
			return false, nil, err
		}
	}
	return false, blockers, nil
}

// flockConflicts returns the live slots other than this node's that claim key
// in a mode conflicting with mode, and whether one of them is lower than this
// node's or torn, which this node must defer to.
func (m *Manager) flockConflicts(row []byte, key FlockKey, mode LockMode) (blockers []int, lower bool) {
	for slot := range SlotsPerRecord {
		if slot == m.nodeSlot {
			continue
		}
		torn, blocks := m.flockSlotBlocks(slot, rowSlot(row, slot), key, mode)
		if !blocks {
			continue
		}
		blockers = append(blockers, slot)
		if slot < m.nodeSlot || torn {
			lower = true
		}
	}
	return blockers, lower
}

// flockSlotBlocks reports whether slot, read as buf, keeps this node from key in
// mode: a live claim on key in a conflicting mode, or a torn slot of a live node.
func (m *Manager) flockSlotBlocks(slot int, buf []byte, key FlockKey, mode LockMode) (torn, blocks bool) {
	_, entries, err := decodeFlockSlot(buf)
	if err == nil {
		e, ok := entries[key]
		if !ok || e.waiting || mode == LockModeShared && e.mode == LockModeShared {
			return false, false
		}
	}
	m.mu.Lock()
	view := m.ownerView
	m.mu.Unlock()
	if view != nil && m.slotState(view, slot, nil).state == OwnerReleased {
		return false, false
	}
	return err != nil, true
}

// awaitFlock waits until none of blockers keeps this node from key in mode, or
// ctx ends. It rereads only those slots, every flockPoll or at a hint.
func (m *Manager) awaitFlock(ctx context.Context, recordID uint64, key FlockKey, mode LockMode, blockers []int) error {
	m.waits.Add(1)
	m.waiting.Add(1)
	defer m.waiting.Add(-1)
	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	wake := m.flockWaker(recordID)
	for {
		poll := m.timing.flockPoll
		timer := time.NewTimer(poll*4/5 + time.Duration(rand.Int63n(int64(poll*2/5)+1)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-wake:
		case <-timer.C:
		}
		timer.Stop()
		wake = m.flockWaker(recordID)
		still := false
		for _, slot := range blockers {
			if _, err := m.dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(recordID, slot)); err != nil {
				return err
			}
			if _, blocks := m.flockSlotBlocks(slot, buf.Bytes(), key, mode); blocks {
				still = true
				break
			}
		}
		if !still {
			return nil
		}
	}
}

// clearFlockSlot clears slot's entries in a flock record, unless the slot is
// this node's and holds entries it keeps. Callers hold the gate.
func (m *Manager) clearFlockSlot(recordID uint64, slot int) error {
	m.mu.Lock()
	kept := slot == m.nodeSlot && len(m.flocks[recordID]) > 0
	m.mu.Unlock()
	if kept {
		return nil
	}
	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	if _, err := m.dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(recordID, slot)); err != nil {
		return err
	}
	if isZeroSlot(buf.Bytes()) {
		return nil
	}
	return m.writeSlot(recordID, slot, nil)
}
