package lock

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// slotKind is what fuzzRow put in a slot.
type slotKind int

const (
	kindEmpty slotKind = iota
	kindClaim
	kindTorn
	kindOdd // passes its checksum but holds arbitrary fields
)

var fuzzDeadOwner = [16]byte{0xde, 0xad}

// fuzzRow builds a claim row from desc, one slot per few bytes, and says what
// each slot holds.
func fuzzRow(desc []byte) ([]byte, [SlotsPerRecord]slotKind) {
	row := make([]byte, RowSize)
	var kinds [SlotsPerRecord]slotKind
	next := func() byte {
		if len(desc) == 0 {
			return 0
		}
		b := desc[0]
		desc = desc[1:]
		return b
	}
	now := time.Now()
	for slot := 0; slot < SlotsPerRecord && len(desc) > 0; slot++ {
		buf := rowSlot(row, slot)
		kind, params := next(), next()
		owner := [16]byte{params}
		if params&0x80 != 0 {
			owner = fuzzDeadOwner
		}
		claim := &claimSlot{
			mode:        LockMode(params%3 + 1),
			lockType:    LockType(next()),
			adopted:     params&0x40 != 0,
			resourceID:  uint64(next()),
			owner:       owner,
			acquireTime: now.UnixNano(),
		}
		if params&0x20 != 0 {
			claim.acquireTime = now.Add(-2 * StaleThreshold).UnixNano()
		}
		switch kind % 5 {
		case 0:
		case 1:
			copy(buf, encodeSlot(claim))
			kinds[slot] = kindClaim
		case 2:
			copy(buf, encodeSlot(claim))
			buf[int(next())*16%SlotSize] ^= 1 << (params % 8)
			kinds[slot] = kindTorn
		case 3:
			for i := range buf[:64] {
				buf[i] = next()
			}
			binary.LittleEndian.PutUint32(buf[0:4], slotMagic)
			binary.LittleEndian.PutUint32(buf[SlotSize-4:], storage.CRC32C(buf[:SlotSize-4]))
			kinds[slot] = kindOdd
		case 4:
			buf[int(next())*16%SlotSize] = params | 1
			kinds[slot] = kindTorn
		}
	}
	return row, kinds
}

// FuzzLockRow checks how a node reads a claim row that other nodes wrote, or
// tore: what conflicts with a claim, and what AdoptOwner and
// ClearForeignClaims write back.
func FuzzLockRow(f *testing.F) {
	f.Add(uint8(3), uint8(0), true, uint8(0), []byte{1, 2, 7, 9})
	f.Add(uint8(0), uint8(5), false, uint8(1), []byte{1, 1, 7, 9, 2, 0x82, 3, 4, 5})
	f.Add(uint8(63), uint8(2), true, uint8(2), []byte{0, 0, 0, 0, 2, 0x40, 1, 2, 3, 3, 9, 9, 9})
	f.Add(uint8(1), uint8(64), true, uint8(3), []byte{4, 0x80, 1, 2, 1, 0xc2, 3, 4, 1, 0x23, 5, 6})

	dir := f.TempDir()
	f.Fuzz(func(t *testing.T, nodeSlot, ignore uint8, exclusive bool, views uint8, desc []byte) {
		self := int(nodeSlot) % SlotsPerRecord
		adoptedSlot := int(ignore) % (SlotsPerRecord + 1)
		ign := adoptedSlot - 1
		mode := LockModeShared
		if exclusive {
			mode = LockModeExclusive
		}
		row, kinds := fuzzRow(desc)

		m := NewManager(nil, 0, RowSize, [16]byte{0xee}, self)
		conflict, lower, by := m.rowConflicts(0, row, mode, ign)
		mustConflict, mayConflict := false, false
		for slot, kind := range kinds {
			if slot == self || slot == ign {
				continue
			}
			s, err := decodeSlot(rowSlot(row, slot))
			switch {
			case err != nil:
				if kind == kindClaim {
					t.Fatalf("slot %d: a claim that passed its checksum reads torn: %v", slot, err)
				}
				mustConflict, mayConflict = true, true
			case s == nil || s.mode == modeWaiting || mode != LockModeExclusive && s.mode != LockModeExclusive:
			case !s.stale() && !s.adopted:
				mustConflict, mayConflict = true, true
			default:
				mayConflict = true
			}
		}
		switch {
		case mustConflict && !conflict:
			t.Fatalf("a live write claim or torn slot did not conflict with a mode %d claim from slot %d", mode, self)
		case !mayConflict && conflict:
			t.Fatalf("slot %d conflicts with a mode %d claim from slot %d, but no slot holds a claim", by.slot, mode, self)
		case lower && !conflict:
			t.Fatal("rowConflicts reported a lower slot without a conflict")
		case conflict && (by.slot == self || by.slot == ign || by.slot < 0 || by.slot >= SlotsPerRecord):
			t.Fatalf("conflict blamed on slot %d (own slot %d, ignored %d)", by.slot, self, ign)
		}
		m.othersWaiting(row)

		state := func(slot int) OwnerState { return OwnerState((int(views) + slot) % 5) }
		m.SetOwnerView(func(slot int, _ bool) ([16]byte, OwnerState) { return [16]byte{byte(slot)}, state(slot) })
		if conflict, _, by := m.rowConflicts(0, row, mode, ign); conflict && state(by.slot) == OwnerReleased {
			t.Fatalf("slot %d conflicts although its registry slot is empty", by.slot)
		}
		m.othersWaiting(row)

		checkRowWrites(t, dir, row, self, adoptedSlot%SlotsPerRecord)
	})
}

// checkRowWrites runs AdoptOwner for the dead node in ownerSlot and
// ClearForeignClaims over a device holding row, and checks that each writes
// only the slot it may: the dead node's, then this node's own.
func checkRowWrites(t *testing.T, dir string, row []byte, self, ownerSlot int) {
	dev := fuzzLockDevice(t, dir)
	buf := storage.NewAlignedBuffer(RowSize)
	copy(buf.Bytes(), row)
	if _, err := dev.WriteAt(buf.Bytes(), 0); err != nil {
		t.Fatal(err)
	}

	m := NewManager(dev, 0, RowSize, [16]byte{0xee}, self)
	adopted, err := m.AdoptOwner(fuzzDeadOwner, ownerSlot)
	if err != nil {
		t.Fatalf("AdoptOwner: %v", err)
	}
	after, err := m.readRow(0)
	if err != nil {
		t.Fatal(err)
	}
	for slot := range SlotsPerRecord {
		if slot != ownerSlot && !bytes.Equal(rowSlot(after, slot), rowSlot(row, slot)) {
			t.Fatalf("AdoptOwner for slot %d changed slot %d", ownerSlot, slot)
		}
	}
	writer := fuzzDeadOwner
	if before, err := decodeSlot(rowSlot(row, ownerSlot)); err == nil && before != nil {
		writer = before.owner
	}
	s, err := decodeSlot(rowSlot(after, ownerSlot))
	switch {
	case err != nil:
		t.Fatalf("AdoptOwner left slot %d torn: %v", ownerSlot, err)
	case len(adopted) == 1 && (s == nil || !s.adopted || s.mode != LockModeExclusive || s.owner != writer || s.owner == m.nodeUUID):
		t.Fatalf("AdoptOwner kept record 0 but slot %d holds %+v", ownerSlot, s)
	case len(adopted) == 0 && s != nil && s.owner != m.nodeUUID:
		t.Fatalf("AdoptOwner left another node's claim in slot %d without keeping it: %+v", ownerSlot, s)
	}
	if err := m.ReleaseAdopted(adopted); err != nil {
		t.Fatalf("ReleaseAdopted: %v", err)
	}

	if _, err := dev.WriteAt(buf.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	m = NewManager(dev, 0, RowSize, [16]byte{0xee}, self)
	if err := m.ClearForeignClaims(); err != nil {
		t.Fatalf("ClearForeignClaims: %v", err)
	}
	if after, err = m.readRow(0); err != nil {
		t.Fatal(err)
	}
	for slot := range SlotsPerRecord {
		if slot != self && !bytes.Equal(rowSlot(after, slot), rowSlot(row, slot)) {
			t.Fatalf("ClearForeignClaims from slot %d changed slot %d", self, slot)
		}
	}
	if s, err := decodeSlot(rowSlot(after, self)); err != nil || s != nil && s.owner != m.nodeUUID {
		t.Fatalf("ClearForeignClaims left slot %d holding %+v (%v)", self, s, err)
	}
}

// fuzzLockDevice opens a one-record lock device in dir, a fresh file per call
// so fuzz workers never share one.
func fuzzLockDevice(t *testing.T, dir string) *storage.Device {
	f, err := os.CreateTemp(dir, "lock-row-*.img")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	t.Cleanup(func() { _ = os.Remove(path) })
	if err := f.Truncate(RowSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	return dev
}
