package lock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const flockRegion = layoutRows * RowSize

// flockNodes returns managers in slots 1..n of one device, each alive in a shared registry view.
func flockNodes(t *testing.T, n int) ([]*Manager, *fakeRegistry) {
	t.Helper()
	dev, cleanup := createTestDevice(t, flockRegion)
	t.Cleanup(cleanup)
	reg := newFakeRegistry()
	nodes := make([]*Manager, n)
	for i := range nodes {
		nodes[i] = newTestNode(dev, 0, flockRegion, i+1)
		nodes[i].SetOwnerView(reg.view)
		reg.set(i+1, OwnerAlive)
	}
	return nodes, reg
}

// keysInOneRecord returns n flock keys that map to one record.
func keysInOneRecord(m *Manager, n int) []FlockKey {
	keys := []FlockKey{{Inode: 1, Birth: 1}}
	record, _ := m.flockRecord(keys[0])
	for ino := uint32(2); len(keys) < n; ino++ {
		key := FlockKey{Inode: ino, Birth: 1}
		if r, _ := m.flockRecord(key); r == record {
			keys = append(keys, key)
		}
	}
	return keys
}

func flockWithin(m *Manager, d time.Duration, key FlockKey, mode LockMode) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return m.Flock(ctx, key, mode, true)
}

func flockInBackground(m *Manager, key FlockKey, mode LockMode) <-chan error {
	result := make(chan error, 1)
	go func() { result <- m.Flock(context.Background(), key, mode, true) }()
	return result
}

func requireFlockWaiting(t *testing.T, result <-chan error, d time.Duration) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("flock returned %v while another node holds it", err)
	case <-time.After(d):
	}
}

func requireFlocked(t *testing.T, result <-chan error, d time.Duration) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("flock = %v", err)
		}
	case <-time.After(d):
		t.Fatalf("flock still waits after %v", d)
	}
}

func ownSlotEntries(t *testing.T, m *Manager, key FlockKey) map[FlockKey]flockEntry {
	t.Helper()
	record, _ := m.flockRecord(key)
	buf := storage.NewAlignedBuffer(SlotSize)
	if _, err := m.dev.ReadAtUnsharded(buf.Bytes(), m.slotOffset(record, m.nodeSlot)); err != nil {
		t.Fatal(err)
	}
	_, entries, err := decodeFlockSlot(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestFlockSlotRoundTrip(t *testing.T) {
	owner := [16]byte{7}
	entries := map[FlockKey]flockEntry{
		{Inode: 9, Birth: 3}:             {mode: LockModeExclusive},
		{Inode: 9, Birth: 4}:             {mode: LockModeShared, waiting: true},
		{Inode: 1 << 31, Birth: 1 << 63}: {mode: LockModeShared},
	}
	buf := encodeFlockSlot(owner, entries)
	gotOwner, got, err := decodeFlockSlot(buf)
	if err != nil || gotOwner != owner || len(got) != len(entries) {
		t.Fatalf("decode = %x, %v, %v; want %x and %d entries", gotOwner, got, err, owner, len(entries))
	}
	for k, e := range entries {
		if got[k] != e {
			t.Errorf("entry %v = %+v, want %+v", k, got[k], e)
		}
	}

	if _, got, err := decodeFlockSlot(encodeFlockSlot(owner, nil)); err != nil || got != nil {
		t.Errorf("empty slot decodes as %v, %v; want no entries", got, err)
	}
	buf[flockHeader] ^= 1
	if _, _, err := decodeFlockSlot(buf); !errors.Is(err, errTornSlot) {
		t.Errorf("slot with a flipped bit decodes with %v, want errTornSlot", err)
	}
	if _, _, err := decodeFlockSlot(encodeSlot(&claimSlot{mode: LockModeExclusive})); !errors.Is(err, errTornSlot) {
		t.Errorf("claim slot decodes as a flock slot with %v, want errTornSlot", err)
	}

	full := map[FlockKey]flockEntry{}
	for i := range FlockCapacity {
		full[FlockKey{Inode: uint32(i)}] = flockEntry{mode: LockModeShared}
	}
	if _, got, err := decodeFlockSlot(encodeFlockSlot(owner, full)); err != nil || len(got) != FlockCapacity {
		t.Errorf("full slot decodes as %d entries, %v; want %d", len(got), err, FlockCapacity)
	}
}

func TestFlockRecordsAreTheirOwn(t *testing.T) {
	m := &Manager{size: flockRegion}
	for ino := range uint32(1 << 12) {
		r, err := m.flockRecord(FlockKey{Inode: ino})
		if err != nil || !m.isFlockRecord(r) {
			t.Fatalf("inode %d maps to record %d (%v), which is not a flock record", ino, r, err)
		}
	}
	for lt := LockType(0); lt < LockTypeFlock; lt++ {
		for id := range uint64(1 << 12) {
			if r := m.hashToRecord(lt, id); m.isFlockRecord(r) {
				t.Fatalf("type %d resource %d maps to flock record %d", lt, id, r)
			}
		}
	}
	if LayoutID() == PreFlockLayoutID() {
		t.Fatal("the flock records left LayoutID unchanged")
	}

	small := &Manager{size: 64 * RowSize}
	if _, err := small.flockRecord(FlockKey{Inode: 1}); !errors.Is(err, ErrNoFlockRecords) {
		t.Fatalf("flock in a region without typed records = %v, want ErrNoFlockRecords", err)
	}
}

func TestExclusiveFlockKeepsOtherNodesOut(t *testing.T) {
	nodes, _ := flockNodes(t, 2)
	a, b := nodes[0], nodes[1]
	key := FlockKey{Inode: 5, Birth: 1}
	ctx := context.Background()

	if err := a.Flock(ctx, key, LockModeExclusive, false); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []LockMode{LockModeShared, LockModeExclusive} {
		if err := b.Flock(ctx, key, mode, false); !errors.Is(err, ErrWouldBlock) {
			t.Fatalf("mode %d next to an exclusive flock = %v, want ErrWouldBlock", mode, err)
		}
		if entries := ownSlotEntries(t, b, key); len(entries) != 0 {
			t.Fatalf("a refused flock left entries %v", entries)
		}
	}
	if err := a.Unflock(key); err != nil {
		t.Fatal(err)
	}
	if err := b.Flock(ctx, key, LockModeExclusive, false); err != nil {
		t.Fatalf("flock after the holder released = %v", err)
	}
}

func TestSharedFlocksHoldTogether(t *testing.T) {
	nodes, _ := flockNodes(t, 3)
	key := FlockKey{Inode: 5, Birth: 1}
	ctx := context.Background()
	for i, m := range nodes[:2] {
		if err := m.Flock(ctx, key, LockModeShared, false); err != nil {
			t.Fatalf("node %d shared flock = %v", i+1, err)
		}
	}
	if err := nodes[2].Flock(ctx, key, LockModeExclusive, false); !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("exclusive flock next to shared ones = %v, want ErrWouldBlock", err)
	}
}

func TestFlocksOnFilesSharingARecordDoNotBlockEachOther(t *testing.T) {
	nodes, _ := flockNodes(t, 2)
	keys := keysInOneRecord(nodes[0], 2)
	ctx := context.Background()
	if err := nodes[0].Flock(ctx, keys[0], LockModeExclusive, false); err != nil {
		t.Fatal(err)
	}
	if err := nodes[1].Flock(ctx, keys[1], LockModeExclusive, false); err != nil {
		t.Fatalf("flock on inode %d next to one on inode %d in the same record = %v", keys[1].Inode, keys[0].Inode, err)
	}
	if err := nodes[1].Flock(ctx, FlockKey{Inode: keys[0].Inode, Birth: 2}, LockModeExclusive, false); err != nil {
		t.Fatalf("flock on a later inode in the slot of a held one = %v", err)
	}
}

func TestFlockWaiterWakesAtAHint(t *testing.T) {
	nodes, _ := flockNodes(t, 2)
	a, b := nodes[0], nodes[1]
	setTimings(func(tm *timings) { tm.flockPoll = time.Hour }, a, b)
	a.SetFlockHint(b.FlockChanged)
	key := FlockKey{Inode: 5, Birth: 1}

	if err := a.Flock(context.Background(), key, LockModeExclusive, true); err != nil {
		t.Fatal(err)
	}
	result := flockInBackground(b, key, LockModeExclusive)
	requireFlockWaiting(t, result, 200*time.Millisecond)
	if err := a.Unflock(key); err != nil {
		t.Fatal(err)
	}
	requireFlocked(t, result, 2*time.Second)
}

func TestFlockWaiterWakesWithoutAHint(t *testing.T) {
	nodes, _ := flockNodes(t, 2)
	a, b := nodes[0], nodes[1]
	setTimings(func(tm *timings) { tm.flockPoll = 50 * time.Millisecond }, a, b)
	key := FlockKey{Inode: 5, Birth: 1}

	if err := a.Flock(context.Background(), key, LockModeShared, true); err != nil {
		t.Fatal(err)
	}
	result := flockInBackground(b, key, LockModeExclusive)
	requireFlockWaiting(t, result, 200*time.Millisecond)
	if err := a.Unflock(key); err != nil {
		t.Fatal(err)
	}
	requireFlocked(t, result, 2*time.Second)
}

func TestEndedFlockWaitLeavesNoEntry(t *testing.T) {
	nodes, _ := flockNodes(t, 3)
	a, b, c := nodes[0], nodes[1], nodes[2]
	setTimings(func(tm *timings) { tm.flockPoll = 20 * time.Millisecond }, a, b, c)
	key := FlockKey{Inode: 5, Birth: 1}

	if err := a.Flock(context.Background(), key, LockModeExclusive, true); err != nil {
		t.Fatal(err)
	}
	for _, waiter := range []*Manager{b, c} {
		if err := flockWithin(waiter, 100*time.Millisecond, key, LockModeExclusive); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("slot %d: ended wait = %v, want context.DeadlineExceeded", waiter.nodeSlot, err)
		}
		if entries := ownSlotEntries(t, waiter, key); len(entries) != 0 {
			t.Fatalf("slot %d: an ended wait left entries %v", waiter.nodeSlot, entries)
		}
	}
	if err := a.Unflock(key); err != nil {
		t.Fatal(err)
	}
	if err := c.Flock(context.Background(), key, LockModeExclusive, false); err != nil {
		t.Fatalf("flock after the waits ended and the holder released = %v", err)
	}
}

// Of two nodes that claim at once, the lower slot keeps its claim and the higher one waits.
func TestConcurrentFlockClaimsLetTheLowerSlotWin(t *testing.T) {
	nodes, _ := flockNodes(t, 2)
	low, high := nodes[0], nodes[1]
	key := FlockKey{Inode: 5, Birth: 1}
	record, _ := low.flockRecord(key)

	gate := high.recordGate(record)
	gate.Lock()
	err := high.writeFlocks(record, map[FlockKey]flockEntry{key: {mode: LockModeExclusive}})
	gate.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	won, blockers, err := low.claimFlock(record, key, LockModeExclusive, true)
	if err != nil || won || len(blockers) != 1 || blockers[0] != high.nodeSlot {
		t.Fatalf("lower claim next to a higher one = won %v, blockers %v, %v; want a wait on slot %d", won, blockers, err, high.nodeSlot)
	}
	if e, _ := low.ownFlock(record, key); e.waiting {
		t.Fatal("the lower slot turned its claim into a waiting entry")
	}

	won, _, err = high.claimFlock(record, key, LockModeExclusive, true)
	if err != nil || won {
		t.Fatalf("higher claim next to a lower one = won %v, %v; want a wait", won, err)
	}
	if e, _ := high.ownFlock(record, key); !e.waiting {
		t.Fatal("the higher slot kept its claim next to a lower one")
	}

	if err := low.Flock(context.Background(), key, LockModeExclusive, false); err != nil {
		t.Fatalf("lower slot's flock after the higher one waits = %v", err)
	}
}

func TestDowngradedFlockLetsSharedFlocksIn(t *testing.T) {
	nodes, _ := flockNodes(t, 2)
	a, b := nodes[0], nodes[1]
	key := FlockKey{Inode: 5, Birth: 1}
	ctx := context.Background()

	if err := a.Flock(ctx, key, LockModeExclusive, false); err != nil {
		t.Fatal(err)
	}
	if err := a.Flock(ctx, key, LockModeShared, false); err != nil {
		t.Fatal(err)
	}
	if err := b.Flock(ctx, key, LockModeShared, false); err != nil {
		t.Fatalf("shared flock next to a downgraded one = %v", err)
	}
	if err := b.Unflock(key); err != nil {
		t.Fatal(err)
	}
	if err := b.Flock(ctx, key, LockModeExclusive, false); !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("exclusive flock next to a downgraded one = %v, want ErrWouldBlock", err)
	}
}

func TestDeadNodesFlocksFreeWhenItIsTakenOver(t *testing.T) {
	nodes, reg := flockNodes(t, 3)
	dead, waiter, survivor := nodes[0], nodes[1], nodes[2]
	setTimings(func(tm *timings) { tm.flockPoll = 20 * time.Millisecond }, dead, waiter, survivor)
	key := FlockKey{Inode: 5, Birth: 1}

	if err := dead.Flock(context.Background(), key, LockModeExclusive, true); err != nil {
		t.Fatal(err)
	}
	result := flockInBackground(waiter, key, LockModeExclusive)
	reg.set(dead.nodeSlot, OwnerFailing)
	requireFlockWaiting(t, result, 200*time.Millisecond)

	reg.set(dead.nodeSlot, OwnerRecovering)
	adopted, err := survivor.AdoptOwner(dead.nodeUUID, dead.nodeSlot)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range adopted {
		if survivor.isFlockRecord(a.Record) {
			t.Fatalf("AdoptOwner kept flock record %d for the replay", a.Record)
		}
	}
	requireFlocked(t, result, 2*time.Second)
}

func TestFlocksOfAnEmptyRegistrySlotDoNotBlock(t *testing.T) {
	nodes, reg := flockNodes(t, 2)
	key := FlockKey{Inode: 5, Birth: 1}
	if err := nodes[0].Flock(context.Background(), key, LockModeExclusive, false); err != nil {
		t.Fatal(err)
	}
	reg.set(nodes[0].nodeSlot, OwnerReleased)
	if err := nodes[1].Flock(context.Background(), key, LockModeExclusive, false); err != nil {
		t.Fatalf("flock next to an entry of an empty registry slot = %v", err)
	}
}

func TestTornFlockSlotBlocksUntilItsRegistrySlotEmpties(t *testing.T) {
	nodes, reg := flockNodes(t, 2)
	torn, m := nodes[0], nodes[1]
	key := FlockKey{Inode: 5, Birth: 1}
	record, _ := m.flockRecord(key)

	buf := storage.NewAlignedBuffer(SlotSize)
	copy(buf.Bytes(), encodeFlockSlot(torn.nodeUUID, map[FlockKey]flockEntry{{Inode: 99}: {mode: LockModeShared}}))
	buf.Bytes()[flockHeader] ^= 1
	if _, err := m.dev.WriteAtUnsharded(buf.Bytes(), m.slotOffset(record, torn.nodeSlot)); err != nil {
		t.Fatal(err)
	}
	if err := m.Flock(context.Background(), key, LockModeShared, false); !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("flock next to a torn slot of a live node = %v, want ErrWouldBlock", err)
	}
	reg.set(torn.nodeSlot, OwnerReleased)
	if err := m.Flock(context.Background(), key, LockModeShared, false); err != nil {
		t.Fatalf("flock next to a torn slot of an empty registry slot = %v", err)
	}
}

func TestFlocksBeyondASlotsCapacityAreRefused(t *testing.T) {
	nodes, _ := flockNodes(t, 1)
	m := nodes[0]
	keys := keysInOneRecord(m, FlockCapacity+1)
	for _, key := range keys[:FlockCapacity] {
		if err := m.Flock(context.Background(), key, LockModeShared, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Flock(context.Background(), keys[FlockCapacity], LockModeShared, false); !errors.Is(err, ErrFlockFull) {
		t.Fatalf("flock %d in one record = %v, want ErrFlockFull", FlockCapacity+1, err)
	}
	if err := m.Flock(context.Background(), keys[0], LockModeExclusive, false); err != nil {
		t.Fatalf("changing the mode of a held flock in a full slot = %v", err)
	}
}

func TestMountClearsLeftoverFlocks(t *testing.T) {
	nodes, _ := flockNodes(t, 2)
	m, peer := nodes[0], nodes[1]
	key := FlockKey{Inode: 5, Birth: 1}
	if err := m.Flock(context.Background(), key, LockModeExclusive, false); err != nil {
		t.Fatal(err)
	}

	remounted := newTestNode(m.dev, 0, flockRegion, m.nodeSlot)
	remounted.SetOwnerView(m.ownerView)
	if err := remounted.ClearForeignClaims(); err != nil {
		t.Fatal(err)
	}
	if entries := ownSlotEntries(t, remounted, key); len(entries) != 0 {
		t.Fatalf("entries %v of the slot's earlier mount survived ClearForeignClaims", entries)
	}
	if err := peer.Flock(context.Background(), key, LockModeExclusive, false); err != nil {
		t.Fatalf("flock after the remount cleared the old entry = %v", err)
	}

	held := FlockKey{Inode: 6, Birth: 1}
	if err := remounted.Flock(context.Background(), held, LockModeExclusive, false); err != nil {
		t.Fatal(err)
	}
	if err := remounted.ClearForeignClaims(); err != nil {
		t.Fatal(err)
	}
	if entries := ownSlotEntries(t, remounted, held); len(entries) != 1 {
		t.Fatalf("ClearForeignClaims cleared a flock this node holds; slot has %v", entries)
	}
}

func TestReleaseFlocksClearsEveryEntry(t *testing.T) {
	nodes, _ := flockNodes(t, 2)
	m, peer := nodes[0], nodes[1]
	keys := []FlockKey{{Inode: 5, Birth: 1}, {Inode: 6, Birth: 1}, {Inode: 700, Birth: 2}}
	for _, key := range keys {
		if err := m.Flock(context.Background(), key, LockModeExclusive, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.ReleaseFlocks(); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if err := peer.Flock(context.Background(), key, LockModeExclusive, false); err != nil {
			t.Fatalf("flock on inode %d after ReleaseFlocks = %v", key.Inode, err)
		}
	}
}

func TestFailedFlockWriteIsWrittenAgain(t *testing.T) {
	nodes, _ := flockNodes(t, 2)
	m, peer := nodes[0], nodes[1]
	key := FlockKey{Inode: 5, Birth: 1}
	if err := m.Flock(context.Background(), key, LockModeExclusive, false); err != nil {
		t.Fatal(err)
	}
	m.slotWrite = func([]byte, int64) (int, error) { return 0, errors.New("injected write failure") }
	if err := m.Unflock(key); err == nil {
		t.Fatal("Unflock with a failing write succeeded")
	}
	m.slotWrite = nil
	if err := peer.Flock(context.Background(), key, LockModeExclusive, false); !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("flock while the release write failed = %v, want ErrWouldBlock", err)
	}
	m.retryFlockWrites()
	if err := peer.Flock(context.Background(), key, LockModeExclusive, false); err != nil {
		t.Fatalf("flock after the retry wrote the release = %v", err)
	}
}
