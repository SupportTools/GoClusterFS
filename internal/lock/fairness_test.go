package lock

import (
	"slices"
	"sync"
	"testing"
	"time"
)

// reclaimAfterExpiry holds the lock briefly, releases it, and claims it again as
// soon as m's cached hold is gone from disk, until stop closes.
func reclaimAfterExpiry(t *testing.T, m *Manager, lt LockType, id uint64, stop <-chan struct{}) {
	recordID := m.hashToRecord(lt, id)
	for {
		select {
		case <-stop:
			return
		default:
		}
		if err := acquireWithin(m, 5*time.Second, lt, id, LockModeExclusive); err != nil {
			t.Errorf("node %d Acquire: %v", m.nodeSlot, err)
			return
		}
		time.Sleep(time.Millisecond)
		release(t, m, lt, id)
		for {
			m.mu.Lock()
			_, kept := m.records[recordID]
			m.mu.Unlock()
			if !kept {
				break
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
}

func TestHigherSlotGetsLockFromLowerSlotThatKeepsReclaimingIt(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	low := newTestNode(dev, 0, 1<<20, 1)
	high := newTestNode(dev, 0, 1<<20, 2)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() { reclaimAfterExpiry(t, low, LockTypeInode, 3, stop) })
	defer func() { close(stop); wg.Wait() }()
	time.Sleep(20 * time.Millisecond)

	var waits []time.Duration
	for range 10 {
		start := time.Now()
		if err := acquireWithin(high, 5*time.Second, LockTypeInode, 3, LockModeExclusive); err != nil {
			t.Fatalf("higher slot never got the lock: %v", err)
		}
		waits = append(waits, time.Since(start))
		release(t, high, LockTypeInode, 3)
		time.Sleep(20 * time.Millisecond)
	}
	if longest := slices.Max(waits); longest > 2*time.Second {
		t.Errorf("higher slot waited up to %v, want under 2s: %v", longest, waits)
	}
}

func TestHolderYieldsOnlyToLiveWaiters(t *testing.T) {
	cases := []struct {
		name   string
		view   bool
		state  OwnerState
		owner  byte
		age    time.Duration
		yields bool
	}{
		{name: "live waiter", view: true, state: OwnerAlive, owner: 5, yields: true},
		{name: "failing waiter", view: true, state: OwnerFailing, owner: 5},
		{name: "replayed waiter", view: true, state: OwnerReleased, owner: 5},
		{name: "mark of the slot's earlier node", view: true, state: OwnerAlive, owner: 9},
		{name: "fresh mark without registry", yields: true},
		{name: "stale mark without registry", age: pastStale},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dev, cleanup := createTestDevice(t, 1<<20)
			defer cleanup()
			holder := newTestNode(dev, 0, 1<<20, 1)
			holder.timing.holdTurn = 50 * time.Millisecond
			if tc.view {
				reg := newFakeRegistry()
				reg.set(5, tc.state)
				holder.SetOwnerView(reg.view)
			}
			mark := &claimSlot{mode: modeWaiting, lockType: LockTypeInode, resourceID: 3, owner: [16]byte{tc.owner}, acquireTime: time.Now().Add(-tc.age).UnixNano()}
			if err := holder.writeSlot(holder.hashToRecord(LockTypeInode, 3), 5, mark); err != nil {
				t.Fatal(err)
			}

			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() { reclaimAfterExpiry(t, holder, LockTypeInode, 3, stop) })
			time.Sleep(4 * holder.timing.holdTurn)
			close(stop)
			wg.Wait()

			var blocked uint64
			for _, ts := range holder.Stats().Types {
				if ts.Name == "inode" {
					blocked = ts.Blocked
				}
			}
			if yielded := blocked > 0; yielded != tc.yields {
				t.Errorf("holder yielded: %v (%d blocked acquires), want %v", yielded, blocked, tc.yields)
			}
		})
	}
}

func TestHolderKeepsRecordNobodyWaitsFor(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	m := newTestNode(dev, 0, 1<<20, 1)
	m.timing.holdTurn = 20 * time.Millisecond
	for deadline := time.Now().Add(10 * m.timing.holdTurn); time.Now().Before(deadline); {
		requireAcquired(t, m, LockTypeInode, 3, LockModeShared)
		time.Sleep(time.Millisecond)
		release(t, m, LockTypeInode, 3)
	}
	for _, ts := range m.Stats().Types {
		if ts.Name == "inode" && ts.Blocked > 0 {
			t.Errorf("%d of %d acquires waited, though no other node wanted the record", ts.Blocked, ts.Acquires)
		}
	}
}

// A node whose new claim finds a waiter it has seen for a whole turn keeps the
// claim for minTurn, so a request that follows another at once, like a write
// after a truncate, does not wait for the other node.
func TestContestedHolderKeepsItsNewClaimBriefly(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	m := newTestNode(dev, 0, 1<<20, 1)
	m.timing.holdTurn = 30 * time.Millisecond
	m.timing.minTurn = 200 * time.Millisecond
	mark := &claimSlot{mode: modeWaiting, wants: LockModeExclusive, lockType: LockTypeFileIO, resourceID: 3, owner: [16]byte{5}, acquireTime: time.Now().UnixNano()}
	if err := m.writeSlot(m.hashToRecord(LockTypeFileIO, 3), 5, mark); err != nil {
		t.Fatal(err)
	}

	requireAcquired(t, m, LockTypeFileIO, 3, LockModeExclusive)
	release(t, m, LockTypeFileIO, 3)
	time.Sleep(3 * m.timing.holdTurn)
	requireAcquired(t, m, LockTypeFileIO, 3, LockModeExclusive)
	release(t, m, LockTypeFileIO, 3)
	requireAcquired(t, m, LockTypeFileIO, 3, LockModeExclusive)
	release(t, m, LockTypeFileIO, 3)
	for _, ts := range m.Stats().Types {
		if ts.Name == "file_io" && ts.Blocked > 0 {
			t.Errorf("an acquire right after a release waited %v, though the claim was new", ts.Wait)
		}
	}
}
