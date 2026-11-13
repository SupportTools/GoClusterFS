package lock

import (
	"context"
	"testing"
	"time"
)

const reclaimRegion = 16 * RowSize

func distinctRecords(t *testing.T, m *Manager, resources ...uint64) {
	t.Helper()
	records := map[uint64]bool{}
	for _, res := range resources {
		records[m.hashToRecord(LockTypeInode, res)] = true
	}
	if len(records) != len(resources) {
		t.Fatal("test resources share a lock record; pick others")
	}
}

func TestAdoptOwnerHoldsDeadNodesLocksUntilReleased(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()

	dead := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	alive := NewManager(dev, 0, reclaimRegion, [16]byte{2}, 2)
	replayer := NewManager(dev, 0, reclaimRegion, [16]byte{3}, 3)
	peer := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	distinctRecords(t, replayer, 2, 5, 7)
	ctx := context.Background()

	for _, res := range []uint64{2, 5} {
		if err := dead.Acquire(ctx, LockTypeInode, res, LockModeExclusive); err != nil {
			t.Fatal(err)
		}
	}
	if err := alive.Acquire(ctx, LockTypeInode, 7, LockModeExclusive); err != nil {
		t.Fatal(err)
	}

	adopted, err := replayer.AdoptOwner([16]byte{1}, 1)
	if err != nil {
		t.Fatalf("AdoptOwner: %v", err)
	}
	if len(adopted) != 2 {
		t.Fatalf("adopted %d records, want the dead node's 2 exclusive records", len(adopted))
	}
	requireBlocked(t, peer, LockTypeInode, 2, LockModeExclusive)
	requireAcquired(t, replayer, LockTypeInode, 2, LockModeExclusive)
	if err := replayer.Release(LockTypeInode, 2); err != nil {
		t.Fatal(err)
	}
	replayer.ReleaseCached()

	if err := replayer.ReleaseAdopted(adopted); err != nil {
		t.Fatalf("ReleaseAdopted: %v", err)
	}
	for _, res := range []uint64{2, 5} {
		requireAcquired(t, peer, LockTypeInode, res, LockModeExclusive)
	}
	requireBlocked(t, peer, LockTypeInode, 7, LockModeExclusive)
}

// A dead node's slot can be older than StaleThreshold by the time it is proven
// dead. AdoptOwner writes it again, so peers stay off until the replay ends.
func TestAdoptOwnerRenewsDeadNodesOldClaim(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()

	dead := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	replayer := NewManager(dev, 0, reclaimRegion, [16]byte{3}, 3)
	peer := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	requireAcquired(t, dead, LockTypeInode, 2, LockModeExclusive)
	ageSlot(t, dead, LockTypeInode, 2, pastStale)

	adopted, err := replayer.AdoptOwner([16]byte{1}, 1)
	if err != nil || len(adopted) != 1 {
		t.Fatalf("AdoptOwner = %v, %v; want the dead node's record", adopted, err)
	}
	requireBlocked(t, peer, LockTypeInode, 2, LockModeExclusive)
	if err := replayer.ReleaseAdopted(adopted); err != nil {
		t.Fatal(err)
	}
	requireAcquired(t, peer, LockTypeInode, 2, LockModeExclusive)
}

// A crash in the middle of a slot write leaves a torn slot, which never goes
// stale. AdoptOwner keeps it for the replay and ReleaseAdopted clears it.
func TestAdoptOwnerClearsDeadNodesTornSlot(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()

	replayer := NewManager(dev, 0, reclaimRegion, [16]byte{3}, 3)
	peer := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	tearSlot(t, replayer, LockTypeInode, 2, 1)

	adopted, err := replayer.AdoptOwner([16]byte{1}, 1)
	if err != nil || len(adopted) != 1 {
		t.Fatalf("AdoptOwner = %v, %v; want the torn slot's record", adopted, err)
	}
	requireBlocked(t, peer, LockTypeInode, 2, LockModeExclusive)
	if err := replayer.ReleaseAdopted(adopted); err != nil {
		t.Fatal(err)
	}
	requireAcquired(t, peer, LockTypeInode, 2, LockModeExclusive)
}

// A node restarted into its own registry slot finds its predecessor's claims
// in that slot. They keep peers off during the replay; after it, ReleaseAdopted
// clears the ones the new node does not use itself.
func TestAdoptOwnerOfOwnSlotPredecessor(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()

	before := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	after := NewManager(dev, 0, reclaimRegion, [16]byte{5}, 1)
	peer := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	distinctRecords(t, after, 2, 5)
	for _, res := range []uint64{2, 5} {
		requireAcquired(t, before, LockTypeInode, res, LockModeExclusive)
	}

	adopted, err := after.AdoptOwner([16]byte{1}, 1)
	if err != nil || len(adopted) != 2 {
		t.Fatalf("AdoptOwner = %v, %v; want the predecessor's 2 records", adopted, err)
	}
	requireBlocked(t, peer, LockTypeInode, 2, LockModeExclusive)
	requireAcquired(t, after, LockTypeInode, 5, LockModeExclusive)

	if err := after.ReleaseAdopted(adopted); err != nil {
		t.Fatal(err)
	}
	requireAcquired(t, peer, LockTypeInode, 2, LockModeExclusive)
	requireBlocked(t, peer, LockTypeInode, 5, LockModeExclusive)
}

// The replay's own acquires write the adopted slot of a node restarted into
// its own registry slot. Releasing them must keep peers off until ReleaseAdopted.
func TestOwnSlotAdoptionOutlivesReplayAcquire(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()

	before := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	after := NewManager(dev, 0, reclaimRegion, [16]byte{5}, 1)
	peer := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	requireAcquired(t, before, LockTypeInode, 2, LockModeExclusive)

	adopted, err := after.AdoptOwner([16]byte{1}, 1)
	if err != nil || len(adopted) != 1 {
		t.Fatalf("AdoptOwner = %v, %v; want the predecessor's record", adopted, err)
	}
	requireAcquired(t, after, LockTypeInode, 2, LockModeShared)
	if err := after.Release(LockTypeInode, 2); err != nil {
		t.Fatal(err)
	}
	after.ReleaseCached()
	requireBlocked(t, peer, LockTypeInode, 2, LockModeShared)

	if err := after.ReleaseAdopted(adopted); err != nil {
		t.Fatal(err)
	}
	requireAcquired(t, peer, LockTypeInode, 2, LockModeExclusive)
}

// A waiter in a lower slot than the replayer is blocked by the adopted slot.
// It must defer to it, or the replay waits behind a waiter that waits for the
// replay.
func TestReplayerWinsAdoptedRecordOverLowerWaiter(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()

	dead := NewManager(dev, 0, reclaimRegion, [16]byte{9}, 9)
	waiter := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	replayer := NewManager(dev, 0, reclaimRegion, [16]byte{3}, 3)
	requireAcquired(t, dead, LockTypeInode, 2, LockModeExclusive)
	adopted, err := replayer.AdoptOwner([16]byte{9}, 9)
	if err != nil || len(adopted) != 1 {
		t.Fatalf("AdoptOwner = %v, %v; want the dead node's record", adopted, err)
	}

	waited := make(chan error, 1)
	go func() {
		err := acquireWithin(waiter, 10*time.Second, LockTypeInode, 2, LockModeExclusive)
		if err == nil {
			release(t, waiter, LockTypeInode, 2)
		}
		waited <- err
	}()
	time.Sleep(100 * time.Millisecond)
	requireAcquired(t, replayer, LockTypeInode, 2, LockModeExclusive)
	if err := replayer.Release(LockTypeInode, 2); err != nil {
		t.Fatal(err)
	}
	replayer.ReleaseCached()
	if err := replayer.ReleaseAdopted(adopted); err != nil {
		t.Fatal(err)
	}
	if err := <-waited; err != nil {
		t.Fatalf("waiter never got the record after the replay: %v", err)
	}
}

// A new node can register into the dead node's slot before ReleaseAdopted.
// What it writes there is its own claim and must stay.
func TestReleaseAdoptedKeepsNewOwnersClaim(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()

	dead := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	replayer := NewManager(dev, 0, reclaimRegion, [16]byte{3}, 3)
	peer := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	requireAcquired(t, dead, LockTypeInode, 2, LockModeExclusive)
	adopted, err := replayer.AdoptOwner([16]byte{1}, 1)
	if err != nil || len(adopted) != 1 {
		t.Fatalf("AdoptOwner = %v, %v; want the dead node's record", adopted, err)
	}

	newcomer := NewManager(dev, 0, reclaimRegion, [16]byte{7}, 1)
	requireAcquired(t, newcomer, LockTypeInode, 2, LockModeExclusive)
	if err := replayer.ReleaseAdopted(adopted); err != nil {
		t.Fatal(err)
	}
	requireBlocked(t, peer, LockTypeInode, 2, LockModeExclusive)
}

func TestAdoptOwnerLeavesSharedRecordOfReleasedReader(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()

	reader := NewManager(dev, 0, reclaimRegion, [16]byte{2}, 2)
	crashed := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	replayer := NewManager(dev, 0, reclaimRegion, [16]byte{3}, 3)
	writer := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	ctx := context.Background()

	if err := reader.Acquire(ctx, LockTypeInode, 9, LockModeShared); err != nil {
		t.Fatal(err)
	}
	if err := crashed.Acquire(ctx, LockTypeInode, 9, LockModeShared); err != nil {
		t.Fatal(err)
	}
	if err := crashed.Release(LockTypeInode, 9); err != nil {
		t.Fatal(err)
	}
	crashed.ReleaseCached()

	if adopted, err := replayer.AdoptOwner([16]byte{1}, 1); err != nil || len(adopted) != 0 {
		t.Fatalf("AdoptOwner = %v, %v; want nothing taken from a reader that already released", adopted, err)
	}
	if err := acquireWithin(writer, time.Second, LockTypeInode, 9, LockModeExclusive); err == nil {
		t.Error("a writer took a record another node is still reading")
	}
}

// A node killed while it kept a read hold leaves its read slot. Once it is
// proven dead, AdoptOwner clears the slot and a writer gets the record at once.
func TestAdoptOwnerClearsDeadReader(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()

	dead := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	alive := NewManager(dev, 0, reclaimRegion, [16]byte{2}, 2)
	writer := NewManager(dev, 0, reclaimRegion, [16]byte{3}, 3)
	ctx := context.Background()
	for _, m := range []*Manager{dead, alive} {
		if err := m.Acquire(ctx, LockTypeInode, 9, LockModeShared); err != nil {
			t.Fatal(err)
		}
	}
	if err := alive.Release(LockTypeInode, 9); err != nil {
		t.Fatal(err)
	}
	alive.ReleaseCached()

	if _, err := writer.AdoptOwner([16]byte{1}, 1); err != nil {
		t.Fatalf("AdoptOwner: %v", err)
	}
	requireAcquired(t, writer, LockTypeInode, 9, LockModeExclusive)
}

// After a failed replay the dead node's claims stay on the device, so peers and
// the node that failed both wait for a later replay.
func TestForgetAdoptedKeepsDeadNodesClaims(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()
	dead := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	replayer := NewManager(dev, 0, reclaimRegion, [16]byte{3}, 3)
	peer := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	requireAcquired(t, dead, LockTypeInode, 2, LockModeExclusive)
	adopted, err := replayer.AdoptOwner([16]byte{1}, 1)
	if err != nil || len(adopted) != 1 {
		t.Fatalf("AdoptOwner = %v, %v", adopted, err)
	}

	replayer.ForgetAdopted(adopted)
	requireBlocked(t, peer, LockTypeInode, 2, LockModeExclusive)
	requireBlocked(t, replayer, LockTypeInode, 2, LockModeExclusive)
}

// A node that takes a slot whose earlier occupant left claims, which no replay
// adopted, clears them so they do not count as its own; its own holds stay.
func TestClearForeignClaimsKeepsOwnHolds(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()
	occupant := NewManager(dev, 0, reclaimRegion, [16]byte{15}, 5)
	peer := NewManager(dev, 0, reclaimRegion, [16]byte{6}, 6)
	reg := newFakeRegistry()
	reg.set(5, OwnerAlive)
	reg.set(6, OwnerAlive)
	peer.SetOwnerView(reg.view)
	distinctRecords(t, peer, 2, 3, 4)

	writeClaim(t, peer, LockTypeInode, 2, 5, LockModeExclusive)
	tearSlot(t, peer, LockTypeInode, 3, 5)
	requireAcquired(t, occupant, LockTypeInode, 4, LockModeExclusive)

	if err := occupant.ClearForeignClaims(); err != nil {
		t.Fatal(err)
	}
	for _, res := range []uint64{2, 3} {
		requireAcquired(t, peer, LockTypeInode, res, LockModeExclusive)
		if err := peer.Release(LockTypeInode, res); err != nil {
			t.Fatal(err)
		}
	}
	requireBlocked(t, peer, LockTypeInode, 4, LockModeExclusive)
}
