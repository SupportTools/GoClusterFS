package lock

import (
	"context"
	"testing"
	"time"
)

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
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()

	dead := NewManager(dev, 0, 1<<20, [16]byte{1}, 1)
	alive := NewManager(dev, 0, 1<<20, [16]byte{2}, 2)
	replayer := NewManager(dev, 0, 1<<20, [16]byte{3}, 3)
	peer := NewManager(dev, 0, 1<<20, [16]byte{4}, 4)
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

	adopted, err := replayer.AdoptOwner([16]byte{1})
	if err != nil {
		t.Fatalf("AdoptOwner: %v", err)
	}
	if len(adopted) != 2 {
		t.Fatalf("adopted %d records, want the dead node's 2 exclusive records", len(adopted))
	}
	if err := acquireWithin(peer, time.Second, LockTypeInode, 2, LockModeExclusive); err == nil {
		t.Error("a peer took a dead node's lock while its journal was being replayed")
	}
	if err := acquireWithin(replayer, time.Second, LockTypeInode, 2, LockModeExclusive); err != nil {
		t.Errorf("the replaying node could not use a lock it adopted: %v", err)
	}
	if err := replayer.Release(LockTypeInode, 2); err != nil {
		t.Fatal(err)
	}

	if err := replayer.ReleaseAdopted(adopted); err != nil {
		t.Fatalf("ReleaseAdopted: %v", err)
	}
	for _, res := range []uint64{2, 5} {
		if err := acquireWithin(peer, 2*time.Second, LockTypeInode, res, LockModeExclusive); err != nil {
			t.Errorf("lock %d still held after the replay released it: %v", res, err)
		}
	}
	held, err := replayer.readRecord(replayer.hashToRecord(LockTypeInode, 7))
	if err != nil {
		t.Fatal(err)
	}
	if held.Mode != LockModeExclusive || held.OwnerUUID != ([16]byte{2}) {
		t.Errorf("live node's lock changed: mode %d owner %x", held.Mode, held.OwnerUUID)
	}
}

func TestAdoptOwnerLeavesSharedRecordOfReleasedReader(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()

	reader := NewManager(dev, 0, 1<<20, [16]byte{2}, 2)
	crashed := NewManager(dev, 0, 1<<20, [16]byte{1}, 1)
	replayer := NewManager(dev, 0, 1<<20, [16]byte{3}, 3)
	writer := NewManager(dev, 0, 1<<20, [16]byte{4}, 4)
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

	if adopted, err := replayer.AdoptOwner([16]byte{1}); err != nil || len(adopted) != 0 {
		t.Fatalf("AdoptOwner = %v, %v; want nothing taken from a reader that already released", adopted, err)
	}
	if err := acquireWithin(writer, time.Second, LockTypeInode, 9, LockModeExclusive); err == nil {
		t.Error("a writer took a record another node is still reading")
	}
}
