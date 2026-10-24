package lock

import (
	"context"
	"testing"
	"time"
)

func TestReclaimOwnerFreesOnlyTheDeadNodesHolds(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()

	dead := NewManager(dev, 0, 1<<20, [16]byte{1}, 1)
	alive := NewManager(dev, 0, 1<<20, [16]byte{2}, 2)
	survivor := NewManager(dev, 0, 1<<20, [16]byte{3}, 3)
	ctx := context.Background()
	records := map[uint64]bool{}
	for _, res := range []uint64{2, 7, 9} {
		records[survivor.hashToRecord(LockTypeInode, res)] = true
	}
	if len(records) != 3 {
		t.Fatal("test resources share a lock record; pick others")
	}

	if err := dead.Acquire(ctx, LockTypeInode, 2, LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	if err := alive.Acquire(ctx, LockTypeInode, 7, LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	if err := alive.Acquire(ctx, LockTypeInode, 9, LockModeShared); err != nil {
		t.Fatal(err)
	}
	if err := dead.Acquire(ctx, LockTypeInode, 9, LockModeShared); err != nil {
		t.Fatal(err)
	}

	n, err := survivor.ReclaimOwner([16]byte{1})
	if err != nil {
		t.Fatalf("ReclaimOwner: %v", err)
	}
	if n != 2 {
		t.Errorf("reclaimed %d records, want the dead node's exclusive record and its shared count", n)
	}

	short, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := survivor.Acquire(short, LockTypeInode, 2, LockModeExclusive); err != nil {
		t.Errorf("dead node's exclusive lock still held after ReclaimOwner: %v", err)
	}

	shared, err := survivor.readRecord(survivor.hashToRecord(LockTypeInode, 9))
	if err != nil {
		t.Fatal(err)
	}
	if shared.Mode != LockModeShared || shared.SharedCount != 1 {
		t.Errorf("shared record is mode %d with %d readers, want the live reader kept", shared.Mode, shared.SharedCount)
	}
	held, err := survivor.readRecord(survivor.hashToRecord(LockTypeInode, 7))
	if err != nil {
		t.Fatal(err)
	}
	if held.Mode != LockModeExclusive || held.OwnerUUID != ([16]byte{2}) {
		t.Errorf("live node's exclusive record changed: mode %d owner %x", held.Mode, held.OwnerUUID)
	}
}
