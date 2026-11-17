package node

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestRegisterRefusesWhileALiveNodeHasAnotherLockLayout(t *testing.T) {
	shortenSilentSlotWatch(t)
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	ctx := context.Background()

	live := NewRegistry(dev, 0)
	live.SetLockLayout(1)
	liveSlot, err := live.Register(ctx, "other-version")
	if err != nil {
		t.Fatal(err)
	}
	if err := live.StartHeartbeat(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Unregister(ctx) }()

	joining := NewRegistry(dev, 0)
	joining.SetLockLayout(2)
	slot, err := joining.Register(ctx, "this-version")
	if !errors.Is(err, ErrLockLayout) || !strings.Contains(err.Error(), "other-version") || !strings.Contains(err.Error(), fmt.Sprintf("slot %d", liveSlot)) {
		t.Fatalf("Register next to a live node with another lock layout = %d, %v; want ErrLockLayout naming node other-version in slot %d", slot, err, liveSlot)
	}
	active, err := joining.GetActiveNodes()
	if err != nil || len(active) != 1 {
		t.Errorf("after the refused Register %d nodes are active (%v), want only the live one", len(active), err)
	}
}

func TestRegisterJoinsPastASilentNodeWithAnotherLockLayout(t *testing.T) {
	shortenSilentSlotWatch(t)
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	gone := writeGoneSlot(t, reg)
	gone.LockLayout = 1
	if err := reg.writeSlot(0, gone); err != nil {
		t.Fatal(err)
	}

	reg.SetLockLayout(2)
	if slot, err := reg.Register(context.Background(), "node"); err != nil || slot != 1 {
		t.Fatalf("Register next to a silent node with another lock layout = %d, %v; want the empty slot 1", slot, err)
	}
}
