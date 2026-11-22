package main

import (
	"slices"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/fence"
	"github.com/supporttools/GoClusterFS/internal/node"
)

func slotOf(state node.NodeState, id byte) *node.Slot {
	return &node.Slot{State: state, UUID: [16]byte{id}}
}

// A key counts as owned while its node is in a slot, live or dead and waiting for a replay. An emptied slot's old
// node, a node in no slot and a tool key own nothing, and a key a node registered on two paths is listed once.
func TestUnownedKeys(t *testing.T) {
	slots := []*node.Slot{slotOf(node.NodeStateActive, 1), slotOf(node.NodeStateDead, 2), slotOf(node.NodeStateEmpty, 3), nil}
	tool := fence.ToolKey()
	keys := []uint64{fence.NodeKey([16]byte{1}), fence.NodeKey([16]byte{2}), fence.NodeKey([16]byte{3}), fence.NodeKey([16]byte{3}), tool, fence.NodeKey([16]byte{4})}
	want := []uint64{fence.NodeKey([16]byte{3}), tool, fence.NodeKey([16]byte{4})}
	if got := unownedKeys(keys, slots); !slices.Equal(got, want) {
		t.Errorf("unownedKeys = %x, want %x", got, want)
	}
}

// A joining node registers its key before its slot claim lands, so only a key without an owner in both checks is
// stale: not one whose node claimed its slot by the second check, nor one registered between the checks. A mount that
// stops before the second check reports nothing.
func TestStaleKeyCheckNeedsBothChecks(t *testing.T) {
	stale, claimed, newcomer := fence.NodeKey([16]byte{7}), fence.NodeKey([16]byte{8}), fence.NodeKey([16]byte{9})
	checks := 0
	keys := func() ([]uint64, error) {
		checks++
		if checks == 1 {
			return []uint64{stale, claimed}, nil
		}
		return []uint64{stale, claimed, newcomer}, nil
	}
	slots := func() ([]*node.Slot, error) {
		if checks == 1 {
			return nil, nil
		}
		return []*node.Slot{slotOf(node.NodeStateActive, 8)}, nil
	}
	got, err := staleKeyCheck{keys: keys, slots: slots, wait: func() bool { return true }}.run()
	if err != nil || !slices.Equal(got, []uint64{stale}) {
		t.Errorf("run = %x, %v; want only the key no slot owned in either check", got, err)
	}
	checks = 0
	if got, err := (staleKeyCheck{keys: keys, slots: slots, wait: func() bool { return false }}).run(); err != nil || len(got) != 0 {
		t.Errorf("run with the mount stopping before the second check = %x, %v; want none", got, err)
	}
}
