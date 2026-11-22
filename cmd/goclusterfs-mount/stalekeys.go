package main

import (
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fence"
	"github.com/supporttools/GoClusterFS/internal/node"
)

// staleKeyRecheck is longer than a joining node keeps its registered key without a slot, through its slot watches.
const staleKeyRecheck = time.Minute

// staleKeyCheck finds the reservation keys that no registry slot owned in two checks, the second after wait.
type staleKeyCheck struct {
	keys  func() ([]uint64, error)
	slots func() ([]*node.Slot, error)
	// wait returns false if the mount stops first.
	wait func() bool
}

func (c staleKeyCheck) run() ([]uint64, error) {
	first, err := c.unowned()
	if err != nil || len(first) == 0 || !c.wait() {
		return nil, err
	}
	second, err := c.unowned()
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(second, func(key uint64) bool { return !slices.Contains(first, key) }), nil
}

// unowned lists the keys before the slots, so a node that registers and claims its slot in between counts as owner.
func (c staleKeyCheck) unowned() ([]uint64, error) {
	keys, err := c.keys()
	if err != nil {
		return nil, err
	}
	slots, err := c.slots()
	if err != nil {
		return nil, err
	}
	return unownedKeys(keys, slots), nil
}

// unownedKeys returns, each once, the keys of no node in a slot: live, stalled, or dead and waiting for a replay.
func unownedKeys(keys []uint64, slots []*node.Slot) []uint64 {
	var unowned []uint64
	for _, key := range keys {
		owned := slices.ContainsFunc(slots, func(s *node.Slot) bool {
			return s != nil && s.State != node.NodeStateEmpty && fence.NodeKey(s.UUID) == key
		})
		if !owned && !slices.Contains(unowned, key) {
			unowned = append(unowned, key)
		}
	}
	return unowned
}

// warnStaleKeys runs the stale key check once after this node joined, records the count for /metrics and warns.
func warnStaleKeys(pr *fence.PR, reg *node.Registry, devicePath string, stop <-chan struct{}) {
	check := staleKeyCheck{keys: pr.Keys, slots: reg.Slots, wait: func() bool {
		select {
		case <-stop:
			return false
		case <-time.After(staleKeyRecheck):
			return true
		}
	}}
	stale, err := check.run()
	if err != nil {
		log.Printf("[INFO] Fencing: cannot list the device's reservation keys to look for stale ones: %v", err)
		return
	}
	reg.NoteStaleKeys(len(stale))
	if len(stale) == 0 {
		return
	}
	names := make([]string, len(stale))
	for i, key := range stale {
		names[i] = fmt.Sprintf("%#016x", key)
	}
	log.Printf("[WARN] Fencing: the device lists reservation keys that no registry slot owned for %v: %s. An unmount that "+
		"could not remove its key, or an older build, leaves such a key behind. On LIO, while its registration holds the "+
		"reservation, its host goes on writing after a peer revokes that host's later key. Once every node is stopped, "+
		"remove all keys with goclusterfs-fsck -clear-reservations -confirm %s. Keys that start with 0x4746 are format's "+
		"or fsck's, and one may belong to an fsck still running", staleKeyRecheck, strings.Join(names, ", "), devicePath)
}
