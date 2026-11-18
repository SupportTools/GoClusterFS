package main

import (
	"fmt"
	"os"
	"syscall"

	"github.com/supporttools/GoClusterFS/internal/fence"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// fsckFencing gives fsck a reservation key when it writes a fenced volume. A node mounted on this host already
// registered the host, whose one registration fsck then shares through that node's key; otherwise fsck registers a
// key of its own, which release removes.
func fsckFencing(dev *storage.Device, writing bool) (preempt func(owner [16]byte) error, release func(), err error) {
	release = func() {}
	if !writing {
		return nil, release, nil
	}
	sb, err := superblock.Read(dev)
	if err != nil || sb.RequiredFlags&superblock.RequiredReservations == 0 {
		return nil, release, nil
	}
	var pr *fence.PR
	if lockErr := syscall.Flock(int(dev.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); lockErr == nil {
		pr = fence.Block(dev.Fd(), fence.ToolKey())
		if err := pr.Register(); err != nil {
			return nil, release, fmt.Errorf("the volume is fenced, and registering a reservation key failed: %w", err)
		}
		release = func() {
			if err := pr.Unregister(); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: removing fsck's reservation key %#x failed: %v\n", pr.Key(), err)
			}
		}
		if err := pr.Reserve(); err != nil {
			release()
			return nil, func() {}, fmt.Errorf("the volume is fenced, and reserving the device failed: %w", err)
		}
	} else {
		key, ok := mountedHere(dev, sb)
		if !ok {
			return nil, release, fmt.Errorf("another process on this host holds the device for fencing (%v), but no node of this host is mounted; stop that process first", lockErr)
		}
		pr = fence.Block(dev.Fd(), key)
	}
	return func(owner [16]byte) error { return pr.Preempt(fence.NodeKey(owner)) }, release, nil
}

// mountedHere returns the key of the node mounted on this host, which holds this host's registration.
func mountedHere(dev *storage.Device, sb *superblock.Superblock) (uint64, bool) {
	slots, err := node.NewRegistry(dev, sb.NodeRegistryOffset).Slots()
	if err != nil {
		return 0, false
	}
	host, _ := os.Hostname()
	boot := node.LocalBootID()
	for _, slot := range slots {
		if slot != nil && slot.State == node.NodeStateActive && slot.BootID == boot && slot.Host() == host {
			return fence.NodeKey(slot.UUID), true
		}
	}
	return 0, false
}
