package goclusterfs

import (
	"context"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
)

// mount opens path as a node of c that holds its registry slot, so the mount's snapshot recovery sees c.
func (c *fakeCluster) mount(t *testing.T, path string) *FS {
	t.Helper()
	fsys := openFormattedComponents(t, path)
	slot := int(fsys.nodeUUID[15])
	c.occupy(slot, fsys.nodeUUID, node.OccupiedLive)
	c.join(t, fsys, slot)
	startSnapshots(t, fsys)
	return fsys
}

// pausedSnapshot starts a snapshot of f and returns once it waits to become active, with its descriptor.
// resume lets it finish and returns its errno.
func pausedSnapshot(t *testing.T, f *File, name string) (*snapshot.Descriptor, func() syscall.Errno) {
	t.Helper()
	paused, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	beforeActivate = func() {
		close(paused)
		<-release
	}
	done := make(chan syscall.Errno, 1)
	go func() { done <- f.createSnapshot(context.Background(), name) }()
	resume := func() syscall.Errno {
		once.Do(func() { close(release) })
		return <-done
	}
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		beforeActivate = nil
	})

	select {
	case <-paused:
	case errno := <-done:
		t.Fatalf("snapshot %s finished with %v before it paused", name, errno)
	case <-time.After(10 * time.Second):
		t.Fatalf("snapshot %s never reached activation", name)
	}
	desc, err := f.fs.snapshotReg.GetByName(context.Background(), name)
	if err != nil || desc.State != snapshot.StateCreating {
		t.Fatalf("paused snapshot %s: %+v, %v; want creating", name, desc, err)
	}
	return desc, resume
}

func snapshotState(t *testing.T, fsys *FS, name string) snapshot.State {
	t.Helper()
	desc, err := fsys.snapshotReg.GetByName(context.Background(), name)
	if err != nil {
		t.Fatalf("snapshot %s: %v", name, err)
	}
	return desc.State
}

// Node A pauses between adding a snapshot to the registry and activating it, and node B mounts meanwhile.
func TestMountLeavesSnapshotALiveNodeIsCreating(t *testing.T) {
	other := [16]byte{0xEE}
	for _, tc := range []struct {
		name     string
		owner    func(a [16]byte) [16]byte
		state    node.Occupancy
		rollBack bool
	}{
		{"creator live", func(a [16]byte) [16]byte { return a }, node.OccupiedLive, false},
		{"creator failing", func(a [16]byte) [16]byte { return a }, node.OccupiedFailing, false},
		{"creator dead, replay pending", func(a [16]byte) [16]byte { return a }, node.OccupiedReplaying, false},
		{"slot empty", func([16]byte) [16]byte { return [16]byte{} }, node.Unoccupied, true},
		{"slot held by another node", func([16]byte) [16]byte { return other }, node.OccupiedLive, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := newFormattedImage(t)
			cluster := newFakeCluster()
			nodeA := cluster.mount(t, path)
			f := createFile(t, nodeA, "f")
			writeAt(t, f, pattern(1, 2*BlockSize), 0)
			desc, resume := pausedSnapshot(t, f, "s")
			if !desc.Creator.Is(nodeA.nodeUUID) || int(desc.Creator.Slot) != int(nodeA.nodeUUID[15]) {
				t.Fatalf("creator of s: %+v, want node A's slot and UUID", desc.Creator)
			}

			cluster.occupy(int(desc.Creator.Slot), tc.owner(nodeA.nodeUUID), tc.state)
			nodeB := cluster.mount(t, path)

			if !tc.rollBack {
				if got := snapshotState(t, nodeB, "s"); got != snapshot.StateCreating {
					t.Fatalf("after node B's mount, s is %v; want node A's snapshot left creating", got)
				}
				if errno := resume(); errno != 0 {
					t.Fatalf("node A's snapshot: %v, want success", errno)
				}
				if got := snapshotState(t, nodeB, "s"); got != snapshot.StateActive {
					t.Fatalf("s is %v after node A finished it, want active", got)
				}
				expectNoProblems(t, nodeB, "after the snapshot")
				return
			}

			if errno := resume(); errno != syscall.EAGAIN {
				t.Fatalf("node A's snapshot after node B rolled it back: %v, want EAGAIN", errno)
			}
			waitSnapshotGone(t, nodeB, "s")
			waitInodeFree(t, nodeB, desc.DataInode)
			expectNoProblems(t, nodeB, "after the rollback")
		})
	}
}

// Node A dies between adding a snapshot to the registry and activating it. Node B replays A's journal, then rolls
// the snapshot back, as goclusterfs-mount's recovery handler does.
func TestDeadNodesSnapshotIsRolledBackAfterReplay(t *testing.T) {
	path := newFormattedImage(t)
	cluster := newFakeCluster()
	nodeA := cluster.mount(t, path)
	nodeB := cluster.mount(t, path)
	ctx := context.Background()
	f := createFile(t, nodeA, "f")
	writeAt(t, f, pattern(1, 2*BlockSize), 0)
	desc, resume := pausedSnapshot(t, f, "s")
	slotA := int(desc.Creator.Slot)
	cluster.occupy(slotA, nodeA.nodeUUID, node.OccupiedReplaying)

	nodeB.RollBackSnapshotsOf(ctx, slotA, [16]byte{0xEE})
	if got := snapshotState(t, nodeB, "s"); got != snapshot.StateCreating {
		t.Fatalf("after rolling back another owner of slot %d, s is %v; want creating", slotA, got)
	}

	nodeB.RollBackSnapshotsOf(ctx, slotA, nodeA.nodeUUID)
	waitSnapshotGone(t, nodeB, "s")
	waitInodeFree(t, nodeB, desc.DataInode)

	// Node A was only paused: its activation fails, and it leaves the delete to node B.
	if errno := resume(); errno != syscall.EAGAIN {
		t.Fatalf("node A's snapshot after its rollback: %v, want EAGAIN", errno)
	}
	expectNoProblems(t, nodeB, "after the rollback")
}
