package node

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fence"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// rejoinWorld is a device whose registry slot 1 holds a mount of host vm2 from an earlier boot, which crashed after
// an iSCSI relogin: its key is under vm2's second session, and the orphan key of the mount before the relogin is
// under the first, as LIO keeps them. Writes are decided as LIO decides them, per initiator.
type rejoinWorld struct {
	target  *fence.Target
	dev     *storage.Device
	earlier *Slot
	key     uint64
}

const earlierSlot = 1

func newRejoinWorld(t *testing.T) *rejoinWorld {
	t.Helper()
	dev, cleanup := createTestDevice(t, 1<<20)
	t.Cleanup(cleanup)
	w := &rejoinWorld{target: fence.NewLIOTarget(), dev: dev}
	gone := NewRegistry(dev, 0)
	w.earlier = &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: gone.LocalUUID(), HeartbeatSeq: 7,
		HeartbeatTime: time.Now().Add(-2 * time.Second).Unix(), BootID: [16]byte{0xee}, JournalOffset: 5 << 20, JournalSize: 4 << 20}
	copy(w.earlier.Hostname[:], "vm2")
	gone.mu.Lock()
	err := gone.writeSlot(earlierSlot, w.earlier)
	gone.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	w.key = fence.NodeKey(gone.LocalUUID())
	for _, pr := range []*fence.PR{w.target.Node("vm2/2", w.key), w.target.Node("vm2/1", 0x47430000000000ff)} {
		if err := pr.Register(); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

// peer registers host vm1's node, which watches the earlier mount's slot silent and records when it revokes its key.
func (w *rejoinWorld) peer(t *testing.T) (reg *Registry, revoked func() (time.Time, []bool)) {
	t.Helper()
	reg = NewRegistry(w.dev, 0)
	if _, err := reg.Register(context.Background(), "vm1"); err != nil {
		t.Fatal(err)
	}
	pr := w.target.Node("vm1/1", fence.NodeKey(reg.LocalUUID()))
	if err := pr.RegisterFresh(); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var at time.Time
	var ownHost []bool
	reg.SetPreempter(func(owner [16]byte, own bool) error {
		mu.Lock()
		at, ownHost = time.Now(), append(ownHost, own)
		mu.Unlock()
		return pr.Preempt(fence.NodeKey(owner))
	})
	reg.SetRecoveryHandler(func(*FailedNodeInfo) error { return nil })
	silence(reg, earlierSlot, w.earlier, deadAfter+time.Second)
	return reg, func() (time.Time, []bool) {
		mu.Lock()
		defer mu.Unlock()
		return at, ownHost
	}
}

// rebooted is vm2's new mount on its first session, wired as goclusterfs-mount wires it: it registers its key when
// Register joins, and registers it anew after it revokes a key of its own host.
func (w *rejoinWorld) rebooted() (reg *Registry, pr *fence.PR, joined *time.Time, ownHost *[]bool) {
	reg = NewRegistry(w.dev, 0)
	pr = w.target.Node("vm2/1", fence.NodeKey(reg.LocalUUID()))
	joined, ownHost = new(time.Time), new([]bool)
	reg.SetJoin(func() error {
		*joined = time.Now()
		return pr.RegisterFresh()
	})
	reg.SetPreempter(func(owner [16]byte, own bool) error {
		*ownHost = append(*ownHost, own)
		if err := pr.Preempt(fence.NodeKey(owner)); err != nil || !own {
			return err
		}
		return pr.RegisterFresh()
	})
	return reg, pr, joined, ownHost
}

// The bug this guards against: vm2 registered its new key at once, and the peer's revoke of the earlier mount's key
// then stopped vm2's writes, though its key stayed registered.
func TestRegisteringBeforeThePeersRevokeLosesWritesOnLIO(t *testing.T) {
	w := newRejoinWorld(t)
	peer, _ := w.peer(t)
	_, pr, _, _ := w.rebooted()
	if err := pr.Register(); err != nil {
		t.Fatal(err)
	}
	if err := peer.RecoverFailedNode(earlierSlot); err != nil {
		t.Fatal(err)
	}
	if w.target.MayWrite("vm2/1") {
		t.Fatal("vm2 may still write: the LIO fake does not model the bug, so the tests below prove nothing")
	}
}

// vm2's mount watches its earlier mount's slot, which a peer takes over during the watch. vm2 registers its key only
// after the peer's revoke, so it may write.
func TestJoinRegistersAfterAPeerRevokesTheEarlierMount(t *testing.T) {
	shortenSilentSlotWatch(t)
	w := newRejoinWorld(t)
	peer, revoked := w.peer(t)
	reg, _, joined, _ := w.rebooted()
	recovered := make(chan error, 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		recovered <- peer.RecoverFailedNode(earlierSlot)
	}()
	if _, err := reg.Register(context.Background(), "vm2"); err != nil {
		t.Fatalf("vm2's Register: %v", err)
	}
	if err := <-recovered; err != nil {
		t.Fatalf("the peer's takeover: %v", err)
	}
	at, ownHost := revoked()
	if at.IsZero() || !joined.After(at) {
		t.Errorf("vm2 joined at %v, the peer revoked at %v; want the join after the revoke", joined.Format(time.StampMilli), at.Format(time.StampMilli))
	}
	if !slices.Equal(ownHost, []bool{false}) {
		t.Errorf("the peer's preempter saw ownHost %v, want false for another host's slot", ownHost)
	}
	if !w.target.MayWrite("vm2/1") {
		t.Error("vm2 may not write after it joined")
	}
}

// With no peer to take it over, vm2's mount watches its earlier mount's slot silent and takes it over itself. Its own
// revoke of the earlier key stops its writes on LIO, so it registers its key anew before the slot claim.
func TestJoinTakesOverTheEarlierMountWithoutPeers(t *testing.T) {
	shortenSilentSlotWatch(t)
	w := newRejoinWorld(t)
	reg, _, _, ownHost := w.rebooted()
	slot, err := reg.Register(context.Background(), "vm2")
	if err != nil || slot != earlierSlot {
		t.Fatalf("vm2's Register: slot %d, %v; want the earlier mount's slot %d", slot, err, earlierSlot)
	}
	if !slices.Equal(*ownHost, []bool{true}) || !slices.Equal(w.target.Preempted(), []uint64{w.key}) {
		t.Errorf("vm2 revoked %x seeing ownHost %v; want the earlier key, as its own host's", w.target.Preempted(), *ownHost)
	}
	if !w.target.MayWrite("vm2/1") {
		t.Error("vm2 may not write after it took over its earlier mount")
	}
}
