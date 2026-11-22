package node

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"syscall"
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
		HeartbeatTime: time.Now().Add(-30 * time.Second).Unix(), BootID: [16]byte{0xee}, JournalOffset: 5 << 20, JournalSize: 4 << 20}
	copy(w.earlier.Hostname[:], "vm2")
	w.key = fence.NodeKey(gone.LocalUUID())
	for _, pr := range []*fence.PR{w.target.Node("vm2/2", w.key), w.target.Node("vm2/1", 0x47430000000000ff)} {
		if err := pr.Register(); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

func (w *rejoinWorld) writeEarlier(t *testing.T) {
	t.Helper()
	reg := NewRegistry(w.dev, 0)
	reg.mu.Lock()
	err := reg.writeSlot(earlierSlot, w.earlier)
	reg.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

// peer registers host vm1's node and reserves the device; it watches the earlier mount's slot silent and records
// when it revokes its key.
func (w *rejoinWorld) peer(t *testing.T) (reg *Registry, revoked func() time.Time) {
	t.Helper()
	reg = NewRegistry(w.dev, 0)
	if _, err := reg.Register(context.Background(), "vm1"); err != nil {
		t.Fatal(err)
	}
	pr := w.target.Node("vm1/1", fence.NodeKey(reg.LocalUUID()))
	if err := pr.Register(); err != nil {
		t.Fatal(err)
	}
	if err := pr.Reserve(); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var at time.Time
	reg.SetPreempter(func(owner [16]byte) error {
		mu.Lock()
		at = time.Now()
		mu.Unlock()
		return pr.Preempt(fence.NodeKey(owner))
	})
	reg.SetRecoveryHandler(func(*FailedNodeInfo) error { return nil })
	silence(reg, earlierSlot, w.earlier, deadAfter+time.Second)
	return reg, func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return at
	}
}

// rebooted is vm2's new mount on its first session, wired as goclusterfs-mount wires it. Its own device refuses its
// writes whenever the target refuses vm2's first session.
func (w *rejoinWorld) rebooted(t *testing.T) (reg *Registry, joined *time.Time, anew *bool, rejoins *int) {
	t.Helper()
	dev, err := storage.Open(w.dev.Path())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	dev.SetWriteFault(func(int64) error {
		if !w.target.MayWrite("vm2/1") {
			return syscall.EBADE
		}
		return nil
	})
	reg = NewRegistry(dev, 0)
	pr := w.target.Node("vm2/1", fence.NodeKey(reg.LocalUUID()))
	joined, anew, rejoins = new(time.Time), new(bool), new(int)
	reg.SetJoin(func() error {
		*joined = time.Now()
		var err error
		*anew, err = pr.Join()
		return err
	}, func() error {
		*rejoins++
		return pr.RegisterFresh()
	})
	reg.SetPreempter(func(owner [16]byte) error { return pr.Preempt(fence.NodeKey(owner)) })
	return reg, joined, anew, rejoins
}

// The bug this guards against: vm2 registered its new key at once, and the peer's revoke of the earlier mount's key
// then stopped vm2's writes, though its key stayed registered.
func TestRegisteringBeforeThePeersRevokeLosesWritesOnLIO(t *testing.T) {
	w := newRejoinWorld(t)
	w.writeEarlier(t)
	peer, _ := w.peer(t)
	if err := w.target.Node("vm2/1", 0x4743000000000abc).Register(); err != nil {
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
// after the peer's revoke. Its register changed the orphan's key in place, so the device refuses its reserve; it
// registers anew, which keeps the peer's reservation.
func TestJoinRegistersAfterAPeerRevokesTheEarlierMount(t *testing.T) {
	shortenSilentSlotWatch(t)
	w := newRejoinWorld(t)
	w.writeEarlier(t)
	peer, revoked := w.peer(t)
	reg, joined, anew, rejoins := w.rebooted(t)
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
	if at := revoked(); at.IsZero() || !joined.After(at) {
		t.Errorf("vm2 joined at %v, the peer revoked at %v; want the join after the revoke", joined.Format(time.StampMilli), at.Format(time.StampMilli))
	}
	if !*anew || *rejoins != 0 || !w.target.MayWrite("vm2/1") || w.target.MayWrite("vm9/1") {
		t.Errorf("vm2's join registered anew: %v, its claim %d times; may write: %v, an unregistered host may write: %v; want the join to register anew under the peer's reservation",
			*anew, *rejoins, w.target.MayWrite("vm2/1"), w.target.MayWrite("vm9/1"))
	}
}

// With no peer to take it over, vm2's mount watches its earlier mount's slot silent and takes it over itself. vm2's
// first session reserved before the relogin, so its new key there holds the reservation, and vm2 writes as the holder
// after revoking the earlier mount's key, without registering anew.
func TestJoinTakesOverTheEarlierMountWithoutPeers(t *testing.T) {
	shortenSilentSlotWatch(t)
	w := newRejoinWorld(t)
	w.writeEarlier(t)
	if err := w.target.Node("vm2/1", 0x47430000000000ff).Reserve(); err != nil {
		t.Fatal(err)
	}
	reg, _, _, rejoins := w.rebooted(t)
	slot, err := reg.Register(context.Background(), "vm2")
	if err != nil || slot != earlierSlot {
		t.Fatalf("vm2's Register: slot %d, %v; want the earlier mount's slot %d", slot, err, earlierSlot)
	}
	if !slices.Equal(w.target.Preempted(), []uint64{w.key}) || *rejoins != 0 || !w.target.MayWrite("vm2/1") {
		t.Errorf("vm2 revoked %x, registered anew %d times, may write: %v; want the earlier key revoked and vm2 writing as holder",
			w.target.Preempted(), *rejoins, w.target.MayWrite("vm2/1"))
	}
}

// After a relogin, vm2's mount is killed and mounts again on the same boot: it registers on its second session, then
// takes its own crashed slot back and revokes the crashed mount's key on its first session. On LIO that stops vm2's
// writes while a peer holds the reservation, so the device refuses its slot claim, and vm2 registers anew.
func TestJoinRegistersAnewAfterRevokingItsOwnOtherSession(t *testing.T) {
	w := newRejoinWorld(t)
	w.earlier.BootID, w.earlier.PIDNamespace, w.earlier.PID = readBootID(), readPIDNamespace(), 0x3ffffff0
	w.writeEarlier(t)
	w.peer(t)
	reg, _, _, rejoins := w.rebooted(t)
	slot, err := reg.Register(context.Background(), "vm2")
	if err != nil || slot != earlierSlot {
		t.Fatalf("vm2's Register: slot %d, %v; want its own crashed slot %d", slot, err, earlierSlot)
	}
	if *rejoins != 1 || !w.target.MayWrite("vm2/1") || w.target.MayWrite("vm9/1") {
		t.Errorf("vm2 registered anew %d times, may write: %v, an unregistered host may write: %v; want one rejoin under the peer's reservation",
			*rejoins, w.target.MayWrite("vm2/1"), w.target.MayWrite("vm9/1"))
	}
}

// Two live nodes on different hosts that share a hostname: the second joins at once, since a slot under its hostname
// whose heartbeat is recent is never taken for its own earlier mount, and it warns about the shared name.
func TestLiveNodesSharingAHostnameJoinWithoutWaiting(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	t.Cleanup(cleanup)
	first := NewRegistry(dev, 0)
	if _, err := first.Register(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	log.SetOutput(io.MultiWriter(os.Stderr, &logged))
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	second := NewRegistry(dev, 0)
	second.bootID = [16]byte{0xbb}
	start := time.Now()
	slot, err := second.Register(context.Background(), "node")
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second || slot == first.localSlot {
		t.Errorf("the second node joined slot %d after %v; want another slot than the first node's %d, at once", slot, took.Round(time.Millisecond), first.localSlot)
	}
	if !strings.Contains(logged.String(), "is live under this host's name") {
		t.Error("the second node did not warn that a live node shares its hostname")
	}
}

// Probe 4: after this node joined, a peer revokes a key this host registered from another session, which on LIO
// clears the host's write permission. The node's revoke of the stalled owner P of the slot it claims is then refused,
// and Preempt reads that as P being gone. The claim is refused too; the node registers anew, and must revoke P again
// before it claims, or P's writes go on landing while the node replays P.
func TestRejoinRevokesTheOwnerAgain(t *testing.T) {
	shortenSilentSlotWatch(t)
	dev, cleanup := createTestDevice(t, 1<<20)
	t.Cleanup(cleanup)
	target := fence.NewLIOTarget()
	q := target.Node("vm1/1", 0x4743000000000001)
	other := target.Node("vm2/2", 0x4743000000000002)
	p := target.Node("vm3/1", 0x4743000000000003)
	for _, pr := range []*fence.PR{q, other, p} {
		if err := pr.Register(); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Reserve(); err != nil {
		t.Fatal(err)
	}
	stalled := NewRegistry(dev, 0)
	slot := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: stalled.LocalUUID(), HeartbeatSeq: 3,
		HeartbeatTime: time.Now().Add(-30 * time.Second).Unix(), JournalOffset: 5 << 20, JournalSize: 4 << 20}
	copy(slot.Hostname[:], "vm3")
	stalled.mu.Lock()
	err := stalled.writeSlot(1, slot)
	stalled.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	p = target.Node("vm3/1", fence.NodeKey(stalled.LocalUUID()))
	if err := p.Register(); err != nil {
		t.Fatal(err)
	}

	nDev, err := storage.Open(dev.Path())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nDev.Close() })
	nDev.SetWriteFault(func(int64) error {
		if !target.MayWrite("vm2/1") {
			return syscall.EBADE
		}
		return nil
	})
	reg := NewRegistry(nDev, 0)
	pr := target.Node("vm2/1", fence.NodeKey(reg.LocalUUID()))
	reg.SetJoin(func() error {
		if _, err := pr.Join(); err != nil {
			return err
		}
		return q.Preempt(other.Key())
	}, pr.RegisterFresh)
	reg.SetPreempter(func(owner [16]byte) error { return pr.Preempt(fence.NodeKey(owner)) })
	reg.PreferSlot(1)
	got, err := reg.Register(context.Background(), "vm2")
	if err != nil || got != 1 {
		t.Fatalf("Register: slot %d, %v; want the stalled node's slot 1", got, err)
	}
	keys, _ := q.Keys()
	if target.MayWrite("vm3/1") || slices.Contains(keys, p.Key()) {
		t.Errorf("the stalled node may write: %v, its key registered: %v; want it revoked before the claim", target.MayWrite("vm3/1"), slices.Contains(keys, p.Key()))
	}
}
