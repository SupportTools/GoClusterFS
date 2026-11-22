package fence

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// rekeyWorld is node n and peer p on one target. A writer keeps writing for n through the device's write hold, and
// counts every write the target took from n after p took n over.
type rekeyWorld struct {
	target     *Target
	node, peer *PR
	age        atomic.Int64
	intent     atomic.Bool
	dead       atomic.Bool
	takenOver  atomic.Bool
	exited     atomic.Bool
	hold       sync.RWMutex
	landed     atomic.Int32
	lateWrites atomic.Int32
	stop       chan struct{}
	writer     sync.WaitGroup
}

func newRekeyWorld(t *testing.T) *rekeyWorld {
	t.Helper()
	w := &rekeyWorld{target: NewTarget(), stop: make(chan struct{})}
	w.node = w.target.Node("n", NodeKey([16]byte{1}))
	w.peer = w.target.Node("p", NodeKey([16]byte{2}))
	for _, pr := range []*PR{w.node, w.peer} {
		if err := pr.Register(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.peer.Reserve(); err != nil {
		t.Fatal(err)
	}
	w.age.Store(int64(5 * time.Second))
	w.writer.Add(1)
	go func() {
		defer w.writer.Done()
		for {
			select {
			case <-w.stop:
				return
			default:
			}
			w.hold.RLock()
			if !w.exited.Load() && w.target.MayWrite("n") {
				w.landed.Add(1)
				if w.takenOver.Load() {
					w.lateWrites.Add(1)
				}
			}
			w.hold.RUnlock()
			time.Sleep(100 * time.Microsecond)
		}
	}()
	t.Cleanup(func() {
		close(w.stop)
		w.writer.Wait()
	})
	return w
}

// loseSession drops n's key from its current session, as an iSCSI logout and login does; LIO keeps it under the old one.
func (w *rekeyWorld) loseSession() {
	w.target.Relogin("n")
}

// takeOver is p proving n dead: n's heartbeat is 20s old by now, and p marks its intent in n's slot, preempts n's key
// and marks the slot Dead.
func (w *rekeyWorld) takeOver(t *testing.T) {
	w.age.Store(int64(20 * time.Second))
	w.revoke(t)
	w.dead.Store(true)
}

// revoke is the first two steps of a takeover: p marks its intent in n's slot, then preempts n's key. From the
// preempt on, no write of n's may land.
func (w *rekeyWorld) revoke(t *testing.T) {
	w.intent.Store(true)
	if err := w.peer.Preempt(w.node.Key()); err != nil {
		t.Errorf("peer's preempt: %v", err)
	}
	w.takenOver.Store(true)
}

// preemptOnly is a preempt of n's key without an intent in n's slot, as an older fsck or peer would send.
func (w *rekeyWorld) preemptOnly(t *testing.T) {
	if err := w.peer.Preempt(w.node.Key()); err != nil {
		t.Errorf("peer's preempt: %v", err)
	}
	w.takenOver.Store(true)
}

func (w *rekeyWorld) rekeyer() *Rekeyer {
	return &Rekeyer{
		PR: w.node,
		HoldWrites: func(fn func() error) error {
			w.hold.Lock()
			defer w.hold.Unlock()
			return fn()
		},
		HeartbeatAge: func() time.Duration { return time.Duration(w.age.Load()) },
		SlotLive: func() error {
			switch {
			case w.dead.Load():
				return errors.New("slot is Dead")
			case w.intent.Load():
				return errors.New("slot names a peer revoking this node")
			}
			return nil
		},
		StallLimit: 15 * time.Second,
	}
}

func (w *rekeyWorld) nodeRegistered() bool {
	keys, _ := w.target.Node("lister", 0).Keys()
	w.target.mu.Lock()
	defer w.target.mu.Unlock()
	_, ok := w.target.keys["n"]
	return ok && slices.Contains(keys, w.node.Key())
}

// A node whose session lost its key registers again and writes on.
func TestRekeyRestoresALostKey(t *testing.T) {
	w := newRekeyWorld(t)
	w.loseSession()
	k := w.rekeyer()
	var restored time.Duration
	k.Restored = func(age time.Duration) { restored = age }
	if err := k.Rekey(); err != nil {
		t.Fatalf("Rekey: %v", err)
	}
	if !w.nodeRegistered() || restored != 5*time.Second {
		t.Errorf("registered %v, Restored saw age %v; want registered at 5s", w.nodeRegistered(), restored)
	}
	before := w.landed.Load()
	time.Sleep(20 * time.Millisecond)
	if w.landed.Load() == before {
		t.Error("no write landed after the re-register")
	}
}

// A stall between the age check and the register lets the peer preempt and mark the slot Dead first. The register
// then succeeds at the device, but the checks after it fail, the key goes again, and no write lands in between.
func TestRekeyStalledBeforeRegisterLandsNoWrite(t *testing.T) {
	w := newRekeyWorld(t)
	w.loseSession()
	w.age.Store(int64(14 * time.Second))
	k := w.rekeyer()
	registeredInStall := false
	k.afterRegister = func() { registeredInStall = w.nodeRegistered() }
	k.beforeRegister = func() { w.takeOver(t) }
	err := k.Rekey()
	if err == nil || !strings.Contains(err.Error(), "past the 15s") {
		t.Errorf("Rekey: %v, want the second check to fail on the heartbeat age", err)
	}
	if !registeredInStall {
		t.Error("the register did not reach the device, so the test proves nothing")
	}
	time.Sleep(20 * time.Millisecond)
	if w.nodeRegistered() || w.lateWrites.Load() != 0 {
		t.Errorf("registered %v, %d writes landed after the takeover; want neither", w.nodeRegistered(), w.lateWrites.Load())
	}
}

// A stall between the register and the second check: the peer's preempt removes the new registration too, the checks
// fail, and no write lands in between.
func TestRekeyStalledAfterRegisterLandsNoWrite(t *testing.T) {
	w := newRekeyWorld(t)
	w.loseSession()
	w.age.Store(int64(14 * time.Second))
	k := w.rekeyer()
	k.afterRegister = func() { w.takeOver(t) }
	if err := k.Rekey(); err == nil {
		t.Error("Rekey passed after the peer took this node over")
	}
	time.Sleep(20 * time.Millisecond)
	if w.nodeRegistered() || w.lateWrites.Load() != 0 {
		t.Errorf("registered %v, %d writes landed after the takeover; want neither", w.nodeRegistered(), w.lateWrites.Load())
	}
}

// Rekey sends no register when the host still holds its key, when the slot is Dead, when the heartbeat is past the
// stall limit, or for a second loss inside RekeyWindow.
func TestRekeyRefusesWithoutRegistering(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(w *rekeyWorld, k *Rekeyer)
		want  string
	}{
		{"key still held", func(w *rekeyWorld, k *Rekeyer) {}, "still holds this node's key"},
		{"slot Dead", func(w *rekeyWorld, k *Rekeyer) { w.loseSession(); w.dead.Store(true) }, "slot is Dead"},
		{"heartbeat too old", func(w *rekeyWorld, k *Rekeyer) { w.loseSession(); w.age.Store(int64(16 * time.Second)) }, "past the 15s"},
		{"second loss", func(w *rekeyWorld, k *Rekeyer) { w.loseSession(); k.last = time.Now().Add(-time.Minute) }, "lost this node's key again"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newRekeyWorld(t)
			k := w.rekeyer()
			tc.setup(w, k)
			registers := 0
			k.beforeRegister = func() { registers++ }
			err := k.Rekey()
			if err == nil || !strings.Contains(err.Error(), tc.want) || registers != 0 {
				t.Errorf("Rekey: %v after %d registers; want an error naming %q and no register", err, registers, tc.want)
			}
		})
	}
}

// A hypervisor pause stops a KVM guest's CLOCK_BOOTTIME, so after one the heartbeat age reads about 1s whatever time
// passed, and only the slot can tell this node that a peer took it over. Each case pauses this node at another point
// of the peer's takeover; a writer keeps writing through the hold, and no write may land after the peer's preempt.
func TestRekeyAfterAPausedClockLandsNoWrite(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pause func(w *rekeyWorld, k *Rekeyer, t *testing.T)
		// registers says whether the node's register reaches the device after the peer's preempt.
		registers bool
	}{
		{"resumes between the preempt and the Dead mark", func(w *rekeyWorld, k *Rekeyer, t *testing.T) { w.revoke(t) }, false},
		{"paused before the register", func(w *rekeyWorld, k *Rekeyer, t *testing.T) {
			k.beforeRegister = func() { w.revoke(t) }
		}, true},
		{"paused between the register and the slot read", func(w *rekeyWorld, k *Rekeyer, t *testing.T) {
			k.afterRegister = func() { w.revoke(t) }
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newRekeyWorld(t)
			w.loseSession()
			w.age.Store(int64(time.Second))
			k := w.rekeyer()
			tc.pause(w, k, t)
			registeredAfterPreempt := false
			if k.afterRegister == nil {
				k.afterRegister = func() { registeredAfterPreempt = w.nodeRegistered() }
			}
			err := k.Rekey()
			w.dead.Store(true)
			if err == nil || !strings.Contains(err.Error(), "revoking") || !strings.Contains(err.Error(), "a peer preempted this node") {
				t.Errorf("Rekey: %v, want both the intent and the key listing to refuse", err)
			}
			if registeredAfterPreempt != tc.registers {
				t.Errorf("the register landed after the preempt: %v, want %v", registeredAfterPreempt, tc.registers)
			}
			time.Sleep(20 * time.Millisecond)
			if n := w.lateWrites.Load(); n != 0 || w.nodeRegistered() {
				t.Errorf("%d writes landed after the peer's preempt, registered %v", n, w.nodeRegistered())
			}
		})
	}
}

// A re-register that lands after the peer's preempt and then cannot be removed, as when the path fails again, leaves
// this host registered. Refused stops the node while writes are still held, so none lands through that registration.
func TestRekeyThatCannotUnregisterStopsWithWritesHeld(t *testing.T) {
	w := newRekeyWorld(t)
	w.loseSession()
	w.age.Store(int64(time.Second))
	k := w.rekeyer()
	k.beforeRegister = func() {
		w.revoke(t)
		w.target.mu.Lock()
		w.target.failUnregister = errors.New("path down")
		w.target.mu.Unlock()
	}
	heldAtRefusal := false
	k.Refused = func(error) {
		if heldAtRefusal = !w.hold.TryRLock(); !heldAtRefusal {
			w.hold.RUnlock()
		}
		w.exited.Store(true)
	}
	if err := k.Rekey(); err == nil || !strings.Contains(err.Error(), "path down") {
		t.Errorf("Rekey: %v, want the intent and the failed unregister", err)
	}
	time.Sleep(20 * time.Millisecond)
	if !w.nodeRegistered() {
		t.Fatal("the failed unregister left no registration, so the test proves nothing")
	}
	if !heldAtRefusal || w.lateWrites.Load() != 0 {
		t.Errorf("Refused ran with writes held: %v; %d writes landed after the preempt", heldAtRefusal, w.lateWrites.Load())
	}
}

// An older fsck or peer preempts without marking an intent, so the slot shows nothing; the key listing still refuses.
func TestRekeyRefusesAPreemptWithoutAnIntent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pause func(w *rekeyWorld, k *Rekeyer, t *testing.T)
	}{
		{"preempted before the rekey", func(w *rekeyWorld, k *Rekeyer, t *testing.T) { w.preemptOnly(t) }},
		{"preempted between the listing and the register", func(w *rekeyWorld, k *Rekeyer, t *testing.T) {
			k.beforeRegister = func() { w.preemptOnly(t) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newRekeyWorld(t)
			w.loseSession()
			w.age.Store(int64(time.Second))
			k := w.rekeyer()
			tc.pause(w, k, t)
			err := k.Rekey()
			w.dead.Store(true)
			if err == nil || !strings.Contains(err.Error(), "a peer preempted this node") {
				t.Errorf("Rekey: %v, want the key listing to refuse", err)
			}
			time.Sleep(20 * time.Millisecond)
			if n := w.lateWrites.Load(); n != 0 || w.nodeRegistered() {
				t.Errorf("%d writes landed after the preempt, registered %v", n, w.nodeRegistered())
			}
		})
	}
}

// A peer that marked its intent but has not preempted yet leaves every registration in place; the slot refuses.
func TestRekeyRefusesAnIntentBeforeThePreempt(t *testing.T) {
	w := newRekeyWorld(t)
	w.loseSession()
	w.age.Store(int64(time.Second))
	w.intent.Store(true)
	k := w.rekeyer()
	registers := 0
	k.beforeRegister = func() { registers++ }
	err := k.Rekey()
	if err == nil || !strings.Contains(err.Error(), "revoking") || strings.Contains(err.Error(), "preempted") || registers != 0 {
		t.Errorf("Rekey: %v after %d registers; want only the slot to refuse, before any register", err, registers)
	}
}

// Without a key listing nothing shows whether a peer preempted this node, so Rekey registers nothing.
func TestRekeyWithoutAKeyListingRegistersNothing(t *testing.T) {
	w := newRekeyWorld(t)
	w.loseSession()
	w.target.Unlistable = true
	k := w.rekeyer()
	registers := 0
	k.beforeRegister = func() { registers++ }
	if err := k.Rekey(); !errors.Is(err, ErrUnsupported) || registers != 0 {
		t.Errorf("Rekey: %v after %d registers; want ErrUnsupported and no register", err, registers)
	}
}

// On a two-path dm-multipath map whose second path lost its session, Rekey re-registers that path only: the first
// path keeps its registration throughout, where a probe through the map would make dm remove it. A preempt before
// or during the rekey is refused, and neither path may write.
func TestRekeyOnTwoPaths(t *testing.T) {
	for _, tc := range []struct {
		name            string
		preempt, during bool
	}{
		{name: "second path lost its session"},
		{name: "preempted before", preempt: true},
		{name: "preempted between the listing and the register", during: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := NewTarget()
			node := target.MultipathNode("n", 2, NodeKey([16]byte{1}))
			peer := target.Node("p", NodeKey([16]byte{2}))
			for _, pr := range []*PR{node, peer} {
				if err := pr.Register(); err != nil {
					t.Fatal(err)
				}
			}
			if err := peer.Reserve(); err != nil {
				t.Fatal(err)
			}
			target.Relogin("n#1")
			if tc.preempt {
				if err := peer.Preempt(node.Key()); err != nil {
					t.Fatal(err)
				}
			}
			firstPathKept := !tc.preempt
			k := &Rekeyer{
				PR:           node,
				HoldWrites:   func(fn func() error) error { return fn() },
				HeartbeatAge: func() time.Duration { return time.Second },
				SlotLive:     func() error { return nil },
				StallLimit:   15 * time.Second,
			}
			k.beforeRegister = func() {
				if !tc.preempt {
					firstPathKept = target.MayWrite("n#0")
				}
				if tc.during {
					if err := peer.Preempt(node.Key()); err != nil {
						t.Errorf("peer's preempt: %v", err)
					}
				}
			}
			err := k.Rekey()
			if tc.preempt || tc.during {
				if err == nil || !strings.Contains(err.Error(), "a peer preempted this node") || target.MayWrite("n#0") || target.MayWrite("n#1") {
					t.Errorf("Rekey: %v, paths write: %v %v; want the key listing to refuse and neither path writing",
						err, target.MayWrite("n#0"), target.MayWrite("n#1"))
				}
				return
			}
			n, _ := node.Registrations()
			if err != nil || !firstPathKept || !target.MayWrite("n#0") || !target.MayWrite("n#1") || n != 3 {
				t.Errorf("Rekey: %v; first path kept its key during the rekey: %v; paths write: %v %v; %d registrations, want 3",
					err, firstPathKept, target.MayWrite("n#0"), target.MayWrite("n#1"), n)
			}
		})
	}
}

// A peer whose own key is gone cannot preempt: the device refuses it and still lists the victim's key, so the error
// says the preempt did not take effect, and that the peer is fenced.
func TestPreemptThatDidNotTakeEffectSaysSo(t *testing.T) {
	target := NewTarget()
	victim := target.Node("v", NodeKey([16]byte{1}))
	peer := target.Node("p", NodeKey([16]byte{2}))
	third := target.Node("t", NodeKey([16]byte{3}))
	for _, pr := range []*PR{victim, peer, third} {
		if err := pr.Register(); err != nil {
			t.Fatal(err)
		}
	}
	if err := third.Preempt(peer.Key()); err != nil {
		t.Fatal(err)
	}
	err := peer.Preempt(victim.Key())
	if !errors.Is(err, ErrNotPreempted) || !errors.Is(err, ErrFenced) {
		t.Errorf("Preempt by a fenced peer = %v, want ErrNotPreempted and ErrFenced", err)
	}
	if err := third.Preempt(victim.Key()); err != nil || errors.Is(err, ErrNotPreempted) {
		t.Errorf("a preempt that took effect = %v, want nil", err)
	}
}

// NVMe registers a host, not a path, so a reconnect loses no key and leaves no registration from before behind. A
// rekey there refuses either way: every path still holds the key, or a preempt removed it.
func TestRekeyOnNVMeRefuses(t *testing.T) {
	for _, preempted := range []bool{false, true} {
		t.Run(fmt.Sprintf("preempted %v", preempted), func(t *testing.T) {
			target := NewTarget()
			node := target.NVMeNode("host", 2, NodeKey([16]byte{1}))
			peer := target.Node("p", NodeKey([16]byte{2}))
			for _, pr := range []*PR{node, peer} {
				if err := pr.Register(); err != nil {
					t.Fatal(err)
				}
			}
			if err := peer.Reserve(); err != nil {
				t.Fatal(err)
			}
			want := "still holds this node's key"
			if preempted {
				if err := peer.Preempt(node.Key()); err != nil {
					t.Fatal(err)
				}
				want = "a peer preempted this node"
			}
			registers := 0
			k := &Rekeyer{
				PR:             node,
				HoldWrites:     func(fn func() error) error { return fn() },
				HeartbeatAge:   func() time.Duration { return time.Second },
				SlotLive:       func() error { return nil },
				StallLimit:     15 * time.Second,
				beforeRegister: func() { registers++ },
			}
			if err := k.Rekey(); err == nil || !strings.Contains(err.Error(), want) || registers != 0 {
				t.Errorf("Rekey: %v after %d registers; want an error naming %q and no register", err, registers, want)
			}
			if target.MayWrite("host") == preempted {
				t.Errorf("host may write: %v, want %v", target.MayWrite("host"), !preempted)
			}
		})
	}
}
