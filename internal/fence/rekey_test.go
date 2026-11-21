package fence

import (
	"errors"
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
	dead       atomic.Bool
	takenOver  atomic.Bool
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
			if w.target.MayWrite("n") {
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
	w.target.mu.Lock()
	defer w.target.mu.Unlock()
	w.target.keys["n-old"] = w.target.keys["n"]
	delete(w.target.keys, "n")
}

// takeOver is p proving n dead: n's heartbeat is 20s old by now, and p preempts n's key and marks n's slot Dead.
func (w *rekeyWorld) takeOver(t *testing.T) {
	w.age.Store(int64(20 * time.Second))
	if err := w.peer.Preempt(w.node.Key()); err != nil {
		t.Errorf("peer's preempt: %v", err)
	}
	w.dead.Store(true)
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
			if w.dead.Load() {
				return errors.New("slot is Dead")
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
		{"key still held", func(w *rekeyWorld, k *Rekeyer) {}, "still holds its key"},
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
