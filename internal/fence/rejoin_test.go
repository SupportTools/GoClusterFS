package fence

import (
	"errors"
	"testing"
)

// After a relogin a host holds keys under two sessions: an orphan from before it, whose unregister failed, and its
// mount's after it. The host crashes and reboots onto its first session, whose REGISTER changes the orphan's key in
// place. A peer then revokes the crashed mount's key, which on LIO stops writes from every session of the host, though
// the new key stays registered. RegisterFresh registers the key as a new registration, which lets the host write,
// and keeps the peer's reservation.
func TestRegisterFreshRestoresWritesAfterAnotherSessionsKeyIsRevoked(t *testing.T) {
	target := NewLIOTarget()
	peer := target.Node("vm1/1", 0x4743000000000001)
	orphan := target.Node("vm2/1", 0x4743000000000002)
	crashed := target.Node("vm2/2", 0x4743000000000003)
	for _, pr := range []*PR{peer, orphan, crashed} {
		if err := pr.Register(); err != nil {
			t.Fatal(err)
		}
	}
	if err := peer.Reserve(); err != nil {
		t.Fatal(err)
	}
	rebooted := target.Node("vm2/1", 0x4743000000000004)
	if err := rebooted.Register(); err != nil {
		t.Fatal(err)
	}
	if err := peer.Preempt(crashed.Key()); err != nil {
		t.Fatal(err)
	}
	if target.MayWrite("vm2/1") {
		t.Fatal("the LIO fake let the rebooted host write after the revoke, so the test proves nothing")
	}
	if err := rebooted.RegisterFresh(); err != nil {
		t.Fatalf("RegisterFresh: %v", err)
	}
	if !target.MayWrite("vm2/1") || target.MayWrite("vm9/1") || target.holder != "vm1/1" {
		t.Errorf("after RegisterFresh the host may write: %v, an unregistered host may: %v, holder %q; want the host writing under the peer's reservation",
			target.MayWrite("vm2/1"), target.MayWrite("vm9/1"), target.holder)
	}
}

// A host that preempts its own key under another session, after it registered on this one, keeps writing when it holds
// the reservation: LIO does not check the holder's own session against the initiator's flag.
func TestTheHolderWritesAfterRevokingItsOwnOtherSession(t *testing.T) {
	target := NewLIOTarget()
	old := target.Node("vm2/1", 0x4743000000000001)
	if err := old.Register(); err != nil {
		t.Fatal(err)
	}
	if err := old.Reserve(); err != nil {
		t.Fatal(err)
	}
	current := target.Node("vm2/2", 0x4743000000000002)
	if err := current.Register(); err != nil {
		t.Fatal(err)
	}
	if err := current.Preempt(old.Key()); err != nil {
		t.Fatal(err)
	}
	if !target.MayWrite("vm2/2") || target.holder != "vm2/2" {
		t.Errorf("after revoking the holder's key the session may write: %v, holder %q; want it holding the reservation", target.MayWrite("vm2/2"), target.holder)
	}
}

// LIO refuses a reserve, as it refuses a write, from a host whose write permission a revoke of its key under another
// session cleared. Join then registers anew once, which lets the host reserve and write under the peer's reservation.
func TestJoinRegistersAnewWhenLIORefusesTheReserve(t *testing.T) {
	target := NewLIOTarget()
	peer := target.Node("vm1/1", 0x4743000000000001)
	orphan := target.Node("vm2/1", 0x4743000000000002)
	crashed := target.Node("vm2/2", 0x4743000000000003)
	for _, pr := range []*PR{peer, orphan, crashed} {
		if err := pr.Register(); err != nil {
			t.Fatal(err)
		}
	}
	if err := peer.Reserve(); err != nil {
		t.Fatal(err)
	}
	if err := peer.Preempt(crashed.Key()); err != nil {
		t.Fatal(err)
	}
	rebooted := target.Node("vm2/1", 0x4743000000000004)
	if err := rebooted.Register(); err != nil {
		t.Fatal(err)
	}
	if err := rebooted.Reserve(); err == nil {
		t.Fatal("the LIO fake let the host reserve after the revoke, so the test proves nothing")
	}
	anew, err := rebooted.Join()
	if err != nil || !anew || !target.MayWrite("vm2/1") || target.holder != "vm1/1" {
		t.Errorf("Join: registered anew %v, %v; the host may write: %v, holder %q; want it registered anew under the peer's reservation",
			anew, err, target.MayWrite("vm2/1"), target.holder)
	}
}

// twoPaths is a dm-multipath map over two sessions of one initiator into one LIO target portal group, routed as dm
// routes reservation commands: registers to every path, reserves and preempts to the first path that takes them. It
// calls after once each path has answered.
type twoPaths struct {
	paths []*fakeDevice
	after func()
}

func (m twoPaths) register(old, new uint64, ignore bool) error {
	for _, p := range m.paths {
		err := p.register(old, new, ignore)
		m.after()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m twoPaths) first(f func(*fakeDevice) error) error {
	var err error
	for _, p := range m.paths {
		err = f(p)
		m.after()
		if err == nil {
			return nil
		}
	}
	return err
}

func (m twoPaths) reserve(key uint64) error {
	return m.first(func(p *fakeDevice) error { return p.reserve(key) })
}

func (m twoPaths) preemptAbort(own, victim uint64) error {
	return m.first(func(p *fakeDevice) error { return p.preemptAbort(own, victim) })
}

func (m twoPaths) clear(key uint64) error                   { return m.paths[0].clear(key) }
func (m twoPaths) readKeys() ([]uint64, error)              { return m.paths[0].readKeys() }
func (m twoPaths) readReservation() (uint64, uint32, error) { return m.paths[0].readReservation() }

// takeOverOverTwoPaths has node H, on two paths, preempt the stalled holder P, which makes H's first path the holder,
// so LIO refuses H's writes on its second path. It returns H and whether P could write after any command since.
func takeOverOverTwoPaths(t *testing.T, target *Target) (h *PR, pWrote func() bool) {
	t.Helper()
	stalled := target.Node("vm1/1", 0x4743000000000001)
	if err := stalled.Register(); err != nil {
		t.Fatal(err)
	}
	if err := stalled.Reserve(); err != nil {
		t.Fatal(err)
	}
	wrote := false
	paths := twoPaths{paths: []*fakeDevice{{target, "vm2/a"}, {target, "vm2/b"}}, after: func() {}}
	h = &PR{dev: paths, key: 0x4743000000000002}
	if _, err := h.Join(); err != nil {
		t.Fatal(err)
	}
	if err := h.Preempt(stalled.Key()); err != nil {
		t.Fatal(err)
	}
	if target.MayWrite("vm2/b") {
		t.Fatal("the fake took H's write on its second path after the preempt, so the test proves nothing")
	}
	paths.after = func() { wrote = wrote || target.MayWrite("vm1/1") }
	h.dev = paths
	return h, func() bool { return wrote }
}

// The review's case: with no other node registered, dropping both paths' registrations would release the
// reservation and let the preempted node write. RegisterFresh refuses instead, and the node never writes.
func TestRegisterFreshKeepsTheReservationOverTwoPaths(t *testing.T) {
	target := NewLIOTarget()
	h, pWrote := takeOverOverTwoPaths(t, target)
	if err := h.RegisterFresh(); !errors.Is(err, ErrNoOtherRegistrant) {
		t.Errorf("RegisterFresh: %v, want ErrNoOtherRegistrant", err)
	}
	if pWrote() || target.MayWrite("vm1/1") {
		t.Error("the preempted node could write")
	}
}

// With another node registered, RegisterFresh drops both paths' registrations: LIO hands the reservation to that
// node, so the preempted node never writes, and both of H's paths write afterwards.
func TestRegisterFreshOverTwoPathsWithAnotherNodeRegistered(t *testing.T) {
	target := NewLIOTarget()
	if err := target.Node("vm3/1", 0x4743000000000003).Register(); err != nil {
		t.Fatal(err)
	}
	h, pWrote := takeOverOverTwoPaths(t, target)
	if err := h.RegisterFresh(); err != nil {
		t.Fatalf("RegisterFresh: %v", err)
	}
	if pWrote() || !target.MayWrite("vm2/a") || !target.MayWrite("vm2/b") {
		t.Errorf("the preempted node could write: %v; H's paths may write: %v, %v; want only H's", pWrote(), target.MayWrite("vm2/a"), target.MayWrite("vm2/b"))
	}
}

// A kernel that cannot list keys leaves RegisterFresh unable to see another registrant, so it changes nothing.
func TestRegisterFreshRefusesWithoutAKeyListing(t *testing.T) {
	target := NewLIOTarget()
	pr := target.Node("vm2/1", 0x4743000000000001)
	if err := pr.Register(); err != nil {
		t.Fatal(err)
	}
	if err := target.Node("vm1/1", 0x4743000000000002).Register(); err != nil {
		t.Fatal(err)
	}
	target.unlistable = true
	if err := pr.RegisterFresh(); !errors.Is(err, errKeysUnlisted) {
		t.Errorf("RegisterFresh without a key listing: %v, want errKeysUnlisted", err)
	}
	if target.keys["vm2/1"] != pr.Key() {
		t.Error("RegisterFresh changed the registration although it could not list keys")
	}
}
