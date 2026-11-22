package fence

import "testing"

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
	if err := old.RegisterFresh(); err != nil {
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
