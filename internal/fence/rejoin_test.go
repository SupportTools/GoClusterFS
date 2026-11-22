package fence

import "testing"

// After a relogin a host holds keys under two sessions: an orphan from before it, whose unregister failed, and its
// mount's after it. The host crashes and reboots onto its first session, whose REGISTER changes the orphan's key in
// place. A peer then revokes the crashed mount's key, which on LIO stops writes from every session of the host, though
// the new key stays registered. RegisterFresh registers the key as a new registration, which lets the host write.
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
	keys, _ := rebooted.Keys()
	if !target.MayWrite("vm2/1") || len(keys) != 2 {
		t.Errorf("after RegisterFresh the host may write: %v, keys %x; want it writing with the peer's key and its own", target.MayWrite("vm2/1"), keys)
	}
}

// A session that holds no key registers once and reserves.
func TestRegisterFreshOnANewSession(t *testing.T) {
	target := NewLIOTarget()
	pr := target.Node("vm1/1", 0x4743000000000001)
	if err := pr.RegisterFresh(); err != nil {
		t.Fatal(err)
	}
	other := target.Node("vm9/1", 0)
	if !target.MayWrite("vm1/1") || target.MayWrite(other.dev.(*fakeDevice).initiator) {
		t.Error("RegisterFresh did not leave the host registered with the device reserved")
	}
}
