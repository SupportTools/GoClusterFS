package fence

import (
	"slices"
	"testing"
)

// After an iSCSI logout and login, the node's key stays registered under its earlier session, and this session holds
// none. Leave removes the key from the earlier session too, and keeps the peer's registration.
func TestLeaveRemovesTheKeyAnEarlierSessionKept(t *testing.T) {
	target := NewTarget()
	peer := target.Node("vm2/1", NodeKey([16]byte{2}))
	key := NodeKey([16]byte{1})
	earlier, current := target.Node("vm1/1", key), target.Node("vm1/2", key)
	for _, n := range []*PR{earlier, peer} {
		if err := n.Register(); err != nil {
			t.Fatal(err)
		}
	}
	if err := earlier.Reserve(); err != nil {
		t.Fatal(err)
	}
	if gotEarlier, err := current.Leave(); err != nil || !gotEarlier {
		t.Fatalf("Leave from a session without the key = %v, %v; want true, nil", gotEarlier, err)
	}
	if keys, err := peer.Keys(); err != nil || !slices.Equal(keys, []uint64{peer.Key()}) {
		t.Errorf("Keys after Leave = %x, %v; want only the peer's", keys, err)
	}
	if target.MayWrite("vm1/1") || target.MayWrite("vm1/2") || !target.MayWrite("vm2/1") {
		t.Error("after Leave, a session of the host that left may write, or the peer may not")
	}

	if gotEarlier, err := peer.Leave(); err != nil || gotEarlier {
		t.Errorf("Leave from the session that holds the key = %v, %v; want false, nil", gotEarlier, err)
	}
	if keys, err := peer.Keys(); err != nil || len(keys) != 0 {
		t.Errorf("Keys after the peer left = %x, %v; want none", keys, err)
	}
}

// multipath routes reservation commands as dm-multipath does: a register goes to every path in order and stops at the
// first error, taking a key it was adding off every path again, and reserves and preempts go to the first path that
// takes them.
type multipath []device

func (m multipath) register(old, new uint64, ignore bool) error {
	for _, path := range m {
		if err := path.register(old, new, ignore); err != nil {
			if new != 0 {
				for _, p := range m {
					_ = p.register(new, 0, false)
				}
			}
			return err
		}
	}
	return nil
}

func (m multipath) first(command func(device) error) (err error) {
	for _, path := range m {
		if err = command(path); err == nil {
			return nil
		}
	}
	return err
}

func (m multipath) reserve(key uint64) error {
	return m.first(func(d device) error { return d.reserve(key) })
}

func (m multipath) preemptAbort(own, victim uint64) error {
	return m.first(func(d device) error { return d.preemptAbort(own, victim) })
}

func (m multipath) preempt(own, victim uint64) error {
	return m.first(func(d device) error { return d.preempt(own, victim) })
}

func (m multipath) clear(key uint64) error                   { return m[0].clear(key) }
func (m multipath) readKeys() ([]uint64, error)              { return m[0].readKeys() }
func (m multipath) readReservation() (uint64, uint32, error) { return m[0].readReservation() }

// pathDown fails the plain preempt, as a path that just went down would, and passes every other command on.
type pathDown struct{ device }

func (pathDown) preempt(uint64, uint64) error { return statusError(statusIOError) }

// When the first path misses the preempt, dm sends it down the next, whose preempt also takes the key off the first
// path. An unregister then stops at the first path, so without the key going back on every path first, the path that
// preempted would keep it, and with it the reservation the earlier session held.
func TestLeaveOverTwoPathsWhenTheFirstPathMissesThePreempt(t *testing.T) {
	target := NewTarget()
	peer := target.Node("vm2/1", NodeKey([16]byte{2}))
	key := NodeKey([16]byte{1})
	earlier := target.Node("vm1/old", key)
	for _, n := range []*PR{earlier, peer} {
		if err := n.Register(); err != nil {
			t.Fatal(err)
		}
	}
	if err := earlier.Reserve(); err != nil {
		t.Fatal(err)
	}
	host := &PR{dev: multipath{pathDown{&fakeDevice{t: target, initiator: "vm1/a"}}, &fakeDevice{t: target, initiator: "vm1/b"}}, key: key}
	if gotEarlier, err := host.Leave(); err != nil || !gotEarlier {
		t.Fatalf("Leave over two paths = %v, %v; want true, nil", gotEarlier, err)
	}
	for _, session := range []string{"vm1/old", "vm1/a", "vm1/b"} {
		if target.MayWrite(session) {
			t.Errorf("session %s may write after Leave", session)
		}
	}
	if keys, err := peer.Keys(); err != nil || !slices.Equal(keys, []uint64{peer.Key()}) {
		t.Errorf("Keys after Leave = %x, %v; want only the peer's", keys, err)
	}
}
