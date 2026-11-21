package fence

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type recorder struct {
	mu     sync.Mutex
	checks []Check
}

func (r *recorder) report(c Check) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks = append(r.checks, c)
}

func (r *recorder) with(outcome Outcome) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var steps []string
	for _, c := range r.checks {
		if c.Outcome == outcome {
			steps = append(steps, c.Step+": "+c.Answer)
		}
	}
	return slices.Clip(steps)
}

func (r *recorder) String() string {
	var b strings.Builder
	for _, c := range r.checks {
		fmt.Fprintf(&b, "\n  %s  %s: %s", c.Outcome, c.Step, c.Answer)
	}
	return b.String()
}

func keysOf(t *testing.T, target *Target) []uint64 {
	t.Helper()
	keys, err := target.Node("lister", 0).Keys()
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

// On a device that fences, every step passes, the clear runs, and the test leaves no key.
func TestSelfTestPassesAndLeavesNoKey(t *testing.T) {
	target := NewTarget()
	var r recorder
	s := target.SelfTest("a", r.report)
	s.Run()
	if s.Failed() || len(r.with(Warn)) > 0 || len(r.with(Skip)) > 0 {
		t.Errorf("want every step to pass:%s", &r)
	}
	if !slices.ContainsFunc(r.checks, func(c Check) bool { return strings.HasPrefix(c.Step, "clear") && c.Outcome == Pass }) {
		t.Errorf("the clear did not run:%s", &r)
	}
	if keys := keysOf(t, target); len(keys) != 0 {
		t.Errorf("the test left keys %x", keys)
	}
}

// Another host's key and reservation stay: the test skips the clear, which would remove them.
func TestSelfTestLeavesAnotherHostsKey(t *testing.T) {
	target := NewTarget()
	other := target.Node("b", NodeKey([16]byte{2}))
	if err := other.Register(); err != nil {
		t.Fatal(err)
	}
	if err := other.Reserve(); err != nil {
		t.Fatal(err)
	}
	var r recorder
	s := target.SelfTest("a", r.report)
	s.Run()
	if s.Failed() {
		t.Errorf("want no failure:%s", &r)
	}
	if skipped := r.with(Skip); len(skipped) != 1 || !strings.HasPrefix(skipped[0], "clear") {
		t.Errorf("want only the clear skipped, got %q", skipped)
	}
	if keys := keysOf(t, target); !slices.Equal(keys, []uint64{other.Key()}) {
		t.Errorf("keys after the test = %x, want only the other host's", keys)
	}
	if target.MayWrite("c") {
		t.Error("the other host's reservation is gone")
	}
}

// Host B's preempt shuts host A out: A's write is refused, and B's key is gone when B is done.
func TestSelfTestTwoHosts(t *testing.T) {
	target := NewTarget()
	var ra, rb recorder
	a, b := target.SelfTest("a", ra.report), target.SelfTest("b", rb.report)
	a.poll = time.Millisecond
	keys := make(chan uint64, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Victim(10*time.Second, func(key uint64) { keys <- key })
	}()
	b.Preempt(<-keys, 200*time.Millisecond)
	<-done
	if a.Failed() || b.Failed() {
		t.Errorf("want both hosts to pass\nhost A:%s\nhost B:%s", &ra, &rb)
	}
	if !slices.ContainsFunc(ra.checks, func(c Check) bool {
		return strings.Contains(c.Step, "after host B preempted") && c.Outcome == Pass && strings.Contains(c.Answer, "EBADE")
	}) {
		t.Errorf("host A's write after the preempt was not refused:%s", &ra)
	}
	if keys := keysOf(t, target); len(keys) != 0 {
		t.Errorf("the test left keys %x", keys)
	}
}

// writableDevice takes every write, as storage that ignores reservations would.
func TestSelfTestFailsWhenAPreemptedHostCanWrite(t *testing.T) {
	target := NewTarget()
	var ra, rb recorder
	a := newSelfTest(&fakeDevice{t: target, initiator: "a"}, func() error { return nil }, ra.report)
	a.poll = time.Millisecond
	keys := make(chan uint64, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Victim(10*time.Second, func(key uint64) { keys <- key })
	}()
	target.SelfTest("b", rb.report).Preempt(<-keys, 200*time.Millisecond)
	<-done
	if failed := ra.with(Fail); len(failed) != 1 || !strings.Contains(failed[0], "cannot fence") {
		t.Errorf("want host A to fail its write after the preempt, got %q", failed)
	}
}

// refusingReserve answers every reserve with a conflict, as a device held by another reservation type does.
type refusingReserve struct{ *fakeDevice }

func (refusingReserve) reserve(uint64) error { return ErrReservationConflict }

// A failed step stops the test, which still removes its key.
func TestSelfTestRemovesItsKeyAfterAFailure(t *testing.T) {
	target := NewTarget()
	var r recorder
	s := newSelfTest(refusingReserve{&fakeDevice{t: target, initiator: "a"}}, func() error { return nil }, r.report)
	s.Run()
	if !s.Failed() || len(r.with(Fail)) != 1 {
		t.Errorf("want only the reserve to fail:%s", &r)
	}
	if last := r.checks[len(r.checks)-1]; !strings.HasPrefix(last.Step, "clean up") || last.Outcome != Pass {
		t.Errorf("want the test to end by removing its key:%s", &r)
	}
	if keys := keysOf(t, target); len(keys) != 0 {
		t.Errorf("the test left keys %x", keys)
	}
}

// zeroingUnregister lists key 0 once a host unregisters, as a device that replaces the key with 0 would.
type zeroingUnregister struct {
	*fakeDevice
	zeroed bool
}

func (d *zeroingUnregister) register(old, new uint64, ignore bool) error {
	err := d.fakeDevice.register(old, new, ignore)
	if err == nil && new == 0 {
		d.zeroed = true
	}
	return err
}

func (d *zeroingUnregister) readKeys() ([]uint64, error) {
	keys, err := d.fakeDevice.readKeys()
	if d.zeroed {
		keys = append(keys, 0)
	}
	return keys, err
}

// An unregister that leaves a key behind fails the listing after it.
func TestSelfTestCatchesAnUnregisterThatLeavesAKey(t *testing.T) {
	target := NewTarget()
	var r recorder
	s := newSelfTest(&zeroingUnregister{fakeDevice: &fakeDevice{t: target, initiator: "a"}}, func() error { return nil }, r.report)
	s.Run()
	if !slices.ContainsFunc(r.with(Fail), func(f string) bool { return strings.Contains(f, "left none") }) {
		t.Errorf("want the listing after the unregister to fail:%s", &r)
	}
}

// unlistedDevice is a kernel without IOC_PR_READ_KEYS or IOC_PR_READ_RESERVATION.
type unlistedDevice struct{ *fakeDevice }

func (unlistedDevice) readKeys() ([]uint64, error) { return nil, ErrUnsupported }
func (unlistedDevice) readReservation() (uint64, uint32, error) {
	return 0, 0, ErrUnsupported
}

// Without listings the test warns once for each, skips the checks that need them and the clear, and still passes.
func TestSelfTestWithoutListings(t *testing.T) {
	target := NewTarget()
	var r recorder
	s := newSelfTest(unlistedDevice{&fakeDevice{t: target, initiator: "a"}}, func() error { return nil }, r.report)
	s.Run()
	if s.Failed() || len(r.with(Warn)) != 2 {
		t.Errorf("want two warnings and no failure:%s", &r)
	}
	if !slices.ContainsFunc(r.with(Skip), func(s string) bool { return strings.HasPrefix(s, "clear") }) {
		t.Errorf("want the clear skipped:%s", &r)
	}
}

// Keep leaves a key for CheckKept, which passes and removes it while the storage kept it, and fails once it is gone.
func TestSelfTestKeepAndCheckKept(t *testing.T) {
	target := NewTarget()
	var r recorder
	key, ok := target.SelfTest("a", r.report).Keep()
	if !ok || !slices.Equal(keysOf(t, target), []uint64{key}) {
		t.Fatalf("Keep did not leave its key:%s", &r)
	}
	kept := target.SelfTest("a", r.report)
	kept.CheckKept(key)
	if kept.Failed() || len(keysOf(t, target)) != 0 {
		t.Errorf("want CheckKept to pass and remove the key:%s", &r)
	}

	key, _ = target.SelfTest("a", r.report).Keep()
	if err := target.Node("other", ToolKey()).Clear(); err != nil {
		t.Fatal(err)
	}
	lost := target.SelfTest("a", r.report)
	lost.CheckKept(key)
	if !lost.Failed() {
		t.Errorf("want CheckKept to fail once the key is gone:%s", &r)
	}
}

func TestReservationConflictShowsTheStatus(t *testing.T) {
	for _, status := range []uintptr{0x18, 0x83} {
		err := ioctlResult(status, 0)
		if !errors.Is(err, ErrReservationConflict) || !strings.Contains(err.Error(), fmt.Sprintf("%#x", status)) {
			t.Errorf("status %#x: %v, want a conflict that names the status", status, err)
		}
	}
}

// Keys print as 16 digits, as -clear-reservations prints them and -preempt and -check-kept read them back.
func TestKeysPrintAsSixteenDigits(t *testing.T) {
	if got := hexKey(0x4746000000000001); got != "0x4746000000000001" {
		t.Errorf("hexKey = %s", got)
	}
}

// stalledReserve never answers a reserve until release closes, as storage that queues I/O while its path is down.
type stalledReserve struct {
	*fakeDevice
	release chan struct{}
}

func (d stalledReserve) reserve(uint64) error {
	<-d.release
	return nil
}

// A command without an answer fails the test, which sends nothing more and names the key left behind.
func TestSelfTestStopsWhenTheDeviceStopsAnswering(t *testing.T) {
	target := NewTarget()
	dev := stalledReserve{&fakeDevice{t: target, initiator: "a"}, make(chan struct{})}
	defer close(dev.release)
	var r recorder
	s := newSelfTest(dev, func() error { return nil }, r.report)
	s.timeout = 50 * time.Millisecond
	s.Run()
	failed := r.with(Fail)
	if len(failed) != 2 || !strings.Contains(failed[0], "no answer within 50ms") || !strings.Contains(failed[1], "not sent") {
		t.Errorf("want the reserve and the clean up to fail:%s", &r)
	}
	if last := r.checks[len(r.checks)-1]; !strings.HasPrefix(last.Step, "clean up") {
		t.Errorf("want nothing reported between the stalled step and the clean up:%s", &r)
	}
}
