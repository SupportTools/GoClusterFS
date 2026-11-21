package fence

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"syscall"
	"time"
)

// Outcome is how one self-test step went.
type Outcome string

const (
	Pass Outcome = "PASS"
	// Warn is a command fencing works without, which the kernel or device lacks.
	Warn Outcome = "WARN"
	Fail Outcome = "FAIL"
	Skip Outcome = "SKIP"
)

// Check is one self-test step: the command, as fencing sends it, and the device's answer.
type Check struct {
	Outcome Outcome
	Step    string
	Answer  string
}

// StepTimeout is how long the self-test waits for any one command. Storage that queues I/O while a path is down may
// never answer.
const StepTimeout = 30 * time.Second

// SelfTest qualifies a device for fencing. It sends the reservation commands this package sends, from temporary keys
// with fsck's 0x4746 prefix, and checks every answer; a write step sends 4 KiB with O_DIRECT. A command that gets no
// answer within StepTimeout fails the test, which then sends nothing more.
type SelfTest struct {
	dev     device
	write   func() error
	sink    func(Check)
	poll    time.Duration
	timeout time.Duration
	held    uint64
	failed  bool
	listing bool
	reading bool
	retried bool
	hung    bool
	stopped bool
}

// NewSelfTest tests the device f is open on. write writes 4 KiB to it with O_DIRECT.
func NewSelfTest(f *os.File, write func() error, report func(Check)) *SelfTest {
	return newSelfTest(blockDevice{f: f, partition: isPartition(f)}, write, report)
}

func newSelfTest(dev device, write func() error, report func(Check)) *SelfTest {
	s := &SelfTest{sink: report, poll: 200 * time.Millisecond, timeout: StepTimeout, listing: true, reading: true}
	s.dev = guarded{dev: dev, s: s}
	s.write = func() error {
		_, err := timed(s, func() (struct{}, error) { return struct{}{}, write() })
		return err
	}
	return s
}

var errNoAnswer = errors.New("no answer")

// timed runs one command with the test's time limit, and once more after status 0x2: the first command from a host
// whose key another host preempted gets a unit attention, which the kernel reports as 0x2.
func timed[T any](s *SelfTest, command func() (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	var zero T
	if s.hung {
		return zero, errNoAnswer
	}
	for try := 0; ; try++ {
		done := make(chan result, 1)
		go func() {
			v, err := command()
			done <- result{v, err}
		}()
		select {
		case r := <-done:
			if try == 0 && errors.Is(r.err, statusError(statusIOError)) {
				s.retried = true
				continue
			}
			return r.v, r.err
		case <-time.After(s.timeout):
			s.hung = true
			return zero, fmt.Errorf("%w within %v", errNoAnswer, s.timeout)
		}
	}
}

// guarded sends every command through timed.
type guarded struct {
	dev device
	s   *SelfTest
}

func (g guarded) register(old, new uint64, ignore bool) error {
	_, err := timed(g.s, func() (struct{}, error) { return struct{}{}, g.dev.register(old, new, ignore) })
	return err
}

func (g guarded) reserve(key uint64) error {
	_, err := timed(g.s, func() (struct{}, error) { return struct{}{}, g.dev.reserve(key) })
	return err
}

func (g guarded) preemptAbort(own, victim uint64) error {
	_, err := timed(g.s, func() (struct{}, error) { return struct{}{}, g.dev.preemptAbort(own, victim) })
	return err
}

func (g guarded) clear(key uint64) error {
	_, err := timed(g.s, func() (struct{}, error) { return struct{}{}, g.dev.clear(key) })
	return err
}

func (g guarded) readKeys() ([]uint64, error) { return timed(g.s, g.dev.readKeys) }

func (g guarded) readReservation() (uint64, uint32, error) {
	type reservation struct {
		key uint64
		typ uint32
	}
	r, err := timed(g.s, func() (reservation, error) {
		key, typ, err := g.dev.readReservation()
		return reservation{key, typ}, err
	})
	return r.key, r.typ, err
}

// report passes c on, noting a retry after status 0x2. Once a command got no answer, it passes on only the
// step that waited for it.
func (s *SelfTest) report(c Check) {
	if s.retried {
		c.Answer += "; the first try got status 0x2, the kernel's code for any failure but a conflict, such as the unit attention a preempted host's first command gets, and was retried"
		s.retried = false
	}
	if s.hung {
		if s.stopped {
			return
		}
		s.stopped = true
		c.Answer += "; the device or its path stopped answering, so the test sends nothing more"
	}
	s.sink(c)
}

// Failed reports whether any step failed.
func (s *SelfTest) Failed() bool { return s.failed }

// Run qualifies the device from this host alone. It never clears another host's key, and it removes its own on every
// path.
func (s *SelfTest) Run() {
	defer s.release()
	before := s.keys("list the keys before the test", nil)
	k1, k2 := ToolKey(), ToolKey()
	if !s.register(fmt.Sprintf("register %s, replacing any key this host holds (mount, format, fsck)", hexKey(k1)), k1) {
		return
	}
	s.expect(fmt.Sprintf("register %s over itself (how a preempt checks that its own key is left)", hexKey(k1)),
		s.dev.register(k1, k1, false), "a node could not tell whether it was fenced")
	s.keys(fmt.Sprintf("list the keys: %s is there", hexKey(k1)), having(k1))
	if !s.expect("reserve Write Exclusive, All Registrants (type 5)", s.dev.reserve(k1),
		"another reservation holds the device, or it lacks type 5, and a mount would refuse") {
		return
	}
	s.reservation("read the reservation", typeWriteExclusiveAllRegs)
	s.expect("reserve again (every mount reserves)", s.dev.reserve(k1), "a second mount would refuse")
	s.writes("write 4 KiB with O_DIRECT as a registered host", true)
	gone := ToolKey()
	if err := s.dev.preemptAbort(k1, gone); err == nil || errors.Is(err, ErrReservationConflict) {
		s.report(Check{Pass, fmt.Sprintf("preempt and abort %s, which no host holds (a dead node's key already gone)", hexKey(gone)), answer(err)})
	} else {
		s.fail(fmt.Sprintf("preempt and abort %s, which no host holds (a dead node's key already gone)", hexKey(gone)),
			answer(err)+"; want success or a reservation conflict, which fencing reads as the key gone already")
	}
	s.expect(fmt.Sprintf("register %s over itself: this host's key outlived that preempt", hexKey(k1)),
		s.dev.register(k1, k1, false), "fencing would read this node as fenced")
	if !s.register(fmt.Sprintf("replace %s with %s (a node mounting again)", hexKey(k1), hexKey(k2)), k2) {
		return
	}
	s.keys(fmt.Sprintf("list the keys: %s replaced %s", hexKey(k2), hexKey(k1)), having(k2).without(k1))
	if !s.unregister(fmt.Sprintf("unregister %s (unmount)", hexKey(k2))) {
		return
	}
	left := having().without(k1, k2)
	if !slices.Contains(before, 0) {
		left = left.without(0)
	}
	s.keys("list the keys: this test left none", left)
	if s.listing && len(before) == 0 {
		s.reservation("read the reservation: it went with the last key", 0)
	}
	s.clear(before)
}

// clear clears every key and the reservation, as -clear-reservations does, unless another host may hold a key.
func (s *SelfTest) clear(before []uint64) {
	const step = "clear every key and the reservation (fsck -clear-reservations)"
	switch {
	case !s.listing:
		s.report(Check{Skip, step, "the keys cannot be listed, so clearing could remove another host's key"})
		return
	case len(before) > 0:
		s.report(Check{Skip, step, "other hosts hold keys, which clearing would remove"})
		return
	}
	k3 := ToolKey()
	if !s.register(fmt.Sprintf("register %s to clear with", hexKey(k3)), k3) ||
		!s.expect("reserve Write Exclusive, All Registrants (type 5)", s.dev.reserve(k3), "a mount would refuse") {
		return
	}
	err := s.dev.clear(k3)
	if err == nil {
		s.held = 0
	}
	if s.expect(step, err, "-clear-reservations could not reset fencing") {
		s.keys("list the keys: none are left", having().only())
		s.reservation("read the reservation: it is gone", 0)
	}
}

// Victim is host A of the two-host test. It registers and reserves, waits up to wait for host B to preempt its key,
// then checks that the device refuses its write. announce tells the operator the key to give host B.
func (s *SelfTest) Victim(wait time.Duration, announce func(key uint64)) {
	defer s.release()
	key := ToolKey()
	if !s.register(fmt.Sprintf("register %s", hexKey(key)), key) ||
		!s.expect("reserve Write Exclusive, All Registrants (type 5)", s.dev.reserve(key), "a mount would refuse") ||
		!s.writes("write 4 KiB with O_DIRECT before host B preempts this host", true) {
		return
	}
	announce(key)
	step := fmt.Sprintf("wait up to %v for host B to preempt %s", wait, hexKey(key))
	start := time.Now()
	for {
		gone, err := s.preempted(key)
		if err != nil {
			s.fail(step, answer(err))
			return
		}
		if gone {
			break
		}
		if time.Since(start) > wait {
			s.fail(step, "host B did not preempt it")
			return
		}
		time.Sleep(s.poll)
	}
	s.held = 0
	s.report(Check{Pass, step, fmt.Sprintf("gone after %v", time.Since(start).Round(time.Millisecond))})
	s.writes("write 4 KiB with O_DIRECT after host B preempted this host", false)
	err := s.dev.register(key, key, false)
	if errors.Is(err, ErrReservationConflict) {
		s.report(Check{Pass, fmt.Sprintf("register %s over itself: the device says it is gone", hexKey(key)), answer(err)})
	} else {
		s.fail(fmt.Sprintf("register %s over itself: the device says it is gone", hexKey(key)),
			answer(err)+"; want a reservation conflict, which tells a node it was fenced")
	}
}

// preempted reports whether key is gone: from the key list where the device lists keys, else from registering it over
// itself, which only a registered key may do.
func (s *SelfTest) preempted(key uint64) (bool, error) {
	if s.listing {
		keys, err := s.dev.readKeys()
		if err == nil {
			return !slices.Contains(keys, key), nil
		}
		if !errors.Is(err, ErrUnsupported) {
			return false, err
		}
		s.listing = false
	}
	err := s.dev.register(key, key, false)
	if errors.Is(err, ErrReservationConflict) {
		return true, nil
	}
	return false, err
}

// Preempt is host B of the two-host test. It preempts and aborts host A's key, as a survivor fences a dead node, then
// keeps its own key registered for hold, so the reservation stands while host A tries its write.
func (s *SelfTest) Preempt(victim uint64, hold time.Duration) {
	defer s.release()
	s.keys(fmt.Sprintf("list the keys: host A's %s is there", hexKey(victim)), having(victim))
	key := ToolKey()
	if !s.register(fmt.Sprintf("register %s", hexKey(key)), key) {
		return
	}
	if !s.expect(fmt.Sprintf("preempt and abort %s (a survivor fencing a dead node)", hexKey(victim)),
		s.dev.preemptAbort(key, victim), "a survivor could not fence a dead node") {
		return
	}
	s.keys(fmt.Sprintf("list the keys: %s is gone", hexKey(victim)), having(key).without(victim))
	s.reservation("read the reservation: it stands", typeWriteExclusiveAllRegs)
	s.writes("write 4 KiB with O_DIRECT as the survivor", true)
	time.Sleep(hold)
	s.report(Check{Pass, fmt.Sprintf("keep %s for %v while host A tries its write", hexKey(key), hold), "done"})
}

// Keep registers a key and reserves, and leaves both for CheckKept to look for after the device is detached and
// attached again, or the storage restarts.
func (s *SelfTest) Keep() (uint64, bool) {
	key := ToolKey()
	if !s.register(fmt.Sprintf("register %s", hexKey(key)), key) {
		return 0, false
	}
	if !s.expect("reserve Write Exclusive, All Registrants (type 5)", s.dev.reserve(key), "a mount would refuse") {
		s.release()
		return 0, false
	}
	s.keys(fmt.Sprintf("list the keys: %s is there", hexKey(key)), having(key))
	s.reservation("read the reservation", typeWriteExclusiveAllRegs)
	return key, true
}

// CheckKept checks that key, which Keep left, and the reservation survived, then removes the key.
func (s *SelfTest) CheckKept(key uint64) {
	defer s.release()
	s.keys(fmt.Sprintf("list the keys: %s survived", hexKey(key)), having(key))
	s.reservation("read the reservation: it survived", typeWriteExclusiveAllRegs)
	err := s.dev.register(key, key, false)
	if err == nil {
		s.held = key
	}
	s.expect(fmt.Sprintf("register %s over itself: this host still holds it", hexKey(key)), err,
		"the registration did not survive, and every node of a volume would be fenced")
}

func (s *SelfTest) register(step string, key uint64) bool {
	err := s.dev.register(0, key, true)
	if err == nil {
		s.held = key
	}
	return s.expect(step, err, "fencing cannot start without it")
}

func (s *SelfTest) unregister(step string) bool {
	err := s.dev.register(s.held, 0, false)
	if err == nil {
		s.held = 0
	}
	return s.expect(step, err, "an unmount would leave its key")
}

// release unregisters the key the test still holds. A conflict means the key is gone already.
func (s *SelfTest) release() {
	if s.held == 0 {
		return
	}
	step := fmt.Sprintf("clean up: unregister %s", hexKey(s.held))
	if s.hung {
		s.failed = true
		s.sink(Check{Fail, step, "not sent; once the device answers, remove the key with goclusterfs-fsck -clear-reservations"})
		return
	}
	err := s.dev.register(s.held, 0, false)
	switch {
	case err == nil:
		s.report(Check{Pass, step, answer(nil)})
	case errors.Is(err, ErrReservationConflict):
		s.report(Check{Pass, step, answer(err) + ": the key was gone already"})
	default:
		s.fail(step, answer(err)+"; remove it with goclusterfs-fsck -clear-reservations")
	}
	s.held = 0
}

func (s *SelfTest) expect(step string, err error, why string) bool {
	if err != nil {
		s.fail(step, answer(err)+"; "+why)
		return false
	}
	s.report(Check{Pass, step, answer(nil)})
	return true
}

func (s *SelfTest) fail(step, answer string) {
	s.failed = true
	s.report(Check{Fail, step, answer})
}

// writes checks that a 4 KiB write lands, or that the device refuses it with a reservation conflict.
func (s *SelfTest) writes(step string, allowed bool) bool {
	err := s.write()
	switch {
	case allowed && err == nil:
		s.report(Check{Pass, step, "written"})
		return true
	case allowed:
		s.fail(step, answer(err)+"; a registered host must be able to write")
	case errors.Is(err, syscall.EBADE):
		s.report(Check{Pass, step, "refused with EBADE, a reservation conflict"})
		return true
	case err == nil:
		s.fail(step, "written: the device took the write of a host whose key was preempted, so it cannot fence")
	default:
		s.fail(step, answer(err)+"; want EBADE, the reservation conflict a fenced node stops on")
	}
	return false
}

// keys checks the device's key list with want. Where the kernel or device cannot list keys, it warns once and skips
// every later listing.
func (s *SelfTest) keys(step string, want *keyCheck) []uint64 {
	if !s.listing {
		s.report(Check{Skip, step, "the keys cannot be listed"})
		return nil
	}
	keys, err := s.dev.readKeys()
	switch {
	case errors.Is(err, ErrUnsupported):
		s.listing = false
		s.report(Check{Warn, step, "this kernel or device cannot list keys (" + cause(err) + "); fencing works without it, but -clear-reservations cannot show them and this test cannot check them"})
		return nil
	case err != nil:
		s.fail(step, answer(err))
		return nil
	}
	if problem := want.check(keys); problem != "" {
		s.fail(step, problem+": "+listed(keys))
	} else {
		s.report(Check{Pass, step, listed(keys)})
	}
	return keys
}

// reservation checks the reservation type, 0 for none, where the kernel can read it.
func (s *SelfTest) reservation(step string, want uint32) {
	if !s.reading {
		s.report(Check{Skip, step, "the reservation cannot be read"})
		return
	}
	_, typ, err := s.dev.readReservation()
	switch {
	case errors.Is(err, ErrUnsupported):
		s.reading = false
		s.report(Check{Warn, step, "this kernel or device cannot read the reservation (" + cause(err) + "); fencing works without it"})
	case err != nil:
		s.fail(step, answer(err))
	case typ != want:
		s.fail(step, fmt.Sprintf("%s, want %s", reservationType(typ), reservationType(want)))
	default:
		s.report(Check{Pass, step, reservationType(typ)})
	}
}

func reservationType(typ uint32) string {
	switch typ {
	case 0:
		return "no reservation"
	case typeWriteExclusiveAllRegs:
		return "type 5, Write Exclusive, All Registrants"
	}
	return fmt.Sprintf("type %d", typ)
}

// keyCheck is what a key listing must hold.
type keyCheck struct {
	has, hasNot []uint64
	exact       bool
}

func having(keys ...uint64) *keyCheck { return &keyCheck{has: keys} }

func (c *keyCheck) without(keys ...uint64) *keyCheck {
	c.hasNot = append(c.hasNot, keys...)
	return c
}

// only makes the listing hold the wanted keys and no other.
func (c *keyCheck) only() *keyCheck {
	c.exact = true
	return c
}

func (c *keyCheck) check(keys []uint64) string {
	if c == nil {
		return ""
	}
	for _, key := range c.has {
		if !slices.Contains(keys, key) {
			return hexKey(key) + " is missing"
		}
	}
	for _, key := range c.hasNot {
		if slices.Contains(keys, key) {
			return hexKey(key) + " is still there"
		}
	}
	if c.exact && len(keys) != len(c.has) {
		return fmt.Sprintf("want %d keys", len(c.has))
	}
	return ""
}

func listed(keys []uint64) string {
	if len(keys) == 0 {
		return "no keys"
	}
	parts := make([]string, len(keys))
	for i, key := range keys {
		parts[i] = hexKey(key)
	}
	return strings.Join(parts, " ")
}

func hexKey(key uint64) string { return fmt.Sprintf("%#016x", key) }

// cause is the errno under err, the kernel's own answer.
func cause(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno.Error()
	}
	return err.Error()
}

func answer(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}
