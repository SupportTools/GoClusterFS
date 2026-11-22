package fence

import (
	"slices"
	"strings"
	"sync"
	"syscall"
)

// Target is an in-memory device with persistent reservations for tests; Node gives each initiator a Fencer.
type Target struct {
	mu        sync.Mutex
	keys      map[string]uint64
	reserved  bool
	preempted []uint64
	// lio decides writes per initiator, as LIO does; writable holds the flag it keeps for each.
	lio      bool
	writable map[string]bool
}

func NewTarget() *Target { return &Target{keys: map[string]uint64{}, writable: map[string]bool{}} }

// NewLIOTarget is a Target that decides writes as LIO does. Name each session "initiator/session": registrations
// belong to sessions, but whether a session may write is one flag per initiator, which a new registration of the
// initiator sets and removing any of its registrations clears.
func NewLIOTarget() *Target {
	t := NewTarget()
	t.lio = true
	return t
}

func initiatorOf(session string) string {
	initiator, _, _ := strings.Cut(session, "/")
	return initiator
}

// drop removes session's registration. Called with t.mu held.
func (t *Target) drop(session string) {
	delete(t.keys, session)
	t.writable[initiatorOf(session)] = false
}

// Node returns the Fencer of initiator, whose registration is separate from every other initiator's.
func (t *Target) Node(initiator string, key uint64) *PR {
	return &PR{dev: &fakeDevice{t: t, initiator: initiator}, key: key}
}

// SelfTest returns a self-test that runs as initiator, whose 4 KiB writes the target refuses with EBADE, as Linux does,
// once a reservation shuts initiator out.
func (t *Target) SelfTest(initiator string, report func(Check)) *SelfTest {
	write := func() error {
		if t.MayWrite(initiator) {
			return nil
		}
		return syscall.EBADE
	}
	return newSelfTest(&fakeDevice{t: t, initiator: initiator}, write, report)
}

// MayWrite reports whether the target accepts writes from initiator.
func (t *Target) MayWrite(initiator string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, registered := t.keys[initiator]
	if t.lio {
		registered = t.writable[initiatorOf(initiator)]
	}
	return registered || !t.reserved
}

// Preempted returns the keys preempted so far, in order.
func (t *Target) Preempted() []uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]uint64(nil), t.preempted...)
}

type fakeDevice struct {
	t         *Target
	initiator string
}

func (d *fakeDevice) register(old, new uint64, ignore bool) error {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	cur, registered := t.keys[d.initiator]
	if !ignore && (registered && cur != old || !registered && old != 0) {
		return ErrReservationConflict
	}
	if new != 0 {
		t.keys[d.initiator] = new
		if !registered {
			t.writable[initiatorOf(d.initiator)] = true
		}
		return nil
	}
	if registered {
		t.drop(d.initiator)
	}
	if len(t.keys) == 0 {
		t.reserved = false
	}
	return nil
}

func (d *fakeDevice) reserve(key uint64) error {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.keys[d.initiator] != key || key == 0 {
		return ErrReservationConflict
	}
	t.reserved = true
	return nil
}

func (d *fakeDevice) preemptAbort(own, victim uint64) error {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, registered := t.keys[d.initiator]; !registered || cur != own {
		return ErrReservationConflict
	}
	found := false
	for initiator, key := range t.keys {
		if key == victim {
			t.drop(initiator)
			found = true
		}
	}
	if !found {
		return ErrReservationConflict
	}
	t.preempted = append(t.preempted, victim)
	return nil
}

func (d *fakeDevice) clear(key uint64) error {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, registered := t.keys[d.initiator]; !registered || cur != key {
		return ErrReservationConflict
	}
	t.keys = map[string]uint64{}
	t.writable = map[string]bool{}
	t.reserved = false
	return nil
}

func (d *fakeDevice) readReservation() (uint64, uint32, error) {
	d.t.mu.Lock()
	defer d.t.mu.Unlock()
	if !d.t.reserved {
		return 0, 0, nil
	}
	return 0, typeWriteExclusiveAllRegs, nil
}

func (d *fakeDevice) readKeys() ([]uint64, error) {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	keys := make([]uint64, 0, len(t.keys))
	for _, key := range t.keys {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys, nil
}
