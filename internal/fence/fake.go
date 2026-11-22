package fence

import (
	"slices"
	"sync"
	"syscall"
)

// Target is an in-memory device with persistent reservations for tests; Node gives each initiator a Fencer.
type Target struct {
	mu        sync.Mutex
	keys      map[string]uint64
	reserved  bool
	preempted []uint64
}

func NewTarget() *Target { return &Target{keys: map[string]uint64{}} }

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
		return nil
	}
	delete(t.keys, d.initiator)
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
			delete(t.keys, initiator)
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

// preempt is a PREEMPT without abort: it removes every registration of victim but the caller's, even of its own key.
func (d *fakeDevice) preempt(own, victim uint64) error {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, registered := t.keys[d.initiator]; !registered || cur != own {
		return ErrReservationConflict
	}
	found := false
	for initiator, key := range t.keys {
		if key == victim {
			found = true
			if initiator != d.initiator {
				delete(t.keys, initiator)
			}
		}
	}
	if !found {
		return ErrReservationConflict
	}
	return nil
}
