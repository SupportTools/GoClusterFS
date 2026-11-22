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
	// lio decides writes as LIO does: writable holds its flag per initiator, holder the session holding the
	// reservation, and order the sessions in the order they registered.
	lio      bool
	writable map[string]bool
	holder   string
	order    []string
	// unlistable makes the target answer key listings as kernels without IOC_PR_READ_KEYS do.
	unlistable bool
	// holderBySession refuses the holder's initiator's other sessions, as LIO does for an initiator whose sessions
	// differ in ISID bytes 0-3. open-iscsi's differ only in the qualifier, so LIO takes all of them for the holder.
	holderBySession bool
}

func NewTarget() *Target { return &Target{keys: map[string]uint64{}, writable: map[string]bool{}} }

// NewLIOTarget is a Target that decides writes as LIO does for open-iscsi initiators. Name each session
// "initiator/session". Registrations belong to sessions. Every session of the initiator holding the reservation may
// write; any other session may while its initiator's flag is set, which a new registration of the initiator sets and
// removing any of its registrations clears. A session that may not write may not reserve or preempt either. Removing the holder's registration hands the reservation to the oldest
// registration, and preempting the holder's key hands it to the preempting session.
func NewLIOTarget() *Target {
	t := NewTarget()
	t.lio = true
	return t
}

func initiatorOf(session string) string {
	initiator, _, _ := strings.Cut(session, "/")
	return initiator
}

// add records session's new registration. Called with t.mu held.
func (t *Target) add(session string, key uint64) {
	t.keys[session] = key
	t.order = append(t.order, session)
	t.writable[initiatorOf(session)] = true
}

// drop removes session's registration. Called with t.mu held.
func (t *Target) drop(session string) {
	delete(t.keys, session)
	t.order = slices.DeleteFunc(t.order, func(s string) bool { return s == session })
	t.writable[initiatorOf(session)] = false
	if t.holder == session {
		t.holder = ""
		if len(t.order) > 0 {
			t.holder = t.order[0]
		}
	}
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
	return t.mayWrite(initiator)
}

func (t *Target) mayWrite(initiator string) bool {
	_, registered := t.keys[initiator]
	if t.lio && t.reserved {
		switch {
		case t.holder == initiator:
			return true
		case initiatorOf(t.holder) == initiatorOf(initiator):
			return !t.holderBySession
		}
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
		if registered {
			t.keys[d.initiator] = new
		} else {
			t.add(d.initiator, new)
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
	if t.keys[d.initiator] != key || key == 0 || t.lio && !t.mayWrite(d.initiator) {
		return ErrReservationConflict
	}
	if !t.reserved {
		t.holder = d.initiator
	}
	t.reserved = true
	return nil
}

func (d *fakeDevice) preemptAbort(own, victim uint64) error {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, registered := t.keys[d.initiator]; !registered || cur != own || t.lio && !t.mayWrite(d.initiator) {
		return ErrReservationConflict
	}
	found, holderKey := false, t.keys[t.holder]
	for initiator, key := range t.keys {
		if key == victim {
			t.drop(initiator)
			found = true
		}
	}
	if !found {
		return ErrReservationConflict
	}
	if t.holder != "" && holderKey == victim {
		t.holder = d.initiator
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
	t.holder, t.order = "", nil
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
	if t.unlistable {
		return nil, ErrUnsupported
	}
	keys := make([]uint64, 0, len(t.keys))
	for _, key := range t.keys {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys, nil
}
