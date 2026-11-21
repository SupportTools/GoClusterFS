package fence

import (
	"fmt"
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
	// generation counts the registers, preempts and clears that took effect, as a device's PRgeneration does.
	generation uint32
	// Unlistable makes key listings fail as on kernels without IOC_PR_READ_KEYS.
	Unlistable bool
}

func NewTarget() *Target { return &Target{keys: map[string]uint64{}} }

// Node returns the Fencer of initiator, whose registration is separate from every other initiator's.
func (t *Target) Node(initiator string, key uint64) *PR {
	return &PR{dev: &fakeDevice{t: t, initiator: initiator}, key: key}
}

// MultipathNode returns the Fencer of initiator on a dm-multipath map with paths paths, named initiator#0 and up. Like
// dm, it registers on every path and, if any path refuses, removes the key from all of them.
func (t *Target) MultipathNode(initiator string, paths int, key uint64) *PR {
	return &PR{dev: &fakeDevice{t: t, initiator: initiator, npaths: paths}, key: key}
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
	npaths    int
}

// nexuses names the device's paths: the initiator itself, or one per dm-multipath path.
func (d *fakeDevice) nexuses() []string {
	if d.npaths <= 1 {
		return []string{d.initiator}
	}
	names := make([]string, d.npaths)
	for i := range names {
		names[i] = fmt.Sprintf("%s#%d", d.initiator, i)
	}
	return names
}

func (d *fakeDevice) register(old, new uint64, ignore bool) error {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, nexus := range d.nexuses() {
		if err := t.registerNexus(nexus, old, new, ignore); err != nil {
			if new != 0 {
				for _, nexus := range d.nexuses() {
					_ = t.registerNexus(nexus, new, 0, false)
				}
			}
			return err
		}
	}
	return nil
}

func (t *Target) registerNexus(nexus string, old, new uint64, ignore bool) error {
	cur, registered := t.keys[nexus]
	if !ignore && (registered && cur != old || !registered && old != 0) {
		return ErrReservationConflict
	}
	t.generation++
	if new != 0 {
		t.keys[nexus] = new
		return nil
	}
	delete(t.keys, nexus)
	if len(t.keys) == 0 {
		t.reserved = false
	}
	return nil
}

func (d *fakeDevice) paths() (int, error) { return len(d.nexuses()), nil }

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
	if cur, registered := t.keys[d.nexuses()[0]]; !registered || cur != own {
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
	t.generation++
	return nil
}

func (d *fakeDevice) clear(key uint64) error {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, registered := t.keys[d.nexuses()[0]]; !registered || cur != key {
		return ErrReservationConflict
	}
	t.keys = map[string]uint64{}
	t.reserved = false
	t.generation++
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
	_, keys, err := d.listKeys()
	return keys, err
}

func (d *fakeDevice) listKeys() (uint32, []uint64, error) {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Unlistable {
		return 0, nil, ErrUnsupported
	}
	keys := make([]uint64, 0, len(t.keys))
	for _, key := range t.keys {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return t.generation, keys, nil
}
