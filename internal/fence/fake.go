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
	// failUnregister, when set, is what removing a key returns, as from a path that went down.
	failUnregister error
	// Unlistable makes key listings fail, as on a device that cannot list keys.
	Unlistable bool
}

func NewTarget() *Target { return &Target{keys: map[string]uint64{}} }

// Node returns the Fencer of initiator, whose registration is separate from every other initiator's.
func (t *Target) Node(initiator string, key uint64) *PR {
	return &PR{dev: &fakeDevice{t: t, initiator: initiator}, key: key}
}

// MultipathNode returns the Fencer of initiator on a dm-multipath map with paths paths, named initiator#0 and up. Like
// dm, it registers through every path and, if any path refuses, removes the key from all of them.
func (t *Target) MultipathNode(initiator string, paths int, key uint64) *PR {
	return &PR{dev: &fakeDevice{t: t, initiator: initiator, paths: paths}, key: key}
}

// NVMeNode returns the Fencer of host on an NVMe namespace reached over paths controllers. NVMe registers a host, not
// a path: every path shares one registration, which a reconnect keeps.
func (t *Target) NVMeNode(host string, paths int, key uint64) *PR {
	return &PR{dev: &fakeDevice{t: t, initiator: host, paths: paths, perHost: true}, key: key}
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

// Relogin moves initiator's key to a session the initiator no longer has, as an iSCSI logout and login does on LIO.
func (t *Target) Relogin(initiator string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if key, ok := t.keys[initiator]; ok {
		t.keys[initiator+"-old"] = key
		delete(t.keys, initiator)
	}
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
	// paths is the number of paths of a dm-multipath map, named initiator#0 and up; 0 is a plain disk.
	paths int
	// perHost makes every path share the initiator's one registration, as NVMe does.
	perHost bool
}

// nexuses names the initiator-target nexuses a command from d reaches: one per map path, or the initiator itself.
func (d *fakeDevice) nexuses() []string {
	if d.paths == 0 || d.perHost {
		return []string{d.initiator}
	}
	names := make([]string, d.paths)
	for i := range names {
		names[i] = fmt.Sprintf("%s#%d", d.initiator, i)
	}
	return names
}

func (d *fakeDevice) pathDevices() ([]device, func(), error) {
	if d.paths == 0 {
		return []device{d}, func() {}, nil
	}
	devs := make([]device, d.paths)
	for i := range devs {
		name := d.initiator
		if !d.perHost {
			name = fmt.Sprintf("%s#%d", d.initiator, i)
		}
		devs[i] = &fakeDevice{t: d.t, initiator: name}
	}
	return devs, func() {}, nil
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
	if new != 0 {
		t.keys[nexus] = new
		return nil
	}
	if t.failUnregister != nil {
		return t.failUnregister
	}
	delete(t.keys, nexus)
	if len(t.keys) == 0 {
		t.reserved = false
	}
	return nil
}

func (d *fakeDevice) reserve(key uint64) error {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.keys[d.nexuses()[0]] != key || key == 0 {
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
	if t.Unlistable {
		return nil, ErrUnsupported
	}
	keys := make([]uint64, 0, len(t.keys))
	for _, key := range t.keys {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys, nil
}
