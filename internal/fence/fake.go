package fence

import (
	"fmt"
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
	// failUnregister, when set, is what removing a key returns, as from a path that went down.
	failUnregister error
	// refusePreempt makes every preempt conflict, as LIO does for a host whose write flag another freed registration
	// cleared.
	refusePreempt bool
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

// Relogin moves initiator's key to a session the initiator no longer has, as an iSCSI logout and login does on LIO.
func (t *Target) Relogin(initiator string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key, ok := t.keys[initiator]
	if !ok {
		return
	}
	old := initiator + "-old"
	t.keys[old] = key
	delete(t.keys, initiator)
	for i, s := range t.order {
		if s == initiator {
			t.order[i] = old
		}
	}
	if t.holder == initiator {
		t.holder = old
	}
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

// registerNexus registers or removes nexus's key. Called with t.mu held.
func (t *Target) registerNexus(nexus string, old, new uint64, ignore bool) error {
	cur, registered := t.keys[nexus]
	if !ignore && (registered && cur != old || !registered && old != 0) {
		return ErrReservationConflict
	}
	if new != 0 {
		if registered {
			t.keys[nexus] = new
		} else {
			t.add(nexus, new)
		}
		return nil
	}
	if t.failUnregister != nil {
		return t.failUnregister
	}
	if registered {
		t.drop(nexus)
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
	first := d.nexuses()[0]
	if t.keys[first] != key || key == 0 || t.lio && !t.mayWrite(first) {
		return ErrReservationConflict
	}
	if !t.reserved {
		t.holder = first
	}
	t.reserved = true
	return nil
}

func (d *fakeDevice) preemptAbort(own, victim uint64) error {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	first := d.nexuses()[0]
	if cur, registered := t.keys[first]; !registered || cur != own || t.lio && !t.mayWrite(first) || t.refusePreempt {
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
		t.holder = first
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

// preempt is a PREEMPT without abort: it removes every registration of victim but the caller's, even of its own key.
func (d *fakeDevice) preempt(own, victim uint64) error {
	t := d.t
	t.mu.Lock()
	defer t.mu.Unlock()
	first := d.nexuses()[0]
	if cur, registered := t.keys[first]; !registered || cur != own || t.lio && !t.mayWrite(first) {
		return ErrReservationConflict
	}
	found, holderKey := false, t.keys[t.holder]
	for initiator, key := range t.keys {
		if key == victim {
			found = true
			if initiator != first {
				t.drop(initiator)
			}
		}
	}
	if !found {
		return ErrReservationConflict
	}
	if t.holder != "" && holderKey == victim {
		t.holder = first
	}
	return nil
}
