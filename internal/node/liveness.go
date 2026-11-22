package node

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// recoveryLease is how long a peer may hold a dead slot's recovery before others take it over.
var recoveryLease = 60 * time.Second

// SetRecoveryLease shortens the recovery lease for tests in other packages, and returns a func that restores it.
func SetRecoveryLease(d time.Duration) (restore func()) {
	old := recoveryLease
	recoveryLease = d
	return func() { recoveryLease = old }
}

// deadAfter is how long this node must read the same heartbeat in a slot before
// it counts the slot's node as dead rather than slow, and replays it.
const deadAfter = 2 * HeartbeatTimeout

// stallLimit is how long a node may go without a heartbeat and still write: one failure scan short of deadAfter.
const stallLimit = deadAfter - FailureCheckInterval

// sighting is what this node last read in a slot: the fields a live node keeps
// changing, and since when this node's monotonic clock has seen them unchanged.
type sighting struct {
	uuid          [16]byte
	state         NodeState
	seq           uint64
	beat          int64
	recoveryOwner [16]byte
	recoveryStart int64
	since         time.Time
}

func sightingOf(slot *Slot) sighting {
	return sighting{uuid: slot.UUID, state: slot.State, seq: slot.HeartbeatSeq, beat: slot.HeartbeatTime,
		recoveryOwner: slot.RecoveryOwner, recoveryStart: slot.RecoveryStart}
}

var processStart = time.Now().UnixNano()

// LocalBootID is this host's kernel boot ID, which a node records in its slot.
func LocalBootID() [16]byte { return readBootID() }

func readBootID() (id [16]byte) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return id
	}
	if _, err := hex.Decode(id[:], bytes.ReplaceAll(bytes.TrimSpace(b), []byte("-"), nil)); err != nil {
		return [16]byte{}
	}
	return id
}

func readPIDNamespace() uint64 {
	var st syscall.Stat_t
	if err := syscall.Stat("/proc/self/ns/pid", &st); err != nil {
		return 0
	}
	return st.Ino
}

// Host returns the hostname the slot's node registered with.
func (s *Slot) Host() string {
	return string(bytes.TrimRight(s.Hostname[:], "\x00"))
}

// sameHost reports whether slot was registered under this node's hostname and kernel boot.
func (r *Registry) sameHost(slot *Slot) bool {
	return r.bootID != ([16]byte{}) && slot.BootID == r.bootID && slot.Host() == r.hostname
}

// deadLocalOwner reports whether slot belongs to an exited mount process in
// this node's PID namespace. PIDs from other namespaces prove nothing.
func (r *Registry) deadLocalOwner(slot *Slot) bool {
	if slot.State != NodeStateActive && slot.State != NodeStateLeaving {
		return false
	}
	if !r.sameHost(slot) || r.pidNS == 0 || slot.PIDNamespace != r.pidNS || slot.PID == 0 {
		return false
	}
	if int(slot.PID) == os.Getpid() {
		return slot.ProcStart != processStart
	}
	return processGone(int(slot.PID))
}

// processGone reports whether process pid has exited: it no longer exists, or every one of its threads has exited
// and only its parent has yet to reap it. In a container whose init does not reap, that can last for good.
func processGone(pid int) bool {
	return syscall.Kill(pid, 0) == syscall.ESRCH || threadsExited(procRoot, pid)
}

// procRoot is the /proc processGone reads; tests point it at a fake tree.
var procRoot = "/proc"

// threadsExited reports whether every thread of process pid is a zombie or dead. A leader that exited stays a zombie
// while another thread runs, and a thread stuck in an uninterruptible write still has that write to land. A /proc it
// cannot read counts as running.
func threadsExited(root string, pid int) bool {
	dir := filepath.Join(root, strconv.Itoa(pid), "task")
	tasks, err := os.ReadDir(dir)
	if err != nil || len(tasks) == 0 {
		return false
	}
	for _, task := range tasks {
		stat, err := os.ReadFile(filepath.Join(dir, task.Name(), "stat"))
		if err != nil {
			return false
		}
		// The state follows the command name, which is in parentheses and may itself hold spaces and parentheses.
		end := bytes.LastIndexByte(stat, ')')
		if end < 0 || end+2 >= len(stat) || (stat[end+2] != 'Z' && stat[end+2] != 'X') {
			return false
		}
	}
	return true
}

// stale reports whether a slot's node is gone: it is a mount process on this
// host that has exited, or this node has read the same heartbeat in the slot for
// longer than timeout, or for a dead slot the same recovery lease for longer than
// recoveryLease. Silence is timed on this node's clock, so node clocks need not
// agree. A slot this node has not watched yet is not stale. Called with r.mu held
// after observe.
func (r *Registry) stale(slotIdx int, slot *Slot, timeout time.Duration) bool {
	switch slot.State {
	case NodeStateActive, NodeStateLeaving:
		return r.deadLocalOwner(slot) || r.silentFor(slotIdx, slot) > timeout
	case NodeStateDead:
		return r.silentFor(slotIdx, slot) > recoveryLease
	}
	return false
}

// heartbeatOlderThan compares a heartbeat with this node's wall clock. Clocks
// differ and step, so it only picks slots to watch and never proves a node dead.
func heartbeatOlderThan(slot *Slot, d time.Duration) bool {
	return time.Since(time.Unix(slot.HeartbeatTime, 0)) > d
}

// LeaseExpiry returns when the slot's node stops counting as alive: its last
// heartbeat plus HeartbeatTimeout, or for a dead slot, the end of the recovery
// lease of the peer replaying its journal. An empty slot returns the zero time.
func (s *Slot) LeaseExpiry() time.Time {
	switch s.State {
	case NodeStateActive, NodeStateLeaving:
		return time.Unix(s.HeartbeatTime, 0).Add(HeartbeatTimeout)
	case NodeStateDead:
		return time.Unix(s.RecoveryStart, 0).Add(recoveryLease)
	}
	return time.Time{}
}

// observe records what this node read in a slot, and keeps it for Occupant so
// both judge the same read. Called with r.mu held.
func (r *Registry) observe(slotIdx int, slot *Slot) {
	r.remember(slotIdx, slot)
	next := sightingOf(slot)
	if last, seen := r.sightings[slotIdx]; seen && last.sinceless() == next {
		return
	}
	next.since = time.Now()
	r.sightings[slotIdx] = next
}

func (s sighting) sinceless() sighting {
	s.since = time.Time{}
	return s
}

// silentFor returns how long this node has read slot unchanged. Called with r.mu held after observe.
func (r *Registry) silentFor(slotIdx int, slot *Slot) time.Duration {
	last, seen := r.sightings[slotIdx]
	if !seen || last.sinceless() != sightingOf(slot) {
		return 0
	}
	return time.Since(last.since)
}

// Occupancy is how the node in a registry slot stands.
type Occupancy uint8

const (
	// OccupiedLive is a node with a current heartbeat.
	OccupiedLive Occupancy = iota
	// OccupiedFailing is a live node's slot whose heartbeat stopped.
	OccupiedFailing
	// OccupiedReplaying is a node marked dead whose journal is being replayed.
	OccupiedReplaying
	// OccupiedLapsed is a dead node whose replay let its recovery lease lapse: the replay failed or was abandoned.
	OccupiedLapsed
	// Unoccupied is an empty slot.
	Unoccupied
)

// Occupant returns the node in registry slot slotIdx and how it stands. With
// fresh set, or before failure detection has read the slot, it reads the slot
// from the device; otherwise it uses failure detection's last read. A slot it
// cannot read counts as live.
func (r *Registry) Occupant(slotIdx int, fresh bool) ([16]byte, Occupancy) {
	r.seenMu.Lock()
	slot, ok := r.seen[slotIdx]
	r.seenMu.Unlock()
	if fresh || !ok {
		read, err := r.readSlot(slotIdx)
		if err != nil {
			return [16]byte{}, OccupiedLive
		}
		slot = read
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observe(slotIdx, slot)
	switch {
	case slot.State == NodeStateEmpty:
		return slot.UUID, Unoccupied
	case slot.State == NodeStateDead && r.stale(slotIdx, slot, HeartbeatTimeout):
		return slot.UUID, OccupiedLapsed
	case slot.State == NodeStateDead:
		return slot.UUID, OccupiedReplaying
	case r.stale(slotIdx, slot, HeartbeatTimeout):
		return slot.UUID, OccupiedFailing
	}
	return slot.UUID, OccupiedLive
}

func (r *Registry) remember(slotIdx int, slot *Slot) {
	r.seenMu.Lock()
	defer r.seenMu.Unlock()
	r.seen[slotIdx] = slot
}
