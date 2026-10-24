package node

import (
	"bytes"
	"encoding/hex"
	"os"
	"syscall"
	"time"
)

// recoveryLease is how long a peer may hold a dead slot's recovery before others take it over.
const recoveryLease = 60 * time.Second

// deathMargin is added to HeartbeatTimeout before this node's own clock counts
// an unchanged heartbeat as proof that its node is dead rather than slow.
const deathMargin = HeartbeatTimeout / 2

// sighting is what failure detection last saw in a slot, timed by this node's monotonic clock.
type sighting struct {
	uuid      [16]byte
	seq       uint64
	beat      int64
	since     time.Time
	changedAt time.Time
}

var processStart = time.Now().UnixNano()

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

func hostnameOf(slot *Slot) string {
	return string(bytes.TrimRight(slot.Hostname[:], "\x00"))
}

// sameHost reports whether slot was registered under this node's hostname and kernel boot.
func (r *Registry) sameHost(slot *Slot) bool {
	return r.bootID != ([16]byte{}) && slot.BootID == r.bootID && hostnameOf(slot) == r.hostname
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
	return syscall.Kill(int(slot.PID), 0) == syscall.ESRCH
}

// failed reports whether a slot's node is gone: its heartbeat is older than timeout, it is a
// mount process on this host that has exited, or the peer replaying its journal let the lease lapse.
func (r *Registry) failed(slot *Slot, timeout time.Duration) bool {
	switch slot.State {
	case NodeStateActive, NodeStateLeaving:
		return time.Since(time.Unix(slot.HeartbeatTime, 0)) > timeout || r.deadLocalOwner(slot)
	case NodeStateDead:
		return time.Since(time.Unix(slot.RecoveryStart, 0)) > recoveryLease
	}
	return false
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

// observe records a slot's heartbeat as failure detection sees it. Called with r.mu held.
func (r *Registry) observe(slotIdx int, slot *Slot) {
	last, seen := r.sightings[slotIdx]
	if seen && last.uuid == slot.UUID && last.seq == slot.HeartbeatSeq && last.beat == slot.HeartbeatTime {
		return
	}
	now := time.Now()
	next := sighting{uuid: slot.UUID, seq: slot.HeartbeatSeq, beat: slot.HeartbeatTime, since: now}
	if seen && last.uuid == slot.UUID {
		next.changedAt = now
	}
	r.sightings[slotIdx] = next
}

// recentlyAlive reports whether this node saw the slot's heartbeat advance within
// HeartbeatTimeout, whatever the heartbeat's wall-clock time says. Called with r.mu held.
func (r *Registry) recentlyAlive(slotIdx int) bool {
	last, seen := r.sightings[slotIdx]
	return seen && !last.changedAt.IsZero() && time.Since(last.changedAt) < HeartbeatTimeout
}

// provenDead reports whether a slot's node is dead, not merely slow: its mount
// process is gone, or this node has seen the same heartbeat for longer than
// HeartbeatTimeout plus deathMargin. Called with r.mu held.
func (r *Registry) provenDead(slotIdx int, slot *Slot) bool {
	if r.deadLocalOwner(slot) {
		return true
	}
	last, seen := r.sightings[slotIdx]
	return seen && last.uuid == slot.UUID && time.Since(last.since) >= HeartbeatTimeout+deathMargin
}
