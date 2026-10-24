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

var processStart = time.Now().UnixNano()

func readBootID() (id [16]byte) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err == nil {
		hex.Decode(id[:], bytes.ReplaceAll(bytes.TrimSpace(b), []byte("-"), nil))
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
