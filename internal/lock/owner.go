package lock

import (
	"context"
	"fmt"
)

// OwnerState is what the node registry says about the node in a registry slot.
type OwnerState uint8

const (
	// OwnerAlive is a registered node with a current heartbeat.
	OwnerAlive OwnerState = iota
	// OwnerFailing is a live node's slot whose heartbeat stopped.
	OwnerFailing
	// OwnerRecovering is a node marked dead whose journal is being replayed.
	OwnerRecovering
	// OwnerLost is a node whose journal replay failed or was abandoned.
	OwnerLost
	// OwnerReleased is an empty registry slot: its node left, or its replay finished.
	OwnerReleased
)

// OwnerView reports which node holds registry slot slot and its state. With
// fresh set it reads the registry from the device; otherwise it may answer from
// a recent read.
type OwnerView func(slot int, fresh bool) (occupant [16]byte, state OwnerState)

// SetOwnerView makes a claim in another node's slot free only once that
// registry slot is empty, and lets a wait for a failing node's claim run
// without a time limit until the node's replay ends. Without a view, claims free
// once they are older than StaleThreshold.
func (m *Manager) SetOwnerView(view OwnerView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ownerView = view
}

// blocker is what kept an attempt from winning: the state of the node behind the
// claim in its way, and that node. The zero value is a live holder.
type blocker struct {
	state    OwnerState
	occupant [16]byte
	slot     int
}

// failing reports whether the node behind the claim is failing or being
// recovered, so a wait for it has no time limit.
func (b blocker) failing() bool {
	return b.state == OwnerFailing || b.state == OwnerRecovering
}

// worse returns the blocker that limits a wait more: a lost owner ends it, a
// live one counts toward the timeout, and a failing one does not.
func worse(a, b blocker) blocker {
	rank := func(s OwnerState) int {
		switch s {
		case OwnerLost:
			return 2
		case OwnerAlive:
			return 1
		}
		return 0
	}
	if rank(b.state) > rank(a.state) {
		return b
	}
	return a
}

// slotState judges a claim in slot, or a torn one (s nil), by the node now in
// the registry slot of the same index, whoever wrote the claim. The claim is
// free only once that registry slot reads empty from the device after the row.
// An adopted claim of a live node means a replay is running.
func (m *Manager) slotState(view OwnerView, slot int, s *claimSlot) blocker {
	occupant, state := view(slot, false)
	if state == OwnerReleased || state == OwnerLost {
		occupant, state = view(slot, true)
	}
	if state == OwnerAlive && s != nil && s.adopted {
		state = OwnerFailing
	}
	return blocker{state: state, occupant: occupant, slot: slot}
}

type replayKey struct{}

// ForReplay marks ctx as a journal replay's. Its lock waits have no time limit,
// so a peer that holds a lock the replay needs gives up first.
func ForReplay(ctx context.Context) context.Context {
	return context.WithValue(ctx, replayKey{}, true)
}

func isReplay(ctx context.Context) bool {
	replay, _ := ctx.Value(replayKey{}).(bool)
	return replay
}

// ErrReplayBlocked is the ErrLockTimeout of a goroutine that holds other locks
// and waited for a failing node's lock: that node's replay may need a lock it
// holds. A caller that can release its locks and retry should do so.
var ErrReplayBlocked = fmt.Errorf("%w: other locks held while a failing node holds this one", ErrLockTimeout)

type retryKey struct{}

// Retryable marks ctx as a caller's that releases its locks and retries on
// ErrReplayBlocked, so such a wait returns it at once instead of after
// DefaultTimeout, and the replay need not wait for the caller.
func Retryable(ctx context.Context) context.Context {
	return context.WithValue(ctx, retryKey{}, true)
}

func isRetryable(ctx context.Context) bool {
	retryable, _ := ctx.Value(retryKey{}).(bool)
	return retryable
}
