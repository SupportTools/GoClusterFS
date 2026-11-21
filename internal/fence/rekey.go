package fence

import (
	"errors"
	"fmt"
	"time"
)

// RekeyWindow is the least time between two re-registers. Losing the key again sooner means the storage path keeps
// losing its session, and each loss leaves another stale registration on the target, so the node stops instead.
const RekeyWindow = 10 * time.Minute

// Rekeyer registers this node's key again after the device stopped knowing it, as when an iSCSI path under
// dm-multipath logged out and in again with a new session ID, but only when no peer can have taken this node over.
// Peers preempt a node only after its heartbeat stayed unchanged for longer than StallLimit, and every check below
// runs with the device's writes held, after the register returned. So a register that passes its checks happened
// before any peer's preempt, which removes it with every other registration of the key.
type Rekeyer struct {
	PR *PR
	// HoldWrites runs fn while no write is in flight and none can start; fn may read the device.
	HoldWrites func(fn func() error) error
	// HeartbeatAge is how long ago this node issued its last heartbeat that landed in time, on CLOCK_BOOTTIME.
	HeartbeatAge func() time.Duration
	// SlotLive returns nil while this node's registry slot is its own and not Dead.
	SlotLive func() error
	// StallLimit is the oldest heartbeat age at which this node still writes.
	StallLimit time.Duration
	// Restored, if set, runs after a re-register with the heartbeat age the last check read.
	Restored func(age time.Duration)
	// Listed, if set, runs with what the device listed around each register attempt.
	Listed func(KeyListing)
	// NoKeyCheck and BeforeRegister exist only for the B key-check POC: one skips the storage proof to compare with
	// the clock and slot checks alone, the other lets a test preempt between the listing and the register.
	NoKeyCheck     bool
	BeforeRegister func()

	last time.Time
	// Tests stall here, as a pause would.
	beforeRegister, afterRegister func()
}

// KeyListing is what the device listed of this node's key around a re-register: its registrations and the
// reservation generation before and after, and the paths the register went out on.
type KeyListing struct {
	Paths                   int
	Before, After           int
	GenBefore, GenAfter     uint32
	Registered, AfterListed bool
}

// Rekey restores this node's key after a write met a reservation conflict. Unless every check passes, it leaves this
// host unregistered and returns why.
func (k *Rekeyer) Rekey() error {
	if !k.last.IsZero() && time.Since(k.last) < RekeyWindow {
		return fmt.Errorf("the device lost this node's key again %v after the last re-register; a path that keeps losing its session needs a remount",
			time.Since(k.last).Round(time.Second))
	}
	return k.HoldWrites(func() error {
		key := k.PR.Key()
		switch err := k.PR.dev.register(key, key, false); {
		case err == nil:
			return errors.New("this host still holds its key, so the conflict has another cause")
		case !errors.Is(err, ErrReservationConflict):
			return fmt.Errorf("checking for this node's key: %w", err)
		}
		// A peer takes a node over with a preempt, which removes every registration of the node's key, the ones a lost
		// session left on the target included. Only those prove that no preempt came: the clock may have missed a
		// pause, and the slot is marked Dead only after the preempt.
		var l KeyListing
		if k.Listed != nil {
			defer func() { k.Listed(l) }()
		}
		paths, err := k.PR.Paths()
		if err != nil {
			return fmt.Errorf("counting the device's paths: %w", err)
		}
		l.Paths = paths
		if k.NoKeyCheck {
			l.Before = 1
		} else if l.GenBefore, l.Before, err = k.PR.Registrations(); err != nil {
			return fmt.Errorf("listing the device's keys, which proves no peer preempted this node: %w", err)
		}
		if l.Before == 0 {
			return fmt.Errorf("the device lists no registration of key %#x, so a peer preempted this node", key)
		}
		if err := k.live(); err != nil {
			return err
		}
		if k.beforeRegister != nil {
			k.beforeRegister()
		}
		if k.BeforeRegister != nil {
			k.BeforeRegister()
		}
		if err := k.PR.Register(); err != nil {
			return fmt.Errorf("registering the key again: %w", err)
		}
		l.Registered = true
		if k.afterRegister != nil {
			k.afterRegister()
		}
		age := k.HeartbeatAge()
		if !k.NoKeyCheck {
			l.GenAfter, l.After, err = k.PR.Registrations()
			l.AfterListed = err == nil
		}
		if err == nil && !k.NoKeyCheck && l.After <= paths {
			err = fmt.Errorf("after the register the device lists %d registrations of key %#x on %d paths, so the ones from before the loss are gone: a peer preempted this node",
				l.After, key, paths)
		}
		if err == nil {
			err = k.live()
		}
		if err == nil {
			err = k.PR.Reserve()
		}
		if err != nil {
			if uerr := k.PR.Unregister(); uerr != nil && !errors.Is(uerr, ErrFenced) {
				err = fmt.Errorf("%w; removing the key again failed: %v", err, uerr)
			}
			return err
		}
		k.last = time.Now()
		if k.Restored != nil {
			k.Restored(age)
		}
		return nil
	})
}

// live checks that no peer can have taken this node over: its heartbeat is recent and its slot is its own and live.
func (k *Rekeyer) live() error {
	if age := k.HeartbeatAge(); age > k.StallLimit {
		return fmt.Errorf("the last heartbeat was %v ago, past the %v after which a peer may take this node over", age.Round(time.Millisecond), k.StallLimit)
	}
	if err := k.SlotLive(); err != nil {
		return fmt.Errorf("this node's registry slot: %w", err)
	}
	return nil
}
