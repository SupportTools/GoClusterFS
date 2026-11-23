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
// Two checks must pass, before and after the register, with the device's writes held:
//   - the storage: a peer takes a node over with a preempt, which removes every registration of the node's key,
//     those of lost sessions included. After the register, the device must list more registrations than this rekey
//     made, so one from before survived and no preempt came, whatever this node's clock says;
//   - the slot: a peer marks its intent in the node's slot before it preempts, so the slot read after the register
//     must name no peer, no Dead mark and no other owner.
//
// The heartbeat age is checked too, but a hypervisor pause can hide time from it.
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
	// Refused, if set, runs with writes still held when Rekey fails, so the node can stop before a held write lands
	// through a registration that a failed unregister left behind.
	Refused func(err error)

	last time.Time
	// Tests stall here, as a pause would.
	beforeRegister, afterRegister func()
}

// Rekey restores this node's key after a write met a reservation conflict. Unless every check passes, it leaves this
// host unregistered and returns why.
func (k *Rekeyer) Rekey() error {
	if !k.last.IsZero() && time.Since(k.last) < RekeyWindow {
		return fmt.Errorf("the device lost this node's key again %v after the last re-register; a path that keeps losing its session needs a remount",
			time.Since(k.last).Round(time.Second))
	}
	return k.HoldWrites(func() error {
		err := k.rekey()
		if err != nil && k.Refused != nil {
			k.Refused(err)
		}
		return err
	})
}

func (k *Rekeyer) rekey() error {
	key := k.PR.Key()
	paths, closePaths, err := k.PR.Paths()
	if err != nil {
		return fmt.Errorf("finding the device's paths: %w", err)
	}
	defer closePaths()
	lost, err := lostPaths(paths, key)
	if err != nil {
		return err
	}
	if len(lost) == 0 {
		return errors.New("every path still holds this node's key, so the conflict has another cause")
	}
	if err := errors.Join(k.listedBefore(), k.live()); err != nil {
		return err
	}
	if k.beforeRegister != nil {
		k.beforeRegister()
	}
	failpointStall("before-register")
	var added []*PR
	for _, path := range lost {
		if err := path.Register(); err != nil {
			return unregister(fmt.Errorf("registering the key again: %w", err), added)
		}
		added = append(added, path)
	}
	if k.afterRegister != nil {
		k.afterRegister()
	}
	failpointStall("after-register")
	age := k.HeartbeatAge()
	err = errors.Join(k.listedAfter(len(added)), k.live())
	if err == nil {
		err = k.PR.Reserve()
	}
	if err != nil {
		return unregister(err, added)
	}
	k.last = time.Now()
	if k.Restored != nil {
		k.Restored(age)
	}
	return nil
}

// lostPaths probes each path for key and returns the ones that no longer hold it. A path that holds the key takes the
// probe as a register of the same key and changes nothing; a dm map would instead drop the key from every path if one
// refused.
func lostPaths(paths []*PR, key uint64) ([]*PR, error) {
	var lost []*PR
	for _, path := range paths {
		switch err := path.dev.register(key, key, false); {
		case errors.Is(err, ErrReservationConflict):
			lost = append(lost, path)
		case err != nil:
			return nil, fmt.Errorf("checking a path for this node's key: %w", err)
		}
	}
	return lost, nil
}

// unregister removes the key from the paths a failed rekey registered, and adds what went wrong doing it to err.
func unregister(err error, added []*PR) error {
	for _, path := range added {
		if uerr := path.Unregister(); uerr != nil && !errors.Is(uerr, ErrFenced) {
			err = fmt.Errorf("%w; removing the key again failed: %v", err, uerr)
		}
	}
	return err
}

// listedBefore checks that the device still lists a registration of this node's key from before the loss. A peer
// takes a node over with a preempt, which removes every registration of its key, the ones of sessions that are gone
// included.
func (k *Rekeyer) listedBefore() error {
	n, err := k.PR.Registrations()
	switch {
	case err != nil:
		return fmt.Errorf("listing the device's keys, which shows whether a peer preempted this node: %w", err)
	case n == 0:
		return fmt.Errorf("the device lists no registration of key %#x, so a peer preempted this node", k.PR.Key())
	}
	return nil
}

// listedAfter checks that the device lists more registrations of this node's key than the added ones this rekey
// made: one from before survived, so no preempt came between the listings, whatever this node's clock says.
func (k *Rekeyer) listedAfter(added int) error {
	n, err := k.PR.Registrations()
	switch {
	case err != nil:
		return fmt.Errorf("listing the device's keys after the register: %w", err)
	case n <= added:
		return fmt.Errorf("after registering %d paths the device lists %d registrations of key %#x, so the ones from before are gone: a peer preempted this node",
			added, n, k.PR.Key())
	}
	return nil
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
