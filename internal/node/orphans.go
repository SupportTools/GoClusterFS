package node

import (
	"context"
	"slices"
)

// SetOrphanHandler sets fn, which the orphan loop calls with each live slot's orphan list and a context that ends at Stop.
func (r *Registry) SetOrphanHandler(fn func(ctx context.Context, slot int, orphans []Orphan)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onOrphans = fn
}

// KickOrphans makes the orphan loop pass every orphan list to the handler soon; it never blocks.
func (r *Registry) KickOrphans() {
	select {
	case r.orphanKick <- struct{}{}:
	default:
	}
}

// orphanLoop runs apart from failure detection: the handler may wait for a
// failed node's inode lock, which only that node's replay frees.
func (r *Registry) orphanLoop() {
	defer r.heartbeatWg.Done()
	stop := r.stopCh
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-stop
		cancel()
	}()
	for {
		select {
		case <-stop:
			return
		case <-r.orphanKick:
			if slots, err := r.readAllSlots(); err == nil {
				r.passOrphans(ctx, slots)
			}
		}
	}
}

// passOrphans hands the handler this node's orphan list and each live peer's.
// A failed peer's list waits for its replay, which adopts it.
func (r *Registry) passOrphans(ctx context.Context, slots []*Slot) {
	r.mu.Lock()
	fn, local := r.onOrphans, r.localSlot
	live := make([]bool, len(slots))
	for i, s := range slots {
		if i != local && s != nil && s.State != NodeStateDead {
			r.observe(i, s)
			live[i] = !r.stale(i, s, HeartbeatTimeout)
		}
	}
	r.mu.Unlock()
	if fn == nil {
		return
	}
	if local >= 0 && slots[local] != nil {
		fn(ctx, local, slots[local].Orphans)
	}
	for i, s := range slots {
		if live[i] && len(s.Orphans) > 0 {
			fn(ctx, i, s.Orphans)
		}
	}
}

// PeersFailing reports whether another slot's node stopped heartbeating or awaits the end of its replay.
func (r *Registry) PeersFailing() bool {
	local := r.LocalSlot()
	for i := range MaxNodes {
		if _, occ := r.Occupant(i, false); i != local && occ != OccupiedLive && occ != Unoccupied {
			return true
		}
	}
	return false
}

func (r *Registry) AddOrphan(o Orphan) error {
	return r.editOrphans(func(list []Orphan) ([]Orphan, error) {
		switch {
		case slices.Contains(list, o):
			return list, nil
		case len(list) >= MaxOrphans:
			return list, ErrOrphansFull
		}
		return append(list, o), nil
	})
}

func (r *Registry) DropOrphans(done func(Orphan) bool) error {
	return r.editOrphans(func(list []Orphan) ([]Orphan, error) {
		return slices.DeleteFunc(list, done), nil
	})
}

func (r *Registry) editOrphans(edit func([]Orphan) ([]Orphan, error)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.localSlot < 0 {
		return ErrNotRegistered
	}
	slot, err := r.readSlot(r.localSlot)
	if err != nil {
		return err
	}
	if slot.UUID != r.localUUID {
		return ErrSlotLost
	}
	list, err := edit(slices.Clone(slot.Orphans))
	if err != nil || slices.Equal(list, slot.Orphans) {
		return err
	}
	slot.Orphans = list
	slot.Addr = r.advertise
	return r.writeSlot(r.localSlot, slot)
}

func (r *Registry) AllOrphans() ([]Orphan, error) {
	slots, err := r.readAllSlots()
	if err != nil {
		return nil, err
	}
	var all []Orphan
	for _, s := range slots {
		if s != nil && s.State != NodeStateDead {
			all = append(all, s.Orphans...)
		}
	}
	return all, nil
}

// LiveSlots reads the registry from the device, so a node that just joined counts; an unreadable slot counts as live.
func (r *Registry) LiveSlots() (uint64, error) {
	slots, err := r.readAllSlots()
	if err != nil {
		return 0, err
	}
	var live uint64
	for i, s := range slots {
		if s == nil || s.Magic == SlotMagic && (s.State == NodeStateActive || s.State == NodeStateLeaving) {
			live |= 1 << i
		}
	}
	return live, nil
}
