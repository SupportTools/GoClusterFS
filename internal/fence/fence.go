// Package fence revokes a dead node's write access at the storage with persistent reservations, so writes the node
// still has queued fail once another node takes over its locks and journal.
package fence

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	ErrUnsupported = errors.New("the device does not support persistent reservations")
	// ErrPartition is ErrUnsupported for a partition: a reservation covers the whole disk, and Linux 6.5 and later refuse
	// it on a partition.
	ErrPartition = fmt.Errorf("%w on a partition, only on the whole disk", ErrUnsupported)
	// ErrFenced means another node removed this node's key: the storage refuses its writes.
	ErrFenced             = errors.New("this node's reservation key is gone: another node revoked its access to the device")
	ErrForeignReservation = errors.New("the device holds a reservation of another type or initiator")
	// ErrReservationConflict is a reservation command the device refused, from SCSI and NVMe alike.
	ErrReservationConflict = errors.New("reservation conflict")
)

// Fencer registers this node's key on the device and revokes the keys of nodes proven dead.
type Fencer interface {
	// Kind is "pr" for persistent reservations and "none" for a volume without fencing.
	Kind() string
	// Register registers this node's key, replacing any key this host registered before.
	Register() error
	// Reserve makes the device refuse writes from initiators without a registered key.
	Reserve() error
	// Preempt removes the registration of key victim and aborts its queued commands. A victim already gone is not an
	// error; ErrFenced means this node's own key is gone.
	Preempt(victim uint64) error
	// Unregister removes this node's key; once no key is left, the reservation goes too.
	Unregister() error
}

const (
	nodePrefix = 0x4743 // "GC"
	toolPrefix = 0x4746 // "GF"
)

// NodeKey is the reservation key of the node whose registry UUID is uuid, so peers can derive it from its slot.
func NodeKey(uuid [16]byte) uint64 {
	var b [8]byte
	binary.BigEndian.PutUint16(b[:], nodePrefix)
	copy(b[2:], uuid[:6])
	return binary.BigEndian.Uint64(b[:])
}

// ToolKey is a random key for format and fsck, which hold no registry slot.
func ToolKey() uint64 {
	var b [8]byte
	binary.BigEndian.PutUint16(b[:], toolPrefix)
	_, _ = rand.Read(b[2:])
	return binary.BigEndian.Uint64(b[:])
}

// None is the Fencer of a volume without persistent reservations.
type None struct{}

func (None) Kind() string         { return "none" }
func (None) Register() error      { return nil }
func (None) Reserve() error       { return nil }
func (None) Preempt(uint64) error { return nil }
func (None) Unregister() error    { return nil }

type device interface {
	register(old, new uint64, ignore bool) error
	reserve(key uint64) error
	preemptAbort(own, victim uint64) error
}

// PR fences with a Write Exclusive, All Registrants reservation: every registered node may write, and no other
// initiator may, while any node stays registered.
type PR struct {
	dev device
	key uint64
}

func (p *PR) Kind() string { return "pr" }

// Key is this node's reservation key.
func (p *PR) Key() uint64 { return p.key }

func (p *PR) Register() error { return p.dev.register(0, p.key, true) }

func (p *PR) Reserve() error {
	if err := p.dev.reserve(p.key); errors.Is(err, ErrReservationConflict) {
		return ErrForeignReservation
	} else if err != nil {
		return err
	}
	return nil
}

func (p *PR) Preempt(victim uint64) error {
	if victim == 0 || victim == p.key {
		return fmt.Errorf("refusing to preempt key %#x: key 0 would remove every other node, and %#x is this node's", victim, p.key)
	}
	err := p.dev.preemptAbort(p.key, victim)
	if !errors.Is(err, ErrReservationConflict) {
		return err
	}
	// A conflict means the victim is gone already or this node's key is: only a registered key can register itself again.
	if err := p.dev.register(p.key, p.key, false); errors.Is(err, ErrReservationConflict) {
		return ErrFenced
	} else if err != nil {
		return err
	}
	return nil
}

func (p *PR) Unregister() error {
	if err := p.dev.register(p.key, 0, false); errors.Is(err, ErrReservationConflict) {
		return ErrFenced
	} else if err != nil {
		return err
	}
	return nil
}
