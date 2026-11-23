// Package fence revokes a dead node's write access at the storage with persistent reservations, so writes the node
// still has queued fail once another node takes over its locks and journal.
package fence

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"syscall"
	"time"
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
	errKeysUnlisted        = errors.New("this kernel or device cannot list the registered keys")
	// ErrNoOtherRegistrant means no other node's key is registered, so dropping this node's could release the reservation.
	ErrNoOtherRegistrant = errors.New("no other node's key is registered, so dropping this node's could release the reservation")
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
	// Leave is Unregister for a node that gave up its slot, which also removes the key an earlier iSCSI session of this
	// host keeps; earlier reports that this session held no key.
	Leave() (earlier bool, err error)
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

// IsToolKey reports whether key is one ToolKey makes, which no node holds.
func IsToolKey(key uint64) bool { return key>>48 == toolPrefix }

// None is the Fencer of a volume without persistent reservations.
type None struct{}

func (None) Kind() string         { return "none" }
func (None) Register() error      { return nil }
func (None) Reserve() error       { return nil }
func (None) Preempt(uint64) error { return nil }
func (None) Unregister() error    { return nil }
func (None) Leave() (bool, error) { return false, nil }

type device interface {
	register(old, new uint64, ignore bool) error
	reserve(key uint64) error
	preemptAbort(own, victim uint64) error
	preempt(own, victim uint64) error
	clear(key uint64) error
	readKeys() ([]uint64, error)
	readReservation() (key uint64, typ uint32, err error)
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

// RegisterFresh drops this session's registration and registers the key anew, then reserves. LIO lets an initiator
// write only while a per-initiator flag is set: each new registration sets it, and removing any registration of the
// initiator clears it, also one an earlier session of this host made. Call it only after the device refused a write
// or reserve of this node. It drops the registration only while another node's key is registered, so LIO hands the
// reservation to that node: over dm-multipath with several paths into one LIO target portal group, the refusal can
// come from this node's own other path holding the reservation, and dropping every path's registration with no other
// registrant would release the reservation. Otherwise it returns ErrNoOtherRegistrant, or why it cannot list keys.
func (p *PR) RegisterFresh() error {
	keys, err := p.Keys()
	if errors.Is(err, ErrUnsupported) {
		return fmt.Errorf("%w (%s), so it cannot check that another node stays registered", errKeysUnlisted, cause(err))
	} else if err != nil {
		return fmt.Errorf("listing the registered keys: %w", err)
	}
	if !slices.ContainsFunc(keys, func(k uint64) bool { return k != p.key }) {
		return ErrNoOtherRegistrant
	}
	if err := p.Register(); err != nil {
		return err
	}
	if err := p.dev.register(p.key, 0, false); err != nil {
		return err
	}
	if err := p.dev.register(0, p.key, false); err != nil {
		return err
	}
	return p.Reserve()
}

// Join registers this node's key and reserves, and reports whether it registered the key anew. LIO refuses a reserve,
// as it refuses a write, from a host whose write permission a revoke of its key under an earlier iSCSI session
// cleared, so after such a refusal Join tries RegisterFresh once.
func (p *PR) Join() (anew bool, err error) {
	if err := p.Register(); err != nil {
		return false, err
	}
	err = p.Reserve()
	if !errors.Is(err, ErrForeignReservation) {
		return false, err
	}
	if ferr := p.RegisterFresh(); ferr != nil {
		return false, fmt.Errorf("%w; not registering anew: %v", err, ferr)
	}
	return true, nil
}

func (p *PR) Reserve() error {
	if err := p.dev.reserve(p.key); errors.Is(err, ErrReservationConflict) {
		return ErrForeignReservation
	} else if err != nil {
		return err
	}
	return nil
}

// Preempt removes victim's registration and aborts its commands. A device without a reservation takes writes from any
// host, so a preempt there would leave the victim writing: Preempt reserves first when the device holds none.
func (p *PR) Preempt(victim uint64) error {
	if victim == 0 || victim == p.key {
		return fmt.Errorf("refusing to preempt key %#x: key 0 would remove every other node, and %#x is this node's", victim, p.key)
	}
	if err := p.reserveIfNone(); err != nil {
		if errors.Is(err, ErrForeignReservation) && errors.Is(p.dev.register(p.key, p.key, false), ErrReservationConflict) {
			return ErrFenced
		}
		return fmt.Errorf("reserving the device before revoking key %#x: %w", victim, err)
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

// reserveIfNone reserves the device unless it holds a reservation, or reserves anyway where the reservation cannot be read.
func (p *PR) reserveIfNone() error {
	if _, typ, err := p.dev.readReservation(); err == nil && typ != 0 {
		return nil
	} else if err != nil && !errors.Is(err, ErrUnsupported) {
		return err
	}
	return p.Reserve()
}

// KeepReserved reserves the device again if it holds no reservation while this node's key is registered, and reports
// whether it did. It returns ErrFenced if this node's key is gone too: the node cannot tell a storage reset from its
// own fencing. A device-mapper map answers EAGAIN while it is suspended, as when multipath reloads its table after a
// path logs out and in, so KeepReserved tries again a few times before it returns EAGAIN.
func (p *PR) KeepReserved() (bool, error) {
	for try := 1; ; try++ {
		reserved, err := p.keepReserved()
		if !errors.Is(err, syscall.EAGAIN) || try == busyTries {
			return reserved, err
		}
		time.Sleep(busyPause)
	}
}

const (
	busyTries = 3
	busyPause = 250 * time.Millisecond
)

func (p *PR) keepReserved() (bool, error) {
	if _, typ, err := p.dev.readReservation(); err != nil || typ != 0 {
		return false, err
	}
	keys, err := p.Keys()
	if err != nil {
		return false, err
	}
	if !slices.Contains(keys, p.key) {
		return false, ErrFenced
	}
	return true, p.Reserve()
}

// Keys lists the keys registered on the device. ErrUnsupported means the kernel or the device cannot list them.
func (p *PR) Keys() ([]uint64, error) { return p.dev.readKeys() }

// Clear removes every initiator's key, p's too, and with them the reservation. Only a registered key may clear, so it
// registers p's key first.
func (p *PR) Clear() error {
	if err := p.Register(); err != nil {
		return err
	}
	return p.dev.clear(p.key)
}

func (p *PR) Unregister() error {
	if err := p.dev.register(p.key, 0, false); errors.Is(err, ErrReservationConflict) {
		return ErrFenced
	} else if err != nil {
		return err
	}
	return nil
}

// Leave unregisters this node's key, also where an earlier iSCSI session of this host keeps it. On LIO no peer revokes
// such a registration, and while it holds the reservation every session of this host writes past a revoke of a later key.
func (p *PR) Leave() (earlier bool, err error) {
	if err := p.Unregister(); !errors.Is(err, ErrFenced) {
		return false, err
	}
	if err := p.Register(); err != nil {
		return true, err
	}
	// Preempting its own key without abort removes the earlier registration and takes the reservation if that one held it.
	if err := p.dev.preempt(p.key, p.key); err != nil {
		return true, err
	}
	// Over dm-multipath the preempt also removed the key from this host's other paths, and dm stops an unregister at the
	// first path without the key, so the key goes back on every path first.
	if err := p.Register(); err != nil {
		return true, err
	}
	if err := p.dev.register(p.key, 0, false); err != nil {
		return true, fmt.Errorf("unregistering the key after removing it from the earlier session: %v", err)
	}
	return true, nil
}
