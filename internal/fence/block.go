package fence

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// Block fences through the block layer's persistent reservation ioctls on f, which SCSI and NVMe disks implement.
func Block(f *os.File, key uint64) *PR { return &PR{dev: blockDevice{f}, key: key} }

type blockDevice struct{ f *os.File }

const (
	typeWriteExclusiveAllRegs = 5
	flagIgnoreKey             = 1
	// Conflicts come back as the ioctl's return value: SCSI RESERVATION CONFLICT, which PR_STS_RESERVATION_CONFLICT
	// keeps, or NVMe's Reservation Conflict status from kernels before PR_STS existed.
	statusConflict     = 0x18
	nvmeStatusConflict = 0x83
)

var (
	iocRegister     = ioc(1, 200, unsafe.Sizeof(registration{}))
	iocReserve      = ioc(1, 201, unsafe.Sizeof(reservation{}))
	iocPreemptAbort = ioc(1, 204, unsafe.Sizeof(preemption{}))
)

func ioc(dir, nr, size uintptr) uintptr { return dir<<30 | size<<16 | 'p'<<8 | nr }

type registration struct {
	old, new   uint64
	flags, pad uint32
}

type reservation struct {
	key        uint64
	typ, flags uint32
}

type preemption struct {
	own, victim uint64
	typ, flags  uint32
}

func (d blockDevice) ioctl(req uintptr, arg unsafe.Pointer) error {
	r, _, errno := syscall.Syscall(syscall.SYS_IOCTL, d.f.Fd(), req, uintptr(arg))
	return ioctlResult(r, errno)
}

// ioctlResult maps a reservation ioctl's outcome: a positive return is the device's status, and any status but a
// conflict is an error that stops the caller.
func ioctlResult(r uintptr, errno syscall.Errno) error {
	switch {
	case errno == syscall.ENOTTY || errno == syscall.EOPNOTSUPP:
		return fmt.Errorf("%w (%v)", ErrUnsupported, errno)
	case errno != 0:
		return errno
	case r == statusConflict || r == nvmeStatusConflict:
		return ErrReservationConflict
	case r != 0:
		return fmt.Errorf("persistent reservation command failed with status %#x", r)
	}
	return nil
}

func (d blockDevice) register(old, new uint64, ignore bool) error {
	r := registration{old: old, new: new}
	if ignore {
		r.flags = flagIgnoreKey
	}
	return d.ioctl(iocRegister, unsafe.Pointer(&r))
}

func (d blockDevice) reserve(key uint64) error {
	r := reservation{key: key, typ: typeWriteExclusiveAllRegs}
	return d.ioctl(iocReserve, unsafe.Pointer(&r))
}

func (d blockDevice) preemptAbort(own, victim uint64) error {
	p := preemption{own: own, victim: victim, typ: typeWriteExclusiveAllRegs}
	return d.ioctl(iocPreemptAbort, unsafe.Pointer(&p))
}
