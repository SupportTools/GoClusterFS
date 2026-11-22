package fence

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Block fences through the block layer's persistent reservation ioctls on f, which SCSI and NVMe disks implement.
func Block(f *os.File, key uint64) *PR {
	return &PR{dev: retrying{device: blockDevice{f: f, partition: isPartition(f)}}, key: key}
}

// retrying sends a reservation command once more after status 0x2, and calls retried if set. A host whose key another
// host preempted gets a unit attention on its first command, which the kernel reports as 0x2; sent again, the command
// gets the device's real answer, such as a conflict. A clear is sent once: a repeat after one that worked would report
// a conflict. An unregister whose first answer was lost reads its repeat's conflict as ErrFenced, which unmount only logs.
type retrying struct {
	device
	retried func()
}

func (d retrying) register(old, new uint64, ignore bool) error {
	return d.again(func() error { return d.device.register(old, new, ignore) })
}

func (d retrying) reserve(key uint64) error {
	return d.again(func() error { return d.device.reserve(key) })
}

func (d retrying) preemptAbort(own, victim uint64) error {
	return d.again(func() error { return d.device.preemptAbort(own, victim) })
}

func (d retrying) preempt(own, victim uint64) error {
	return d.again(func() error { return d.device.preempt(own, victim) })
}

func (d retrying) readKeys() (keys []uint64, err error) {
	err = d.again(func() error {
		keys, err = d.device.readKeys()
		return err
	})
	return keys, err
}

func (d retrying) readReservation() (key uint64, typ uint32, err error) {
	err = d.again(func() error {
		key, typ, err = d.device.readReservation()
		return err
	})
	return key, typ, err
}

func (d retrying) again(command func() error) error {
	if err := command(); !errors.Is(err, statusError(statusIOError)) {
		return err
	}
	if d.retried != nil {
		d.retried()
	}
	return command()
}

type blockDevice struct {
	f         *os.File
	partition bool
}

// isPartition reports whether f is a block device that sysfs lists as a partition.
func isPartition(f *os.File) bool {
	var st unix.Stat_t
	if unix.Fstat(int(f.Fd()), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFBLK {
		return false
	}
	return partitionIn("/sys/dev/block", st.Rdev)
}

func partitionIn(sysDevBlock string, rdev uint64) bool {
	_, err := os.Stat(filepath.Join(sysDevBlock, fmt.Sprintf("%d:%d", unix.Major(rdev), unix.Minor(rdev)), "partition"))
	return err == nil
}

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
	iocPreempt      = ioc(1, 203, unsafe.Sizeof(preemption{}))
	iocPreemptAbort = ioc(1, 204, unsafe.Sizeof(preemption{}))
	iocClear        = ioc(1, 205, unsafe.Sizeof(clearing{}))
	iocReadKeys     = ioc(3, 206, unsafe.Sizeof(keyList{}))
	iocReadResv     = ioc(2, 207, unsafe.Sizeof(reservationState{}))
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

type clearing struct {
	key        uint64
	flags, pad uint32
}

type keyList struct {
	generation, count uint32
	keys              uint64
}

type reservationState struct {
	key             uint64
	generation, typ uint32
}

// maxListedKeys bounds a key listing: a volume has at most 64 nodes, plus fsck's and format's keys.
const maxListedKeys = 256

func (d blockDevice) ioctl(req uintptr, arg unsafe.Pointer) error {
	if d.partition {
		return ErrPartition
	}
	r, _, errno := syscall.Syscall(syscall.SYS_IOCTL, d.f.Fd(), req, uintptr(arg))
	return ioctlResult(r, errno)
}

// ioctlResult maps a reservation ioctl's outcome: a positive return is the device's status, and any status but a
// conflict is an error that stops the caller.
func ioctlResult(r uintptr, errno syscall.Errno) error {
	switch {
	case errno == syscall.ENOTTY || errno == syscall.EOPNOTSUPP:
		return fmt.Errorf("%w (%w)", ErrUnsupported, errno)
	case errno != 0:
		return errno
	case r == statusConflict || r == nvmeStatusConflict:
		return fmt.Errorf("%w (status %#x)", ErrReservationConflict, r)
	case r != 0:
		return statusError(r)
	}
	return nil
}

// statusError is a device status other than success or a conflict.
type statusError uintptr

func (s statusError) Error() string {
	return fmt.Sprintf("persistent reservation command failed with status %#x", uintptr(s))
}

// statusIOError is PR_STS_IOERR, the kernel's status for any failure but a conflict. A SCSI unit attention, which the
// first command from a host whose key another host preempted gets, is one.
const statusIOError = 0x02

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

func (d blockDevice) preempt(own, victim uint64) error {
	p := preemption{own: own, victim: victim, typ: typeWriteExclusiveAllRegs}
	return d.ioctl(iocPreempt, unsafe.Pointer(&p))
}

func (d blockDevice) clear(key uint64) error {
	c := clearing{key: key}
	return d.ioctl(iocClear, unsafe.Pointer(&c))
}

func (d blockDevice) readKeys() ([]uint64, error) {
	keys, err := d.listKeys()
	if errors.Is(err, ErrUnsupported) && !d.partition {
		return d.passthrough().readKeys()
	}
	return keys, err
}

func (d blockDevice) readReservation() (key uint64, typ uint32, err error) {
	key, typ, err = d.readHeldReservation()
	if errors.Is(err, ErrUnsupported) && !d.partition {
		return d.passthrough().readReservation()
	}
	return key, typ, err
}

func (d blockDevice) passthrough() passthrough {
	return passthrough{ioctl: func(req uintptr, arg unsafe.Pointer) (uintptr, syscall.Errno) {
		r, _, errno := syscall.Syscall(syscall.SYS_IOCTL, d.f.Fd(), req, uintptr(arg))
		return r, errno
	}}
}

func (d blockDevice) listKeys() ([]uint64, error) {
	keys := make([]uint64, maxListedKeys)
	list := keyList{count: maxListedKeys, keys: uint64(uintptr(unsafe.Pointer(&keys[0])))}
	err := unlistable(d.ioctl(iocReadKeys, unsafe.Pointer(&list)))
	runtime.KeepAlive(keys)
	if err != nil {
		return nil, err
	}
	if list.count > maxListedKeys {
		return keys, fmt.Errorf("the device lists %d keys, more than the %d a volume can hold", list.count, maxListedKeys)
	}
	return keys[:list.count], nil
}

func (d blockDevice) readHeldReservation() (key uint64, typ uint32, err error) {
	var r reservationState
	if err := unlistable(d.ioctl(iocReadResv, unsafe.Pointer(&r))); err != nil {
		return 0, 0, err
	}
	return r.key, r.typ, nil
}

// unlistable reports EINVAL from a listing as ErrUnsupported: on kernels without IOC_PR_READ_KEYS or
// IOC_PR_READ_RESERVATION, sd passes the unknown command to scsi_ioctl, which answers EINVAL instead of ENOTTY.
func unlistable(err error) error {
	if errors.Is(err, syscall.EINVAL) {
		return fmt.Errorf("%w (%w)", ErrUnsupported, err)
	}
	return err
}
