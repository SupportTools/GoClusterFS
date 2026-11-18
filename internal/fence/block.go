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
	return &PR{dev: blockDevice{f: f, partition: isPartition(f)}, key: key}
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
	iocPreemptAbort = ioc(1, 204, unsafe.Sizeof(preemption{}))
	iocClear        = ioc(1, 205, unsafe.Sizeof(clearing{}))
	iocReadKeys     = ioc(3, 206, unsafe.Sizeof(keyList{}))
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

func (d blockDevice) clear(key uint64) error {
	c := clearing{key: key}
	return d.ioctl(iocClear, unsafe.Pointer(&c))
}

func (d blockDevice) readKeys() ([]uint64, error) {
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

// unlistable reports EINVAL from a key listing as ErrUnsupported: on kernels without IOC_PR_READ_KEYS, sd passes the
// unknown command to scsi_ioctl, which answers EINVAL instead of ENOTTY.
func unlistable(err error) error {
	if errors.Is(err, syscall.EINVAL) {
		return fmt.Errorf("%w (%v)", ErrUnsupported, err)
	}
	return err
}
