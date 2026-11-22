package fence

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"syscall"
	"unsafe"
)

// passthrough lists a device's keys and reservation with the storage's own commands, for kernels without
// IOC_PR_READ_KEYS and IOC_PR_READ_RESERVATION, which arrived in Linux 6.19: SCSI PERSISTENT RESERVE IN through SG_IO,
// or an NVMe Reservation Report through the NVMe passthrough ioctl. dm-multipath passes both to one of its paths.
type passthrough struct {
	ioctl func(req uintptr, arg unsafe.Pointer) (uintptr, syscall.Errno)
}

// errNotSCSI means the device does not take SG_IO, as an NVMe namespace does not.
var errNotSCSI = errors.New("the device does not take SCSI commands")

func (p passthrough) readKeys() ([]uint64, error) {
	buf := make([]byte, 8+8*maxListedKeys)
	err := p.scsiPRIn(prInReadKeys, buf)
	if errors.Is(err, errNotSCSI) {
		regs, _, err := p.nvmeReport()
		keys := make([]uint64, len(regs))
		for i, r := range regs {
			keys[i] = r.key
		}
		return keys, err
	}
	if err != nil {
		return nil, err
	}
	return parseSCSIKeys(buf)
}

func (p passthrough) readReservation() (key uint64, typ uint32, err error) {
	buf := make([]byte, 24)
	err = p.scsiPRIn(prInReadReservation, buf)
	if errors.Is(err, errNotSCSI) {
		regs, rtype, err := p.nvmeReport()
		if err != nil || rtype == 0 {
			return 0, 0, err
		}
		for _, r := range regs {
			if r.holds {
				return r.key, uint32(rtype), nil
			}
		}
		return 0, uint32(rtype), nil
	}
	if err != nil {
		return 0, 0, err
	}
	return parseSCSIReservation(buf)
}

const (
	sgIO                = 0x2285
	sgDxferFromDev      = -3
	prIn                = 0x5e
	prInReadKeys        = 0x00
	prInReadReservation = 0x01
	scsiCheckCondition  = 0x02
	scsiConflict        = 0x18
	senseIllegalRequest = 0x05
	senseUnitAttention  = 0x06
)

// sgIOHdr is struct sg_io_hdr from <scsi/sg.h>.
type sgIOHdr struct {
	interfaceID    int32
	dxferDirection int32
	cmdLen         uint8
	mxSbLen        uint8
	iovecCount     uint16
	dxferLen       uint32
	dxferp         unsafe.Pointer
	cmdp           unsafe.Pointer
	sbp            unsafe.Pointer
	timeout        uint32
	flags          uint32
	packID         int32
	usrPtr         unsafe.Pointer
	status         uint8
	maskedStatus   uint8
	msgStatus      uint8
	sbLenWr        uint8
	hostStatus     uint16
	driverStatus   uint16
	resid          int32
	duration       uint32
	info           uint32
}

// scsiPRIn sends PERSISTENT RESERVE IN with service action action and reads its answer into buf.
func (p passthrough) scsiPRIn(action byte, buf []byte) error {
	cdb := make([]byte, 10)
	cdb[0], cdb[1] = prIn, action
	binary.BigEndian.PutUint16(cdb[7:9], uint16(len(buf)))
	sense := make([]byte, 32)
	var pins runtime.Pinner
	defer pins.Unpin()
	pins.Pin(&buf[0])
	pins.Pin(&cdb[0])
	pins.Pin(&sense[0])
	h := sgIOHdr{interfaceID: 'S', dxferDirection: sgDxferFromDev, cmdLen: uint8(len(cdb)), mxSbLen: uint8(len(sense)),
		dxferLen: uint32(len(buf)), dxferp: unsafe.Pointer(&buf[0]), cmdp: unsafe.Pointer(&cdb[0]), sbp: unsafe.Pointer(&sense[0]),
		timeout: 30000}
	if _, errno := p.ioctl(sgIO, unsafe.Pointer(&h)); errno != 0 {
		if errno == syscall.ENOTTY || errno == syscall.EINVAL {
			return fmt.Errorf("%w (%w)", errNotSCSI, errno)
		}
		return fmt.Errorf("SG_IO: %w", errno)
	}
	switch {
	case h.hostStatus != 0:
		return fmt.Errorf("SG_IO: host status %#x", h.hostStatus)
	case h.status == 0:
		return nil
	case h.status == scsiConflict:
		return fmt.Errorf("%w (SCSI status %#x)", ErrReservationConflict, h.status)
	case h.status != scsiCheckCondition:
		return statusError(h.status)
	}
	key, asc, ascq := senseKey(sense[:h.sbLenWr])
	switch key {
	case senseIllegalRequest:
		return fmt.Errorf("%w (sense key %#x, ASC %#x, ASCQ %#x)", ErrUnsupported, key, asc, ascq)
	case senseUnitAttention:
		return statusError(statusIOError)
	}
	return fmt.Errorf("PERSISTENT RESERVE IN failed: sense key %#x, ASC %#x, ASCQ %#x", key, asc, ascq)
}

// senseKey reads the sense key and additional sense code of fixed or descriptor format sense data.
func senseKey(sense []byte) (key, asc, ascq byte) {
	switch {
	case len(sense) >= 14 && sense[0]&0x7e == 0x70:
		return sense[2] & 0x0f, sense[12], sense[13]
	case len(sense) >= 4 && sense[0]&0x7e == 0x72:
		return sense[1] & 0x0f, sense[2], sense[3]
	}
	return 0, 0, 0
}

func parseSCSIKeys(buf []byte) ([]uint64, error) {
	n := binary.BigEndian.Uint32(buf[4:8]) / 8
	if n > maxListedKeys {
		return nil, fmt.Errorf("the device lists %d keys, more than the %d a volume can hold", n, maxListedKeys)
	}
	keys := make([]uint64, n)
	for i := range keys {
		keys[i] = binary.BigEndian.Uint64(buf[8+8*i:])
	}
	return keys, nil
}

// scsiToBlockType maps SCSI persistent reservation types to the kernel's enum pr_type.
var scsiToBlockType = map[byte]uint32{1: 1, 3: 2, 5: 3, 6: 4, 7: 5, 8: 6}

func parseSCSIReservation(buf []byte) (key uint64, typ uint32, err error) {
	if binary.BigEndian.Uint32(buf[4:8]) < 16 {
		return 0, 0, nil
	}
	typ, ok := scsiToBlockType[buf[21]&0x0f]
	if !ok {
		return 0, 0, fmt.Errorf("the device holds a reservation of unknown SCSI type %#x", buf[21]&0x0f)
	}
	return binary.BigEndian.Uint64(buf[8:16]), typ, nil
}

const (
	nvmeIoctlID              = 0x4e40
	nvmeIoctlIOCmd           = 0xc0484e43
	nvmeCmdResvReport        = 0x0e
	nvmeStatusMask           = 0x7ff
	nvmeSCInvalidOpcode      = 0x01
	nvmeSCInvalidField       = 0x02
	nvmeSCHostIDInconsistent = 0x18
)

// nvmePassthruCmd is struct nvme_passthru_cmd from <linux/nvme_ioctl.h>.
type nvmePassthruCmd struct {
	opcode, flags        uint8
	rsvd1                uint16
	nsid, cdw2, cdw3     uint32
	metadata, addr       uint64
	metadataLen, dataLen uint32
	cdw10, cdw11, cdw12  uint32
	cdw13, cdw14, cdw15  uint32
	timeoutMs, result    uint32
}

type nvmeRegistrant struct {
	key   uint64
	holds bool
}

// nvmeReport sends a Reservation Report, first asking for 128-bit host IDs as Linux does, and without them if the
// controller answers that host IDs are inconsistent. NVMe reservation types number as the kernel's enum pr_type does.
func (p passthrough) nvmeReport() (regs []nvmeRegistrant, rtype uint8, err error) {
	nsid, errno := p.ioctl(nvmeIoctlID, nil)
	if errno == syscall.ENOTTY || errno == syscall.EINVAL {
		return nil, 0, fmt.Errorf("%w: the device takes neither SCSI nor NVMe passthrough commands (%w)", ErrUnsupported, errno)
	} else if errno != 0 {
		return nil, 0, fmt.Errorf("NVME_IOCTL_ID: %w", errno)
	}
	buf := make([]byte, 64+64*maxListedKeys)
	var pins runtime.Pinner
	defer pins.Unpin()
	pins.Pin(&buf[0])
	for eds := true; ; eds = false {
		cmd := nvmePassthruCmd{opcode: nvmeCmdResvReport, nsid: uint32(nsid), addr: uint64(uintptr(unsafe.Pointer(&buf[0]))),
			dataLen: uint32(len(buf)), cdw10: uint32(len(buf)/4 - 1), timeoutMs: 30000}
		if eds {
			cmd.cdw11 = 1
		}
		status, errno := p.ioctl(nvmeIoctlIOCmd, unsafe.Pointer(&cmd))
		if errno != 0 {
			return nil, 0, fmt.Errorf("NVMe Reservation Report: %w", errno)
		}
		switch status & nvmeStatusMask {
		case 0:
			return parseNVMeReport(buf, eds)
		case nvmeSCHostIDInconsistent:
			if eds {
				continue
			}
		case nvmeSCInvalidOpcode, nvmeSCInvalidField:
			return nil, 0, fmt.Errorf("%w (NVMe status %#x)", ErrUnsupported, status)
		}
		return nil, 0, fmt.Errorf("NVMe Reservation Report failed with status %#x", status)
	}
}

func parseNVMeReport(buf []byte, eds bool) ([]nvmeRegistrant, uint8, error) {
	header, entry, keyAt := 24, 24, 16
	if eds {
		header, entry, keyAt = 64, 64, 8
	}
	n := int(binary.LittleEndian.Uint16(buf[5:7]))
	if n > maxListedKeys {
		return nil, 0, fmt.Errorf("the device lists %d registrants, more than the %d a volume can hold", n, maxListedKeys)
	}
	regs := make([]nvmeRegistrant, n)
	for i := range regs {
		at := header + i*entry
		regs[i] = nvmeRegistrant{key: binary.LittleEndian.Uint64(buf[at+keyAt:]), holds: buf[at+2]&1 != 0}
	}
	return regs, buf[4], nil
}
