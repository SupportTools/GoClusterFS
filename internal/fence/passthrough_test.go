package fence

import (
	"encoding/binary"
	"errors"
	"os"
	"slices"
	"syscall"
	"testing"
	"unsafe"
)

func TestPassthroughStructsMatchTheKernel(t *testing.T) {
	if s := unsafe.Sizeof(sgIOHdr{}); s != 88 {
		t.Errorf("sg_io_hdr is %d bytes, the kernel's is 88", s)
	}
	if s := unsafe.Sizeof(nvmePassthruCmd{}); s != 72 {
		t.Errorf("nvme_passthru_cmd is %d bytes, the kernel's is 72", s)
	}
}

// scsiDisk answers SG_IO as a SCSI disk holding keys and a reservation of SCSI type resvType, or with the SCSI status
// and sense key it is given.
type scsiDisk struct {
	keys      []uint64
	resvType  byte
	status    byte
	senseKey  byte
	commands  int
	failFirst bool
}

func (d *scsiDisk) ioctl(req uintptr, arg unsafe.Pointer) (uintptr, syscall.Errno) {
	if req != sgIO {
		return 0, syscall.ENOTTY
	}
	d.commands++
	h := (*sgIOHdr)(arg)
	cdb := unsafe.Slice((*byte)(h.cmdp), h.cmdLen)
	buf := unsafe.Slice((*byte)(h.dxferp), h.dxferLen)
	if cdb[0] != prIn {
		h.status = scsiCheckCondition
		return 0, 0
	}
	if d.status != 0 && (!d.failFirst || d.commands == 1) {
		h.status = d.status
		sense := unsafe.Slice((*byte)(h.sbp), h.mxSbLen)
		sense[0], sense[2], sense[12] = 0x70, d.senseKey, 0x29
		h.sbLenWr = 18
		return 0, 0
	}
	switch cdb[1] {
	case prInReadKeys:
		binary.BigEndian.PutUint32(buf[4:], uint32(8*len(d.keys)))
		for i, k := range d.keys {
			binary.BigEndian.PutUint64(buf[8+8*i:], k)
		}
	case prInReadReservation:
		if d.resvType != 0 {
			binary.BigEndian.PutUint32(buf[4:], 16)
			buf[21] = d.resvType
		}
	}
	return 0, 0
}

func TestSCSIPassthroughListsKeysAndTheReservation(t *testing.T) {
	disk := &scsiDisk{keys: []uint64{0x4743000000000001, 0x4743000000000002}, resvType: 7}
	p := passthrough{ioctl: disk.ioctl}
	keys, err := p.readKeys()
	if err != nil || !slices.Equal(keys, disk.keys) {
		t.Errorf("readKeys: %x, %v; want %x", keys, err, disk.keys)
	}
	key, typ, err := p.readReservation()
	if err != nil || key != 0 || typ != typeWriteExclusiveAllRegs {
		t.Errorf("readReservation: key %#x type %d, %v; want key 0, Write Exclusive All Registrants", key, typ, err)
	}
	disk.resvType = 0
	if _, typ, err := p.readReservation(); err != nil || typ != 0 {
		t.Errorf("readReservation with no reservation: type %d, %v", typ, err)
	}
}

// A device that refuses PERSISTENT RESERVE IN cannot list keys; a unit attention is retried once, as the retrying
// wrapper does for the reservation ioctls.
func TestSCSIPassthroughErrors(t *testing.T) {
	refusing := &scsiDisk{status: scsiCheckCondition, senseKey: senseIllegalRequest}
	if _, err := (passthrough{ioctl: refusing.ioctl}).readKeys(); !errors.Is(err, ErrUnsupported) {
		t.Errorf("ILLEGAL REQUEST: %v, want ErrUnsupported", err)
	}
	attention := &scsiDisk{keys: []uint64{0x4743000000000001}, status: scsiCheckCondition, senseKey: senseUnitAttention, failFirst: true}
	keys, err := retrying{device: passthroughDevice{passthrough{ioctl: attention.ioctl}}}.readKeys()
	if err != nil || len(keys) != 1 || attention.commands != 2 {
		t.Errorf("after a unit attention: %x, %v in %d commands; want the key after one retry", keys, err, attention.commands)
	}
}

// passthroughDevice is a device whose only working commands are the passthrough listings.
type passthroughDevice struct{ passthrough }

func (passthroughDevice) register(uint64, uint64, bool) error { return ErrUnsupported }
func (passthroughDevice) reserve(uint64) error                { return ErrUnsupported }
func (passthroughDevice) preemptAbort(uint64, uint64) error   { return ErrUnsupported }
func (passthroughDevice) preempt(uint64, uint64) error        { return ErrUnsupported }
func (passthroughDevice) clear(uint64) error                  { return ErrUnsupported }

// nvmeNamespace answers the NVMe passthrough ioctls as namespace 1 with registrants, holding a reservation of NVMe
// type rtype, and refuses extended host IDs when compactOnly is set. status overrides the answer.
type nvmeNamespace struct {
	regs        []nvmeRegistrant
	rtype       byte
	compactOnly bool
	status      uintptr
	reports     []uint32
}

func (n *nvmeNamespace) ioctl(req uintptr, arg unsafe.Pointer) (uintptr, syscall.Errno) {
	switch req {
	case nvmeIoctlID:
		return 1, 0
	case nvmeIoctlIOCmd:
	default:
		return 0, syscall.ENOTTY
	}
	cmd := (*nvmePassthruCmd)(arg)
	n.reports = append(n.reports, cmd.cdw11)
	if cmd.opcode != nvmeCmdResvReport || cmd.nsid != 1 || cmd.cdw10 != cmd.dataLen/4-1 {
		return nvmeSCInvalidField, 0
	}
	if n.status != 0 {
		return n.status, 0
	}
	eds := cmd.cdw11&1 != 0
	if eds && n.compactOnly {
		return 0x4000 | nvmeSCHostIDInconsistent, 0
	}
	buf := unsafe.Slice((*byte)(*(*unsafe.Pointer)(unsafe.Pointer(&cmd.addr))), cmd.dataLen)
	header, entry, keyAt := 24, 24, 16
	if eds {
		header, entry, keyAt = 64, 64, 8
	}
	buf[4] = n.rtype
	binary.LittleEndian.PutUint16(buf[5:], uint16(len(n.regs)))
	for i, r := range n.regs {
		at := header + i*entry
		if r.holds {
			buf[at+2] = 1
		}
		binary.LittleEndian.PutUint64(buf[at+keyAt:], r.key)
	}
	return 0, 0
}

func TestNVMePassthroughListsKeysAndTheReservation(t *testing.T) {
	for _, compact := range []bool{false, true} {
		ns := &nvmeNamespace{regs: []nvmeRegistrant{{0x4743000000000001, false}, {0x4743000000000002, true}}, rtype: 5, compactOnly: compact}
		p := passthrough{ioctl: ns.ioctl}
		keys, err := p.readKeys()
		if err != nil || !slices.Equal(keys, []uint64{0x4743000000000001, 0x4743000000000002}) {
			t.Errorf("compact %v: readKeys %x, %v", compact, keys, err)
		}
		key, typ, err := p.readReservation()
		if err != nil || key != 0x4743000000000002 || typ != typeWriteExclusiveAllRegs {
			t.Errorf("compact %v: readReservation key %#x type %d, %v; want the holder's key and Write Exclusive All Registrants", compact, key, typ, err)
		}
		want := []uint32{1, 1}
		if compact {
			want = []uint32{1, 0, 1, 0}
		}
		if !slices.Equal(ns.reports, want) {
			t.Errorf("compact %v: reports sent with cdw11 %v, want %v: extended host IDs first, then compact ones if refused", compact, ns.reports, want)
		}
	}
}

// A namespace without reservations answers Invalid Opcode, and a device that takes neither passthrough cannot list.
func TestNVMePassthroughUnsupported(t *testing.T) {
	ns := &nvmeNamespace{status: nvmeSCInvalidOpcode}
	if _, err := (passthrough{ioctl: ns.ioctl}).readKeys(); !errors.Is(err, ErrUnsupported) {
		t.Errorf("Invalid Opcode: %v, want ErrUnsupported", err)
	}
	none := func(uintptr, unsafe.Pointer) (uintptr, syscall.Errno) { return 0, syscall.ENOTTY }
	if _, err := (passthrough{ioctl: none}).readKeys(); !errors.Is(err, ErrUnsupported) {
		t.Errorf("no passthrough: %v, want ErrUnsupported", err)
	}
}

func TestPassthroughRefusesMoreKeysThanAVolumeHolds(t *testing.T) {
	buf := make([]byte, 8+8*maxListedKeys)
	binary.BigEndian.PutUint32(buf[4:], 8*(maxListedKeys+1))
	if _, err := parseSCSIKeys(buf); err == nil {
		t.Error("parseSCSIKeys took more keys than a volume can hold")
	}
	report := make([]byte, 64+64*maxListedKeys)
	binary.LittleEndian.PutUint16(report[5:], maxListedKeys+1)
	if _, _, err := parseNVMeReport(report, true); err == nil {
		t.Error("parseNVMeReport took more registrants than a volume can hold")
	}
}

// On the disk GOCLUSTERFS_TEST_PR_DEVICE names, on a kernel with IOC_PR_READ_KEYS, the passthrough listing matches the
// kernel's: the test registers a key and reserves, compares both listings, and removes the key.
func TestPassthroughMatchesTheKernel(t *testing.T) {
	path := os.Getenv("GOCLUSTERFS_TEST_PR_DEVICE")
	if path == "" {
		t.Skip("set GOCLUSTERFS_TEST_PR_DEVICE to a whole disk with persistent reservations and no other keys")
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	d := blockDevice{f: f}
	pr := retrying{device: d}
	key := ToolKey()
	if err := pr.register(0, key, true); err != nil {
		t.Fatalf("register: %v", err)
	}
	defer func() { _ = pr.register(key, 0, false) }()
	if err := pr.reserve(key); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	kernelKeys, err := d.listKeys()
	if err != nil {
		t.Fatalf("the kernel's listing: %v; this test needs Linux 6.19 or later", err)
	}
	rawKeys, err := d.passthrough().readKeys()
	slices.Sort(kernelKeys)
	slices.Sort(rawKeys)
	if err != nil || !slices.Equal(rawKeys, kernelKeys) || !slices.Contains(rawKeys, key) {
		t.Errorf("passthrough keys %x, %v; the kernel lists %x", rawKeys, err, kernelKeys)
	}
	kernelKey, kernelType, kerr := d.readHeldReservation()
	rawKey, rawType, err := d.passthrough().readReservation()
	if kerr != nil || err != nil || rawKey != kernelKey || rawType != kernelType || rawType != typeWriteExclusiveAllRegs {
		t.Errorf("passthrough reservation key %#x type %d (%v); the kernel's key %#x type %d (%v)", rawKey, rawType, err, kernelKey, kernelType, kerr)
	}
	t.Logf("%s: keys %x, reservation key %#x type %d, from both", path, rawKeys, rawKey, rawType)
}
