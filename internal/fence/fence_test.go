package fence

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestNodeKey(t *testing.T) {
	a := NodeKey([16]byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0xff})
	if a != 0x4743deadbeef0102 {
		t.Errorf("NodeKey = %#x, want 0x4743deadbeef0102: the GC prefix and the UUID's first 6 bytes", a)
	}
	if b := NodeKey([16]byte{}); b == 0 {
		t.Error("a zero UUID gave key 0, which a preempt would treat as every other node")
	}
	if first, second := ToolKey(), ToolKey(); first>>48 != toolPrefix || first == second {
		t.Error("tool keys should carry the GF prefix and be random")
	}
}

// The ioctl numbers are the kernel's, from linux/pr.h.
func TestIoctlNumbers(t *testing.T) {
	for got, want := range map[uintptr]uintptr{
		iocRegister: 0x401870c8, iocReserve: 0x401070c9, iocPreemptAbort: 0x401870cc, iocClear: 0x401070cd,
		iocReadKeys: 0xc01070ce, iocReadResv: 0x801070cf,
	} {
		if got != want {
			t.Errorf("ioctl %#x, want %#x", got, want)
		}
	}
}

// Clear removes every key and the reservation: any initiator may write until a node reserves the device again, and a
// node whose key was cleared finds it gone.
func TestClearRemovesEveryKeyAndTheReservation(t *testing.T) {
	target := NewTarget()
	a, b := target.Node("a", NodeKey([16]byte{1})), target.Node("b", NodeKey([16]byte{2}))
	for _, n := range []*PR{a, b} {
		if err := n.Register(); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Reserve(); err != nil {
		t.Fatal(err)
	}
	if target.MayWrite("tool") {
		t.Fatal("an unregistered initiator may write under the reservation")
	}
	tool := target.Node("tool", ToolKey())
	if keys, err := tool.Keys(); err != nil || !slices.Equal(keys, []uint64{a.Key(), b.Key()}) {
		t.Fatalf("Keys before Clear = %x, %v; want a's and b's", keys, err)
	}
	if err := tool.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if keys, err := tool.Keys(); err != nil || len(keys) != 0 {
		t.Errorf("Keys after Clear = %x, %v; want none", keys, err)
	}
	for _, initiator := range []string{"a", "b", "tool", "other"} {
		if !target.MayWrite(initiator) {
			t.Errorf("%s may not write after Clear", initiator)
		}
	}
	if err := a.Unregister(); !errors.Is(err, ErrFenced) {
		t.Errorf("a's key after Clear: Unregister = %v, want ErrFenced", err)
	}
}

// A survivor's preempt shuts the dead node out: its writes stop, it cannot preempt back, and a second preempt of
// the same key is harmless.
func TestPreemptRevokesTheVictim(t *testing.T) {
	target := NewTarget()
	a, b := target.Node("a", NodeKey([16]byte{1})), target.Node("b", NodeKey([16]byte{2}))
	for _, n := range []*PR{a, b} {
		if err := n.Register(); err != nil {
			t.Fatal(err)
		}
		if err := n.Reserve(); err != nil {
			t.Fatal(err)
		}
	}
	if !target.MayWrite("a") || !target.MayWrite("b") || target.MayWrite("c") {
		t.Fatal("registered nodes may write and others may not")
	}
	if err := a.Preempt(b.Key()); err != nil {
		t.Fatalf("preempt: %v", err)
	}
	if target.MayWrite("b") {
		t.Error("the preempted node may still write")
	}
	if err := a.Preempt(b.Key()); err != nil {
		t.Errorf("preempting a key already gone: %v, want nil", err)
	}
	if err := b.Preempt(a.Key()); !errors.Is(err, ErrFenced) {
		t.Errorf("the preempted node's preempt: %v, want ErrFenced", err)
	}
	if err := b.Unregister(); !errors.Is(err, ErrFenced) {
		t.Errorf("the preempted node's unregister: %v, want ErrFenced", err)
	}
	if !target.MayWrite("a") {
		t.Error("the survivor lost its access")
	}
	if err := b.Register(); err != nil || !target.MayWrite("b") {
		t.Errorf("a node mounting again registers anew: %v", err)
	}
}

func TestPreemptRefusesKeyZeroAndItsOwn(t *testing.T) {
	target := NewTarget()
	a := target.Node("a", NodeKey([16]byte{1}))
	if err := a.Register(); err != nil {
		t.Fatal(err)
	}
	for _, victim := range []uint64{0, a.Key()} {
		if err := a.Preempt(victim); err == nil {
			t.Errorf("preempt of %#x succeeded", victim)
		}
	}
	if len(target.Preempted()) != 0 {
		t.Error("a refused preempt reached the device")
	}
}

// A regular file has no reservations: the ioctl fails with ENOTTY, which is ErrUnsupported.
func TestBlockOnRegularFileIsUnsupported(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "img"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := Block(f, ToolKey()).Register(); !errors.Is(err, ErrUnsupported) {
		t.Errorf("register on a regular file: %v, want ErrUnsupported", err)
	}
}

// A reservation covers the whole disk, so a partition gets ErrPartition, which auto treats as unsupported, before any
// command reaches the device.
func TestPartitionRefusesEveryReservationCommand(t *testing.T) {
	pr := &PR{dev: blockDevice{partition: true}, key: ToolKey()}
	for name, err := range map[string]error{
		"register": pr.Register(),
		"reserve":  pr.Reserve(),
		"preempt":  pr.Preempt(NodeKey([16]byte{1})),
	} {
		if !errors.Is(err, ErrPartition) || !errors.Is(err, ErrUnsupported) {
			t.Errorf("%s on a partition: %v, want ErrPartition, which is ErrUnsupported", name, err)
		}
	}
}

// Kernels without IOC_PR_READ_KEYS answer it with EINVAL through sd, or ENOTTY; both mean the keys cannot be listed.
func TestKeyListingWithoutTheIoctlIsUnsupported(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EINVAL, syscall.ENOTTY} {
		if err := unlistable(ioctlResult(0, errno)); !errors.Is(err, ErrUnsupported) {
			t.Errorf("%v from a key listing: %v, want ErrUnsupported", errno, err)
		}
	}
	if err := unlistable(ioctlResult(0, syscall.EIO)); errors.Is(err, ErrUnsupported) {
		t.Errorf("EIO from a key listing: %v, want an error that is not ErrUnsupported", err)
	}
}

func TestPartitionInSysfs(t *testing.T) {
	sysfs := t.TempDir()
	for _, dir := range []string{"8:0", "8:1", "259:3"} {
		if err := os.Mkdir(filepath.Join(sysfs, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, part := range []string{"8:1", "259:3"} {
		if err := os.WriteFile(filepath.Join(sysfs, part, "partition"), []byte("1\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		major, minor uint32
		want         bool
	}{{8, 0, false}, {8, 1, true}, {259, 3, true}, {7, 0, false}} {
		if got := partitionIn(sysfs, unix.Mkdev(tc.major, tc.minor)); got != tc.want {
			t.Errorf("partitionIn(%d:%d) = %v, want %v", tc.major, tc.minor, got, tc.want)
		}
	}
}

func TestIoctlResultMapsBothTransports(t *testing.T) {
	for _, tc := range []struct {
		name  string
		r     uintptr
		errno syscall.Errno
		want  error
	}{
		{"SCSI conflict", 0x18, 0, ErrReservationConflict},
		{"NVMe conflict", 0x83, 0, ErrReservationConflict},
		{"no persistent reservations", 0, syscall.ENOTTY, ErrUnsupported},
		{"no pr_ops", 0, syscall.EOPNOTSUPP, ErrUnsupported},
		{"not root", 0, syscall.EPERM, syscall.EPERM},
		{"success", 0, 0, nil},
	} {
		if err := ioctlResult(tc.r, tc.errno); !errors.Is(err, tc.want) || (tc.want == nil) != (err == nil) {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.want)
		}
	}
	if err := ioctlResult(0x02, 0); err == nil || errors.Is(err, ErrReservationConflict) {
		t.Errorf("another device status: %v, want an error that is not a conflict", err)
	}
}

// statusDevice answers each command with a raw device status, as the sd and nvme drivers return them.
type statusDevice struct{ preemptStatus, registerStatus uintptr }

func (d statusDevice) register(old, new uint64, ignore bool) error {
	return ioctlResult(d.registerStatus, 0)
}
func (d statusDevice) reserve(uint64) error                     { return nil }
func (d statusDevice) preemptAbort(own, victim uint64) error    { return ioctlResult(d.preemptStatus, 0) }
func (d statusDevice) clear(uint64) error                       { return nil }
func (d statusDevice) readKeys() ([]uint64, error)              { return nil, nil }
func (d statusDevice) readReservation() (uint64, uint32, error) { return 0, 0, nil }

// Preempt reads a conflict alike from SCSI and NVMe, and stops on any other status.
func TestPreemptStatusesFromBothTransports(t *testing.T) {
	for _, conflict := range []uintptr{0x18, 0x83} {
		gone := &PR{dev: statusDevice{preemptStatus: conflict}, key: 1}
		if err := gone.Preempt(2); err != nil {
			t.Errorf("status %#x with this node still registered: %v, want nil (the victim is gone already)", conflict, err)
		}
		fenced := &PR{dev: statusDevice{preemptStatus: conflict, registerStatus: conflict}, key: 1}
		if err := fenced.Preempt(2); !errors.Is(err, ErrFenced) {
			t.Errorf("status %#x with this node's key gone: %v, want ErrFenced", conflict, err)
		}
	}
	other := &PR{dev: statusDevice{preemptStatus: 0x02}, key: 1}
	if err := other.Preempt(2); err == nil || errors.Is(err, ErrFenced) {
		t.Errorf("status 0x02: %v, want an error that stops the takeover", err)
	}
}

// scripted answers each command with the next raw status of its list, as the kernel returns them.
type scripted struct{ registers, preempts []uintptr }

func (d *scripted) next(statuses *[]uintptr) error {
	r := (*statuses)[0]
	*statuses = (*statuses)[1:]
	return ioctlResult(r, 0)
}

func (d *scripted) register(uint64, uint64, bool) error      { return d.next(&d.registers) }
func (d *scripted) reserve(uint64) error                     { return nil }
func (d *scripted) preemptAbort(uint64, uint64) error        { return d.next(&d.preempts) }
func (d *scripted) clear(uint64) error                       { return nil }
func (d *scripted) readKeys() ([]uint64, error)              { return nil, nil }
func (d *scripted) readReservation() (uint64, uint32, error) { return 0, 0, nil }

// A preempted host's first reservation command gets a unit attention, which the kernel reports as status 0x2. Sent
// again, the command meets the conflict behind it, so Unregister and Preempt report ErrFenced; 0x2 twice stays an
// error that is not.
func TestUnitAttentionAfterAPreemptReadsAsFenced(t *testing.T) {
	unmounting := &PR{dev: retrying{device: &scripted{registers: []uintptr{0x02, 0x18}}}, key: 1}
	if err := unmounting.Unregister(); !errors.Is(err, ErrFenced) {
		t.Errorf("Unregister after a unit attention: %v, want ErrFenced", err)
	}
	preempting := &PR{dev: retrying{device: &scripted{preempts: []uintptr{0x02, 0x18}, registers: []uintptr{0x02, 0x18}}}, key: 1}
	if err := preempting.Preempt(2); !errors.Is(err, ErrFenced) {
		t.Errorf("Preempt after a unit attention: %v, want ErrFenced", err)
	}
	failing := &PR{dev: retrying{device: &scripted{registers: []uintptr{0x02, 0x02}}}, key: 1}
	if err := failing.Unregister(); err == nil || errors.Is(err, ErrFenced) {
		t.Errorf("Unregister answered 0x2 twice: %v, want an error that is not ErrFenced", err)
	}
	f, err := os.CreateTemp(t.TempDir(), "disk")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, ok := Block(f, 1).dev.(retrying); !ok {
		t.Error("Block does not send a command again after status 0x2")
	}
}
