package fence

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestNodeKey(t *testing.T) {
	a := NodeKey([16]byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0xff})
	if a != 0x4743deadbeef0102 {
		t.Errorf("NodeKey = %#x, want 0x4743deadbeef0102: the GC prefix and the UUID's first 6 bytes", a)
	}
	if b := NodeKey([16]byte{}); b == 0 {
		t.Error("a zero UUID gave key 0, which a preempt would treat as every other node")
	}
	if ToolKey()>>48 != toolPrefix || ToolKey() == ToolKey() {
		t.Error("tool keys should carry the GF prefix and be random")
	}
}

// The ioctl numbers are the kernel's, from linux/pr.h.
func TestIoctlNumbers(t *testing.T) {
	for got, want := range map[uintptr]uintptr{iocRegister: 0x401870c8, iocReserve: 0x401070c9, iocPreemptAbort: 0x401870cc} {
		if got != want {
			t.Errorf("ioctl %#x, want %#x", got, want)
		}
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
	if err := Block(f.Fd(), ToolKey()).Register(); !errors.Is(err, ErrUnsupported) {
		t.Errorf("register on a regular file: %v, want ErrUnsupported", err)
	}
}
