package fence

import (
	"errors"
	"syscall"
	"testing"
)

// The device lost its reservation while both nodes' keys stayed. A device without a reservation takes writes from any
// host, so a preempt that only removed the victim's key would leave it writing; Preempt reserves first.
func TestPreemptReservesWhenTheDeviceHasNone(t *testing.T) {
	target := NewTarget()
	victim := target.Node("a", 0x4743000000000001)
	survivor := target.Node("b", 0x4743000000000002)
	for _, pr := range []*PR{victim, survivor} {
		if err := pr.Register(); err != nil {
			t.Fatal(err)
		}
	}
	if !target.MayWrite("a") {
		t.Fatal("the fake refuses writes without a reservation, so the test proves nothing")
	}
	if err := survivor.Preempt(victim.Key()); err != nil {
		t.Fatalf("Preempt: %v", err)
	}
	if target.MayWrite("a") || !target.MayWrite("b") {
		t.Errorf("after the preempt the victim may write: %v, the survivor: %v; want only the survivor", target.MayWrite("a"), target.MayWrite("b"))
	}
}

func TestKeepReserved(t *testing.T) {
	for _, tc := range []struct {
		name              string
		reserved, own     bool
		wantReserved      bool
		wantErr           error
		wantOthersRefused bool
	}{
		{"a held reservation is left alone", true, true, false, nil, true},
		{"a lost reservation is taken again", false, true, true, nil, true},
		{"a lost reservation and key stop the node", false, false, false, ErrFenced, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := NewTarget()
			node := target.Node("a", 0x4743000000000001)
			peer := target.Node("b", 0x4743000000000002)
			for _, pr := range []*PR{node, peer} {
				if err := pr.Register(); err != nil {
					t.Fatal(err)
				}
			}
			if err := peer.Reserve(); err != nil {
				t.Fatal(err)
			}
			target.reserved = tc.reserved
			if !tc.own {
				delete(target.keys, "a")
			}
			reserved, err := node.KeepReserved()
			if reserved != tc.wantReserved || !errors.Is(err, tc.wantErr) {
				t.Errorf("KeepReserved: %v, %v; want %v, %v", reserved, err, tc.wantReserved, tc.wantErr)
			}
			if refused := !target.MayWrite("c"); refused != tc.wantOthersRefused {
				t.Errorf("an unregistered host's writes refused: %v, want %v", refused, tc.wantOthersRefused)
			}
		})
	}
}

// busy answers its first busyReads reservation reads with EAGAIN, as a suspended device-mapper map does.
type busy struct {
	device
	busyReads *int
}

func (d busy) readReservation() (uint64, uint32, error) {
	if *d.busyReads > 0 {
		*d.busyReads--
		return 0, 0, syscall.EAGAIN
	}
	return d.device.readReservation()
}

// A device that answers EAGAIN once, then the truth, gets its lost reservation back; one that stays busy returns EAGAIN.
func TestKeepReservedWaitsOutABusyDevice(t *testing.T) {
	for _, tc := range []struct {
		busyReads    int
		wantReserved bool
		wantErr      error
	}{
		{1, true, nil},
		{busyTries, false, syscall.EAGAIN},
	} {
		target := NewTarget()
		reads := tc.busyReads
		node := &PR{dev: busy{&fakeDevice{t: target, initiator: "a"}, &reads}, key: 0x4743000000000001}
		if err := node.Register(); err != nil {
			t.Fatal(err)
		}
		reserved, err := node.KeepReserved()
		if reserved != tc.wantReserved || !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
			t.Errorf("%d busy reads: KeepReserved %v, %v; want %v, %v", tc.busyReads, reserved, err, tc.wantReserved, tc.wantErr)
		}
		if reads != 0 {
			t.Errorf("%d busy reads: %d left unread", tc.busyReads, reads)
		}
	}
}
