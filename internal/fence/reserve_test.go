package fence

import (
	"errors"
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
