package broadcast

import (
	"net"
	"strings"
	"testing"
)

func TestProblemSaysWhyPeersMissInvalidations(t *testing.T) {
	tr, err := NewTransport(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if got := tr.Problem(); got != "broadcast is not running" {
		t.Errorf("before Start: %q", got)
	}

	tr.running.Store(true)
	if got := tr.Problem(); !strings.Contains(got, "no unicast peers") {
		t.Errorf("no multicast and no peers: %q", got)
	}
	tr.AddPeer("peer", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9})
	if got := tr.Problem(); got != "" {
		t.Errorf("a unicast peer is known: %q", got)
	}

	if err := tr.Send(NewMessageBuilder([16]byte{1}).InvalidateAll()); err == nil {
		t.Fatal("Send without a socket succeeded")
	}
	if got := tr.Problem(); !strings.Contains(got, "the last invalidation send failed") {
		t.Errorf("after a failed send: %q", got)
	}

	disabled := DefaultConfig()
	disabled.Enabled = false
	off, _ := NewTransport(disabled)
	if got := off.Problem(); got != "" {
		t.Errorf("broadcast off: %q", got)
	}
}
