package broadcast

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func freeUDPPorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, n)
	for i := range ports {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ports[i] = conn.LocalAddr().(*net.UDPAddr).Port
	}
	return ports
}

// startTransport starts a transport whose received messages arrive on the returned channel.
func startTransport(t *testing.T, cfg Config) (*Transport, chan *Message) {
	t.Helper()
	tr, err := NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan *Message, 16)
	if err := tr.Start(context.Background(), func(m *Message, _ *net.UDPAddr) {
		select {
		case got <- m:
		default:
		}
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = tr.Stop() })
	return tr, got
}

func receive(t *testing.T, got chan *Message, want MessageType) {
	t.Helper()
	select {
	case m := <-got:
		if m.Type != want {
			t.Errorf("received %v, want %v", m.Type, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("no %v message arrived within 3s", want)
	}
}

// TestDefaultLayoutStarts checks the default socket layout. With the unicast
// port equal to the multicast port, Start always failed, so broadcast never ran.
// Free ports stand in for 31337 and 31338 so the test does not depend on them.
func TestDefaultLayoutStarts(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.UnicastPort == cfg.MulticastPort {
		t.Fatalf("default unicast port %d equals the multicast port", cfg.UnicastPort)
	}
	ports := freeUDPPorts(t, 2)
	cfg.MulticastPort, cfg.UnicastPort = ports[0], ports[1]
	startTransport(t, cfg)
}

func TestSharedPortRejected(t *testing.T) {
	port := freeUDPPorts(t, 1)[0]
	cfg := DefaultConfig()
	cfg.MulticastPort, cfg.UnicastPort = port, port
	tr, err := NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = tr.Start(context.Background(), nil)
	if err == nil {
		_ = tr.Stop()
		t.Fatal("Start accepted the same port for multicast and unicast")
	}
	if !strings.Contains(err.Error(), "must differ") {
		t.Errorf("error does not say the ports must differ: %v", err)
	}
}

// TestTwoNodesOnOneHost checks that nodes sharing a host and a multicast port,
// each with its own unicast port, exchange messages over both paths.
func TestTwoNodesOnOneHost(t *testing.T) {
	ports := freeUDPPorts(t, 3)
	cfgA, cfgB := DefaultConfig(), DefaultConfig()
	cfgA.MulticastPort, cfgB.MulticastPort = ports[0], ports[0]
	cfgA.UnicastPort, cfgB.UnicastPort = ports[1], ports[2]

	a, gotA := startTransport(t, cfgA)
	_, gotB := startTransport(t, cfgB)
	builder := NewMessageBuilder([16]byte{1})

	t.Run("multicast", func(t *testing.T) {
		if a.multicastConn == nil {
			t.Skip("multicast is not available on this host")
		}
		if err := a.Send(builder.InvalidateInode(7, 1, 1)); err != nil {
			t.Fatal(err)
		}
		receive(t, gotB, TypeInvalidateInode)
	})

	t.Run("unicast", func(t *testing.T) {
		a.config.MaxMulticastFailures = 0
		a.AddPeer("b", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: ports[2]})
		if err := a.Send(builder.InvalidateExtent(7, 1)); err != nil {
			t.Fatal(err)
		}
		receive(t, gotB, TypeInvalidateExtent)
	})

	select {
	case m := <-gotA:
		if m.SenderUUID != [16]byte{1} {
			t.Errorf("node A received a message it did not send: %+v", m)
		}
	default:
	}
}
