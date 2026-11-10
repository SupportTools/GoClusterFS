//go:build linux

package broadcast

import (
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

func sockoptInt(t *testing.T, conn *net.UDPConn, opt int) int {
	t.Helper()
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var v int
	var gerr error
	if err := raw.Control(func(fd uintptr) { v, gerr = unix.GetsockoptInt(int(fd), unix.IPPROTO_IP, opt) }); err != nil {
		t.Fatal(err)
	}
	if gerr != nil {
		t.Fatal(gerr)
	}
	return v
}

func TestSendSocketMulticastOptions(t *testing.T) {
	ports := freeUDPPorts(t, 2)
	cfg := DefaultConfig()
	cfg.MulticastPort, cfg.UnicastPort = ports[0], ports[1]
	cfg.MulticastTTL = 2
	tr, _ := startTransport(t, cfg)
	if tr.multicastConn == nil {
		t.Skip("multicast is not available on this host")
	}
	if loop := sockoptInt(t, tr.unicastConn, unix.IP_MULTICAST_LOOP); loop != 1 {
		t.Errorf("IP_MULTICAST_LOOP = %d on the send socket, want 1", loop)
	}
	if ttl := sockoptInt(t, tr.unicastConn, unix.IP_MULTICAST_TTL); ttl != 2 {
		t.Errorf("IP_MULTICAST_TTL = %d on the send socket, want 2", ttl)
	}
}

// TestTwoNodesOnExplicitInterface checks that with -broadcast-iface the send
// socket uses the interface the listener joined.
func TestTwoNodesOnExplicitInterface(t *testing.T) {
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	var name string
	for _, ifi := range ifaces {
		if ifi.Flags&net.FlagUp != 0 && ifi.Flags&net.FlagMulticast != 0 && ifi.Flags&net.FlagLoopback == 0 {
			if addrs, _ := ifi.Addrs(); len(addrs) > 0 {
				name = ifi.Name
				break
			}
		}
	}
	if name == "" {
		t.Skip("no multicast interface with an address")
	}

	ports := freeUDPPorts(t, 3)
	cfgA, cfgB := DefaultConfig(), DefaultConfig()
	cfgA.Interface, cfgB.Interface = name, name
	cfgA.MulticastPort, cfgB.MulticastPort = ports[0], ports[0]
	cfgA.UnicastPort, cfgB.UnicastPort = ports[1], ports[2]
	a, _ := startTransport(t, cfgA)
	_, gotB := startTransport(t, cfgB)
	if a.multicastConn == nil {
		t.Skipf("multicast is not available on %s", name)
	}

	if err := a.Send(NewMessageBuilder([16]byte{1}).InvalidateInode(7, 1, 1)); err != nil {
		t.Fatal(err)
	}
	receive(t, gotB, TypeInvalidateInode)
}
