package broadcast

import (
	"context"
	"net"
	"net/netip"
	"slices"
	"sync"
	"testing"
	"time"
)

func loopbackAddr(port int) netip.AddrPort {
	return netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port))
}

func listenLoopback(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func readMessage(conn *net.UDPConn, wait time.Duration) *Message {
	buf := make([]byte, MaxMessageSize)
	_ = conn.SetReadDeadline(time.Now().Add(wait))
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return nil
	}
	msg, err := Unmarshal(buf[:n], nil)
	if err != nil {
		return nil
	}
	return msg
}

func TestAdvertiseAddr(t *testing.T) {
	cfg := DefaultConfig()
	for override, want := range map[string]string{
		"10.1.2.3":           "10.1.2.3:24731",
		"10.1.2.3:9000":      "10.1.2.3:9000",
		"[fd00::1]:7":        "[fd00::1]:7",
		"::ffff:10.1.2.3":    "10.1.2.3:24731",
		"[::ffff:1.2.3.4]:5": "1.2.3.4:5",
	} {
		got, err := AdvertiseAddr(cfg, override)
		if err != nil || got.String() != want {
			t.Errorf("AdvertiseAddr(%q) = %v, %v; want %s", override, got, err, want)
		}
	}
	for _, override := range []string{"node1", "0.0.0.0", "10.1.2.3:0", "[::]:24731"} {
		if got, err := AdvertiseAddr(cfg, override); err == nil {
			t.Errorf("AdvertiseAddr(%q) = %v, want an error", override, got)
		}
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, ifi := range ifaces {
		if ifi.Flags&net.FlagLoopback != 0 {
			cfg.Interface = ifi.Name
			if got, err := AdvertiseAddr(cfg, ""); err == nil {
				t.Errorf("AdvertiseAddr on loopback interface %s = %v, want an error: peers cannot reach it", ifi.Name, got)
			}
			break
		}
	}
}

// A node asks for unicast from each registry peer whose multicast it has not
// heard, and from no other node. The request carries sequence 0.
func TestNodeAsksPeersItMissesOnMulticastForUnicast(t *testing.T) {
	ports := freeUDPPorts(t, 2)
	cfg := DefaultConfig()
	cfg.MulticastPort, cfg.UnicastPort = ports[0], ports[1]
	cfg.HeartbeatInterval = time.Hour
	local, missed, heard := [16]byte{1}, [16]byte{2}, [16]byte{3}
	b, err := NewBroadcaster(cfg, local, 0, newMockHandler())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop() })

	missedConn, heardConn := listenLoopback(t), listenLoopback(t)
	b.sendersMu.Lock()
	b.senders[heard] = &senderSeq{heard: time.Now(), multicast: time.Now()}
	b.sendersMu.Unlock()
	peers := []ExpectedPeer{
		{UUID: local, Name: "self", Addr: loopbackAddr(cfg.UnicastPort)},
		{UUID: missed, Name: "missed", Addr: loopbackAddr(missedConn.LocalAddr().(*net.UDPAddr).Port)},
		{UUID: heard, Name: "heard", Addr: loopbackAddr(heardConn.LocalAddr().(*net.UDPAddr).Port)},
	}
	b.WatchPeers(func() ([]ExpectedPeer, error) { return peers, nil })

	msg := readMessage(missedConn, 3*time.Second)
	if msg == nil || msg.Type != TypeUnicastRequest || msg.Sequence != 0 || msg.SenderUUID != local {
		t.Fatalf("the peer whose multicast is missed got %+v, want a UnicastRequest with sequence 0 from this node", msg)
	}
	if msg := readMessage(heardConn, 300*time.Millisecond); msg != nil {
		t.Errorf("the peer heard on multicast got %v, want nothing", msg.Type)
	}
}

// A peer that asked gets every message by unicast at its registry address,
// even while multicast sends succeed, until its request is older than the
// silence threshold. A request does not count as a sequenced message.
func TestRequestedPeersGetUnicastWhileMulticastWorks(t *testing.T) {
	sink, peerConn, conn := listenLoopback(t), listenLoopback(t), listenLoopback(t)
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = time.Second
	b, err := NewBroadcaster(cfg, [16]byte{1}, 0, newMockHandler())
	if err != nil {
		t.Fatal(err)
	}
	tr := b.transport
	tr.unicastConn, tr.multicastConn, tr.multicastAddr = conn, conn, sink.LocalAddr().(*net.UDPAddr)

	asker, stranger := [16]byte{2}, [16]byte{3}
	b.peerAddrs[asker] = peerConn.LocalAddr().(*net.UDPAddr).AddrPort()
	now := time.Now()
	b.handleMessage(NewMessageBuilder(asker).UnicastRequest(), nil, false)
	b.handleMessage(NewMessageBuilder(stranger).UnicastRequest(), nil, false)
	if len(b.senders) != 0 || b.Stats().MessagesLost != 0 {
		t.Errorf("unicast requests were tracked as sequenced messages: senders %d, lost %d", len(b.senders), b.Stats().MessagesLost)
	}
	if len(b.relist) != 1 {
		t.Errorf("a request from a node missing from the registry listing did not ask for a new listing")
	}

	if err := tr.deliver([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	if readRaw(sink) == "" {
		t.Errorf("the multicast send did not happen")
	}
	if got := readRaw(peerConn); got != "payload" {
		t.Errorf("the peer that asked got %q by unicast while multicast works, want the payload", got)
	}

	b.sendersMu.Lock()
	b.applyRequestsLocked(now.Add(b.silenceThreshold() + time.Second))
	b.sendersMu.Unlock()
	if err := tr.deliver([]byte("later")); err != nil {
		t.Fatal(err)
	}
	if got := readRaw(peerConn); got != "" {
		t.Errorf("the peer got %q after its request expired, want nothing", got)
	}
}

func readRaw(conn *net.UDPConn) string {
	buf := make([]byte, 64)
	_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return ""
	}
	return string(buf[:n])
}

// Under QEMU user networking every node advertises the same address, so a
// node must not take its own address and port for a peer's.
func TestOwnAddressIsNotAPeer(t *testing.T) {
	cfg := DefaultConfig()
	b, err := NewBroadcaster(cfg, [16]byte{1}, 0, newMockHandler())
	if err != nil {
		t.Fatal(err)
	}
	guest := netip.MustParseAddr("10.0.2.15")
	local := map[netip.Addr]bool{guest: true}
	port := uint16(cfg.UnicastPort)
	for addr, self := range map[netip.AddrPort]bool{
		netip.AddrPortFrom(guest, port):                            true,
		netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port): true,
		netip.AddrPortFrom(guest, port+1):                          false,
		netip.AddrPortFrom(netip.MustParseAddr("10.0.2.16"), port): false,
	} {
		if got := b.isSelf(addr, local); got != self {
			t.Errorf("isSelf(%v) = %v, want %v", addr, got, self)
		}
	}

	twin, other := [16]byte{2}, [16]byte{3}
	b.peerAddrs[twin] = netip.AddrPortFrom(netip.MustParseAddr("10.0.2.16"), port)
	if addr, ok := b.peerAddr(ExpectedPeer{UUID: twin, Addr: netip.AddrPortFrom(guest, port)}, local); ok {
		t.Errorf("a peer advertising this node's own address is reached at %v, want not at all", addr)
	}
	want := netip.AddrPortFrom(netip.MustParseAddr("10.0.2.17"), port)
	if addr, ok := b.peerAddr(ExpectedPeer{UUID: other, Addr: want}, local); !ok || addr != want {
		t.Errorf("a peer advertising %v is reached at %v, %v", want, addr, ok)
	}
}

// A peer whose slot drops its address, because its orphan list outgrew the
// room, keeps its last address while it stays listed: it still gets the
// unicast it asked for and is still asked for unicast. Leaving the listing drops it.
func TestPeerKeepsItsLastAddressWhileListed(t *testing.T) {
	ports := freeUDPPorts(t, 2)
	cfg := DefaultConfig()
	cfg.MulticastPort, cfg.UnicastPort = ports[0], ports[1]
	cfg.HeartbeatInterval = time.Hour
	local, peer := [16]byte{1}, [16]byte{2}
	b, err := NewBroadcaster(cfg, local, 0, newMockHandler())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop() })

	peerConn := listenLoopback(t)
	addr := loopbackAddr(peerConn.LocalAddr().(*net.UDPAddr).Port)
	var mu sync.Mutex
	listing := []ExpectedPeer{{UUID: peer, Name: "peer", Addr: addr}}
	relist := func(peers []ExpectedPeer) {
		mu.Lock()
		listing = peers
		mu.Unlock()
		b.relistSoon()
	}
	b.WatchPeers(func() ([]ExpectedPeer, error) {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(listing), nil
	})
	if msg := readMessage(peerConn, 3*time.Second); msg == nil || msg.Type != TypeUnicastRequest {
		t.Fatalf("the listed peer got %+v, want a UnicastRequest", msg)
	}
	b.handleUnicastRequest(peer, time.Now())

	relist([]ExpectedPeer{{UUID: peer, Name: "peer"}})
	if msg := readMessage(peerConn, 3*time.Second); msg == nil || msg.Type != TypeUnicastRequest {
		t.Errorf("after its slot lost the address, the peer got %+v, want a UnicastRequest at its last address", msg)
	}
	if got := b.transport.requestedPeers(); len(got) != 1 || got[0].AddrPort() != addr {
		t.Errorf("after its slot lost the address, the peer that asked gets unicast at %v, want [%v]", got, addr)
	}

	relist(nil)
	deadline := time.Now().Add(3 * time.Second)
	for len(b.transport.requestedPeers()) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := b.transport.requestedPeers(); len(got) != 0 {
		t.Errorf("after the peer left the listing it still gets unicast at %v", got)
	}
}
