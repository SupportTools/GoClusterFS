package broadcast

import (
	"log"
	"net"
	"net/netip"
	"sort"
	"time"
)

// silentAfter is how many heartbeat intervals a node may go unheard before
// it counts as silent.
const silentAfter = 3

// ExpectedPeer is a node that should be sending this node broadcasts.
type ExpectedPeer struct {
	UUID [16]byte
	Name string
	// Addr is where the node takes unicast, from its registry slot; invalid when it advertises none.
	Addr netip.AddrPort
}

// WatchPeers has the broadcaster check, every heartbeat interval, that it
// hears from each node that expected lists. A node silent for silentAfter
// intervals since this node first saw it listed, by this node's clock, is
// reported: dropped multicast is otherwise invisible, because a node that
// hears nothing from a peer finds no gap in its sequence numbers either.
func (b *Broadcaster) WatchPeers(expected func() ([]ExpectedPeer, error)) {
	b.expected.Store(&expected)
	b.relistSoon()
}

func (b *Broadcaster) relistSoon() {
	select {
	case b.relist <- struct{}{}:
	default:
	}
}

// SilentPeers names the expected nodes not heard from for the returned time.
func (b *Broadcaster) SilentPeers() ([]string, time.Duration) {
	b.silentMu.Lock()
	defer b.silentMu.Unlock()
	names := make([]string, 0, len(b.silent))
	for _, p := range b.silent {
		names = append(names, p.Name)
	}
	sort.Strings(names)
	return names, b.silenceThreshold()
}

func (b *Broadcaster) silenceThreshold() time.Duration {
	return silentAfter * b.transport.config.HeartbeatInterval
}

func (b *Broadcaster) watchLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.transport.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case now := <-ticker.C:
			b.checkPeers(now)
		case <-b.relist:
			b.checkPeers(time.Now())
		}
	}
}

func (b *Broadcaster) checkPeers(now time.Time) {
	expected := b.expected.Load()
	if expected == nil {
		return
	}
	peers, err := (*expected)()
	if err != nil {
		log.Printf("[broadcast] could not list the nodes that should be sending broadcasts: %v", err)
		return
	}

	quiet := b.silenceThreshold()
	local := localAddrs()
	var ask []netip.AddrPort
	defer func() { b.requestUnicast(ask) }()
	b.silentMu.Lock()
	defer b.silentMu.Unlock()
	listed := make(map[[16]byte]time.Time, len(peers))
	silent := map[[16]byte]ExpectedPeer{}
	b.sendersMu.Lock()
	addrs := make(map[[16]byte]netip.AddrPort, len(peers))
	for _, p := range peers {
		if p.UUID == b.localUUID {
			continue
		}
		if addr, known := b.peerAddr(p, local); known {
			addrs[p.UUID] = addr
			if s, ok := b.senders[p.UUID]; !ok || now.Sub(s.multicast) > quiet {
				ask = append(ask, addr)
			}
		}
		first, ok := b.firstListed[p.UUID]
		if !ok {
			first = now
		}
		listed[p.UUID] = first
		last := b.started
		if first.After(last) {
			last = first
		}
		if s, ok := b.senders[p.UUID]; ok && s.heard.After(last) {
			last = s.heard
		}
		if now.Sub(last) > quiet {
			silent[p.UUID] = p
		}
	}
	b.peerAddrs = addrs
	b.applyRequestsLocked(now)
	b.sendersMu.Unlock()

	for id, p := range silent {
		if _, ok := b.silent[id]; !ok {
			log.Printf("[broadcast] WARN: no broadcast heard from node %s for over %v, though the registry shows it mounted; this node sees its changes only after -cache-ttl. Multicast may not reach this node, or that node runs with -broadcast=false or another -mcast-group or -mcast-port",
				p.Name, quiet)
		}
	}
	for id, p := range b.silent {
		if _, ok := silent[id]; !ok {
			if _, still := listed[id]; still {
				log.Printf("[broadcast] heard from node %s again", p.Name)
			}
		}
	}
	b.silent, b.firstListed = silent, listed
}

// requestUnicast asks each peer at addrs, whose multicast this node misses, to
// also send to this node's registry address.
func (b *Broadcaster) requestUnicast(addrs []netip.AddrPort) {
	for _, addr := range addrs {
		if err := b.transport.SendTo(b.builder.UnicastRequest(), net.UDPAddrFromAddrPort(addr)); err != nil {
			log.Printf("[broadcast] asking %s for unicast failed: %v", addr, err)
		}
	}
}

func (b *Broadcaster) handleUnicastRequest(sender [16]byte, now time.Time) {
	b.sendersMu.Lock()
	defer b.sendersMu.Unlock()
	b.requests[sender] = now
	if _, known := b.peerAddrs[sender]; !known {
		b.relistSoon()
		return
	}
	b.applyRequestsLocked(now)
}

// applyRequestsLocked sends unicast to the registry address of every peer that
// asked within the silence threshold. The caller holds sendersMu.
func (b *Broadcaster) applyRequestsLocked(now time.Time) {
	var addrs []*net.UDPAddr
	for id, at := range b.requests {
		if now.Sub(at) > b.silenceThreshold() {
			delete(b.requests, id)
			continue
		}
		if addr, ok := b.peerAddrs[id]; ok {
			addrs = append(addrs, net.UDPAddrFromAddrPort(addr))
		}
	}
	b.transport.SetRequestedPeers(addrs)
}

// peerAddr is where a listed peer takes unicast: its slot's address or, while
// its orphan list leaves no room for one, the last address read. The caller holds sendersMu.
func (b *Broadcaster) peerAddr(p ExpectedPeer, local map[netip.Addr]bool) (netip.AddrPort, bool) {
	if p.Addr.IsValid() {
		return p.Addr, !b.isSelf(p.Addr, local)
	}
	addr, ok := b.peerAddrs[p.UUID]
	return addr, ok
}

// isSelf says whether addr reaches this node: under QEMU user networking every
// node has the same address, so a peer's advertised address can be this node's.
func (b *Broadcaster) isSelf(addr netip.AddrPort, local map[netip.Addr]bool) bool {
	ip := addr.Addr().Unmap()
	return addr.Port() == b.localPort && (ip.IsLoopback() || ip.IsUnspecified() || local[ip])
}

func localAddrs() map[netip.Addr]bool {
	local := map[netip.Addr]bool{}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return local
	}
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok {
			if ip, ok := netip.AddrFromSlice(n.IP); ok {
				local[ip.Unmap()] = true
			}
		}
	}
	return local
}
