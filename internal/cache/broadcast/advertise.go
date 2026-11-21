package broadcast

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
)

// AdvertiseAddr returns where peers that miss this node's multicast should send
// it unicast. override is an IP or IP:port; without a port it takes the unicast
// port. Empty picks the first global unicast address of cfg.Interface, or the
// source address of the route to the multicast group.
func AdvertiseAddr(cfg Config, override string) (netip.AddrPort, error) {
	port := uint16(cfg.UnicastPort)
	if override != "" {
		addr, err := netip.ParseAddrPort(override)
		if err != nil {
			ip, ipErr := netip.ParseAddr(override)
			if ipErr != nil {
				return netip.AddrPort{}, fmt.Errorf("%q is neither an IP nor an IP:port", override)
			}
			addr = netip.AddrPortFrom(ip, port)
		}
		if addr.Port() == 0 || addr.Addr().IsUnspecified() {
			return netip.AddrPort{}, fmt.Errorf("%q names no address peers can send to", override)
		}
		return netip.AddrPortFrom(addr.Addr().Unmap(), addr.Port()), nil
	}

	if cfg.Interface != "" {
		ifi, err := net.InterfaceByName(cfg.Interface)
		if err != nil {
			return netip.AddrPort{}, fmt.Errorf("interface %s: %w", cfg.Interface, err)
		}
		addrs, err := ifi.Addrs()
		if err != nil {
			return netip.AddrPort{}, fmt.Errorf("interface %s: %w", cfg.Interface, err)
		}
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok {
				if ip, ok := netip.AddrFromSlice(n.IP); ok && ip.Unmap().IsGlobalUnicast() {
					return netip.AddrPortFrom(ip.Unmap(), port), nil
				}
			}
		}
		return netip.AddrPort{}, fmt.Errorf("interface %s has no global unicast address", cfg.Interface)
	}

	// Connecting a UDP socket only picks the route; it sends nothing.
	conn, err := net.Dial("udp", net.JoinHostPort(cfg.MulticastGroup, strconv.Itoa(cfg.MulticastPort)))
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("no route to %s: %w", cfg.MulticastGroup, err)
	}
	defer func() { _ = conn.Close() }()
	local, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return netip.AddrPort{}, fmt.Errorf("no local address for the route to %s", cfg.MulticastGroup)
	}
	ip, ok := netip.AddrFromSlice(local.IP)
	if !ok || !ip.Unmap().IsGlobalUnicast() {
		return netip.AddrPort{}, fmt.Errorf("the route to %s leaves from %v, which peers cannot reach", cfg.MulticastGroup, local.IP)
	}
	return netip.AddrPortFrom(ip.Unmap(), port), nil
}
