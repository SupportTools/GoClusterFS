//go:build linux

package broadcast

import (
	"net"

	"golang.org/x/sys/unix"
)

// setMulticastSendOptions makes conn send multicast out of ifi, the interface
// the listener joined (nil leaves it to the route, which is also how the
// listener picked one), loop it back to other sockets on this host, and use ttl.
func setMulticastSendOptions(conn *net.UDPConn, ifi *net.Interface, ttl int) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	err = raw.Control(func(fd uintptr) {
		s := int(fd)
		if ifi != nil {
			if serr = unix.SetsockoptIPMreqn(s, unix.IPPROTO_IP, unix.IP_MULTICAST_IF, &unix.IPMreqn{Ifindex: int32(ifi.Index)}); serr != nil {
				return
			}
		}
		if serr = unix.SetsockoptInt(s, unix.IPPROTO_IP, unix.IP_MULTICAST_LOOP, 1); serr != nil {
			return
		}
		serr = unix.SetsockoptInt(s, unix.IPPROTO_IP, unix.IP_MULTICAST_TTL, ttl)
	})
	if err != nil {
		return err
	}
	return serr
}
