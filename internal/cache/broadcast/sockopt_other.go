//go:build !linux

package broadcast

import "net"

func setMulticastSendOptions(*net.UDPConn, *net.Interface, int) error { return nil }
