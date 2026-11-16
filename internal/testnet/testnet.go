// Package testnet gives tests local ports that their own traffic reaches. On
// a Kubernetes node, kube-proxy sends local traffic for NodePorts (30000-32767
// by default), 127.0.0.1 included, to its services. Where the ephemeral port
// range overlaps them, a port a test binds can be answered by a pod, or
// refused.
package testnet

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"time"
)

const tries = 20

// Listen is net.Listen that binds again until a connection to the bound
// address reaches the new socket.
func Listen(network, addr string) (net.Listener, error) {
	var unreachable []net.Listener
	defer func() {
		for _, ln := range unreachable {
			_ = ln.Close()
		}
	}()
	for range tries {
		ln, err := net.Listen(network, addr)
		if err != nil {
			return nil, err
		}
		if reaches(ln) {
			return ln, nil
		}
		unreachable = append(unreachable, ln)
	}
	return nil, fmt.Errorf("no port for %s that connections reach after %d tries", addr, tries)
}

// LoopbackAddr returns a free 127.0.0.1 address that connections reach, for a
// process the test starts to listen on. Another process can still take the
// port before that one binds it.
func LoopbackAddr() (string, error) {
	ln, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	return addr, ln.Close()
}

// UDPPorts returns n distinct ports, free on all interfaces, that datagrams
// sent to 127.0.0.1 reach.
func UDPPorts(n int) ([]int, error) {
	var held, unreachable []*net.UDPConn
	defer func() {
		for _, c := range append(held, unreachable...) {
			_ = c.Close()
		}
	}()
	for len(held) < n && len(unreachable) < tries {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{})
		if err != nil {
			return nil, err
		}
		if receives(conn) {
			held = append(held, conn)
		} else {
			unreachable = append(unreachable, conn)
		}
	}
	if len(held) < n {
		return nil, fmt.Errorf("found %d of %d UDP ports that datagrams reach after %d misses", len(held), n, tries)
	}
	ports := make([]int, n)
	for i, c := range held {
		ports[i] = c.LocalAddr().(*net.UDPAddr).Port
	}
	return ports, nil
}

// NewHTTPServer is httptest.NewServer on a port that requests reach.
func NewHTTPServer(h http.Handler) (*httptest.Server, error) {
	ln, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	srv := httptest.NewUnstartedServer(h)
	_ = srv.Listener.Close()
	srv.Listener = ln
	srv.Start()
	return srv, nil
}

func reaches(ln net.Listener) bool {
	tl, ok := ln.(*net.TCPListener)
	if !ok {
		return true
	}
	conn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	if err := tl.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return false
	}
	defer func() { _ = tl.SetDeadline(time.Time{}) }()
	got, err := tl.Accept()
	if err != nil {
		return false
	}
	defer func() { _ = got.Close() }()
	return got.RemoteAddr().String() == conn.LocalAddr().String()
}

func receives(conn *net.UDPConn) bool {
	port := conn.LocalAddr().(*net.UDPAddr).Port
	probe := []byte(fmt.Sprintf("testnet probe %d %d", port, time.Now().UnixNano()))
	sender, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	if err != nil {
		return false
	}
	defer func() { _ = sender.Close() }()
	if _, err := sender.Write(probe); err != nil {
		return false
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		return false
	}
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	buf := make([]byte, len(probe)+1)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return false
		}
		if bytes.Equal(buf[:n], probe) {
			return true
		}
	}
}
