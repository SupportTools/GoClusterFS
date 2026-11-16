package api

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	listen = reachableListen
	os.Exit(m.Run())
}

// reachableListen is net.Listen that binds again until a connection to the
// bound address reaches the new socket. On a Kubernetes node, kube-proxy sends
// local connections to NodePorts (30000-32767 by default) to its services, so
// a test server given such an ephemeral port gets another server's answers.
func reachableListen(network, addr string) (net.Listener, error) {
	var unreachable []net.Listener
	defer func() {
		for _, ln := range unreachable {
			_ = ln.Close()
		}
	}()
	for range 20 {
		ln, err := net.Listen(network, addr)
		if err != nil {
			return nil, err
		}
		if reaches(ln) {
			return ln, nil
		}
		unreachable = append(unreachable, ln)
	}
	return nil, fmt.Errorf("no port for %s that connections reach after 20 tries", addr)
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
