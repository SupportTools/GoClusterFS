package testnet

import (
	"net"
	"net/http"
	"testing"
)

func TestPortsReachThisProcess(t *testing.T) {
	ln, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	if !reaches(ln) {
		t.Errorf("Listen returned %s, which a connection does not reach", ln.Addr())
	}

	ports, err := UDPPorts(3)
	if err != nil {
		t.Fatal(err)
	}
	if ports[0] == ports[1] || ports[1] == ports[2] || ports[0] == ports[2] {
		t.Errorf("UDPPorts(3) = %v, want distinct ports", ports)
	}
	for _, p := range ports {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: p})
		if err != nil {
			t.Errorf("UDP port %d is not free: %v", p, err)
			continue
		}
		_ = conn.Close()
	}

	srv, err := NewHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("GET %s = %d, want this server's 418", srv.URL, resp.StatusCode)
	}
}
