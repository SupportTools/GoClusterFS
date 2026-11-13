package broadcast

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// readBufferSize is what each socket asks for; Linux caps it at net.core.rmem_max.
const readBufferSize = 1 << 20

// Transport handles UDP multicast and unicast communication.
type Transport struct {
	config Config

	// Multicast connection (receive only; sends go through unicastConn)
	multicastConn *net.UDPConn
	multicastAddr *net.UDPAddr

	// Unicast connection (fallback when multicast fails)
	unicastConn *net.UDPConn

	// Peer tracking for unicast fallback
	peersMu sync.RWMutex
	peers   map[string]*net.UDPAddr // UUID string -> address

	// Statistics
	multicastSent     uint64
	multicastRecv     uint64
	unicastSent       uint64
	unicastRecv       uint64
	multicastFailures uint64

	// State
	running   atomic.Bool
	closeChan chan struct{}
	wg        sync.WaitGroup
}

// Config holds network broadcast configuration.
type Config struct {
	// Enabled controls whether network broadcast is active.
	// When false, cache invalidation relies solely on generation counters.
	Enabled bool

	// MulticastGroup is the multicast IP address for cluster communication.
	// Default: "239.192.74.75" (administratively scoped multicast)
	MulticastGroup string

	// MulticastPort is the UDP port for multicast communication.
	// Default: 31337
	MulticastPort int

	// UnicastPort is the UDP port for unicast messages. It must differ from
	// MulticastPort, which the multicast socket holds.
	// Default: 31338
	UnicastPort int

	// MulticastTTL controls how far multicast packets travel.
	// Default: 1 (same subnet only)
	MulticastTTL int

	// MaxMulticastFailures is the threshold before switching to unicast.
	// Default: 3
	MaxMulticastFailures int

	// HeartbeatInterval controls how often heartbeats are sent.
	// Default: 30s
	HeartbeatInterval time.Duration

	// Secret is the shared secret for HMAC authentication.
	// Empty string disables authentication.
	Secret string

	// Interface is the network interface for multicast.
	// Empty string uses the default interface.
	Interface string
}

// DefaultConfig returns sensible defaults for network broadcast.
func DefaultConfig() Config {
	return Config{
		Enabled:              true,
		MulticastGroup:       "239.192.74.75",
		MulticastPort:        31337,
		UnicastPort:          31338,
		MulticastTTL:         1,
		MaxMulticastFailures: 3,
		HeartbeatInterval:    30 * time.Second,
	}
}

// DisabledConfig returns a config with broadcast disabled.
func DisabledConfig() Config {
	return Config{Enabled: false}
}

// NewTransport creates a new broadcast transport.
func NewTransport(cfg Config) (*Transport, error) {
	if !cfg.Enabled {
		return &Transport{
			config:    cfg,
			closeChan: make(chan struct{}),
			peers:     make(map[string]*net.UDPAddr),
		}, nil
	}

	// Parse multicast address
	multicastAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", cfg.MulticastGroup, cfg.MulticastPort))
	if err != nil {
		return nil, fmt.Errorf("invalid multicast address: %w", err)
	}

	t := &Transport{
		config:        cfg,
		multicastAddr: multicastAddr,
		closeChan:     make(chan struct{}),
		peers:         make(map[string]*net.UDPAddr),
	}

	return t, nil
}

// Start begins listening for broadcast messages.
func (t *Transport) Start(ctx context.Context, handler func(*Message, *net.UDPAddr)) error {
	if !t.config.Enabled {
		return nil
	}

	if t.running.Swap(true) {
		return fmt.Errorf("transport already running")
	}

	multicastConn, unicastConn, mcastErr, err := listen(t.config, t.multicastAddr)
	if err != nil {
		return err
	}
	if mcastErr != nil {
		log.Printf("[broadcast] multicast unavailable: %v, using unicast only", mcastErr)
	}
	t.multicastConn, t.unicastConn = multicastConn, unicastConn

	// Start receiver goroutines
	if t.multicastConn != nil {
		t.wg.Add(1)
		go t.receiveLoop(t.multicastConn, handler, true)
	}

	t.wg.Add(1)
	go t.receiveLoop(t.unicastConn, handler, false)

	return nil
}

// CheckPorts opens and releases the sockets Start would, so a mount can fail
// on a taken port before it changes anything on disk.
func CheckPorts(cfg Config) error {
	if !cfg.Enabled {
		return nil
	}
	group, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", cfg.MulticastGroup, cfg.MulticastPort))
	if err != nil {
		return fmt.Errorf("invalid multicast address: %w", err)
	}
	multicastConn, unicastConn, _, err := listen(cfg, group)
	if err != nil {
		return err
	}
	if multicastConn != nil {
		multicastConn.Close()
	}
	return unicastConn.Close()
}

// listen opens the multicast and unicast sockets. If multicast is unavailable,
// multicastConn is nil and mcastErr says why; the unicast socket must bind.
func listen(cfg Config, group *net.UDPAddr) (multicastConn, unicastConn *net.UDPConn, mcastErr, err error) {
	if cfg.UnicastPort == cfg.MulticastPort {
		return nil, nil, nil, fmt.Errorf("unicast port %d is also the multicast port; the multicast socket holds it, so the unicast port must differ", cfg.UnicastPort)
	}

	var ifi *net.Interface
	if cfg.Interface != "" {
		if ifi, err = net.InterfaceByName(cfg.Interface); err != nil {
			return nil, nil, nil, fmt.Errorf("interface %s not found: %w", cfg.Interface, err)
		}
	}

	multicastConn, mcastErr = net.ListenMulticastUDP("udp", ifi, group)
	if mcastErr != nil {
		multicastConn = nil
	} else if err := multicastConn.SetReadBuffer(readBufferSize); err != nil {
		log.Printf("[broadcast] failed to set read buffer: %v", err)
	}

	unicastConn, err = net.ListenUDP("udp", &net.UDPAddr{Port: cfg.UnicastPort})
	if err != nil {
		if multicastConn != nil {
			multicastConn.Close()
		}
		return nil, nil, nil, fmt.Errorf("failed to bind unicast port: %w", err)
	}
	if err := unicastConn.SetReadBuffer(readBufferSize); err != nil {
		log.Printf("[broadcast] failed to set read buffer: %v", err)
	}

	if multicastConn != nil {
		ttl := cfg.MulticastTTL
		if ttl <= 0 {
			ttl = 1
		}
		if err := setMulticastSendOptions(unicastConn, ifi, ttl); err != nil {
			multicastConn.Close()
			unicastConn.Close()
			return nil, nil, nil, fmt.Errorf("failed to set multicast options on the send socket: %w", err)
		}
	}
	return multicastConn, unicastConn, mcastErr, nil
}

// receiveLoop reads messages from a UDP connection.
func (t *Transport) receiveLoop(conn *net.UDPConn, handler func(*Message, *net.UDPAddr), isMulticast bool) {
	defer t.wg.Done()

	buf := make([]byte, MaxMessageSize)
	secret := []byte(t.config.Secret)
	if len(secret) == 0 {
		secret = nil
	}

	for {
		select {
		case <-t.closeChan:
			return
		default:
		}

		// Set read deadline to allow checking closeChan
		if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			continue
		}

		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			// Connection closed or error
			select {
			case <-t.closeChan:
				return
			default:
				log.Printf("[broadcast] receive error: %v", err)
				continue
			}
		}

		// Parse message
		msg, err := Unmarshal(buf[:n], secret)
		if err != nil {
			// Invalid message, ignore
			continue
		}

		// Update stats
		if isMulticast {
			atomic.AddUint64(&t.multicastRecv, 1)
		} else {
			atomic.AddUint64(&t.unicastRecv, 1)
		}

		// Handle message
		if handler != nil {
			handler(msg, addr)
		}
	}
}

// Send broadcasts a message to all peers.
func (t *Transport) Send(msg *Message) error {
	if !t.config.Enabled {
		return nil
	}

	secret := []byte(t.config.Secret)
	if len(secret) == 0 {
		secret = nil
	}

	data, err := msg.Marshal(secret)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	// Try multicast first. It goes out on the unicast socket: ListenMulticastUDP
	// turns off multicast loopback on its socket, so other nodes on this host
	// would not receive what that socket sends.
	if t.multicastConn != nil && atomic.LoadUint64(&t.multicastFailures) < uint64(t.config.MaxMulticastFailures) {
		_, err := t.unicastConn.WriteToUDP(data, t.multicastAddr)
		if err != nil {
			atomic.AddUint64(&t.multicastFailures, 1)
			log.Printf("[broadcast] multicast send failed (%d failures): %v",
				atomic.LoadUint64(&t.multicastFailures), err)
		} else {
			atomic.AddUint64(&t.multicastSent, 1)
			return nil
		}
	}

	// Fall back to unicast
	return t.sendUnicast(data)
}

// sendUnicast sends a message to all known peers via unicast.
func (t *Transport) sendUnicast(data []byte) error {
	if t.unicastConn == nil {
		return fmt.Errorf("unicast connection not available")
	}

	t.peersMu.RLock()
	peers := make([]*net.UDPAddr, 0, len(t.peers))
	for _, addr := range t.peers {
		peers = append(peers, addr)
	}
	t.peersMu.RUnlock()

	var lastErr error
	for _, addr := range peers {
		if _, err := t.unicastConn.WriteToUDP(data, addr); err != nil {
			lastErr = err
		} else {
			atomic.AddUint64(&t.unicastSent, 1)
		}
	}

	return lastErr
}

// AddPeer registers a peer for unicast fallback.
func (t *Transport) AddPeer(uuid string, addr *net.UDPAddr) {
	t.peersMu.Lock()
	t.peers[uuid] = addr
	t.peersMu.Unlock()
}

// RemovePeer unregisters a peer.
func (t *Transport) RemovePeer(uuid string) {
	t.peersMu.Lock()
	delete(t.peers, uuid)
	t.peersMu.Unlock()
}

// PeerCount returns the number of known peers.
func (t *Transport) PeerCount() int {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()
	return len(t.peers)
}

// ResetMulticastFailures resets the failure counter to re-enable multicast.
func (t *Transport) ResetMulticastFailures() {
	atomic.StoreUint64(&t.multicastFailures, 0)
}

// Stats returns transport statistics.
func (t *Transport) Stats() TransportStats {
	return TransportStats{
		MulticastSent:     atomic.LoadUint64(&t.multicastSent),
		MulticastRecv:     atomic.LoadUint64(&t.multicastRecv),
		UnicastSent:       atomic.LoadUint64(&t.unicastSent),
		UnicastRecv:       atomic.LoadUint64(&t.unicastRecv),
		MulticastFailures: atomic.LoadUint64(&t.multicastFailures),
		PeerCount:         t.PeerCount(),
		MulticastEnabled:  t.multicastConn != nil,
	}
}

// TransportStats holds transport statistics.
type TransportStats struct {
	MulticastSent     uint64
	MulticastRecv     uint64
	UnicastSent       uint64
	UnicastRecv       uint64
	MulticastFailures uint64
	PeerCount         int
	MulticastEnabled  bool
}

// Stop shuts down the transport.
func (t *Transport) Stop() error {
	if !t.running.Swap(false) {
		return nil
	}

	close(t.closeChan)

	if t.multicastConn != nil {
		t.multicastConn.Close()
	}
	if t.unicastConn != nil {
		t.unicastConn.Close()
	}

	t.wg.Wait()
	return nil
}

// LocalAddr returns the local unicast address.
func (t *Transport) LocalAddr() net.Addr {
	if t.unicastConn != nil {
		return t.unicastConn.LocalAddr()
	}
	return nil
}
