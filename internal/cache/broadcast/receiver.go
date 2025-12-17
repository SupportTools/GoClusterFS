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

// InvalidationHandler is called when an invalidation message is received.
type InvalidationHandler interface {
	// InvalidateInode invalidates a cached inode.
	InvalidateInode(inodeNum uint32)

	// InvalidateExtentsForInode invalidates cached extents for an inode.
	InvalidateExtentsForInode(inodeNum uint32)

	// InvalidateAll clears all caches.
	Clear()
}

// Broadcaster manages cache invalidation broadcasting.
type Broadcaster struct {
	transport   *Transport
	builder     *MessageBuilder
	handler     InvalidationHandler
	localUUID   [16]byte
	nodeSlot    uint32
	localPort   uint16

	// Deduplication
	seenMu sync.RWMutex
	seen   map[uint64]time.Time // sequence -> first seen time

	// Stats
	invalidationsReceived uint64
	invalidationsSent     uint64
	duplicatesDropped     uint64
	messagesIgnored       uint64 // messages from self

	// Lifecycle
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	running    atomic.Bool
}

// NewBroadcaster creates a new cache invalidation broadcaster.
func NewBroadcaster(cfg Config, localUUID [16]byte, nodeSlot uint32, handler InvalidationHandler) (*Broadcaster, error) {
	transport, err := NewTransport(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	b := &Broadcaster{
		transport: transport,
		builder:   NewMessageBuilder(localUUID),
		handler:   handler,
		localUUID: localUUID,
		nodeSlot:  nodeSlot,
		localPort: uint16(cfg.UnicastPort),
		seen:      make(map[uint64]time.Time),
	}

	return b, nil
}

// Start begins the broadcaster.
func (b *Broadcaster) Start(ctx context.Context) error {
	if !b.transport.config.Enabled {
		log.Printf("[broadcast] disabled, using storage-only cache validation")
		return nil
	}

	if b.running.Swap(true) {
		return fmt.Errorf("broadcaster already running")
	}

	b.ctx, b.cancel = context.WithCancel(ctx)

	// Start transport with our message handler
	if err := b.transport.Start(b.ctx, b.handleMessage); err != nil {
		b.running.Store(false)
		return fmt.Errorf("failed to start transport: %w", err)
	}

	// Start background tasks
	b.wg.Add(2)
	go b.heartbeatLoop()
	go b.cleanupLoop()

	// Announce ourselves
	b.announcePresence()

	log.Printf("[broadcast] started on port %d (multicast: %s:%d)",
		b.transport.config.UnicastPort,
		b.transport.config.MulticastGroup,
		b.transport.config.MulticastPort)

	return nil
}

// Stop shuts down the broadcaster.
func (b *Broadcaster) Stop() error {
	if !b.running.Swap(false) {
		return nil
	}

	if b.cancel != nil {
		b.cancel()
	}

	b.wg.Wait()

	if err := b.transport.Stop(); err != nil {
		return err
	}

	log.Printf("[broadcast] stopped")
	return nil
}

// handleMessage processes an incoming broadcast message.
func (b *Broadcaster) handleMessage(msg *Message, addr *net.UDPAddr) {
	// Ignore messages from ourselves
	if msg.SenderUUID == b.localUUID {
		atomic.AddUint64(&b.messagesIgnored, 1)
		return
	}

	// Deduplicate
	if b.isDuplicate(msg.Sequence) {
		atomic.AddUint64(&b.duplicatesDropped, 1)
		return
	}

	switch msg.Type {
	case TypeInvalidateInode:
		b.handleInvalidateInode(msg)

	case TypeInvalidateExtent:
		b.handleInvalidateExtent(msg)

	case TypeInvalidateAll:
		b.handleInvalidateAll(msg)

	case TypeHeartbeat:
		b.handleHeartbeat(msg, addr)

	case TypePeerAnnounce:
		b.handlePeerAnnounce(msg, addr)
	}
}

// handleInvalidateInode processes an inode invalidation message.
func (b *Broadcaster) handleInvalidateInode(msg *Message) {
	payload, err := UnmarshalInvalidateInodePayload(msg.Payload)
	if err != nil {
		return
	}

	atomic.AddUint64(&b.invalidationsReceived, 1)

	if b.handler != nil {
		b.handler.InvalidateInode(payload.InodeNum)
	}
}

// handleInvalidateExtent processes an extent invalidation message.
func (b *Broadcaster) handleInvalidateExtent(msg *Message) {
	payload, err := UnmarshalInvalidateExtentPayload(msg.Payload)
	if err != nil {
		return
	}

	atomic.AddUint64(&b.invalidationsReceived, 1)

	if b.handler != nil {
		b.handler.InvalidateExtentsForInode(payload.InodeNum)
	}
}

// handleInvalidateAll processes a full cache invalidation message.
func (b *Broadcaster) handleInvalidateAll(msg *Message) {
	atomic.AddUint64(&b.invalidationsReceived, 1)

	if b.handler != nil {
		b.handler.Clear()
	}

	log.Printf("[broadcast] received InvalidateAll from %x", msg.SenderUUID[:8])
}

// handleHeartbeat processes a heartbeat message.
func (b *Broadcaster) handleHeartbeat(msg *Message, addr *net.UDPAddr) {
	// Update peer list for unicast fallback
	uuidStr := fmt.Sprintf("%x", msg.SenderUUID)
	b.transport.AddPeer(uuidStr, addr)
}

// handlePeerAnnounce processes a peer announcement message.
func (b *Broadcaster) handlePeerAnnounce(msg *Message, addr *net.UDPAddr) {
	payload, err := UnmarshalPeerAnnouncePayload(msg.Payload)
	if err != nil {
		return
	}

	// Register peer for unicast
	uuidStr := fmt.Sprintf("%x", msg.SenderUUID)
	peerAddr := &net.UDPAddr{
		IP:   net.IP(payload.IPAddr[:]),
		Port: int(payload.Port),
	}

	// Use the address from the payload if valid, otherwise use source address
	if peerAddr.IP.IsUnspecified() {
		peerAddr = addr
	}

	b.transport.AddPeer(uuidStr, peerAddr)

	log.Printf("[broadcast] peer announced: slot=%d uuid=%s addr=%s",
		payload.NodeSlot, uuidStr[:16], peerAddr)
}

// isDuplicate checks if we've seen this sequence number recently.
func (b *Broadcaster) isDuplicate(seq uint64) bool {
	b.seenMu.Lock()
	defer b.seenMu.Unlock()

	if _, exists := b.seen[seq]; exists {
		return true
	}

	b.seen[seq] = time.Now()
	return false
}

// heartbeatLoop sends periodic heartbeats.
func (b *Broadcaster) heartbeatLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.transport.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			msg := b.builder.Heartbeat()
			if err := b.transport.Send(msg); err != nil {
				// Heartbeat failures are expected during network issues
			}
		}
	}
}

// cleanupLoop periodically cleans up old deduplication entries.
func (b *Broadcaster) cleanupLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.cleanupSeen()
		}
	}
}

// cleanupSeen removes old entries from the deduplication map.
func (b *Broadcaster) cleanupSeen() {
	b.seenMu.Lock()
	defer b.seenMu.Unlock()

	cutoff := time.Now().Add(-5 * time.Minute)
	for seq, seen := range b.seen {
		if seen.Before(cutoff) {
			delete(b.seen, seq)
		}
	}
}

// announcePresence broadcasts our presence to the cluster.
func (b *Broadcaster) announcePresence() {
	localAddr := b.transport.LocalAddr()
	var ipAddr [16]byte

	if udpAddr, ok := localAddr.(*net.UDPAddr); ok && udpAddr != nil {
		// Convert to IPv6 format
		ip := udpAddr.IP.To16()
		if ip != nil {
			copy(ipAddr[:], ip)
		}
	}

	msg := b.builder.PeerAnnounce(b.nodeSlot, b.localPort, ipAddr)
	if err := b.transport.Send(msg); err != nil {
		log.Printf("[broadcast] failed to announce presence: %v", err)
	}
}

// BroadcastInvalidateInode sends an inode invalidation to all peers.
func (b *Broadcaster) BroadcastInvalidateInode(inodeNum uint32, dataGen, metaGen uint64) {
	if !b.running.Load() {
		return
	}

	msg := b.builder.InvalidateInode(inodeNum, dataGen, metaGen)
	if err := b.transport.Send(msg); err != nil {
		// Log but don't fail - correctness comes from generation counters
		log.Printf("[broadcast] failed to send inode invalidation: %v", err)
	}
	atomic.AddUint64(&b.invalidationsSent, 1)
}

// BroadcastInvalidateExtent sends an extent invalidation to all peers.
func (b *Broadcaster) BroadcastInvalidateExtent(inodeNum uint32, dataGen uint64) {
	if !b.running.Load() {
		return
	}

	msg := b.builder.InvalidateExtent(inodeNum, dataGen)
	if err := b.transport.Send(msg); err != nil {
		log.Printf("[broadcast] failed to send extent invalidation: %v", err)
	}
	atomic.AddUint64(&b.invalidationsSent, 1)
}

// BroadcastInvalidateAll sends a full cache invalidation to all peers.
func (b *Broadcaster) BroadcastInvalidateAll() {
	if !b.running.Load() {
		return
	}

	msg := b.builder.InvalidateAll()
	if err := b.transport.Send(msg); err != nil {
		log.Printf("[broadcast] failed to send InvalidateAll: %v", err)
	}
	atomic.AddUint64(&b.invalidationsSent, 1)

	log.Printf("[broadcast] sent InvalidateAll to cluster")
}

// Stats returns broadcaster statistics.
func (b *Broadcaster) Stats() BroadcasterStats {
	return BroadcasterStats{
		InvalidationsReceived: atomic.LoadUint64(&b.invalidationsReceived),
		InvalidationsSent:     atomic.LoadUint64(&b.invalidationsSent),
		DuplicatesDropped:     atomic.LoadUint64(&b.duplicatesDropped),
		MessagesIgnored:       atomic.LoadUint64(&b.messagesIgnored),
		Transport:             b.transport.Stats(),
	}
}

// BroadcasterStats holds broadcaster statistics.
type BroadcasterStats struct {
	InvalidationsReceived uint64
	InvalidationsSent     uint64
	DuplicatesDropped     uint64
	MessagesIgnored       uint64
	Transport             TransportStats
}

// ResetStats resets all statistics.
func (b *Broadcaster) ResetStats() {
	atomic.StoreUint64(&b.invalidationsReceived, 0)
	atomic.StoreUint64(&b.invalidationsSent, 0)
	atomic.StoreUint64(&b.duplicatesDropped, 0)
	atomic.StoreUint64(&b.messagesIgnored, 0)
}

// IsEnabled returns whether broadcasting is enabled.
func (b *Broadcaster) IsEnabled() bool {
	return b.transport.config.Enabled
}
