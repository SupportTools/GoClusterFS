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
	transport *Transport
	builder   *MessageBuilder
	handler   InvalidationHandler
	localUUID [16]byte
	nodeSlot  uint32
	localPort uint16

	sendMu sync.Mutex

	sendersMu sync.Mutex
	senders   map[[16]byte]*senderSeq

	lossLogMu    sync.Mutex
	lossLoggedAt time.Time

	clearMu      sync.Mutex
	clearRunning bool
	clearAgain   bool

	trailersLeft atomic.Int32

	// Stats
	invalidationsReceived uint64
	invalidationsSent     uint64
	duplicatesDropped     uint64
	messagesIgnored       uint64 // messages from self
	messagesLost          uint64

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running atomic.Bool
}

const (
	seqWindow = 64

	// After an invalidation, a node sends a heartbeat every trailerDelay, trailers
	// times, so a peer that lost the end of a burst still sees the sequence gap.
	trailerDelay = 50 * time.Millisecond
	trailers     = 10

	lossLogInterval = 10 * time.Second
	senderExpiry    = 5 * time.Minute
)

// senderSeq tracks one sender's sequence numbers. Bit i of seen marks
// highest-1-i as received.
type senderSeq struct {
	highest uint64
	seen    uint64
	heard   time.Time
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
		senders:   make(map[[16]byte]*senderSeq),
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

	dup, missed := b.track(msg.SenderUUID, msg.Sequence)
	if dup {
		atomic.AddUint64(&b.duplicatesDropped, 1)
		return
	}
	if missed > 0 {
		b.dropCachesAfterLoss(msg.SenderUUID, missed)
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
	b.transport.AddPeer(addr)
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

	b.transport.AddPeer(peerAddr)

	log.Printf("[broadcast] peer announced: slot=%d uuid=%s addr=%s",
		payload.NodeSlot, uuidStr[:16], peerAddr)
}

// track records seq from sender. It reports whether seq was already handled,
// and how many of the sender's messages were skipped before seq. A seq older
// than the window counts as handled: it was either seen, or counted missing
// when highest passed it.
func (b *Broadcaster) track(sender [16]byte, seq uint64) (dup bool, missed uint64) {
	b.sendersMu.Lock()
	defer b.sendersMu.Unlock()

	s, ok := b.senders[sender]
	if !ok {
		if len(b.senders) >= b.transport.config.maxPeers() {
			delete(b.senders, oldest(b.senders, func(s *senderSeq) time.Time { return s.heard }))
		}
		b.senders[sender] = &senderSeq{highest: seq, heard: time.Now()}
		return false, 0
	}
	s.heard = time.Now()
	switch {
	case seq > s.highest:
		ahead := seq - s.highest
		s.seen = s.seen<<ahead | 1<<(ahead-1)
		s.highest = seq
		return false, ahead - 1
	case seq == s.highest:
		return true, 0
	case s.highest-seq > seqWindow:
		return true, 0
	default:
		bit := uint64(1) << (s.highest - seq - 1)
		if s.seen&bit != 0 {
			return true, 0
		}
		s.seen |= bit
		return false, 0
	}
}

// dropCachesAfterLoss clears the caches, since any lost message may have been
// an invalidation this node never applied.
func (b *Broadcaster) dropCachesAfterLoss(sender [16]byte, missed uint64) {
	total := atomic.AddUint64(&b.messagesLost, missed)
	if b.handler != nil {
		b.requestClear()
	}

	b.lossLogMu.Lock()
	defer b.lossLogMu.Unlock()
	if time.Since(b.lossLoggedAt) < lossLogInterval {
		return
	}
	b.lossLoggedAt = time.Now()
	log.Printf("[broadcast] missed %d messages from %x, dropped all cached inodes and extents (%d missed since start, logged at most every %v). If this repeats, raise net.core.rmem_max to at least 1 MB",
		missed, sender[:8], total, lossLogInterval)
}

// requestClear makes sure a cache clear starts after this call. Clears run off
// the receive path, one at a time: a full clear takes over a millisecond, and a
// receiver that clears for every message it reads falls further behind.
func (b *Broadcaster) requestClear() {
	b.clearMu.Lock()
	defer b.clearMu.Unlock()
	if b.clearRunning {
		b.clearAgain = true
		return
	}
	b.clearRunning = true
	go b.runClears()
}

func (b *Broadcaster) runClears() {
	for {
		b.handler.Clear()
		b.clearMu.Lock()
		if !b.clearAgain {
			b.clearRunning = false
			b.clearMu.Unlock()
			return
		}
		b.clearAgain = false
		b.clearMu.Unlock()
	}
}

// heartbeatLoop sends periodic heartbeats, and trailing ones after invalidations.
func (b *Broadcaster) heartbeatLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.transport.config.HeartbeatInterval)
	defer ticker.Stop()
	trailer := time.NewTicker(trailerDelay)
	defer trailer.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-trailer.C:
			if b.trailersLeft.Load() > 0 {
				b.trailersLeft.Add(-1)
				_ = b.send(b.builder.Heartbeat)
			}
		case <-ticker.C:
			// Heartbeat failures are expected during network issues
			_ = b.send(b.builder.Heartbeat)
		}
	}
}

func (b *Broadcaster) cleanupLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.forgetQuietSenders()
		}
	}
}

// forgetQuietSenders drops senders not heard from in senderExpiry. Each mount
// has a new UUID, so this removes nodes that unmounted.
func (b *Broadcaster) forgetQuietSenders() {
	b.sendersMu.Lock()
	defer b.sendersMu.Unlock()

	cutoff := time.Now().Add(-senderExpiry)
	for sender, s := range b.senders {
		if s.heard.Before(cutoff) {
			delete(b.senders, sender)
		}
	}
}

// send numbers and sends a message under sendMu, so peers receive this node's
// messages in sequence order and read a gap as a lost message.
func (b *Broadcaster) send(build func() *Message) error {
	b.sendMu.Lock()
	defer b.sendMu.Unlock()
	return b.transport.Send(build())
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

	announce := func() *Message { return b.builder.PeerAnnounce(b.nodeSlot, b.localPort, ipAddr) }
	if err := b.send(announce); err != nil {
		log.Printf("[broadcast] failed to announce presence: %v", err)
	}
}

// BroadcastInvalidateInode sends an inode invalidation to all peers.
func (b *Broadcaster) BroadcastInvalidateInode(inodeNum uint32, dataGen, metaGen uint64) {
	if !b.running.Load() {
		return
	}

	b.trailersLeft.Store(trailers)
	if err := b.send(func() *Message { return b.builder.InvalidateInode(inodeNum, dataGen, metaGen) }); err != nil {
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

	b.trailersLeft.Store(trailers)
	if err := b.send(func() *Message { return b.builder.InvalidateExtent(inodeNum, dataGen) }); err != nil {
		log.Printf("[broadcast] failed to send extent invalidation: %v", err)
	}
	atomic.AddUint64(&b.invalidationsSent, 1)
}

// BroadcastInvalidateAll sends a full cache invalidation to all peers.
func (b *Broadcaster) BroadcastInvalidateAll() {
	if !b.running.Load() {
		return
	}

	b.trailersLeft.Store(trailers)
	if err := b.send(b.builder.InvalidateAll); err != nil {
		log.Printf("[broadcast] failed to send InvalidateAll: %v", err)
	}
	atomic.AddUint64(&b.invalidationsSent, 1)

	log.Printf("[broadcast] sent InvalidateAll to cluster")
}

// Stats returns broadcaster statistics.
func (b *Broadcaster) Stats() BroadcasterStats {
	b.sendersMu.Lock()
	senders := len(b.senders)
	b.sendersMu.Unlock()
	return BroadcasterStats{
		TrackedSenders:        senders,
		MaxPeers:              b.transport.config.maxPeers(),
		InvalidationsReceived: atomic.LoadUint64(&b.invalidationsReceived),
		InvalidationsSent:     atomic.LoadUint64(&b.invalidationsSent),
		DuplicatesDropped:     atomic.LoadUint64(&b.duplicatesDropped),
		MessagesIgnored:       atomic.LoadUint64(&b.messagesIgnored),
		MessagesLost:          atomic.LoadUint64(&b.messagesLost),
		Transport:             b.transport.Stats(),
	}
}

// BroadcasterStats holds broadcaster statistics.
type BroadcasterStats struct {
	InvalidationsReceived uint64
	InvalidationsSent     uint64
	DuplicatesDropped     uint64
	MessagesIgnored       uint64
	MessagesLost          uint64
	TrackedSenders        int
	MaxPeers              int
	Transport             TransportStats
}

// ResetStats resets all statistics.
func (b *Broadcaster) ResetStats() {
	atomic.StoreUint64(&b.invalidationsReceived, 0)
	atomic.StoreUint64(&b.invalidationsSent, 0)
	atomic.StoreUint64(&b.duplicatesDropped, 0)
	atomic.StoreUint64(&b.messagesIgnored, 0)
	atomic.StoreUint64(&b.messagesLost, 0)
}

// Problem says why peers may miss this node's invalidations, or returns ""
// when they should get them.
func (b *Broadcaster) Problem() string {
	return b.transport.Problem()
}

// IsEnabled returns whether broadcasting is enabled.
func (b *Broadcaster) IsEnabled() bool {
	return b.transport.config.Enabled
}
