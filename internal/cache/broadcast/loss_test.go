package broadcast

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func startBroadcaster(t *testing.T, cfg Config, id [16]byte, h InvalidationHandler) *Broadcaster {
	t.Helper()
	b, err := NewBroadcaster(cfg, id, 0, h)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	return b
}

// unicastTo starts a broadcaster that sends only by unicast, to a socket the
// test reads.
func unicastTo(t *testing.T) (*Broadcaster, *net.UDPConn) {
	t.Helper()
	raw, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	ports := freeUDPPorts(t, 2)
	cfg := DefaultConfig()
	cfg.MulticastPort, cfg.UnicastPort = ports[0], ports[1]
	cfg.MaxMulticastFailures = 0
	b := startBroadcaster(t, cfg, [16]byte{1}, nil)
	b.transport.AddPeer("raw", raw.LocalAddr().(*net.UDPAddr))
	return b, raw
}

// readMessages reads until it has want messages or none arrives for idle.
func readMessages(t *testing.T, conn *net.UDPConn, want int, idle time.Duration) []*Message {
	t.Helper()
	var msgs []*Message
	buf := make([]byte, MaxMessageSize)
	for len(msgs) < want {
		if err := conn.SetReadDeadline(time.Now().Add(idle)); err != nil {
			t.Fatal(err)
		}
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			break
		}
		msg, err := Unmarshal(buf[:n], nil)
		if err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

func TestLostMessageDropsCaches(t *testing.T) {
	handler := newMockHandler()
	b, err := NewBroadcaster(DisabledConfig(), [16]byte{1}, 0, handler)
	if err != nil {
		t.Fatal(err)
	}
	peer := NewMessageBuilder([16]byte{2})

	b.handleMessage(peer.InvalidateInode(10, 1, 1), nil)
	lost := peer.InvalidateInode(11, 1, 1)
	b.handleMessage(peer.Heartbeat(), nil)
	waitForClears(t, handler, 1)
	if got := b.Stats().MessagesLost; got != 1 {
		t.Errorf("MessagesLost = %d, want 1", got)
	}

	b.handleMessage(lost, nil)
	b.handleMessage(peer.InvalidateInode(12, 1, 1), nil)
	time.Sleep(50 * time.Millisecond)
	if got := handler.getClearCount(); got != 1 {
		t.Errorf("Clear called %d times after the late message and the next one, want still 1", got)
	}
	if got := handler.getInodeInvalidations(); len(got) != 3 {
		t.Errorf("invalidated inodes = %v, want [10 11 12]", got)
	}
}

// TestVeryLateMessageKeepsTracking checks that a message older than the window,
// such as one delayed on the other path, does not move tracking back.
func TestVeryLateMessageKeepsTracking(t *testing.T) {
	handler := newMockHandler()
	b, err := NewBroadcaster(DisabledConfig(), [16]byte{1}, 0, handler)
	if err != nil {
		t.Fatal(err)
	}
	peer := NewMessageBuilder([16]byte{2})
	late := peer.InvalidateInode(10, 1, 1)
	for i := 0; i < 100; i++ {
		b.handleMessage(peer.Heartbeat(), nil)
	}

	b.handleMessage(late, nil)
	b.handleMessage(peer.InvalidateInode(11, 1, 1), nil)
	time.Sleep(50 * time.Millisecond)

	stats := b.Stats()
	if got := handler.getClearCount(); got != 0 || stats.MessagesLost != 0 {
		t.Errorf("after a very late message: %d clears, %d lost; want 0 and 0", got, stats.MessagesLost)
	}
	if stats.DuplicatesDropped != 1 {
		t.Errorf("DuplicatesDropped = %d, want 1 for the very late message", stats.DuplicatesDropped)
	}
	if got := handler.getInodeInvalidations(); len(got) != 1 || got[0] != 11 {
		t.Errorf("invalidated inodes = %v, want [11]", got)
	}
}

func waitForClears(t *testing.T, h *mockHandler, want int) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); h.getClearCount() < want; {
		if time.Now().After(deadline) {
			t.Fatalf("Clear called %d times, want %d", h.getClearCount(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// slowClearHandler holds each Clear until the test releases it.
type slowClearHandler struct {
	*mockHandler
	release chan struct{}
}

func (h slowClearHandler) Clear() {
	<-h.release
	h.mockHandler.Clear()
}

// TestClearsCoalesce checks that gaps found while a clear runs cost one more
// clear, which starts after them.
func TestClearsCoalesce(t *testing.T) {
	h := slowClearHandler{newMockHandler(), make(chan struct{})}
	b, err := NewBroadcaster(DisabledConfig(), [16]byte{1}, 0, h)
	if err != nil {
		t.Fatal(err)
	}
	peer := NewMessageBuilder([16]byte{2})
	b.handleMessage(peer.Heartbeat(), nil)
	for i := 0; i < 5; i++ {
		peer.Heartbeat()
		b.handleMessage(peer.Heartbeat(), nil)
	}
	h.release <- struct{}{}
	h.release <- struct{}{}
	waitForClears(t, h.mockHandler, 2)

	select {
	case h.release <- struct{}{}:
		t.Error("a third clear ran for gaps that two clears covered")
	case <-time.After(50 * time.Millisecond):
	}
	if got := b.Stats().MessagesLost; got != 5 {
		t.Errorf("MessagesLost = %d, want 5", got)
	}
}

// TestHeartbeatsFollowInvalidation checks the heartbeats that let a peer
// notice it lost the last invalidation of a burst.
func TestHeartbeatsFollowInvalidation(t *testing.T) {
	b, raw := unicastTo(t)

	b.BroadcastInvalidateInode(7, 1, 1)
	msgs := readMessages(t, raw, 1+trailers, 4*trailerDelay)

	if len(msgs) != 1+trailers {
		t.Fatalf("received %d messages, want the invalidation and %d heartbeats", len(msgs), trailers)
	}
	for i, m := range msgs {
		want := TypeHeartbeat
		if i == 0 {
			want = TypeInvalidateInode
		}
		if m.Type != want || m.Sequence != msgs[0].Sequence+uint64(i) {
			t.Errorf("message %d: %v seq %d, want %v seq %d", i, m.Type, m.Sequence, want, msgs[0].Sequence+uint64(i))
		}
	}
}

// TestConcurrentSendsArriveInOrder checks that concurrent writers' invalidations
// leave in sequence order, so a peer does not read reordering as loss.
func TestConcurrentSendsArriveInOrder(t *testing.T) {
	b, raw := unicastTo(t)
	const writers, each = 8, 250

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				b.BroadcastInvalidateInode(uint32(w*each+i), 1, 1)
			}
		}()
	}
	msgs := readMessages(t, raw, writers*each, 500*time.Millisecond)
	wg.Wait()

	if len(msgs) == 0 {
		t.Fatal("received nothing")
	}
	for i := 1; i < len(msgs); i++ {
		if msgs[i].Sequence <= msgs[i-1].Sequence {
			t.Fatalf("message %d has seq %d after seq %d", i, msgs[i].Sequence, msgs[i-1].Sequence)
		}
	}
}

// coverageHandler records which inodes a node invalidated and when it last
// dropped its caches. delay slows it down enough to overflow its socket.
type coverageHandler struct {
	delay     time.Duration
	mu        sync.Mutex
	got       map[uint32]bool
	lastClear time.Time
}

func (h *coverageHandler) InvalidateInode(n uint32) {
	for start := time.Now(); time.Since(start) < h.delay; {
	}
	h.mu.Lock()
	h.got[n] = true
	h.mu.Unlock()
}

func (h *coverageHandler) InvalidateExtentsForInode(uint32) {}

func (h *coverageHandler) Clear() {
	h.mu.Lock()
	h.lastClear = time.Now()
	h.mu.Unlock()
}

// uncovered lists the inodes whose invalidation neither arrived nor was
// followed by a cache drop.
func (h *coverageHandler) uncovered(first uint32, sentAt []time.Time) []uint32 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var missing []uint32
	for i, at := range sentAt {
		n := first + uint32(i)
		if !h.got[n] && !h.lastClear.After(at) {
			missing = append(missing, n)
		}
	}
	return missing
}

// TestBurstOfInvalidationsReachesPeer has one node change a few thousand inodes
// as fast as it can. Within a second, well under the 5s default -cache-ttl, the
// other node must have dropped each inode: by its own message, or by clearing
// its cache after noticing lost messages. A slow receiver makes sure some are
// lost.
func TestBurstOfInvalidationsReachesPeer(t *testing.T) {
	if testing.Short() {
		t.Skip("sends thousands of UDP messages")
	}
	for _, unicast := range []bool{false, true} {
		for _, delay := range []time.Duration{0, 100 * time.Microsecond} {
			path := "multicast"
			if unicast {
				path = "unicast"
			}
			t.Run(fmt.Sprintf("%s/handler-%v", path, delay), func(t *testing.T) {
				invalidationBurst(t, unicast, delay)
			})
		}
	}
}

func invalidationBurst(t *testing.T, unicast bool, delay time.Duration) {
	const (
		inodes = 3000
		first  = 100
		window = time.Second
	)
	senderID, receiverID := [16]byte{1}, [16]byte{2}
	ports := freeUDPPorts(t, 3)
	cfgA, cfgB := DefaultConfig(), DefaultConfig()
	cfgA.MulticastPort, cfgB.MulticastPort = ports[0], ports[0]
	cfgA.UnicastPort, cfgB.UnicastPort = ports[1], ports[2]
	if unicast {
		cfgA.MaxMulticastFailures = 0
	}

	h := &coverageHandler{delay: delay, got: map[uint32]bool{}}
	receiver := startBroadcaster(t, cfgB, receiverID, h)
	sender := startBroadcaster(t, cfgA, senderID, nil)
	if unicast {
		sender.transport.AddPeer(fmt.Sprintf("%x", receiverID), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: ports[2]})
	} else if sender.transport.multicastConn == nil {
		t.Skip("multicast is not available on this host")
	}

	heard := func() bool {
		receiver.sendersMu.Lock()
		defer receiver.sendersMu.Unlock()
		return receiver.senders[senderID] != nil
	}
	for deadline := time.Now().Add(3 * time.Second); !heard(); {
		if time.Now().After(deadline) {
			t.Fatal("receiver never heard from the sender")
		}
		_ = sender.send(sender.builder.Heartbeat)
		time.Sleep(10 * time.Millisecond)
	}

	sentAt := make([]time.Time, inodes)
	for i := range sentAt {
		sender.BroadcastInvalidateInode(first+uint32(i), 1, 1)
		sentAt[i] = time.Now()
	}
	lastSend := sentAt[inodes-1]

	missing := h.uncovered(first, sentAt)
	for len(missing) > 0 && time.Since(lastSend) < window {
		time.Sleep(5 * time.Millisecond)
		missing = h.uncovered(first, sentAt)
	}
	coveredAfter := time.Since(lastSend)

	stats := receiver.Stats()
	t.Logf("%d invalidations sent in %v; received %d, %d messages lost; every inode covered %v after the last send",
		inodes, lastSend.Sub(sentAt[0]), stats.InvalidationsReceived, stats.MessagesLost, coveredAfter)
	if len(missing) > 0 {
		t.Errorf("%d of %d invalidations neither arrived nor were followed by a cache drop within %v; first: inode %d",
			len(missing), inodes, window, missing[0])
	}
}
