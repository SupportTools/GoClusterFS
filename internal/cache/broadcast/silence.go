package broadcast

import (
	"log"
	"sort"
	"time"
)

// silentAfter is how many heartbeat intervals a node may go unheard before
// it counts as silent.
const silentAfter = 3

// ExpectedPeer is a node that should be sending this node broadcasts.
type ExpectedPeer struct {
	UUID    [16]byte
	Name    string
	Mounted time.Time
}

// WatchPeers has the broadcaster check, every heartbeat interval, that it
// hears from each node that expected lists. A node silent for silentAfter
// intervals since it mounted or this node started is reported: dropped
// multicast is otherwise invisible, because a node that hears nothing from a
// peer finds no gap in its sequence numbers either.
func (b *Broadcaster) WatchPeers(expected func() ([]ExpectedPeer, error)) {
	b.expected.Store(&expected)
}

// SilentPeers names the expected nodes not heard from for the returned time.
func (b *Broadcaster) SilentPeers() ([]string, time.Duration) {
	b.silentMu.Lock()
	defer b.silentMu.Unlock()
	names := make([]string, 0, len(b.silent))
	for _, p := range b.silent {
		names = append(names, p.Name)
	}
	sort.Strings(names)
	return names, b.silenceThreshold()
}

func (b *Broadcaster) silenceThreshold() time.Duration {
	return silentAfter * b.transport.config.HeartbeatInterval
}

func (b *Broadcaster) watchLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.transport.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case now := <-ticker.C:
			b.checkPeers(now)
		}
	}
}

func (b *Broadcaster) checkPeers(now time.Time) {
	expected := b.expected.Load()
	if expected == nil {
		return
	}
	peers, err := (*expected)()
	if err != nil {
		log.Printf("[broadcast] could not list the nodes that should be sending broadcasts: %v", err)
		return
	}

	quiet := b.silenceThreshold()
	mounted := map[[16]byte]bool{}
	silent := map[[16]byte]ExpectedPeer{}
	b.sendersMu.Lock()
	for _, p := range peers {
		if p.UUID == b.localUUID {
			continue
		}
		mounted[p.UUID] = true
		last := b.started
		if p.Mounted.After(last) {
			last = p.Mounted
		}
		if s, ok := b.senders[p.UUID]; ok && s.heard.After(last) {
			last = s.heard
		}
		if now.Sub(last) > quiet {
			silent[p.UUID] = p
		}
	}
	b.sendersMu.Unlock()

	b.silentMu.Lock()
	defer b.silentMu.Unlock()
	for id, p := range silent {
		if _, ok := b.silent[id]; !ok {
			log.Printf("[broadcast] WARN: no broadcast heard from node %s for over %v, though the registry shows it mounted; this node sees its changes only after -cache-ttl. Multicast may not reach this node, or that node runs with -broadcast=false or another -mcast-group or -mcast-port",
				p.Name, quiet)
		}
	}
	for id, p := range b.silent {
		if _, ok := silent[id]; !ok && mounted[id] {
			log.Printf("[broadcast] heard from node %s again", p.Name)
		}
	}
	b.silent = silent
}
