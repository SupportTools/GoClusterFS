package broadcast

import (
	"errors"
	"slices"
	"testing"
	"time"
)

// TestSilentPeers counts silence from when this node first saw each peer
// listed, by its own clock, so a peer's clock cannot delay the report.
func TestSilentPeers(t *testing.T) {
	cfg := DisabledConfig()
	cfg.HeartbeatInterval = time.Second
	local := [16]byte{1}
	b, err := NewBroadcaster(cfg, local, 0, newMockHandler())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.started = now.Add(-10 * time.Minute)

	heard, never, quiet, newcomer := [16]byte{2}, [16]byte{3}, [16]byte{4}, [16]byte{5}
	peers := []ExpectedPeer{{UUID: local, Name: "self"}, {UUID: heard, Name: "heard"}, {UUID: never, Name: "never"}, {UUID: quiet, Name: "quiet"}}
	b.WatchPeers(func() ([]ExpectedPeer, error) { return peers, nil })

	b.checkPeers(now)
	if names, _ := b.SilentPeers(); len(names) != 0 {
		t.Errorf("silent = %v at the first look, before any grace period passed", names)
	}

	later := now.Add(4 * time.Second)
	peers = append(peers, ExpectedPeer{UUID: newcomer, Name: "newcomer"})
	b.senders[heard] = &senderSeq{heard: later.Add(-time.Second)}
	b.senders[quiet] = &senderSeq{heard: now.Add(-10 * time.Second)}
	b.checkPeers(later)
	names, after := b.SilentPeers()
	if want := []string{"never", "quiet"}; !slices.Equal(names, want) {
		t.Errorf("silent = %v, want %v", names, want)
	}
	if after != 3*time.Second {
		t.Errorf("silent after %v, want 3 heartbeat intervals (3s)", after)
	}
	if got := b.Stats().SilentPeers; got != 2 {
		t.Errorf("Stats().SilentPeers = %d, want 2", got)
	}

	b.senders[quiet].heard = later
	b.checkPeers(later)
	if names, _ := b.SilentPeers(); !slices.Equal(names, []string{"never"}) {
		t.Errorf("after hearing quiet: silent = %v, want [never]", names)
	}

	b.WatchPeers(func() ([]ExpectedPeer, error) { return nil, errors.New("registry unreadable") })
	b.checkPeers(later)
	if names, _ := b.SilentPeers(); !slices.Equal(names, []string{"never"}) {
		t.Errorf("an unreadable registry changed the silent peers to %v", names)
	}

	peers = slices.DeleteFunc(peers, func(p ExpectedPeer) bool { return p.UUID == never })
	b.WatchPeers(func() ([]ExpectedPeer, error) { return peers, nil })
	b.checkPeers(later)
	if names, _ := b.SilentPeers(); len(names) != 0 {
		t.Errorf("after never unmounted: silent = %v, want none", names)
	}
	if _, ok := b.firstListed[never]; ok {
		t.Error("an unmounted peer is still tracked")
	}
}

func TestSilentPeersWithoutWatch(t *testing.T) {
	cfg := DisabledConfig()
	cfg.HeartbeatInterval = time.Second
	b, err := NewBroadcaster(cfg, [16]byte{1}, 0, newMockHandler())
	if err != nil {
		t.Fatal(err)
	}
	b.checkPeers(time.Now())
	if names, _ := b.SilentPeers(); len(names) != 0 {
		t.Errorf("silent = %v with nothing to watch", names)
	}
}
