package broadcast

import (
	"errors"
	"slices"
	"testing"
	"time"
)

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
	long := now.Add(-time.Minute)

	heard, newcomer, never, quiet := [16]byte{2}, [16]byte{3}, [16]byte{4}, [16]byte{5}
	b.senders[heard] = &senderSeq{heard: now.Add(-time.Second)}
	b.senders[quiet] = &senderSeq{heard: now.Add(-10 * time.Second)}
	peers := []ExpectedPeer{
		{UUID: local, Name: "self", Mounted: long},
		{UUID: heard, Name: "heard", Mounted: long},
		{UUID: newcomer, Name: "newcomer", Mounted: now.Add(-2 * time.Second)},
		{UUID: never, Name: "never", Mounted: long},
		{UUID: quiet, Name: "quiet", Mounted: long},
	}
	b.WatchPeers(func() ([]ExpectedPeer, error) { return peers, nil })

	b.checkPeers(now)
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

	b.track(quiet, 1)
	b.checkPeers(now)
	if names, _ := b.SilentPeers(); !slices.Equal(names, []string{"never"}) {
		t.Errorf("after hearing quiet: silent = %v, want [never]", names)
	}

	b.WatchPeers(func() ([]ExpectedPeer, error) { return nil, errors.New("registry unreadable") })
	b.checkPeers(now)
	if names, _ := b.SilentPeers(); !slices.Equal(names, []string{"never"}) {
		t.Errorf("an unreadable registry changed the silent peers to %v", names)
	}

	peers = slices.DeleteFunc(peers, func(p ExpectedPeer) bool { return p.UUID == never })
	b.WatchPeers(func() ([]ExpectedPeer, error) { return peers, nil })
	b.checkPeers(now)
	if names, _ := b.SilentPeers(); len(names) != 0 {
		t.Errorf("after never unmounted: silent = %v, want none", names)
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
