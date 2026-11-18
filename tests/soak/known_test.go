//go:build linux

package main

import (
	"strings"
	"testing"
)

func TestAbandonedByPeer(t *testing.T) {
	const (
		own = "2026/11/18 22:38:54 [snapshot/worker] Snapshot 1183681 deleted\n" +
			"2026/11/18 22:38:56 [snapshot] Failed to create 'l2-s29157' for inode 16396: snapshot in invalid state\n" +
			"2026/11/18 22:38:56 [snapshot/worker] Queued delete job 65 for snapshot 1184128\n" +
			"2026/11/18 22:38:56 [snapshot/worker] Snapshot 1184128 deleted\n"
		scan     = "2026/11/18 22:38:57 [snapshot] Registry scan: 32 active, 1 creating, 0 deleting\n"
		queued   = "2026/11/18 22:38:57 [snapshot/worker] Queued delete job 1 for snapshot 1184128\n"
		cleaning = "2026/11/18 22:38:57 [snapshot/worker] Cleaning up 1 incomplete snapshot(s)\n"
		mounted  = "2026/11/18 22:38:57 Filesystem mounted. Press Ctrl+C to unmount.\n"
	)
	opStart := int64(1795041538226)
	cases := []struct {
		name  string
		own   string
		peers []string
		snap  string
		from  int64
		want  bool
	}{
		{"the op from RC failure 4", own, []string{"", scan + queued + cleaning + mounted}, "l2-s29157", opStart, true},
		{"another snapshot's name", own, []string{scan + queued + cleaning}, "l2-s29158", opStart, false},
		{"the peer abandoned another snapshot", own, []string{scan + strings.Replace(queued, "1184128", "1184129", 1) + cleaning}, "l2-s29157", opStart, false},
		{"the peer deleted it outside a mount", own, []string{queued + mounted}, "l2-s29157", opStart, false},
		{"no peer abandoned it", own, []string{scan + mounted}, "l2-s29157", opStart, false},
		{"an op a minute later", own, []string{scan + queued + cleaning}, "l2-s29157", opStart + 60000, false},
	}
	for _, c := range cases {
		var peers [][]byte
		for _, p := range c.peers {
			peers = append(peers, []byte(p))
		}
		if got := abandonedByPeer([]byte(c.own), peers, c.snap, c.from, c.from+106); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
