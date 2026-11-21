package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/api"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/testnet"
)

func TestHealthChecksSeparateDegradedFromDown(t *testing.T) {
	tests := []struct {
		name    string
		src     healthSources
		want    api.HealthStatus
		code    int
		problem string
	}{
		{name: "all fine", src: fineSources(), want: api.HealthStatusOK, code: http.StatusOK},
		{name: "journal off and broadcast off are settings", src: func() healthSources {
			s := fineSources()
			s.journal = func() *api.JournalStatsResponse { return nil }
			s.broadcastOn = false
			return s
		}(), want: api.HealthStatusOK, code: http.StatusOK},
		{name: "heartbeats late", src: func() healthSources {
			s := fineSources()
			s.heartbeatAge = func() time.Duration { return 3 * time.Second }
			return s
		}(), want: api.HealthStatusDegraded, code: http.StatusOK, problem: "heartbeat"},
		{name: "not registered yet", src: func() healthSources {
			s := fineSources()
			s.heartbeatAge = func() time.Duration { return 0 }
			return s
		}(), want: api.HealthStatusOK, code: http.StatusOK},
		{name: "replaying a crashed peer", src: func() healthSources {
			s := fineSources()
			s.replaying = func() []int { return []int{3, 7} }
			return s
		}(), want: api.HealthStatusDegraded, code: http.StatusOK, problem: "recovery"},
		{name: "a crashed peer's replay keeps failing", src: func() healthSources {
			s := fineSources()
			s.failingReplays = func() []node.FailingReplay { return []node.FailingReplay{{Slot: 5, Failures: 3, Err: "bad record"}} }
			return s
		}(), want: api.HealthStatusDegraded, code: http.StatusOK, problem: "recovery"},
		{name: "broadcast down", src: func() healthSources {
			s := fineSources()
			s.broadcastProblem = func() string { return "multicast is unavailable and no unicast peers are known" }
			return s
		}(), want: api.HealthStatusDegraded, code: http.StatusOK, problem: "broadcast"},
		{name: "a mounted peer is silent", src: func() healthSources {
			s := fineSources()
			s.silentPeers = func() ([]string, time.Duration) { return []string{"node3 (1a2b3c4d)"}, 90 * time.Second }
			return s
		}(), want: api.HealthStatusDegraded, code: http.StatusOK, problem: "broadcast-peers"},
		{name: "device failing", src: func() healthSources {
			s := fineSources()
			s.deviceError = func() error { return syscall.EIO }
			return s
		}(), want: api.HealthStatusUnhealthy, code: http.StatusServiceUnavailable, problem: "device"},
		{name: "journal corrupt", src: func() healthSources {
			s := fineSources()
			s.journal = func() *api.JournalStatsResponse { return &api.JournalStatsResponse{State: "corrupt"} }
			return s
		}(), want: api.HealthStatusUnhealthy, code: http.StatusServiceUnavailable, problem: "journal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := testnet.LoopbackAddr()
			if err != nil {
				t.Fatal(err)
			}
			srv, err := api.NewServer(api.DefaultConfig().WithListenAddr(addr))
			if err != nil {
				t.Fatal(err)
			}
			registerHealthChecks(srv, tt.src)
			if err := srv.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = srv.Stop() }()
			resp := api.AggregateHealth(context.Background(), srv.GetHealthCheckers()...)
			if resp.Status != tt.want {
				t.Errorf("status = %s, want %s: %+v", resp.Status, tt.want, resp.Checks)
			}
			if len(resp.Checks) != 7 {
				t.Errorf("checks = %v, want device, journal, recovery, broadcast, broadcast-peers, heartbeat and fencing", resp.Checks)
			}
			for name, check := range resp.Checks {
				if (name == tt.problem) != (check.Status != api.HealthStatusOK) {
					t.Errorf("check %s = %+v", name, check)
				}
				if check.Status != api.HealthStatusOK && check.Message == "" {
					t.Errorf("check %s is %s without saying why", name, check.Status)
				}
			}

			got, err := http.Get("http://" + srv.Addr() + "/health")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = got.Body.Close() }()
			var body api.HealthResponse
			if err := json.NewDecoder(got.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if got.StatusCode != tt.code || body.Status != tt.want {
				t.Errorf("GET /health = %d %s, want %d %s", got.StatusCode, body.Status, tt.code, tt.want)
			}
		})
	}
}

func TestHealthMessagesNameTheSlotsAndTheError(t *testing.T) {
	if msg := recoveryHealth([]int{3, 7}, nil).Message; !strings.Contains(msg, "slot 3, 7") {
		t.Errorf("recovery message = %q", msg)
	}
	failing := []node.FailingReplay{{Slot: 5, Failures: 4, Err: "bad record"}}
	if msg := recoveryHealth([]int{5}, failing).Message; !strings.Contains(msg, "slot 5 failed 4 times in a row: bad record") || !strings.Contains(msg, "-inspect-slot 5") {
		t.Errorf("failing recovery message = %q", msg)
	}
	if msg := heartbeatHealth(3200 * time.Millisecond).Message; !strings.Contains(msg, "3.2s ago") || !strings.Contains(msg, "at 5s this node stops") {
		t.Errorf("heartbeat message = %q", msg)
	}
	if msg := deviceHealth(errors.New("input/output error")).Message; !strings.Contains(msg, "input/output error") || !strings.Contains(msg, "goclusterfs-fsck") {
		t.Errorf("device message = %q", msg)
	}
	if got := journalHealth(&api.JournalStatsResponse{State: "recovering"}); got.Status != api.HealthStatusDegraded {
		t.Errorf("recovering journal = %+v, want degraded", got)
	}
	if msg := silentPeersHealth([]string{"a (01)", "b (02)"}, 90*time.Second).Message; !strings.Contains(msg, "a (01), b (02)") || !strings.Contains(msg, "1m30s") {
		t.Errorf("silent peers message = %q", msg)
	}
}

func fineSources() healthSources {
	return healthSources{
		deviceError:      func() error { return nil },
		journal:          func() *api.JournalStatsResponse { return &api.JournalStatsResponse{State: "dirty"} },
		replaying:        func() []int { return nil },
		failingReplays:   func() []node.FailingReplay { return nil },
		broadcastOn:      true,
		broadcastProblem: func() string { return "" },
		silentPeers:      func() ([]string, time.Duration) { return nil, 90 * time.Second },
		heartbeatAge:     func() time.Duration { return 800 * time.Millisecond },
		keyRestores:      func() (uint64, time.Time) { return 0, time.Time{} },
	}
}

func TestFencingHealthNamesAMissingFence(t *testing.T) {
	if h := fencingHealth("none", 0, time.Time{}); h.Status != api.HealthStatusOK || !strings.HasPrefix(h.Message, "none:") {
		t.Errorf("unfenced: %+v, want healthy with a message that starts with none:", h)
	}
	if h := fencingHealth("pr", 0, time.Time{}); h.Status != api.HealthStatusOK || strings.HasPrefix(h.Message, "none") || strings.Contains(h.Message, "again") {
		t.Errorf("fenced: %+v", h)
	}
	at := time.Date(2026, 11, 21, 18, 0, 0, 0, time.UTC)
	if h := fencingHealth("pr", 2, at); h.Status != api.HealthStatusOK || !strings.Contains(h.Message, "registered its key again 2 times") || !strings.Contains(h.Message, "2026-11-21T18:00:00Z") {
		t.Errorf("after two key restores: %+v", h)
	}
}
