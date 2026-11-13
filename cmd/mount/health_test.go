package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"syscall"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/api"
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
		{name: "replaying a crashed peer", src: func() healthSources {
			s := fineSources()
			s.replaying = func() []int { return []int{3, 7} }
			return s
		}(), want: api.HealthStatusDegraded, code: http.StatusOK, problem: "recovery"},
		{name: "broadcast down", src: func() healthSources {
			s := fineSources()
			s.broadcastProblem = func() string { return "multicast is unavailable and no unicast peers are known" }
			return s
		}(), want: api.HealthStatusDegraded, code: http.StatusOK, problem: "broadcast"},
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
			srv, err := api.NewServer(api.DefaultConfig().WithListenAddr("127.0.0.1:0"))
			if err != nil {
				t.Fatal(err)
			}
			registerHealthChecks(srv, tt.src)
			if err := srv.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer srv.Stop()
			resp := api.AggregateHealth(context.Background(), srv.GetHealthCheckers()...)
			if resp.Status != tt.want {
				t.Errorf("status = %s, want %s: %+v", resp.Status, tt.want, resp.Checks)
			}
			if len(resp.Checks) != 4 {
				t.Errorf("checks = %v, want device, journal, recovery and broadcast", resp.Checks)
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
			defer got.Body.Close()
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
	if msg := recoveryHealth([]int{3, 7}).Message; !strings.Contains(msg, "slot 3, 7") {
		t.Errorf("recovery message = %q", msg)
	}
	if msg := deviceHealth(errors.New("input/output error")).Message; !strings.Contains(msg, "input/output error") || !strings.Contains(msg, "goclusterfs-fsck") {
		t.Errorf("device message = %q", msg)
	}
	if got := journalHealth(&api.JournalStatsResponse{State: "recovering"}); got.Status != api.HealthStatusDegraded {
		t.Errorf("recovering journal = %+v, want degraded", got)
	}
}

func fineSources() healthSources {
	return healthSources{
		deviceError:      func() error { return nil },
		journal:          func() *api.JournalStatsResponse { return &api.JournalStatsResponse{State: "dirty"} },
		replaying:        func() []int { return nil },
		broadcastOn:      true,
		broadcastProblem: func() string { return "" },
	}
}
