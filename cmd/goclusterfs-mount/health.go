package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/supporttools/GoClusterFS/internal/api"
	"github.com/supporttools/GoClusterFS/internal/node"
)

type healthSources struct {
	deviceError      func() error
	journal          func() *api.JournalStatsResponse
	replaying        func() []int
	broadcastOn      bool
	broadcastProblem func() string
	silentPeers      func() ([]string, time.Duration)
	heartbeatAge     func() time.Duration
}

func registerHealthChecks(srv *api.Server, src healthSources) {
	check := func(name string, fn func() api.ComponentHealth) {
		srv.RegisterHealthChecker(api.NewHealthCheckFunc(name, func(context.Context) api.ComponentHealth { return fn() }))
	}
	check("device", func() api.ComponentHealth { return deviceHealth(src.deviceError()) })
	check("journal", func() api.ComponentHealth { return journalHealth(src.journal()) })
	check("recovery", func() api.ComponentHealth { return recoveryHealth(src.replaying()) })
	check("broadcast", func() api.ComponentHealth { return broadcastHealth(src.broadcastOn, src.broadcastProblem()) })
	check("broadcast-peers", func() api.ComponentHealth { return silentPeersHealth(src.silentPeers()) })
	check("heartbeat", func() api.ComponentHealth { return heartbeatHealth(src.heartbeatAge()) })
}

func healthy(message string) api.ComponentHealth {
	return api.ComponentHealth{Status: api.HealthStatusOK, Message: message}
}

func degraded(format string, args ...any) api.ComponentHealth {
	return api.ComponentHealth{Status: api.HealthStatusDegraded, Message: fmt.Sprintf(format, args...)}
}

func unhealthy(format string, args ...any) api.ComponentHealth {
	return api.ComponentHealth{Status: api.HealthStatusUnhealthy, Message: fmt.Sprintf(format, args...)}
}

func deviceHealth(lastErr error) api.ComponentHealth {
	if lastErr != nil {
		return unhealthy("the last device read or write failed: %v; check the device and the path to it, then unmount and run goclusterfs-fsck if it keeps failing", lastErr)
	}
	return healthy("")
}

func journalHealth(j *api.JournalStatsResponse) api.ComponentHealth {
	switch {
	case j == nil:
		return healthy("disabled with -journal=false: a crash can leave metadata half written")
	case j.State == "corrupt":
		return unhealthy("this node's journal is corrupt; unmount the filesystem and run goclusterfs-fsck")
	case j.State == "recovering":
		return degraded("replaying this node's journal")
	}
	return healthy("")
}

func recoveryHealth(slots []int) api.ComponentHealth {
	if len(slots) == 0 {
		return healthy("")
	}
	names := make([]string, len(slots))
	for i, slot := range slots {
		names[i] = fmt.Sprint(slot)
	}
	return degraded("replaying the journal of the crashed node in slot %s; files it had locked wait until the replay ends", strings.Join(names, ", "))
}

func broadcastHealth(on bool, problem string) api.ComponentHealth {
	switch {
	case !on:
		return healthy("disabled with -broadcast=false: peers see this node's changes after -cache-ttl")
	case problem != "":
		return degraded("%s; peers see this node's changes only after -cache-ttl until it recovers. Check the network between the nodes and the -mcast-port and -unicast-port firewall rules", problem)
	}
	return healthy("")
}

// heartbeatHealth warns before the node stops itself: past node.HeartbeatTimeout/2 without a heartbeat, it exits.
func heartbeatHealth(age time.Duration) api.ComponentHealth {
	if age <= 2*node.HeartbeatInterval {
		return healthy("")
	}
	return degraded("the last heartbeat reached this node's registry slot %v ago; at %v this node stops so peers can take over its locks. Heartbeats are writes to the shared device: check its latency",
		age.Round(100*time.Millisecond), node.HeartbeatTimeout/2)
}

func silentPeersHealth(names []string, quiet time.Duration) api.ComponentHealth {
	if len(names) == 0 {
		return healthy("")
	}
	return degraded("no broadcast heard from %s for over %v, though the registry shows them mounted; this node sees their changes only after -cache-ttl. Check that multicast reaches this node, and that they run with -broadcast and the same -mcast-group and -mcast-port", strings.Join(names, ", "), quiet)
}
