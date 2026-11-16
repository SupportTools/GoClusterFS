package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/supporttools/GoClusterFS/internal/api"
)

type healthSources struct {
	deviceError      func() error
	journal          func() *api.JournalStatsResponse
	replaying        func() []int
	broadcastOn      bool
	broadcastProblem func() string
}

func registerHealthChecks(srv *api.Server, src healthSources) {
	check := func(name string, fn func() api.ComponentHealth) {
		srv.RegisterHealthChecker(api.NewHealthCheckFunc(name, func(context.Context) api.ComponentHealth { return fn() }))
	}
	check("device", func() api.ComponentHealth { return deviceHealth(src.deviceError()) })
	check("journal", func() api.ComponentHealth { return journalHealth(src.journal()) })
	check("recovery", func() api.ComponentHealth { return recoveryHealth(src.replaying()) })
	check("broadcast", func() api.ComponentHealth { return broadcastHealth(src.broadcastOn, src.broadcastProblem()) })
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
