package main

import (
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"

	"github.com/supporttools/GoClusterFS/internal/node"
)

// joinUnfenced lets this node, registered on a volume without storage fencing, go on only when no other node has
// the volume mounted, or when allow says the operator accepts the risk. It checks after Register, so of two nodes
// mounting at once at least one sees the other. A refused node leaves its slot as a crash would: Register may have
// taken back this host's crashed mount with its unreplayed journal, which a peer must then replay.
func joinUnfenced(reg *node.Registry, devicePath string, allow bool) {
	peers, err := reg.MountedPeers()
	switch {
	case err != nil && !allow:
		failf("Cannot join the cluster: reading the node registry to look for other nodes failed: %v. %s has no storage fencing, "+
			"so a node mounts it only alone, or with -unsafe-allow-unfenced", err, devicePath)
	case len(peers) == 0:
		return
	case !allow:
		failf("Cannot join the cluster: %s has no storage fencing, and %s. %s "+
			"Format the volume on a device with persistent reservations (-fencing=pr) so nodes can be fenced, or mount every node "+
			"with -unsafe-allow-unfenced to accept the risk. If those nodes are gone, release their slots with goclusterfs-fsck -release-slot",
			devicePath, describePeers(peers), unfencedRisk)
	}
	log.Printf("[WARN] UNSAFE: %s has no storage fencing, and %s; -unsafe-allow-unfenced lets this node join anyway. %s",
		devicePath, describePeers(peers), unfencedRisk)
}

const unfencedRisk = "Without fencing, a node that pauses, as a VM its hypervisor paused, can resume and write after another node " +
	"replayed its journal. That corrupts file data, and goclusterfs-fsck cannot detect it."

func describePeers(peers map[int]*node.Slot) string {
	var names []string
	for _, i := range slices.Sorted(maps.Keys(peers)) {
		names = append(names, fmt.Sprintf("%d (%s)", i, peers[i].Host()))
	}
	if len(names) == 1 {
		return "the node in slot " + names[0] + " has it mounted"
	}
	return "the nodes in slots " + strings.Join(names, ", ") + " have it mounted"
}
