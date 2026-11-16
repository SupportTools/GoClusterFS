package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/supporttools/GoClusterFS/internal/cache/broadcast"
	"github.com/supporttools/GoClusterFS/internal/node"
)

// expectedPeers lists the nodes the registry shows mounted, which should all
// be sending this node broadcasts.
func expectedPeers(reg *node.Registry) func() ([]broadcast.ExpectedPeer, error) {
	return func() ([]broadcast.ExpectedPeer, error) {
		slots, err := reg.GetActiveNodes()
		if err != nil {
			return nil, err
		}
		peers := make([]broadcast.ExpectedPeer, 0, len(slots))
		for _, s := range slots {
			host := strings.TrimRight(string(s.Hostname[:]), "\x00")
			peers = append(peers, broadcast.ExpectedPeer{
				UUID:    s.UUID,
				Name:    fmt.Sprintf("%s (%x)", host, s.UUID[:4]),
				Mounted: time.Unix(s.MountTime, 0),
			})
		}
		return peers, nil
	}
}
