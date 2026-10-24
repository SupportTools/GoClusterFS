package fsck

import "time"

// SetHeartbeatWatch shortens how long fsck watches node heartbeats.
func SetHeartbeatWatch(stale, recent time.Duration) (restore func()) {
	oldStale, oldRecent := staleHeartbeatWatch, recentHeartbeatWatch
	staleHeartbeatWatch, recentHeartbeatWatch = stale, recent
	return func() { staleHeartbeatWatch, recentHeartbeatWatch = oldStale, oldRecent }
}
