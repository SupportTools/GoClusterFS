package fsck

import "time"

// SetHeartbeatWatch shortens how long fsck watches node heartbeats.
func SetHeartbeatWatch(stale, recent time.Duration) (restore func()) {
	oldStale, oldRecent := staleHeartbeatWatch, recentHeartbeatWatch
	staleHeartbeatWatch, recentHeartbeatWatch = stale, recent
	return func() { staleHeartbeatWatch, recentHeartbeatWatch = oldStale, oldRecent }
}

// SetBeforeRepairWrites runs f right before repair checks the registry again
// and writes refcounts.
func SetBeforeRepairWrites(f func()) (restore func()) {
	old := beforeRepairWrites
	beforeRepairWrites = f
	return func() { beforeRepairWrites = old }
}
