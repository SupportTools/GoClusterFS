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

// SetReleaseHooks runs before once Release has checked the registry, and
// during once Release holds the slot's recovery lease.
func SetReleaseHooks(before, during func()) (restore func()) {
	oldBefore, oldDuring := beforeReleaseWrites, whileReleaseHoldsLease
	beforeReleaseWrites, whileReleaseHoldsLease = before, during
	return func() { beforeReleaseWrites, whileReleaseHoldsLease = oldBefore, oldDuring }
}
