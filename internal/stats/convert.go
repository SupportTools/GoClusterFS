package stats

import (
	"encoding/hex"
	"strings"
	"time"

	"github.com/supporttools/GoClusterFS/internal/api"
	"github.com/supporttools/GoClusterFS/internal/cache"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/trim"
)

func hitRate(hits, misses uint64) float64 {
	if hits+misses == 0 {
		return 0
	}
	return float64(hits) / float64(hits+misses)
}

func hitRateInfo(size int, hits, misses uint64) api.CacheHitRateInfo {
	return api.CacheHitRateInfo{Size: size, Hits: hits, Misses: misses, HitRate: hitRate(hits, misses)}
}

func cacheResponse(c cache.Stats, l lock.Stats) *api.CacheStatsResponse {
	resp := &api.CacheStatsResponse{
		InodeCache:    hitRateInfo(c.InodeCacheSize, c.InodeCacheHits, c.InodeCacheMisses),
		ExtentCache:   hitRateInfo(c.ExtentCacheSize, c.ExtentCacheHits, c.ExtentCacheMisses),
		NegativeCache: hitRateInfo(c.NegativeCacheSize, c.NegativeHits, c.NegativeMisses),
		LockHintCache: hitRateInfo(l.HintCacheSize, l.HintHits, l.HintMisses),
	}
	resp.InodeCache.Capacity = c.InodeCacheCapacity
	resp.ExtentCache.Capacity = c.ExtentCacheCapacity
	resp.NegativeCache.Capacity = c.NegativeCacheCapacity
	if c.BroadcastEnabled {
		resp.Broadcast = &api.BroadcastTables{Peers: c.PeerCount, Senders: c.BroadcastSenders, MaxPeers: c.BroadcastMaxPeers}
	}
	return resp
}

func dirIndexInfo(d dir.IndexStats) *api.DirIndexInfo {
	return &api.DirIndexInfo{
		Dirs:     d.Dirs,
		Entries:  d.Entries,
		Limit:    d.Limit,
		Hits:     d.Hits,
		Misses:   d.Misses,
		Rebuilds: d.Rebuilds,
		HitRate:  hitRate(d.Hits, d.Misses),
	}
}

func lockResponse(l lock.Stats) *api.LockStatsResponse {
	r := &api.LockStatsResponse{
		HintCacheHits:    l.HintHits,
		HintCacheMisses:  l.HintMisses,
		HintCacheHitRate: hitRate(l.HintHits, l.HintMisses),
		ActiveLocks:      l.Held,
		PendingLocks:     l.Waiting,
	}
	for _, t := range l.Types {
		r.Types = append(r.Types, api.LockTypeStats{Type: t.Name, Acquires: t.Acquires, Blocked: t.Blocked, WaitSeconds: t.Wait.Seconds()})
	}
	return r
}

func journalResponse(j journal.JournalStats) *api.JournalStatsResponse {
	return &api.JournalStatsResponse{
		State:             strings.ToLower(j.State.String()),
		HeadOffset:        j.HeadOffset,
		TailOffset:        j.TailOffset,
		AvailableSpace:    j.AvailableSpace,
		ActiveTxnCount:    j.ActiveTxnCount,
		CommitCount:       j.CommitCount,
		BytesWritten:      j.BytesWritten,
		LastCheckpointSeq: j.LastCheckpointSeq,
		BatchSyncCount:    j.BatchSyncCount,
	}
}

func trimResponse(t trim.Stats) *api.TrimStatsResponse {
	return &api.TrimStatsResponse{
		TrimOperations:    t.DiscardCalls,
		TrimBytes:         t.BlocksDiscarded * trim.BlockSize,
		TrimBlocks:        t.BlocksDiscarded,
		PendingTrimBlocks: t.PendingBlocks,
		LastTrimAt:        timestamp(t.LastDiscard),
		TrimErrors:        t.DiscardErrors,
		TrimEnabled:       !t.Unsupported,
		BatchSize:         t.BatchSize,
	}
}

func capacityInfo(totalBlocks, freeBlocks, blockSize uint64) *api.CapacityInfo {
	freeBlocks = min(freeBlocks, totalBlocks)
	used := totalBlocks - freeBlocks
	info := &api.CapacityInfo{
		TotalBytes:     totalBlocks * blockSize,
		UsedBytes:      used * blockSize,
		FreeBytes:      freeBlocks * blockSize,
		TotalBlocks:    totalBlocks,
		UsedBlocks:     used,
		FreeBlocks:     freeBlocks,
		BlockSizeBytes: blockSize,
	}
	if totalBlocks > 0 {
		info.UsedPercent = float64(used) / float64(totalBlocks) * 100
	}
	return info
}

// clusterResponse describes the registry's non-empty slots. GoClusterFS has no
// leader, so every node reports the role "peer".
func clusterResponse(localUUID [16]byte, localSlot int, slots []*node.Slot, now time.Time) *api.ClusterInfoResponse {
	resp := &api.ClusterInfoResponse{
		NodeID: hex.EncodeToString(localUUID[:]),
		Role:   "peer",
		State:  "unknown",
	}
	for i, slot := range slots {
		if slot == nil {
			continue
		}
		state := slotState(slot, now)
		resp.ClusterSize++
		if state == node.NodeStateActive.String() {
			resp.HealthyNodes++
		}
		if i == localSlot {
			resp.NodeAddress = slot.Host()
			resp.State = state
			resp.LastHeartbeatAt = timestamp(unixTime(slot.HeartbeatTime))
			resp.JoinedAt = timestamp(unixTime(slot.MountTime))
			continue
		}
		resp.Peers = append(resp.Peers, api.ClusterPeerInfo{
			NodeID:   hex.EncodeToString(slot.UUID[:]),
			Address:  slot.Host(),
			State:    state,
			LastSeen: timestamp(unixTime(slot.HeartbeatTime)),
		})
	}
	return resp
}

// slotState is the slot's registry state, or "stale" for a node whose heartbeat
// is older than node.HeartbeatTimeout but that no peer has declared dead yet.
func slotState(slot *node.Slot, now time.Time) string {
	live := slot.State == node.NodeStateActive || slot.State == node.NodeStateLeaving
	if live && now.After(slot.LeaseExpiry()) {
		return "stale"
	}
	return slot.State.String()
}

func unixTime(sec int64) time.Time {
	if sec == 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0)
}

func timestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
