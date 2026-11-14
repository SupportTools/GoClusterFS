package stats

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/supporttools/GoClusterFS/internal/api"
	"github.com/supporttools/GoClusterFS/internal/cache"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/reserve"
	"github.com/supporttools/GoClusterFS/internal/trim"
)

const (
	CounterInterval = time.Second
	DeviceInterval  = 15 * time.Second
)

type CacheSource interface{ Stats() cache.Stats }

type LockSource interface{ Stats() lock.Stats }

// ReserveSource reports the node's block reserve.
type ReserveSource interface{ Stats() reserve.Stats }

type DirSource interface{ IndexStats() dir.IndexStats }

type JournalSource interface{ Stats() journal.JournalStats }

type TrimSource interface{ Stats() trim.Stats }

type BlockSource interface {
	TotalBlocks() uint64
	FreeCount() uint64
}

type NodeSource interface {
	LocalUUID() [16]byte
	LocalSlot() int
	Slots() ([]*node.Slot, error)
}

// Sources are the managers a Sampler reads. Leave Journal or Trim nil when the
// mount runs without them.
type Sources struct {
	Cache     CacheSource
	Locks     LockSource
	Dirs      DirSource
	Journal   JournalSource
	Trim      TrimSource
	Blocks    BlockSource
	Nodes     NodeSource
	Reserve   ReserveSource
	BlockSize uint64
}

// Sampler implements api.StatsProvider and the capacity half of
// api.StatusProvider from snapshots it refreshes in the background.
type Sampler struct {
	src          Sources
	counterEvery time.Duration
	deviceEvery  time.Duration

	current    api.AllStatsResponse
	lastDevice time.Time
	all        atomic.Pointer[api.AllStatsResponse]
	capacity   atomic.Pointer[api.CapacityInfo]
	wanted     atomic.Bool
	nudge      chan struct{}

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

var _ api.StatsProvider = (*Sampler)(nil)

// StartSampler takes a first sample of every source and keeps refreshing it until Stop.
func StartSampler(src Sources) *Sampler {
	return startSampler(src, CounterInterval, DeviceInterval)
}

func startSampler(src Sources, counterEvery, deviceEvery time.Duration) *Sampler {
	s := &Sampler{
		src:          src,
		counterEvery: counterEvery,
		deviceEvery:  deviceEvery,
		nudge:        make(chan struct{}, 1),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	s.sampleDevice()
	s.sampleCounters()
	go s.run()
	return s
}

// Stop ends sampling and waits for a sample in progress to finish. The last
// snapshot stays readable.
func (s *Sampler) Stop() {
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.done
}

func (s *Sampler) run() {
	defer close(s.done)
	counters := time.NewTicker(s.counterEvery)
	defer counters.Stop()
	device := time.NewTicker(s.deviceEvery)
	defer device.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-counters.C:
			s.sampleCounters()
		case <-device.C:
			if s.wanted.Swap(false) {
				s.sampleDevice()
			}
		case <-s.nudge:
			if time.Since(s.lastDevice) >= s.deviceEvery {
				s.wanted.Store(false)
				s.sampleDevice()
			}
		}
	}
}

func (s *Sampler) sampleCounters() {
	var locks lock.Stats
	if s.src.Locks != nil {
		locks = s.src.Locks.Stats()
		s.current.Lock = lockResponse(locks)
	}
	if s.src.Cache != nil {
		s.current.Cache = cacheResponse(s.src.Cache.Stats(), locks)
	}
	if s.src.Dirs != nil {
		if s.current.Cache == nil {
			s.current.Cache = &api.CacheStatsResponse{}
		}
		s.current.Cache.DirIndex = dirIndexInfo(s.src.Dirs.IndexStats())
	}
	if s.src.Journal != nil {
		s.current.Journal = journalResponse(s.src.Journal.Stats())
	}
	if s.src.Reserve != nil {
		r := s.src.Reserve.Stats()
		s.current.Reserve = &api.ReserveStatsResponse{Blocks: r.Blocks, Refills: r.Refills, Returns: r.Returns,
			PeerWaits: r.Waits, PeerWaitSeconds: time.Duration(r.WaitNanos).Seconds()}
	}
	s.current.Trim = &api.TrimStatsResponse{}
	if s.src.Trim != nil {
		s.current.Trim = trimResponse(s.src.Trim.Stats())
	}
	s.current.CollectedAt = time.Now().UTC().Format(time.RFC3339Nano)
	s.publish()
}

// sampleDevice runs only when a request read capacity or cluster data since the
// last device sample, so an idle mount does not scan the bitmap.
func (s *Sampler) sampleDevice() {
	s.lastDevice = time.Now()
	if s.src.Blocks != nil {
		s.capacity.Store(capacityInfo(s.src.Blocks.TotalBlocks(), s.src.Blocks.FreeCount(), s.src.BlockSize))
	}
	if s.src.Nodes != nil {
		slots, _ := s.src.Nodes.Slots()
		s.current.Cluster = clusterResponse(s.src.Nodes.LocalUUID(), s.src.Nodes.LocalSlot(), slots, time.Now())
	}
	s.publish()
}

// wantDevice asks for a device sample at the next tick, or right away when the
// last one is older than the device interval.
func (s *Sampler) wantDevice() {
	if !s.wanted.Swap(true) {
		select {
		case s.nudge <- struct{}{}:
		default:
		}
	}
}

func (s *Sampler) publish() {
	snapshot := s.current
	s.all.Store(&snapshot)
}

func (s *Sampler) CacheStats() *api.CacheStatsResponse     { return s.all.Load().Cache }
func (s *Sampler) JournalStats() *api.JournalStatsResponse { return s.all.Load().Journal }
func (s *Sampler) TrimStats() *api.TrimStatsResponse       { return s.all.Load().Trim }
func (s *Sampler) LockStats() *api.LockStatsResponse       { return s.all.Load().Lock }

func (s *Sampler) ClusterInfo() *api.ClusterInfoResponse {
	s.wantDevice()
	return s.all.Load().Cluster
}

func (s *Sampler) AllStats() *api.AllStatsResponse {
	s.wantDevice()
	return s.all.Load()
}

// SFBStats returns nil because SFB containers are disabled.
func (s *Sampler) SFBStats() *api.SFBStatsResponse { return nil }

// Capacity returns the block bitmap's counts from the last device sample.
func (s *Sampler) Capacity() *api.CapacityInfo {
	s.wantDevice()
	return s.capacity.Load()
}
