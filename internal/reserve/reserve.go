// Package reserve hands writes data blocks from a per-node batch, recorded in the journal, without the bitmap lock.
package reserve

import (
	"context"
	"errors"
	"log"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/supporttools/GoClusterFS/internal/journal"
)

const (
	DefaultSize = 64
	DefaultIdle = 10 * time.Second
	// MaxPeerWait bounds how long a full bitmap waits for peers' reserves before ENOSPC.
	MaxPeerWait = 10 * time.Second
)

type Bitmap interface {
	Reserve(ctx context.Context, n int, record func([]uint64) error) ([]uint64, error)
	FreeCount() uint64
	TotalBlocks() uint64
}

type Journal interface {
	Begin() *journal.Transaction
	Commit(txn *journal.Transaction) (uint64, error)
	CompleteIntent(seq uint64) error
	IntentLag(seq uint64) (lag, size uint32, ok bool)
}

type Config struct {
	Size int           // blocks per reserve; 0 turns reserves off
	Idle time.Duration // an untouched reserve goes back after this long
}

type Stats struct {
	Blocks    int    // blocks reserved and not handed out
	Refills   uint64 // reserves taken from the bitmap
	Returns   uint64 // reserves given back
	Waits     uint64 // allocations that found the bitmap full and waited for other nodes
	WaitNanos uint64 // time those allocations waited
}

type Manager struct {
	cfg       Config
	bm        Bitmap
	j         Journal
	free      func(ctx context.Context, blocks []uint64)
	liveNodes func() int
	broadcast func()

	// Shared while a write logs a use, exclusive while the record changes.
	commits sync.RWMutex
	mu      sync.Mutex
	untaken []uint64
	out     map[uint64]bool // handed out, use not logged yet
	seq     uint64          // the reserve record, 0 when there is none
	touched time.Time

	freeCount uint64
	countedAt time.Time
	nodes     int
	nodesAt   time.Time

	refills, returns, waits, waitNanos atomic.Uint64
	stop                               chan struct{}
	done                               sync.WaitGroup
}

func New(cfg Config, bm Bitmap, j Journal, free func(context.Context, []uint64), liveNodes func() int) *Manager {
	if cfg.Idle <= 0 {
		cfg.Idle = DefaultIdle
	}
	return &Manager{cfg: cfg, bm: bm, j: j, free: free, liveNodes: liveNodes, out: make(map[uint64]bool)}
}

func (m *Manager) SetBroadcast(fn func()) {
	m.broadcast = fn
}

// Take reports false when reserves are off or space is low; the caller then uses the bitmap.
func (m *Manager) Take(ctx context.Context) (uint64, bool) {
	if m.cfg.Size == 0 {
		return 0, false
	}
	m.mu.Lock()
	if len(m.untaken) > 0 && !m.lowSpaceLocked() {
		defer m.mu.Unlock()
		return m.handOutLocked(), true
	}
	m.mu.Unlock()

	m.commits.Lock()
	defer m.commits.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lowSpaceLocked() {
		m.returnLocked(ctx)
		return 0, false
	}
	if len(m.untaken) == 0 && !m.refillLocked(ctx) {
		return 0, false
	}
	return m.handOutLocked(), true
}

// TakeReady never refills, since refilling takes the bitmap lock and callers hold an inode lock.
func (m *Manager) TakeReady() (uint64, bool) {
	if m.cfg.Size == 0 {
		return 0, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.untaken) == 0 || m.lowSpaceLocked() {
		return 0, false
	}
	return m.handOutLocked(), true
}

func (m *Manager) handOutLocked() uint64 {
	m.touched = time.Now()
	b := m.untaken[0]
	m.untaken = m.untaken[1:]
	m.out[b] = true
	return b
}

func (m *Manager) Untake(blocks ...uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range blocks {
		if m.out[b] {
			delete(m.out, b)
			m.untaken = append(m.untaken, b)
		}
	}
	slices.Sort(m.untaken)
}

// Logged runs log, which commits the use of blocks, while the reserve record cannot change.
func (m *Manager) Logged(blocks []uint64, log func() error) error {
	m.commits.RLock()
	defer m.commits.RUnlock()
	if err := log(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range blocks {
		delete(m.out, b)
	}
	return nil
}

// ReturnAll frees the untaken blocks; blocks still handed out move to a new record.
func (m *Manager) ReturnAll(ctx context.Context) {
	m.commits.Lock()
	defer m.commits.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.returnLocked(ctx)
}

// Refresh re-logs a record that has fallen far behind the tail, so it stops pinning the journal head.
func (m *Manager) Refresh(ctx context.Context) {
	m.commits.Lock()
	defer m.commits.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seq == 0 {
		return
	}
	lag, size, ok := m.j.IntentLag(m.seq)
	if !ok || lag < size/4 {
		return
	}
	if err := m.relogLocked(append(slices.Clone(m.untaken), m.outBlocks()...)); err != nil {
		log.Printf("[reserve] renewing the reserve record failed: %v", err)
	}
}

// WaitForPeers returns this node's reserve, asks peers for theirs and reports whether to retry.
func (m *Manager) WaitForPeers(ctx context.Context, waited time.Duration) bool {
	if m.cfg.Size == 0 {
		return false
	}
	if waited >= MaxPeerWait {
		m.waits.Add(1)
		m.waitNanos.Add(uint64(waited))
		log.Printf("[INFO] [reserve] no free blocks after waiting %v for other nodes' reserves", waited.Round(time.Millisecond))
		return false
	}
	if waited == 0 {
		log.Printf("[INFO] [reserve] the bitmap is full; asking other nodes to give back their reserved blocks")
		m.ReturnAll(ctx)
		if m.broadcast != nil {
			m.broadcast()
		}
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(200 * time.Millisecond):
	}
	return true
}

func (m *Manager) Waited(d time.Duration) {
	m.waits.Add(1)
	m.waitNanos.Add(uint64(d))
	log.Printf("[INFO] [reserve] got blocks after waiting %v for other nodes' reserves", d.Round(time.Millisecond))
}

func (m *Manager) Start() {
	if m.cfg.Size == 0 {
		return
	}
	m.stop = make(chan struct{})
	m.done.Go(func() {
		tick := time.NewTicker(m.cfg.Idle / 2)
		defer tick.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-tick.C:
				m.mu.Lock()
				idle := m.seq != 0 && len(m.out) == 0 && time.Since(m.touched) >= m.cfg.Idle
				m.mu.Unlock()
				if idle {
					m.ReturnAll(context.Background())
				}
			}
		}
	})
}

func (m *Manager) Stop(ctx context.Context) {
	if m.stop != nil {
		close(m.stop)
		m.done.Wait()
		m.stop = nil
	}
	m.ReturnAll(ctx)
}

func (m *Manager) Stats() Stats {
	m.mu.Lock()
	n := len(m.untaken)
	m.mu.Unlock()
	return Stats{Blocks: n, Refills: m.refills.Load(), Returns: m.returns.Load(), Waits: m.waits.Load(), WaitNanos: m.waitNanos.Load()}
}

func (m *Manager) lowSpaceLocked() bool {
	free, nodes := m.freeCountLocked(), m.liveNodesLocked()
	return free < max(m.bm.TotalBlocks()/100, uint64(2*m.cfg.Size*nodes))
}

// Blocks still handed out move into the new record.
func (m *Manager) refillLocked(ctx context.Context) bool {
	n := min(m.cfg.Size, int(m.freeCountLocked()/uint64(16*m.liveNodesLocked())))
	if n <= 0 {
		return false
	}
	carried := m.outBlocks()
	var newSeq uint64
	blocks, err := m.bm.Reserve(ctx, n, func(picked []uint64) error {
		var err error
		newSeq, err = m.commitRecord(append(slices.Clone(picked), carried...))
		return err
	})
	if err != nil {
		if newSeq != 0 {
			m.endLocked(newSeq, blocks)
		}
		if !errors.Is(err, context.Canceled) {
			log.Printf("[reserve] taking %d blocks failed: %v", n, err)
		}
		return false
	}
	if m.seq != 0 {
		if err := m.j.CompleteIntent(m.seq); err != nil {
			log.Printf("[reserve] ending reserve record %d failed: %v", m.seq, err)
		}
	}
	m.seq, m.untaken = newSeq, blocks
	m.freeCount -= uint64(len(blocks))
	m.refills.Add(1)
	return true
}

// Ending the record before freeing means a crash leaks blocks instead of replay double-freeing them.
func (m *Manager) returnLocked(ctx context.Context) {
	if m.seq == 0 {
		return
	}
	untaken := m.untaken
	m.untaken = nil
	if out := m.outBlocks(); len(out) > 0 {
		if err := m.relogLocked(out); err != nil {
			log.Printf("[reserve] keeping %d reserved blocks: renewing the record failed: %v", len(untaken), err)
			m.untaken = untaken
			return
		}
	} else {
		if err := m.j.CompleteIntent(m.seq); err != nil {
			log.Printf("[reserve] keeping %d reserved blocks: ending the record failed: %v", len(untaken), err)
			m.untaken = untaken
			return
		}
		m.seq = 0
	}
	if len(untaken) > 0 {
		m.free(ctx, untaken)
		m.freeCount += uint64(len(untaken))
	}
	m.returns.Add(1)
}

func (m *Manager) relogLocked(blocks []uint64) error {
	seq, err := m.commitRecord(blocks)
	if err != nil {
		return err
	}
	if err := m.j.CompleteIntent(m.seq); err != nil {
		log.Printf("[reserve] ending reserve record %d failed: %v", m.seq, err)
	}
	m.seq = seq
	return nil
}

func (m *Manager) endLocked(seq uint64, blocks []uint64) {
	if err := m.j.CompleteIntent(seq); err != nil {
		log.Printf("[reserve] leaving %d blocks allocated: ending record %d failed: %v", len(blocks), seq, err)
		return
	}
	m.free(context.Background(), blocks)
}

func (m *Manager) commitRecord(blocks []uint64) (uint64, error) {
	txn := m.j.Begin()
	if err := txn.LogBlockReserve(blocks); err != nil {
		return 0, err
	}
	return m.j.Commit(txn)
}

func (m *Manager) outBlocks() []uint64 {
	out := make([]uint64, 0, len(m.out))
	for b := range m.out {
		out = append(out, b)
	}
	slices.Sort(out)
	return out
}

func (m *Manager) freeCountLocked() uint64 {
	if time.Since(m.countedAt) > 2*time.Second {
		m.freeCount, m.countedAt = m.bm.FreeCount(), time.Now()
	}
	return m.freeCount
}

func (m *Manager) liveNodesLocked() int {
	if m.liveNodes == nil {
		return 1
	}
	if time.Since(m.nodesAt) > 10*time.Second {
		m.nodes, m.nodesAt = max(1, m.liveNodes()), time.Now()
	}
	return m.nodes
}
