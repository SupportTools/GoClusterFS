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
	tick        = time.Second
	// MaxPeerWait covers a peer's idle return, so a lost broadcast cannot cause ENOSPC.
	MaxPeerWait = DefaultIdle + 2*tick
)

type Bitmap interface {
	Reserve(ctx context.Context, n int, record func([]uint64) (keep bool), end func() error) ([]uint64, error)
	FreeCount() uint64
	TotalBlocks() uint64
}

type Journal interface {
	Begin() *journal.Transaction
	Commit(txn *journal.Transaction) (uint64, error)
	Abort(txn *journal.Transaction)
	CompleteIntentDurable(seq uint64) error
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

type state int

const (
	open state = iota
	ending
	ended
)

// A record is one BlockReserve journal entry; its blocks are freed only after it ended.
type record struct {
	seq   uint64
	live  int // its blocks that are untaken or handed out
	state state
	back  []uint64 // handed back while ending
}

// Writers hold an inode lock when they call TakeReady, Untake and Logged, so mu
// never spans a cluster lock or I/O, and writers never take endMu.
type Manager struct {
	cfg       Config
	bm        Bitmap
	j         Journal
	release   func(ctx context.Context, blocks []uint64)
	liveNodes func() int
	broadcast func()

	endMu sync.Mutex

	mu        sync.Mutex
	untaken   []uint64
	owner     map[uint64]*record // untaken and handed-out blocks
	out       map[uint64]bool
	used      []*record // all blocks used, end not written yet
	stale     []*record // ending failed; nothing is freed until they end
	orphans   []uint64  // handed back after their record ended
	refilling bool
	touched   time.Time

	freeCount   atomic.Uint64
	countedAt   atomic.Int64
	nodes       atomic.Int64
	nodesAt     atomic.Int64
	askedAt     atomic.Int64
	returnAsked atomic.Bool

	refills, returns, waits, waitNanos atomic.Uint64
	stop                               chan struct{}
	done                               sync.WaitGroup
}

func New(cfg Config, bm Bitmap, j Journal, release func(context.Context, []uint64), liveNodes func() int) *Manager {
	if cfg.Idle <= 0 {
		cfg.Idle = DefaultIdle
	}
	m := &Manager{cfg: cfg, bm: bm, j: j, release: release, liveNodes: liveNodes,
		owner: make(map[uint64]*record), out: make(map[uint64]bool)}
	m.refreshCounts()
	return m
}

func (m *Manager) SetBroadcast(fn func()) {
	m.broadcast = fn
}

// Take reports false when reserves are off or space is low; the caller then uses
// the bitmap. Refilling takes the bitmap lock, so the caller must not hold an inode lock.
func (m *Manager) Take(ctx context.Context) (uint64, bool) {
	if m.cfg.Size == 0 {
		return 0, false
	}
	m.refreshCounts()
	if m.lowSpace() {
		m.ReturnAll(ctx)
		return 0, false
	}
	if b, ok := m.TakeReady(); ok {
		return b, true
	}
	if !m.refill(ctx) {
		return 0, false
	}
	return m.TakeReady()
}

func (m *Manager) TakeReady() (uint64, bool) {
	if m.cfg.Size == 0 || m.lowSpace() {
		return 0, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.untaken) == 0 {
		return 0, false
	}
	m.touched = time.Now()
	b := m.untaken[0]
	m.untaken = m.untaken[1:]
	m.out[b] = true
	return b, true
}

// Untake puts back blocks a write took and never tried to log.
func (m *Manager) Untake(blocks ...uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range blocks {
		if !m.out[b] {
			continue
		}
		delete(m.out, b)
		switch r := m.owner[b]; r.state {
		case open:
			m.untaken = append(m.untaken, b)
		case ending:
			r.back = append(r.back, b)
		case ended:
			delete(m.owner, b)
			r.live--
			m.orphans = append(m.orphans, b)
		}
	}
	slices.Sort(m.untaken)
}

// Logged runs log, which commits the use of blocks. They leave the reserve even
// if log fails, since a failed commit may still be on disk, unless the journal
// was full and wrote nothing.
func (m *Manager) Logged(blocks []uint64, log func() error) error {
	err := log()
	if errors.Is(err, journal.ErrJournalFull) {
		m.Untake(blocks...)
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range blocks {
		if !m.out[b] {
			continue
		}
		delete(m.out, b)
		r := m.owner[b]
		delete(m.owner, b)
		r.live--
		if r.live == 0 && r.state == open {
			r.state = ending
			m.used = append(m.used, r)
		}
	}
	return err
}

// ReturnAll gives the untaken blocks back. Each record ends before its blocks are
// freed, so a crash in between leaks them instead of replay freeing them twice.
// The frees run after endMu is dropped, so its holders wait only for the journal.
func (m *Manager) ReturnAll(ctx context.Context) {
	freed, ended := m.endForReturn()
	m.free(ctx, freed)
	if ended {
		m.returns.Add(1)
	}
}

func (m *Manager) endForReturn() (freed []uint64, ended bool) {
	m.endMu.Lock()
	defer m.endMu.Unlock()
	m.endUsed()
	if !m.endStale() {
		return nil, false
	}
	groups := m.takeUntaken(func(*record) bool { return true }, false)
	for r, blocks := range groups {
		if err := m.j.CompleteIntentDurable(r.seq); err != nil {
			log.Printf("[reserve] keeping %d reserved blocks: ending record %d failed: %v", len(blocks), r.seq, err)
			m.reopen(r, blocks)
			continue
		}
		m.finishEnd(r)
		freed, ended = append(freed, blocks...), true
	}
	return append(freed, m.takeOrphans()...), ended
}

// Refresh moves the untaken blocks of records far behind the journal tail into a
// new record and ends the old ones, so they stop holding the journal head back.
// A crash then leaks the old records' handed-out blocks instead of freeing them.
func (m *Manager) Refresh(ctx context.Context) {
	if !m.endMu.TryLock() {
		return
	}
	defer m.endMu.Unlock()
	m.endUsed()
	m.endStale()
	old := m.oldRecords()
	if len(old) == 0 {
		return
	}
	moved := m.takeUntaken(func(r *record) bool { return old[r] }, true)
	var blocks []uint64
	for _, bs := range moved {
		blocks = append(blocks, bs...)
	}
	if len(blocks) > 0 {
		seq, err := m.commitRecord(blocks)
		if err != nil {
			log.Printf("[reserve] renewing %d reserved blocks failed: %v", len(blocks), err)
			for r, bs := range moved {
				m.reopen(r, bs)
			}
			return
		}
		m.add(seq, blocks)
	}
	for r := range moved {
		if err := m.j.CompleteIntentDurable(r.seq); err != nil {
			log.Printf("[reserve] ending renewed record %d failed: %v", r.seq, err)
			m.mu.Lock()
			m.stale = append(m.stale, r)
			m.mu.Unlock()
			continue
		}
		m.finishEnd(r)
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
		m.AskReturn()
	}
	if now := time.Now(); m.broadcast != nil && now.Sub(time.Unix(0, m.askedAt.Load())) >= tick {
		m.askedAt.Store(now.UnixNano())
		m.broadcast()
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

// AskReturn gives the reserve back at the next tick, so a flood of peer requests costs one return per tick.
func (m *Manager) AskReturn() {
	m.returnAsked.Store(true)
}

func (m *Manager) Start() {
	if m.cfg.Size == 0 {
		return
	}
	m.stop = make(chan struct{})
	m.done.Go(func() {
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-t.C:
				m.refreshCounts()
				if m.returnAsked.Swap(false) || m.idle() {
					m.ReturnAll(context.Background())
				} else {
					m.tidy(context.Background())
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

func (m *Manager) refill(ctx context.Context) bool {
	m.mu.Lock()
	if m.refilling || len(m.untaken) > 0 {
		ready := len(m.untaken) > 0
		m.mu.Unlock()
		return ready
	}
	m.refilling = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.refilling = false
		m.mu.Unlock()
	}()

	n := min(m.cfg.Size, int(m.freeCount.Load()/uint64(16*m.nodes.Load())))
	if n <= 0 {
		return false
	}
	var seq uint64
	var recordErr, endErr error
	blocks, err := m.bm.Reserve(ctx, n, func(picked []uint64) bool {
		seq, recordErr = m.commitRecord(picked)
		return !errors.Is(recordErr, journal.ErrJournalFull)
	}, func() error {
		if endErr = recordErr; endErr == nil {
			endErr = m.j.CompleteIntentDurable(seq)
		}
		return endErr
	})
	switch {
	case recordErr != nil:
		if !errors.Is(recordErr, journal.ErrJournalFull) {
			log.Printf("[reserve] leaving %d blocks allocated, since their record may be on disk: %v", len(blocks), recordErr)
		}
		return false
	case err != nil:
		if !errors.Is(err, context.Canceled) {
			log.Printf("[reserve] taking %d blocks failed: %v", n, err)
		}
		if endErr != nil {
			m.mu.Lock()
			m.stale = append(m.stale, &record{seq: seq, state: ending})
			m.mu.Unlock()
		}
		return false
	}
	m.add(seq, blocks)
	m.adjustFree(-len(blocks))
	m.refills.Add(1)
	return true
}

func (m *Manager) add(seq uint64, blocks []uint64) {
	r := &record{seq: seq, live: len(blocks)}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range blocks {
		m.owner[b] = r
	}
	m.untaken = append(m.untaken, blocks...)
	slices.Sort(m.untaken)
}

// takeUntaken removes the untaken blocks of the picked records and marks those
// records ending; withOut also picks records whose blocks are all handed out.
func (m *Manager) takeUntaken(pick func(*record) bool, withOut bool) map[*record][]uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	candidates := make(map[*record]bool)
	if withOut {
		for _, r := range m.owner {
			candidates[r] = true
		}
	} else {
		for _, b := range m.untaken {
			candidates[m.owner[b]] = true
		}
	}
	groups := make(map[*record][]uint64)
	for r := range candidates {
		if r.state == open && pick(r) {
			r.state = ending
			groups[r] = nil
		}
	}
	kept := m.untaken[:0]
	for _, b := range m.untaken {
		r := m.owner[b]
		if _, ok := groups[r]; !ok {
			kept = append(kept, b)
			continue
		}
		groups[r] = append(groups[r], b)
		delete(m.owner, b)
		r.live--
	}
	m.untaken = kept
	return groups
}

func (m *Manager) reopen(r *record, blocks []uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r.state = open
	r.live += len(blocks)
	for _, b := range blocks {
		m.owner[b] = r
	}
	m.untaken = append(m.untaken, blocks...)
	m.untaken = append(m.untaken, r.back...)
	r.back = nil
	slices.Sort(m.untaken)
	if r.live == 0 {
		r.state = ending
		m.used = append(m.used, r)
	}
}

func (m *Manager) finishEnd(r *record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r.state = ended
	for _, b := range r.back {
		delete(m.owner, b)
		r.live--
	}
	m.orphans = append(m.orphans, r.back...)
	r.back = nil
}

func (m *Manager) endUsed() {
	m.mu.Lock()
	used := m.used
	m.used = nil
	m.mu.Unlock()
	for _, r := range used {
		if err := m.j.CompleteIntentDurable(r.seq); err != nil {
			log.Printf("[reserve] ending used record %d failed: %v", r.seq, err)
			m.mu.Lock()
			m.stale = append(m.stale, r)
			m.mu.Unlock()
			continue
		}
		m.finishEnd(r)
	}
}

// endStale retries ending records whose end failed and reports whether none is left.
func (m *Manager) endStale() bool {
	m.mu.Lock()
	stale := m.stale
	m.stale = nil
	m.mu.Unlock()
	for _, r := range stale {
		if err := m.j.CompleteIntentDurable(r.seq); err != nil {
			m.mu.Lock()
			m.stale = append(m.stale, r)
			m.mu.Unlock()
			continue
		}
		m.finishEnd(r)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.stale) > 0 {
		log.Printf("[reserve] freeing no reserved blocks until %d unfinished records end", len(m.stale))
	}
	return len(m.stale) == 0
}

func (m *Manager) takeOrphans() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.stale) > 0 {
		return nil
	}
	orphans := m.orphans
	m.orphans = nil
	return orphans
}

func (m *Manager) free(ctx context.Context, blocks []uint64) {
	if len(blocks) == 0 {
		return
	}
	m.release(context.WithoutCancel(ctx), blocks)
	m.adjustFree(len(blocks))
}

func (m *Manager) adjustFree(delta int) {
	for {
		old := m.freeCount.Load()
		if m.freeCount.CompareAndSwap(old, uint64(max(0, int64(old)+int64(delta)))) {
			return
		}
	}
}

func (m *Manager) tidy(ctx context.Context) {
	m.endMu.Lock()
	m.endUsed()
	m.endStale()
	orphans := m.takeOrphans()
	m.endMu.Unlock()
	m.free(ctx, orphans)
}

func (m *Manager) idle() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.untaken) > 0 && len(m.out) == 0 && time.Since(m.touched) >= m.cfg.Idle
}

func (m *Manager) oldRecords() map[*record]bool {
	m.mu.Lock()
	live := make(map[*record]bool)
	for _, r := range m.owner {
		if r.state == open {
			live[r] = true
		}
	}
	m.mu.Unlock()
	old := make(map[*record]bool)
	for r := range live {
		if lag, size, ok := m.j.IntentLag(r.seq); ok && lag >= size/4 {
			old[r] = true
		}
	}
	return old
}

func (m *Manager) lowSpace() bool {
	nodes := uint64(m.nodes.Load())
	return m.freeCount.Load() < max(m.bm.TotalBlocks()/100, 2*uint64(m.cfg.Size)*nodes)
}

func (m *Manager) refreshCounts() {
	now := time.Now()
	if now.Sub(time.Unix(0, m.countedAt.Load())) > 2*time.Second {
		m.freeCount.Store(m.bm.FreeCount())
		m.countedAt.Store(now.UnixNano())
	}
	if now.Sub(time.Unix(0, m.nodesAt.Load())) > 10*time.Second {
		n := 1
		if m.liveNodes != nil {
			n = max(1, m.liveNodes())
		}
		m.nodes.Store(int64(n))
		m.nodesAt.Store(now.UnixNano())
	}
}

func (m *Manager) commitRecord(blocks []uint64) (uint64, error) {
	txn := m.j.Begin()
	if err := txn.LogBlockReserve(blocks); err != nil {
		m.j.Abort(txn)
		return 0, err
	}
	seq, err := m.j.Commit(txn)
	if err != nil {
		m.j.Abort(txn)
	}
	return seq, err
}
