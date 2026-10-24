// Package trim implements batched TRIM/discard operations for freed blocks.
//
// When blocks are freed (file deletion/truncation), they are queued for TRIM
// rather than immediately discarded. The TRIM manager batches these requests
// and issues BLKDISCARD ioctl when:
//   - Batch size reaches 256 blocks, OR
//   - Oldest request exceeds 5 seconds
//
// Adjacent blocks are coalesced into contiguous ranges before issuing discards
// to minimize ioctl overhead.
package trim

import (
	"context"
	"log"
	"sort"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// BLKDISCARD is the Linux ioctl for TRIM/discard.
	// Defined in <linux/fs.h> as _IO(0x12, 119) = 0x1277
	BLKDISCARD = 0x1277

	// DefaultBatchSize is the number of blocks before triggering a flush.
	DefaultBatchSize = 256

	// DefaultMaxAge is the maximum age before flushing pending TRIMs.
	DefaultMaxAge = 5 * time.Second

	// BlockSize is the size of each data block (1MB).
	BlockSize = 1 << 20
)

// Config holds TRIM manager configuration.
type Config struct {
	// BatchSize is the number of freed blocks before triggering flush.
	BatchSize int

	// MaxAge is the maximum time a block can wait in the queue.
	MaxAge time.Duration

	// DataOffset is the byte offset where data blocks start on the device.
	DataOffset uint64

	// Enabled controls whether TRIM operations are actually issued.
	// When false, blocks are still tracked but no ioctl is issued.
	Enabled bool

	// Discard replaces the BLKDISCARD ioctl when set. Offsets and lengths are in bytes.
	Discard func(offset, length uint64) error
}

// FreeGuard calls discard with the blocks that are still free, and keeps them
// from being allocated until discard returns. Freed blocks wait in the queue
// for up to MaxAge, and any node may allocate and write one in that time.
type FreeGuard = func(ctx context.Context, blocks []uint64, discard func(free []uint64)) error

// guardTimeout bounds how long a flush waits for the guard's allocation lock.
const guardTimeout = 30 * time.Second

// DefaultConfig returns the default TRIM configuration.
func DefaultConfig() Config {
	return Config{
		BatchSize:  DefaultBatchSize,
		MaxAge:     DefaultMaxAge,
		DataOffset: 0,
		Enabled:    true,
	}
}

// pendingBlock represents a block queued for TRIM.
type pendingBlock struct {
	blockNum uint64
	queuedAt time.Time
}

// blockRange represents a contiguous range of blocks for TRIM.
type blockRange struct {
	startBlock uint64
	count      uint64
}

// Manager manages batched TRIM operations.
type Manager struct {
	mu     sync.Mutex
	dev    *storage.Device
	config Config

	// Pending blocks for TRIM
	pending []pendingBlock
	guard   FreeGuard

	// Background loop control
	stopCh   chan struct{}
	flushCh  chan struct{}
	flushWg  sync.WaitGroup
	stopOnce sync.Once // Ensures Stop() is idempotent

	// Stats
	blocksQueued    uint64
	blocksDiscarded uint64
	discardCalls    uint64
	coalesceRatio   float64 // Average blocks per ioctl
}

// NewManager creates a new TRIM manager.
func NewManager(dev *storage.Device, cfg Config) *Manager {
	return &Manager{
		dev:     dev,
		config:  cfg,
		pending: make([]pendingBlock, 0, cfg.BatchSize),
		stopCh:  make(chan struct{}),
		flushCh: make(chan struct{}, 1),
	}
}

// SetFreeGuard makes every flush discard only the blocks the guard reports as free.
func (m *Manager) SetFreeGuard(guard FreeGuard) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.guard = guard
}

// Start begins the background flush loop.
func (m *Manager) Start() {
	m.flushWg.Add(1)
	go m.flushLoop()
	log.Printf("[trim] started (batch=%d, maxAge=%v, enabled=%v)",
		m.config.BatchSize, m.config.MaxAge, m.config.Enabled)
}

// Stop stops the background loop and flushes pending TRIMs.
// This function is idempotent - calling it multiple times is safe.
func (m *Manager) Stop() error {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		m.flushWg.Wait()

		m.flush("final")

		stats := m.Stats()
		log.Printf("[trim] stopped (queued=%d, discarded=%d, calls=%d)",
			stats.BlocksQueued, stats.BlocksDiscarded, stats.DiscardCalls)
	})
	return nil
}

// Queue adds a freed block to the pending queue.
// This is called from bitmap.Free() when a block is deallocated.
func (m *Manager) Queue(blockNum uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.pending = append(m.pending, pendingBlock{
		blockNum: blockNum,
		queuedAt: time.Now(),
	})
	m.blocksQueued++
	m.wakeIfFullLocked()
}

// wakeIfFullLocked asks the flush loop to flush once the batch is full. Queue
// runs inside bitmap.Free, which holds the bitmap lock the guard needs, so it
// must not flush itself.
func (m *Manager) wakeIfFullLocked() {
	if len(m.pending) < m.config.BatchSize {
		return
	}
	select {
	case m.flushCh <- struct{}{}:
	default:
	}
}

// QueueRange adds multiple contiguous freed blocks to the pending queue.
// This is more efficient than calling Queue() for each block.
func (m *Manager) QueueRange(startBlock, count uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for i := uint64(0); i < count; i++ {
		m.pending = append(m.pending, pendingBlock{
			blockNum: startBlock + i,
			queuedAt: now,
		})
		m.blocksQueued++
	}
	m.wakeIfFullLocked()
}

// flushLoop runs periodically to check age-based flush conditions.
func (m *Manager) flushLoop() {
	defer m.flushWg.Done()

	// Check more frequently than MaxAge to ensure timely flushes
	ticker := time.NewTicker(m.config.MaxAge / 2)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-m.flushCh:
			m.flush("batch threshold")
		case <-ticker.C:
			m.checkAndFlush()
		}
	}
}

// checkAndFlush flushes if the oldest pending block exceeds MaxAge.
func (m *Manager) checkAndFlush() {
	m.mu.Lock()
	due := false
	if len(m.pending) > 0 {
		oldest := m.pending[0].queuedAt
		for _, p := range m.pending {
			if p.queuedAt.Before(oldest) {
				oldest = p.queuedAt
			}
		}
		due = time.Since(oldest) >= m.config.MaxAge
	}
	m.mu.Unlock()

	if due {
		m.flush("age threshold")
	}
}

// flush discards the pending blocks. It runs the guard without holding m.mu,
// because bitmap.Free holds the bitmap lock while it calls Queue.
func (m *Manager) flush(reason string) {
	m.mu.Lock()
	blocks := make([]uint64, len(m.pending))
	for i, p := range m.pending {
		blocks[i] = p.blockNum
	}
	m.pending = m.pending[:0]
	guard := m.guard
	m.mu.Unlock()

	if len(blocks) == 0 {
		return
	}
	log.Printf("[trim] %s: flushing %d blocks", reason, len(blocks))

	if guard == nil {
		m.discardBlocks(blocks)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), guardTimeout)
	defer cancel()
	if err := guard(ctx, blocks, m.discardBlocks); err != nil {
		log.Printf("[trim] skipping discard of %d blocks: %v", len(blocks), err)
	}
}

// discardBlocks issues one discard per contiguous range of blocks.
func (m *Manager) discardBlocks(blocks []uint64) {
	ranges := coalesceBlocks(blocks)

	var discarded, calls uint64
	for _, r := range ranges {
		if err := m.issueDiscard(r); err != nil {
			log.Printf("[trim] discard failed for range [%d, %d): %v",
				r.startBlock, r.startBlock+r.count, err)
			continue
		}
		discarded += r.count
		calls++
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocksDiscarded += discarded
	m.discardCalls += calls
	if len(ranges) > 0 {
		m.coalesceRatio = float64(discarded) / float64(len(ranges))
	}
}

// coalesceBlocks sorts blocks, drops duplicates and groups adjacent blocks into
// contiguous ranges. A block freed, reallocated and freed again is queued twice.
func coalesceBlocks(blocks []uint64) []blockRange {
	if len(blocks) == 0 {
		return nil
	}

	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })

	var ranges []blockRange
	current := blockRange{
		startBlock: blocks[0],
		count:      1,
	}

	for i := 1; i < len(blocks); i++ {
		blockNum := blocks[i]

		if blockNum < current.startBlock+current.count {
			continue
		}
		if blockNum == current.startBlock+current.count {
			current.count++
		} else {
			// Not adjacent - save current range and start new one
			ranges = append(ranges, current)
			current = blockRange{
				startBlock: blockNum,
				count:      1,
			}
		}
	}

	// Don't forget the last range
	ranges = append(ranges, current)

	return ranges
}

// issueDiscard issues BLKDISCARD ioctl for a block range.
func (m *Manager) issueDiscard(r blockRange) error {
	if !m.config.Enabled {
		// TRIM disabled - just log at debug level
		log.Printf("[trim] (disabled) would discard range [%d, %d) = %d blocks",
			r.startBlock, r.startBlock+r.count, r.count)
		return nil
	}

	// Calculate byte offset and length
	// startBlock is relative to data region, add DataOffset
	byteOffset := m.config.DataOffset + r.startBlock*BlockSize
	byteLength := r.count * BlockSize
	if m.config.Discard != nil {
		return m.config.Discard(byteOffset, byteLength)
	}

	// BLKDISCARD takes [2]uint64{offset, length}
	args := [2]uint64{byteOffset, byteLength}

	// Issue ioctl
	fd := m.dev.Fd()
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		fd,
		BLKDISCARD,
		uintptr(unsafe.Pointer(&args[0])),
	)

	if errno != 0 {
		// EOPNOTSUPP is common for file-backed devices - treat as warning, not error
		if errno == syscall.EOPNOTSUPP || errno == syscall.ENOTTY {
			log.Printf("[trim] device does not support BLKDISCARD (normal for files)")
			return nil // Not an error - just unsupported
		}
		return errno
	}

	return nil
}

// Stats returns TRIM manager statistics.
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()

	return Stats{
		BlocksQueued:    m.blocksQueued,
		BlocksDiscarded: m.blocksDiscarded,
		DiscardCalls:    m.discardCalls,
		PendingBlocks:   uint64(len(m.pending)),
		CoalesceRatio:   m.coalesceRatio,
	}
}

// Stats holds TRIM manager statistics.
type Stats struct {
	BlocksQueued    uint64  // Total blocks queued for TRIM
	BlocksDiscarded uint64  // Total blocks successfully discarded
	DiscardCalls    uint64  // Number of BLKDISCARD ioctls issued
	PendingBlocks   uint64  // Blocks currently waiting
	CoalesceRatio   float64 // Average blocks per ioctl call
}
