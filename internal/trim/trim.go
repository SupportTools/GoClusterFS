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
}

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

	// Background loop control
	stopCh   chan struct{}
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
	}
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

		// Final flush of any remaining blocks
		m.mu.Lock()
		defer m.mu.Unlock()

		if len(m.pending) > 0 {
			log.Printf("[trim] final flush of %d pending blocks", len(m.pending))
			m.flushLocked()
		}

		log.Printf("[trim] stopped (queued=%d, discarded=%d, calls=%d)",
			m.blocksQueued, m.blocksDiscarded, m.discardCalls)
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

	// Check if batch size threshold reached
	if len(m.pending) >= m.config.BatchSize {
		log.Printf("[trim] batch threshold reached (%d blocks)", len(m.pending))
		m.flushLocked()
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

	if len(m.pending) >= m.config.BatchSize {
		log.Printf("[trim] batch threshold reached (%d blocks)", len(m.pending))
		m.flushLocked()
	}
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
		case <-ticker.C:
			m.checkAndFlush()
		}
	}
}

// checkAndFlush flushes if the oldest pending block exceeds MaxAge.
func (m *Manager) checkAndFlush() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.pending) == 0 {
		return
	}

	// Find oldest entry
	oldest := m.pending[0].queuedAt
	for _, p := range m.pending {
		if p.queuedAt.Before(oldest) {
			oldest = p.queuedAt
		}
	}

	if time.Since(oldest) >= m.config.MaxAge {
		log.Printf("[trim] age threshold reached (%d blocks, oldest=%v)",
			len(m.pending), time.Since(oldest))
		m.flushLocked()
	}
}

// flushLocked issues BLKDISCARD for all pending blocks.
// Must be called with m.mu held.
func (m *Manager) flushLocked() {
	if len(m.pending) == 0 {
		return
	}

	// Coalesce adjacent blocks into ranges
	ranges := m.coalesceBlocks()

	log.Printf("[trim] flushing %d blocks in %d ranges", len(m.pending), len(ranges))

	// Issue BLKDISCARD for each range
	discarded := uint64(0)
	for _, r := range ranges {
		if err := m.issueDiscard(r); err != nil {
			log.Printf("[trim] discard failed for range [%d, %d): %v",
				r.startBlock, r.startBlock+r.count, err)
			continue
		}
		discarded += r.count
		m.discardCalls++
	}

	m.blocksDiscarded += discarded
	if len(ranges) > 0 {
		m.coalesceRatio = float64(discarded) / float64(len(ranges))
	}

	// Clear pending list
	m.pending = m.pending[:0]
}

// coalesceBlocks groups adjacent blocks into contiguous ranges.
// Returns sorted ranges for efficient BLKDISCARD.
func (m *Manager) coalesceBlocks() []blockRange {
	if len(m.pending) == 0 {
		return nil
	}

	// Sort blocks by block number
	sort.Slice(m.pending, func(i, j int) bool {
		return m.pending[i].blockNum < m.pending[j].blockNum
	})

	var ranges []blockRange
	current := blockRange{
		startBlock: m.pending[0].blockNum,
		count:      1,
	}

	for i := 1; i < len(m.pending); i++ {
		blockNum := m.pending[i].blockNum

		// Check if adjacent to current range
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
