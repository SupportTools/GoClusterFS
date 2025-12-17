// Package lock implements distributed CAS-based locking for GoClusterFS.
//
// Locks are stored on the shared block device and acquired using atomic
// compare-and-swap operations. This allows coordination between cluster
// nodes without any external services.
//
// Lock Hierarchy (must acquire in this order to prevent deadlocks):
//
//	Level 0: Volume
//	Level 1: SnapshotRegistry
//	Level 2: Snapshot (by ID)
//	Level 3: Journal (per-node)
//	Level 4: RefCount Region
//	Level 5: LockRegion
//	Level 6: BlockBitmap
//	Level 7: SFBContainer
//	Level 8: Inode (by number)
//	Level 9: Extent (by block)
package lock

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// RecordSize is the size of each lock record.
	// Must match SectorSize (4096) for O_DIRECT alignment.
	RecordSize = 4096

	// MaxLocks is the maximum number of concurrent locks.
	MaxLocks = 1 << 20 // 1M locks

	// DefaultTimeout is the default lock acquisition timeout.
	DefaultTimeout = 30 * time.Second

	// RetryBaseDelay is the initial retry delay.
	RetryBaseDelay = 10 * time.Millisecond

	// RetryMaxDelay is the maximum retry delay.
	RetryMaxDelay = 1 * time.Second

	// StaleThreshold is how old a lock must be to consider it stale.
	StaleThreshold = 15 * time.Second
)

// LockType identifies what kind of resource is being locked.
type LockType uint8

const (
	LockTypeVolume       LockType = 0
	LockTypeSnapshotReg  LockType = 1
	LockTypeSnapshot     LockType = 2
	LockTypeJournal      LockType = 3
	LockTypeRefCount     LockType = 4
	LockTypeLockRegion   LockType = 5
	LockTypeBlockBitmap  LockType = 6
	LockTypeSFBContainer LockType = 7
	LockTypeInode        LockType = 8
	LockTypeExtent       LockType = 9
)

// LockMode indicates shared (read) or exclusive (write) access.
type LockMode uint8

const (
	LockModeNone      LockMode = 0
	LockModeShared    LockMode = 1 // Multiple readers allowed
	LockModeExclusive LockMode = 2 // Single writer, no readers
)

// Record represents an on-disk lock entry.
// Size: 4096 bytes (sector-aligned for O_DIRECT atomic writes)
type Record struct {
	Magic       uint32 // 0x4C4F434B "LOCK"
	Type        LockType
	Mode        LockMode
	_           [2]byte    // Padding
	ResourceID  uint64     // What's being locked (inode#, block#, etc.)
	OwnerUUID   [16]byte   // Node that holds the lock
	OwnerSlot   uint32     // Owner's slot in node registry
	AcquireTime int64      // When lock was acquired (Unix timestamp)
	Sequence    uint64     // Monotonic sequence for CAS
	SharedCount uint32     // Number of shared lock holders (for shared mode)
	Reserved    [4032]byte // Padding to 4096 bytes
	Checksum    uint32     // CRC32C of bytes 0-4091
}

const RecordMagic uint32 = 0x4C4F434B // "LOCK"

// OnAcquireFunc is called when a lock is successfully acquired.
// This is used to trigger cache invalidation - the cache manager
// registers a callback to invalidate cached data when locks are acquired.
// Parameters: lockType identifies the resource type, resourceID identifies
// the specific resource (e.g., inode number), mode indicates shared/exclusive.
type OnAcquireFunc func(lockType LockType, resourceID uint64, mode LockMode)

// Manager handles distributed lock operations.
type Manager struct {
	mu        sync.Mutex
	dev       *storage.Device
	offset    uint64   // Lock region offset on device
	size      uint64   // Lock region size
	nodeUUID  [16]byte // This node's UUID
	nodeSlot  int      // This node's slot
	heldLocks map[lockKey]*heldLock

	// Callback for cache invalidation
	onAcquire OnAcquireFunc
}

type lockKey struct {
	Type       LockType
	ResourceID uint64
}

type heldLock struct {
	Mode     LockMode
	RecordID uint64 // Position in lock region
	refCount int    // Number of goroutines from this node holding the lock
}

// Errors
var (
	ErrLockTimeout   = errors.New("lock acquisition timed out")
	ErrLockHeld      = errors.New("lock already held by another node")
	ErrNotLockOwner  = errors.New("not the lock owner")
	ErrDeadlockRisk  = errors.New("lock order violation would cause deadlock")
	ErrInvalidRecord = errors.New("invalid lock record")
)

// NewManager creates a new lock manager.
func NewManager(dev *storage.Device, offset, size uint64, nodeUUID [16]byte, nodeSlot int) *Manager {
	return &Manager{
		dev:       dev,
		offset:    offset,
		size:      size,
		nodeUUID:  nodeUUID,
		nodeSlot:  nodeSlot,
		heldLocks: make(map[lockKey]*heldLock),
	}
}

// SetOnAcquire registers a callback that is invoked when a lock is acquired.
// This is typically used by the cache manager to invalidate cached data
// when a lock is acquired, ensuring fresh data is read from disk.
//
// Only exclusive lock acquisitions trigger invalidation since shared locks
// (readers) don't modify data and can safely use cached values.
func (m *Manager) SetOnAcquire(fn OnAcquireFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onAcquire = fn
}

// Acquire attempts to acquire a lock with the given parameters.
// Blocks until the lock is acquired or context is cancelled.
//
// Same-node reference counting: When multiple goroutines from the same node
// need the same lock, the first goroutine acquires the distributed lock and
// subsequent goroutines increment a local reference count. This allows
// concurrent FUSE operations from the same node to share locks efficiently
// while still providing cross-node serialization.
func (m *Manager) Acquire(ctx context.Context, lockType LockType, resourceID uint64, mode LockMode) error {
	key := lockKey{Type: lockType, ResourceID: resourceID}

	m.mu.Lock()
	// Check if we already hold this lock (same node) - just increment refCount
	if held, ok := m.heldLocks[key]; ok {
		held.refCount++
		log.Printf("[DEBUG] lock.Acquire: type=%d res=%d - reusing existing lock (refCount=%d)", lockType, resourceID, held.refCount)
		m.mu.Unlock()
		return nil
	}

	// Check lock ordering to prevent deadlocks
	if err := m.checkLockOrder(lockType); err != nil {
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()

	// Calculate record position (hash-based)
	recordID := m.hashToRecord(lockType, resourceID)
	retryCount := 0
	startTime := time.Now()

	// Retry loop with exponential backoff
	delay := RetryBaseDelay
	for {
		// Always attempt acquire first, then check context after failure
		// This ensures we try at least once even if context is nearly expired
		acquired, err := m.tryAcquire(recordID, lockType, resourceID, mode)
		if err != nil {
			log.Printf("[DEBUG] lock.Acquire: type=%d res=%d - tryAcquire error: %v", lockType, resourceID, err)
			return err
		}
		if acquired {
			m.mu.Lock()
			// Double-check in case another goroutine acquired while we were trying
			if held, ok := m.heldLocks[key]; ok {
				// Another goroutine beat us - just increment refCount
				held.refCount++
				log.Printf("[DEBUG] lock.Acquire: type=%d res=%d - late arrival, reusing (refCount=%d)", lockType, resourceID, held.refCount)
				m.mu.Unlock()
				// Release the on-disk lock we just acquired since we'll share the existing one
				m.releaseRecord(recordID)
				return nil
			}
			m.heldLocks[key] = &heldLock{Mode: mode, RecordID: recordID, refCount: 1}
			log.Printf("[DEBUG] lock.Acquire: type=%d res=%d - acquired distributed lock (retries=%d, dur=%v)", lockType, resourceID, retryCount, time.Since(startTime))

			// Trigger cache invalidation callback for exclusive locks.
			// We do this before returning so the caller reads fresh data.
			// Only exclusive locks trigger invalidation since shared locks
			// are for reading and can safely use cached values.
			onAcquire := m.onAcquire
			m.mu.Unlock()

			if onAcquire != nil && mode == LockModeExclusive {
				onAcquire(lockType, resourceID, mode)
			}
			return nil
		}

		// Check again if another goroutine acquired while we were waiting
		m.mu.Lock()
		if held, ok := m.heldLocks[key]; ok {
			held.refCount++
			log.Printf("[DEBUG] lock.Acquire: type=%d res=%d - piggyback on existing (refCount=%d)", lockType, resourceID, held.refCount)
			m.mu.Unlock()
			return nil
		}
		m.mu.Unlock()

		retryCount++
		log.Printf("[DEBUG] lock.Acquire: type=%d res=%d - loop iteration, retryCount now %d", lockType, resourceID, retryCount)

		// Check context cancellation, but allow at least 3 retries for same-node
		// CAS races where the other goroutine is still adding to heldLocks
		if retryCount >= 3 {
			select {
			case <-ctx.Done():
				log.Printf("[DEBUG] lock.Acquire: type=%d res=%d - TIMEOUT after %d retries (%v)", lockType, resourceID, retryCount, time.Since(startTime))
				return ErrLockTimeout
			default:
			}
		}

		// Add jitter to prevent thundering herd
		jitter := time.Duration(rand.Int63n(int64(delay / 2)))
		time.Sleep(delay + jitter)

		// Exponential backoff
		delay *= 2
		if delay > RetryMaxDelay {
			delay = RetryMaxDelay
		}
	}
}

// Release releases a held lock.
// With reference counting, this decrements the local count and only releases
// the distributed lock when the last goroutine releases.
func (m *Manager) Release(lockType LockType, resourceID uint64) error {
	key := lockKey{Type: lockType, ResourceID: resourceID}

	m.mu.Lock()
	held, ok := m.heldLocks[key]
	if !ok {
		log.Printf("[DEBUG] lock.Release: type=%d res=%d - NOT FOUND in heldLocks!", lockType, resourceID)
		m.mu.Unlock()
		return ErrNotLockOwner
	}

	// Decrement reference count
	held.refCount--
	if held.refCount > 0 {
		// Other goroutines still using the lock
		log.Printf("[DEBUG] lock.Release: type=%d res=%d - decremented refCount to %d", lockType, resourceID, held.refCount)
		m.mu.Unlock()
		return nil
	}

	// Last reference - release the distributed lock
	log.Printf("[DEBUG] lock.Release: type=%d res=%d - last ref, releasing distributed lock", lockType, resourceID)
	delete(m.heldLocks, key)
	recordID := held.RecordID
	m.mu.Unlock()

	err := m.releaseRecord(recordID)
	if err != nil {
		log.Printf("[DEBUG] lock.Release: type=%d res=%d - releaseRecord error: %v", lockType, resourceID, err)
	}
	return err
}

// tryAcquire attempts a single lock acquisition.
func (m *Manager) tryAcquire(recordID uint64, lockType LockType, resourceID uint64, mode LockMode) (bool, error) {
	// Read current record
	record, err := m.readRecord(recordID)
	if err != nil {
		return false, err
	}

	now := time.Now()

	// Check if lock is free or can be acquired
	canAcquire := false
	reason := "unknown"
	switch {
	case record.Mode == LockModeNone:
		// Lock is free
		canAcquire = true
		reason = "free"

	case record.Type != lockType || record.ResourceID != resourceID:
		// Hash collision - different resource, treat as free
		// In production, we'd use a more sophisticated collision resolution
		canAcquire = true
		reason = "hash_collision"

	case m.isLockStale(record):
		// Lock is stale (owner died)
		canAcquire = true
		reason = "stale"

	case mode == LockModeShared && record.Mode == LockModeShared:
		// Adding another shared holder
		canAcquire = true
		reason = "shared_add"

	case record.OwnerUUID == m.nodeUUID:
		// Same node owns the lock - this can happen due to a race between
		// Release clearing heldLocks and releaseRecord clearing on-disk.
		// Safe to re-acquire since the previous holder from this node is gone.
		canAcquire = true
		reason = "same_node_reacquire"
	}

	if !canAcquire {
		log.Printf("[DEBUG] lock.tryAcquire: type=%d res=%d - cannot acquire, held by node=%x mode=%d", lockType, resourceID, record.OwnerUUID[:8], record.Mode)
		return false, nil
	}

	log.Printf("[DEBUG] lock.tryAcquire: type=%d res=%d - canAcquire=true reason=%s", lockType, resourceID, reason)

	// Prepare new record
	newRecord := &Record{
		Magic:       RecordMagic,
		Type:        lockType,
		Mode:        mode,
		ResourceID:  resourceID,
		OwnerUUID:   m.nodeUUID,
		OwnerSlot:   uint32(m.nodeSlot),
		AcquireTime: now.Unix(),
		Sequence:    record.Sequence + 1,
	}
	if mode == LockModeShared {
		newRecord.SharedCount = record.SharedCount + 1
	}

	// CAS the record
	return m.casRecord(recordID, record, newRecord)
}

// releaseRecord releases a lock record.
func (m *Manager) releaseRecord(recordID uint64) error {
	record, err := m.readRecord(recordID)
	if err != nil {
		return err
	}

	// Verify we own it
	if record.OwnerUUID != m.nodeUUID {
		// For shared locks, just decrement count
		if record.Mode == LockModeShared && record.SharedCount > 1 {
			newRecord := *record
			newRecord.SharedCount--
			newRecord.Sequence++
			_, err := m.casRecord(recordID, record, &newRecord)
			return err
		}
		return ErrNotLockOwner
	}

	// Clear the record
	newRecord := &Record{
		Sequence: record.Sequence + 1,
	}

	_, err = m.casRecord(recordID, record, newRecord)
	return err
}

// isLockStale checks if a lock's owner is dead.
func (m *Manager) isLockStale(record *Record) bool {
	if record.Mode == LockModeNone {
		return false
	}
	age := time.Since(time.Unix(record.AcquireTime, 0))
	return age > StaleThreshold
}

// checkLockOrder ensures we're acquiring locks in hierarchy order.
func (m *Manager) checkLockOrder(newType LockType) error {
	for key := range m.heldLocks {
		if key.Type > newType {
			return fmt.Errorf("%w: cannot acquire %v while holding %v",
				ErrDeadlockRisk, newType, key.Type)
		}
	}
	return nil
}

// hashToRecord maps a lock to a record position.
func (m *Manager) hashToRecord(lockType LockType, resourceID uint64) uint64 {
	// Simple hash combining type and resource ID
	hash := uint64(lockType) * 0x9E3779B97F4A7C15
	hash ^= resourceID * 0xC6A4A7935BD1E995
	hash ^= hash >> 33
	hash *= 0xFF51AFD7ED558CCD
	hash ^= hash >> 33

	maxRecords := m.size / RecordSize
	return hash % maxRecords
}

// readRecord reads a lock record from disk.
func (m *Manager) readRecord(recordID uint64) (*Record, error) {
	offset := int64(m.offset) + int64(recordID)*RecordSize

	buf := storage.GetBuffer(RecordSize)
	defer storage.PutBuffer(buf)

	if _, err := m.dev.ReadAt(buf.Bytes(), offset); err != nil {
		return nil, err
	}

	return deserializeRecord(buf.Bytes())
}

// writeRecord writes a lock record to disk.
func (m *Manager) writeRecord(recordID uint64, record *Record) error {
	offset := int64(m.offset) + int64(recordID)*RecordSize

	data := serializeRecord(record)

	buf := storage.GetBuffer(RecordSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), data)

	_, err := m.dev.WriteAt(buf.Bytes(), offset)
	return err
}

// casRecord performs compare-and-swap on a lock record.
func (m *Manager) casRecord(recordID uint64, expected, new *Record) (bool, error) {
	offset := int64(m.offset) + int64(recordID)*RecordSize

	expectedData := serializeRecord(expected)
	newData := serializeRecord(new)

	buf := storage.GetBuffer(RecordSize)
	defer storage.PutBuffer(buf)

	// Read current
	if _, err := m.dev.ReadAt(buf.Bytes(), offset); err != nil {
		return false, err
	}

	// Compare (check sequence number is sufficient for CAS)
	currentSeq := binary.LittleEndian.Uint64(buf.Bytes()[48:56])
	expectedSeq := binary.LittleEndian.Uint64(expectedData[48:56])
	if currentSeq != expectedSeq {
		return false, nil
	}

	// Write new
	copy(buf.Bytes(), newData)
	if _, err := m.dev.WriteAt(buf.Bytes(), offset); err != nil {
		return false, err
	}

	return true, nil
}

// serializeRecord converts a Record to bytes.
func serializeRecord(r *Record) []byte {
	buf := make([]byte, RecordSize)

	binary.LittleEndian.PutUint32(buf[0:4], r.Magic)
	buf[4] = byte(r.Type)
	buf[5] = byte(r.Mode)
	binary.LittleEndian.PutUint64(buf[8:16], r.ResourceID)
	copy(buf[16:32], r.OwnerUUID[:])
	binary.LittleEndian.PutUint32(buf[32:36], r.OwnerSlot)
	binary.LittleEndian.PutUint64(buf[40:48], uint64(r.AcquireTime))
	binary.LittleEndian.PutUint64(buf[48:56], r.Sequence)
	binary.LittleEndian.PutUint32(buf[56:60], r.SharedCount)

	// Checksum
	checksum := storage.CRC32C(buf[:RecordSize-4])
	binary.LittleEndian.PutUint32(buf[RecordSize-4:], checksum)

	return buf
}

// deserializeRecord parses a Record from bytes.
func deserializeRecord(buf []byte) (*Record, error) {
	if len(buf) < RecordSize {
		return nil, ErrInvalidRecord
	}

	magic := binary.LittleEndian.Uint32(buf[0:4])
	sequence := binary.LittleEndian.Uint64(buf[48:56])

	// Empty/released record is valid (Mode == None)
	// IMPORTANT: Preserve Sequence for proper CAS operation even when Magic==0.
	// A released lock has Magic=0 but Sequence=N, and we must read that Sequence
	// so the next acquisition can properly compare-and-swap.
	if magic == 0 {
		return &Record{
			Sequence: sequence,
		}, nil
	}

	// Verify checksum for active records
	stored := binary.LittleEndian.Uint32(buf[RecordSize-4:])
	computed := storage.CRC32C(buf[:RecordSize-4])
	if stored != computed {
		return nil, ErrInvalidRecord
	}

	r := &Record{
		Magic:       magic,
		Type:        LockType(buf[4]),
		Mode:        LockMode(buf[5]),
		ResourceID:  binary.LittleEndian.Uint64(buf[8:16]),
		OwnerSlot:   binary.LittleEndian.Uint32(buf[32:36]),
		AcquireTime: int64(binary.LittleEndian.Uint64(buf[40:48])),
		Sequence:    sequence,
		SharedCount: binary.LittleEndian.Uint32(buf[56:60]),
		Checksum:    stored,
	}
	copy(r.OwnerUUID[:], buf[16:32])

	return r, nil
}
