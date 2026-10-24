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
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// getGoroutineID returns the current goroutine's ID.
// This is used for per-goroutine lock order tracking.
// Note: This is a common pattern for debugging/tracing, but should not be
// used for program logic beyond tracking/debugging.
func getGoroutineID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	// Parse "goroutine 123 [...]"
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

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

	// RetryMaxDelay is the maximum retry delay. It is short so a waiter notices
	// soon after another node lets a cached hold go.
	RetryMaxDelay = 100 * time.Millisecond

	// ClaimSettleDelay is how long a node waits after writing a claim before it
	// re-reads the record to check the claim survived. Taking a lock is a read
	// then a write, so it must exceed the longest time between another node's
	// read of the record and its write landing on disk.
	ClaimSettleDelay = 20 * time.Millisecond

	// CacheGrace is how long a node keeps a record after its last local holder
	// releases it, so the next local acquire needs no disk I/O and no settle wait.
	CacheGrace = 250 * time.Millisecond

	// StaleThreshold is how old a lock must be to consider it stale.
	StaleThreshold = 15 * time.Second

	// HintCacheSize is the default capacity for the sequence hint cache.
	// The cache stores expected sequence numbers to enable optimistic lock
	// acquisition, saving one disk read when the hint is correct.
	HintCacheSize = 4096
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

// hintLRU is a simple LRU cache for lock sequence hints.
// It's embedded in the lock package to avoid import cycles with cache package.
type hintLRU struct {
	mu       sync.Mutex
	capacity int
	items    map[uint64]*list.Element
	order    *list.List
}

type hintEntry struct {
	key   uint64
	value *lockHint
}

func newHintLRU(capacity int) *hintLRU {
	return &hintLRU{
		capacity: capacity,
		items:    make(map[uint64]*list.Element),
		order:    list.New(),
	}
}

func (c *hintLRU) Put(key uint64, value *lockHint) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		// Update existing
		c.order.MoveToFront(elem)
		elem.Value.(*hintEntry).value = value
		return
	}

	// Evict oldest if at capacity
	if c.order.Len() >= c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			c.order.Remove(oldest)
			delete(c.items, oldest.Value.(*hintEntry).key)
		}
	}

	// Add new entry
	entry := &hintEntry{key: key, value: value}
	elem := c.order.PushFront(entry)
	c.items[key] = elem
}

// Peek returns the value without updating LRU order.
func (c *hintLRU) Peek(key uint64) (*lockHint, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		return elem.Value.(*hintEntry).value, true
	}
	return nil, false
}

// lockHint stores cached sequence numbers for optimistic lock acquisition.
// When we release a lock, we know the new sequence number. On the next
// acquire attempt, we can try using that cached sequence directly, saving
// one disk read if no other node modified the record.
type lockHint struct {
	Sequence uint64    // Expected sequence number
	Mode     LockMode  // Last known mode (None, Shared, Exclusive)
	UpdateAt time.Time // When this hint was last updated
}

// Manager handles distributed lock operations.
type Manager struct {
	mu        sync.Mutex
	dev       *storage.Device
	offset    uint64   // Lock region offset on device
	size      uint64   // Lock region size
	nodeUUID  [16]byte // This node's UUID
	nodeSlot  int      // This node's slot
	heldLocks map[lockKey]*heldLock

	// Several keys can hash to one record; the record stays held until the last key is released.
	records map[uint64]*heldRecord
	// gates serialize this node's on-disk operations per record.
	gates map[uint64]*sync.Mutex

	// Callback for cache invalidation
	onAcquire OnAcquireFunc

	// Hint cache for optimistic lock acquisition.
	// Stores expected sequence numbers indexed by recordID.
	// When a lock is released, we cache the new sequence number.
	// On the next acquire, we can try an optimistic CAS using
	// the cached sequence, saving one disk read if correct.
	hintCache *hintLRU

	// Metrics for hint cache effectiveness
	hintHits   uint64 // Optimistic acquires that succeeded
	hintMisses uint64 // Optimistic acquires that failed (fell back to read)
}

type lockKey struct {
	Type       LockType
	ResourceID uint64
}

type heldLock struct {
	Mode       LockMode
	RecordID   uint64         // Position in lock region
	refCount   int            // Number of goroutines from this node holding the lock
	goroutines map[uint64]int // Goroutine ID -> reference count for that goroutine
}

type heldRecord struct {
	mode    LockMode
	keys    int
	renewed time.Time
	lost    bool
	// cacheGen identifies the current cached period; keys == 0 means cached.
	cacheGen uint64
}

func covers(held, want LockMode) bool {
	return held == LockModeExclusive || held == want
}

// renewAfter is the age of an exclusive hold after which a key joining the record
// rewrites AcquireTime, so a record kept busy by overlapping keys never looks stale.
var renewAfter = StaleThreshold / 3

var (
	claimSettleDelay = ClaimSettleDelay
	cacheGrace       = CacheGrace
)

// holdAge is the time since a hold was last written to disk. It takes the larger
// of the monotonic and wall-clock ages, so a pause that stops one clock still counts.
func holdAge(renewed time.Time) time.Duration {
	return max(time.Since(renewed), time.Now().Round(0).Sub(renewed.Round(0)))
}

// Errors
var (
	ErrLockTimeout   = errors.New("lock acquisition timed out")
	ErrLockHeld      = errors.New("lock already held by another node")
	ErrNotLockOwner  = errors.New("not the lock owner")
	ErrDeadlockRisk  = errors.New("lock order violation would cause deadlock")
	ErrInvalidRecord = errors.New("invalid lock record")
	ErrLockLost      = errors.New("lock record was taken over by another node")
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
		records:   make(map[uint64]*heldRecord),
		gates:     make(map[uint64]*sync.Mutex),
		hintCache: newHintLRU(HintCacheSize),
	}
}

// NewManagerWithHintCacheSize creates a lock manager with a custom hint cache size.
// Use this for testing or when you need to tune the cache size.
func NewManagerWithHintCacheSize(dev *storage.Device, offset, size uint64, nodeUUID [16]byte, nodeSlot int, hintCacheSize int) *Manager {
	return &Manager{
		dev:       dev,
		offset:    offset,
		size:      size,
		nodeUUID:  nodeUUID,
		nodeSlot:  nodeSlot,
		heldLocks: make(map[lockKey]*heldLock),
		records:   make(map[uint64]*heldRecord),
		gates:     make(map[uint64]*sync.Mutex),
		hintCache: newHintLRU(hintCacheSize),
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

// HintStats returns statistics about the hint cache effectiveness.
// hits = optimistic acquires that succeeded (saved one disk read)
// misses = optimistic acquires that failed (fell back to normal path)
// hitRate = hits / (hits + misses), or 0 if no attempts
func (m *Manager) HintStats() (hits, misses uint64, hitRate float64) {
	hits = atomic.LoadUint64(&m.hintHits)
	misses = atomic.LoadUint64(&m.hintMisses)
	total := hits + misses
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}
	return
}

// Acquire attempts to acquire a lock with the given parameters.
// Blocks until the lock is acquired or context is cancelled.
//
// Same-node reference counting: When multiple goroutines from the same node
// need the same lock, the first goroutine acquires the distributed lock and
// subsequent goroutines increment a local reference count. This allows
// concurrent FUSE operations from the same node to share locks efficiently
// while still providing cross-node serialization.
//
// Resources that hash to the same record share it: a node holding the record
// for one resource excludes other nodes from every resource in that record.
func (m *Manager) Acquire(ctx context.Context, lockType LockType, resourceID uint64, mode LockMode) error {
	key := lockKey{Type: lockType, ResourceID: resourceID}
	recordID := m.hashToRecord(lockType, resourceID)
	gid := getGoroutineID()

	m.mu.Lock()
	if m.reuseHeldKeyLocked(key, mode, gid) {
		m.mu.Unlock()
		return nil
	}
	if err := m.checkLockOrder(lockType); err != nil {
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()

	retryCount := 0
	startTime := time.Now()

	// Retry loop with exponential backoff
	delay := RetryBaseDelay
	for {
		// Always attempt acquire first, then check context after failure
		// This ensures we try at least once even if context is nearly expired
		held, added, err := m.attemptAcquire(key, recordID, mode, gid)
		if err != nil {
			log.Printf("[DEBUG] lock.Acquire: type=%d res=%d - attempt error: %v", lockType, resourceID, err)
			return err
		}
		if held {
			log.Printf("[DEBUG] lock.Acquire: type=%d res=%d - acquired (retries=%d, dur=%v, gid=%d)", lockType, resourceID, retryCount, time.Since(startTime), gid)

			// Trigger cache invalidation callback for exclusive locks.
			// We do this before returning so the caller reads fresh data.
			// Only exclusive locks trigger invalidation since shared locks
			// are for reading and can safely use cached values.
			m.mu.Lock()
			onAcquire := m.onAcquire
			m.mu.Unlock()
			if added && onAcquire != nil && mode == LockModeExclusive {
				onAcquire(lockType, resourceID, mode)
			}
			return nil
		}

		retryCount++

		// Only a deadline, or DefaultTimeout, ends the wait: FUSE cancels a request's
		// context whenever the caller gets a signal, and the caller expects the op to finish.
		if retryCount >= 3 && (errors.Is(ctx.Err(), context.DeadlineExceeded) || time.Since(startTime) > DefaultTimeout) {
			log.Printf("[DEBUG] lock.Acquire: type=%d res=%d - TIMEOUT after %d retries (%v)", lockType, resourceID, retryCount, time.Since(startTime))
			return ErrLockTimeout
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

// attemptAcquire makes one acquisition attempt. added reports whether this
// call registered the key rather than joining another goroutine's hold.
//
// A key joins this node's hold on its record when the hold's mode covers the
// request. An exclusive request waits for this node's own readers, so callers
// must not hold a local mutex across Acquire that those readers may need.
func (m *Manager) attemptAcquire(key lockKey, recordID uint64, mode LockMode, gid uint64) (held, added bool, err error) {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	m.mu.Lock()
	if rec, ok := m.records[recordID]; ok && rec.keys == 0 && (rec.lost || !covers(rec.mode, mode) || holdAge(rec.renewed) > renewAfter) {
		// A cached hold that is lost, too weak, or old enough that another node may
		// have taken it over (after a pause, say) is released, then acquired afresh.
		delete(m.records, recordID)
		m.mu.Unlock()
		if err := m.releaseRecord(recordID, rec.mode); err != nil && !errors.Is(err, ErrNotLockOwner) {
			return false, false, err
		}
		m.mu.Lock()
	}
	if rec, ok := m.records[recordID]; ok {
		switch {
		case rec.lost:
			m.mu.Unlock()
			return false, false, ErrLockLost
		case !covers(rec.mode, mode):
			selfDeadlock := m.goroutineHoldsRecordLocked(recordID, gid)
			m.mu.Unlock()
			if selfDeadlock {
				return false, false, fmt.Errorf("%w: cannot upgrade a shared lock to exclusive", ErrDeadlockRisk)
			}
			return false, false, nil
		}
		renew := rec.mode == LockModeExclusive && holdAge(rec.renewed) > renewAfter
		m.mu.Unlock()
		if renew {
			if err := m.renewRecord(recordID, rec); err != nil {
				return false, false, err
			}
		}
		m.mu.Lock()
		if held, ok := m.heldLocks[key]; ok {
			held.refCount++
			held.goroutines[gid]++
			m.mu.Unlock()
			return true, false, nil
		}
		rec.keys++
		m.addHeldKeyLocked(key, recordID, mode, gid)
		m.mu.Unlock()
		return true, true, nil
	}
	m.mu.Unlock()

	acquired, err := m.tryAcquire(recordID, key.Type, key.ResourceID, mode)
	if err != nil || !acquired {
		return false, false, err
	}

	m.mu.Lock()
	m.records[recordID] = &heldRecord{mode: mode, keys: 1, renewed: time.Now()}
	m.addHeldKeyLocked(key, recordID, mode, gid)
	m.mu.Unlock()
	return true, true, nil
}

func (m *Manager) reuseHeldKeyLocked(key lockKey, mode LockMode, gid uint64) bool {
	held, ok := m.heldLocks[key]
	if !ok {
		return false
	}
	rec := m.records[held.RecordID]
	if rec.lost || !covers(rec.mode, mode) || (rec.mode == LockModeExclusive && holdAge(rec.renewed) > renewAfter) {
		return false
	}
	held.refCount++
	held.goroutines[gid]++
	return true
}

func (m *Manager) addHeldKeyLocked(key lockKey, recordID uint64, mode LockMode, gid uint64) {
	m.heldLocks[key] = &heldLock{
		Mode:       mode,
		RecordID:   recordID,
		refCount:   1,
		goroutines: map[uint64]int{gid: 1},
	}
}

func (m *Manager) goroutineHoldsRecordLocked(recordID uint64, gid uint64) bool {
	for _, held := range m.heldLocks {
		if held.RecordID == recordID && held.goroutines[gid] > 0 {
			return true
		}
	}
	return false
}

func (m *Manager) recordGate(recordID uint64) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	gate, ok := m.gates[recordID]
	if !ok {
		gate = &sync.Mutex{}
		m.gates[recordID] = gate
	}
	return gate
}

// Release releases a held lock.
// With reference counting, this decrements the local count and only releases
// the distributed lock when the last goroutine releases.
func (m *Manager) Release(lockType LockType, resourceID uint64) error {
	key := lockKey{Type: lockType, ResourceID: resourceID}
	gid := getGoroutineID()

	m.mu.Lock()
	held, ok := m.heldLocks[key]
	if !ok {
		log.Printf("[DEBUG] lock.Release: type=%d res=%d gid=%d - NOT FOUND in heldLocks!", lockType, resourceID, gid)
		m.mu.Unlock()
		return ErrNotLockOwner
	}

	held.goroutines[gid]--
	if held.goroutines[gid] <= 0 {
		delete(held.goroutines, gid)
	}

	// Decrement overall reference count
	held.refCount--
	if held.refCount > 0 {
		m.mu.Unlock()
		return nil
	}

	delete(m.heldLocks, key)
	recordID := held.RecordID
	m.mu.Unlock()

	err := m.dropRecordKey(recordID)
	if err != nil {
		log.Printf("[DEBUG] lock.Release: type=%d res=%d - release error: %v", lockType, resourceID, err)
	}
	return err
}

func (m *Manager) dropRecordKey(recordID uint64) error {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	m.mu.Lock()
	rec, ok := m.records[recordID]
	if !ok {
		m.mu.Unlock()
		return ErrNotLockOwner
	}
	rec.keys--
	if rec.keys > 0 {
		m.mu.Unlock()
		return nil
	}
	rec.cacheGen++
	gen := rec.cacheGen
	m.mu.Unlock()

	time.AfterFunc(cacheGrace, func() { m.expireCached(recordID, gen) })
	return nil
}

// expireCached releases a cached hold on disk unless a key joined it again.
func (m *Manager) expireCached(recordID uint64, gen uint64) {
	gate := m.recordGate(recordID)
	gate.Lock()
	defer gate.Unlock()

	m.mu.Lock()
	rec, ok := m.records[recordID]
	if !ok || rec.keys > 0 || rec.cacheGen != gen {
		m.mu.Unlock()
		return
	}
	delete(m.records, recordID)
	m.mu.Unlock()

	if err := m.releaseRecord(recordID, rec.mode); err != nil {
		log.Printf("[DEBUG] lock: releasing cached record %d: %v", recordID, err)
	}
}

// ReleaseCached releases every hold no local key is using. Call it on unmount.
func (m *Manager) ReleaseCached() {
	m.mu.Lock()
	var cached []uint64
	for recordID, rec := range m.records {
		if rec.keys == 0 {
			cached = append(cached, recordID)
		}
	}
	m.mu.Unlock()

	for _, recordID := range cached {
		m.mu.Lock()
		gen := uint64(0)
		if rec, ok := m.records[recordID]; ok {
			gen = rec.cacheGen
		}
		m.mu.Unlock()
		m.expireCached(recordID, gen)
	}
}

// tryAcquire attempts a single lock acquisition.
// First tries an optimistic path using cached sequence hints, then falls back
// to reading the current record if the hint is stale.
func (m *Manager) tryAcquire(recordID uint64, lockType LockType, resourceID uint64, mode LockMode) (bool, error) {
	// Try optimistic path first if we have a hint
	if hint, ok := m.hintCache.Peek(recordID); ok {
		// Only use hint if the lock was free (Mode == None)
		// For held locks, we need to read current state to check conditions
		if hint.Mode == LockModeNone {
			acquired, newSeq, err := m.tryAcquireOptimistic(recordID, lockType, resourceID, mode, hint.Sequence)
			if err != nil {
				return false, err
			}
			if acquired {
				if ok, err := m.confirmClaim(recordID, mode, newSeq); err != nil || !ok {
					return false, err
				}
				// Update hint with new sequence
				m.hintCache.Put(recordID, &lockHint{
					Sequence: newSeq,
					Mode:     mode,
					UpdateAt: time.Now(),
				})
				atomic.AddUint64(&m.hintHits, 1)
				log.Printf("[DEBUG] lock.tryAcquire: type=%d res=%d - optimistic acquire SUCCESS", lockType, resourceID)
				return true, nil
			}
			// Optimistic failed - sequence was wrong, fall through to normal path
			atomic.AddUint64(&m.hintMisses, 1)
			log.Printf("[DEBUG] lock.tryAcquire: type=%d res=%d - optimistic acquire FAILED, falling back", lockType, resourceID)
		}
	}

	// Normal path: read current record
	record, err := m.readRecord(recordID)
	if err != nil {
		return false, err
	}

	now := time.Now()

	// A record held for a colliding resource is still held; taking it over lets two nodes hold it.
	var sharedCount uint32
	acquireTime := now.Unix()
	reason := ""
	switch {
	case record.Mode == LockModeNone:
		reason = "free"

	case m.isLockStale(record):
		// Owner presumed dead; its shared holders are gone too.
		reason = "stale"

	case mode == LockModeShared && record.Mode == LockModeShared:
		reason = "shared_add"
		sharedCount = record.SharedCount
		// Keep the first reader's time: a count leaked by a lost CAS update must still go stale.
		acquireTime = record.AcquireTime

	case record.Mode == LockModeExclusive && record.OwnerUUID == m.nodeUUID:
		// Leftover from a failed release or an earlier mount; this node holds no key on it.
		reason = "leftover_from_this_node"

	default:
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
		AcquireTime: acquireTime,
		Sequence:    record.Sequence + 1,
	}
	if mode == LockModeShared {
		newRecord.SharedCount = sharedCount + 1
	}

	// CAS the record
	acquired, err := m.casRecord(recordID, record, newRecord)
	if err != nil {
		return false, err
	}
	// A shared add only raises the count of a record that is already shared; a lost
	// race there can over-count readers, which delays writers but never admits two.
	if acquired && reason != "shared_add" {
		if acquired, err = m.confirmClaim(recordID, mode, newRecord.Sequence); err != nil {
			return false, err
		}
	}

	if acquired {
		// Update hint cache with new sequence
		m.hintCache.Put(recordID, &lockHint{
			Sequence: newRecord.Sequence,
			Mode:     mode,
			UpdateAt: time.Now(),
		})
	}

	return acquired, nil
}

// confirmClaim waits out the window in which a node that read the record before
// this node's claim landed can still overwrite it, then checks the claim survived.
func (m *Manager) confirmClaim(recordID uint64, mode LockMode, sequence uint64) (bool, error) {
	time.Sleep(claimSettleDelay)
	record, err := m.readRecord(recordID)
	if err != nil {
		return false, err
	}
	return record.Mode == mode && record.OwnerUUID == m.nodeUUID && record.Sequence == sequence, nil
}

// tryAcquireOptimistic attempts to acquire a lock using a cached sequence hint.
// This saves one disk read when the hint is correct (common case for uncontended locks).
// Returns: (acquired, newSequence, error)
func (m *Manager) tryAcquireOptimistic(recordID uint64, lockType LockType, resourceID uint64, mode LockMode, expectedSeq uint64) (bool, uint64, error) {
	now := time.Now()
	newSeq := expectedSeq + 1

	// Prepare new record assuming lock is free
	newRecord := &Record{
		Magic:       RecordMagic,
		Type:        lockType,
		Mode:        mode,
		ResourceID:  resourceID,
		OwnerUUID:   m.nodeUUID,
		OwnerSlot:   uint32(m.nodeSlot),
		AcquireTime: now.Unix(),
		Sequence:    newSeq,
	}
	if mode == LockModeShared {
		newRecord.SharedCount = 1
	}

	// Try direct CAS with expected sequence
	acquired, err := m.casRecordOptimistic(recordID, expectedSeq, newRecord)
	if err != nil {
		return false, 0, err
	}

	return acquired, newSeq, nil
}

// renewRecord rewrites AcquireTime on a record this node holds exclusively. It
// runs under the record gate. If another node took the record over as stale,
// the hold is marked lost and no new key may join it.
func (m *Manager) renewRecord(recordID uint64, rec *heldRecord) error {
	for {
		record, err := m.readRecord(recordID)
		if err != nil {
			return err
		}
		if record.Mode != LockModeExclusive || record.OwnerUUID != m.nodeUUID {
			log.Printf("[WARN] lock: record %d was taken over while this node held it", recordID)
			m.mu.Lock()
			rec.lost = true
			m.mu.Unlock()
			return ErrLockLost
		}
		renewed := *record
		renewed.AcquireTime = time.Now().Unix()
		renewed.Sequence++
		ok, err := m.casRecord(recordID, record, &renewed)
		if err != nil {
			return err
		}
		if ok {
			if ok, err = m.confirmClaim(recordID, LockModeExclusive, renewed.Sequence); err != nil {
				return err
			}
		}
		if ok {
			m.mu.Lock()
			rec.renewed = time.Now()
			m.mu.Unlock()
			return nil
		}
	}
}

// releaseRecord releases this node's hold on a lock record.
// After successful release, caches the new sequence number to enable
// optimistic acquisition on the next lock attempt.
func (m *Manager) releaseRecord(recordID uint64, mode LockMode) error {
	for {
		record, err := m.readRecord(recordID)
		if err != nil {
			return err
		}

		var newRecord *Record
		switch {
		case mode == LockModeExclusive && record.Mode == LockModeExclusive && record.OwnerUUID == m.nodeUUID,
			mode == LockModeShared && record.Mode == LockModeShared && record.SharedCount <= 1:
			newRecord = &Record{Sequence: record.Sequence + 1}
		case mode == LockModeShared && record.Mode == LockModeShared:
			// OwnerUUID names only the latest reader, so readers release by count.
			next := *record
			next.SharedCount--
			next.Sequence++
			newRecord = &next
		default:
			// Another node reclaimed the record after it went stale.
			return ErrNotLockOwner
		}

		success, err := m.casRecord(recordID, record, newRecord)
		if err != nil {
			return err
		}
		if success {
			m.hintCache.Put(recordID, &lockHint{
				Sequence: newRecord.Sequence,
				Mode:     newRecord.Mode,
				UpdateAt: time.Now(),
			})
			return nil
		}
	}
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
// Only checks against locks held by the CURRENT goroutine to avoid
// false positives when different goroutines hold different locks concurrently.
func (m *Manager) checkLockOrder(newType LockType) error {
	gid := getGoroutineID()
	for key, held := range m.heldLocks {
		// Only check order for locks held by THIS goroutine
		if held.goroutines != nil && held.goroutines[gid] > 0 {
			if key.Type > newType {
				return fmt.Errorf("%w: cannot acquire %v while holding %v",
					ErrDeadlockRisk, newType, key.Type)
			}
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

	// A read racing another node's write of the record can come back torn; read again.
	for attempt := 0; ; attempt++ {
		if _, err := m.dev.ReadAt(buf.Bytes(), offset); err != nil {
			return nil, err
		}
		record, err := deserializeRecord(buf.Bytes())
		if !errors.Is(err, ErrInvalidRecord) || attempt == 4 {
			return record, err
		}
		time.Sleep(time.Millisecond)
	}
}

// casRecordOptimistic performs an optimistic CAS using only the expected sequence.
// It writes the new record directly, then reads back to verify the sequence.
// Returns true if the write succeeded (sequence matched), false if stale.
// This saves one disk read compared to casRecord when the hint is correct.
func (m *Manager) casRecordOptimistic(recordID uint64, expectedSeq uint64, new *Record) (bool, error) {
	offset := int64(m.offset) + int64(recordID)*RecordSize

	buf := storage.GetBuffer(RecordSize)
	defer storage.PutBuffer(buf)

	// Read current record to check sequence
	if _, err := m.dev.ReadAt(buf.Bytes(), offset); err != nil {
		return false, err
	}

	// Check if sequence matches our expectation
	currentSeq := binary.LittleEndian.Uint64(buf.Bytes()[48:56])
	if currentSeq != expectedSeq {
		// Sequence doesn't match - hint was stale
		return false, nil
	}

	// Also verify the lock is actually free (Mode == None)
	// This prevents acquiring a lock that another node just took
	currentMode := LockMode(buf.Bytes()[5])
	if currentMode != LockModeNone {
		// Lock is held - can't acquire optimistically
		return false, nil
	}

	// Sequence matched and lock is free - write new record
	newData := serializeRecord(new)
	copy(buf.Bytes(), newData)
	if _, err := m.dev.WriteAt(buf.Bytes(), offset); err != nil {
		return false, err
	}

	return true, nil
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
