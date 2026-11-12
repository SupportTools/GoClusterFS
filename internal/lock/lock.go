// Package lock implements distributed locking for GoClusterFS.
//
// Locks are stored on the shared block device. Each lock record is a row of
// claim slots, one per registry slot, and a node writes only its own slot (see
// slots.go). This allows coordination between cluster nodes without any
// external services.
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
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"runtime"
	"slices"
	"strconv"
	"strings"
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
	// MaxLocks is the maximum number of concurrent locks.
	MaxLocks = 1 << 20 // 1M locks

	// DefaultTimeout is the default lock acquisition timeout.
	DefaultTimeout = 30 * time.Second

	// RetryBaseDelay is the initial retry delay.
	RetryBaseDelay = 10 * time.Millisecond

	// RetryMaxDelay is the maximum retry delay. It is short so a waiter polls at
	// least once while another node yields a record.
	RetryMaxDelay = 25 * time.Millisecond

	// CacheGrace is how long a node keeps a record after its last local holder
	// releases it, so the next local acquire needs no disk I/O. It is short
	// because an idle hold makes other nodes wait.
	CacheGrace = 10 * time.Millisecond

	// HoldTurn is how long a node keeps reusing a record it claimed from disk.
	// After that it releases and yields the record when its last holder lets go.
	HoldTurn = 250 * time.Millisecond

	// YieldGap is how long a node waits before claiming a record it yielded, so
	// another node polling for it gets a turn.
	YieldGap = 2 * RetryMaxDelay

	// StaleThreshold is how old a claim slot must be for other nodes to ignore it.
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

	// Several keys can hash to one record; the record stays held until the last key is released.
	records map[uint64]*heldRecord
	// gates serialize this node's on-disk operations per record.
	gates map[uint64]*sync.Mutex
	// yieldUntil holds records this node gave up and will not claim again before then.
	yieldUntil map[uint64]time.Time
	// claims tracks the Acquire calls waiting for a record this node does not hold.
	claims map[uint64]*pendingClaim
	// adopted maps each record AdoptOwner kept for a dead node to that node's slot.
	adopted map[uint64]int
	// slotWrite, when a test sets it, replaces the device write of a slot.
	slotWrite func(buf []byte, offset int64) (int, error)

	// Callback for cache invalidation
	onAcquire OnAcquireFunc

	waiting atomic.Int64

	slotWrites, rowReads, conflicts, waits, deferred atomic.Uint64
	// claimTimes is a histogram of the time from a claim's slot write to the end of its row read.
	claimTimes   [numTimeBuckets]atomic.Uint64
	maxClaimTime atomic.Int64
	acquires     [numLockTypes]acquireCounts
}

// Stats counts this node's lock activity. Claim slots need no hint cache, so
// the hint fields stay zero.
type Stats struct {
	Held          int // lock keys this node holds
	Waiting       int // Acquire calls whose first attempt found the lock taken
	HintCacheSize int
	HintHits      uint64
	HintMisses    uint64
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
	since   time.Time // when this node claimed the record
	renewed time.Time // when this node last wrote its slot
	// cacheGen identifies the current cached period; keys == 0 means cached.
	cacheGen uint64
}

// pendingClaim counts the local Acquire calls waiting for a record. When
// written is set, this node's slot holds a claim in mode that it has not won yet.
// behindLower means the last claim lost to a lower slot.
type pendingClaim struct {
	waiters     int
	mode        LockMode
	written     time.Time
	behindLower bool
}

func covers(held, want LockMode) bool {
	return held == LockModeExclusive || held == want
}

// renewAfter is the age of a slot after which this node writes it again before
// relying on it, so other nodes never see a slot in use as stale.
var renewAfter = StaleThreshold / 3

// acquireTimeout is how long Acquire waits at most; tests shorten it.
var acquireTimeout = DefaultTimeout

var (
	cacheGrace = CacheGrace
	holdTurn   = HoldTurn
	yieldGap   = YieldGap
)

// releaseParallelism is how many kept records ReleaseCached frees at once.
const releaseParallelism = 8

// age is the time since t. It takes the larger of the monotonic and wall-clock
// ages, so a pause that stops one clock still counts.
func age(t time.Time) time.Duration {
	return max(time.Since(t), time.Now().Round(0).Sub(t.Round(0)))
}

// Errors
var (
	ErrLockTimeout  = errors.New("lock acquisition timed out")
	ErrLockHeld     = errors.New("lock already held by another node")
	ErrNotLockOwner = errors.New("not the lock owner")
	ErrDeadlockRisk = errors.New("lock order violation would cause deadlock")
)

// NewManager creates a new lock manager.
func NewManager(dev *storage.Device, offset, size uint64, nodeUUID [16]byte, nodeSlot int) *Manager {
	return &Manager{
		dev:        dev,
		offset:     offset,
		size:       size,
		nodeUUID:   nodeUUID,
		nodeSlot:   nodeSlot,
		heldLocks:  make(map[lockKey]*heldLock),
		records:    make(map[uint64]*heldRecord),
		gates:      make(map[uint64]*sync.Mutex),
		yieldUntil: make(map[uint64]time.Time),
		claims:     make(map[uint64]*pendingClaim),
		adopted:    make(map[uint64]int),
	}
}

// SetOnAcquire registers a callback that is invoked when a lock is acquired.
// This is typically used by the cache manager to invalidate cached data
// when acquiring locks, ensuring fresh data is read from disk.
//
// Only exclusive lock acquisitions trigger invalidation since shared locks
// don't modify data.
func (m *Manager) SetOnAcquire(fn OnAcquireFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onAcquire = fn
}

// Stats returns this node's lock counts.
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	held := len(m.heldLocks)
	m.mu.Unlock()
	return Stats{Held: held, Waiting: int(m.waiting.Load())}
}

const numLockTypes = int(LockTypeExtent) + 1

// acquireCounts says how one lock type's acquires on this node were served:
// a key this node already held, a join of a held record, a cached record, a
// claim won on disk, and tries that had to wait.
type acquireCounts struct {
	held, joined, cached, claimed, blocked atomic.Uint64
}

func (m *Manager) countAcquire(lockType LockType, pick func(*acquireCounts) *atomic.Uint64) {
	if int(lockType) < numLockTypes {
		pick(&m.acquires[lockType]).Add(1)
	}
}

// Claim time histogram buckets grow by 20% from 10µs; the last one holds the rest.
const (
	numTimeBuckets = 71
	firstTimeBound = 10 * time.Microsecond
	timeGrowth     = 1.2
)

func timeBound(i int) time.Duration {
	return time.Duration(float64(firstTimeBound) * math.Pow(timeGrowth, float64(i)))
}

func (m *Manager) recordClaimTime(d time.Duration) {
	i := 0
	if d > firstTimeBound {
		i = min(int(math.Ceil(math.Log(float64(d)/float64(firstTimeBound))/math.Log(timeGrowth))), numTimeBuckets-1)
	}
	m.claimTimes[i].Add(1)
	for {
		cur := m.maxClaimTime.Load()
		if int64(d) <= cur || m.maxClaimTime.CompareAndSwap(cur, int64(d)) {
			return
		}
	}
}

// LogStats logs how this node's acquires were served, how often claims
// collided, and how long claims took.
func (m *Manager) LogStats() {
	log.Printf("lock: %d slot writes, %d row reads; %d claims cleared after a collision, %d kept to wait, %d tries waited for a lower slot",
		m.slotWrites.Load(), m.rowReads.Load(), m.conflicts.Load(), m.waits.Load(), m.deferred.Load())
	for typ := range m.acquires {
		c := &m.acquires[typ]
		counts := []uint64{c.held.Load(), c.joined.Load(), c.cached.Load(), c.claimed.Load(), c.blocked.Load()}
		if slices.Max(counts) > 0 {
			log.Printf("lock: type %d acquires: held=%d joined=%d cached=%d claimed=%d blocked=%d",
				typ, counts[0], counts[1], counts[2], counts[3], counts[4])
		}
	}
	var times strings.Builder
	for i := range m.claimTimes {
		if n := m.claimTimes[i].Load(); n > 0 {
			fmt.Fprintf(&times, " %d:%d", timeBound(i).Microseconds(), n)
		}
	}
	log.Printf("lock: claim times (us upper bound:count):%s max=%dus", times.String(), time.Duration(m.maxClaimTime.Load()).Microseconds())
}

// Acquire attempts to acquire a lock with the given parameters.
// Blocks until the lock is acquired, the context ends, or DefaultTimeout passes.
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
		m.countAcquire(lockType, func(c *acquireCounts) *atomic.Uint64 { return &c.held })
		return nil
	}
	if err := m.checkLockOrder(lockType); err != nil {
		m.mu.Unlock()
		return err
	}
	m.addClaimWaiterLocked(recordID)
	m.mu.Unlock()
	defer m.dropClaimWaiter(recordID)

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
		m.countAcquire(lockType, func(c *acquireCounts) *atomic.Uint64 { return &c.blocked })
		if retryCount == 1 {
			m.waiting.Add(1)
			defer m.waiting.Add(-1)
		}

		// Check context cancellation, but allow at least 3 retries. A caller whose
		// context never ends still gives up after acquireTimeout.
		if retryCount >= 3 && (ctx.Err() != nil || time.Since(startTime) > acquireTimeout) {
			log.Printf("[WARN] lock: gave up waiting for type=%d res=%d mode=%d after %v (%d tries); record %d %s",
				lockType, resourceID, mode, time.Since(startTime).Round(time.Millisecond), retryCount, recordID, m.describeHolder(recordID))
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
	if rec, ok := m.records[recordID]; ok && rec.keys == 0 && (!covers(rec.mode, mode) || age(rec.renewed) > renewAfter) {
		// A cached hold too weak for this request, or old enough that another node
		// may soon ignore it as stale, is claimed afresh over this node's slot.
		delete(m.records, recordID)
	}
	if rec, ok := m.records[recordID]; ok {
		if !covers(rec.mode, mode) {
			selfDeadlock := m.goroutineHoldsRecordLocked(recordID, gid)
			m.mu.Unlock()
			if selfDeadlock {
				return false, false, fmt.Errorf("%w: cannot upgrade a shared lock to exclusive", ErrDeadlockRisk)
			}
			return false, false, nil
		}
		renew := age(rec.renewed) > renewAfter
		m.mu.Unlock()
		if renew {
			if err := m.renewRecord(recordID, key, rec); err != nil {
				return false, false, err
			}
		}
		m.mu.Lock()
		if held, ok := m.heldLocks[key]; ok {
			held.refCount++
			held.goroutines[gid]++
			m.mu.Unlock()
			m.countAcquire(key.Type, func(c *acquireCounts) *atomic.Uint64 { return &c.held })
			return true, false, nil
		}
		if rec.keys == 0 {
			m.countAcquire(key.Type, func(c *acquireCounts) *atomic.Uint64 { return &c.cached })
		} else {
			m.countAcquire(key.Type, func(c *acquireCounts) *atomic.Uint64 { return &c.joined })
		}
		rec.keys++
		m.addHeldKeyLocked(key, recordID, mode, gid)
		m.mu.Unlock()
		return true, true, nil
	}
	if until, ok := m.yieldUntil[recordID]; ok {
		if time.Now().Before(until) {
			m.mu.Unlock()
			return false, false, nil
		}
		delete(m.yieldUntil, recordID)
	}
	m.mu.Unlock()

	written, won, err := m.claim(recordID, key, mode)
	if err != nil || !won {
		return false, false, err
	}
	m.countAcquire(key.Type, func(c *acquireCounts) *atomic.Uint64 { return &c.claimed })

	m.mu.Lock()
	m.records[recordID] = &heldRecord{mode: mode, keys: 1, since: time.Now(), renewed: written}
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
	if !covers(rec.mode, mode) || age(rec.renewed) > renewAfter {
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
	if age(rec.since) > holdTurn {
		// Held long enough: give other nodes a turn instead of keeping it.
		delete(m.records, recordID)
		m.yieldUntil[recordID] = time.Now().Add(yieldGap)
		m.mu.Unlock()
		return m.releaseRecord(recordID)
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

	if err := m.releaseRecord(recordID); err != nil {
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

	// Records release in parallel: each is a device write, and an unmount may keep many.
	var wg sync.WaitGroup
	slots := make(chan struct{}, releaseParallelism)
	for _, recordID := range cached {
		m.mu.Lock()
		gen := uint64(0)
		if rec, ok := m.records[recordID]; ok {
			gen = rec.cacheGen
		}
		m.mu.Unlock()
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			m.expireCached(recordID, gen)
		})
	}
	wg.Wait()
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

	return hash % (m.size / RowSize)
}
