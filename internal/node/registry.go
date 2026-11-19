// Package node implements cluster node registration and heartbeat management.
//
// In a shared-storage cluster, nodes coordinate through on-disk structures
// rather than network protocols. Each node claims a slot in the registry
// using CAS (compare-and-swap), then maintains periodic heartbeats.
// Failed nodes are detected when their heartbeat becomes stale.
package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"math/bits"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// MaxNodes is the maximum number of nodes in a cluster.
	MaxNodes = 64

	// SlotSize is the size of each node slot in bytes.
	// Must match SectorSize (4096) for O_DIRECT alignment.
	// Each slot uses one full sector to ensure atomic writes.
	SlotSize = 4096

	// HeartbeatInterval is how often nodes update their heartbeat.
	HeartbeatInterval = 1 * time.Second

	// HeartbeatTimeout is how long before a node is considered dead.
	HeartbeatTimeout = 10 * time.Second

	// Magic identifies a valid node slot.
	SlotMagic uint32 = 0x4E4F4445 // "NODE"

	// claimSettle bounds how late a racing node's write to the same slot can land.
	claimSettle = 100 * time.Millisecond
)

// NodeState represents the state of a node slot.
type NodeState uint32

const (
	NodeStateEmpty   NodeState = 0 // Slot is free
	NodeStateActive  NodeState = 1 // Node is active
	NodeStateLeaving NodeState = 2 // Node is gracefully leaving
	NodeStateDead    NodeState = 3 // Node failed (detected by others)
)

func (s NodeState) String() string {
	switch s {
	case NodeStateEmpty:
		return "empty"
	case NodeStateActive:
		return "active"
	case NodeStateLeaving:
		return "leaving"
	case NodeStateDead:
		return "dead"
	}
	return fmt.Sprintf("unknown(%d)", uint32(s))
}

// Slot represents a node's on-disk registration entry.
// Size: 4096 bytes (sector-aligned for O_DIRECT atomic writes)
type Slot struct {
	Magic         uint32    // 0x4E4F4445 "NODE"
	State         NodeState // Current state
	UUID          [16]byte  // Node's unique identifier
	HeartbeatSeq  uint64    // Monotonic heartbeat counter
	HeartbeatTime int64     // Unix timestamp of last heartbeat
	MountTime     int64     // When this node mounted
	JournalOffset uint64    // This node's journal region offset
	JournalSize   uint64    // This node's journal size
	Hostname      [64]byte  // Node hostname
	BootID        [16]byte  // Kernel boot ID of the node's host
	PID           uint32    // Mount process ID
	ProcStart     int64     // Mount process start time (Unix ns)
	RecoveryOwner [16]byte  // Dead slots: node replaying the journal
	RecoveryStart int64     // Dead slots: when that replay last renewed its lease (Unix s)
	PIDNamespace  uint64    // Inode of the mount process's PID namespace
	Orphans       []Orphan  // Files this node unlinked that some node may still have open
	LockLayout    uint32    // How the node's build maps locks to lock records; 0 before builds recorded it
	Checksum      uint32    // CRC32C of bytes 0-4091
}

// Orphan names an unlinked file by its inode and the low bits of its birth generation, so a reused slot never matches.
type Orphan struct {
	Inode uint32
	Birth uint32
}

const MaxOrphans = (lockLayoutOffset - orphansOffset - 4) / 8

const (
	orphansOffset    = 188
	lockLayoutOffset = SlotSize - 8
)

// Registry manages node registration and heartbeats.
type Registry struct {
	mu     sync.RWMutex
	dev    *storage.Device
	offset uint64 // Registry region offset on device

	localUUID   [16]byte
	localSlot   int // -1 if not registered
	preferred   int // slot Register tries first, or -1
	stopCh      chan struct{}
	heartbeatWg sync.WaitGroup

	journalOffset   uint64
	journalSlotSize uint64

	hostname   string
	bootID     [16]byte
	pidNS      uint64
	lockLayout uint32
	onSlotLost atomic.Pointer[func(SlotLoss)]

	onOwnerDead func(owner [16]byte, slot int) (done func(replayed bool))
	onOrphans   func(ctx context.Context, slot int, orphans []Orphan)
	// preempt revokes a dead node's access to the device before this node acts on its death; nil when unfenced.
	preempt       func(owner [16]byte) error
	orphanKick    chan struct{}
	previousOwner *[16]byte
	sightings     map[int]sighting

	// seen holds the last read of each slot, for Occupant.
	seenMu sync.Mutex
	seen   map[int]*Slot

	// lastBeat is the bootClock reading when this node issued its last good heartbeat, 0 before any; beatSeq is its last heartbeat number.
	lastBeat atomic.Int64
	beatSeq  uint64
	// lost is set once this node's slot was taken over or its heartbeats failed.
	lost  atomic.Bool
	ownMu sync.Mutex

	// Cross-node recovery
	recoveryHandler      RecoveryHandler
	failureCheckInterval time.Duration // How often to scan for failed nodes
	recoveryInProgress   map[int]bool  // Slots currently being recovered
	replaying            atomic.Uint64 // Bit i: this node is replaying slot i's journal
	replays              atomic.Uint64
	replayFailures       atomic.Uint64
	failures             map[int]*replayFailure // the last run of identical replay failures per slot
}

// emptySlot is a slot no node ever wrote; it has no checksum.
var emptySlot = make([]byte, SlotSize)

// Errors
var (
	ErrNoFreeSlots        = errors.New("no free node slots available")
	ErrSlotTaken          = errors.New("node slot was taken by another node")
	ErrNotRegistered      = errors.New("node is not registered")
	ErrInvalidSlot        = errors.New("invalid slot data")
	ErrRecoveryInProgress = errors.New("recovery already in progress for this node")
	ErrLeaseLost          = errors.New("another node took over this slot's recovery")
	ErrSlotLost           = errors.New("this node no longer owns its registry slot")
	ErrOrphansFull        = errors.New("this node's registry slot has no room for another orphan")
	ErrLockLayout         = errors.New("a live node maps locks to lock records differently from this build")
	// ErrReplayContended marks a replay that failed waiting on other nodes, such as for a lock; it never counts toward a backoff.
	ErrReplayContended = errors.New("the replay waited too long on other nodes")
)

// FailedNodeInfo contains information about a failed node for recovery.
type FailedNodeInfo struct {
	SlotIndex     int
	UUID          [16]byte
	JournalOffset uint64
	JournalSize   uint64
	LastHeartbeat time.Time
	Hostname      string
	Orphans       []Orphan // files the dead node unlinked that may still be held open

	// CheckLease fails once this node no longer holds the slot's recovery
	// lease, and renews the lease while it does. Replay calls it before each
	// transaction and before resetting the journal.
	CheckLease func() error
}

// RecoveryHandler is called when a failed node is detected.
// The handler should replay the failed node's journal.
// Return nil on success, or an error to retry later.
type RecoveryHandler func(info *FailedNodeInfo) error

// FailureCheckInterval is how often to scan for failed nodes.
const FailureCheckInterval = 5 * time.Second

// NewRegistry creates a new registry manager.
func NewRegistry(dev *storage.Device, registryOffset uint64) *Registry {
	r := &Registry{
		dev:                  dev,
		offset:               registryOffset,
		localSlot:            -1,
		preferred:            -1,
		stopCh:               make(chan struct{}),
		orphanKick:           make(chan struct{}, 1),
		failureCheckInterval: FailureCheckInterval,
		recoveryInProgress:   make(map[int]bool),
		failures:             make(map[int]*replayFailure),
		sightings:            make(map[int]sighting),
		seen:                 make(map[int]*Slot),
		bootID:               readBootID(),
		pidNS:                readPIDNamespace(),
	}

	// Generate a unique UUID for this node
	rand.Read(r.localUUID[:])

	return r
}

// SetPreempter makes this node revoke a node's access to the device, through preempt, before it marks that node's
// slot dead, releases it or mounts into it. Call it before Register.
func (r *Registry) SetPreempter(preempt func(owner [16]byte) error) {
	r.preempt = preempt
}

func (r *Registry) fenceOut(owner [16]byte) error {
	if r.preempt == nil || owner == ([16]byte{}) {
		return nil
	}
	if err := r.preempt(owner); err != nil {
		return fmt.Errorf("revoking node %x's access to the device: %w", owner[:8], err)
	}
	log.Printf("[Recovery] Revoked node %x's access to the device", owner[:8])
	return nil
}

// SetRecoveryHandler sets the callback for failed node recovery.
// This should be called before StartHeartbeat.
func (r *Registry) SetRecoveryHandler(handler RecoveryHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recoveryHandler = handler
}

// SlotLoss is why this node must stop writing: its slot no longer names it as alive, or its heartbeats stopped landing.
type SlotLoss struct {
	Slot int
	// Taken is set when the slot no longer names this node as alive; Owner is then the UUID the slot names.
	Taken bool
	Owner [16]byte
	// Local is this node's UUID, and Stalled how long its heartbeats had failed.
	Local   [16]byte
	Stalled time.Duration
	// Paused is set when this node went so long without a heartbeat that a peer may have replayed it.
	Paused bool
	// Fenced is the device's refusal of a write after another node revoked this node's access.
	Fenced error
}

// String names the loss for a log line, with "owner" only when another node holds the slot.
func (l SlotLoss) String() string {
	switch {
	case l.Fenced != nil:
		return fmt.Sprintf("The device refused a write from this node (%v): another node declared it dead and revoked its access", l.Fenced)
	case l.Paused:
		return fmt.Sprintf("Node registry slot %d may no longer be this node's: it went %v without a heartbeat, so a peer may have replayed it",
			l.Slot, l.Stalled.Round(time.Millisecond))
	case !l.Taken:
		return fmt.Sprintf("Heartbeats to registry slot %d failed for %v", l.Slot, l.Stalled.Round(time.Millisecond))
	case l.Owner == l.Local:
		return fmt.Sprintf("Node registry slot %d is no longer this node's: a peer declared this node dead", l.Slot)
	case l.Owner == [16]byte{}:
		return fmt.Sprintf("Node registry slot %d is no longer this node's: the slot is empty", l.Slot)
	}
	return fmt.Sprintf("Node registry slot %d is no longer this node's (owner %x)", l.Slot, l.Owner[:8])
}

// SetSlotLostHandler sets what the heartbeat does when this node loses its slot. Without a handler it only logs.
func (r *Registry) SetSlotLostHandler(fn func(SlotLoss)) {
	r.onSlotLost.Store(&fn)
}

// SetOwnerDeadHandler sets what failure detection calls, before replaying a
// proven-dead node's journal, with that node's UUID and registry slot; for
// example to take over its locks. The returned function runs after the replay
// and is told whether it succeeded: after a failed replay, the dead node's locks
// must stay held until a later replay succeeds.
func (r *Registry) SetOwnerDeadHandler(fn func(owner [16]byte, slot int) (done func(replayed bool))) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onOwnerDead = fn
}

// PreviousOwner returns the UUID of the node whose slot Register took, if the
// slot was not empty. Its journal is this node's to replay at mount, and its
// lock claims are this node's to keep until then.
func (r *Registry) PreviousOwner() ([16]byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.previousOwner == nil {
		return [16]byte{}, false
	}
	return *r.previousOwner, true
}

func (r *Registry) ownerDead(owner [16]byte, slot int) func(replayed bool) {
	r.mu.Lock()
	fn := r.onOwnerDead
	r.mu.Unlock()
	if fn == nil {
		return func(bool) {}
	}
	return fn(owner, slot)
}

// SetJournalArea makes Register record a journal region in the claimed slot:
// slot i owns slotSize bytes at offset + i*slotSize.
func (r *Registry) SetJournalArea(offset, slotSize uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.journalOffset = offset
	r.journalSlotSize = slotSize
}

// LocalJournal returns the journal region of this node's slot.
func (r *Registry) LocalJournal() (offset, size uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.localSlot < 0 {
		return 0, 0
	}
	return r.journalRegion(r.localSlot)
}

func (r *Registry) journalRegion(slotIdx int) (offset, size uint64) {
	return r.journalOffset + uint64(slotIdx)*r.journalSlotSize, r.journalSlotSize
}

// Register claims a slot in the node registry and returns its index. It
// prefers a slot of this host, most likely this node's previous incarnation,
// so the caller replays that journal before serving. It takes such a slot at
// once if its mount process has exited, and otherwise only after the slot
// stays unchanged for silentSlotWatch. Next it takes an empty slot, and last
// another host's slot that stays unchanged for silentSlotWatch.
func (r *Registry) Register(ctx context.Context, hostname string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.localSlot >= 0 {
		return r.localSlot, nil // Already registered
	}

	var name [64]byte
	copy(name[:], hostname)
	r.hostname = string(bytes.TrimRight(name[:], "\x00"))
	if err := r.checkLockLayouts(); err != nil {
		return -1, err
	}

	empty := func(slot *Slot) bool { return slot.State == NodeStateEmpty }
	// The slot -skip-recovery names comes first: the operator chose it.
	if r.preferred >= 0 {
		if i, err := r.claimFirst([]int{r.preferred}, func(slot *Slot) bool { return empty(slot) || r.deadLocalOwner(slot) }); i >= 0 || err != nil {
			return i, err
		}
		// A slot under another node's recovery is taken only once its lease has lapsed unrenewed.
		if _, slot, err := r.readSlotRaw(r.preferred); err == nil && slot.State != NodeStateEmpty {
			watch := silentSlotWatch
			if slot.State == NodeStateDead {
				watch = recoveryLease
			}
			if i, err := r.claimWatched(r.awaitSilentSlots([]int{r.preferred}, watch)); i >= 0 || err != nil {
				return i, err
			}
		}
	}
	if i, err := r.claimFirst(allSlots(), r.deadLocalOwner); i >= 0 || err != nil {
		return i, err
	}
	// A slot under this hostname is likely this host's mount before a crash, a container restart or a reboot; taking
	// it back replays its journal before this mount serves. Its mount may still run, or another host may share the
	// name, so it is watched first.
	if i, err := r.claimWatched(r.awaitSilentSlots(r.ownHostSlots(), silentSlotWatch)); i >= 0 || err != nil {
		return i, err
	}
	if i, err := r.claimFirst(allSlots(), empty); i >= 0 || err != nil {
		return i, err
	}
	// Another host's heartbeat that looks old by this clock may be a live node's whose clock is behind, so its slot is
	// taken only once the heartbeat has not changed for silentSlotWatch by this node's clock.
	i, err := r.claimWatched(r.awaitSilentSlots(r.otherHostSlots(), silentSlotWatch))
	if i < 0 && err == nil {
		err = ErrNoFreeSlots
	}
	return i, err
}

// SetLockLayout sets the lock record layout Register writes in this node's
// slot. Register refuses to join while a live node's slot holds another one.
func (r *Registry) SetLockLayout(id uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lockLayout = id
}

// checkLockLayouts returns ErrLockLayout naming a node whose slot holds another
// lock layout, unless its heartbeat stays unchanged for silentSlotWatch: two
// such nodes would not exclude each other. Called with r.mu held.
func (r *Registry) checkLockLayouts() error {
	slots, err := r.readAllSlots()
	if err != nil {
		return err
	}
	differs := func(s *Slot) bool {
		return s != nil && (s.State == NodeStateActive || s.State == NodeStateLeaving) && s.LockLayout != r.lockLayout &&
			(!r.sameHost(s) || !r.deadLocalOwner(s))
	}
	var other []int
	for i, s := range slots {
		if differs(s) {
			other = append(other, i)
		}
	}
	if len(other) == 0 {
		return nil
	}
	silent := map[int]bool{}
	for _, w := range r.awaitSilentSlots(other, silentSlotWatch) {
		silent[w.slot] = true
	}
	for _, i := range other {
		if _, s, err := r.readSlotRaw(i); err == nil && !silent[i] && differs(s) {
			return fmt.Errorf("%w: node %s in slot %d (%x) has lock layout %08x, this build %08x",
				ErrLockLayout, s.Host(), i, s.UUID[:8], s.LockLayout, r.lockLayout)
		}
	}
	return nil
}

// PreferSlot makes Register try slot first: it takes it if it is empty, or if
// what it watches in the slot stays unchanged: a node's heartbeat for
// silentSlotWatch, or a recovery lease for recoveryLease. Mount passes the slot
// -skip-recovery names, so the mount can take a crashed node's slot and discard
// its journal.
func (r *Registry) PreferSlot(slot int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.preferred = slot
}

// silentSlotWatch is how long Register watches a slot it cannot prove dead before it
// takes it, if its heartbeat never changed: as long as peers watch before they
// replay, so a node that stalled for less than stallLimit keeps its slot. Tests shorten it.
var silentSlotWatch = deadAfter

// ownHostSlots returns the slots under this node's hostname whose mount it
// cannot prove exited: one in another PID namespace on this boot, which a
// restarted container left, or one whose heartbeat looks old by this node's clock.
func (r *Registry) ownHostSlots() []int {
	var slots []int
	for i := 0; i < MaxNodes; i++ {
		_, slot, err := r.readSlotRaw(i)
		if err != nil || (slot.State != NodeStateActive && slot.State != NodeStateLeaving) || slot.Host() != r.hostname {
			continue
		}
		if r.sameHost(slot) && slot.PIDNamespace != r.pidNS || heartbeatOlderThan(slot, HeartbeatTimeout) {
			slots = append(slots, i)
		}
	}
	return slots
}

// otherHostSlots returns the slots of other hosts' nodes whose heartbeat looks old by this node's clock.
func (r *Registry) otherHostSlots() []int {
	var slots []int
	for i := 0; i < MaxNodes; i++ {
		_, slot, err := r.readSlotRaw(i)
		if err == nil && (slot.State == NodeStateActive || slot.State == NodeStateLeaving) && !r.sameHost(slot) &&
			heartbeatOlderThan(slot, HeartbeatTimeout) {
			slots = append(slots, i)
		}
	}
	return slots
}

// watchedSlot is a slot and the bytes awaitSilentSlots read in it throughout its watch.
type watchedSlot struct {
	slot int
	raw  []byte
}

// afterSilentWatch, when a test sets it, runs after awaitSilentSlots ends its watch.
var afterSilentWatch func()

// claimWatched claims the first slot that still holds the bytes watched in it,
// so a slot a peer marked dead, or whose node beat again, since is left alone.
func (r *Registry) claimWatched(slots []watchedSlot) (int, error) {
	for _, w := range slots {
		issued := bootClock()
		if err := r.claimSlot(w.slot, w.raw); err != nil {
			if errors.Is(err, ErrSlotTaken) {
				continue
			}
			return -1, err
		}
		slot, err := deserializeSlot(w.raw)
		if err != nil {
			return -1, err
		}
		r.took(w.slot, slot, issued)
		return w.slot, nil
	}
	return -1, nil
}

// awaitSilentSlots reads slots until each has changed or for d, and returns the
// ones that never changed with the bytes they held. It times the wait on this node's clock.
func (r *Registry) awaitSilentSlots(slots []int, d time.Duration) []watchedSlot {
	first := map[int][]byte{}
	for _, i := range slots {
		if raw, _, err := r.readSlotRaw(i); err == nil {
			first[i] = raw
		}
	}
	deadline := time.Now().Add(d)
	for len(first) > 0 && time.Now().Before(deadline) {
		time.Sleep(HeartbeatInterval / 4)
		for i, was := range first {
			if raw, _, err := r.readSlotRaw(i); err != nil || !bytes.Equal(raw, was) {
				delete(first, i)
			}
		}
	}
	if afterSilentWatch != nil {
		afterSilentWatch()
	}
	var silent []watchedSlot
	for _, i := range slots {
		if raw, ok := first[i]; ok {
			silent = append(silent, watchedSlot{i, raw})
		}
	}
	return silent
}

func allSlots() []int {
	slots := make([]int, MaxNodes)
	for i := range slots {
		slots[i] = i
	}
	return slots
}

// claimFirst claims the first of slots that claimable accepts. It returns -1 if none could be claimed.
func (r *Registry) claimFirst(slots []int, claimable func(*Slot) bool) (int, error) {
	for _, i := range slots {
		raw, slot, err := r.readSlotRaw(i)
		if err != nil || !claimable(slot) {
			continue
		}
		issued := bootClock()
		if err := r.claimSlot(i, raw); err != nil {
			if errors.Is(err, ErrSlotTaken) {
				continue
			}
			return -1, err
		}
		r.took(i, slot, issued)
		return i, nil
	}
	return -1, nil
}

// took records that this node registered in slot i, which held slot, with a
// claim write issued at bootClock time issued. Called with r.mu held.
func (r *Registry) took(i int, slot *Slot, issued time.Duration) {
	r.localSlot = i
	r.beatSeq = 1
	r.lastBeat.Store(int64(issued))
	if slot.State != NodeStateEmpty {
		r.previousOwner = &slot.UUID
	}
}

// Unregister gracefully releases the node's slot.
func (r *Registry) Unregister(ctx context.Context) error {
	r.mu.Lock()

	if r.localSlot < 0 {
		r.mu.Unlock()
		return ErrNotRegistered
	}

	// Save slot number for later
	slotNum := r.localSlot

	r.closeStop()

	// IMPORTANT: Release lock before waiting on WaitGroup to avoid deadlock.
	// Background goroutines (failureDetectionLoop, heartbeatLoop) may be holding
	// or waiting for the lock when they receive the stop signal.
	r.mu.Unlock()

	// Wait for background goroutines to finish
	r.heartbeatWg.Wait()

	// Re-acquire lock for final cleanup
	r.mu.Lock()
	defer r.mu.Unlock()

	slot, err := r.readSlot(slotNum)
	if err != nil {
		return err
	}
	if slot.UUID == r.localUUID {
		if err := r.writeSlot(slotNum, &Slot{Magic: SlotMagic, State: NodeStateEmpty, Orphans: slot.Orphans}); err != nil {
			return err
		}
	}

	r.localSlot = -1
	r.stopCh = make(chan struct{})
	return nil
}

// Stop ends the heartbeat and failure detection. It waits for a journal
// replay in progress, so call it before closing the device.
func (r *Registry) Stop() {
	r.mu.Lock()
	r.closeStop()
	r.mu.Unlock()
	r.heartbeatWg.Wait()
}

func (r *Registry) closeStop() {
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
}

// StartHeartbeat begins the background heartbeat goroutine.
func (r *Registry) StartHeartbeat() error {
	r.mu.RLock()
	if r.localSlot < 0 {
		r.mu.RUnlock()
		return ErrNotRegistered
	}
	slotIdx := r.localSlot
	r.mu.RUnlock()

	r.heartbeatWg.Add(1)
	go r.heartbeatLoop(slotIdx)
	return nil
}

// testHeartbeatHold, from GOCLUSTERFS_TEST_HEARTBEAT_HOLD and for tests only,
// is how long the heartbeat waits once it finds it missed beats, as after a
// pause, so a write that was stalled with it reaches the write gate first.
var testHeartbeatHold, _ = time.ParseDuration(os.Getenv("GOCLUSTERFS_TEST_HEARTBEAT_HOLD"))

// heartbeatLoop periodically updates the node's heartbeat.
func (r *Registry) heartbeatLoop(slotIdx int) {
	defer r.heartbeatWg.Done()

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	if testHeartbeatHold > 0 {
		log.Printf("[WARN] node: GOCLUSTERFS_TEST_HEARTBEAT_HOLD is set: after a pause the heartbeat waits %v; use it only in tests", testHeartbeatHold)
	}

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			if testHeartbeatHold > 0 && r.sinceLastBeat() > HeartbeatTimeout/2 {
				time.Sleep(testHeartbeatHold)
			}
			_ = r.beat(slotIdx)
		}
	}
}

// afterBeatRead lets tests pause a heartbeat between its slot read and its write, as a stall would.
var afterBeatRead func()

// beat writes a heartbeat while slotIdx is still this node's. Peers prove a
// silent node dead and take its locks, so a node whose slot was taken over, or
// whose heartbeats have failed for HeartbeatTimeout/2, must stop: beat tells the
// slot-lost handler once and returns ErrSlotLost. So must a node that went
// stallLimit without a beat, before its write or by the time the write
// completed: a peer may have replayed it and emptied or reused the slot since
// the read, and writing the stale copy back would make two owners.
func (r *Registry) beat(slotIdx int) error {
	r.mu.Lock()
	slot, err := r.readSlot(slotIdx)
	if afterBeatRead != nil {
		afterBeatRead()
	}
	ours := err == nil && slot.UUID == r.localUUID && slot.State != NodeStateDead
	takenOver := err == nil && !ours
	paused := r.sinceLastBeat()
	if ours && paused <= stallLimit {
		r.beatSeq++
		slot.HeartbeatSeq = r.beatSeq
		slot.HeartbeatTime = time.Now().Unix()
		issued := bootClock()
		if r.writeSlot(slotIdx, slot) == nil {
			if paused = r.sinceLastBeat(); paused <= stallLimit {
				r.lastBeat.Store(int64(issued))
			}
		}
	}
	r.mu.Unlock()

	sinceBeat := r.sinceLastBeat()
	var loss SlotLoss
	switch {
	case takenOver:
		loss = SlotLoss{Slot: slotIdx, Taken: true, Owner: slot.UUID, Local: r.localUUID, Stalled: sinceBeat}
	case paused > stallLimit:
		loss = SlotLoss{Slot: slotIdx, Paused: true, Local: r.localUUID, Stalled: paused}
	case sinceBeat > HeartbeatTimeout/2:
		loss = SlotLoss{Slot: slotIdx, Local: r.localUUID, Stalled: sinceBeat}
	default:
		return nil
	}
	r.loseSlot(loss)
	return ErrSlotLost
}

// Fenced stops this node through the slot-lost handler once the device refused one of its writes. The writer may hold
// r.mu, as the heartbeat does, so this takes no registry lock.
func (r *Registry) Fenced(err error) {
	r.loseSlot(SlotLoss{Slot: -1, Fenced: err, Local: r.localUUID})
}

// loseSlot tells the slot-lost handler, once, that this node lost its slot.
func (r *Registry) loseSlot(loss SlotLoss) {
	if !r.lost.CompareAndSwap(false, true) {
		return
	}
	if onSlotLost := r.onSlotLost.Load(); onSlotLost != nil {
		(*onSlotLost)(loss)
	} else {
		log.Printf("ERROR: %s; this node no longer owns its journal region", loss)
	}
}

// CheckOwnership returns nil while this node still owns its registry slot and
// ErrSlotLost once it may not, so a node that stalled past its failover stops
// writing. A heartbeat written within HeartbeatTimeout/2 proves ownership
// without I/O, because peers declare a node dead only after 2*HeartbeatTimeout
// of silence. Up to stallLimit it writes a heartbeat first, as the
// heartbeat loop would. After longer, no read of the slot proves anything: a
// stalled heartbeat may already have written it back as this node's.
func (r *Registry) CheckOwnership() error {
	if r.lost.Load() {
		return ErrSlotLost
	}
	if r.sinceLastBeat() < HeartbeatTimeout/2 {
		return nil
	}
	r.ownMu.Lock()
	defer r.ownMu.Unlock()
	if r.sinceLastBeat() < HeartbeatTimeout/2 {
		return nil
	}
	r.mu.RLock()
	slotIdx := r.localSlot
	r.mu.RUnlock()
	if slotIdx < 0 {
		return ErrNotRegistered
	}
	if paused := r.sinceLastBeat(); paused > stallLimit {
		log.Printf("[WARN] node: the last heartbeat to registry slot %d was %v ago; refusing writes without trusting the slot",
			slotIdx, paused.Round(time.Millisecond))
		r.loseSlot(SlotLoss{Slot: slotIdx, Paused: true, Local: r.localUUID, Stalled: paused})
		return ErrSlotLost
	}
	log.Printf("[WARN] node: the last heartbeat to registry slot %d was %v ago; checking the slot before a write",
		slotIdx, r.sinceLastBeat().Round(time.Millisecond))
	return r.beat(slotIdx)
}

// WriteGate is a device write gate: it passes writes to the registry, which
// the heartbeat itself must make, and checks ownership before any other write.
func (r *Registry) WriteGate(offset int64) error {
	if r.inRegistry(offset) {
		return nil
	}
	return r.CheckOwnership()
}

// WriteFresh reports, without I/O, whether a write at offset that passed
// WriteGate may still land: a registry write always may, any other only while
// CheckOwnership would pass it without writing a heartbeat.
func (r *Registry) WriteFresh(offset int64) bool {
	return r.inRegistry(offset) || (!r.lost.Load() && r.sinceLastBeat() < HeartbeatTimeout/2)
}

func (r *Registry) inRegistry(offset int64) bool {
	return offset >= int64(r.offset) && offset < int64(r.offset)+MaxNodes*SlotSize
}

func (r *Registry) sinceLastBeat() time.Duration {
	t := r.lastBeat.Load()
	if t == 0 {
		return math.MaxInt64
	}
	return bootClock() - time.Duration(t)
}

// bootClock reads CLOCK_BOOTTIME, which unlike Go's monotonic clock keeps counting while the host is suspended, and ignores wall clock steps.
func bootClock() time.Duration {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		panic(fmt.Sprintf("node: read CLOCK_BOOTTIME: %v", err))
	}
	return time.Duration(ts.Nano())
}

// claimSlot replaces the slot bytes Register judged claimable with this node's
// registration, then re-reads the slot to catch a racing node.
func (r *Registry) claimSlot(slotIdx int, expected []byte) error {
	newSlot := &Slot{
		Magic:         SlotMagic,
		State:         NodeStateActive,
		UUID:          r.localUUID,
		HeartbeatSeq:  1,
		HeartbeatTime: time.Now().Unix(),
		MountTime:     time.Now().Unix(),
		BootID:        r.bootID,
		PID:           uint32(os.Getpid()),
		ProcStart:     processStart,
		PIDNamespace:  r.pidNS,
		LockLayout:    r.lockLayout,
	}
	newSlot.JournalOffset, newSlot.JournalSize = r.journalRegion(slotIdx)
	copy(newSlot.Hostname[:], r.hostname)
	// Peers may still hold orphans an earlier owner of this slot listed.
	if old, err := deserializeSlot(expected); err == nil {
		newSlot.Orphans = old.Orphans
		// Register holds r.mu, but no heartbeat runs yet for a slow preempt to hold up.
		if old.State != NodeStateEmpty {
			if err := r.fenceOut(old.UUID); err != nil {
				return err
			}
		}
	}

	success, err := r.casSlot(slotIdx, expected, serializeSlot(newSlot))
	if err != nil {
		return err
	}
	if !success {
		return ErrSlotTaken
	}

	time.Sleep(claimSettle)
	current, err := r.readSlot(slotIdx)
	if err != nil {
		return err
	}
	if current.UUID != r.localUUID {
		return ErrSlotTaken
	}
	return nil
}

// casSlot performs compare-and-swap on a slot.
// Returns true if the swap succeeded.
func (r *Registry) casSlot(slotIdx int, expected, new []byte) (bool, error) {
	offset := int64(r.offset) + int64(slotIdx)*SlotSize

	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)

	// Read current value
	if _, err := r.dev.ReadAtUnsharded(buf.Bytes(), offset); err != nil {
		return false, err
	}

	// Compare
	for i := 0; i < SlotSize; i++ {
		if buf.Bytes()[i] != expected[i] {
			return false, nil // Slot changed
		}
	}

	// Write new value
	copy(buf.Bytes(), new)
	if _, err := r.dev.WriteAtUnsharded(buf.Bytes(), offset); err != nil {
		return false, err
	}

	return true, nil
}

// readSlotRaw reads a node slot and returns its bytes for a later CAS along with the parsed slot.
func (r *Registry) readSlotRaw(slotIdx int) ([]byte, *Slot, error) {
	if slotIdx < 0 || slotIdx >= MaxNodes {
		return nil, nil, fmt.Errorf("slot index out of range: %d", slotIdx)
	}

	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)

	if _, err := r.dev.ReadAtUnsharded(buf.Bytes(), int64(r.offset)+int64(slotIdx)*SlotSize); err != nil {
		return nil, nil, err
	}
	raw := bytes.Clone(buf.Bytes())
	slot, err := deserializeSlot(raw)
	return raw, slot, err
}

// readSlot reads a node slot from disk.
func (r *Registry) readSlot(slotIdx int) (*Slot, error) {
	if slotIdx < 0 || slotIdx >= MaxNodes {
		return nil, fmt.Errorf("slot index out of range: %d", slotIdx)
	}

	offset := int64(r.offset) + int64(slotIdx)*SlotSize

	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)

	if _, err := r.dev.ReadAtUnsharded(buf.Bytes(), offset); err != nil {
		return nil, err
	}

	return deserializeSlot(buf.Bytes())
}

// writeSlot writes a node slot to disk.
func (r *Registry) writeSlot(slotIdx int, slot *Slot) error {
	if slotIdx < 0 || slotIdx >= MaxNodes {
		return fmt.Errorf("slot index out of range: %d", slotIdx)
	}

	offset := int64(r.offset) + int64(slotIdx)*SlotSize

	data := serializeSlot(slot)

	buf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), data)

	_, err := r.dev.WriteAtUnsharded(buf.Bytes(), offset)
	return err
}

// GetActiveNodes returns the active nodes whose heartbeat this node has not seen stop.
func (r *Registry) GetActiveNodes() ([]*Slot, error) {
	slots, err := r.readAllSlots()
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var active []*Slot
	for i, slot := range slots {
		if slot == nil || slot.State != NodeStateActive {
			continue
		}
		r.observe(i, slot)
		if !r.stale(i, slot, HeartbeatTimeout) {
			active = append(active, slot)
		}
	}
	return active, nil
}

// RunningSlots returns the slots whose bytes change while it watches the registry for watch, as a running node's
// heartbeat changes its slot every HeartbeatInterval. A stopped or paused node's slot does not change.
func (r *Registry) RunningSlots(watch time.Duration) (map[int]bool, error) {
	before, err := r.readRegistryRaw()
	if err != nil {
		return nil, err
	}
	time.Sleep(watch)
	after, err := r.readRegistryRaw()
	if err != nil {
		return nil, err
	}
	running := make(map[int]bool)
	for i := range MaxNodes {
		if !bytes.Equal(before[i*SlotSize:(i+1)*SlotSize], after[i*SlotSize:(i+1)*SlotSize]) {
			running[i] = true
		}
	}
	return running, nil
}

func (r *Registry) readRegistryRaw() ([]byte, error) {
	buf := storage.NewAlignedBuffer(MaxNodes * SlotSize)
	if _, err := r.dev.ReadAtUnsharded(buf.Bytes(), int64(r.offset)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// readAllSlots reads the whole registry in one I/O, without a lock, so slow
// storage costs a scan one round trip and never holds up the heartbeat. A slot
// that fails to decode is nil.
func (r *Registry) readAllSlots() ([]*Slot, error) {
	buf := storage.GetBuffer(MaxNodes * SlotSize)
	defer storage.PutBuffer(buf)
	if _, err := r.dev.ReadAtUnsharded(buf.Bytes()[:MaxNodes*SlotSize], int64(r.offset)); err != nil {
		return nil, err
	}
	slots := make([]*Slot, MaxNodes)
	for i := range slots {
		if slot, err := deserializeSlot(buf.Bytes()[i*SlotSize : (i+1)*SlotSize]); err == nil {
			slots[i] = slot
		}
	}
	return slots, nil
}

// Slots returns every node slot, with nil for empty or unreadable ones. The
// error names the unreadable ones. It holds no lock, so it never delays a heartbeat.
func (r *Registry) Slots() ([]*Slot, error) {
	slots := make([]*Slot, MaxNodes)
	var errs []error
	for i := range slots {
		slot, err := r.readSlot(i)
		if err != nil {
			errs = append(errs, fmt.Errorf("slot %d: %w", i, err))
			continue
		}
		if slot.State != NodeStateEmpty {
			slots[i] = slot
		}
	}
	return slots, errors.Join(errs...)
}

// LocalUUID returns this node's UUID.
func (r *Registry) LocalUUID() [16]byte {
	return r.localUUID
}

// LocalSlot returns this node's slot index, or -1 if not registered.
func (r *Registry) LocalSlot() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.localSlot
}

// serializeSlot converts a Slot to bytes.
func serializeSlot(slot *Slot) []byte {
	buf := make([]byte, SlotSize)

	binary.LittleEndian.PutUint32(buf[0:4], slot.Magic)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(slot.State))
	copy(buf[8:24], slot.UUID[:])
	binary.LittleEndian.PutUint64(buf[24:32], slot.HeartbeatSeq)
	binary.LittleEndian.PutUint64(buf[32:40], uint64(slot.HeartbeatTime))
	binary.LittleEndian.PutUint64(buf[40:48], uint64(slot.MountTime))
	binary.LittleEndian.PutUint64(buf[48:56], slot.JournalOffset)
	binary.LittleEndian.PutUint64(buf[56:64], slot.JournalSize)
	copy(buf[64:128], slot.Hostname[:])
	copy(buf[128:144], slot.BootID[:])
	binary.LittleEndian.PutUint32(buf[144:148], slot.PID)
	binary.LittleEndian.PutUint64(buf[148:156], uint64(slot.ProcStart))
	copy(buf[156:172], slot.RecoveryOwner[:])
	binary.LittleEndian.PutUint64(buf[172:180], uint64(slot.RecoveryStart))
	binary.LittleEndian.PutUint64(buf[180:188], slot.PIDNamespace)
	n := min(len(slot.Orphans), MaxOrphans)
	binary.LittleEndian.PutUint32(buf[orphansOffset:], uint32(n))
	for i, o := range slot.Orphans[:n] {
		at := orphansOffset + 4 + 8*i
		binary.LittleEndian.PutUint32(buf[at:], o.Inode)
		binary.LittleEndian.PutUint32(buf[at+4:], o.Birth)
	}
	binary.LittleEndian.PutUint32(buf[lockLayoutOffset:], slot.LockLayout)

	// Compute checksum
	checksum := storage.CRC32C(buf[:SlotSize-4])
	binary.LittleEndian.PutUint32(buf[SlotSize-4:], checksum)

	return buf
}

// ParseSlot decodes one raw registry slot.
func ParseSlot(buf []byte) (*Slot, error) {
	return deserializeSlot(buf)
}

// deserializeSlot parses a Slot from bytes.
func deserializeSlot(buf []byte) (*Slot, error) {
	if len(buf) < SlotSize {
		return nil, ErrInvalidSlot
	}

	// Verify checksum
	stored := binary.LittleEndian.Uint32(buf[SlotSize-4:])
	computed := storage.CRC32C(buf[:SlotSize-4])
	if stored != computed && !bytes.Equal(buf[:SlotSize], emptySlot) {
		return nil, ErrInvalidSlot
	}

	slot := &Slot{
		Magic:         binary.LittleEndian.Uint32(buf[0:4]),
		State:         NodeState(binary.LittleEndian.Uint32(buf[4:8])),
		HeartbeatSeq:  binary.LittleEndian.Uint64(buf[24:32]),
		HeartbeatTime: int64(binary.LittleEndian.Uint64(buf[32:40])),
		MountTime:     int64(binary.LittleEndian.Uint64(buf[40:48])),
		JournalOffset: binary.LittleEndian.Uint64(buf[48:56]),
		JournalSize:   binary.LittleEndian.Uint64(buf[56:64]),
		PID:           binary.LittleEndian.Uint32(buf[144:148]),
		ProcStart:     int64(binary.LittleEndian.Uint64(buf[148:156])),
		RecoveryStart: int64(binary.LittleEndian.Uint64(buf[172:180])),
		PIDNamespace:  binary.LittleEndian.Uint64(buf[180:188]),
		LockLayout:    binary.LittleEndian.Uint32(buf[lockLayoutOffset:]),
		Checksum:      stored,
	}
	copy(slot.UUID[:], buf[8:24])
	copy(slot.Hostname[:], buf[64:128])
	copy(slot.BootID[:], buf[128:144])
	copy(slot.RecoveryOwner[:], buf[156:172])
	n := min(int(binary.LittleEndian.Uint32(buf[orphansOffset:])), MaxOrphans)
	for i := range n {
		at := orphansOffset + 4 + 8*i
		slot.Orphans = append(slot.Orphans, Orphan{Inode: binary.LittleEndian.Uint32(buf[at:]), Birth: binary.LittleEndian.Uint32(buf[at+4:])})
	}

	return slot, nil
}

// StartFailureDetection replays the journals of failed nodes it finds now,
// then keeps scanning in the background.
func (r *Registry) StartFailureDetection() {
	r.mu.RLock()
	if r.localSlot < 0 {
		r.mu.RUnlock()
		return // Not registered, can't do failure detection
	}
	r.mu.RUnlock()

	if testNoPeerReplay {
		log.Printf("[WARN] node: GOCLUSTERFS_TEST_NO_PEER_REPLAY is set: this node replays no other node's journal; use it only in tests")
	}
	r.detectAndRecoverFailedNodes()
	r.heartbeatWg.Add(2)
	go r.failureDetectionLoop()
	go r.orphanLoop()
}

// testNoPeerReplay, from GOCLUSTERFS_TEST_NO_PEER_REPLAY and for tests only,
// stops failure detection replaying other nodes, so a test can free a stopped
// node's slot with goclusterfs-fsck while this node runs.
var testNoPeerReplay = os.Getenv("GOCLUSTERFS_TEST_NO_PEER_REPLAY") != ""

// failureDetectionLoop scans for failed nodes and triggers recovery.
func (r *Registry) failureDetectionLoop() {
	defer r.heartbeatWg.Done()

	ticker := time.NewTicker(r.failureCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.detectAndRecoverFailedNodes()
		}
	}
}

// detectAndRecoverFailedNodes scans all slots for failed nodes.
func (r *Registry) detectAndRecoverFailedNodes() {
	r.mu.Lock()
	handler := r.recoveryHandler
	localSlot := r.localSlot
	r.mu.Unlock()

	if handler == nil {
		return // No recovery handler registered
	}

	slots, err := r.readAllSlots()
	if err != nil {
		return
	}
	r.KickOrphans()
	if testNoPeerReplay {
		return
	}
	for i, slot := range slots {
		if i == localSlot || slot == nil {
			continue
		}
		r.mu.Lock()
		r.observe(i, slot)
		if f := r.failures[i]; f != nil && (slot.UUID != f.uuid || slot.State != NodeStateDead) {
			delete(r.failures, i)
		}

		// Peers wait for a failed node's locks until its replay ends, so only a node proven dead, not one paused, is replayed.
		if !r.stale(i, slot, deadAfter) || r.backingOff(i, slot) {
			r.mu.Unlock()
			continue
		}

		// Skip if already recovering
		if r.recoveryInProgress[i] {
			r.mu.Unlock()
			continue
		}
		r.recoveryInProgress[i] = true
		r.mu.Unlock()

		// Try to acquire the slot for recovery
		info, err := r.acquireForRecovery(i, slot, deadAfter)
		if err != nil {
			r.mu.Lock()
			delete(r.recoveryInProgress, i)
			r.mu.Unlock()
			continue // Another node beat us to it
		}
		done := r.ownerDead(info.UUID, i)
		err = r.replay(handler, i, info)
		done(err == nil)
		if err != nil {
			r.mu.Lock()
			delete(r.recoveryInProgress, i)
			msg := r.replayFailed(i, info.UUID, err)
			r.mu.Unlock()
			if msg != "" {
				log.Print(msg)
			}
			continue
		}

		if err := r.clearFailedSlot(i, info.UUID); err != nil {
			log.Printf("[Recovery] Replayed slot %d (node %x), but emptying the slot failed: %v; any node replays it again after %v",
				i, info.UUID[:8], err, recoveryLease)
		}
		r.mu.Lock()
		delete(r.recoveryInProgress, i)
		delete(r.failures, i)
		r.mu.Unlock()
	}
}

const (
	// replayRepeats identical failures in a row make this node back off from a slot's replay.
	replayRepeats = 3
	// ReplayBackoff is how long a backed-off slot must stay unchanged before this node replays it again.
	// Every attempt, by any node, renews the slot's lease, so once all nodes back off the cluster tries about this often.
	ReplayBackoff = 10 * time.Minute
	// failureLogEvery is how often a backed-off slot's failures are logged.
	failureLogEvery = time.Hour
)

// replayFailure is a run of replay failures of one dead node's slot, all with the same error.
type replayFailure struct {
	uuid   [16]byte
	err    string
	count  int
	logged time.Time
}

// backingOff reports whether this node leaves slot i alone for now. Called with r.mu held after observe.
func (r *Registry) backingOff(i int, slot *Slot) bool {
	f := r.failures[i]
	return f != nil && f.count >= replayRepeats && r.silentFor(i, slot) < ReplayBackoff
}

// replayFailed counts a failed replay of slot i and returns what to log, or "" once the slot is backed off
// and was logged within failureLogEvery. Called with r.mu held.
func (r *Registry) replayFailed(i int, uuid [16]byte, err error) string {
	failed := fmt.Sprintf("[Recovery] Replaying slot %d (node %x) failed: %v", i, uuid[:8], err)
	if errors.Is(err, ErrReplayContended) || errors.Is(err, ErrLeaseLost) {
		return fmt.Sprintf("%s; any node retries after %v", failed, recoveryLease)
	}
	f := r.failures[i]
	if f == nil || f.uuid != uuid || f.err != err.Error() {
		f = &replayFailure{uuid: uuid, err: err.Error()}
		r.failures[i] = f
	}
	f.count++
	switch {
	case f.count < replayRepeats:
		return fmt.Sprintf("%s; any node retries after %v. To see what its journal holds, run goclusterfs-fsck -inspect-slot %d; "+
			"to discard it, mount a node with -skip-recovery=%d", failed, recoveryLease, i, i)
	case f.count == replayRepeats || time.Since(f.logged) >= failureLogEvery:
		f.logged = time.Now()
		return fmt.Sprintf("%s, %d times in a row; this node now retries it after %v without a change and logs this at most hourly. "+
			"Files it had locked stay locked until a replay succeeds. Run goclusterfs-fsck -inspect-slot %d, then see Damaged Journal Slot in OPERATIONS.md",
			failed, f.count, ReplayBackoff, i)
	}
	return ""
}

// FailingReplay is a dead node's slot whose replay failed on this node with the same error several times in a row.
type FailingReplay struct {
	Slot     int
	Failures int
	Err      string
}

// acquireForRecovery marks a failed node's slot Dead with this node as the
// recovery lease holder. The CAS lets only one node replay the journal; a
// lease that lapses lets another node resume an interrupted replay.
func (r *Registry) acquireForRecovery(slotIdx int, observed *Slot, timeout time.Duration) (*FailedNodeInfo, error) {
	raw, slot, err := r.confirmStale(slotIdx, observed, timeout)
	if err != nil {
		return nil, err
	}
	// Before the CAS, so the dead node's late heartbeat cannot land over the Dead mark, and outside r.mu, so a slow
	// preempt cannot hold up this node's own heartbeat.
	if err := r.fenceOut(slot.UUID); err != nil {
		log.Printf("[Recovery] Not recovering slot %d: %v", slotIdx, err)
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	slot.State = NodeStateDead
	slot.RecoveryOwner = r.localUUID
	slot.RecoveryStart = time.Now().Unix()
	if success, err := r.casSlot(slotIdx, raw, serializeSlot(slot)); err != nil || !success {
		return nil, ErrSlotTaken
	}
	time.Sleep(claimSettle)
	if current, err := r.readSlot(slotIdx); err != nil || current.RecoveryOwner != r.localUUID {
		return nil, ErrSlotTaken
	}

	return &FailedNodeInfo{
		SlotIndex:     slotIdx,
		UUID:          slot.UUID,
		JournalOffset: slot.JournalOffset,
		JournalSize:   slot.JournalSize,
		LastHeartbeat: time.Unix(slot.HeartbeatTime, 0),
		Hostname:      slot.Host(),
		Orphans:       slot.Orphans,
		CheckLease:    r.leaseCheck(slotIdx, slot.UUID, time.Now()),
	}, nil
}

// confirmStale reads slot slotIdx again and returns its raw bytes if it still holds observed's node, stale for timeout.
func (r *Registry) confirmStale(slotIdx int, observed *Slot, timeout time.Duration) ([]byte, *Slot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	raw, slot, err := r.readSlotRaw(slotIdx)
	if err != nil {
		return nil, nil, err
	}
	r.observe(slotIdx, slot)
	if slot.UUID != observed.UUID || !r.stale(slotIdx, slot, timeout) {
		return nil, nil, ErrSlotTaken
	}
	return raw, slot, nil
}

// leaseCheck returns a check that fails once this node no longer holds the
// recovery lease of the slot, and renews the lease after a quarter of its term
// since renewed, on this node's clock. Peers see a renewal as a change.
func (r *Registry) leaseCheck(slotIdx int, uuid [16]byte, renewed time.Time) func() error {
	return func() error {
		r.mu.Lock()
		defer r.mu.Unlock()

		raw, slot, err := r.readSlotRaw(slotIdx)
		if err != nil {
			return err
		}
		if slot.State != NodeStateDead || slot.UUID != uuid || slot.RecoveryOwner != r.localUUID {
			return ErrLeaseLost
		}
		if time.Since(renewed) < recoveryLease/4 {
			return nil
		}
		now := time.Now()
		slot.RecoveryStart = max(now.Unix(), slot.RecoveryStart+1)
		if success, err := r.casSlot(slotIdx, raw, serializeSlot(slot)); err != nil || !success {
			return ErrLeaseLost
		}
		time.Sleep(claimSettle)
		if current, err := r.readSlot(slotIdx); err != nil || current.RecoveryOwner != r.localUUID {
			return ErrLeaseLost
		}
		renewed = now
		return nil
	}
}

// clearFailedSlot empties a slot after this node replayed its journal, unless
// another node has claimed it or taken over the recovery since.
func (r *Registry) clearFailedSlot(slotIdx int, uuid [16]byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	slot, err := r.readSlot(slotIdx)
	if err != nil {
		return err
	}
	if slot.UUID != uuid || slot.State != NodeStateDead || slot.RecoveryOwner != r.localUUID {
		return nil
	}
	_, err = r.casSlot(slotIdx, serializeSlot(slot), serializeSlot(&Slot{Magic: SlotMagic, State: NodeStateEmpty}))
	return err
}

// releaseLeaseBudget is how long ReleaseSlot trusts its recovery lease before
// it frees the slot, well inside recoveryLease so peers' clocks have room.
var releaseLeaseBudget = recoveryLease / 2

// ReleaseSlot frees slot slotIdx, whose node will not return, if the slot
// still holds expected, the raw bytes the caller checked. Like a peer that
// replays a crashed node's journal, it first takes the slot's recovery lease,
// so neither Register nor failure detection touches the slot while reset
// clears its journal; then it empties the slot. A long reset calls renew, as a
// replay does before each transaction: it keeps the lease and fails once
// another node holds it. ReleaseSlot returns false without running reset if
// the slot changed or another node won the lease.
func (r *Registry) ReleaseSlot(slotIdx int, expected []byte, reset func(renew func() error) error) (bool, error) {
	if slotIdx < 0 || slotIdx >= MaxNodes {
		return false, fmt.Errorf("slot index out of range: %d", slotIdx)
	}
	slot, err := deserializeSlot(expected)
	if err != nil {
		return false, err
	}
	if err := r.fenceOut(slot.UUID); err != nil {
		return false, err
	}
	slot.Magic = SlotMagic
	slot.State = NodeStateDead
	slot.RecoveryOwner = r.localUUID
	leaseStart := time.Now()
	slot.RecoveryStart = leaseStart.Unix()
	leased := serializeSlot(slot)
	if ok, err := r.takeReleaseLease(slotIdx, expected, leased); err != nil || !ok {
		return false, err
	}

	held := leaseStart
	check := r.leaseCheck(slotIdx, slot.UUID, leaseStart)
	renew := func() error {
		if err := check(); err != nil {
			return err
		}
		held = time.Now()
		return nil
	}
	if err := reset(renew); err != nil {
		return false, err
	}
	if age := time.Since(held); age > releaseLeaseBudget {
		return false, fmt.Errorf("the journal reset ran %s without renewing the slot's recovery lease, too long to trust it; "+
			"the slot stays dead until the lease lapses", age.Round(time.Millisecond))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	raw, current, err := r.readSlotRaw(slotIdx)
	if err != nil {
		return false, err
	}
	// Renewals move only RecoveryStart; any other change means another writer.
	renewed := *current
	renewed.RecoveryStart = slot.RecoveryStart
	if !bytes.Equal(serializeSlot(&renewed), leased) {
		return false, fmt.Errorf("slot %d changed while its recovery lease was held", slotIdx)
	}
	// No node replayed this slot to take over its orphans, so the slot keeps them for live nodes and its next owner.
	ok, err := r.casSlot(slotIdx, raw, serializeSlot(&Slot{Magic: SlotMagic, State: NodeStateEmpty, Orphans: current.Orphans}))
	if err == nil && !ok {
		err = fmt.Errorf("slot %d changed while its recovery lease was held", slotIdx)
	}
	return ok, err
}

// takeReleaseLease writes leased over expected and reports whether the slot still holds it after claimSettle.
func (r *Registry) takeReleaseLease(slotIdx int, expected, leased []byte) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ok, err := r.casSlot(slotIdx, expected, leased); err != nil || !ok {
		return false, err
	}
	time.Sleep(claimSettle)
	current, _, err := r.readSlotRaw(slotIdx)
	return err == nil && bytes.Equal(current, leased), err
}

// GetFailedNodes returns information about nodes that have failed (stale heartbeat).
// Useful for manual inspection or triggering recovery outside the normal flow.
func (r *Registry) GetFailedNodes() ([]*FailedNodeInfo, error) {
	slots, err := r.readAllSlots()
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var failed []*FailedNodeInfo
	for i, slot := range slots {
		if slot == nil || slot.State == NodeStateEmpty {
			continue
		}
		r.observe(i, slot)
		if slot.State != NodeStateDead && !r.stale(i, slot, HeartbeatTimeout) {
			continue
		}
		failed = append(failed, &FailedNodeInfo{
			SlotIndex:     i,
			UUID:          slot.UUID,
			JournalOffset: slot.JournalOffset,
			JournalSize:   slot.JournalSize,
			LastHeartbeat: time.Unix(slot.HeartbeatTime, 0),
			Hostname:      slot.Host(),
		})
	}
	return failed, nil
}

func (r *Registry) replay(handler RecoveryHandler, slotIdx int, info *FailedNodeInfo) error {
	bit := uint64(1) << slotIdx
	r.replaying.Or(bit)
	defer r.replaying.And(^bit)
	err := handler(info)
	if err != nil {
		r.replayFailures.Add(1)
	} else {
		r.replays.Add(1)
	}
	return err
}

// Stats is what the registry tells monitoring about this node's liveness and its replays of other nodes' journals.
type Stats struct {
	// HeartbeatAge is how long ago this node's last heartbeat landed; 0 before it registered.
	HeartbeatAge   time.Duration
	Replays        uint64 // other nodes' journals this node replayed
	ReplayFailures uint64 // replays that failed and were left for a retry
	Replaying      []int  // slots whose journal this node is replaying now
	Failing        []FailingReplay
}

// Stats returns this node's liveness and replay counts.
func (r *Registry) Stats() Stats {
	st := Stats{Replays: r.replays.Load(), ReplayFailures: r.replayFailures.Load(), Replaying: r.Replaying(), Failing: r.FailingReplays()}
	if r.lastBeat.Load() != 0 {
		st.HeartbeatAge = r.sinceLastBeat()
	}
	return st
}

// Replaying returns the slots whose journal this node is replaying now.
func (r *Registry) Replaying() []int {
	var slots []int
	for mask := r.replaying.Load(); mask != 0; mask &= mask - 1 {
		slots = append(slots, bits.TrailingZeros64(mask))
	}
	return slots
}

// FailingReplays returns the slots this node backs off from, after their replay failed the same way replayRepeats times in a row.
func (r *Registry) FailingReplays() []FailingReplay {
	r.mu.Lock()
	defer r.mu.Unlock()
	var failing []FailingReplay
	for i, f := range r.failures {
		if f.count >= replayRepeats {
			failing = append(failing, FailingReplay{Slot: i, Failures: f.count, Err: f.err})
		}
	}
	slices.SortFunc(failing, func(a, b FailingReplay) int { return a.Slot - b.Slot })
	return failing
}

// RecoverFailedNode manually triggers recovery for a specific node.
// Returns error if the node is not in a failed state or recovery fails.
func (r *Registry) RecoverFailedNode(slotIdx int) error {
	r.mu.Lock()
	handler := r.recoveryHandler
	r.mu.Unlock()

	if handler == nil {
		return errors.New("no recovery handler registered")
	}

	r.mu.Lock()
	slot, err := r.readSlot(slotIdx)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	r.observe(slotIdx, slot)
	if !r.stale(slotIdx, slot, deadAfter) {
		r.mu.Unlock()
		return errors.New("node is not in a failed state")
	}

	if r.recoveryInProgress[slotIdx] {
		r.mu.Unlock()
		return ErrRecoveryInProgress
	}
	r.recoveryInProgress[slotIdx] = true
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.recoveryInProgress, slotIdx)
		r.mu.Unlock()
	}()

	info, err := r.acquireForRecovery(slotIdx, slot, deadAfter)
	if err != nil {
		return err
	}

	if err := r.replay(handler, slotIdx, info); err != nil {
		return fmt.Errorf("recovery handler failed: %w", err)
	}

	return r.clearFailedSlot(slotIdx, info.UUID)
}
