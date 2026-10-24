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
	"os"
	"sync"
	"time"

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
	Magic         uint32     // 0x4E4F4445 "NODE"
	State         NodeState  // Current state
	UUID          [16]byte   // Node's unique identifier
	HeartbeatSeq  uint64     // Monotonic heartbeat counter
	HeartbeatTime int64      // Unix timestamp of last heartbeat
	MountTime     int64      // When this node mounted
	JournalOffset uint64     // This node's journal region offset
	JournalSize   uint64     // This node's journal size
	Hostname      [64]byte   // Node hostname
	BootID        [16]byte   // Kernel boot ID of the node's host
	PID           uint32     // Mount process ID
	ProcStart     int64      // Mount process start time (Unix ns)
	RecoveryOwner [16]byte   // Dead slots: node replaying the journal
	RecoveryStart int64      // Dead slots: when that replay last renewed its lease (Unix s)
	PIDNamespace  uint64     // Inode of the mount process's PID namespace
	Reserved      [3904]byte // Padding to 4096 bytes
	Checksum      uint32     // CRC32C of bytes 0-4091
}

// Registry manages node registration and heartbeats.
type Registry struct {
	mu     sync.RWMutex
	dev    *storage.Device
	offset uint64 // Registry region offset on device

	localUUID   [16]byte
	localSlot   int // -1 if not registered
	stopCh      chan struct{}
	heartbeatWg sync.WaitGroup

	journalOffset   uint64
	journalSlotSize uint64

	hostname   string
	bootID     [16]byte
	pidNS      uint64
	onSlotLost func(slot int, owner [16]byte)

	onOwnerDead  func(owner [16]byte) (release func())
	deadPrevious *[16]byte
	sightings    map[int]sighting

	// Cross-node recovery
	recoveryHandler      RecoveryHandler
	failureCheckInterval time.Duration // How often to scan for failed nodes
	recoveryInProgress   map[int]bool  // Slots currently being recovered
}

// Errors
var (
	ErrNoFreeSlots        = errors.New("no free node slots available")
	ErrSlotTaken          = errors.New("node slot was taken by another node")
	ErrNotRegistered      = errors.New("node is not registered")
	ErrInvalidSlot        = errors.New("invalid slot data")
	ErrRecoveryInProgress = errors.New("recovery already in progress for this node")
	ErrLeaseLost          = errors.New("another node took over this slot's recovery")
)

// FailedNodeInfo contains information about a failed node for recovery.
type FailedNodeInfo struct {
	SlotIndex     int
	UUID          [16]byte
	JournalOffset uint64
	JournalSize   uint64
	LastHeartbeat time.Time
	Hostname      string

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
		stopCh:               make(chan struct{}),
		failureCheckInterval: FailureCheckInterval,
		recoveryInProgress:   make(map[int]bool),
		sightings:            make(map[int]sighting),
		bootID:               readBootID(),
		pidNS:                readPIDNamespace(),
	}

	// Generate a unique UUID for this node
	rand.Read(r.localUUID[:])

	return r
}

// SetRecoveryHandler sets the callback for failed node recovery.
// This should be called before StartHeartbeat.
func (r *Registry) SetRecoveryHandler(handler RecoveryHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recoveryHandler = handler
}

// SetSlotLostHandler sets what the heartbeat does when it finds another node in this node's slot.
// Without a handler it only logs.
func (r *Registry) SetSlotLostHandler(fn func(slot int, owner [16]byte)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onSlotLost = fn
}

// SetOwnerDeadHandler sets what failure detection calls, before replaying a
// proven-dead node's journal, with that node's UUID; for example to take over
// its locks. The returned function runs after the replay.
func (r *Registry) SetOwnerDeadHandler(fn func(owner [16]byte) (release func())) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onOwnerDead = fn
}

// DeadPreviousOwner returns the UUID of the exited mount on this host whose
// slot Register took, if any. Its journal is this node's to replay at mount.
func (r *Registry) DeadPreviousOwner() ([16]byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.deadPrevious == nil {
		return [16]byte{}, false
	}
	return *r.deadPrevious, true
}

func (r *Registry) ownerDead(owner [16]byte) func() {
	r.mu.Lock()
	fn := r.onOwnerDead
	r.mu.Unlock()
	if fn == nil {
		return func() {}
	}
	return fn(owner)
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

// Register claims a slot in the node registry and returns its index.
// It first takes back a failed slot of this host, most likely this node's
// previous incarnation, so the caller replays that journal before serving.
// Slots of this host in another PID namespace (a restarted container, or a
// host-network container) cannot be checked by PID; Register waits for each
// to heartbeat or go stale. Otherwise it takes an empty or failed slot.
func (r *Registry) Register(ctx context.Context, hostname string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.localSlot >= 0 {
		return r.localSlot, nil // Already registered
	}

	var name [64]byte
	copy(name[:], hostname)
	r.hostname = string(bytes.TrimRight(name[:], "\x00"))

	ownFailed := func(slot *Slot) bool {
		return r.sameHost(slot) && (r.deadLocalOwner(slot) || r.failed(slot, HeartbeatTimeout))
	}
	if i, err := r.claimFirst(allSlots(), ownFailed); i >= 0 || err != nil {
		return i, err
	}
	// A slot can go stale between scans, so recheck every slot after the wait.
	if i, err := r.claimFirst(append(r.awaitOtherNamespaceSlots(), allSlots()...), ownFailed); i >= 0 || err != nil {
		return i, err
	}
	i, err := r.claimFirst(allSlots(), func(slot *Slot) bool {
		return slot.State == NodeStateEmpty || r.failed(slot, HeartbeatTimeout)
	})
	if i < 0 && err == nil {
		err = ErrNoFreeSlots
	}
	return i, err
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
		if err := r.claimSlot(i, raw); err != nil {
			if errors.Is(err, ErrSlotTaken) {
				continue
			}
			return -1, err
		}
		r.localSlot = i
		if r.deadLocalOwner(slot) {
			r.deadPrevious = &slot.UUID
		}
		return i, nil
	}
	return -1, nil
}

// awaitOtherNamespaceSlots watches the live-looking slots of this host in
// another PID namespace until each heartbeats or goes stale, and returns the stale ones.
func (r *Registry) awaitOtherNamespaceSlots() []int {
	type beat struct {
		seq  uint64
		time int64
	}
	watch := map[int]beat{}
	for i := 0; i < MaxNodes; i++ {
		_, slot, err := r.readSlotRaw(i)
		if err == nil && r.sameHost(slot) && slot.PIDNamespace != r.pidNS && !r.failed(slot, HeartbeatTimeout) {
			watch[i] = beat{slot.HeartbeatSeq, slot.HeartbeatTime}
		}
	}

	var stale []int
	deadline := time.Now().Add(HeartbeatTimeout + 2*HeartbeatInterval)
	for len(watch) > 0 && time.Now().Before(deadline) {
		time.Sleep(HeartbeatInterval / 4)
		for i, last := range watch {
			_, slot, err := r.readSlotRaw(i)
			switch {
			case err != nil || beat{slot.HeartbeatSeq, slot.HeartbeatTime} != last:
				delete(watch, i)
			case r.failed(slot, HeartbeatTimeout):
				stale = append(stale, i)
				delete(watch, i)
			}
		}
	}
	return stale
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
		if err := r.writeSlot(slotNum, &Slot{Magic: SlotMagic, State: NodeStateEmpty}); err != nil {
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

// heartbeatLoop periodically updates the node's heartbeat.
func (r *Registry) heartbeatLoop(slotIdx int) {
	defer r.heartbeatWg.Done()

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	seq := uint64(1)
	lost := false
	lastBeat := time.Now()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.mu.Lock()
			slot, err := r.readSlot(slotIdx)
			ours := err == nil && slot.UUID == r.localUUID && slot.State != NodeStateDead
			takenOver := err == nil && !ours
			if ours {
				slot.HeartbeatSeq = seq
				slot.HeartbeatTime = time.Now().Unix()
				if r.writeSlot(slotIdx, slot) == nil {
					lastBeat = time.Now()
				}
				seq++
			}
			onSlotLost := r.onSlotLost
			r.mu.Unlock()

			// Peers prove a silent node dead and take its locks; a node that cannot heartbeat must stop.
			failing := time.Since(lastBeat) > HeartbeatTimeout/2
			if (takenOver || failing) && !lost {
				lost = true
				owner := r.localUUID
				if takenOver {
					owner = slot.UUID
				} else {
					log.Printf("ERROR: heartbeats to node registry slot %d have failed for %v", slotIdx, time.Since(lastBeat))
				}
				if onSlotLost != nil {
					onSlotLost(slotIdx, owner)
				} else {
					log.Printf("ERROR: node registry slot %d is no longer this node's (owner %x); "+
						"this node no longer owns its journal region", slotIdx, owner[:8])
				}
			}
		}
	}
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
	}
	newSlot.JournalOffset, newSlot.JournalSize = r.journalRegion(slotIdx)
	copy(newSlot.Hostname[:], r.hostname)

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
	if _, err := r.dev.ReadAt(buf.Bytes(), offset); err != nil {
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
	if _, err := r.dev.WriteAt(buf.Bytes(), offset); err != nil {
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

	if _, err := r.dev.ReadAt(buf.Bytes(), int64(r.offset)+int64(slotIdx)*SlotSize); err != nil {
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

	if _, err := r.dev.ReadAt(buf.Bytes(), offset); err != nil {
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

	_, err := r.dev.WriteAt(buf.Bytes(), offset)
	return err
}

// isHeartbeatStale checks if a slot's heartbeat is beyond the timeout.
func (r *Registry) isHeartbeatStale(slot *Slot) bool {
	age := time.Since(time.Unix(slot.HeartbeatTime, 0))
	return age > HeartbeatTimeout
}

// GetActiveNodes returns information about all active nodes.
func (r *Registry) GetActiveNodes() ([]*Slot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var active []*Slot
	for i := 0; i < MaxNodes; i++ {
		slot, err := r.readSlot(i)
		if err != nil {
			continue
		}
		if slot.State == NodeStateActive && !r.isHeartbeatStale(slot) {
			active = append(active, slot)
		}
	}
	return active, nil
}

// Slots returns every node slot, with nil for empty or unreadable ones. The
// error names the unreadable ones.
func (r *Registry) Slots() ([]*Slot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

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
	if stored != computed && stored != 0 { // Allow zero checksum for empty slots
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
		Checksum:      stored,
	}
	copy(slot.UUID[:], buf[8:24])
	copy(slot.Hostname[:], buf[64:128])
	copy(slot.BootID[:], buf[128:144])
	copy(slot.RecoveryOwner[:], buf[156:172])

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

	r.detectAndRecoverFailedNodes()
	r.heartbeatWg.Add(1)
	go r.failureDetectionLoop()
}

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

	for i := 0; i < MaxNodes; i++ {
		if i == localSlot {
			continue // Skip our own slot
		}

		r.mu.Lock()
		slot, err := r.readSlot(i)
		if err != nil {
			r.mu.Unlock()
			continue
		}
		r.observe(i, slot)

		// Stale heartbeats need 2x timeout for consensus, and none this node saw advance lately.
		if !r.failed(slot, 2*HeartbeatTimeout) || (r.recentlyAlive(i) && !r.deadLocalOwner(slot)) {
			r.mu.Unlock()
			continue
		}
		dead := r.provenDead(i, slot)

		// Skip if already recovering
		if r.recoveryInProgress[i] {
			r.mu.Unlock()
			continue
		}
		r.recoveryInProgress[i] = true
		r.mu.Unlock()

		// Try to acquire the slot for recovery
		info, err := r.acquireForRecovery(i, slot, 2*HeartbeatTimeout)
		if err != nil {
			r.mu.Lock()
			delete(r.recoveryInProgress, i)
			r.mu.Unlock()
			continue // Another node beat us to it
		}
		release := func() {}
		if dead {
			release = r.ownerDead(info.UUID)
		}
		err = handler(info)
		release()
		if err != nil {
			log.Printf("[Recovery] Replaying slot %d (node %x) failed: %v; any node retries after %v",
				i, info.UUID[:8], err, recoveryLease)
			r.mu.Lock()
			delete(r.recoveryInProgress, i)
			r.mu.Unlock()
			continue
		}

		if err := r.clearFailedSlot(i, info.UUID); err != nil {
			log.Printf("[Recovery] Clearing slot %d (node %x) after replay failed: %v; any node replays it again after %v",
				i, info.UUID[:8], err, recoveryLease)
		}
		r.mu.Lock()
		delete(r.recoveryInProgress, i)
		r.mu.Unlock()
	}
}

// acquireForRecovery marks a failed node's slot Dead with this node as the
// recovery lease holder. The CAS lets only one node replay the journal; a
// lease that lapses lets another node resume an interrupted replay.
func (r *Registry) acquireForRecovery(slotIdx int, observed *Slot, timeout time.Duration) (*FailedNodeInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	raw, slot, err := r.readSlotRaw(slotIdx)
	if err != nil {
		return nil, err
	}
	if slot.UUID != observed.UUID || !r.failed(slot, timeout) {
		return nil, ErrSlotTaken
	}

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
		Hostname:      hostnameOf(slot),
		CheckLease:    r.leaseCheck(slotIdx, slot.UUID),
	}, nil
}

// leaseCheck returns a check that fails once this node no longer holds the
// recovery lease of the slot, and renews the lease after a quarter of its term.
func (r *Registry) leaseCheck(slotIdx int, uuid [16]byte) func() error {
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
		if time.Since(time.Unix(slot.RecoveryStart, 0)) < recoveryLease/4 {
			return nil
		}
		slot.RecoveryStart = time.Now().Unix()
		if success, err := r.casSlot(slotIdx, raw, serializeSlot(slot)); err != nil || !success {
			return ErrLeaseLost
		}
		time.Sleep(claimSettle)
		if current, err := r.readSlot(slotIdx); err != nil || current.RecoveryOwner != r.localUUID {
			return ErrLeaseLost
		}
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

// ReleaseSlot frees slot slotIdx, whose node will not return, if the slot
// still holds expected, the raw bytes the caller checked. Like a peer that
// replays a crashed node's journal, it first takes the slot's recovery lease,
// so neither Register nor failure detection touches the slot while reset
// clears its journal; then it empties the slot. It returns false without
// running reset if the slot changed or another node won the lease.
func (r *Registry) ReleaseSlot(slotIdx int, expected []byte, reset func() error) (bool, error) {
	if slotIdx < 0 || slotIdx >= MaxNodes {
		return false, fmt.Errorf("slot index out of range: %d", slotIdx)
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	slot, err := deserializeSlot(expected)
	if err != nil {
		return false, err
	}
	slot.Magic = SlotMagic
	slot.State = NodeStateDead
	slot.RecoveryOwner = r.localUUID
	slot.RecoveryStart = time.Now().Unix()
	leased := serializeSlot(slot)
	if ok, err := r.casSlot(slotIdx, expected, leased); err != nil || !ok {
		return false, err
	}
	time.Sleep(claimSettle)
	if current, _, err := r.readSlotRaw(slotIdx); err != nil || !bytes.Equal(current, leased) {
		return false, err
	}

	if err := reset(); err != nil {
		return false, err
	}
	ok, err := r.casSlot(slotIdx, leased, serializeSlot(&Slot{Magic: SlotMagic, State: NodeStateEmpty}))
	if err == nil && !ok {
		err = fmt.Errorf("slot %d changed while its recovery lease was held", slotIdx)
	}
	return ok, err
}

// GetFailedNodes returns information about nodes that have failed (stale heartbeat).
// Useful for manual inspection or triggering recovery outside the normal flow.
func (r *Registry) GetFailedNodes() ([]*FailedNodeInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var failed []*FailedNodeInfo
	for i := 0; i < MaxNodes; i++ {
		slot, err := r.readSlot(i)
		if err != nil {
			continue
		}

		isFailed := false
		switch slot.State {
		case NodeStateActive, NodeStateLeaving:
			if r.isHeartbeatStale(slot) {
				isFailed = true
			}
		case NodeStateDead:
			isFailed = true
		}

		if isFailed {
			hostname := string(slot.Hostname[:])
			for j, b := range slot.Hostname {
				if b == 0 {
					hostname = string(slot.Hostname[:j])
					break
				}
			}

			failed = append(failed, &FailedNodeInfo{
				SlotIndex:     i,
				UUID:          slot.UUID,
				JournalOffset: slot.JournalOffset,
				JournalSize:   slot.JournalSize,
				LastHeartbeat: time.Unix(slot.HeartbeatTime, 0),
				Hostname:      hostname,
			})
		}
	}
	return failed, nil
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

	if !r.failed(slot, HeartbeatTimeout) {
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

	info, err := r.acquireForRecovery(slotIdx, slot, HeartbeatTimeout)
	if err != nil {
		return err
	}

	if err := handler(info); err != nil {
		return fmt.Errorf("recovery handler failed: %w", err)
	}

	return r.clearFailedSlot(slotIdx, info.UUID)
}
