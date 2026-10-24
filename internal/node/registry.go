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
	RecoveryStart int64      // Dead slots: when that replay started (Unix s)
	Reserved      [3912]byte // Padding to 4096 bytes
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
	onSlotLost func(slot int, owner [16]byte)

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
)

// FailedNodeInfo contains information about a failed node for recovery.
type FailedNodeInfo struct {
	SlotIndex     int
	UUID          [16]byte
	JournalOffset uint64
	JournalSize   uint64
	LastHeartbeat time.Time
	Hostname      string
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
		bootID:               readBootID(),
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
// It first takes back a slot left by a mount process on this host that has
// exited, so the caller replays that journal before serving. Otherwise it
// takes an empty slot or one whose node has failed.
func (r *Registry) Register(ctx context.Context, hostname string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.localSlot >= 0 {
		return r.localSlot, nil // Already registered
	}

	var name [64]byte
	copy(name[:], hostname)
	r.hostname = string(bytes.TrimRight(name[:], "\x00"))

	for _, claimable := range []func(*Slot) bool{
		r.deadLocalOwner,
		func(slot *Slot) bool { return slot.State == NodeStateEmpty || r.failed(slot, HeartbeatTimeout) },
	} {
		for i := 0; i < MaxNodes; i++ {
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
			return i, nil
		}
	}

	return -1, ErrNoFreeSlots
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

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.mu.Lock()
			slot, err := r.readSlot(slotIdx)
			takenOver := err == nil && slot.UUID != r.localUUID && !lost
			if err == nil && slot.UUID == r.localUUID {
				slot.HeartbeatSeq = seq
				slot.HeartbeatTime = time.Now().Unix()
				r.writeSlot(slotIdx, slot)
				seq++
			}
			onSlotLost := r.onSlotLost
			r.mu.Unlock()

			if takenOver {
				lost = true
				if onSlotLost != nil {
					onSlotLost(slotIdx, slot.UUID)
				} else {
					log.Printf("ERROR: node registry slot %d was taken over by node %x; "+
						"this node no longer owns its journal region", slotIdx, slot.UUID[:8])
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

	// Compute checksum
	checksum := storage.CRC32C(buf[:SlotSize-4])
	binary.LittleEndian.PutUint32(buf[SlotSize-4:], checksum)

	return buf
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

		// Stale heartbeats need 2x timeout for consensus
		if !r.failed(slot, 2*HeartbeatTimeout) {
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
		info, err := r.acquireForRecovery(i, slot, 2*HeartbeatTimeout)
		if err != nil {
			r.mu.Lock()
			delete(r.recoveryInProgress, i)
			r.mu.Unlock()
			continue // Another node beat us to it
		}

		if err := handler(info); err != nil {
			log.Printf("[Recovery] Replaying slot %d (node %x) failed: %v; any node retries after %v",
				i, info.UUID[:8], err, recoveryLease)
			r.mu.Lock()
			delete(r.recoveryInProgress, i)
			r.mu.Unlock()
			continue
		}

		// Clear the failed node's slot
		r.clearFailedSlot(i, info.UUID)
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

	return &FailedNodeInfo{
		SlotIndex:     slotIdx,
		UUID:          slot.UUID,
		JournalOffset: slot.JournalOffset,
		JournalSize:   slot.JournalSize,
		LastHeartbeat: time.Unix(slot.HeartbeatTime, 0),
		Hostname:      hostnameOf(slot),
	}, nil
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
