// Package node implements cluster node registration and heartbeat management.
//
// In a shared-storage cluster, nodes coordinate through on-disk structures
// rather than network protocols. Each node claims a slot in the registry
// using CAS (compare-and-swap), then maintains periodic heartbeats.
// Failed nodes are detected when their heartbeat becomes stale.
package node

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
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
	Hostname      [64]byte   // Node hostname (informational)
	Reserved      [3964]byte // Padding to 4096 bytes
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

// Register claims a slot in the node registry.
// Returns the slot index on success.
func (r *Registry) Register(ctx context.Context, hostname string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.localSlot >= 0 {
		return r.localSlot, nil // Already registered
	}

	// Find a free slot or reclaim a dead one
	for i := 0; i < MaxNodes; i++ {
		slot, err := r.readSlot(i)
		if err != nil {
			continue
		}

		canClaim := false
		switch slot.State {
		case NodeStateEmpty:
			canClaim = true
		case NodeStateActive, NodeStateLeaving:
			// Check if heartbeat is stale
			if r.isHeartbeatStale(slot) {
				canClaim = true
			}
		case NodeStateDead:
			canClaim = true
		}

		if canClaim {
			if err := r.claimSlot(i, hostname); err != nil {
				if errors.Is(err, ErrSlotTaken) {
					continue // Race condition, try next slot
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

	// Signal goroutines to stop
	close(r.stopCh)

	// IMPORTANT: Release lock before waiting on WaitGroup to avoid deadlock.
	// Background goroutines (failureDetectionLoop, heartbeatLoop) may be holding
	// or waiting for the lock when they receive the stop signal.
	r.mu.Unlock()

	// Wait for background goroutines to finish
	r.heartbeatWg.Wait()

	// Re-acquire lock for final cleanup
	r.mu.Lock()
	defer r.mu.Unlock()

	// Mark slot as empty
	slot := &Slot{
		Magic: SlotMagic,
		State: NodeStateEmpty,
	}

	if err := r.writeSlot(slotNum, slot); err != nil {
		return err
	}

	r.localSlot = -1
	r.stopCh = make(chan struct{})
	return nil
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

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.mu.Lock()
			slot, err := r.readSlot(slotIdx)
			if err == nil && slot.UUID == r.localUUID {
				slot.HeartbeatSeq = seq
				slot.HeartbeatTime = time.Now().Unix()
				r.writeSlot(slotIdx, slot)
				seq++
			}
			r.mu.Unlock()
		}
	}
}

// claimSlot attempts to atomically claim a slot using CAS.
func (r *Registry) claimSlot(slotIdx int, hostname string) error {
	offset := int64(r.offset) + int64(slotIdx)*SlotSize

	// Read raw bytes directly (not deserialize) for CAS comparison
	oldBuf := storage.GetBuffer(SlotSize)
	defer storage.PutBuffer(oldBuf)

	if _, err := r.dev.ReadAt(oldBuf.Bytes(), offset); err != nil {
		return err
	}

	// Make a copy of old data for comparison
	oldData := make([]byte, SlotSize)
	copy(oldData, oldBuf.Bytes())

	// Prepare new slot
	newSlot := &Slot{
		Magic:         SlotMagic,
		State:         NodeStateActive,
		UUID:          r.localUUID,
		HeartbeatSeq:  1,
		HeartbeatTime: time.Now().Unix(),
		MountTime:     time.Now().Unix(),
	}
	copy(newSlot.Hostname[:], hostname)

	newData := serializeSlot(newSlot)

	// Perform CAS with raw bytes
	success, err := r.casSlot(slotIdx, oldData, newData)
	if err != nil {
		return err
	}
	if !success {
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
		Checksum:      stored,
	}
	copy(slot.UUID[:], buf[8:24])
	copy(slot.Hostname[:], buf[64:128])

	return slot, nil
}

// StartFailureDetection begins the background failure detection goroutine.
// This scans for failed nodes and triggers recovery via the registered handler.
func (r *Registry) StartFailureDetection() {
	r.mu.RLock()
	if r.localSlot < 0 {
		r.mu.RUnlock()
		return // Not registered, can't do failure detection
	}
	r.mu.RUnlock()

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

		// Check for failed nodes
		needsRecovery := false
		switch slot.State {
		case NodeStateActive, NodeStateLeaving:
			// Check for stale heartbeat (2x timeout for consensus)
			age := time.Since(time.Unix(slot.HeartbeatTime, 0))
			if age > 2*HeartbeatTimeout {
				needsRecovery = true
			}
		case NodeStateDead:
			// Already marked dead, try to recover if we haven't
			needsRecovery = true
		}

		if !needsRecovery {
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
		info, err := r.acquireForRecovery(i, slot)
		if err != nil {
			r.mu.Lock()
			delete(r.recoveryInProgress, i)
			r.mu.Unlock()
			continue // Another node beat us to it
		}

		// Perform recovery
		if err := handler(info); err != nil {
			// Recovery failed, release lock
			r.releaseRecoveryLock(i, slot)
			r.mu.Lock()
			delete(r.recoveryInProgress, i)
			r.mu.Unlock()
			continue
		}

		// Clear the failed node's slot
		r.clearFailedSlot(i)
		r.mu.Lock()
		delete(r.recoveryInProgress, i)
		r.mu.Unlock()
	}
}

// acquireForRecovery attempts to atomically acquire a failed node's slot.
// Uses CAS to ensure only one node performs recovery.
func (r *Registry) acquireForRecovery(slotIdx int, currentSlot *Slot) (*FailedNodeInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Re-read to get current state for CAS
	slot, err := r.readSlot(slotIdx)
	if err != nil {
		return nil, err
	}

	// Verify slot still needs recovery
	if slot.State == NodeStateEmpty {
		return nil, ErrSlotTaken // Already cleared
	}
	if slot.UUID != currentSlot.UUID {
		return nil, ErrSlotTaken // Different node claimed it
	}

	// Prepare slot for recovery (mark as Dead if not already)
	if slot.State != NodeStateDead {
		oldData := serializeSlot(slot)

		slot.State = NodeStateDead
		newData := serializeSlot(slot)

		success, err := r.casSlot(slotIdx, oldData, newData)
		if err != nil || !success {
			return nil, ErrSlotTaken
		}
	}

	// Extract hostname as string
	hostname := string(slot.Hostname[:])
	for i, b := range slot.Hostname {
		if b == 0 {
			hostname = string(slot.Hostname[:i])
			break
		}
	}

	return &FailedNodeInfo{
		SlotIndex:     slotIdx,
		UUID:          slot.UUID,
		JournalOffset: slot.JournalOffset,
		JournalSize:   slot.JournalSize,
		LastHeartbeat: time.Unix(slot.HeartbeatTime, 0),
		Hostname:      hostname,
	}, nil
}

// releaseRecoveryLock releases the lock on a failed slot after a failed recovery.
// The slot stays in Dead state for another node to retry.
func (r *Registry) releaseRecoveryLock(slotIdx int, originalSlot *Slot) {
	// Nothing to do - slot stays in Dead state
	// Another node can retry recovery
}

// clearFailedSlot clears a slot after successful recovery.
func (r *Registry) clearFailedSlot(slotIdx int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	emptySlot := &Slot{
		Magic: SlotMagic,
		State: NodeStateEmpty,
	}

	return r.writeSlot(slotIdx, emptySlot)
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

	// Verify node is actually failed
	needsRecovery := false
	switch slot.State {
	case NodeStateActive, NodeStateLeaving:
		if r.isHeartbeatStale(slot) {
			needsRecovery = true
		}
	case NodeStateDead:
		needsRecovery = true
	}

	if !needsRecovery {
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

	info, err := r.acquireForRecovery(slotIdx, slot)
	if err != nil {
		return err
	}

	if err := handler(info); err != nil {
		return fmt.Errorf("recovery handler failed: %w", err)
	}

	return r.clearFailedSlot(slotIdx)
}
