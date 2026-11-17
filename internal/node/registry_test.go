package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

func TestSlotSerializeDeserialize(t *testing.T) {
	slot := &Slot{
		Magic:         SlotMagic,
		State:         NodeStateActive,
		HeartbeatSeq:  12345,
		HeartbeatTime: time.Now().Unix(),
		MountTime:     time.Now().Unix() - 1000,
		JournalOffset: 1 << 20,
		JournalSize:   4 << 20,
	}
	copy(slot.UUID[:], []byte("0123456789abcdef"))
	copy(slot.Hostname[:], "test-node-01")

	// Serialize
	data := serializeSlot(slot)

	if len(data) != SlotSize {
		t.Fatalf("serialized size = %d, want %d", len(data), SlotSize)
	}

	// Deserialize
	restored, err := deserializeSlot(data)
	if err != nil {
		t.Fatalf("deserializeSlot failed: %v", err)
	}

	// Verify fields
	if restored.Magic != slot.Magic {
		t.Errorf("Magic = %x, want %x", restored.Magic, slot.Magic)
	}
	if restored.State != slot.State {
		t.Errorf("State = %d, want %d", restored.State, slot.State)
	}
	if restored.HeartbeatSeq != slot.HeartbeatSeq {
		t.Errorf("HeartbeatSeq = %d, want %d", restored.HeartbeatSeq, slot.HeartbeatSeq)
	}
	if restored.HeartbeatTime != slot.HeartbeatTime {
		t.Errorf("HeartbeatTime = %d, want %d", restored.HeartbeatTime, slot.HeartbeatTime)
	}
	if restored.UUID != slot.UUID {
		t.Errorf("UUID mismatch")
	}
	if string(restored.Hostname[:12]) != "test-node-01" {
		t.Errorf("Hostname = %q, want %q", string(restored.Hostname[:12]), "test-node-01")
	}
}

func TestSlotDeserializeInvalid(t *testing.T) {
	// Buffer too small
	_, err := deserializeSlot(make([]byte, 100))
	if err == nil {
		t.Error("expected error for small buffer")
	}

	// Corrupted checksum (but non-zero)
	slot := &Slot{Magic: SlotMagic, State: NodeStateActive}
	data := serializeSlot(slot)
	data[100] ^= 0xFF // Corrupt data
	_, err = deserializeSlot(data)
	if err == nil {
		t.Error("expected error for corrupted checksum")
	}
}

func TestSlotStates(t *testing.T) {
	// Verify state constants
	if NodeStateEmpty != 0 {
		t.Error("NodeStateEmpty should be 0")
	}
	if NodeStateActive != 1 {
		t.Error("NodeStateActive should be 1")
	}
	if NodeStateLeaving != 2 {
		t.Error("NodeStateLeaving should be 2")
	}
	if NodeStateDead != 3 {
		t.Error("NodeStateDead should be 3")
	}
}

func TestSlotMagic(t *testing.T) {
	// Verify magic is a recognizable constant
	// 0x4E4F4445 in big-endian order spells "NODE"
	if SlotMagic == 0 {
		t.Error("SlotMagic should not be zero")
	}
}

func TestSlotSize(t *testing.T) {
	// SlotSize should match SectorSize for O_DIRECT alignment
	if SlotSize != storage.SectorSize {
		t.Errorf("SlotSize = %d, should match storage.SectorSize = %d", SlotSize, storage.SectorSize)
	}
}

// createTestDevice creates a test file for O_DIRECT testing.
// Returns the device and a cleanup function.
func createTestDevice(t *testing.T, size int64) (*storage.Device, func()) {
	t.Helper()

	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_registry.bin")

	f, err := os.Create(testFile)
	if err != nil {
		t.Skipf("Cannot create test file: %v", err)
	}

	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		_ = os.Remove(testFile)
		t.Skipf("Cannot truncate test file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	dev, err := storage.Open(testFile)
	if err != nil {
		_ = os.Remove(testFile)
		t.Skipf("O_DIRECT not supported: %v", err)
	}

	cleanup := func() {
		_ = dev.Close()
		_ = os.Remove(testFile)
	}

	return dev, cleanup
}

func TestRegistryCreate(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20) // 10MB
	defer cleanup()

	reg := NewRegistry(dev, 0)

	// UUID should be generated
	allZero := true
	for _, b := range reg.LocalUUID() {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Error("LocalUUID should be randomly generated")
	}

	// Should not be registered yet
	if reg.LocalSlot() != -1 {
		t.Errorf("LocalSlot = %d, want -1", reg.LocalSlot())
	}
}

func TestRegistryRegisterUnregister(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	ctx := context.Background()

	// Register
	slot, err := reg.Register(ctx, "test-host")
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if slot < 0 || slot >= MaxNodes {
		t.Errorf("slot = %d, want [0, %d)", slot, MaxNodes)
	}

	if reg.LocalSlot() != slot {
		t.Errorf("LocalSlot = %d, want %d", reg.LocalSlot(), slot)
	}

	// Register again should return same slot
	slot2, err := reg.Register(ctx, "test-host")
	if err != nil {
		t.Fatalf("Second Register failed: %v", err)
	}
	if slot2 != slot {
		t.Errorf("second Register returned slot %d, want %d", slot2, slot)
	}

	// Start heartbeat
	if err := reg.StartHeartbeat(); err != nil {
		t.Fatalf("StartHeartbeat failed: %v", err)
	}

	// Wait a bit for heartbeat to run
	time.Sleep(100 * time.Millisecond)

	// Unregister
	if err := reg.Unregister(ctx); err != nil {
		t.Fatalf("Unregister failed: %v", err)
	}

	if reg.LocalSlot() != -1 {
		t.Errorf("LocalSlot after unregister = %d, want -1", reg.LocalSlot())
	}
}

func TestRegistryUnregisterWithoutRegister(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	ctx := context.Background()

	err := reg.Unregister(ctx)
	if err != ErrNotRegistered {
		t.Errorf("Unregister without register: err = %v, want ErrNotRegistered", err)
	}
}

func TestRegistryStartHeartbeatWithoutRegister(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)

	err := reg.StartHeartbeat()
	if err != ErrNotRegistered {
		t.Errorf("StartHeartbeat without register: err = %v, want ErrNotRegistered", err)
	}
}

func TestRegistryMultipleNodes(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	ctx := context.Background()

	// Create multiple registries (simulating multiple nodes)
	reg1 := NewRegistry(dev, 0)
	reg2 := NewRegistry(dev, 0)

	// Register first node
	slot1, err := reg1.Register(ctx, "node-1")
	if err != nil {
		t.Fatalf("Register node-1 failed: %v", err)
	}

	// Register second node
	slot2, err := reg2.Register(ctx, "node-2")
	if err != nil {
		t.Fatalf("Register node-2 failed: %v", err)
	}

	// Slots should be different
	if slot1 == slot2 {
		t.Errorf("Both nodes got same slot: %d", slot1)
	}

	// Cleanup
	if err := reg1.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
	if err := reg2.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryGetActiveNodes(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	ctx := context.Background()
	reg := NewRegistry(dev, 0)

	// Initially no active nodes
	active, err := reg.GetActiveNodes()
	if err != nil {
		t.Fatalf("GetActiveNodes failed: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected 0 active nodes, got %d", len(active))
	}

	// Register
	_, err = reg.Register(ctx, "test-host")
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Now should have 1 active node
	active, err = reg.GetActiveNodes()
	if err != nil {
		t.Fatalf("GetActiveNodes failed: %v", err)
	}
	if len(active) != 1 {
		t.Errorf("expected 1 active node, got %d", len(active))
	}

	if err := reg.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkSlotSerialize(b *testing.B) {
	slot := &Slot{
		Magic:         SlotMagic,
		State:         NodeStateActive,
		HeartbeatSeq:  12345,
		HeartbeatTime: time.Now().Unix(),
	}
	copy(slot.UUID[:], []byte("0123456789abcdef"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = serializeSlot(slot)
	}
}

func BenchmarkSlotDeserialize(b *testing.B) {
	slot := &Slot{
		Magic:         SlotMagic,
		State:         NodeStateActive,
		HeartbeatSeq:  12345,
		HeartbeatTime: time.Now().Unix(),
	}
	copy(slot.UUID[:], []byte("0123456789abcdef"))
	data := serializeSlot(slot)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = deserializeSlot(data)
	}
}

// --- Cross-Node Recovery Tests ---

func TestSetRecoveryHandler(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)

	called := false
	handler := func(info *FailedNodeInfo) error {
		called = true
		return nil
	}

	reg.SetRecoveryHandler(handler)

	// Handler should be set (can't directly verify, but test compilation)
	if called {
		t.Error("handler should not be called yet")
	}
}

func TestGetFailedNodesEmpty(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)

	failed, err := reg.GetFailedNodes()
	if err != nil {
		t.Fatalf("GetFailedNodes failed: %v", err)
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed nodes, got %d", len(failed))
	}
}

func TestGetFailedNodesWithStaleHeartbeat(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	ctx := context.Background()

	// Register a node
	slot, err := reg.Register(ctx, "failing-node")
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Manually write a stale heartbeat (simulate node crash)
	staleSlot := &Slot{
		Magic:         SlotMagic,
		State:         NodeStateActive,
		UUID:          reg.LocalUUID(),
		HeartbeatSeq:  1,
		HeartbeatTime: time.Now().Add(-2 * HeartbeatTimeout).Unix(), // Stale
		MountTime:     time.Now().Unix(),
		JournalOffset: 1 << 20,
		JournalSize:   4 << 20,
	}
	copy(staleSlot.Hostname[:], "failing-node")

	reg.mu.Lock()
	err = reg.writeSlot(slot, staleSlot)
	reg.mu.Unlock()
	if err != nil {
		t.Fatalf("writeSlot failed: %v", err)
	}

	// Create a second registry to detect the failure
	reg2 := NewRegistry(dev, 0)
	silence(reg2, slot, staleSlot, HeartbeatTimeout+time.Second)

	failed, err := reg2.GetFailedNodes()
	if err != nil {
		t.Fatalf("GetFailedNodes failed: %v", err)
	}
	if len(failed) != 1 {
		t.Errorf("expected 1 failed node, got %d", len(failed))
	}

	if len(failed) > 0 {
		if failed[0].SlotIndex != slot {
			t.Errorf("failed node slot = %d, want %d", failed[0].SlotIndex, slot)
		}
		if failed[0].JournalOffset != staleSlot.JournalOffset {
			t.Errorf("JournalOffset = %d, want %d", failed[0].JournalOffset, staleSlot.JournalOffset)
		}
		if failed[0].JournalSize != staleSlot.JournalSize {
			t.Errorf("JournalSize = %d, want %d", failed[0].JournalSize, staleSlot.JournalSize)
		}
	}
}

func TestRecoverFailedNode(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	// Create both registries first
	reg := NewRegistry(dev, 0)
	reg2 := NewRegistry(dev, 0)
	ctx := context.Background()

	// Register the recovery node FIRST to claim slot 0
	recoverySlot, err := reg2.Register(ctx, "recovery-node")
	if err != nil {
		t.Fatalf("reg2 Register failed: %v", err)
	}
	t.Logf("reg2 claimed slot %d", recoverySlot)

	// Register the failing node (will get slot 1)
	failingSlot, err := reg.Register(ctx, "failing-node")
	if err != nil {
		t.Fatalf("reg Register failed: %v", err)
	}
	t.Logf("reg claimed slot %d", failingSlot)

	// Now write a stale heartbeat to the failing node's slot
	staleUUID := reg.LocalUUID()
	staleSlot := &Slot{
		Magic:         SlotMagic,
		State:         NodeStateActive,
		UUID:          staleUUID,
		HeartbeatSeq:  1,
		HeartbeatTime: time.Now().Add(-2 * HeartbeatTimeout).Unix(),
		MountTime:     time.Now().Unix(),
		JournalOffset: 1 << 20,
		JournalSize:   4 << 20,
	}
	copy(staleSlot.Hostname[:], "failing-node")

	reg.mu.Lock()
	err = reg.writeSlot(failingSlot, staleSlot)
	reg.mu.Unlock()
	if err != nil {
		t.Fatalf("writeSlot failed: %v", err)
	}

	// Set up recovery handler
	var recoveredInfo *FailedNodeInfo
	reg2.SetRecoveryHandler(func(info *FailedNodeInfo) error {
		recoveredInfo = info
		return nil
	})

	silence(reg2, failingSlot, staleSlot, deadAfter+time.Second)

	// Trigger recovery on the failing node's slot
	err = reg2.RecoverFailedNode(failingSlot)
	if err != nil {
		t.Fatalf("RecoverFailedNode failed: %v", err)
	}

	// Verify recovery was called with correct info
	if recoveredInfo == nil {
		t.Fatal("recovery handler was not called")
	}
	if recoveredInfo.SlotIndex != failingSlot {
		t.Errorf("SlotIndex = %d, want %d", recoveredInfo.SlotIndex, failingSlot)
	}
	if recoveredInfo.JournalOffset != staleSlot.JournalOffset {
		t.Errorf("JournalOffset = %d, want %d", recoveredInfo.JournalOffset, staleSlot.JournalOffset)
	}

	// Verify slot is now empty
	reg2.mu.Lock()
	clearedSlot, err := reg2.readSlot(failingSlot)
	reg2.mu.Unlock()
	if err != nil {
		t.Fatalf("readSlot failed: %v", err)
	}
	if clearedSlot.State != NodeStateEmpty {
		t.Errorf("slot state after recovery = %v, want Empty", clearedSlot.State)
	}
}

func TestRecoverFailedNodeNoHandler(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)

	err := reg.RecoverFailedNode(0)
	if err == nil {
		t.Error("RecoverFailedNode should fail without handler")
	}
}

func TestRecoverFailedNodeNotFailed(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	ctx := context.Background()

	// Register a node
	slot, err := reg.Register(ctx, "active-node")
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Create second registry
	reg2 := NewRegistry(dev, 0)
	reg2.SetRecoveryHandler(func(info *FailedNodeInfo) error {
		return nil
	})

	// Try to recover an active node (should fail)
	err = reg2.RecoverFailedNode(slot)
	if err == nil {
		t.Error("RecoverFailedNode should fail for active node")
	}
}

func TestFailureDetectionLoop(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	ctx := context.Background()

	// Create both registries
	reg1 := NewRegistry(dev, 0)
	reg2 := NewRegistry(dev, 0)
	reg2.failureCheckInterval = 10 * time.Millisecond

	// Register recovery node FIRST so it gets slot 0
	recoverySlot, err := reg2.Register(ctx, "recovery-node")
	if err != nil {
		t.Fatalf("reg2 Register failed: %v", err)
	}
	t.Logf("recovery node got slot %d", recoverySlot)

	// Register failing node (will get slot 1)
	failingSlot, err := reg1.Register(ctx, "failing-node")
	if err != nil {
		t.Fatalf("reg1 Register failed: %v", err)
	}
	t.Logf("failing node got slot %d", failingSlot)

	// Now write stale heartbeat to the failing node's slot
	staleSlot := &Slot{
		Magic:         SlotMagic,
		State:         NodeStateActive,
		UUID:          reg1.LocalUUID(),
		HeartbeatSeq:  1,
		HeartbeatTime: time.Now().Add(-3 * HeartbeatTimeout).Unix(),
		MountTime:     time.Now().Unix(),
		JournalOffset: 1 << 20,
		JournalSize:   4 << 20,
	}
	copy(staleSlot.Hostname[:], "failing-node")

	reg1.mu.Lock()
	err = reg1.writeSlot(failingSlot, staleSlot)
	reg1.mu.Unlock()
	if err != nil {
		t.Fatalf("writeSlot failed: %v", err)
	}
	silence(reg2, failingSlot, staleSlot, deadAfter+time.Second)

	// Set up recovery handler
	recovered := make(chan *FailedNodeInfo, 1)
	reg2.SetRecoveryHandler(func(info *FailedNodeInfo) error {
		select {
		case recovered <- info:
		default:
		}
		return nil
	})

	// Start failure detection
	reg2.StartFailureDetection()

	// Wait for detection and recovery
	select {
	case info := <-recovered:
		if info.SlotIndex != failingSlot {
			t.Errorf("recovered wrong slot: %d, want %d", info.SlotIndex, failingSlot)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timeout waiting for failure detection")
	}

	// Stop
	if err := reg2.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestMaxNodesMatchesSuperblock(t *testing.T) {
	if MaxNodes != superblock.MaxNodes {
		t.Errorf("node.MaxNodes = %d, superblock.MaxNodes = %d", MaxNodes, superblock.MaxNodes)
	}
}

func TestRegisterRecordsJournalRegion(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	ctx := context.Background()

	for want := 0; want < 3; want++ {
		reg := NewRegistry(dev, 0)
		reg.SetJournalArea(1<<20, 4<<20)
		slot, err := reg.Register(ctx, "node")
		if err != nil {
			t.Fatalf("Register: %v", err)
		}
		if slot != want {
			t.Fatalf("slot = %d, want %d", slot, want)
		}

		offset, size := reg.LocalJournal()
		if wantOffset := uint64(1<<20 + want*4<<20); offset != wantOffset || size != 4<<20 {
			t.Errorf("slot %d journal at %d size %d, want %d size %d", slot, offset, size, wantOffset, 4<<20)
		}
		onDisk, err := reg.readSlot(slot)
		if err != nil {
			t.Fatal(err)
		}
		if onDisk.JournalOffset != offset || onDisk.JournalSize != size {
			t.Errorf("slot %d records journal at %d size %d, want %d size %d",
				slot, onDisk.JournalOffset, onDisk.JournalSize, offset, size)
		}
	}
}

func TestRegisterSkipsSlotUnderRecovery(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	for i, started := range []time.Duration{0, 2 * recoveryLease} {
		dead := &Slot{Magic: SlotMagic, State: NodeStateDead, UUID: [16]byte{byte(9 + i)},
			HeartbeatTime: time.Now().Add(-time.Hour).Unix(), RecoveryOwner: [16]byte{7},
			RecoveryStart: time.Now().Add(-started).Unix()}
		if err := reg.writeSlot(i, dead); err != nil {
			t.Fatal(err)
		}
	}

	slot, err := reg.Register(context.Background(), "node")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if slot != 2 {
		t.Errorf("Register took slot %d, want empty slot 2: a slot under recovery is failure detection's to judge, by watching its lease", slot)
	}
}

func TestRegisterYieldsSlotToRacingWriter(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	own := reg.localUUID
	result := make(chan int, 1)
	go func() {
		slot, err := reg.Register(context.Background(), "slow")
		if err != nil {
			t.Errorf("Register: %v", err)
		}
		result <- slot
	}()

	// Overwrite only once Register's claim is on disk, inside its settle window, as a racing node's write would land.
	for deadline := time.Now().Add(10 * time.Second); ; {
		if slot, err := reg.readSlot(0); err == nil && slot.UUID == own {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Register never claimed slot 0")
		}
		time.Sleep(time.Millisecond)
	}
	racer := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{7}, HeartbeatTime: time.Now().Unix()}
	if err := reg.writeSlot(0, racer); err != nil {
		t.Fatal(err)
	}

	if slot := <-result; slot == 0 {
		t.Error("Register kept slot 0 after another node overwrote the claim")
	}
}

func TestClearFailedSlotKeepsNewOwner(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	failed := [16]byte{1}
	newOwner := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{2}, HeartbeatTime: time.Now().Unix()}
	if err := reg.writeSlot(3, newOwner); err != nil {
		t.Fatal(err)
	}

	if err := reg.clearFailedSlot(3, failed); err != nil {
		t.Fatalf("clearFailedSlot: %v", err)
	}
	slot, err := reg.readSlot(3)
	if err != nil {
		t.Fatal(err)
	}
	if slot.UUID != newOwner.UUID || slot.State != NodeStateActive {
		t.Errorf("slot after clear: state %v uuid %x, want the new owner kept", slot.State, slot.UUID)
	}
}

func TestUnregisterKeepsNewOwner(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	ctx := context.Background()

	reg := NewRegistry(dev, 0)
	slot, err := reg.Register(ctx, "node")
	if err != nil {
		t.Fatal(err)
	}
	newOwner := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{2}, HeartbeatTime: time.Now().Unix()}
	if err := reg.writeSlot(slot, newOwner); err != nil {
		t.Fatal(err)
	}

	if err := reg.Unregister(ctx); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	got, err := reg.readSlot(slot)
	if err != nil {
		t.Fatal(err)
	}
	if got.UUID != newOwner.UUID {
		t.Errorf("Unregister cleared slot %d now owned by %x", slot, newOwner.UUID)
	}
}

func writeLocalSlot(t *testing.T, reg *Registry, idx int, pid int, procStart int64) {
	t.Helper()
	slot := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{byte(idx + 1)},
		HeartbeatTime: time.Now().Unix(), BootID: reg.bootID, PID: uint32(pid), ProcStart: procStart,
		PIDNamespace: reg.pidNS}
	copy(slot.Hostname[:], "node")
	if err := reg.writeSlot(idx, slot); err != nil {
		t.Fatal(err)
	}
}

func exitedPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Skipf("cannot run true: %v", err)
	}
	return cmd.Process.Pid
}

func TestRegisterReclaimsSlotOfExitedLocalProcess(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	if reg.bootID == ([16]byte{}) {
		t.Skip("no kernel boot ID")
	}
	writeLocalSlot(t, reg, 0, os.Getppid(), 1)
	writeLocalSlot(t, reg, 1, os.Getpid(), processStart)
	writeLocalSlot(t, reg, 3, exitedPID(t), 1)

	slot, err := reg.Register(context.Background(), "node")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if slot != 3 {
		t.Errorf("Register took slot %d, want slot 3 of the exited local mount", slot)
	}
}

func TestRecoveryOfExitedLocalProcessSkipsHeartbeatTimeout(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	if reg.bootID == ([16]byte{}) {
		t.Skip("no kernel boot ID")
	}
	if _, err := reg.Register(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	writeLocalSlot(t, reg, 2, exitedPID(t), 1)
	writeLocalSlot(t, reg, 3, os.Getppid(), 1)

	var recovered []int
	reg.SetRecoveryHandler(func(info *FailedNodeInfo) error {
		recovered = append(recovered, info.SlotIndex)
		return nil
	})
	reg.detectAndRecoverFailedNodes()

	if len(recovered) != 1 || recovered[0] != 2 {
		t.Errorf("recovered slots %v, want only slot 2 of the exited local mount", recovered)
	}
}

func TestRecoveryLeaseBlocksOthersUntilItLapses(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	failed := &Slot{Magic: SlotMagic, State: NodeStateDead, UUID: [16]byte{5},
		RecoveryOwner: [16]byte{6}, RecoveryStart: time.Now().Unix()}
	if err := reg.writeSlot(4, failed); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.acquireForRecovery(4, failed, HeartbeatTimeout); !errors.Is(err, ErrSlotTaken) {
		t.Fatalf("acquire during another node's lease: err = %v, want ErrSlotTaken", err)
	}

	silence(reg, 4, failed, recoveryLease+time.Second)
	if _, err := reg.acquireForRecovery(4, failed, HeartbeatTimeout); err != nil {
		t.Fatalf("acquire after the lease lapsed: %v", err)
	}
	slot, err := reg.readSlot(4)
	if err != nil {
		t.Fatal(err)
	}
	if slot.RecoveryOwner != reg.LocalUUID() || time.Since(time.Unix(slot.RecoveryStart, 0)) > time.Minute {
		t.Errorf("lease owner %x started %v, want this node now", slot.RecoveryOwner, time.Unix(slot.RecoveryStart, 0))
	}
}

func TestHeartbeatReportsLostSlot(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	lost := make(chan SlotLoss, 1)
	reg.SetSlotLostHandler(func(loss SlotLoss) { lost <- loss })
	slot, err := reg.Register(context.Background(), "node")
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.StartHeartbeat(); err != nil {
		t.Fatal(err)
	}
	defer reg.Stop()

	if err := reg.writeSlot(slot, &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{8}, HeartbeatTime: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	select {
	case loss := <-lost:
		if loss.Owner != ([16]byte{8}) || !strings.HasSuffix(loss.String(), "(owner 0800000000000000)") {
			t.Errorf("lost slot to %x, saying %q; want owner 08", loss.Owner, loss.String())
		}
	case <-time.After(3 * HeartbeatInterval):
		t.Error("heartbeat did not report the lost slot")
	}
}

func writeOtherNamespaceSlot(t *testing.T, reg *Registry, idx int, heartbeat time.Time) *Slot {
	t.Helper()
	slot := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{byte(idx + 1)},
		HeartbeatTime: heartbeat.Unix(), BootID: reg.bootID, PID: 1, ProcStart: 1,
		PIDNamespace: reg.pidNS + 1}
	copy(slot.Hostname[:], "node")
	if err := reg.writeSlot(idx, slot); err != nil {
		t.Fatal(err)
	}
	return slot
}

func TestRegisterLeavesLiveSlotOfOtherPIDNamespace(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	if reg.bootID == ([16]byte{}) || reg.pidNS == 0 {
		t.Skip("no kernel boot ID or PID namespace")
	}
	sibling := writeOtherNamespaceSlot(t, reg, 0, time.Now())
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(250 * time.Millisecond):
				sibling.HeartbeatSeq++
				sibling.HeartbeatTime = time.Now().Unix()
				if err := reg.writeSlot(0, sibling); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()

	slot, err := reg.Register(context.Background(), "node")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if slot == 0 {
		t.Error("Register took the live slot of a mount in another PID namespace with the same hostname")
	}
}

func TestRegisterTakesOtherPIDNamespaceSlotOnceStale(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	if reg.bootID == ([16]byte{}) || reg.pidNS == 0 {
		t.Skip("no kernel boot ID or PID namespace")
	}
	writeOtherNamespaceSlot(t, reg, 2, time.Now().Add(-HeartbeatTimeout+time.Second))

	start := time.Now()
	slot, err := reg.Register(context.Background(), "node")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if slot != 2 {
		t.Errorf("Register took slot %d, want slot 2 of this host's previous container once its heartbeat went stale", slot)
	}
	t.Logf("waited %v for the slot to go stale", time.Since(start))
}

func TestLeaseCheckRenewsAndDetectsLoss(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	failed := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{5},
		HeartbeatTime: time.Now().Add(-time.Hour).Unix()}
	if err := reg.writeSlot(4, failed); err != nil {
		t.Fatal(err)
	}
	silence(reg, 4, failed, HeartbeatTimeout+time.Second)
	info, err := reg.acquireForRecovery(4, failed, HeartbeatTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if err := info.CheckLease(); err != nil {
		t.Fatalf("CheckLease right after acquire: %v", err)
	}

	// A replayer whose clock is behind still renews its lease to a value peers see change.
	held, _ := reg.readSlot(4)
	held.RecoveryStart = time.Now().Add(time.Hour).Unix()
	if err := reg.writeSlot(4, held); err != nil {
		t.Fatal(err)
	}
	if err := reg.leaseCheck(4, failed.UUID, time.Now().Add(-recoveryLease/2))(); err != nil {
		t.Fatalf("CheckLease halfway through the lease: %v", err)
	}
	if renewed, _ := reg.readSlot(4); renewed.RecoveryStart <= held.RecoveryStart {
		t.Errorf("lease not renewed: start %d, was %d", renewed.RecoveryStart, held.RecoveryStart)
	}
	held, _ = reg.readSlot(4)

	held.RecoveryOwner = [16]byte{6}
	if err := reg.writeSlot(4, held); err != nil {
		t.Fatal(err)
	}
	if err := info.CheckLease(); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("CheckLease after another node took over: err = %v, want ErrLeaseLost", err)
	}
}

func TestRegisterTakesOwnSlotThatGoesStaleWhileWaiting(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	if reg.bootID == ([16]byte{}) || reg.pidNS == 0 {
		t.Skip("no kernel boot ID or PID namespace")
	}
	previous := writeOtherNamespaceSlot(t, reg, 2, time.Now())
	go func() {
		time.Sleep(600 * time.Millisecond)
		previous.HeartbeatTime = time.Now().Add(-2 * HeartbeatTimeout).Unix()
		if err := reg.writeSlot(2, previous); err != nil {
			t.Error(err)
		}
	}()

	slot, err := reg.Register(context.Background(), "node")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if slot != 2 {
		t.Errorf("Register took slot %d, want slot 2, which went stale after the first scan", slot)
	}
}

func TestDetectionReplaysOnlyProvenDeadNodes(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	if reg.bootID == ([16]byte{}) || reg.pidNS == 0 {
		t.Skip("no kernel boot ID or PID namespace")
	}
	if _, err := reg.Register(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	writeLocalSlot(t, reg, 2, exitedPID(t), 1)
	slow := writeLocalSlotAt(t, reg, 3, os.Getppid(), time.Now().Add(-3*HeartbeatTimeout))

	var events []string
	reg.SetRecoveryHandler(func(info *FailedNodeInfo) error {
		events = append(events, fmt.Sprintf("replay %d", info.SlotIndex))
		return nil
	})
	reg.SetOwnerDeadHandler(func(owner [16]byte, _ int) func(bool) {
		events = append(events, fmt.Sprintf("adopt %x", owner[:1]))
		return func(bool) { events = append(events, fmt.Sprintf("release %x", owner[:1])) }
	})
	reg.detectAndRecoverFailedNodes()

	want := []string{"adopt 03", "replay 2", "release 03"}
	if fmt.Sprint(events) != fmt.Sprint(want) {
		t.Errorf("events %v, want %v: only the exited node is replayed, with its locks held through the replay", events, want)
	}
	if slow.UUID != ([16]byte{4}) {
		t.Fatalf("slow node UUID %x", slow.UUID)
	}
}

func TestRegisterReportsItsDeadPreviousIncarnation(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	if reg.bootID == ([16]byte{}) || reg.pidNS == 0 {
		t.Skip("no kernel boot ID or PID namespace")
	}
	writeLocalSlot(t, reg, 0, exitedPID(t), 1)
	if slot, err := reg.Register(context.Background(), "node"); err != nil || slot != 0 {
		t.Fatalf("Register = %d, %v; want slot 0 of the exited mount", slot, err)
	}

	if owner, ok := reg.PreviousOwner(); !ok || owner != ([16]byte{1}) {
		t.Errorf("PreviousOwner = %x, %v; want the previous incarnation 01", owner, ok)
	}
}

func TestSlotLossMessages(t *testing.T) {
	local, other := [16]byte{1, 2, 3}, [16]byte{0xab, 0xcd}
	for _, tc := range []struct {
		loss SlotLoss
		want string
	}{
		{SlotLoss{Slot: 3, Local: local, Stalled: 7277 * time.Millisecond}, "Heartbeats to registry slot 3 failed for 7.277s"},
		{SlotLoss{Slot: 3, Local: local, Taken: true, Owner: other}, "Node registry slot 3 is no longer this node's (owner abcd000000000000)"},
		{SlotLoss{Slot: 3, Local: local, Taken: true, Owner: local}, "Node registry slot 3 is no longer this node's: a peer declared this node dead"},
		{SlotLoss{Slot: 3, Local: local, Taken: true}, "Node registry slot 3 is no longer this node's: the slot is empty"},
	} {
		if got := tc.loss.String(); got != tc.want {
			t.Errorf("%+v: %q, want %q", tc.loss, got, tc.want)
		}
	}
}

func TestStatsCountReplaysAndHeartbeatAge(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	if st := reg.Stats(); st.HeartbeatAge != 0 || st.Replays != 0 {
		t.Fatalf("before registering: %+v, want zeros", st)
	}
	if _, err := reg.Register(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	inReplay := make(chan []int, 1)
	ok := func(*FailedNodeInfo) error { inReplay <- reg.Stats().Replaying; return nil }
	failing := func(*FailedNodeInfo) error { return errors.New("bad record") }
	if err := reg.replay(ok, 7, &FailedNodeInfo{}); err != nil {
		t.Fatal(err)
	}
	if got := <-inReplay; fmt.Sprint(got) != "[7]" {
		t.Errorf("during the replay of slot 7, Replaying = %v", got)
	}
	if err := reg.replay(failing, 7, &FailedNodeInfo{}); err == nil {
		t.Fatal("a failing replay returned nil")
	}
	st := reg.Stats()
	if st.Replays != 1 || st.ReplayFailures != 1 || len(st.Replaying) != 0 {
		t.Errorf("after one replay and one failure: %+v", st)
	}
	if st.HeartbeatAge <= 0 || st.HeartbeatAge > HeartbeatTimeout {
		t.Errorf("heartbeat age right after registering = %v", st.HeartbeatAge)
	}
}

func TestPeerReplayBacksOffAfterIdenticalFailures(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	if _, err := reg.Register(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	const dead = 4
	deadNode := func(uuid byte) {
		t.Helper()
		if err := reg.writeSlot(dead, &Slot{Magic: SlotMagic, State: NodeStateDead, UUID: [16]byte{uuid}, RecoveryOwner: [16]byte{9}, RecoveryStart: 1}); err != nil {
			t.Fatal(err)
		}
	}
	deadNode(5)
	var replayErr error
	tries := 0
	reg.SetRecoveryHandler(func(*FailedNodeInfo) error { tries++; return replayErr })
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)

	// scan runs failure detection after this node has seen the dead slot unchanged for quiet.
	scan := func(quiet time.Duration) (tried bool, logged string) {
		t.Helper()
		slot, err := reg.readSlot(dead)
		if err != nil {
			t.Fatal(err)
		}
		silence(reg, dead, slot, quiet)
		before := tries
		logs.Reset()
		reg.detectAndRecoverFailedNodes()
		return tries > before, logs.String()
	}
	lapsed := recoveryLease + time.Second
	failing := func() string { return fmt.Sprint(reg.FailingReplays()) }

	for k, err := range []error{fmt.Errorf("%w: inode 9", ErrReplayContended), ErrLeaseLost, fmt.Errorf("%w: inode 9", ErrReplayContended), ErrLeaseLost} {
		replayErr = err
		if tried, _ := scan(lapsed); !tried {
			t.Fatalf("after %d lock timeouts and lost leases, no retry once the lease lapsed", k)
		}
	}
	if got := failing(); got != "[]" {
		t.Fatalf("lock timeouts and lost leases made the slot failing: %s", got)
	}

	replayErr = errors.New("bad record")
	var logged string
	for k := 1; k <= replayRepeats; k++ {
		var tried bool
		if tried, logged = scan(lapsed); !tried || !strings.Contains(logged, "bad record") {
			t.Fatalf("failure %d: tried %v, logged %q", k, tried, logged)
		}
	}
	if !strings.Contains(logged, "3 times in a row") || !strings.Contains(logged, "-inspect-slot 4") {
		t.Errorf("third identical failure logged %q", logged)
	}
	if got := failing(); got != "[{4 3 bad record}]" {
		t.Errorf("FailingReplays after 3 identical failures = %s", got)
	}
	if tried, _ := scan(ReplayBackoff - time.Minute); tried {
		t.Fatal("retried within the backoff")
	}
	if tried, logged := scan(ReplayBackoff + time.Second); !tried || logged != "" {
		t.Fatalf("after the backoff: tried %v, logged %q; want a retry that logs nothing within the hour", tried, logged)
	}
	reg.mu.Lock()
	reg.failures[dead].logged = time.Now().Add(-failureLogEvery)
	reg.mu.Unlock()
	if _, logged := scan(ReplayBackoff + time.Second); !strings.Contains(logged, "5 times in a row") {
		t.Errorf("an hour after the last log, the failure logged %q", logged)
	}

	replayErr = errors.New("short read")
	if tried, logged := scan(ReplayBackoff + time.Second); !tried || !strings.Contains(logged, "any node retries after") {
		t.Fatalf("a different error: tried %v, logged %q", tried, logged)
	}
	if got := failing(); got != "[]" {
		t.Errorf("a different error kept the slot failing: %s", got)
	}
	if tried, _ := scan(lapsed); !tried {
		t.Fatal("a different error kept the backoff")
	}
	replayErr = nil
	if tried, _ := scan(lapsed); !tried || len(reg.failures) != 0 {
		t.Fatalf("successful replay: tried %v, failures %v", tried, reg.failures)
	}

	deadNode(6)
	replayErr = errors.New("bad record")
	for range replayRepeats {
		scan(lapsed)
	}
	if got := failing(); got != "[{4 3 bad record}]" {
		t.Fatalf("FailingReplays for the next dead node = %s", got)
	}
	if err := reg.writeSlot(dead, &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{7}, HeartbeatTime: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	if tried, _ := scan(0); tried || failing() != "[]" {
		t.Errorf("a new node in the slot: tried %v, FailingReplays = %s", tried, failing())
	}
}

func TestHeartbeatStopsWhenWritesFail(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	lost := make(chan SlotLoss, 1)
	reg.SetSlotLostHandler(func(loss SlotLoss) { lost <- loss })
	if _, err := reg.Register(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	if err := reg.StartHeartbeat(); err != nil {
		t.Fatal(err)
	}
	defer reg.Stop()

	if err := dev.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case loss := <-lost:
		if msg := loss.String(); !strings.HasPrefix(msg, "Heartbeats to registry slot") || strings.Contains(msg, "owner") {
			t.Errorf("after failed heartbeats the node says %q, want the stall without an owner", msg)
		}
	case <-time.After(HeartbeatTimeout):
		t.Error("node kept running while its heartbeats failed; peers would prove it dead and take its locks")
	}
}

func TestHeartbeatStopsWhenSlotIsMarkedDead(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	lost := make(chan SlotLoss, 1)
	reg.SetSlotLostHandler(func(loss SlotLoss) { lost <- loss })
	slot, err := reg.Register(context.Background(), "node")
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.StartHeartbeat(); err != nil {
		t.Fatal(err)
	}
	defer reg.Stop()

	own, err := reg.readSlot(slot)
	if err != nil {
		t.Fatal(err)
	}
	own.State = NodeStateDead
	if err := reg.writeSlot(slot, own); err != nil {
		t.Fatal(err)
	}
	select {
	case loss := <-lost:
		if msg := loss.String(); !strings.HasSuffix(msg, "a peer declared this node dead") {
			t.Errorf("in a slot marked dead the node says %q", msg)
		}
	case <-time.After(3 * HeartbeatInterval):
		t.Error("heartbeat kept running in a slot another node marked dead")
	}
}

// silence makes reg treat slot as read unchanged for d, as if it had watched the slot that long.
func silence(reg *Registry, idx int, slot *Slot, d time.Duration) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	seen := sightingOf(slot)
	seen.since = time.Now().Add(-d)
	reg.sightings[idx] = seen
}

func TestOwnClockDecidesDeathNotWallClock(t *testing.T) {
	for _, beat := range []time.Duration{-time.Hour, 3 * time.Minute} {
		reg := &Registry{sightings: make(map[int]sighting)}
		slot := &Slot{UUID: [16]byte{4}, State: NodeStateActive, HeartbeatSeq: 1, HeartbeatTime: time.Now().Add(beat).Unix()}

		reg.observe(1, slot)
		if reg.stale(1, slot, deadAfter) {
			t.Errorf("heartbeat %v from now: first seen just now, it counted as dead", beat)
		}
		silence(reg, 1, slot, deadAfter+time.Second)
		if !reg.stale(1, slot, deadAfter) {
			t.Errorf("heartbeat %v from now: unchanged past deadAfter by this node's clock, it did not count as dead", beat)
		}
		slot.HeartbeatSeq++
		reg.observe(1, slot)
		if reg.stale(1, slot, deadAfter) {
			t.Errorf("heartbeat %v from now: a heartbeat that advanced still counted as dead", beat)
		}
	}
}

func writeLocalSlotAt(t *testing.T, reg *Registry, idx int, pid int, heartbeat time.Time) *Slot {
	t.Helper()
	slot := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{byte(idx + 1)},
		HeartbeatTime: heartbeat.Unix(), BootID: reg.bootID, PID: uint32(pid), ProcStart: 1,
		PIDNamespace: reg.pidNS}
	copy(slot.Hostname[:], "node")
	if err := reg.writeSlot(idx, slot); err != nil {
		t.Fatal(err)
	}
	return slot
}

func TestReleaseSlotKeepsOrphans(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	orphans := []Orphan{{Inode: 12, Birth: 3}, {Inode: 40, Birth: 9}}
	gone := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{5}, HeartbeatTime: time.Now().Add(-time.Hour).Unix(), Orphans: orphans}
	if err := reg.writeSlot(2, gone); err != nil {
		t.Fatal(err)
	}
	raw, _, err := reg.readSlotRaw(2)
	if err != nil {
		t.Fatal(err)
	}
	if released, err := reg.ReleaseSlot(2, raw, func(func() error) error { return nil }); !released || err != nil {
		t.Fatalf("ReleaseSlot = %v, %v", released, err)
	}
	slot, err := reg.readSlot(2)
	if err != nil || slot.State != NodeStateEmpty || !slices.Equal(slot.Orphans, orphans) {
		t.Fatalf("released slot: %+v, %v; want empty with orphans %v", slot, err, orphans)
	}
	if all, err := reg.AllOrphans(); err != nil || !slices.Equal(all, orphans) {
		t.Errorf("AllOrphans after the release = %v, %v; want %v, so live nodes still free them", all, err, orphans)
	}
}

func TestReleaseSlotGivesUpOnSlowReset(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	defer func(old time.Duration) { releaseLeaseBudget = old }(releaseLeaseBudget)
	releaseLeaseBudget = claimSettle

	reg := NewRegistry(dev, 0)
	gone := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{5}, HeartbeatTime: time.Now().Add(-time.Hour).Unix()}
	if err := reg.writeSlot(0, gone); err != nil {
		t.Fatal(err)
	}
	raw, _, err := reg.readSlotRaw(0)
	if err != nil {
		t.Fatal(err)
	}
	released, err := reg.ReleaseSlot(0, raw, func(func() error) error {
		time.Sleep(2 * claimSettle)
		return nil
	})
	if released || err == nil {
		t.Fatalf("ReleaseSlot after a slow reset = %v, %v; want an error", released, err)
	}
	if slot, err := reg.readSlot(0); err != nil || slot.State != NodeStateDead || slot.RecoveryOwner != reg.localUUID {
		t.Errorf("slot after the abandoned release: %+v, %v; want dead under this registry's lease", slot, err)
	}
}

func TestReleaseSlotRenewsItsLease(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	t.Cleanup(SetRecoveryLease(8 * claimSettle))
	defer func(old time.Duration) { releaseLeaseBudget = old }(releaseLeaseBudget)
	releaseLeaseBudget = 2 * claimSettle

	reg := NewRegistry(dev, 0)
	orphans := []Orphan{{Inode: 12, Birth: 3}}
	gone := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{5}, HeartbeatTime: time.Now().Add(-time.Hour).Unix(), Orphans: orphans}
	for _, idx := range []int{0, 1} {
		if err := reg.writeSlot(idx, gone); err != nil {
			t.Fatal(err)
		}
	}
	raw, _, err := reg.readSlotRaw(0)
	if err != nil {
		t.Fatal(err)
	}

	var starts []int64
	released, err := reg.ReleaseSlot(0, raw, func(renew func() error) error {
		for range 6 {
			time.Sleep(claimSettle)
			if err := renew(); err != nil {
				return err
			}
			slot, err := reg.readSlot(0)
			if err != nil {
				return err
			}
			starts = append(starts, slot.RecoveryStart)
		}
		return nil
	})
	if !released || err != nil {
		t.Fatalf("ReleaseSlot with a reset that renews for %v, past the %v budget = %v, %v", 6*claimSettle, releaseLeaseBudget, released, err)
	}
	if starts[0] == starts[len(starts)-1] {
		t.Errorf("the lease start never moved during the reset: %v", starts)
	}
	if slot, err := reg.readSlot(0); err != nil || slot.State != NodeStateEmpty || !slices.Equal(slot.Orphans, orphans) {
		t.Errorf("released slot: %+v, %v; want empty with orphans %v", slot, err, orphans)
	}

	raw, _, err = reg.readSlotRaw(1)
	if err != nil {
		t.Fatal(err)
	}
	released, err = reg.ReleaseSlot(1, raw, func(renew func() error) error {
		taken, err := reg.readSlot(1)
		if err != nil {
			return err
		}
		taken.RecoveryOwner = [16]byte{9}
		if err := reg.writeSlot(1, taken); err != nil {
			return err
		}
		return renew()
	})
	if released || !errors.Is(err, ErrLeaseLost) {
		t.Errorf("ReleaseSlot after another node took the lease = %v, %v; want ErrLeaseLost", released, err)
	}
}

func TestRunningSlots(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	running := NewRegistry(dev, 0)
	runningSlot, err := running.Register(context.Background(), "running")
	if err != nil {
		t.Fatal(err)
	}
	if err := running.StartHeartbeat(); err != nil {
		t.Fatal(err)
	}
	defer running.Stop()
	if _, err := NewRegistry(dev, 0).Register(context.Background(), "stopped"); err != nil {
		t.Fatal(err)
	}

	got, err := NewRegistry(dev, 0).RunningSlots(2 * HeartbeatInterval)
	if err != nil || len(got) != 1 || !got[runningSlot] {
		t.Errorf("RunningSlots = %v, %v; want only the heartbeating slot %d", got, err, runningSlot)
	}
}

func TestOccupantReportsHowSlotStands(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	now := time.Now()
	slots := []*Slot{
		{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{1}, HeartbeatTime: now.Unix()},
		{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{2}, HeartbeatTime: now.Add(-3 * HeartbeatTimeout).Unix()},
		{Magic: SlotMagic, State: NodeStateDead, UUID: [16]byte{3}, RecoveryStart: now.Unix()},
		{Magic: SlotMagic, State: NodeStateDead, UUID: [16]byte{4}, RecoveryStart: now.Add(-2 * recoveryLease).Unix()},
		{Magic: SlotMagic, State: NodeStateEmpty},
	}
	want := []Occupancy{OccupiedLive, OccupiedFailing, OccupiedReplaying, OccupiedLapsed, Unoccupied}
	unchanged := map[int]time.Duration{1: HeartbeatTimeout + time.Second, 3: recoveryLease + time.Second}
	for i, slot := range slots {
		if err := reg.writeSlot(i, slot); err != nil {
			t.Fatal(err)
		}
		if d, ok := unchanged[i]; ok {
			silence(reg, i, slot, d)
		}
		if uuid, got := reg.Occupant(i, true); got != want[i] || uuid != slot.UUID {
			t.Errorf("slot %d: Occupant = %x, %d; want %x, %d", i, uuid[:1], got, slot.UUID[:1], want[i])
		}
	}
}

func TestOccupantAnswersFromLastReadUnlessFresh(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	if _, got := reg.Occupant(0, false); got != Unoccupied {
		t.Fatalf("empty slot: Occupant = %d", got)
	}
	if err := reg.writeSlot(0, &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{1}, HeartbeatTime: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	if _, got := reg.Occupant(0, false); got != Unoccupied {
		t.Errorf("cached read: Occupant = %d, want the earlier read's %d", got, Unoccupied)
	}
	if _, got := reg.Occupant(0, true); got != OccupiedLive {
		t.Errorf("fresh read: Occupant = %d, want %d", got, OccupiedLive)
	}
}

// fillWithReplays marks every slot from first on as dead and under another node's recovery, so none is empty.
func fillWithReplays(t *testing.T, reg *Registry, first int) {
	t.Helper()
	for i := first; i < MaxNodes; i++ {
		dead := &Slot{Magic: SlotMagic, State: NodeStateDead, UUID: [16]byte{byte(100 + i)}, RecoveryOwner: [16]byte{7}, RecoveryStart: time.Now().Unix()}
		if err := reg.writeSlot(i, dead); err != nil {
			t.Fatal(err)
		}
	}
}

func shortenSilentSlotWatch(t *testing.T) {
	t.Helper()
	old := silentSlotWatch
	t.Cleanup(func() { silentSlotWatch = old })
	silentSlotWatch = 2 * time.Second
}

// writeGoneSlot writes slot 0 as another host's node whose heartbeat stopped long ago by this clock.
func writeGoneSlot(t *testing.T, reg *Registry) *Slot {
	t.Helper()
	gone := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{7}, HeartbeatSeq: 3, HeartbeatTime: time.Now().Add(-3 * HeartbeatTimeout).Unix()}
	copy(gone.Hostname[:], "elsewhere")
	if err := reg.writeSlot(0, gone); err != nil {
		t.Fatal(err)
	}
	return gone
}

func TestRegisterTakesEmptySlotBeforeAnotherHostsSilentOne(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	writeGoneSlot(t, reg)
	if slot, err := reg.Register(context.Background(), "node"); err != nil || slot != 1 {
		t.Fatalf("Register = %d, %v; want the empty slot 1", slot, err)
	}
	if owner, ok := reg.PreviousOwner(); ok {
		t.Errorf("PreviousOwner = %x after taking an empty slot, want none", owner)
	}
}

// A host that rebooted after a crash takes back the slot it held under the same
// hostname once the slot proves silent, so its mount replays that journal. A
// live host that shares the name beats during the watch and keeps its slot.
func TestRegisterTakesBackSlotOfItsEarlierBoot(t *testing.T) {
	for _, beating := range []bool{false, true} {
		t.Run(map[bool]string{false: "silent", true: "another host with the name"}[beating], func(t *testing.T) {
			shortenSilentSlotWatch(t)
			dev, cleanup := createTestDevice(t, 10<<20)
			defer cleanup()
			reg := NewRegistry(dev, 0)
			earlier := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{7}, HeartbeatSeq: 3,
				HeartbeatTime: time.Now().Add(-3 * HeartbeatTimeout).Unix(), BootID: [16]byte{0xEE}}
			copy(earlier.Hostname[:], "node")
			if err := reg.writeSlot(0, earlier); err != nil {
				t.Fatal(err)
			}
			if beating {
				defer keepWriting(t, reg, 0, earlier, 3*HeartbeatTimeout)()
			}
			slot, err := reg.Register(context.Background(), "node")
			if err != nil {
				t.Fatal(err)
			}
			owner, ok := reg.PreviousOwner()
			switch {
			case beating && slot == 0:
				t.Error("Register took the slot of a live host that shares this hostname")
			case !beating && (slot != 0 || !ok || owner != earlier.UUID):
				t.Errorf("Register = %d, previous owner %x %v; want slot 0 of this hostname's earlier boot, node %x", slot, owner, ok, earlier.UUID)
			}
		})
	}
}

func TestRegisterTakesAnotherHostsSilentSlotWhenNamedOrNoneIsEmpty(t *testing.T) {
	for _, named := range []bool{true, false} {
		t.Run(map[bool]string{true: "named", false: "none empty"}[named], func(t *testing.T) {
			shortenSilentSlotWatch(t)
			dev, cleanup := createTestDevice(t, 10<<20)
			defer cleanup()
			reg := NewRegistry(dev, 0)
			gone := writeGoneSlot(t, reg)
			if named {
				reg.PreferSlot(0)
			} else {
				fillWithReplays(t, reg, 1)
			}
			if slot, err := reg.Register(context.Background(), "node"); err != nil || slot != 0 {
				t.Fatalf("Register = %d, %v; want slot 0, whose heartbeat never changed", slot, err)
			}
			if owner, ok := reg.PreviousOwner(); !ok || owner != gone.UUID {
				t.Errorf("PreviousOwner = %x, %v; want the failed node %x of another host", owner, ok, gone.UUID)
			}
		})
	}
}

// A named slot under another node's recovery is taken once its lease lapses
// unrenewed, and left alone while the lease is renewed.
func TestRegisterTakesNamedSlotUnderRecoveryOnlyOnceItsLeaseLapses(t *testing.T) {
	for _, renewed := range []bool{false, true} {
		t.Run(map[bool]string{false: "lapsed", true: "renewed"}[renewed], func(t *testing.T) {
			t.Cleanup(SetRecoveryLease(2 * time.Second))
			dev, cleanup := createTestDevice(t, 10<<20)
			defer cleanup()
			reg := NewRegistry(dev, 0)
			dead := &Slot{Magic: SlotMagic, State: NodeStateDead, UUID: [16]byte{5}, RecoveryOwner: [16]byte{6}, RecoveryStart: time.Now().Unix()}
			if err := reg.writeSlot(0, dead); err != nil {
				t.Fatal(err)
			}
			if renewed {
				defer keepWriting(t, reg, 0, dead, 0)()
			}
			reg.PreferSlot(0)
			slot, err := reg.Register(context.Background(), "node")
			if err != nil {
				t.Fatal(err)
			}
			if took := slot == 0; took == renewed {
				t.Errorf("Register took slot %d; want slot 0 only when its lease was not renewed", slot)
			}
		})
	}
}

// A slot that changes after Register watched it, because a peer marked it dead
// to replay it or its node beat again, is not taken.
func TestRegisterLeavesWatchedSlotThatChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Slot)
	}{
		{"marked dead by a peer", func(s *Slot) {
			s.State, s.RecoveryOwner, s.RecoveryStart = NodeStateDead, [16]byte{8}, time.Now().Unix()
		}},
		{"its node beat again", func(s *Slot) { s.HeartbeatSeq++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shortenSilentSlotWatch(t)
			dev, cleanup := createTestDevice(t, 10<<20)
			defer cleanup()
			reg := NewRegistry(dev, 0)
			gone := writeGoneSlot(t, reg)
			reg.PreferSlot(0)
			t.Cleanup(func() { afterSilentWatch = nil })
			afterSilentWatch = func() {
				tc.change(gone)
				if err := reg.writeSlot(0, gone); err != nil {
					t.Error(err)
				}
			}
			if slot, err := reg.Register(context.Background(), "node"); err != nil || slot == 0 {
				t.Fatalf("Register = %d, %v; want an empty slot, not slot 0, which changed after the watch", slot, err)
			}
		})
	}
}

func TestFailedReplayKeepsDeadNodesLocks(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	if reg.bootID == ([16]byte{}) || reg.pidNS == 0 {
		t.Skip("no kernel boot ID or PID namespace")
	}
	if _, err := reg.Register(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	writeLocalSlot(t, reg, 2, exitedPID(t), 1)

	var replayed []bool
	reg.SetRecoveryHandler(func(*FailedNodeInfo) error { return errors.New("journal replay failed") })
	reg.SetOwnerDeadHandler(func([16]byte, int) func(bool) {
		return func(ok bool) { replayed = append(replayed, ok) }
	})
	reg.detectAndRecoverFailedNodes()

	if fmt.Sprint(replayed) != "[false]" {
		t.Fatalf("lock handler told %v, want one failed replay", replayed)
	}
	slot, err := reg.readSlot(2)
	if err != nil {
		t.Fatal(err)
	}
	if slot.State != NodeStateDead || slot.UUID != ([16]byte{3}) {
		t.Errorf("after a failed replay slot 2 is %s with node %x; want it still dead with the failed node", slot.State, slot.UUID[:1])
	}
}

func TestCheckOwnershipWritesHeartbeatAfterPause(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	slot, err := reg.Register(context.Background(), "node")
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.CheckOwnership(); err != nil {
		t.Fatalf("right after Register: %v", err)
	}

	paused := time.Now().Add(-HeartbeatTimeout)
	reg.lastBeat.Store(&paused)
	before, err := reg.readSlot(slot)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	log.SetOutput(&logs)
	err = reg.CheckOwnership()
	log.SetOutput(os.Stderr)
	if err != nil {
		t.Fatalf("slot still ours after a pause: %v", err)
	}
	if !strings.Contains(logs.String(), "checking the slot before a write") {
		t.Errorf("the check after a pause logged nothing:\n%s", logs.String())
	}
	after, err := reg.readSlot(slot)
	if err != nil {
		t.Fatal(err)
	}
	if after.HeartbeatSeq <= before.HeartbeatSeq {
		t.Errorf("heartbeat %d after the check, want it past %d", after.HeartbeatSeq, before.HeartbeatSeq)
	}
}

func TestCheckOwnershipFailsOnceSlotIsTakenOver(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	slot, err := reg.Register(context.Background(), "node")
	if err != nil {
		t.Fatal(err)
	}
	var lost []string
	reg.SetSlotLostHandler(func(loss SlotLoss) { lost = append(lost, fmt.Sprintf("%d %x", loss.Slot, loss.Owner[:1])) })

	if err := reg.writeSlot(slot, &Slot{Magic: SlotMagic, State: NodeStateDead, UUID: reg.LocalUUID(), RecoveryOwner: [16]byte{9}, RecoveryStart: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	if err := reg.CheckOwnership(); err != nil {
		t.Fatalf("with a recent heartbeat the check needs no read: %v", err)
	}
	paused := time.Now().Add(-HeartbeatTimeout)
	reg.lastBeat.Store(&paused)
	for range 2 {
		if err := reg.CheckOwnership(); !errors.Is(err, ErrSlotLost) {
			t.Fatalf("after peers marked the slot dead: %v, want ErrSlotLost", err)
		}
	}
	uuid := reg.LocalUUID()
	if want := []string{fmt.Sprintf("%d %x", slot, uuid[:1])}; fmt.Sprint(lost) != fmt.Sprint(want) {
		t.Errorf("slot-lost handler calls %v, want %v", lost, want)
	}
}

func TestWriteGateRefusesWritesOnceSlotIsLost(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	slot, err := reg.Register(context.Background(), "node")
	if err != nil {
		t.Fatal(err)
	}
	dev.SetWriteGate(reg.WriteGate)
	data := storage.NewAlignedBuffer(storage.SectorSize)
	copy(data.Bytes(), bytes.Repeat([]byte{'x'}, storage.SectorSize))
	if _, err := dev.WriteAt(data.Bytes(), 1<<20); err != nil {
		t.Fatalf("write while the slot is ours: %v", err)
	}

	if err := reg.writeSlot(slot, &Slot{Magic: SlotMagic, State: NodeStateDead, UUID: reg.LocalUUID(), RecoveryOwner: [16]byte{9}, RecoveryStart: time.Now().Unix()}); err != nil {
		t.Fatalf("a registry write must pass the gate: %v", err)
	}
	paused := time.Now().Add(-HeartbeatTimeout)
	reg.lastBeat.Store(&paused)
	if _, err := dev.WriteAt(data.Bytes(), 2<<20); !errors.Is(err, ErrSlotLost) {
		t.Fatalf("write after peers marked the slot dead: %v, want ErrSlotLost", err)
	}
	if _, err := dev.ReadAt(data.Bytes(), 2<<20); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data.Bytes(), make([]byte, storage.SectorSize)) {
		t.Error("the refused write reached the device")
	}
}
