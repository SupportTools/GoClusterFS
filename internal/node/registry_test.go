package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

func TestIsHeartbeatStale(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)

	// Fresh heartbeat - not stale
	freshSlot := &Slot{
		HeartbeatTime: time.Now().Unix(),
	}
	if reg.isHeartbeatStale(freshSlot) {
		t.Error("fresh heartbeat should not be stale")
	}

	// Old heartbeat - stale
	staleSlot := &Slot{
		HeartbeatTime: time.Now().Add(-HeartbeatTimeout - time.Second).Unix(),
	}
	if !reg.isHeartbeatStale(staleSlot) {
		t.Error("old heartbeat should be stale")
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
	if slot != 1 {
		t.Errorf("Register took slot %d, want slot 1, whose recovery lease lapsed", slot)
	}
}

func TestRegisterYieldsSlotToRacingWriter(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	result := make(chan int, 1)
	go func() {
		slot, err := reg.Register(context.Background(), "slow")
		if err != nil {
			t.Errorf("Register: %v", err)
		}
		result <- slot
	}()

	time.Sleep(claimSettle / 3)
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

	failed.RecoveryStart = time.Now().Add(-2 * recoveryLease).Unix()
	if err := reg.writeSlot(4, failed); err != nil {
		t.Fatal(err)
	}
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
	lost := make(chan [16]byte, 1)
	reg.SetSlotLostHandler(func(slot int, owner [16]byte) { lost <- owner })
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
	case owner := <-lost:
		if owner != ([16]byte{8}) {
			t.Errorf("lost slot to %x, want 08", owner)
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
	info, err := reg.acquireForRecovery(4, failed, HeartbeatTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if err := info.CheckLease(); err != nil {
		t.Fatalf("CheckLease right after acquire: %v", err)
	}

	held, _ := reg.readSlot(4)
	held.RecoveryStart = time.Now().Add(-recoveryLease / 2).Unix()
	if err := reg.writeSlot(4, held); err != nil {
		t.Fatal(err)
	}
	if err := info.CheckLease(); err != nil {
		t.Fatalf("CheckLease halfway through the lease: %v", err)
	}
	if renewed, _ := reg.readSlot(4); time.Since(time.Unix(renewed.RecoveryStart, 0)) > 5*time.Second {
		t.Errorf("lease not renewed: started %v", time.Unix(renewed.RecoveryStart, 0))
	}

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

func TestDetectionReleasesLocksOnlyOfProvenDeadNodes(t *testing.T) {
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
	reg.SetOwnerDeadHandler(func(owner [16]byte) func() {
		events = append(events, fmt.Sprintf("adopt %x", owner[:1]))
		return func() { events = append(events, fmt.Sprintf("release %x", owner[:1])) }
	})
	reg.detectAndRecoverFailedNodes()

	want := []string{"adopt 03", "replay 2", "release 03", "replay 3"}
	if fmt.Sprint(events) != fmt.Sprint(want) {
		t.Errorf("events %v, want %v: only the exited node's locks are taken, and held through its replay", events, want)
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

	if owner, ok := reg.DeadPreviousOwner(); !ok || owner != ([16]byte{1}) {
		t.Errorf("DeadPreviousOwner = %x, %v; want the previous incarnation 01", owner, ok)
	}
}

func TestHeartbeatStopsWhenWritesFail(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	lost := make(chan struct{}, 1)
	reg.SetSlotLostHandler(func(int, [16]byte) { lost <- struct{}{} })
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
	case <-lost:
	case <-time.After(HeartbeatTimeout):
		t.Error("node kept running while its heartbeats failed; peers would prove it dead and take its locks")
	}
}

func TestHeartbeatStopsWhenSlotIsMarkedDead(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	reg := NewRegistry(dev, 0)
	lost := make(chan struct{}, 1)
	reg.SetSlotLostHandler(func(int, [16]byte) { lost <- struct{}{} })
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
	case <-lost:
	case <-time.After(3 * HeartbeatInterval):
		t.Error("heartbeat kept running in a slot another node marked dead")
	}
}

func TestOwnClockDecidesDeathNotWallClock(t *testing.T) {
	reg := &Registry{sightings: make(map[int]sighting)}
	slot := &Slot{UUID: [16]byte{4}, State: NodeStateActive, HeartbeatSeq: 1,
		HeartbeatTime: time.Now().Add(-time.Hour).Unix()}

	reg.observe(1, slot)
	if reg.provenDead(1, slot) {
		t.Error("a heartbeat first seen just now counted as dead because its wall-clock time is old")
	}

	reg.sightings[1] = sighting{uuid: slot.UUID, seq: 1, beat: slot.HeartbeatTime,
		since: time.Now().Add(-(HeartbeatTimeout + deathMargin + time.Second))}
	if !reg.provenDead(1, slot) {
		t.Error("a heartbeat unchanged past timeout plus margin by this node's clock did not count as dead")
	}

	slot.HeartbeatSeq++
	reg.observe(1, slot)
	if reg.provenDead(1, slot) || !reg.recentlyAlive(1) {
		t.Error("a heartbeat that advanced still counted as dead")
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
	released, err := reg.ReleaseSlot(0, raw, func() error {
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
