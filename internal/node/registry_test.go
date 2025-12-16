package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
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
		f.Close()
		os.Remove(testFile)
		t.Skipf("Cannot truncate test file: %v", err)
	}
	f.Close()

	dev, err := storage.Open(testFile)
	if err != nil {
		os.Remove(testFile)
		t.Skipf("O_DIRECT not supported: %v", err)
	}

	cleanup := func() {
		dev.Close()
		os.Remove(testFile)
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
	reg1.Unregister(ctx)
	reg2.Unregister(ctx)
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

	reg.Unregister(ctx)
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
