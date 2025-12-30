package lock

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func TestRecordSerializeDeserialize(t *testing.T) {
	record := &Record{
		Magic:       RecordMagic,
		Type:        LockTypeInode,
		Mode:        LockModeExclusive,
		ResourceID:  12345,
		OwnerSlot:   5,
		AcquireTime: time.Now().Unix(),
		Sequence:    42,
		SharedCount: 0,
	}
	copy(record.OwnerUUID[:], []byte("0123456789abcdef"))

	// Serialize
	data := serializeRecord(record)

	if len(data) != RecordSize {
		t.Fatalf("serialized size = %d, want %d", len(data), RecordSize)
	}

	// Deserialize
	restored, err := deserializeRecord(data)
	if err != nil {
		t.Fatalf("deserializeRecord failed: %v", err)
	}

	// Verify fields
	if restored.Magic != record.Magic {
		t.Errorf("Magic = %x, want %x", restored.Magic, record.Magic)
	}
	if restored.Type != record.Type {
		t.Errorf("Type = %d, want %d", restored.Type, record.Type)
	}
	if restored.Mode != record.Mode {
		t.Errorf("Mode = %d, want %d", restored.Mode, record.Mode)
	}
	if restored.ResourceID != record.ResourceID {
		t.Errorf("ResourceID = %d, want %d", restored.ResourceID, record.ResourceID)
	}
	if restored.Sequence != record.Sequence {
		t.Errorf("Sequence = %d, want %d", restored.Sequence, record.Sequence)
	}
	if restored.OwnerUUID != record.OwnerUUID {
		t.Error("OwnerUUID mismatch")
	}
}

func TestRecordDeserializeEmpty(t *testing.T) {
	// Empty (zero) record should deserialize as Mode=None
	data := make([]byte, RecordSize)
	record, err := deserializeRecord(data)
	if err != nil {
		t.Fatalf("deserializeRecord empty failed: %v", err)
	}
	if record.Mode != LockModeNone {
		t.Errorf("empty record Mode = %d, want %d", record.Mode, LockModeNone)
	}
}

func TestRecordDeserializeInvalid(t *testing.T) {
	// Buffer too small
	_, err := deserializeRecord(make([]byte, 100))
	if err == nil {
		t.Error("expected error for small buffer")
	}

	// Corrupted checksum (non-zero magic)
	record := &Record{Magic: RecordMagic, Type: LockTypeInode, Mode: LockModeExclusive}
	data := serializeRecord(record)
	data[100] ^= 0xFF // Corrupt data
	_, err = deserializeRecord(data)
	if err == nil {
		t.Error("expected error for corrupted checksum")
	}
}

func TestLockTypes(t *testing.T) {
	// Verify lock types are in hierarchy order
	types := []LockType{
		LockTypeVolume,
		LockTypeSnapshotReg,
		LockTypeSnapshot,
		LockTypeJournal,
		LockTypeRefCount,
		LockTypeLockRegion,
		LockTypeBlockBitmap,
		LockTypeSFBContainer,
		LockTypeInode,
		LockTypeExtent,
	}

	for i := 1; i < len(types); i++ {
		if types[i] <= types[i-1] {
			t.Errorf("LockType %d should be > %d", types[i], types[i-1])
		}
	}
}

func TestLockModes(t *testing.T) {
	if LockModeNone != 0 {
		t.Error("LockModeNone should be 0")
	}
	if LockModeShared != 1 {
		t.Error("LockModeShared should be 1")
	}
	if LockModeExclusive != 2 {
		t.Error("LockModeExclusive should be 2")
	}
}

func TestRecordSize(t *testing.T) {
	// RecordSize should match SectorSize for O_DIRECT alignment
	if RecordSize != storage.SectorSize {
		t.Errorf("RecordSize = %d, should match storage.SectorSize = %d", RecordSize, storage.SectorSize)
	}
}

// createTestDevice creates a test file for O_DIRECT testing.
// Uses testing.TB interface to support both tests and benchmarks.
func createTestDevice(tb testing.TB, size int64) (*storage.Device, func()) {
	tb.Helper()

	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_locks.bin")

	f, err := os.Create(testFile)
	if err != nil {
		tb.Skipf("Cannot create test file: %v", err)
	}

	if err := f.Truncate(size); err != nil {
		f.Close()
		os.Remove(testFile)
		tb.Skipf("Cannot truncate test file: %v", err)
	}
	f.Close()

	dev, err := storage.Open(testFile)
	if err != nil {
		os.Remove(testFile)
		tb.Skipf("O_DIRECT not supported: %v", err)
	}

	cleanup := func() {
		dev.Close()
		os.Remove(testFile)
	}

	return dev, cleanup
}

func TestManagerCreate(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20) // 10MB
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	if mgr == nil {
		t.Fatal("NewManager returned nil")
	}
}

func TestManagerAcquireRelease(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Acquire exclusive lock
	err := mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Acquiring same lock again should succeed (reference counting)
	err = mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive)
	if err != nil {
		t.Fatalf("Second Acquire should succeed: %v", err)
	}

	// First Release - decrements refCount from 2 to 1
	err = mgr.Release(LockTypeInode, 100)
	if err != nil {
		t.Fatalf("First Release failed: %v", err)
	}

	// Second Release - decrements refCount from 1 to 0 and releases distributed lock
	err = mgr.Release(LockTypeInode, 100)
	if err != nil {
		t.Fatalf("Second Release failed: %v", err)
	}

	// Third Release should fail - lock no longer held
	err = mgr.Release(LockTypeInode, 100)
	if err != ErrNotLockOwner {
		t.Errorf("Third Release: err = %v, want ErrNotLockOwner", err)
	}
}

func TestManagerSharedLocks(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid1, uuid2 [16]byte
	copy(uuid1[:], "test-node-uuid01")
	copy(uuid2[:], "test-node-uuid02")

	mgr1 := NewManager(dev, 0, 10<<20, uuid1, 0)
	mgr2 := NewManager(dev, 0, 10<<20, uuid2, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Both nodes acquire shared lock
	err := mgr1.Acquire(ctx, LockTypeInode, 100, LockModeShared)
	if err != nil {
		t.Fatalf("mgr1 Acquire shared failed: %v", err)
	}

	err = mgr2.Acquire(ctx, LockTypeInode, 100, LockModeShared)
	if err != nil {
		t.Fatalf("mgr2 Acquire shared failed: %v", err)
	}

	// Release both
	mgr1.Release(LockTypeInode, 100)
	mgr2.Release(LockTypeInode, 100)
}

func TestManagerLockOrdering(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Acquire high-level lock first (Extent)
	err := mgr.Acquire(ctx, LockTypeExtent, 100, LockModeExclusive)
	if err != nil {
		t.Fatalf("Acquire Extent failed: %v", err)
	}

	// Try to acquire lower-level lock (should fail - deadlock risk)
	err = mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive)
	if err == nil {
		t.Error("Expected deadlock prevention error when acquiring lower-level lock")
	}

	mgr.Release(LockTypeExtent, 100)
}

func TestManagerCorrectLockOrdering(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Acquire in correct order (low to high)
	err := mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive)
	if err != nil {
		t.Fatalf("Acquire Inode failed: %v", err)
	}

	err = mgr.Acquire(ctx, LockTypeExtent, 200, LockModeExclusive)
	if err != nil {
		t.Fatalf("Acquire Extent failed: %v", err)
	}

	// Release in reverse order
	mgr.Release(LockTypeExtent, 200)
	mgr.Release(LockTypeInode, 100)
}

func TestManagerHelpers(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test helper methods
	err := mgr.AcquireInode(ctx, 100, LockModeExclusive)
	if err != nil {
		t.Fatalf("AcquireInode failed: %v", err)
	}

	err = mgr.ReleaseInode(100)
	if err != nil {
		t.Fatalf("ReleaseInode failed: %v", err)
	}
}

func TestManagerWithInodeLock(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	executed := false
	err := mgr.WithInodeLock(ctx, 100, LockModeExclusive, func() error {
		executed = true
		return nil
	})

	if err != nil {
		t.Fatalf("WithInodeLock failed: %v", err)
	}

	if !executed {
		t.Error("Function was not executed")
	}
}

func TestHashToRecord(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	// Same inputs should give same hash
	hash1 := mgr.hashToRecord(LockTypeInode, 100)
	hash2 := mgr.hashToRecord(LockTypeInode, 100)
	if hash1 != hash2 {
		t.Errorf("hash not deterministic: %d != %d", hash1, hash2)
	}

	// Different inputs should (usually) give different hash
	hash3 := mgr.hashToRecord(LockTypeInode, 101)
	if hash1 == hash3 {
		t.Log("Warning: hash collision (may happen rarely)")
	}

	// Hash should be within bounds
	maxRecords := mgr.size / RecordSize
	if hash1 >= maxRecords {
		t.Errorf("hash %d >= maxRecords %d", hash1, maxRecords)
	}
}

func TestIsLockStale(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	// Free lock - not stale
	freeRecord := &Record{Mode: LockModeNone}
	if mgr.isLockStale(freeRecord) {
		t.Error("free lock should not be stale")
	}

	// Fresh lock - not stale
	freshRecord := &Record{
		Mode:        LockModeExclusive,
		AcquireTime: time.Now().Unix(),
	}
	if mgr.isLockStale(freshRecord) {
		t.Error("fresh lock should not be stale")
	}

	// Old lock - stale
	staleRecord := &Record{
		Mode:        LockModeExclusive,
		AcquireTime: time.Now().Add(-StaleThreshold - time.Second).Unix(),
	}
	if !mgr.isLockStale(staleRecord) {
		t.Error("old lock should be stale")
	}
}

func TestHeldLocks(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Initially no held locks
	held := mgr.HeldLocks()
	if len(held) != 0 {
		t.Errorf("expected 0 held locks, got %d", len(held))
	}

	// Acquire some locks
	mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive)
	mgr.Acquire(ctx, LockTypeExtent, 200, LockModeShared)

	held = mgr.HeldLocks()
	if len(held) != 2 {
		t.Errorf("expected 2 held locks, got %d", len(held))
	}

	// Release and check
	mgr.Release(LockTypeInode, 100)
	mgr.Release(LockTypeExtent, 200)

	held = mgr.HeldLocks()
	if len(held) != 0 {
		t.Errorf("expected 0 held locks after release, got %d", len(held))
	}
}

func BenchmarkRecordSerialize(b *testing.B) {
	record := &Record{
		Magic:       RecordMagic,
		Type:        LockTypeInode,
		Mode:        LockModeExclusive,
		ResourceID:  12345,
		AcquireTime: time.Now().Unix(),
		Sequence:    42,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = serializeRecord(record)
	}
}

func BenchmarkRecordDeserialize(b *testing.B) {
	record := &Record{
		Magic:       RecordMagic,
		Type:        LockTypeInode,
		Mode:        LockModeExclusive,
		ResourceID:  12345,
		AcquireTime: time.Now().Unix(),
		Sequence:    42,
	}
	data := serializeRecord(record)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = deserializeRecord(data)
	}
}

func BenchmarkHashToRecord(b *testing.B) {
	dev, cleanup := createTestDevice(b, 10<<20)
	defer cleanup()

	var uuid [16]byte
	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mgr.hashToRecord(LockTypeInode, uint64(i))
	}
}

// TestHintCacheOptimisticAcquire tests that the hint cache enables
// optimistic lock acquisition after release.
func TestHintCacheOptimisticAcquire(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Initially, no hint cache hits (cache is empty)
	hits, misses, _ := mgr.HintStats()
	if hits != 0 || misses != 0 {
		t.Errorf("initial stats should be zero: hits=%d, misses=%d", hits, misses)
	}

	// Acquire and release a lock - this populates the hint cache
	err := mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive)
	if err != nil {
		t.Fatalf("first Acquire failed: %v", err)
	}
	err = mgr.Release(LockTypeInode, 100)
	if err != nil {
		t.Fatalf("first Release failed: %v", err)
	}

	// Second acquire should use the hint cache (optimistic path)
	err = mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive)
	if err != nil {
		t.Fatalf("second Acquire failed: %v", err)
	}
	err = mgr.Release(LockTypeInode, 100)
	if err != nil {
		t.Fatalf("second Release failed: %v", err)
	}

	// Check that we had at least one hint hit
	hits, misses, hitRate := mgr.HintStats()
	t.Logf("Hint cache stats: hits=%d, misses=%d, hitRate=%.2f", hits, misses, hitRate)

	// The second acquire should have hit the cache (sequence was correct)
	if hits == 0 {
		t.Error("expected at least one hint cache hit after release and re-acquire")
	}
}

// TestHintCacheStats tests the HintStats method.
func TestHintCacheStats(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	// Test initial stats
	hits, misses, hitRate := mgr.HintStats()
	if hits != 0 || misses != 0 || hitRate != 0 {
		t.Errorf("initial: hits=%d, misses=%d, rate=%.2f, want all zero", hits, misses, hitRate)
	}
}

// BenchmarkLockAcquireRelease measures lock cycle time with hint cache.
func BenchmarkLockAcquireRelease(b *testing.B) {
	dev, cleanup := createTestDevice(b, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Acquire(ctx, LockTypeInode, uint64(i%1000), LockModeExclusive)
		mgr.Release(LockTypeInode, uint64(i%1000))
	}
	b.StopTimer()

	hits, misses, hitRate := mgr.HintStats()
	b.Logf("Hint cache: hits=%d, misses=%d, hitRate=%.2f", hits, misses, hitRate)
}

// BenchmarkLockRepeatedAcquire measures repeated access to same lock (hint cache hot path).
func BenchmarkLockRepeatedAcquire(b *testing.B) {
	dev, cleanup := createTestDevice(b, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	ctx := context.Background()

	// Warm up the hint cache with one cycle
	mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive)
	mgr.Release(LockTypeInode, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive)
		mgr.Release(LockTypeInode, 100)
	}
	b.StopTimer()

	hits, misses, hitRate := mgr.HintStats()
	b.Logf("Hint cache (repeated lock 100): hits=%d, misses=%d, hitRate=%.2f", hits, misses, hitRate)
}
