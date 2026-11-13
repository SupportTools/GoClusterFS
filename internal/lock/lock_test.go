package lock

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

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
		_ = f.Close()
		_ = os.Remove(testFile)
		tb.Skipf("Cannot truncate test file: %v", err)
	}
	if err := f.Close(); err != nil {
		tb.Fatal(err)
	}

	dev, err := storage.Open(testFile)
	if err != nil {
		_ = os.Remove(testFile)
		tb.Skipf("O_DIRECT not supported: %v", err)
	}

	cleanup := func() {
		_ = dev.Close()
		_ = os.Remove(testFile)
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
	release(t, mgr1, LockTypeInode, 100)
	release(t, mgr2, LockTypeInode, 100)
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

	release(t, mgr, LockTypeExtent, 100)
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
	release(t, mgr, LockTypeExtent, 200)
	release(t, mgr, LockTypeInode, 100)
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
	maxRecords := mgr.size / RowSize
	if hash1 >= maxRecords {
		t.Errorf("hash %d >= maxRecords %d", hash1, maxRecords)
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
	if err := mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Acquire(ctx, LockTypeExtent, 200, LockModeShared); err != nil {
		t.Fatal(err)
	}

	held = mgr.HeldLocks()
	if len(held) != 2 {
		t.Errorf("expected 2 held locks, got %d", len(held))
	}

	// Release and check
	release(t, mgr, LockTypeInode, 100)
	release(t, mgr, LockTypeExtent, 200)

	held = mgr.HeldLocks()
	if len(held) != 0 {
		t.Errorf("expected 0 held locks after release, got %d", len(held))
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

// BenchmarkLockAcquireRelease measures lock cycles over 1000 resources.
func BenchmarkLockAcquireRelease(b *testing.B) {
	dev, cleanup := createTestDevice(b, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := mgr.Acquire(ctx, LockTypeInode, uint64(i%1000), LockModeExclusive); err != nil {
			b.Fatal(err)
		}
		release(b, mgr, LockTypeInode, uint64(i%1000))
	}
}

// BenchmarkLockRepeatedAcquire measures repeated cycles of one lock, which stays cached.
func BenchmarkLockRepeatedAcquire(b *testing.B) {
	dev, cleanup := createTestDevice(b, 10<<20)
	defer cleanup()

	var uuid [16]byte
	copy(uuid[:], "test-node-uuid00")

	mgr := NewManager(dev, 0, 10<<20, uuid, 0)

	ctx := context.Background()

	// Warm up the hint cache with one cycle
	if err := mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive); err != nil {
		b.Fatal(err)
	}
	release(b, mgr, LockTypeInode, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := mgr.Acquire(ctx, LockTypeInode, 100, LockModeExclusive); err != nil {
			b.Fatal(err)
		}
		release(b, mgr, LockTypeInode, 100)
	}
}
