package snapshot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// createTestDevice creates a test file for O_DIRECT testing.
func createTestDevice(tb testing.TB, size int64) (*storage.Device, func()) {
	tb.Helper()

	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_snapshot.bin")

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

func TestConstants(t *testing.T) {
	// Verify descriptor size is 128 bytes
	if DescriptorSize != 128 {
		t.Errorf("DescriptorSize = %d, want 128", DescriptorSize)
	}

	// Verify sector size matches storage
	if SectorSize != storage.SectorSize {
		t.Errorf("SectorSize = %d, want %d", SectorSize, storage.SectorSize)
	}

	// Verify descriptors per sector calculation
	expectedDesc := (SectorSize - ChecksumSize) / DescriptorSize
	if DescriptorsPerSector != expectedDesc {
		t.Errorf("DescriptorsPerSector = %d, want %d", DescriptorsPerSector, expectedDesc)
	}

	// Verify 31 descriptors per 4KB sector
	if DescriptorsPerSector != 31 {
		t.Errorf("DescriptorsPerSector = %d, want 31", DescriptorsPerSector)
	}

	// Verify bytes per sector
	if BytesPerSector != DescriptorsPerSector*DescriptorSize {
		t.Errorf("BytesPerSector = %d, want %d", BytesPerSector, DescriptorsPerSector*DescriptorSize)
	}

	// Verify max name length
	if MaxSnapshotNameLen != 48 {
		t.Errorf("MaxSnapshotNameLen = %d, want 48", MaxSnapshotNameLen)
	}
}

func TestStateString(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StateInvalid, "invalid"},
		{StateCreating, "creating"},
		{StateActive, "active"},
		{StateDeleting, "deleting"},
		{State(99), "unknown(99)"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("State(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestDescriptorHelpers(t *testing.T) {
	tests := []struct {
		name       string
		desc       Descriptor
		isValid    bool
		isFree     bool
		isActive   bool
		isReadOnly bool
		isWritable bool
	}{
		{
			name:       "free slot",
			desc:       Descriptor{},
			isValid:    false,
			isFree:     true,
			isActive:   false,
			isReadOnly: false,
			isWritable: false,
		},
		{
			name:       "active read-only snapshot",
			desc:       Descriptor{Magic: DescriptorMagic, State: StateActive, Flags: FlagReadOnly},
			isValid:    true,
			isFree:     false,
			isActive:   true,
			isReadOnly: true,
			isWritable: false,
		},
		{
			name:       "writable clone",
			desc:       Descriptor{Magic: DescriptorMagic, State: StateActive, Flags: FlagWritable},
			isValid:    true,
			isFree:     false,
			isActive:   true,
			isReadOnly: false,
			isWritable: true,
		},
		{
			name:       "creating snapshot",
			desc:       Descriptor{Magic: DescriptorMagic, State: StateCreating, Flags: FlagReadOnly},
			isValid:    true,
			isFree:     false,
			isActive:   false,
			isReadOnly: true,
			isWritable: false,
		},
		{
			name:       "deleting snapshot",
			desc:       Descriptor{Magic: DescriptorMagic, State: StateDeleting},
			isValid:    true,
			isFree:     false,
			isActive:   false,
			isReadOnly: false,
			isWritable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.desc.IsValid(); got != tt.isValid {
				t.Errorf("IsValid() = %v, want %v", got, tt.isValid)
			}
			if got := tt.desc.IsFree(); got != tt.isFree {
				t.Errorf("IsFree() = %v, want %v", got, tt.isFree)
			}
			if got := tt.desc.IsActive(); got != tt.isActive {
				t.Errorf("IsActive() = %v, want %v", got, tt.isActive)
			}
			if got := tt.desc.IsReadOnly(); got != tt.isReadOnly {
				t.Errorf("IsReadOnly() = %v, want %v", got, tt.isReadOnly)
			}
			if got := tt.desc.IsWritable(); got != tt.isWritable {
				t.Errorf("IsWritable() = %v, want %v", got, tt.isWritable)
			}
		})
	}
}

func TestDescriptorName(t *testing.T) {
	desc := &Descriptor{}

	// Set a name
	err := desc.SetName("test-snapshot")
	if err != nil {
		t.Fatalf("SetName failed: %v", err)
	}

	// Get the name back
	if got := desc.GetName(); got != "test-snapshot" {
		t.Errorf("GetName() = %q, want %q", got, "test-snapshot")
	}

	// Test name too long
	longName := "this-is-a-very-long-snapshot-name-that-exceeds-the-maximum-allowed-length"
	err = desc.SetName(longName)
	if err != ErrNameTooLong {
		t.Errorf("Expected ErrNameTooLong, got %v", err)
	}

	// Test empty name
	err = desc.SetName("")
	if err != nil {
		t.Fatalf("SetName('') failed: %v", err)
	}
	if got := desc.GetName(); got != "" {
		t.Errorf("GetName() = %q, want empty", got)
	}
}

func TestNewRegistry(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20) // 10MB
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)

	if registry == nil {
		t.Fatal("NewRegistry returned nil")
	}

	expectedMax := uint64(10 * DescriptorsPerSector)
	if registry.MaxDescriptors() != expectedMax {
		t.Errorf("MaxDescriptors() = %d, want %d", registry.MaxDescriptors(), expectedMax)
	}

	if registry.Count() != 0 {
		t.Errorf("Count() = %d, want 0", registry.Count())
	}
}

func TestRegistryCreate(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create a snapshot
	desc, err := registry.Create(ctx, "snap1", 100, InvalidSnapshotID,
		1000, 50, 1024*1024, FlagReadOnly)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if desc.Magic != DescriptorMagic {
		t.Errorf("Magic = 0x%x, want 0x%x", desc.Magic, DescriptorMagic)
	}
	if desc.SnapshotID == InvalidSnapshotID {
		t.Error("SnapshotID should not be invalid")
	}
	if desc.TargetInode != 100 {
		t.Errorf("TargetInode = %d, want 100", desc.TargetInode)
	}
	if desc.State != StateCreating {
		t.Errorf("State = %v, want %v", desc.State, StateCreating)
	}
	if desc.GetName() != "snap1" {
		t.Errorf("Name = %q, want %q", desc.GetName(), "snap1")
	}
	if desc.ExtentTreeRoot != 1000 {
		t.Errorf("ExtentTreeRoot = %d, want 1000", desc.ExtentTreeRoot)
	}
	if desc.TotalBlocks != 50 {
		t.Errorf("TotalBlocks = %d, want 50", desc.TotalBlocks)
	}
	if desc.TotalSize != 1024*1024 {
		t.Errorf("TotalSize = %d, want %d", desc.TotalSize, 1024*1024)
	}

	if registry.Count() != 1 {
		t.Errorf("Count() = %d, want 1", registry.Count())
	}
}

func TestRegistryGet(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create a snapshot
	created, err := registry.Create(ctx, "snap1", 100, InvalidSnapshotID,
		1000, 50, 1024*1024, FlagReadOnly)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Get by ID
	retrieved, err := registry.Get(ctx, created.SnapshotID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.SnapshotID != created.SnapshotID {
		t.Errorf("SnapshotID = %d, want %d", retrieved.SnapshotID, created.SnapshotID)
	}
	if retrieved.GetName() != "snap1" {
		t.Errorf("Name = %q, want %q", retrieved.GetName(), "snap1")
	}
}

func TestRegistryGetByName(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create snapshots
	registry.Create(ctx, "snap1", 100, InvalidSnapshotID, 1000, 50, 1024, FlagReadOnly)
	registry.Create(ctx, "snap2", 200, InvalidSnapshotID, 2000, 100, 2048, FlagReadOnly)

	// Get by name
	desc, err := registry.GetByName(ctx, "snap2")
	if err != nil {
		t.Fatalf("GetByName failed: %v", err)
	}
	if desc.TargetInode != 200 {
		t.Errorf("TargetInode = %d, want 200", desc.TargetInode)
	}

	// Non-existent name
	_, err = registry.GetByName(ctx, "nonexistent")
	if err != ErrSnapshotNotFound {
		t.Errorf("Expected ErrSnapshotNotFound, got %v", err)
	}
}

func TestRegistryGetNotFound(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Get non-existent snapshot
	_, err := registry.Get(ctx, 999)
	if err != ErrSnapshotNotFound {
		t.Errorf("Expected ErrSnapshotNotFound, got %v", err)
	}
}

func TestRegistryGetInvalidID(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	_, err := registry.Get(ctx, InvalidSnapshotID)
	if err != ErrInvalidID {
		t.Errorf("Expected ErrInvalidID, got %v", err)
	}
}

func TestRegistryList(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create several snapshots
	registry.Create(ctx, "snap1", 100, InvalidSnapshotID, 1000, 50, 1024, FlagReadOnly)
	registry.Create(ctx, "snap2", 200, InvalidSnapshotID, 2000, 100, 2048, FlagReadOnly)
	registry.Create(ctx, "hidden", 300, InvalidSnapshotID, 3000, 150, 3072, FlagHidden)

	// List should not include hidden
	snapshots, err := registry.List(ctx)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(snapshots) != 2 {
		t.Errorf("List returned %d snapshots, want 2", len(snapshots))
	}
}

func TestRegistryListForInode(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create snapshots for different inodes
	registry.Create(ctx, "snap1-a", 100, InvalidSnapshotID, 1000, 50, 1024, FlagReadOnly)
	registry.Create(ctx, "snap1-b", 100, InvalidSnapshotID, 1001, 51, 1025, FlagReadOnly)
	registry.Create(ctx, "snap2", 200, InvalidSnapshotID, 2000, 100, 2048, FlagReadOnly)

	// List for inode 100
	snapshots, err := registry.ListForInode(ctx, 100)
	if err != nil {
		t.Fatalf("ListForInode failed: %v", err)
	}
	if len(snapshots) != 2 {
		t.Errorf("ListForInode(100) returned %d snapshots, want 2", len(snapshots))
	}

	// List for inode 200
	snapshots, err = registry.ListForInode(ctx, 200)
	if err != nil {
		t.Fatalf("ListForInode failed: %v", err)
	}
	if len(snapshots) != 1 {
		t.Errorf("ListForInode(200) returned %d snapshots, want 1", len(snapshots))
	}
}

func TestRegistryUpdateState(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create a snapshot
	desc, err := registry.Create(ctx, "snap1", 100, InvalidSnapshotID,
		1000, 50, 1024, FlagReadOnly)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Update to active
	err = registry.UpdateState(ctx, desc.SnapshotID, StateActive)
	if err != nil {
		t.Fatalf("UpdateState failed: %v", err)
	}

	// Verify
	retrieved, err := registry.Get(ctx, desc.SnapshotID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved.State != StateActive {
		t.Errorf("State = %v, want %v", retrieved.State, StateActive)
	}
}

func TestRegistryDelete(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create and activate a snapshot
	desc, _ := registry.Create(ctx, "snap1", 100, InvalidSnapshotID,
		1000, 50, 1024, FlagReadOnly)
	registry.UpdateState(ctx, desc.SnapshotID, StateActive)

	// Delete (mark for deletion)
	err := registry.Delete(ctx, desc.SnapshotID)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify state
	retrieved, err := registry.Get(ctx, desc.SnapshotID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved.State != StateDeleting {
		t.Errorf("State = %v, want %v", retrieved.State, StateDeleting)
	}
	if retrieved.DeleteTime == 0 {
		t.Error("DeleteTime should be set")
	}
}

func TestRegistryRemove(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create, activate, and delete a snapshot
	desc, _ := registry.Create(ctx, "snap1", 100, InvalidSnapshotID,
		1000, 50, 1024, FlagReadOnly)
	registry.UpdateState(ctx, desc.SnapshotID, StateActive)
	registry.Delete(ctx, desc.SnapshotID)

	// Remove permanently
	err := registry.Remove(ctx, desc.SnapshotID)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Should no longer be found
	_, err = registry.Get(ctx, desc.SnapshotID)
	if err != ErrSnapshotNotFound {
		t.Errorf("Expected ErrSnapshotNotFound, got %v", err)
	}

	if registry.Count() != 0 {
		t.Errorf("Count() = %d, want 0", registry.Count())
	}
}

func TestRegistryRemoveNotDeleting(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create an active snapshot
	desc, _ := registry.Create(ctx, "snap1", 100, InvalidSnapshotID,
		1000, 50, 1024, FlagReadOnly)
	registry.UpdateState(ctx, desc.SnapshotID, StateActive)

	// Try to remove without deleting first
	err := registry.Remove(ctx, desc.SnapshotID)
	if err != ErrSnapshotInvalidState {
		t.Errorf("Expected ErrSnapshotInvalidState, got %v", err)
	}
}

func TestRegistrySetRefCountJobID(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create a snapshot
	desc, _ := registry.Create(ctx, "snap1", 100, InvalidSnapshotID,
		1000, 50, 1024, FlagReadOnly)

	// Set job ID
	err := registry.SetRefCountJobID(ctx, desc.SnapshotID, 12345)
	if err != nil {
		t.Fatalf("SetRefCountJobID failed: %v", err)
	}

	// Verify
	retrieved, _ := registry.Get(ctx, desc.SnapshotID)
	if retrieved.RefCountJobID != 12345 {
		t.Errorf("RefCountJobID = %d, want 12345", retrieved.RefCountJobID)
	}
}

func TestRegistryInitialize(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create some snapshots
	registry.Create(ctx, "snap1", 100, InvalidSnapshotID, 1000, 50, 1024, FlagReadOnly)
	registry.Create(ctx, "snap2", 200, InvalidSnapshotID, 2000, 100, 2048, FlagReadOnly)

	// Initialize should clear everything
	err := registry.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if registry.Count() != 0 {
		t.Errorf("Count() = %d, want 0", registry.Count())
	}

	// List should be empty
	snapshots, _ := registry.List(ctx)
	if len(snapshots) != 0 {
		t.Errorf("List returned %d snapshots, want 0", len(snapshots))
	}
}

func TestRegistryScan(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create snapshots in various states
	desc1, _ := registry.Create(ctx, "active1", 100, InvalidSnapshotID, 1000, 50, 1024, FlagReadOnly)
	registry.UpdateState(ctx, desc1.SnapshotID, StateActive)

	desc2, _ := registry.Create(ctx, "active2", 200, InvalidSnapshotID, 2000, 100, 2048, FlagReadOnly)
	registry.UpdateState(ctx, desc2.SnapshotID, StateActive)

	_, _ = registry.Create(ctx, "creating", 300, InvalidSnapshotID, 3000, 75, 1536, FlagReadOnly)
	// Leave in creating state

	desc4, _ := registry.Create(ctx, "deleting", 400, InvalidSnapshotID, 4000, 25, 512, FlagReadOnly)
	registry.UpdateState(ctx, desc4.SnapshotID, StateActive)
	registry.Delete(ctx, desc4.SnapshotID)

	// Scan
	stats, err := registry.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if stats.ActiveCount != 2 {
		t.Errorf("ActiveCount = %d, want 2", stats.ActiveCount)
	}
	if stats.CreatingCount != 1 {
		t.Errorf("CreatingCount = %d, want 1", stats.CreatingCount)
	}
	if stats.DeletingCount != 1 {
		t.Errorf("DeletingCount = %d, want 1", stats.DeletingCount)
	}
	// Total blocks = 50 + 100 + 75 + 25 = 250
	if stats.TotalBlocks != 250 {
		t.Errorf("TotalBlocks = %d, want 250", stats.TotalBlocks)
	}
}

func TestRegistryPersistence(t *testing.T) {
	tmpDir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if tmpDir == "" {
		tmpDir = "."
	}

	testFile := filepath.Join(tmpDir, "test_snapshot_persist.bin")
	defer os.Remove(testFile)

	var snapshotID uint64

	// Create and write
	func() {
		f, err := os.Create(testFile)
		if err != nil {
			t.Skipf("Cannot create test file: %v", err)
		}
		if err := f.Truncate(10 << 20); err != nil {
			f.Close()
			t.Skipf("Cannot truncate: %v", err)
		}
		f.Close()

		dev, err := storage.Open(testFile)
		if err != nil {
			t.Skipf("O_DIRECT not supported: %v", err)
		}
		defer dev.Close()

		registry := NewRegistry(dev, 0, 10*SectorSize, nil)
		ctx := context.Background()

		desc, err := registry.Create(ctx, "persistent-snap", 100, InvalidSnapshotID,
			1000, 50, 1024*1024, FlagReadOnly)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		snapshotID = desc.SnapshotID

		registry.UpdateState(ctx, snapshotID, StateActive)
	}()

	// Reopen and verify
	dev, err := storage.Open(testFile)
	if err != nil {
		t.Skipf("Cannot reopen: %v", err)
	}
	defer dev.Close()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Scan to rebuild state
	registry.Scan(ctx)

	// Retrieve
	desc, err := registry.Get(ctx, snapshotID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if desc.GetName() != "persistent-snap" {
		t.Errorf("Name = %q, want %q", desc.GetName(), "persistent-snap")
	}
	if desc.TargetInode != 100 {
		t.Errorf("TargetInode = %d, want 100", desc.TargetInode)
	}
	if desc.State != StateActive {
		t.Errorf("State = %v, want %v", desc.State, StateActive)
	}
	if desc.ExtentTreeRoot != 1000 {
		t.Errorf("ExtentTreeRoot = %d, want 1000", desc.ExtentTreeRoot)
	}
}

func TestSizeForSnapshots(t *testing.T) {
	tests := []struct {
		count       uint64
		wantSectors uint64
	}{
		{0, 0},
		{1, 1},
		{DescriptorsPerSector, 1},
		{DescriptorsPerSector + 1, 2},
		{DescriptorsPerSector * 2, 2},
		{DescriptorsPerSector * 3, 3},
		{1000, (1000 + DescriptorsPerSector - 1) / DescriptorsPerSector},
	}

	for _, tt := range tests {
		size := SizeForSnapshots(tt.count)
		wantSize := tt.wantSectors * SectorSize
		if size != wantSize {
			t.Errorf("SizeForSnapshots(%d) = %d, want %d", tt.count, size, wantSize)
		}
	}
}

func TestDescriptorsInSize(t *testing.T) {
	tests := []struct {
		size uint64
		want uint64
	}{
		{0, 0},
		{SectorSize, DescriptorsPerSector},
		{SectorSize * 2, DescriptorsPerSector * 2},
		{SectorSize * 10, DescriptorsPerSector * 10},
	}

	for _, tt := range tests {
		got := DescriptorsInSize(tt.size)
		if got != tt.want {
			t.Errorf("DescriptorsInSize(%d) = %d, want %d", tt.size, got, tt.want)
		}
	}
}

func TestMultipleSectors(t *testing.T) {
	dev, cleanup := createTestDevice(t, 100<<20) // 100MB
	defer cleanup()

	// Create registry spanning 5 sectors = 5 * 31 = 155 max descriptors
	registry := NewRegistry(dev, 0, 5*SectorSize, nil)
	ctx := context.Background()

	// Create more snapshots than fit in one sector
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("snap%d", i)
		_, err := registry.Create(ctx, name, uint32(100+i), InvalidSnapshotID,
			uint64(1000+i), uint64(50+i), uint64(1024*(i+1)), FlagReadOnly)
		if err != nil {
			t.Fatalf("Create(%s) failed: %v", name, err)
		}
	}

	if registry.Count() != 50 {
		t.Errorf("Count() = %d, want 50", registry.Count())
	}

	// Verify we can retrieve snapshots from different sectors
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("snap%d", i)
		desc, err := registry.GetByName(ctx, name)
		if err != nil {
			t.Fatalf("GetByName(%s) failed: %v", name, err)
		}
		if desc.TargetInode != uint32(100+i) {
			t.Errorf("snap%d: TargetInode = %d, want %d", i, desc.TargetInode, 100+i)
		}
	}
}

func TestParentSnapshot(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()

	// Create root snapshot
	root, err := registry.Create(ctx, "root", 100, InvalidSnapshotID,
		1000, 50, 1024, FlagReadOnly)
	if err != nil {
		t.Fatalf("Create root failed: %v", err)
	}

	// Create child snapshot pointing to root
	child, err := registry.Create(ctx, "child", 100, root.SnapshotID,
		1000, 50, 1024, FlagReadOnly)
	if err != nil {
		t.Fatalf("Create child failed: %v", err)
	}

	if child.ParentID != root.SnapshotID {
		t.Errorf("ParentID = %d, want %d", child.ParentID, root.SnapshotID)
	}

	// Retrieve and verify
	retrieved, _ := registry.Get(ctx, child.SnapshotID)
	if retrieved.ParentID != root.SnapshotID {
		t.Errorf("Retrieved ParentID = %d, want %d", retrieved.ParentID, root.SnapshotID)
	}
}

// Benchmarks

func BenchmarkCreate(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 100*SectorSize, nil)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Create(ctx, fmt.Sprintf("snap%d", i), uint32(100+i),
			InvalidSnapshotID, 1000, 50, 1024, FlagReadOnly)
	}
}

func BenchmarkGet(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 100*SectorSize, nil)
	ctx := context.Background()

	// Pre-create snapshots
	var ids []uint64
	for i := 0; i < 100; i++ {
		desc, _ := registry.Create(ctx, fmt.Sprintf("snap%d", i), uint32(100+i),
			InvalidSnapshotID, 1000, 50, 1024, FlagReadOnly)
		ids = append(ids, desc.SnapshotID)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.Get(ctx, ids[i%100])
	}
}

func BenchmarkList(b *testing.B) {
	dev, cleanup := createTestDevice(b, 100<<20)
	defer cleanup()

	registry := NewRegistry(dev, 0, 100*SectorSize, nil)
	ctx := context.Background()

	// Pre-create snapshots
	for i := 0; i < 100; i++ {
		registry.Create(ctx, fmt.Sprintf("snap%d", i), uint32(100+i),
			InvalidSnapshotID, 1000, 50, 1024, FlagReadOnly)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		registry.List(ctx)
	}
}
