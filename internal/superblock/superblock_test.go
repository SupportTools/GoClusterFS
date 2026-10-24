package superblock

import (
	"errors"
	"testing"
)

func TestSerializeDeserialize(t *testing.T) {
	// Create a superblock with known values
	sb := &Superblock{
		Version:       CurrentVersion,
		FeatureFlags:  FeatureSparseFiles | FeatureLargeFiles,
		RequiredFlags: RequiredJournal,
		BlockSize:     1 << 20,
		TotalBlocks:   1000,
		MetaBlockSize: 4096,
		MaxNodes:      64,
		InodeSize:     256,
		InodeCount:    100000,
		FreeInodes:    99999,
		FreeBlocks:    900,
		SnapshotCount: 5,
		MountCount:    10,
		CreateTime:    1700000000,
		LastMountTime: 1700001000,
		LastWriteTime: 1700002000,
	}
	copy(sb.Magic[:], Magic)
	copy(sb.UUID[:], []byte("0123456789abcdef"))

	// Set region offsets
	sb.NodeRegistryOffset = 4096
	sb.JournalRegionOffset = 4096 + 1<<20
	sb.LockBitmapOffset = sb.JournalRegionOffset + 256<<20
	sb.DataRegionOffset = sb.LockBitmapOffset + 1<<20

	// Serialize
	data := sb.Serialize()

	if len(data) != SuperblockSize {
		t.Fatalf("serialized size = %d, want %d", len(data), SuperblockSize)
	}

	// Deserialize
	restored, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Verify all fields
	if string(restored.Magic[:len(Magic)]) != Magic {
		t.Errorf("Magic = %q, want %q", restored.Magic[:], Magic)
	}
	if restored.Version != sb.Version {
		t.Errorf("Version = %d, want %d", restored.Version, sb.Version)
	}
	if restored.FeatureFlags != sb.FeatureFlags {
		t.Errorf("FeatureFlags = %d, want %d", restored.FeatureFlags, sb.FeatureFlags)
	}
	if restored.RequiredFlags != sb.RequiredFlags {
		t.Errorf("RequiredFlags = %d, want %d", restored.RequiredFlags, sb.RequiredFlags)
	}
	if restored.BlockSize != sb.BlockSize {
		t.Errorf("BlockSize = %d, want %d", restored.BlockSize, sb.BlockSize)
	}
	if restored.TotalBlocks != sb.TotalBlocks {
		t.Errorf("TotalBlocks = %d, want %d", restored.TotalBlocks, sb.TotalBlocks)
	}
	if restored.MaxNodes != sb.MaxNodes {
		t.Errorf("MaxNodes = %d, want %d", restored.MaxNodes, sb.MaxNodes)
	}
	if restored.InodeCount != sb.InodeCount {
		t.Errorf("InodeCount = %d, want %d", restored.InodeCount, sb.InodeCount)
	}
	if restored.FreeBlocks != sb.FreeBlocks {
		t.Errorf("FreeBlocks = %d, want %d", restored.FreeBlocks, sb.FreeBlocks)
	}
	if restored.MountCount != sb.MountCount {
		t.Errorf("MountCount = %d, want %d", restored.MountCount, sb.MountCount)
	}
	if restored.CreateTime != sb.CreateTime {
		t.Errorf("CreateTime = %d, want %d", restored.CreateTime, sb.CreateTime)
	}
	if restored.NodeRegistryOffset != sb.NodeRegistryOffset {
		t.Errorf("NodeRegistryOffset = %d, want %d", restored.NodeRegistryOffset, sb.NodeRegistryOffset)
	}
	if restored.DataRegionOffset != sb.DataRegionOffset {
		t.Errorf("DataRegionOffset = %d, want %d", restored.DataRegionOffset, sb.DataRegionOffset)
	}
}

func TestDeserializeChecksumMismatch(t *testing.T) {
	sb := &Superblock{Version: CurrentVersion}
	copy(sb.Magic[:], Magic)

	data := sb.Serialize()

	// Corrupt a byte in the middle
	data[100] ^= 0xFF

	_, err := Deserialize(data)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Errorf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestDeserializeInvalidMagic(t *testing.T) {
	sb := &Superblock{Version: CurrentVersion}
	copy(sb.Magic[:], "BADMAGIC123") // Wrong magic

	data := sb.Serialize()

	// Checksum will be computed for the bad magic, so it passes checksum
	// But should fail magic validation
	_, err := Deserialize(data)
	if !errors.Is(err, ErrInvalidMagic) {
		t.Errorf("expected ErrInvalidMagic, got %v", err)
	}
}

func TestDeserializeBufferTooSmall(t *testing.T) {
	_, err := Deserialize(make([]byte, 100))
	if err == nil {
		t.Error("expected error for small buffer")
	}
}

func TestCanMountCurrentVersion(t *testing.T) {
	sb := &Superblock{
		Version:       CurrentVersion,
		RequiredFlags: RequiredJournal, // We support this
	}

	if err := sb.CanMount(); err != nil {
		t.Errorf("CanMount should succeed for current version: %v", err)
	}
}

func TestCanMountOlderVersion(t *testing.T) {
	// Test that we accept older versions within range
	sb := &Superblock{
		Version:       MinSupportedVersion,
		RequiredFlags: 0, // No required flags
	}

	if err := sb.CanMount(); err != nil {
		t.Errorf("CanMount should succeed for MinSupportedVersion: %v", err)
	}
}

func TestCanMountNewerVersion(t *testing.T) {
	sb := &Superblock{
		Version:       CurrentVersion + 1, // Future version
		RequiredFlags: 0,
	}

	err := sb.CanMount()
	if !errors.Is(err, ErrNewerVersion) {
		t.Errorf("expected ErrNewerVersion, got %v", err)
	}
}

func TestCanMountTooOldVersion(t *testing.T) {
	if MinSupportedVersion == 0 {
		t.Skip("MinSupportedVersion is 0, can't test too-old")
	}

	sb := &Superblock{
		Version:       MinSupportedVersion - 1,
		RequiredFlags: 0,
	}

	err := sb.CanMount()
	if !errors.Is(err, ErrTooOldVersion) {
		t.Errorf("expected ErrTooOldVersion, got %v", err)
	}
}

func TestCanMountUnknownRequiredFlags(t *testing.T) {
	sb := &Superblock{
		Version:       CurrentVersion,
		RequiredFlags: RequiredEncryption, // We don't support encryption yet
	}

	err := sb.CanMount()
	if !errors.Is(err, ErrUnknownRequired) {
		t.Errorf("expected ErrUnknownRequired, got %v", err)
	}
}

func TestCanMountWithOptionalFeatures(t *testing.T) {
	// Optional features should not block mounting
	sb := &Superblock{
		Version:       CurrentVersion,
		FeatureFlags:  0xFFFFFFFF, // All optional flags set
		RequiredFlags: RequiredJournal,
	}

	if err := sb.CanMount(); err != nil {
		t.Errorf("CanMount should ignore optional features: %v", err)
	}
}

func TestNewSuperblock(t *testing.T) {
	deviceSize := int64(10 << 30) // 10GB
	sb := New(deviceSize)

	// Check magic
	if string(sb.Magic[:len(Magic)]) != Magic {
		t.Errorf("Magic = %q, want %q", sb.Magic[:], Magic)
	}

	// Check version
	if sb.Version != CurrentVersion {
		t.Errorf("Version = %d, want %d", sb.Version, CurrentVersion)
	}

	// Check geometry
	if sb.BlockSize != 1<<20 {
		t.Errorf("BlockSize = %d, want 1MB", sb.BlockSize)
	}
	if sb.MaxNodes != 64 {
		t.Errorf("MaxNodes = %d, want 64", sb.MaxNodes)
	}

	want := []struct {
		name         string
		offset, size uint64
	}{
		{"block bitmap", 1 << 20, 1 << 20},
		{"inode table", 2 << 20, 1 << 20},
		{"node registry", 3 << 20, 1 << 20},
		{"locks", 4 << 20, 1 << 20},
		{"journals", 5 << 20, 64 * 4 << 20},
		{"refcounts", 261 << 20, 1 << 20},
		{"snapshots", 262 << 20, 1 << 20},
	}
	got := [][2]uint64{
		{sb.BlockBitmapOffset, sb.BlockBitmapSize},
		{sb.InodeTableOffset, sb.InodeTableSize},
		{sb.NodeRegistryOffset, sb.NodeRegistrySize},
		{sb.LockBitmapOffset, sb.LockBitmapSize},
		{sb.JournalRegionOffset, sb.JournalRegionSize},
		{sb.RefCountTableOffset, sb.RefCountTableSize},
		{sb.SnapshotRegOffset, sb.SnapshotRegSize},
	}
	for i, w := range want {
		if got[i] != [2]uint64{w.offset, w.size} {
			t.Errorf("%s at %d size %d, want %d size %d", w.name, got[i][0], got[i][1], w.offset, w.size)
		}
	}
	if sb.DataRegionOffset != 263<<20 {
		t.Errorf("DataRegionOffset = %d, want %d", sb.DataRegionOffset, 263<<20)
	}
	if want := uint64(deviceSize-263<<20) >> 20; sb.TotalBlocks != want {
		t.Errorf("TotalBlocks = %d, want %d", sb.TotalBlocks, want)
	}
	if sb.JournalSlotSize() != 4<<20 {
		t.Errorf("JournalSlotSize = %d, want 4MB", sb.JournalSlotSize())
	}
	if sb.InodeCount != 4096 {
		t.Errorf("InodeCount = %d, want 4096", sb.InodeCount)
	}
	if New(MinDeviceSize).TotalBlocks != 1 {
		t.Errorf("MinDeviceSize leaves %d data blocks, want 1", New(MinDeviceSize).TotalBlocks)
	}

	// Check timestamps
	if sb.CreateTime == 0 {
		t.Error("CreateTime should be set")
	}

	// UUID should be set (random, not all zeros)
	allZero := true
	for _, b := range sb.UUID {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Error("UUID should be randomly generated, not all zeros")
	}
}

func TestSuperblockSize(t *testing.T) {
	// Verify our struct fits in the declared size
	sb := Superblock{}
	data := sb.Serialize()

	if len(data) != SuperblockSize {
		t.Errorf("Serialized size = %d, expected %d", len(data), SuperblockSize)
	}
}

func TestChecksumDeterminism(t *testing.T) {
	sb := &Superblock{
		Version:     CurrentVersion,
		TotalBlocks: 1000,
	}
	copy(sb.Magic[:], Magic)

	data1 := sb.Serialize()
	checksum1 := sb.Checksum

	data2 := sb.Serialize()
	checksum2 := sb.Checksum

	if checksum1 != checksum2 {
		t.Errorf("checksums should be deterministic: %x != %x", checksum1, checksum2)
	}

	// Data should be identical
	for i := range data1 {
		if data1[i] != data2[i] {
			t.Errorf("data differs at byte %d: %x != %x", i, data1[i], data2[i])
			break
		}
	}
}

func TestFeatureFlags(t *testing.T) {
	// Test that feature flags are independent
	if FeatureSparseFiles&FeatureExtendedAttrs != 0 {
		t.Error("Feature flags should be independent bits")
	}
	if RequiredJournal&RequiredSnapshots != 0 {
		t.Error("Required flags should be independent bits")
	}
}

func BenchmarkSerialize(b *testing.B) {
	sb := New(10 << 30)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sb.Serialize()
	}
}

func BenchmarkDeserialize(b *testing.B) {
	sb := New(10 << 30)
	data := sb.Serialize()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Deserialize(data)
	}
}
