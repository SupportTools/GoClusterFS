package fsck_test

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// testDevice creates a temporary file-backed device for testing.
// Uses a directory that supports O_DIRECT (not tmpfs).
func testDevice(t *testing.T, sizeMB int) (*storage.Device, func()) {
	t.Helper()

	// Create temp directory in /tmp (not tmpfs, which doesn't support O_DIRECT)
	tmpDir, err := os.MkdirTemp("/tmp", "fsck-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	filePath := filepath.Join(tmpDir, "test.img")

	f, err := os.Create(filePath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create temp file: %v", err)
	}

	// Size the file
	sizeBytes := int64(sizeMB * 1024 * 1024)
	if err := f.Truncate(sizeBytes); err != nil {
		f.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to truncate: %v", err)
	}
	f.Close()

	dev, err := storage.Open(filePath)
	if err != nil {
		os.RemoveAll(tmpDir)
		// If O_DIRECT is not supported, skip the test
		if os.IsNotExist(err) || os.IsPermission(err) {
			t.Skip("O_DIRECT not supported on this filesystem")
		}
		t.Fatalf("failed to open device: %v", err)
	}

	cleanup := func() {
		dev.Close()
		os.RemoveAll(tmpDir)
	}

	return dev, cleanup
}

// writeSuperblock writes a valid superblock to the device.
func writeSuperblock(t *testing.T, dev *storage.Device, totalBlocks uint64) {
	t.Helper()

	const (
		bitmapOffset       = 1 * 1024 * 1024
		inodeOffset        = 2 * 1024 * 1024
		nodeRegistryOffset = 3 * 1024 * 1024
		lockOffset         = 4 * 1024 * 1024
		journalOffset      = 5 * 1024 * 1024
		refcountOffset     = 9 * 1024 * 1024
		snapshotOffset     = 10 * 1024 * 1024
		dataOffset         = 11 * 1024 * 1024
	)

	sb := &superblock.Superblock{
		Magic:               [12]byte{'G', 'O', 'C', 'L', 'U', 'S', 'T', 'E', 'R', 'F', 'S', 0},
		Version:             superblock.CurrentVersion,
		FeatureFlags:        0,
		RequiredFlags:       superblock.RequiredJournal,
		BlockSize:           1048576, // 1MB
		TotalBlocks:         totalBlocks,
		MetaBlockSize:       4096,
		MaxNodes:            16,
		InodeSize:           256,
		NodeRegistryOffset:  nodeRegistryOffset,
		JournalRegionOffset: journalOffset,
		LockBitmapOffset:    lockOffset,
		InodeTableOffset:    inodeOffset,
		BlockBitmapOffset:   bitmapOffset,
		RefCountTableOffset: refcountOffset,
		SnapshotRegOffset:   snapshotOffset,
		DataRegionOffset:    dataOffset,
		NodeRegistrySize:    1 * 1024 * 1024,
		JournalRegionSize:   4 * 1024 * 1024,
		LockBitmapSize:      1 * 1024 * 1024,
		InodeTableSize:      1 * 1024 * 1024,
		BlockBitmapSize:     1 * 1024 * 1024,
		RefCountTableSize:   1 * 1024 * 1024,
		SnapshotRegSize:     1 * 1024 * 1024,
		InodeCount:          4096,
		FreeInodes:          4093, // 3 reserved: 0, 1, 2
		FreeBlocks:          totalBlocks,
		SnapshotCount:       0,
		MountCount:          0,
	}

	buf := sb.Serialize()

	if _, err := dev.WriteAt(buf, 0); err != nil {
		t.Fatalf("failed to write superblock: %v", err)
	}
}

// initRootInode writes a minimal root directory inode.
// Uses aligned I/O by reading the full sector, modifying, and writing back.
func initRootInode(t *testing.T, dev *storage.Device) {
	t.Helper()

	const (
		inodeOffset = 2 * 1024 * 1024
		inodeSize   = 256
		inodeMagic  = 0x494E4F44 // "INOD"
		inodeRoot   = 2
		sIfdir      = 0o040000
		sectorSize  = 4096
	)

	// Calculate which sector contains inode 2
	inodeByteOffset := int64(inodeRoot) * int64(inodeSize)
	sectorOffset := (int64(inodeOffset) + inodeByteOffset) &^ (sectorSize - 1)
	offsetInSector := (int64(inodeOffset) + inodeByteOffset) - sectorOffset

	// Read the full sector using aligned buffer
	sectorBuf := storage.NewAlignedBuffer(sectorSize)
	if _, err := dev.ReadAt(sectorBuf.Bytes(), sectorOffset); err != nil {
		t.Fatalf("failed to read sector: %v", err)
	}

	// Prepare inode data in the sector
	inodeBuf := sectorBuf.Bytes()[offsetInSector : offsetInSector+inodeSize]

	// Magic
	binary.LittleEndian.PutUint32(inodeBuf[0:4], inodeMagic)
	// InodeNum
	binary.LittleEndian.PutUint32(inodeBuf[4:8], inodeRoot)
	// Mode (directory)
	binary.LittleEndian.PutUint16(inodeBuf[8:10], sIfdir|0755)
	// Flags
	binary.LittleEndian.PutUint16(inodeBuf[10:12], 0)
	// UID
	binary.LittleEndian.PutUint32(inodeBuf[12:16], 0)
	// GID
	binary.LittleEndian.PutUint32(inodeBuf[16:20], 0)
	// LinkCount
	binary.LittleEndian.PutUint32(inodeBuf[20:24], 2) // . and ..
	// Size (empty directory)
	binary.LittleEndian.PutUint64(inodeBuf[24:32], 0)

	// Calculate and store checksum
	checksum := crc32.Checksum(inodeBuf[:252], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(inodeBuf[252:256], checksum)

	// Write the sector back
	if _, err := dev.WriteAt(sectorBuf.Bytes(), sectorOffset); err != nil {
		t.Fatalf("failed to write root inode: %v", err)
	}
}

func TestFsckCleanFilesystem(t *testing.T) {
	dev, cleanup := testDevice(t, 20) // 20MB device
	defer cleanup()

	// Set up a basic formatted filesystem
	writeSuperblock(t, dev, 9) // 9 data blocks (20MB - 11MB metadata)
	initRootInode(t, dev)

	// Run fsck
	opts := fsck.DefaultOptions()
	opts.Verbose = true
	opts.Output = &bytes.Buffer{} // Capture output

	checker := fsck.New(dev, opts)
	result := checker.Check()

	// Verify results
	if result.Aborted {
		t.Errorf("fsck aborted: %s", result.AbortReason)
	}

	// A freshly created filesystem should have minimal issues
	// (Some warnings are OK for missing structures)
	if result.FatalCount > 0 {
		t.Errorf("expected no fatal errors, got %d", result.FatalCount)
		for _, p := range result.Problems {
			if p.Severity == fsck.Fatal {
				t.Logf("Fatal: %s", p.String())
			}
		}
	}

	// Should have detected the root inode
	if result.UsedInodes == 0 {
		t.Error("expected to find at least root inode")
	}
}

func TestFsckCorruptedSuperblockMagic(t *testing.T) {
	dev, cleanup := testDevice(t, 20)
	defer cleanup()

	// Write valid superblock first
	writeSuperblock(t, dev, 9)

	// Read the full superblock sector
	sectorBuf := storage.NewAlignedBuffer(4096)
	if _, err := dev.ReadAt(sectorBuf.Bytes(), 0); err != nil {
		t.Fatalf("failed to read superblock: %v", err)
	}

	// Corrupt the magic number
	copy(sectorBuf.Bytes()[0:12], []byte("NOTCLUSTER\x00\x00"))

	// Write back the full sector
	if _, err := dev.WriteAt(sectorBuf.Bytes(), 0); err != nil {
		t.Fatalf("failed to corrupt magic: %v", err)
	}

	// Run fsck
	opts := fsck.DefaultOptions()
	checker := fsck.New(dev, opts)
	result := checker.Check()

	// Should abort with fatal error
	if !result.Aborted {
		t.Error("expected fsck to abort on invalid magic")
	}
	if result.FatalCount == 0 {
		t.Error("expected fatal error for invalid magic")
	}
}

func TestFsckCorruptedSuperblockChecksum(t *testing.T) {
	dev, cleanup := testDevice(t, 20)
	defer cleanup()

	// Write valid superblock
	writeSuperblock(t, dev, 9)

	// Read it back using aligned buffer
	buf := storage.NewAlignedBuffer(4096)
	if _, err := dev.ReadAt(buf.Bytes(), 0); err != nil {
		t.Fatalf("failed to read superblock: %v", err)
	}

	// Corrupt the checksum (last 4 bytes)
	binary.LittleEndian.PutUint32(buf.Bytes()[4092:], 0xDEADBEEF)
	if _, err := dev.WriteAt(buf.Bytes(), 0); err != nil {
		t.Fatalf("failed to write corrupted superblock: %v", err)
	}

	// Run fsck in check-only mode
	opts := fsck.DefaultOptions()
	opts.Verbose = true
	opts.Output = &bytes.Buffer{}

	checker := fsck.New(dev, opts)
	result := checker.Check()

	// Should detect checksum error
	foundChecksumError := false
	for _, p := range result.Problems {
		if p.Phase == fsck.PhaseSuperblock && p.Repairable {
			foundChecksumError = true
			break
		}
	}
	if !foundChecksumError {
		t.Error("expected to detect superblock checksum corruption")
	}
}

func TestFsckRepairMode(t *testing.T) {
	dev, cleanup := testDevice(t, 20)
	defer cleanup()

	// Write valid superblock
	writeSuperblock(t, dev, 9)

	// Read and corrupt the checksum using aligned buffer
	buf := storage.NewAlignedBuffer(4096)
	if _, err := dev.ReadAt(buf.Bytes(), 0); err != nil {
		t.Fatalf("failed to read superblock: %v", err)
	}
	binary.LittleEndian.PutUint32(buf.Bytes()[4092:], 0xDEADBEEF)
	if _, err := dev.WriteAt(buf.Bytes(), 0); err != nil {
		t.Fatalf("failed to write corrupted superblock: %v", err)
	}

	// Run fsck in repair mode
	opts := fsck.DefaultOptions()
	opts.Mode = fsck.Repair
	opts.Output = &bytes.Buffer{}

	checker := fsck.New(dev, opts)
	result := checker.Check()

	// Should have repaired
	if result.RepairedCount == 0 {
		t.Error("expected at least one repair")
	}

	// Verify the checksum is now correct
	if _, err := dev.ReadAt(buf.Bytes(), 0); err != nil {
		t.Fatalf("failed to re-read superblock: %v", err)
	}

	storedChecksum := binary.LittleEndian.Uint32(buf.Bytes()[4092:])
	computedChecksum := crc32.Checksum(buf.Bytes()[:4092], crc32.MakeTable(crc32.Castagnoli))

	if storedChecksum != computedChecksum {
		t.Errorf("checksum not repaired: stored=0x%08X, computed=0x%08X",
			storedChecksum, computedChecksum)
	}
}

func TestFsckPhaseSkipping(t *testing.T) {
	dev, cleanup := testDevice(t, 20)
	defer cleanup()

	writeSuperblock(t, dev, 9)
	initRootInode(t, dev)

	// Run fsck skipping directories phase
	opts := fsck.DefaultOptions()
	opts.SkipPhases = map[fsck.Phase]bool{
		fsck.PhaseDirectories: true,
	}
	opts.Output = &bytes.Buffer{}

	checker := fsck.New(dev, opts)
	result := checker.Check()

	// Should complete without the skipped phase
	if result.Aborted {
		t.Errorf("fsck aborted: %s", result.AbortReason)
	}

	// CompletedPhase should be the last phase we ran
	// (Cross-validation if not skipping it)
	if result.CompletedPhase == fsck.PhaseDirectories {
		t.Error("completed phase should not be the skipped phase")
	}
}

func TestFsckQuickValidation(t *testing.T) {
	// Test the standalone quick validation function
	buf := make([]byte, 4096)

	// Create valid superblock data
	copy(buf[0:12], []byte("GOCLUSTERFS\x00"))
	binary.LittleEndian.PutUint32(buf[12:16], superblock.CurrentVersion)

	// Calculate and set checksum
	checksum := crc32.Checksum(buf[:4092], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(buf[4092:], checksum)

	// Should pass validation
	if err := fsck.ValidateSuperblockQuick(buf); err != nil {
		t.Errorf("valid superblock failed quick validation: %v", err)
	}

	// Corrupt magic and verify failure
	buf[0] = 'X'
	if err := fsck.ValidateSuperblockQuick(buf); err == nil {
		t.Error("corrupted magic should fail quick validation")
	}
}

func TestFsckModeStrings(t *testing.T) {
	modes := []struct {
		mode fsck.Mode
		str  string
	}{
		{fsck.CheckOnly, "check-only"},
		{fsck.Repair, "repair"},
		{fsck.Force, "force"},
	}

	for _, m := range modes {
		if m.mode.String() != m.str {
			t.Errorf("Mode(%d).String() = %q, want %q", m.mode, m.mode.String(), m.str)
		}
	}
}

func TestFsckSeverityStrings(t *testing.T) {
	severities := []struct {
		sev fsck.Severity
		str string
	}{
		{fsck.Info, "INFO"},
		{fsck.Warning, "WARN"},
		{fsck.Error, "ERROR"},
		{fsck.Critical, "CRIT"},
		{fsck.Fatal, "FATAL"},
	}

	for _, s := range severities {
		if s.sev.String() != s.str {
			t.Errorf("Severity(%d).String() = %q, want %q", s.sev, s.sev.String(), s.str)
		}
	}
}

func TestFsckPhaseStrings(t *testing.T) {
	phases := []struct {
		phase fsck.Phase
		name  string
	}{
		{fsck.PhaseSuperblock, "Phase 1 (Superblock)"},
		{fsck.PhaseBitmap, "Phase 2 (Bitmap)"},
		{fsck.PhaseInodes, "Phase 3 (Inodes)"},
		{fsck.PhaseExtents, "Phase 4 (Extents)"},
		{fsck.PhaseRefcounts, "Phase 5 (Refcounts)"},
		{fsck.PhaseDirectories, "Phase 6 (Directories)"},
		{fsck.PhaseSnapshots, "Phase 7 (Snapshots)"},
		{fsck.PhaseCrossValidation, "Phase 8 (Cross-Validation)"},
	}

	for _, p := range phases {
		if p.phase.String() != p.name {
			t.Errorf("Phase(%d).String() = %q, want %q", p.phase, p.phase.String(), p.name)
		}
	}
}

func TestFsckResultSummary(t *testing.T) {
	result := &fsck.Result{
		Mode:            fsck.CheckOnly,
		CompletedPhase:  fsck.PhaseCrossValidation,
		TotalBlocks:     100,
		AllocatedBlocks: 50,
		FreeBlocks:      50,
		TotalInodes:     1000,
		UsedInodes:      100,
		FreeInodes:      900,
	}

	summary := result.Summary()

	// Verify key parts are in summary
	if !bytes.Contains([]byte(summary), []byte("CLEAN")) {
		t.Error("expected CLEAN status in summary")
	}
	if !bytes.Contains([]byte(summary), []byte("check-only")) {
		t.Error("expected mode in summary")
	}
}

func TestFsckResultHasErrors(t *testing.T) {
	result := &fsck.Result{}

	if result.HasErrors() {
		t.Error("empty result should not have errors")
	}

	result.ErrorCount = 1
	if !result.HasErrors() {
		t.Error("result with ErrorCount > 0 should have errors")
	}

	result = &fsck.Result{CriticalCount: 1}
	if !result.HasErrors() {
		t.Error("result with CriticalCount > 0 should have errors")
	}

	result = &fsck.Result{FatalCount: 1}
	if !result.HasErrors() {
		t.Error("result with FatalCount > 0 should have errors")
	}
}

func TestFsckResultIsClean(t *testing.T) {
	result := &fsck.Result{}

	if !result.IsClean() {
		t.Error("empty result should be clean")
	}

	result.AddProblem(fsck.Problem{
		Phase:    fsck.PhaseSuperblock,
		Severity: fsck.Warning,
		Message:  "test warning",
	})

	if result.IsClean() {
		t.Error("result with problems should not be clean")
	}
}
