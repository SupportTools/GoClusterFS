package dir

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// testSetup creates a test environment with all dependencies.
func testSetup(t *testing.T) (context.Context, *Manager, *inode.Table, func()) {
	t.Helper()

	// Create temp file
	tmpFile := filepath.Join(t.TempDir(), "test.img")
	f, err := os.Create(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Size: 1GB for testing
	size := int64(1024 * 1024 * 1024)
	if err := f.Truncate(size); err != nil {
		f.Close()
		t.Fatalf("Failed to truncate: %v", err)
	}
	f.Close()

	// Open as device
	dev, err := storage.Open(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open device: %v", err)
	}

	// Test node UUID
	var nodeUUID [16]byte
	copy(nodeUUID[:], "test-node-uuid00")

	// Create bitmap (starts at 2MB, tracks 100 blocks)
	// Pass nil for lockMgr to avoid distributed locking complexity in unit tests
	bm := bitmap.NewRegion(dev, 2*1024*1024, 100, nil, nodeUUID)

	// Create inode table (starts at 3MB)
	// Pass nil for lockMgr for unit tests
	inodeTbl := inode.NewTable(dev, 3*1024*1024, 100*inode.InodeSize, nil, nodeUUID)

	// Data blocks start at 10MB (past all metadata regions)
	dataOffset := uint64(10 * 1024 * 1024)

	// Create extent manager (nil lockMgr for unit tests)
	extentMgr := extent.NewManager(dev, bm, nil, dataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)

	// Create directory manager (nil lockMgr for unit tests)
	dirMgr := NewManager(dev, inodeTbl, extentMgr, nil, dataOffset)

	cleanup := func() {
		dev.Close()
	}

	return context.Background(), dirMgr, inodeTbl, cleanup
}

func TestConstants(t *testing.T) {
	// Verify constants are sensible
	if DirEntryHeaderSize != 12 {
		t.Errorf("DirEntryHeaderSize = %d, want 12", DirEntryHeaderSize)
	}

	if MinDirEntrySize != 16 {
		t.Errorf("MinDirEntrySize = %d, want 16", MinDirEntrySize)
	}

	if MaxNameLen != 255 {
		t.Errorf("MaxNameLen = %d, want 255", MaxNameLen)
	}

	if BlockSize != extent.BlockSize {
		t.Errorf("BlockSize = %d, want %d", BlockSize, extent.BlockSize)
	}
}

func TestFileTypes(t *testing.T) {
	tests := []struct {
		name     string
		fileType uint8
		expected uint8
	}{
		{"DT_UNKNOWN", DT_UNKNOWN, 0},
		{"DT_REG", DT_REG, 1},
		{"DT_DIR", DT_DIR, 2},
		{"DT_LNK", DT_LNK, 7},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.fileType != tc.expected {
				t.Errorf("%s = %d, want %d", tc.name, tc.fileType, tc.expected)
			}
		})
	}
}

func TestEntrySize(t *testing.T) {
	tests := []struct {
		nameLen  uint8
		expected uint16
	}{
		{0, 12},    // Header only, rounds to 12
		{1, 16},    // 12 + 1 = 13, rounds to 16
		{2, 16},    // 12 + 2 = 14, rounds to 16
		{3, 16},    // 12 + 3 = 15, rounds to 16
		{4, 16},    // 12 + 4 = 16, already aligned
		{5, 20},    // 12 + 5 = 17, rounds to 20
		{100, 112}, // 12 + 100 = 112, already aligned
		{255, 268}, // 12 + 255 = 267, rounds to 268
	}

	for _, tc := range tests {
		entry := &DirEntry{NameLen: tc.nameLen}
		got := entrySize(entry)
		if got != tc.expected {
			t.Errorf("entrySize(nameLen=%d) = %d, want %d", tc.nameLen, got, tc.expected)
		}
	}
}

func TestSerializeParseEntry(t *testing.T) {
	tests := []struct {
		name  string
		entry DirEntry
	}{
		{
			name: "short_name",
			entry: DirEntry{
				InodeNum: 100,
				RecLen:   16,
				NameLen:  4,
				FileType: DT_REG,
				Name:     "test",
			},
		},
		{
			name: "directory",
			entry: DirEntry{
				InodeNum: 2,
				RecLen:   16,
				NameLen:  1,
				FileType: DT_DIR,
				Name:     ".",
			},
		},
		{
			name: "long_name",
			entry: DirEntry{
				InodeNum: 500,
				RecLen:   268,
				NameLen:  255,
				FileType: DT_REG,
				Name:     string(make([]byte, 255)),
			},
		},
		{
			name: "deleted",
			entry: DirEntry{
				InodeNum: 0,
				RecLen:   16,
				NameLen:  4,
				FileType: DT_UNKNOWN,
				Name:     "gone",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.entry.RecLen)
			serializeEntry(&tc.entry, buf)

			parsed, err := parseEntry(buf)
			if err != nil {
				t.Fatalf("parseEntry failed: %v", err)
			}

			if parsed.InodeNum != tc.entry.InodeNum {
				t.Errorf("InodeNum = %d, want %d", parsed.InodeNum, tc.entry.InodeNum)
			}
			if parsed.RecLen != tc.entry.RecLen {
				t.Errorf("RecLen = %d, want %d", parsed.RecLen, tc.entry.RecLen)
			}
			if parsed.NameLen != tc.entry.NameLen {
				t.Errorf("NameLen = %d, want %d", parsed.NameLen, tc.entry.NameLen)
			}
			if parsed.FileType != tc.entry.FileType {
				t.Errorf("FileType = %d, want %d", parsed.FileType, tc.entry.FileType)
			}
			if parsed.Name != tc.entry.Name {
				t.Errorf("Name = %q, want %q", parsed.Name, tc.entry.Name)
			}
		})
	}
}

func TestParseEntryInvalid(t *testing.T) {
	tests := []struct {
		name string
		buf  []byte
	}{
		{"too_short", make([]byte, 8)},
		{"reclen_too_small", []byte{0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 5, 0}}, // RecLen=4 < 12
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseEntry(tc.buf)
			if err == nil {
				t.Error("Expected error for invalid entry")
			}
		})
	}
}

func TestFileTypeFromMode(t *testing.T) {
	tests := []struct {
		mode     uint16
		expected uint8
	}{
		{inode.S_IFREG | 0644, DT_REG},
		{inode.S_IFDIR | 0755, DT_DIR},
		{inode.S_IFLNK | 0777, DT_LNK},
		{inode.S_IFBLK | 0600, DT_BLK},
		{inode.S_IFCHR | 0600, DT_CHR},
		{inode.S_IFIFO | 0644, DT_FIFO},
		{inode.S_IFSOCK | 0755, DT_SOCK},
		{0, DT_UNKNOWN},
	}

	for _, tc := range tests {
		got := FileTypeFromMode(tc.mode)
		if got != tc.expected {
			t.Errorf("FileTypeFromMode(0x%04x) = %d, want %d", tc.mode, got, tc.expected)
		}
	}
}

func TestInitDirectory(t *testing.T) {
	ctx, dirMgr, inodeTbl, cleanup := testSetup(t)
	defer cleanup()

	// Allocate a directory inode
	dirIno, err := inodeTbl.Alloc(ctx)
	if err != nil {
		t.Fatalf("Failed to allocate inode: %v", err)
	}
	dirIno.Mode = inode.S_IFDIR | 0755

	// Initialize the directory
	parentInode := uint64(2) // Parent is root
	if err := dirMgr.InitDirectory(ctx, dirIno, parentInode); err != nil {
		t.Fatalf("InitDirectory failed: %v", err)
	}

	// Verify size is one block
	if dirIno.Size != BlockSize {
		t.Errorf("Directory size = %d, want %d", dirIno.Size, BlockSize)
	}

	// List entries
	entries, err := dirMgr.List(ctx, dirIno)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	// Should have "." and ".."
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}

	foundDot := false
	foundDotDot := false
	for _, e := range entries {
		if e.Name == "." {
			foundDot = true
			if e.InodeNum != uint64(dirIno.InodeNum) {
				t.Errorf(". inode = %d, want %d", e.InodeNum, dirIno.InodeNum)
			}
			if e.FileType != DT_DIR {
				t.Errorf(". type = %d, want %d", e.FileType, DT_DIR)
			}
		}
		if e.Name == ".." {
			foundDotDot = true
			if e.InodeNum != parentInode {
				t.Errorf(".. inode = %d, want %d", e.InodeNum, parentInode)
			}
			if e.FileType != DT_DIR {
				t.Errorf(".. type = %d, want %d", e.FileType, DT_DIR)
			}
		}
	}

	if !foundDot {
		t.Error("Missing . entry")
	}
	if !foundDotDot {
		t.Error("Missing .. entry")
	}
}

func TestLookup(t *testing.T) {
	ctx, dirMgr, inodeTbl, cleanup := testSetup(t)
	defer cleanup()

	// Create and initialize directory
	dirIno, err := inodeTbl.Alloc(ctx)
	if err != nil {
		t.Fatalf("Failed to allocate inode: %v", err)
	}
	dirIno.Mode = inode.S_IFDIR | 0755
	if err := dirMgr.InitDirectory(ctx, dirIno, 2); err != nil {
		t.Fatalf("InitDirectory failed: %v", err)
	}

	// Lookup "."
	ino, ftype, err := dirMgr.Lookup(ctx, dirIno, ".")
	if err != nil {
		t.Fatalf("Lookup(.) failed: %v", err)
	}
	if ino != uint64(dirIno.InodeNum) {
		t.Errorf(". inode = %d, want %d", ino, dirIno.InodeNum)
	}
	if ftype != DT_DIR {
		t.Errorf(". type = %d, want %d", ftype, DT_DIR)
	}

	// Lookup ".."
	ino, _, err = dirMgr.Lookup(ctx, dirIno, "..")
	if err != nil {
		t.Fatalf("Lookup(..) failed: %v", err)
	}
	if ino != 2 {
		t.Errorf(".. inode = %d, want 2", ino)
	}

	// Lookup non-existent
	_, _, err = dirMgr.Lookup(ctx, dirIno, "nonexistent")
	if err != ErrNotFound {
		t.Errorf("Lookup(nonexistent) err = %v, want ErrNotFound", err)
	}
}

func TestLookupErrors(t *testing.T) {
	ctx, dirMgr, inodeTbl, cleanup := testSetup(t)
	defer cleanup()

	// Create a regular file (not a directory)
	fileIno, err := inodeTbl.Alloc(ctx)
	if err != nil {
		t.Fatalf("Failed to allocate inode: %v", err)
	}
	fileIno.Mode = inode.S_IFREG | 0644

	// Lookup on non-directory should fail
	_, _, err = dirMgr.Lookup(ctx, fileIno, "test")
	if err != ErrNotDirectory {
		t.Errorf("Lookup on file err = %v, want ErrNotDirectory", err)
	}

	// Create directory
	dirIno, _ := inodeTbl.Alloc(ctx)
	dirIno.Mode = inode.S_IFDIR | 0755
	dirMgr.InitDirectory(ctx, dirIno, 2)

	// Empty name
	_, _, err = dirMgr.Lookup(ctx, dirIno, "")
	if err != ErrNameEmpty {
		t.Errorf("Lookup('') err = %v, want ErrNameEmpty", err)
	}

	// Name too long
	longName := string(make([]byte, 300))
	_, _, err = dirMgr.Lookup(ctx, dirIno, longName)
	if err != ErrNameTooLong {
		t.Errorf("Lookup(longName) err = %v, want ErrNameTooLong", err)
	}
}

func TestAddEntry(t *testing.T) {
	ctx, dirMgr, inodeTbl, cleanup := testSetup(t)
	defer cleanup()

	// Create and initialize directory
	dirIno, _ := inodeTbl.Alloc(ctx)
	dirIno.Mode = inode.S_IFDIR | 0755
	dirMgr.InitDirectory(ctx, dirIno, 2)

	// Add a file entry
	fileInode := uint64(100)
	if err := dirMgr.Add(ctx, dirIno, "testfile.txt", fileInode, DT_REG); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Verify it can be looked up
	ino, ftype, err := dirMgr.Lookup(ctx, dirIno, "testfile.txt")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if ino != fileInode {
		t.Errorf("inode = %d, want %d", ino, fileInode)
	}
	if ftype != DT_REG {
		t.Errorf("type = %d, want %d", ftype, DT_REG)
	}

	// Add a subdirectory entry
	subdirInode := uint64(101)
	if err := dirMgr.Add(ctx, dirIno, "subdir", subdirInode, DT_DIR); err != nil {
		t.Fatalf("Add subdir failed: %v", err)
	}

	// List all entries
	entries, err := dirMgr.List(ctx, dirIno)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	// Should have 4 entries: ".", "..", "testfile.txt", "subdir"
	if len(entries) != 4 {
		t.Errorf("Expected 4 entries, got %d", len(entries))
		for _, e := range entries {
			t.Logf("  %q (inode %d, type %d)", e.Name, e.InodeNum, e.FileType)
		}
	}
}

func TestAddDuplicate(t *testing.T) {
	ctx, dirMgr, inodeTbl, cleanup := testSetup(t)
	defer cleanup()

	// Create and initialize directory
	dirIno, _ := inodeTbl.Alloc(ctx)
	dirIno.Mode = inode.S_IFDIR | 0755
	dirMgr.InitDirectory(ctx, dirIno, 2)

	// Add a file
	if err := dirMgr.Add(ctx, dirIno, "test", 100, DT_REG); err != nil {
		t.Fatalf("First Add failed: %v", err)
	}

	// Try to add duplicate
	err := dirMgr.Add(ctx, dirIno, "test", 200, DT_REG)
	if err != ErrExists {
		t.Errorf("Add duplicate err = %v, want ErrExists", err)
	}
}

func TestAddErrors(t *testing.T) {
	ctx, dirMgr, inodeTbl, cleanup := testSetup(t)
	defer cleanup()

	// Create a regular file
	fileIno, _ := inodeTbl.Alloc(ctx)
	fileIno.Mode = inode.S_IFREG | 0644

	// Add to non-directory should fail
	err := dirMgr.Add(ctx, fileIno, "test", 100, DT_REG)
	if err != ErrNotDirectory {
		t.Errorf("Add to file err = %v, want ErrNotDirectory", err)
	}

	// Create directory
	dirIno, _ := inodeTbl.Alloc(ctx)
	dirIno.Mode = inode.S_IFDIR | 0755
	dirMgr.InitDirectory(ctx, dirIno, 2)

	// Empty name
	err = dirMgr.Add(ctx, dirIno, "", 100, DT_REG)
	if err != ErrNameEmpty {
		t.Errorf("Add('') err = %v, want ErrNameEmpty", err)
	}

	// Name too long
	longName := string(make([]byte, 300))
	err = dirMgr.Add(ctx, dirIno, longName, 100, DT_REG)
	if err != ErrNameTooLong {
		t.Errorf("Add(longName) err = %v, want ErrNameTooLong", err)
	}
}

func TestRemoveEntry(t *testing.T) {
	ctx, dirMgr, inodeTbl, cleanup := testSetup(t)
	defer cleanup()

	// Create and initialize directory
	dirIno, _ := inodeTbl.Alloc(ctx)
	dirIno.Mode = inode.S_IFDIR | 0755
	dirMgr.InitDirectory(ctx, dirIno, 2)

	// Add entries
	dirMgr.Add(ctx, dirIno, "file1", 100, DT_REG)
	dirMgr.Add(ctx, dirIno, "file2", 101, DT_REG)
	dirMgr.Add(ctx, dirIno, "file3", 102, DT_REG)

	// Remove file2
	if err := dirMgr.Remove(ctx, dirIno, "file2"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify it's gone
	_, _, err := dirMgr.Lookup(ctx, dirIno, "file2")
	if err != ErrNotFound {
		t.Errorf("Lookup after remove err = %v, want ErrNotFound", err)
	}

	// Other entries should still exist
	_, _, err = dirMgr.Lookup(ctx, dirIno, "file1")
	if err != nil {
		t.Errorf("file1 lookup failed: %v", err)
	}
	_, _, err = dirMgr.Lookup(ctx, dirIno, "file3")
	if err != nil {
		t.Errorf("file3 lookup failed: %v", err)
	}
}

func TestRemoveErrors(t *testing.T) {
	ctx, dirMgr, inodeTbl, cleanup := testSetup(t)
	defer cleanup()

	// Create and initialize directory
	dirIno, _ := inodeTbl.Alloc(ctx)
	dirIno.Mode = inode.S_IFDIR | 0755
	dirMgr.InitDirectory(ctx, dirIno, 2)

	// Remove non-existent
	err := dirMgr.Remove(ctx, dirIno, "nonexistent")
	if err != ErrNotFound {
		t.Errorf("Remove(nonexistent) err = %v, want ErrNotFound", err)
	}

	// Cannot remove "."
	err = dirMgr.Remove(ctx, dirIno, ".")
	if err == nil {
		t.Error("Remove(.) should fail")
	}

	// Cannot remove ".."
	err = dirMgr.Remove(ctx, dirIno, "..")
	if err == nil {
		t.Error("Remove(..) should fail")
	}
}

func TestIsEmpty(t *testing.T) {
	ctx, dirMgr, inodeTbl, cleanup := testSetup(t)
	defer cleanup()

	// Create and initialize directory
	dirIno, _ := inodeTbl.Alloc(ctx)
	dirIno.Mode = inode.S_IFDIR | 0755
	dirMgr.InitDirectory(ctx, dirIno, 2)

	// Should be empty (only . and ..)
	empty, err := dirMgr.IsEmpty(ctx, dirIno)
	if err != nil {
		t.Fatalf("IsEmpty failed: %v", err)
	}
	if !empty {
		t.Error("New directory should be empty")
	}

	// Add a file
	dirMgr.Add(ctx, dirIno, "test", 100, DT_REG)

	// Should not be empty now
	empty, err = dirMgr.IsEmpty(ctx, dirIno)
	if err != nil {
		t.Fatalf("IsEmpty failed: %v", err)
	}
	if empty {
		t.Error("Directory with file should not be empty")
	}
}

func TestManyEntries(t *testing.T) {
	ctx, dirMgr, inodeTbl, cleanup := testSetup(t)
	defer cleanup()

	// Create and initialize directory
	dirIno, _ := inodeTbl.Alloc(ctx)
	dirIno.Mode = inode.S_IFDIR | 0755
	dirMgr.InitDirectory(ctx, dirIno, 2)

	// Add many entries (enough to potentially need multiple blocks)
	numEntries := 50
	for i := 0; i < numEntries; i++ {
		name := make([]byte, 100) // 100-char names
		for j := range name {
			name[j] = 'a' + byte(j%26)
		}
		// Make unique by embedding the number
		copy(name[:10], []byte("file"))
		name[4] = byte('0' + (i/100)%10)
		name[5] = byte('0' + (i/10)%10)
		name[6] = byte('0' + i%10)

		if err := dirMgr.Add(ctx, dirIno, string(name), uint64(100+i), DT_REG); err != nil {
			t.Fatalf("Add entry %d failed: %v", i, err)
		}
	}

	// List and verify
	entries, err := dirMgr.List(ctx, dirIno)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	// Should have numEntries + 2 (for . and ..)
	expected := numEntries + 2
	if len(entries) != expected {
		t.Errorf("Expected %d entries, got %d", expected, len(entries))
	}
}

func BenchmarkLookup(b *testing.B) {
	// Create temp file
	tmpFile := filepath.Join(b.TempDir(), "bench.img")
	f, _ := os.Create(tmpFile)
	f.Truncate(1024 * 1024 * 1024)
	f.Close()

	dev, _ := storage.Open(tmpFile)
	defer dev.Close()

	var nodeUUID [16]byte
	copy(nodeUUID[:], "bench-node-uuid0")

	bm := bitmap.NewRegion(dev, 2*1024*1024, 100, nil, nodeUUID)
	inodeTbl := inode.NewTable(dev, 3*1024*1024, 100*inode.InodeSize, nil, nodeUUID)
	dataOffset := uint64(10 * 1024 * 1024)
	extentMgr := extent.NewManager(dev, bm, nil, dataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	dirMgr := NewManager(dev, inodeTbl, extentMgr, nil, dataOffset)

	ctx := context.Background()

	// Create directory with entries
	dirIno, _ := inodeTbl.Alloc(ctx)
	dirIno.Mode = inode.S_IFDIR | 0755
	dirMgr.InitDirectory(ctx, dirIno, 2)

	for i := 0; i < 20; i++ {
		name := make([]byte, 50)
		for j := range name {
			name[j] = 'a' + byte((i+j)%26)
		}
		dirMgr.Add(ctx, dirIno, string(name), uint64(100+i), DT_REG)
	}

	// Find middle entry to search for
	searchName := make([]byte, 50)
	for j := range searchName {
		searchName[j] = 'a' + byte((10+j)%26)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dirMgr.Lookup(ctx, dirIno, string(searchName))
	}
}

func BenchmarkAdd(b *testing.B) {
	// Create temp file
	tmpFile := filepath.Join(b.TempDir(), "bench.img")
	f, _ := os.Create(tmpFile)
	f.Truncate(1024 * 1024 * 1024)
	f.Close()

	dev, _ := storage.Open(tmpFile)
	defer dev.Close()

	var nodeUUID [16]byte
	copy(nodeUUID[:], "bench-node-uuid0")

	bm := bitmap.NewRegion(dev, 2*1024*1024, 1000, nil, nodeUUID) // More blocks for benchmark
	inodeTbl := inode.NewTable(dev, 3*1024*1024, 100*inode.InodeSize, nil, nodeUUID)
	dataOffset := uint64(10 * 1024 * 1024)
	extentMgr := extent.NewManager(dev, bm, nil, dataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	dirMgr := NewManager(dev, inodeTbl, extentMgr, nil, dataOffset)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Create fresh directory for each iteration
		dirIno, _ := inodeTbl.Alloc(ctx)
		dirIno.Mode = inode.S_IFDIR | 0755
		dirMgr.InitDirectory(ctx, dirIno, 2)
		b.StartTimer()

		dirMgr.Add(ctx, dirIno, "testfile", uint64(100+i), DT_REG)
	}
}

// TestDirectoryGrowsPastInlineExtentsUnderLocks adds directory blocks while the
// directory's inode lock is held, so the extent tree block must come from the
// reservation taken before the lock rather than from the bitmap.
func TestDirectoryGrowsPastInlineExtentsUnderLocks(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "locked.img")
	if err := os.WriteFile(tmpFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(tmpFile, 200<<20); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(tmpFile)
	if err != nil {
		t.Fatalf("open device: %v", err)
	}
	defer dev.Close()

	var nodeUUID [16]byte
	copy(nodeUUID[:], "test-node-uuid00")
	lockMgr := lock.NewManager(dev, 0, 1<<20, nodeUUID, 0)
	bm := bitmap.NewRegion(dev, 2<<20, 100, lockMgr, nodeUUID)
	inodeTbl := inode.NewTable(dev, 3<<20, 100*inode.InodeSize, lockMgr, nodeUUID)
	dataOffset := uint64(10 << 20)
	extentMgr := extent.NewManager(dev, bm, lockMgr, dataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	dirMgr := NewManager(dev, inodeTbl, extentMgr, lockMgr, dataOffset)

	ctx := context.Background()
	num, err := inodeTbl.FindFreeSlot()
	if err != nil {
		t.Fatal(err)
	}
	dirIno, err := inodeTbl.ClaimSlot(ctx, num)
	if err != nil {
		t.Fatal(err)
	}
	dirIno.Mode = inode.S_IFDIR | 0o755
	if err := dirMgr.InitDirectory(ctx, dirIno, uint64(inode.InodeRoot)); err != nil {
		t.Fatalf("InitDirectory: %v", err)
	}

	const blocks = 6
	var stale *inode.Inode
	for i := 0; i < blocks; i++ {
		if i == 3 {
			if stale, err = inodeTbl.Read(dirIno.InodeNum); err != nil {
				t.Fatal(err)
			}
		}
		// Take the next free block so directory blocks are never contiguous.
		if _, err := extentMgr.AllocatePhysicalBlock(ctx); err != nil {
			t.Fatal(err)
		}
		fresh, err := inodeTbl.Read(dirIno.InodeNum)
		if err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("entry-%d", i)
		entry := &DirEntry{InodeNum: uint64(50 + i), NameLen: uint8(len(name)), FileType: DT_REG, Name: name}
		entry.RecLen = entrySize(entry)
		if err := dirMgr.addToNewBlock(ctx, fresh, entry); err != nil {
			t.Fatalf("addToNewBlock %d: %v", i, err)
		}
	}

	fresh, err := inodeTbl.Read(dirIno.InodeNum)
	if err != nil {
		t.Fatal(err)
	}
	// A copy read before the last commits points at rewritten tree nodes; a
	// manager with an empty cache has to fall back to the current inode.
	remounted := NewManager(dev, inodeTbl, extent.NewManager(dev, bm, lockMgr, dataOffset), lockMgr, dataOffset)
	if num, _, err := remounted.Lookup(ctx, stale, "entry-5"); err != nil || num != 55 {
		t.Fatalf("lookup through a stale directory inode: inode %d, %v", num, err)
	}
	if !fresh.HasExtentTree() || fresh.ExtentCount != blocks+1 {
		t.Fatalf("directory should use an extent tree with %d extents, got tree=%v count=%d",
			blocks+1, fresh.HasExtentTree(), fresh.ExtentCount)
	}
	for i := 0; i < blocks; i++ {
		num, _, err := dirMgr.Lookup(ctx, fresh, fmt.Sprintf("entry-%d", i))
		if err != nil || num != uint64(50+i) {
			t.Fatalf("lookup entry-%d: inode %d, %v", i, num, err)
		}
	}
}
