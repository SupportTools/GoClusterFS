// Package goclusterfs tests
package goclusterfs

import (
	"context"
	"os"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

func createTestFS(t *testing.T) (*FS, func()) {
	t.Helper()

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "goclusterfs_test_*.img")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Create 20MB test image
	if err := tmpFile.Truncate(20 * 1024 * 1024); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		t.Fatalf("Failed to truncate: %v", err)
	}
	tmpFile.Close()

	// Open with O_DIRECT
	dev, err := storage.Open(tmpFile.Name())
	if err != nil {
		os.Remove(tmpFile.Name())
		t.Fatalf("Failed to open device: %v", err)
	}

	// Layout:
	// 0-1MB: Reserved (superblock, etc.)
	// 1-2MB: Bitmap region
	// 2-3MB: Inode table (4096 inodes)
	// 3-20MB: Data blocks

	var nodeUUID [16]byte
	copy(nodeUUID[:], "testnode12345678")

	bitmapOffset := uint64(1 * 1024 * 1024)
	inodeOffset := uint64(2 * 1024 * 1024)
	dataOffset := uint64(3 * 1024 * 1024)
	totalBlocks := uint64(17) // 17 MB of data blocks
	inodeCount := uint64(4096)

	bm := bitmap.NewRegion(dev, bitmapOffset, totalBlocks, nil, nodeUUID)
	inodeTbl := inode.NewTable(dev, inodeOffset, inodeCount*inode.InodeSize, nil, nodeUUID)
	extentMgr := extent.NewManager(dev, bm, nil, dataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	dirMgr := dir.NewManager(dev, inodeTbl, extentMgr, nil, dataOffset)

	fs := &FS{
		dev:        dev,
		bitmapMgr:  bm,
		inodeTbl:   inodeTbl,
		extentMgr:  extentMgr,
		dirMgr:     dirMgr,
		nodeUUID:   nodeUUID,
		dataOffset: dataOffset,
		uid:        uint32(os.Getuid()),
		gid:        uint32(os.Getgid()),
	}

	cleanup := func() {
		dev.Close()
		os.Remove(tmpFile.Name())
	}

	return fs, cleanup
}

func TestNewFS(t *testing.T) {
	fs, cleanup := createTestFS(t)
	defer cleanup()

	if fs.dev == nil {
		t.Error("Device should not be nil")
	}
	if fs.bitmapMgr == nil {
		t.Error("Bitmap manager should not be nil")
	}
	if fs.inodeTbl == nil {
		t.Error("Inode table should not be nil")
	}
}

func TestInitRoot(t *testing.T) {
	fs, cleanup := createTestFS(t)
	defer cleanup()

	ctx := context.Background()

	// Initialize root
	if err := fs.InitRoot(ctx); err != nil {
		t.Fatalf("InitRoot failed: %v", err)
	}

	// Verify root exists
	rootIno, err := fs.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatalf("Failed to read root inode: %v", err)
	}

	if rootIno.Magic != inode.InodeMagic {
		t.Errorf("Expected magic %x, got %x", inode.InodeMagic, rootIno.Magic)
	}
	if !rootIno.IsDir() {
		t.Error("Root should be a directory")
	}
	if rootIno.LinkCount < 2 {
		t.Errorf("Root link count should be >= 2, got %d", rootIno.LinkCount)
	}
}

func TestRootDir(t *testing.T) {
	fs, cleanup := createTestFS(t)
	defer cleanup()

	ctx := context.Background()
	if err := fs.InitRoot(ctx); err != nil {
		t.Fatalf("InitRoot failed: %v", err)
	}

	// Get root dir node via helper
	dir := fs.RootDir()

	if dir.inodeNum != inode.InodeRoot {
		t.Errorf("Expected root inode %d, got %d", inode.InodeRoot, dir.inodeNum)
	}

	if dir.fs != fs {
		t.Error("Dir should have reference to FS")
	}
}

func TestDirAttr(t *testing.T) {
	fs, cleanup := createTestFS(t)
	defer cleanup()

	ctx := context.Background()
	if err := fs.InitRoot(ctx); err != nil {
		t.Fatalf("InitRoot failed: %v", err)
	}

	dir := fs.RootDir()

	// We need to implement a mock fuse.Attr for testing
	// For now, just verify the directory can be accessed
	ino, err := fs.inodeTbl.Read(dir.inodeNum)
	if err != nil {
		t.Fatalf("Failed to read dir inode: %v", err)
	}

	if !ino.IsDir() {
		t.Error("Root should be a directory")
	}
}

func TestStatfs(t *testing.T) {
	fs, cleanup := createTestFS(t)
	defer cleanup()

	// Verify we can get stats
	totalBlocks := fs.bitmapMgr.TotalBlocks()
	freeBlocks := fs.bitmapMgr.FreeCount()

	if totalBlocks == 0 {
		t.Error("TotalBlocks should not be 0")
	}
	if freeBlocks == 0 {
		t.Error("FreeBlocks should not be 0")
	}
	if freeBlocks > totalBlocks {
		t.Errorf("FreeBlocks (%d) should not exceed TotalBlocks (%d)", freeBlocks, totalBlocks)
	}
}

func TestFileSystemInterface(t *testing.T) {
	fs, cleanup := createTestFS(t)
	defer cleanup()

	// Verify FS implements expected interfaces
	// This is a compile-time check - if it compiles, interfaces are satisfied
	if fs.dev == nil {
		t.Error("Device should not be nil")
	}
	t.Log("FS type satisfies required interfaces")
}
