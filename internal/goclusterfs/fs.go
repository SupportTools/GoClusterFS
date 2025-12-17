// Package goclusterfs implements a FUSE filesystem using GoClusterFS structures.
//
// This package integrates all Phase 2 components (bitmap, inode, extent, dir)
// into a working FUSE filesystem that can be mounted and used.
package goclusterfs

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/cache"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// errRetryWrite is an internal sentinel error indicating that a concurrent
// modification was detected and the Write operation should be retried.
var errRetryWrite = errors.New("retry write operation")

// MaxWriteRetries is the maximum number of retry attempts for Write operations
// when concurrent modifications are detected.
const MaxWriteRetries = 3

// BlockSize is the size of data blocks (1MB).
const BlockSize = 1 << 20

// FS represents the GoClusterFS filesystem.
type FS struct {
	mu sync.RWMutex

	// Storage layer
	dev *storage.Device
	sb  *superblock.Superblock

	// Managers
	lockMgr   *lock.Manager
	bitmapMgr *bitmap.Region
	inodeTbl  *inode.Table
	extentMgr *extent.Manager
	dirMgr    *dir.Manager
	cacheMgr  *cache.Manager // Inode cache for read optimization

	// Configuration
	nodeUUID   [16]byte
	dataOffset uint64 // Offset where data blocks start on disk

	// Runtime state
	uid uint32
	gid uint32
}

// Config holds filesystem configuration.
type Config struct {
	DevicePath string
	NodeUUID   [16]byte

	// Region offsets (from superblock or config)
	BitmapOffset uint64
	InodeOffset  uint64
	DataOffset   uint64

	// Region sizes
	TotalBlocks uint64 // Number of data blocks
	InodeCount  uint64 // Number of inodes
}

// New creates a new GoClusterFS instance.
func New(cfg *Config) (*FS, error) {
	// Open device with O_DIRECT
	dev, err := storage.Open(cfg.DevicePath)
	if err != nil {
		return nil, err
	}

	// Create lock manager (nil for single-node operation)
	var lockMgr *lock.Manager
	// lockMgr = lock.NewManager(dev, lockOffset, cfg.NodeUUID)

	// Create bitmap region
	bitmapMgr := bitmap.NewRegion(dev, cfg.BitmapOffset, cfg.TotalBlocks, lockMgr, cfg.NodeUUID)

	// Create inode table
	inodeTbl := inode.NewTable(dev, cfg.InodeOffset, cfg.InodeCount*inode.InodeSize, lockMgr, cfg.NodeUUID)

	// Create extent manager
	extentMgr := extent.NewManager(dev, bitmapMgr, lockMgr)

	// Create directory manager
	dirMgr := dir.NewManager(dev, inodeTbl, extentMgr, lockMgr, cfg.DataOffset)

	return &FS{
		dev:        dev,
		lockMgr:    lockMgr,
		bitmapMgr:  bitmapMgr,
		inodeTbl:   inodeTbl,
		extentMgr:  extentMgr,
		dirMgr:     dirMgr,
		nodeUUID:   cfg.NodeUUID,
		dataOffset: cfg.DataOffset,
		uid:        uint32(os.Getuid()),
		gid:        uint32(os.Getgid()),
	}, nil
}

// NewWithComponents creates an FS with pre-built components.
// This is useful for testing and for the mount command where
// components are configured externally.
func NewWithComponents(
	dev *storage.Device,
	bm *bitmap.Region,
	inodeTbl *inode.Table,
	extentMgr *extent.Manager,
	dirMgr *dir.Manager,
	cacheMgr *cache.Manager,
	nodeUUID [16]byte,
	dataOffset uint64,
) *FS {
	return &FS{
		dev:        dev,
		bitmapMgr:  bm,
		inodeTbl:   inodeTbl,
		extentMgr:  extentMgr,
		dirMgr:     dirMgr,
		cacheMgr:   cacheMgr,
		nodeUUID:   nodeUUID,
		dataOffset: dataOffset,
		uid:        uint32(os.Getuid()),
		gid:        uint32(os.Getgid()),
	}
}

// Close closes the filesystem and releases resources.
func (f *FS) Close() error {
	return f.dev.Close()
}

// getInode retrieves an inode, using the cache when available.
// This is the preferred method for read paths where the inode won't be modified.
func (f *FS) getInode(inodeNum uint32) (*inode.Inode, error) {
	if f.cacheMgr != nil {
		return f.cacheMgr.GetInode(inodeNum)
	}
	return f.inodeTbl.Read(inodeNum)
}

// invalidateInode removes an inode from the cache before modification.
// Safe to call even if cache is nil.
func (f *FS) invalidateInode(inodeNum uint32) {
	if f.cacheMgr != nil {
		f.cacheMgr.InvalidateInode(inodeNum)
	}
}

// cacheInode adds a freshly-modified inode to the cache for subsequent reads.
// Safe to call even if cache is nil.
func (f *FS) cacheInode(ino *inode.Inode) {
	if f.cacheMgr != nil {
		f.cacheMgr.CacheInode(ino)
	}
}

// lookupPhysical finds the physical block for a logical block, using cache.
// Returns 0 for holes (sparse file regions with no allocated blocks).
// This is the preferred method for read paths.
func (f *FS) lookupPhysical(ino *inode.Inode, logicalBlock uint64) (uint64, error) {
	if f.cacheMgr != nil {
		return f.cacheMgr.LookupPhysicalCached(f.extentMgr, ino, logicalBlock)
	}
	return f.extentMgr.LookupPhysical(ino, logicalBlock)
}

// CacheStats returns cache statistics for monitoring.
// Returns zero values if cache is not enabled.
func (f *FS) CacheStats() cache.Stats {
	if f.cacheMgr != nil {
		return f.cacheMgr.Stats()
	}
	return cache.Stats{}
}

// InitRoot initializes the root directory if it doesn't exist.
func (f *FS) InitRoot(ctx context.Context) error {
	// Check if root inode (2) exists
	rootIno, err := f.inodeTbl.Read(inode.InodeRoot)
	if err == nil && rootIno.Magic == inode.InodeMagic {
		// Root already exists
		return nil
	}

	// Initialize root inode via the inode table's init method
	if err := f.inodeTbl.InitRootInode(ctx); err != nil {
		return err
	}

	// Re-read the root inode
	rootIno, err = f.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		return err
	}

	// Set up root directory inode
	now := time.Now().UnixNano()
	rootIno.Mode = inode.S_IFDIR | 0755
	rootIno.UID = f.uid
	rootIno.GID = f.gid
	rootIno.LinkCount = 2 // . and parent (itself for root)
	rootIno.Atime = now
	rootIno.Mtime = now
	rootIno.Ctime = now

	// Initialize directory with . and ..
	if err := f.dirMgr.InitDirectory(ctx, rootIno, uint64(inode.InodeRoot)); err != nil {
		return err
	}

	// Write inode back
	return f.inodeTbl.Write(ctx, rootIno)
}

// Root returns the root directory node.
func (f *FS) Root() (fs.Node, error) {
	return &Dir{
		fs:       f,
		inodeNum: inode.InodeRoot,
	}, nil
}

// Statfs returns filesystem statistics.
func (f *FS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	totalBlocks := f.bitmapMgr.TotalBlocks()
	freeBlocks := f.bitmapMgr.FreeCount()

	resp.Blocks = totalBlocks
	resp.Bfree = freeBlocks
	resp.Bavail = freeBlocks
	resp.Files = f.inodeTbl.TotalInodes()
	resp.Ffree = f.inodeTbl.FreeCount()
	resp.Bsize = BlockSize
	resp.Namelen = 255
	resp.Frsize = BlockSize

	return nil
}

// Dir represents a directory in the filesystem.
type Dir struct {
	fs       *FS
	inodeNum uint32
}

var (
	_ fs.Node                = (*Dir)(nil)
	_ fs.HandleReadDirAller  = (*Dir)(nil)
	_ fs.NodeRequestLookuper = (*Dir)(nil)
	_ fs.NodeCreater         = (*Dir)(nil)
	_ fs.NodeMkdirer         = (*Dir)(nil)
	_ fs.NodeRemover         = (*Dir)(nil)
)

// Attr returns directory attributes.
func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	ino, err := d.fs.getInode(d.inodeNum)
	if err != nil {
		return syscall.EIO
	}

	// Disable FUSE kernel caching for cluster operation.
	// Other nodes may modify attributes at any time.
	a.Valid = 0

	a.Inode = uint64(ino.InodeNum)
	a.Mode = os.ModeDir | os.FileMode(ino.Mode&0777)
	a.Nlink = ino.LinkCount
	a.Uid = ino.UID
	a.Gid = ino.GID
	a.Size = ino.Size
	a.Atime = time.Unix(0, ino.Atime)
	a.Mtime = time.Unix(0, ino.Mtime)
	a.Ctime = time.Unix(0, ino.Ctime)
	a.Blocks = ino.AllocatedBlocks * (BlockSize / 512)

	return nil
}

// Lookup finds a child by name.
func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	dirIno, err := d.fs.getInode(d.inodeNum)
	if err != nil {
		return nil, syscall.EIO
	}

	childInodeNum, fileType, err := d.fs.dirMgr.Lookup(ctx, dirIno, req.Name)
	if err != nil {
		if err == dir.ErrNotFound {
			return nil, fuse.ENOENT
		}
		return nil, syscall.EIO
	}

	// Disable FUSE kernel caching for cluster operation.
	// Other nodes may modify the filesystem at any time.
	resp.EntryValid = 0
	resp.Attr.Valid = 0

	if fileType == dir.DT_DIR {
		return &Dir{fs: d.fs, inodeNum: uint32(childInodeNum)}, nil
	}
	return &File{fs: d.fs, inodeNum: uint32(childInodeNum)}, nil
}

// ReadDirAll returns all directory entries.
func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dirIno, err := d.fs.getInode(d.inodeNum)
	if err != nil {
		return nil, syscall.EIO
	}

	entries, err := d.fs.dirMgr.List(ctx, dirIno)
	if err != nil {
		return nil, syscall.EIO
	}

	var result []fuse.Dirent
	for _, entry := range entries {
		dtype := fuse.DT_File
		switch entry.FileType {
		case dir.DT_DIR:
			dtype = fuse.DT_Dir
		case dir.DT_LNK:
			dtype = fuse.DT_Link
		}

		result = append(result, fuse.Dirent{
			Inode: entry.InodeNum,
			Name:  entry.Name,
			Type:  dtype,
		})
	}

	return result, nil
}

// Create creates a new file.
//
// Lock ordering: This function uses a two-phase allocation pattern to respect
// the lock hierarchy (BlockBitmap Level 6 < Inode Level 8):
//  1. FindFreeSlot() - no locks
//  2. dirMgr.Add() - may acquire BlockBitmap lock (Level 6) for directory expansion
//  3. ClaimSlot() - acquires Inode lock (Level 8)
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	// Phase 1: Find a free inode slot (no distributed locks held)
	inodeNum, err := d.fs.inodeTbl.FindFreeSlot()
	if err != nil {
		log.Printf("[DEBUG] Create %q: FindFreeSlot failed: %v", req.Name, err)
		return nil, nil, syscall.ENOSPC
	}

	// Phase 2: Read parent directory inode
	dirIno, err := d.fs.inodeTbl.Read(d.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Create %q: Read parent inode %d failed: %v", req.Name, d.inodeNum, err)
		return nil, nil, syscall.EIO
	}

	// Phase 3: Add directory entry FIRST (may acquire BlockBitmap lock for directory expansion)
	// This respects lock ordering: BlockBitmap (Level 6) before Inode (Level 8)
	if err := d.fs.dirMgr.Add(ctx, dirIno, req.Name, uint64(inodeNum), dir.DT_REG); err != nil {
		log.Printf("[DEBUG] Create %q: dirMgr.Add failed: %v", req.Name, err)
		return nil, nil, syscall.EIO
	}

	// Phase 4: Claim the inode slot (acquires Inode lock)
	// If this fails, another node claimed the slot - rollback the directory entry
	childIno, err := d.fs.inodeTbl.ClaimSlot(ctx, inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Create %q: ClaimSlot %d failed: %v - rolling back", req.Name, inodeNum, err)
		// Rollback: remove the directory entry we just added
		if removeErr := d.fs.dirMgr.Remove(ctx, dirIno, req.Name); removeErr != nil {
			log.Printf("[ERROR] Create %q: rollback Remove failed: %v", req.Name, removeErr)
		}
		// If slot was taken by another node, retry with a different slot
		if err == inode.ErrInodeInUse {
			return nil, nil, syscall.EAGAIN
		}
		return nil, nil, syscall.ENOSPC
	}

	// Phase 5: Initialize file inode attributes
	now := time.Now().UnixNano()
	childIno.Mode = uint16(req.Mode & 0777)
	childIno.UID = req.Uid
	childIno.GID = req.Gid
	childIno.LinkCount = 1
	childIno.Atime = now
	childIno.Mtime = now
	childIno.Ctime = now

	// Phase 6: Write updated inode
	if err := d.fs.inodeTbl.Write(ctx, childIno); err != nil {
		log.Printf("[DEBUG] Create %q: Write inode %d failed: %v", req.Name, childIno.InodeNum, err)
		// Rollback both: free inode and remove directory entry
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		d.fs.dirMgr.Remove(ctx, dirIno, req.Name)
		return nil, nil, syscall.EIO
	}

	// Update directory mtime
	dirIno.Mtime = now
	d.fs.inodeTbl.Write(ctx, dirIno)

	file := &File{fs: d.fs, inodeNum: childIno.InodeNum}
	return file, file, nil
}

// Mkdir creates a new directory.
//
// Lock ordering: This function uses a two-phase allocation pattern to respect
// the lock hierarchy (BlockBitmap Level 6 < Inode Level 8):
//  1. FindFreeSlot() - no locks
//  2. dirMgr.InitDirectory() - allocates blocks first (Level 6), then acquires inode lock
//  3. dirMgr.Add() - may acquire BlockBitmap lock (Level 6) for parent directory expansion
//  4. ClaimSlot() - acquires Inode lock (Level 8)
//
// Note: dirMgr.InitDirectory() already handles lock ordering internally by allocating
// blocks before acquiring the inode lock.
func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	// Phase 1: Find a free inode slot (no distributed locks held)
	inodeNum, err := d.fs.inodeTbl.FindFreeSlot()
	if err != nil {
		log.Printf("[DEBUG] Mkdir %q: FindFreeSlot failed: %v", req.Name, err)
		return nil, syscall.ENOSPC
	}

	// Phase 2: Read parent directory inode
	dirIno, err := d.fs.inodeTbl.Read(d.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Mkdir %q: Read parent inode %d failed: %v", req.Name, d.inodeNum, err)
		return nil, syscall.EIO
	}

	// Phase 3: Add directory entry in parent FIRST (may acquire BlockBitmap lock)
	// This respects lock ordering: BlockBitmap (Level 6) before Inode (Level 8)
	if err := d.fs.dirMgr.Add(ctx, dirIno, req.Name, uint64(inodeNum), dir.DT_DIR); err != nil {
		log.Printf("[DEBUG] Mkdir %q: dirMgr.Add failed: %v", req.Name, err)
		return nil, syscall.EIO
	}

	// Phase 4: Claim the inode slot (acquires Inode lock)
	childIno, err := d.fs.inodeTbl.ClaimSlot(ctx, inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Mkdir %q: ClaimSlot %d failed: %v - rolling back", req.Name, inodeNum, err)
		// Rollback: remove the directory entry we just added
		if removeErr := d.fs.dirMgr.Remove(ctx, dirIno, req.Name); removeErr != nil {
			log.Printf("[ERROR] Mkdir %q: rollback Remove failed: %v", req.Name, removeErr)
		}
		if err == inode.ErrInodeInUse {
			return nil, syscall.EAGAIN
		}
		return nil, syscall.ENOSPC
	}

	// Phase 5: Initialize directory inode attributes
	now := time.Now().UnixNano()
	childIno.Mode = inode.S_IFDIR | uint16(req.Mode&0777)
	childIno.UID = req.Uid
	childIno.GID = req.Gid
	childIno.LinkCount = 2 // . and parent
	childIno.Atime = now
	childIno.Mtime = now
	childIno.Ctime = now

	// Phase 6: Initialize directory contents (allocates data block for . and .. entries)
	// Note: InitDirectory() handles lock ordering internally
	if err := d.fs.dirMgr.InitDirectory(ctx, childIno, uint64(d.inodeNum)); err != nil {
		log.Printf("[DEBUG] Mkdir %q: InitDirectory failed: %v", req.Name, err)
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		d.fs.dirMgr.Remove(ctx, dirIno, req.Name)
		return nil, syscall.EIO
	}

	// Phase 7: Write updated inode
	if err := d.fs.inodeTbl.Write(ctx, childIno); err != nil {
		log.Printf("[DEBUG] Mkdir %q: Write inode failed: %v", req.Name, err)
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		d.fs.dirMgr.Remove(ctx, dirIno, req.Name)
		return nil, syscall.EIO
	}

	// Increment parent link count (for ..)
	dirIno.LinkCount++
	dirIno.Mtime = now
	d.fs.inodeTbl.Write(ctx, dirIno)

	return &Dir{fs: d.fs, inodeNum: childIno.InodeNum}, nil
}

// Remove removes a file or empty directory.
func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	// Invalidate parent directory cache before modifying
	d.fs.invalidateInode(d.inodeNum)

	dirIno, err := d.fs.inodeTbl.Read(d.inodeNum)
	if err != nil {
		return syscall.EIO
	}

	// Look up the entry
	childInodeNum, fileType, err := d.fs.dirMgr.Lookup(ctx, dirIno, req.Name)
	if err != nil {
		if err == dir.ErrNotFound {
			return fuse.ENOENT
		}
		return syscall.EIO
	}

	// Invalidate child inode cache - it's being removed
	d.fs.invalidateInode(uint32(childInodeNum))

	// Handle directory removal
	if req.Dir {
		if fileType != dir.DT_DIR {
			return syscall.ENOTDIR
		}

		// Check if directory is empty
		childIno, err := d.fs.inodeTbl.Read(uint32(childInodeNum))
		if err != nil {
			return syscall.EIO
		}

		isEmpty, err := d.fs.dirMgr.IsEmpty(ctx, childIno)
		if err != nil {
			return syscall.EIO
		}
		if !isEmpty {
			return syscall.ENOTEMPTY
		}

		// Decrement parent link count
		dirIno.LinkCount--
	}

	// Remove directory entry
	if err := d.fs.dirMgr.Remove(ctx, dirIno, req.Name); err != nil {
		return syscall.EIO
	}

	// Decrement child link count and potentially free
	childIno, err := d.fs.inodeTbl.Read(uint32(childInodeNum))
	if err != nil {
		return syscall.EIO
	}

	childIno.LinkCount--
	if childIno.LinkCount == 0 {
		// Free all extents
		d.fs.extentMgr.Truncate(ctx, childIno, 0)
		// Free inode
		d.fs.inodeTbl.Free(ctx, uint32(childInodeNum))
	} else {
		d.fs.inodeTbl.Write(ctx, childIno)
		// Cache the updated child inode
		d.fs.cacheInode(childIno)
	}

	// Update directory mtime
	now := time.Now().UnixNano()
	dirIno.Mtime = now
	d.fs.inodeTbl.Write(ctx, dirIno)

	// Cache the updated parent directory inode
	d.fs.cacheInode(dirIno)

	return nil
}

// File represents a file in the filesystem.
type File struct {
	fs       *FS
	inodeNum uint32
}

var (
	_ fs.Node          = (*File)(nil)
	_ fs.HandleReader  = (*File)(nil)
	_ fs.HandleWriter  = (*File)(nil)
	_ fs.NodeSetattrer = (*File)(nil)
	_ fs.NodeFsyncer   = (*File)(nil)
)

// Attr returns file attributes.
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	ino, err := f.fs.getInode(f.inodeNum)
	if err != nil {
		return syscall.EIO
	}

	// Disable FUSE kernel caching for cluster operation.
	// Other nodes may modify attributes at any time.
	a.Valid = 0

	a.Inode = uint64(ino.InodeNum)
	a.Mode = os.FileMode(ino.Mode & 0777)
	a.Nlink = ino.LinkCount
	a.Uid = ino.UID
	a.Gid = ino.GID
	a.Size = ino.Size
	a.Atime = time.Unix(0, ino.Atime)
	a.Mtime = time.Unix(0, ino.Mtime)
	a.Ctime = time.Unix(0, ino.Ctime)
	a.Blocks = ino.AllocatedBlocks * (BlockSize / 512)

	return nil
}

// Read reads file data.
func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	ino, err := f.fs.getInode(f.inodeNum)
	if err != nil {
		return syscall.EIO
	}

	// Check bounds
	if uint64(req.Offset) >= ino.Size {
		resp.Data = nil
		return nil
	}

	// Calculate how much to read
	readSize := req.Size
	if uint64(req.Offset)+uint64(readSize) > ino.Size {
		readSize = int(ino.Size - uint64(req.Offset))
	}

	// Read data block by block
	data := make([]byte, readSize)
	offset := uint64(req.Offset)
	bytesRead := 0

	for bytesRead < readSize {
		blockNum := offset / BlockSize
		blockOffset := offset % BlockSize

		// Find physical block for this logical block (uses extent cache)
		physBlock, err := f.fs.lookupPhysical(ino, blockNum)
		if err != nil {
			return syscall.EIO
		}
		if physBlock == 0 {
			// Hole - return zeros
			toRead := int(BlockSize - blockOffset)
			if toRead > readSize-bytesRead {
				toRead = readSize - bytesRead
			}
			// data is already zeroed
			bytesRead += toRead
			offset += uint64(toRead)
			continue
		}

		// Calculate physical disk location
		diskOffset := int64(f.fs.dataOffset + physBlock*BlockSize + blockOffset)

		// Read from disk
		toRead := int(BlockSize - blockOffset)
		if toRead > readSize-bytesRead {
			toRead = readSize - bytesRead
		}

		buf := storage.GetBuffer(storage.SectorSize)
		alignedOffset := diskOffset &^ (storage.SectorSize - 1)
		sectorOffset := diskOffset - alignedOffset

		for bytesInBlock := 0; bytesInBlock < toRead; {
			n, err := f.fs.dev.ReadAt(buf.Bytes(), alignedOffset)
			if err != nil || n != storage.SectorSize {
				storage.PutBuffer(buf)
				return syscall.EIO
			}

			copyLen := storage.SectorSize - int(sectorOffset)
			if copyLen > toRead-bytesInBlock {
				copyLen = toRead - bytesInBlock
			}
			copy(data[bytesRead+bytesInBlock:], buf.Bytes()[sectorOffset:sectorOffset+int64(copyLen)])

			bytesInBlock += copyLen
			alignedOffset += storage.SectorSize
			sectorOffset = 0
		}
		storage.PutBuffer(buf)

		bytesRead += toRead
		offset += uint64(toRead)
	}

	resp.Data = data
	return nil
}

// Write writes file data.
// Uses an optimistic retry pattern to handle concurrent modifications from other nodes.
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	// Retry loop to handle concurrent modifications from other nodes.
	// When multiple nodes try to extend the same file simultaneously, they may race
	// to allocate blocks. If a conflict is detected, errRetryWrite is returned and
	// we retry the operation.
	for attempt := 0; attempt < MaxWriteRetries; attempt++ {
		bytesWritten, err := f.writeInternal(ctx, req)
		if err == errRetryWrite {
			log.Printf("[DEBUG] Write inode %d: retry attempt %d due to concurrent modification",
				f.inodeNum, attempt+1)
			continue
		}
		if err != nil {
			return err
		}
		resp.Size = bytesWritten
		return nil
	}

	log.Printf("[ERROR] Write inode %d: concurrent modification detected after %d retries",
		f.inodeNum, MaxWriteRetries)
	return syscall.EAGAIN
}

// writeInternal performs the actual write operation.
// Returns errRetryWrite if a concurrent modification is detected and the caller should retry.
func (f *File) writeInternal(ctx context.Context, req *fuse.WriteRequest) (int, error) {
	offset := uint64(req.Offset)
	data := req.Data
	bytesWritten := 0

	// Track if we hold the inode lock
	lockHeld := false
	defer func() {
		if lockHeld && f.fs.lockMgr != nil {
			f.fs.lockMgr.Release(lock.LockTypeInode, uint64(f.inodeNum))
		}
	}()

	// Read initial inode state
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Write inode %d: Read failed: %v", f.inodeNum, err)
		return 0, syscall.EIO
	}

	for bytesWritten < len(data) {
		blockNum := offset / BlockSize
		blockOffset := offset % BlockSize

		// Try to find existing extent first (without lock for read-only check)
		ext, extOffset, err := f.fs.extentMgr.Lookup(ino, blockNum)
		if err != nil {
			// Need to allocate - must acquire lock first
			if !lockHeld && f.fs.lockMgr != nil {
				if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
					log.Printf("[DEBUG] Write inode %d: failed to acquire lock: %v", f.inodeNum, err)
					return 0, syscall.EIO
				}
				lockHeld = true

				// Re-read inode after acquiring lock to detect concurrent changes
				freshIno, err := f.fs.inodeTbl.Read(f.inodeNum)
				if err != nil {
					log.Printf("[DEBUG] Write inode %d: re-read failed: %v", f.inodeNum, err)
					return 0, syscall.EIO
				}

				// Check if MetaGeneration changed (concurrent modification)
				if freshIno.MetaGeneration != ino.MetaGeneration {
					log.Printf("[DEBUG] Write inode %d: MetaGeneration changed %d -> %d, retrying",
						f.inodeNum, ino.MetaGeneration, freshIno.MetaGeneration)
					return 0, errRetryWrite
				}
				ino = freshIno

				// Re-check if block now exists (another node may have allocated it)
				ext, extOffset, err = f.fs.extentMgr.Lookup(ino, blockNum)
				if err == nil {
					// Block was allocated by another node - use it
					goto writeBlock
				}
			}

			// Allocate block while holding lock
			ext, err = f.fs.extentMgr.AllocateBlocks(ctx, ino, blockNum, 1)
			if err != nil {
				// Check for extent conflicts indicating concurrent allocation
				if err == extent.ErrExtentOverlap || err == extent.ErrNoSpaceForExtent {
					log.Printf("[DEBUG] Write inode %d: extent conflict at block %d, retrying",
						f.inodeNum, blockNum)
					return 0, errRetryWrite
				}
				log.Printf("[DEBUG] Write inode %d: AllocateBlocks failed: %v", f.inodeNum, err)
				return 0, syscall.ENOSPC
			}
			extOffset = 0
		}

	writeBlock:
		// Calculate physical location
		physBlock := ext.PhysicalBlock + extOffset
		diskOffset := int64(f.fs.dataOffset + physBlock*BlockSize + blockOffset)

		// Write to disk
		toWrite := int(BlockSize - blockOffset)
		if toWrite > len(data)-bytesWritten {
			toWrite = len(data) - bytesWritten
		}

		// Handle unaligned writes by read-modify-write
		buf := storage.GetBuffer(storage.SectorSize)
		alignedOffset := diskOffset &^ (storage.SectorSize - 1)
		sectorOffset := diskOffset - alignedOffset

		for bytesInBlock := 0; bytesInBlock < toWrite; {
			// Read existing sector
			if _, err := f.fs.dev.ReadAt(buf.Bytes(), alignedOffset); err != nil {
				// New sector - zero it
				for i := range buf.Bytes() {
					buf.Bytes()[i] = 0
				}
			}

			// Modify
			copyLen := storage.SectorSize - int(sectorOffset)
			if copyLen > toWrite-bytesInBlock {
				copyLen = toWrite - bytesInBlock
			}
			copy(buf.Bytes()[sectorOffset:], data[bytesWritten+bytesInBlock:bytesWritten+bytesInBlock+copyLen])

			// Write back
			if _, err := f.fs.dev.WriteAt(buf.Bytes(), alignedOffset); err != nil {
				log.Printf("[DEBUG] Write inode %d: WriteAt offset %d failed: %v", f.inodeNum, alignedOffset, err)
				storage.PutBuffer(buf)
				return 0, syscall.EIO
			}

			bytesInBlock += copyLen
			alignedOffset += storage.SectorSize
			sectorOffset = 0
		}
		storage.PutBuffer(buf)

		bytesWritten += toWrite
		offset += uint64(toWrite)
	}

	// Update inode size if extended
	if offset > ino.Size {
		ino.Size = offset
	}
	ino.Mtime = time.Now().UnixNano()

	// Write inode - use WriteWithLockHeld if we hold the lock
	if lockHeld {
		if err := f.fs.inodeTbl.WriteWithLockHeld(ino); err != nil {
			log.Printf("[DEBUG] Write inode %d: WriteWithLockHeld failed: %v", f.inodeNum, err)
			return 0, syscall.EIO
		}
	} else {
		if err := f.fs.inodeTbl.Write(ctx, ino); err != nil {
			log.Printf("[DEBUG] Write inode %d: final inodeTbl.Write failed: %v", f.inodeNum, err)
			return 0, syscall.EIO
		}
	}

	// Cache the freshly-written inode for subsequent reads
	f.fs.cacheInode(ino)

	return bytesWritten, nil
}

// Setattr sets file attributes (used for truncate).
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	// Invalidate cache before modifying - ensures we read fresh data
	f.fs.invalidateInode(f.inodeNum)

	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		return syscall.EIO
	}

	now := time.Now().UnixNano()

	if req.Valid.Size() {
		if req.Size < ino.Size {
			// Truncate
			if err := f.fs.extentMgr.Truncate(ctx, ino, req.Size); err != nil {
				return syscall.EIO
			}
		}
		ino.Size = req.Size
		ino.Mtime = now
	}

	if req.Valid.Mode() {
		ino.Mode = uint16(req.Mode & 0777)
		ino.Ctime = now
	}

	if req.Valid.Uid() {
		ino.UID = req.Uid
		ino.Ctime = now
	}

	if req.Valid.Gid() {
		ino.GID = req.Gid
		ino.Ctime = now
	}

	if req.Valid.Atime() {
		ino.Atime = req.Atime.UnixNano()
	}

	if req.Valid.Mtime() {
		ino.Mtime = req.Mtime.UnixNano()
	}

	if err := f.fs.inodeTbl.Write(ctx, ino); err != nil {
		return syscall.EIO
	}

	// Cache the freshly-written inode for subsequent reads
	f.fs.cacheInode(ino)

	return f.Attr(ctx, &resp.Attr)
}

// Fsync flushes file data to disk.
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return f.fs.dev.Sync()
}
