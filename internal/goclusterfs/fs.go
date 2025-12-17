// Package goclusterfs implements a FUSE filesystem using GoClusterFS structures.
//
// This package integrates all Phase 2 components (bitmap, inode, extent, dir)
// into a working FUSE filesystem that can be mounted and used.
package goclusterfs

import (
	"context"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

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
	nodeUUID [16]byte,
	dataOffset uint64,
) *FS {
	return &FS{
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
}

// Close closes the filesystem and releases resources.
func (f *FS) Close() error {
	return f.dev.Close()
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
	ino, err := d.fs.inodeTbl.Read(d.inodeNum)
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
	dirIno, err := d.fs.inodeTbl.Read(d.inodeNum)
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
	dirIno, err := d.fs.inodeTbl.Read(d.inodeNum)
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
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	// Allocate new inode
	childIno, err := d.fs.inodeTbl.Alloc(ctx)
	if err != nil {
		log.Printf("[DEBUG] Create %q: Alloc failed: %v", req.Name, err)
		return nil, nil, syscall.ENOSPC
	}

	// Initialize file inode
	now := time.Now().UnixNano()
	childIno.Mode = uint16(req.Mode & 0777)
	childIno.UID = req.Uid
	childIno.GID = req.Gid
	childIno.LinkCount = 1
	childIno.Atime = now
	childIno.Mtime = now
	childIno.Ctime = now

	// Write inode
	if err := d.fs.inodeTbl.Write(ctx, childIno); err != nil {
		log.Printf("[DEBUG] Create %q: Write inode %d failed: %v", req.Name, childIno.InodeNum, err)
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		return nil, nil, syscall.EIO
	}

	// Add directory entry
	dirIno, err := d.fs.inodeTbl.Read(d.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Create %q: Read parent inode %d failed: %v", req.Name, d.inodeNum, err)
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		return nil, nil, syscall.EIO
	}

	if err := d.fs.dirMgr.Add(ctx, dirIno, req.Name, uint64(childIno.InodeNum), dir.DT_REG); err != nil {
		log.Printf("[DEBUG] Create %q: dirMgr.Add failed: %v", req.Name, err)
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		return nil, nil, syscall.EIO
	}

	// Update directory mtime
	dirIno.Mtime = now
	d.fs.inodeTbl.Write(ctx, dirIno)

	file := &File{fs: d.fs, inodeNum: childIno.InodeNum}
	return file, file, nil
}

// Mkdir creates a new directory.
func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	// Allocate new inode
	childIno, err := d.fs.inodeTbl.Alloc(ctx)
	if err != nil {
		return nil, syscall.ENOSPC
	}

	// Initialize directory inode
	now := time.Now().UnixNano()
	childIno.Mode = inode.S_IFDIR | uint16(req.Mode&0777)
	childIno.UID = req.Uid
	childIno.GID = req.Gid
	childIno.LinkCount = 2 // . and parent
	childIno.Atime = now
	childIno.Mtime = now
	childIno.Ctime = now

	// Initialize directory contents
	if err := d.fs.dirMgr.InitDirectory(ctx, childIno, uint64(d.inodeNum)); err != nil {
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		return nil, syscall.EIO
	}

	// Write inode
	if err := d.fs.inodeTbl.Write(ctx, childIno); err != nil {
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		return nil, syscall.EIO
	}

	// Add directory entry in parent
	dirIno, err := d.fs.inodeTbl.Read(d.inodeNum)
	if err != nil {
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		return nil, syscall.EIO
	}

	if err := d.fs.dirMgr.Add(ctx, dirIno, req.Name, uint64(childIno.InodeNum), dir.DT_DIR); err != nil {
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
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
	}

	// Update directory mtime
	now := time.Now().UnixNano()
	dirIno.Mtime = now
	d.fs.inodeTbl.Write(ctx, dirIno)

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
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
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
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
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

		// Find extent for this block
		ext, extOffset, err := f.fs.extentMgr.Lookup(ino, blockNum)
		if err != nil {
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

		// Calculate physical location
		physBlock := ext.PhysicalBlock + extOffset
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
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Write inode %d: Read failed: %v", f.inodeNum, err)
		return syscall.EIO
	}

	offset := uint64(req.Offset)
	data := req.Data
	bytesWritten := 0

	for bytesWritten < len(data) {
		blockNum := offset / BlockSize
		blockOffset := offset % BlockSize

		// Allocate block if needed
		ext, extOffset, err := f.fs.extentMgr.Lookup(ino, blockNum)
		if err != nil {
			// Need to allocate
			ext, err = f.fs.extentMgr.AllocateBlocks(ctx, ino, blockNum, 1)
			if err != nil {
				log.Printf("[DEBUG] Write inode %d: AllocateBlocks failed: %v", f.inodeNum, err)
				return syscall.ENOSPC
			}
			extOffset = 0
		}

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
				return syscall.EIO
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

	if err := f.fs.inodeTbl.Write(ctx, ino); err != nil {
		log.Printf("[DEBUG] Write inode %d: final inodeTbl.Write failed: %v", f.inodeNum, err)
		return syscall.EIO
	}

	resp.Size = bytesWritten
	return nil
}

// Setattr sets file attributes (used for truncate).
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
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

	return f.Attr(ctx, &resp.Attr)
}

// Fsync flushes file data to disk.
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return f.fs.dev.Sync()
}
