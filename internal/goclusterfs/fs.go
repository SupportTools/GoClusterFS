// Package goclusterfs implements a FUSE filesystem using GoClusterFS structures.
//
// This package integrates all Phase 2 components (bitmap, inode, extent, dir)
// into a working FUSE filesystem that can be mounted and used.
//
// Uses hanwen/go-fuse v2 for proper FUSE capability negotiation (including fallocate).
package goclusterfs

import (
	"context"
	"errors"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/cache"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
	"github.com/supporttools/GoClusterFS/internal/trim"
)

// errRetryWrite is an internal sentinel error indicating that a concurrent
// modification was detected and the Write operation should be retried.
var errRetryWrite = errors.New("retry write operation")

// MaxWriteRetries is the maximum number of retry attempts for Write operations
// when concurrent modifications are detected.
const MaxWriteRetries = 3

// errRetryCreate is an internal sentinel error indicating that an inode slot
// collision was detected during Create/Mkdir and the operation should be retried.
var errRetryCreate = errors.New("retry create: inode slot collision")

// MaxCreateRetries is the maximum number of retry attempts for Create/Mkdir
// operations when inode slot collisions occur across cluster nodes.
const MaxCreateRetries = 5

// BlockSize is the size of data blocks (1MB).
const BlockSize = 1 << 20

// FS represents the GoClusterFS filesystem root.
// Embeds fs.Inode for go-fuse v2 node identity.
type FS struct {
	fs.Inode

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
	cacheMgr  *cache.Manager   // Inode cache for read optimization
	journal   *journal.Journal // Per-node journal for crash consistency (WAL)
	trimMgr   *trim.Manager    // TRIM manager for batched block discards

	// Configuration
	nodeUUID   [16]byte
	dataOffset uint64 // Offset where data blocks start on disk

	// Runtime state
	uid uint32
	gid uint32
}

// Verify interface compliance at compile time
// FS is the root directory node - it implements all directory operations
var (
	_ fs.InodeEmbedder = (*FS)(nil)
	_ fs.NodeOnAdder   = (*FS)(nil)
	_ fs.NodeStatfser  = (*FS)(nil)
	_ fs.NodeGetattrer = (*FS)(nil)
	_ fs.NodeLookuper  = (*FS)(nil)
	_ fs.NodeReaddirer = (*FS)(nil)
	_ fs.NodeCreater   = (*FS)(nil)
	_ fs.NodeMkdirer   = (*FS)(nil)
	_ fs.NodeUnlinker  = (*FS)(nil)
	_ fs.NodeRmdirer   = (*FS)(nil)
)

// Config holds filesystem configuration.
type Config struct {
	DevicePath string
	NodeUUID   [16]byte

	// Region offsets (from superblock or config)
	BitmapOffset  uint64
	InodeOffset   uint64
	DataOffset    uint64
	JournalOffset uint64 // Journal region start offset (0 = no journal)

	// Region sizes
	TotalBlocks uint64 // Number of data blocks
	InodeCount  uint64 // Number of inodes
	JournalSize uint64 // Journal region size (default 4MB if JournalOffset > 0)
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
	j *journal.Journal, // May be nil for backward compatibility
	tm *trim.Manager, // May be nil if TRIM is disabled
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
		journal:    j,
		trimMgr:    tm,
		nodeUUID:   nodeUUID,
		dataOffset: dataOffset,
		uid:        uint32(os.Getuid()),
		gid:        uint32(os.Getgid()),
	}
}

// Close closes the filesystem and releases resources.
func (f *FS) Close() error {
	// Stop TRIM manager first to flush pending discards
	if f.trimMgr != nil {
		if err := f.trimMgr.Stop(); err != nil {
			log.Printf("Warning: failed to stop TRIM manager cleanly: %v", err)
		}
	}

	// Stop journal to ensure clean shutdown
	if f.journal != nil {
		if err := f.journal.Stop(); err != nil {
			log.Printf("Warning: failed to stop journal cleanly: %v", err)
		}
	}
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

// RootDir returns the root directory node for testing purposes.
// This is a helper method to support tests that need direct access to
// the root Dir without going through the FUSE layer.
func (f *FS) RootDir() *Dir {
	return &Dir{
		fs:       f,
		inodeNum: inode.InodeRoot,
	}
}

// hashString returns a 64-bit FNV-1a hash of the string.
// Used for journal verification of filenames.
func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
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

// OnAdd is called when the filesystem is mounted.
// In go-fuse v2, the FS node passed to Mount() IS the root directory.
// We don't need to add any children here - FS implements the directory
// operations directly for the root.
func (f *FS) OnAdd(ctx context.Context) {
	// FS itself is the root directory node (inode 2).
	// Nothing special needed here - FS implements all directory interfaces.
}

// Statfs returns filesystem statistics.
func (f *FS) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	f.mu.RLock()
	defer f.mu.RUnlock()

	totalBlocks := f.bitmapMgr.TotalBlocks()
	freeBlocks := f.bitmapMgr.FreeCount()

	out.Blocks = totalBlocks
	out.Bfree = freeBlocks
	out.Bavail = freeBlocks
	out.Files = f.inodeTbl.TotalInodes()
	out.Ffree = f.inodeTbl.FreeCount()
	out.Bsize = BlockSize
	out.NameLen = 255
	out.Frsize = BlockSize

	return 0
}

// Getattr returns the root directory attributes.
func (f *FS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	ino, err := f.getInode(inode.InodeRoot)
	if err != nil {
		return syscall.EIO
	}

	// Disable FUSE kernel caching for cluster operation.
	out.AttrValid = 0

	out.Ino = uint64(ino.InodeNum)
	out.Mode = uint32(fuse.S_IFDIR | (ino.Mode & 0777))
	out.Nlink = uint32(ino.LinkCount)
	out.Uid = ino.UID
	out.Gid = ino.GID
	out.Size = ino.Size
	out.Atime = uint64(ino.Atime / 1e9)
	out.Atimensec = uint32(ino.Atime % 1e9)
	out.Mtime = uint64(ino.Mtime / 1e9)
	out.Mtimensec = uint32(ino.Mtime % 1e9)
	out.Ctime = uint64(ino.Ctime / 1e9)
	out.Ctimensec = uint32(ino.Ctime % 1e9)
	out.Blocks = ino.AllocatedBlocks * (BlockSize / 512)
	out.Blksize = BlockSize

	return 0
}

// Lookup finds a child of the root directory by name.
func (f *FS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	dirIno, err := f.getInode(inode.InodeRoot)
	if err != nil {
		return nil, syscall.EIO
	}

	childInodeNum, fileType, err := f.dirMgr.Lookup(ctx, dirIno, name)
	if err != nil {
		if err == dir.ErrNotFound {
			return nil, syscall.ENOENT
		}
		return nil, syscall.EIO
	}

	// Disable FUSE kernel caching for cluster operation.
	out.EntryValid = 0
	out.AttrValid = 0

	if fileType == dir.DT_DIR {
		childDir := &Dir{fs: f, inodeNum: uint32(childInodeNum)}
		ch := f.NewPersistentInode(ctx, childDir, fs.StableAttr{
			Mode: fuse.S_IFDIR,
			Ino:  childInodeNum,
		})
		return ch, 0
	}

	childFile := &File{fs: f, inodeNum: uint32(childInodeNum)}
	ch := f.NewPersistentInode(ctx, childFile, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  childInodeNum,
	})
	return ch, 0
}

// Readdir returns a DirStream for the root directory listing.
func (f *FS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	dirIno, err := f.getInode(inode.InodeRoot)
	if err != nil {
		return nil, syscall.EIO
	}

	entries, err := f.dirMgr.List(ctx, dirIno)
	if err != nil {
		return nil, syscall.EIO
	}

	var result []fuse.DirEntry
	for _, entry := range entries {
		mode := uint32(fuse.S_IFREG)
		switch entry.FileType {
		case dir.DT_DIR:
			mode = fuse.S_IFDIR
		case dir.DT_LNK:
			mode = fuse.S_IFLNK
		}

		result = append(result, fuse.DirEntry{
			Ino:  entry.InodeNum,
			Name: entry.Name,
			Mode: mode,
		})
	}

	return &dirStream{entries: result}, 0
}

// Create creates a new file in the root directory.
func (f *FS) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	for attempt := 0; attempt < MaxCreateRetries; attempt++ {
		node, fh, fuseFlags, errno = f.createFileInternal(ctx, name, flags, mode, out)
		if errno == syscall.EAGAIN {
			baseDelay := time.Millisecond * time.Duration(1<<attempt)
			jitter := time.Duration(rand.Int63n(int64(baseDelay)))
			backoff := baseDelay + jitter
			log.Printf("[DEBUG] Create %q: inode slot collision, retry %d/%d (backoff %v)", name, attempt+1, MaxCreateRetries, backoff)
			time.Sleep(backoff)
			continue
		}
		return
	}
	log.Printf("[WARN] Create %q: exhausted %d retries due to inode slot collisions", name, MaxCreateRetries)
	return nil, nil, 0, syscall.EAGAIN
}

// createFileInternal implements file creation for the root directory.
func (f *FS) createFileInternal(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	inodeNum, err := f.inodeTbl.FindFreeSlot()
	if err != nil {
		return nil, nil, 0, syscall.ENOSPC
	}

	dirIno, err := f.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	uid := f.uid
	gid := f.gid
	caller, ok := fuse.FromContext(ctx)
	if ok {
		uid = caller.Uid
		gid = caller.Gid
	}

	// WAL logging
	var txn *journal.Transaction
	if f.journal != nil {
		txn = f.journal.Begin()
		if err := txn.LogInodeCreate(&journal.InodeCreatePayload{
			InodeNum:       uint64(inodeNum),
			Mode:           mode,
			UID:            uid,
			GID:            gid,
			ParentInodeNum: uint64(inode.InodeRoot),
			NameHash:       hashString(name),
		}); err != nil {
			f.journal.Abort(txn)
			return nil, nil, 0, syscall.EIO
		}
		if err := txn.LogDirAddEntry(&journal.DirAddEntryPayload{
			ParentInodeNum: uint64(inode.InodeRoot),
			ChildInodeNum:  uint64(inodeNum),
			EntryType:      journal.DirEntryRegular,
			Name:           name,
		}); err != nil {
			f.journal.Abort(txn)
			return nil, nil, 0, syscall.EIO
		}
		if err := f.journal.Commit(txn); err != nil {
			return nil, nil, 0, syscall.EIO
		}
	}

	if err := f.dirMgr.Add(ctx, dirIno, name, uint64(inodeNum), dir.DT_REG); err != nil {
		return nil, nil, 0, syscall.EIO
	}

	childIno, err := f.inodeTbl.ClaimSlot(ctx, inodeNum)
	if err != nil {
		f.dirMgr.Remove(ctx, dirIno, name)
		if err == inode.ErrInodeInUse {
			return nil, nil, 0, syscall.EAGAIN
		}
		return nil, nil, 0, syscall.ENOSPC
	}

	now := time.Now().UnixNano()
	childIno.Mode = inode.S_IFREG | uint16(mode&0777)
	childIno.UID = uid
	childIno.GID = gid
	childIno.LinkCount = 1
	childIno.Atime = now
	childIno.Mtime = now
	childIno.Ctime = now

	if err := f.inodeTbl.Write(ctx, childIno); err != nil {
		f.inodeTbl.Free(ctx, childIno.InodeNum)
		f.dirMgr.Remove(ctx, dirIno, name)
		return nil, nil, 0, syscall.EIO
	}

	dirIno.Mtime = now
	f.inodeTbl.Write(ctx, dirIno)

	file := &File{fs: f, inodeNum: childIno.InodeNum}
	ch := f.NewPersistentInode(ctx, file, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  uint64(childIno.InodeNum),
	})

	out.EntryValid = 0
	out.AttrValid = 0
	out.Ino = uint64(childIno.InodeNum)
	out.Mode = uint32(fuse.S_IFREG | (mode & 0777))
	out.Nlink = 1
	out.Uid = uid
	out.Gid = gid
	out.Size = 0
	out.Atime = uint64(now / 1e9)
	out.Atimensec = uint32(now % 1e9)
	out.Mtime = uint64(now / 1e9)
	out.Mtimensec = uint32(now % 1e9)
	out.Ctime = uint64(now / 1e9)
	out.Ctimensec = uint32(now % 1e9)
	out.Blksize = BlockSize

	return ch, nil, 0, 0
}

// Mkdir creates a new directory in the root directory.
func (f *FS) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	for attempt := 0; attempt < MaxCreateRetries; attempt++ {
		node, errno := f.mkdirInternal(ctx, name, mode, out)
		if errno == syscall.EAGAIN {
			baseDelay := time.Millisecond * time.Duration(1<<attempt)
			jitter := time.Duration(rand.Int63n(int64(baseDelay)))
			backoff := baseDelay + jitter
			log.Printf("[DEBUG] Mkdir %q: inode slot collision, retry %d/%d (backoff %v)", name, attempt+1, MaxCreateRetries, backoff)
			time.Sleep(backoff)
			continue
		}
		return node, errno
	}
	log.Printf("[WARN] Mkdir %q: exhausted %d retries due to inode slot collisions", name, MaxCreateRetries)
	return nil, syscall.EAGAIN
}

// mkdirInternal implements directory creation for the root directory.
func (f *FS) mkdirInternal(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	inodeNum, err := f.inodeTbl.FindFreeSlot()
	if err != nil {
		return nil, syscall.ENOSPC
	}

	dirIno, err := f.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		return nil, syscall.EIO
	}

	uid := f.uid
	gid := f.gid
	caller, ok := fuse.FromContext(ctx)
	if ok {
		uid = caller.Uid
		gid = caller.Gid
	}

	// WAL logging
	var txn *journal.Transaction
	if f.journal != nil {
		txn = f.journal.Begin()
		if err := txn.LogInodeCreate(&journal.InodeCreatePayload{
			InodeNum:       uint64(inodeNum),
			Mode:           mode | uint32(inode.S_IFDIR),
			UID:            uid,
			GID:            gid,
			ParentInodeNum: uint64(inode.InodeRoot),
			NameHash:       hashString(name),
		}); err != nil {
			f.journal.Abort(txn)
			return nil, syscall.EIO
		}
		if err := txn.LogDirAddEntry(&journal.DirAddEntryPayload{
			ParentInodeNum: uint64(inode.InodeRoot),
			ChildInodeNum:  uint64(inodeNum),
			EntryType:      journal.DirEntryDirectory,
			Name:           name,
		}); err != nil {
			f.journal.Abort(txn)
			return nil, syscall.EIO
		}
		if err := f.journal.Commit(txn); err != nil {
			return nil, syscall.EIO
		}
	}

	if err := f.dirMgr.Add(ctx, dirIno, name, uint64(inodeNum), dir.DT_DIR); err != nil {
		return nil, syscall.EIO
	}

	childIno, err := f.inodeTbl.ClaimSlot(ctx, inodeNum)
	if err != nil {
		f.dirMgr.Remove(ctx, dirIno, name)
		if err == inode.ErrInodeInUse {
			return nil, syscall.EAGAIN
		}
		return nil, syscall.ENOSPC
	}

	if err := f.dirMgr.InitDirectory(ctx, childIno, uint64(inode.InodeRoot)); err != nil {
		f.inodeTbl.Free(ctx, childIno.InodeNum)
		f.dirMgr.Remove(ctx, dirIno, name)
		return nil, syscall.EIO
	}

	now := time.Now().UnixNano()
	childIno.Mode = inode.S_IFDIR | uint16(mode&0777)
	childIno.UID = uid
	childIno.GID = gid
	childIno.Atime = now
	childIno.Mtime = now
	childIno.Ctime = now

	if err := f.inodeTbl.Write(ctx, childIno); err != nil {
		f.inodeTbl.Free(ctx, childIno.InodeNum)
		f.dirMgr.Remove(ctx, dirIno, name)
		return nil, syscall.EIO
	}

	dirIno.LinkCount++
	dirIno.Mtime = now
	f.inodeTbl.Write(ctx, dirIno)

	childDir := &Dir{fs: f, inodeNum: childIno.InodeNum}
	ch := f.NewPersistentInode(ctx, childDir, fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  uint64(childIno.InodeNum),
	})

	out.EntryValid = 0
	out.AttrValid = 0
	out.Ino = uint64(childIno.InodeNum)
	out.Mode = uint32(fuse.S_IFDIR | (mode & 0777))
	out.Nlink = uint32(childIno.LinkCount)
	out.Uid = uid
	out.Gid = gid
	out.Size = childIno.Size
	out.Atime = uint64(now / 1e9)
	out.Atimensec = uint32(now % 1e9)
	out.Mtime = uint64(now / 1e9)
	out.Mtimensec = uint32(now % 1e9)
	out.Ctime = uint64(now / 1e9)
	out.Ctimensec = uint32(now % 1e9)
	out.Blksize = BlockSize

	return ch, 0
}

// Unlink removes a file from the root directory.
func (f *FS) Unlink(ctx context.Context, name string) syscall.Errno {
	dirIno, err := f.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		return syscall.EIO
	}

	childInodeNum, fileType, err := f.dirMgr.Lookup(ctx, dirIno, name)
	if err != nil {
		if err == dir.ErrNotFound {
			return syscall.ENOENT
		}
		return syscall.EIO
	}

	if fileType == dir.DT_DIR {
		return syscall.EISDIR
	}

	childIno, err := f.inodeTbl.Read(uint32(childInodeNum))
	if err != nil {
		return syscall.EIO
	}

	// WAL logging
	var txn *journal.Transaction
	if f.journal != nil {
		txn = f.journal.Begin()
		if err := txn.LogDirRemoveEntry(&journal.DirRemoveEntryPayload{
			ParentInodeNum: uint64(inode.InodeRoot),
			ChildInodeNum:  childInodeNum,
			Name:           name,
		}); err != nil {
			f.journal.Abort(txn)
			return syscall.EIO
		}
		if err := f.journal.Commit(txn); err != nil {
			return syscall.EIO
		}
	}

	if err := f.dirMgr.Remove(ctx, dirIno, name); err != nil {
		return syscall.EIO
	}

	childIno.LinkCount--
	now := time.Now().UnixNano()
	childIno.Ctime = now

	if childIno.LinkCount == 0 {
		if err := f.extentMgr.Truncate(ctx, childIno, 0); err != nil {
			log.Printf("[DEBUG] Unlink %q: Truncate failed: %v", name, err)
		}
		f.inodeTbl.Free(ctx, childIno.InodeNum)
	} else {
		f.inodeTbl.Write(ctx, childIno)
	}

	dirIno.Mtime = now
	f.inodeTbl.Write(ctx, dirIno)

	return 0
}

// Rmdir removes an empty directory from the root directory.
func (f *FS) Rmdir(ctx context.Context, name string) syscall.Errno {
	dirIno, err := f.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		return syscall.EIO
	}

	childInodeNum, fileType, err := f.dirMgr.Lookup(ctx, dirIno, name)
	if err != nil {
		if err == dir.ErrNotFound {
			return syscall.ENOENT
		}
		return syscall.EIO
	}

	if fileType != dir.DT_DIR {
		return syscall.ENOTDIR
	}

	childIno, err := f.inodeTbl.Read(uint32(childInodeNum))
	if err != nil {
		return syscall.EIO
	}

	isEmpty, err := f.dirMgr.IsEmpty(ctx, childIno)
	if err != nil {
		return syscall.EIO
	}
	if !isEmpty {
		return syscall.ENOTEMPTY
	}

	// WAL logging
	var txn *journal.Transaction
	if f.journal != nil {
		txn = f.journal.Begin()
		if err := txn.LogDirRemoveEntry(&journal.DirRemoveEntryPayload{
			ParentInodeNum: uint64(inode.InodeRoot),
			ChildInodeNum:  childInodeNum,
			Name:           name,
		}); err != nil {
			f.journal.Abort(txn)
			return syscall.EIO
		}
		if err := f.journal.Commit(txn); err != nil {
			return syscall.EIO
		}
	}

	if err := f.dirMgr.Remove(ctx, dirIno, name); err != nil {
		return syscall.EIO
	}

	if err := f.extentMgr.Truncate(ctx, childIno, 0); err != nil {
		log.Printf("[DEBUG] Rmdir %q: Truncate failed: %v", name, err)
	}
	f.inodeTbl.Free(ctx, childIno.InodeNum)

	now := time.Now().UnixNano()
	dirIno.LinkCount--
	dirIno.Mtime = now
	f.inodeTbl.Write(ctx, dirIno)

	return 0
}

// Dir represents a directory in the filesystem.
// Embeds fs.Inode for go-fuse v2 node identity.
type Dir struct {
	fs.Inode

	fs       *FS
	inodeNum uint32
}

// Verify interface compliance at compile time
var (
	_ fs.InodeEmbedder = (*Dir)(nil)
	_ fs.NodeGetattrer = (*Dir)(nil)
	_ fs.NodeLookuper  = (*Dir)(nil)
	_ fs.NodeReaddirer = (*Dir)(nil)
	_ fs.NodeCreater   = (*Dir)(nil)
	_ fs.NodeMkdirer   = (*Dir)(nil)
	_ fs.NodeUnlinker  = (*Dir)(nil)
	_ fs.NodeRmdirer   = (*Dir)(nil)
)

// Getattr returns directory attributes.
func (d *Dir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	ino, err := d.fs.getInode(d.inodeNum)
	if err != nil {
		return syscall.EIO
	}

	// Disable FUSE kernel caching for cluster operation.
	// Other nodes may modify attributes at any time.
	out.AttrValid = 0

	out.Ino = uint64(ino.InodeNum)
	out.Mode = uint32(fuse.S_IFDIR | (ino.Mode & 0777))
	out.Nlink = uint32(ino.LinkCount)
	out.Uid = ino.UID
	out.Gid = ino.GID
	out.Size = ino.Size
	out.Atime = uint64(ino.Atime / 1e9)
	out.Atimensec = uint32(ino.Atime % 1e9)
	out.Mtime = uint64(ino.Mtime / 1e9)
	out.Mtimensec = uint32(ino.Mtime % 1e9)
	out.Ctime = uint64(ino.Ctime / 1e9)
	out.Ctimensec = uint32(ino.Ctime % 1e9)
	out.Blocks = ino.AllocatedBlocks * (BlockSize / 512)
	out.Blksize = BlockSize

	return 0
}

// Lookup finds a child by name.
func (d *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	dirIno, err := d.fs.getInode(d.inodeNum)
	if err != nil {
		return nil, syscall.EIO
	}

	childInodeNum, fileType, err := d.fs.dirMgr.Lookup(ctx, dirIno, name)
	if err != nil {
		if err == dir.ErrNotFound {
			return nil, syscall.ENOENT
		}
		return nil, syscall.EIO
	}

	// Disable FUSE kernel caching for cluster operation.
	// Other nodes may modify the filesystem at any time.
	out.EntryValid = 0
	out.AttrValid = 0

	if fileType == dir.DT_DIR {
		childDir := &Dir{fs: d.fs, inodeNum: uint32(childInodeNum)}
		ch := d.NewPersistentInode(ctx, childDir, fs.StableAttr{
			Mode: fuse.S_IFDIR,
			Ino:  childInodeNum,
		})
		return ch, 0
	}

	childFile := &File{fs: d.fs, inodeNum: uint32(childInodeNum)}
	ch := d.NewPersistentInode(ctx, childFile, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  childInodeNum,
	})
	return ch, 0
}

// dirStream implements fs.DirStream for directory listing.
type dirStream struct {
	entries []fuse.DirEntry
	pos     int
}

// HasNext returns true if there are more entries.
func (ds *dirStream) HasNext() bool {
	return ds.pos < len(ds.entries)
}

// Next returns the next directory entry.
func (ds *dirStream) Next() (fuse.DirEntry, syscall.Errno) {
	if ds.pos >= len(ds.entries) {
		return fuse.DirEntry{}, 0 // No more entries
	}
	e := ds.entries[ds.pos]
	ds.pos++
	return e, 0
}

// Close releases resources.
func (ds *dirStream) Close() {}

// Readdir returns a DirStream for directory listing.
func (d *Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	dirIno, err := d.fs.getInode(d.inodeNum)
	if err != nil {
		return nil, syscall.EIO
	}

	entries, err := d.fs.dirMgr.List(ctx, dirIno)
	if err != nil {
		return nil, syscall.EIO
	}

	var result []fuse.DirEntry
	for _, entry := range entries {
		mode := uint32(fuse.S_IFREG)
		switch entry.FileType {
		case dir.DT_DIR:
			mode = fuse.S_IFDIR
		case dir.DT_LNK:
			mode = fuse.S_IFLNK
		}

		result = append(result, fuse.DirEntry{
			Ino:  entry.InodeNum,
			Name: entry.Name,
			Mode: mode,
		})
	}

	return &dirStream{entries: result}, 0
}

// Create creates a new file.
//
// This function wraps createInternal() with a retry loop to handle inode slot
// collisions that occur when multiple nodes race to create files concurrently.
// When another node claims the same inode slot, we rollback and retry with a
// different slot.
func (d *Dir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	for attempt := 0; attempt < MaxCreateRetries; attempt++ {
		node, fh, fuseFlags, errno = d.createInternal(ctx, name, flags, mode, out)
		if errno == syscall.EAGAIN {
			// Jittered exponential backoff: base * 2^attempt + random jitter
			// This spreads out retries to reduce thundering herd collisions
			baseDelay := time.Millisecond * time.Duration(1<<attempt) // 1ms, 2ms, 4ms, 8ms, 16ms
			jitter := time.Duration(rand.Int63n(int64(baseDelay)))    // 0 to baseDelay
			backoff := baseDelay + jitter
			log.Printf("[DEBUG] Create %q: inode slot collision, retry %d/%d (backoff %v)", name, attempt+1, MaxCreateRetries, backoff)
			time.Sleep(backoff)
			continue
		}
		return
	}
	log.Printf("[WARN] Create %q: exhausted %d retries due to inode slot collisions", name, MaxCreateRetries)
	return nil, nil, 0, syscall.EAGAIN
}

// createInternal performs the actual file creation with two-phase allocation.
//
// Lock ordering: This function uses a two-phase allocation pattern to respect
// the lock hierarchy (BlockBitmap Level 6 < Inode Level 8):
//  1. FindFreeSlot() - no locks
//  2. dirMgr.Add() - may acquire BlockBitmap lock (Level 6) for directory expansion
//  3. ClaimSlot() - acquires Inode lock (Level 8)
//
// WAL Protocol: If journaling is enabled, the function logs intent BEFORE
// actual metadata writes. The commit point (journal.Commit) ensures crash
// consistency - operations after commit are recoverable.
//
// Returns syscall.EAGAIN if the inode slot was claimed by another node.
func (d *Dir) createInternal(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	// Phase 1: Find a free inode slot (no distributed locks held)
	inodeNum, err := d.fs.inodeTbl.FindFreeSlot()
	if err != nil {
		log.Printf("[DEBUG] Create %q: FindFreeSlot failed: %v", name, err)
		return nil, nil, 0, syscall.ENOSPC
	}

	// Phase 2: Read parent directory inode
	dirIno, err := d.fs.inodeTbl.Read(d.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Create %q: Read parent inode %d failed: %v", name, d.inodeNum, err)
		return nil, nil, 0, syscall.EIO
	}

	// Get uid/gid from context or use defaults
	uid := d.fs.uid
	gid := d.fs.gid
	caller, ok := fuse.FromContext(ctx)
	if ok {
		uid = caller.Uid
		gid = caller.Gid
	}

	// === WAL: Begin transaction and log intent ===
	var txn *journal.Transaction
	if d.fs.journal != nil {
		txn = d.fs.journal.Begin()

		// Log inode creation intent
		if err := txn.LogInodeCreate(&journal.InodeCreatePayload{
			InodeNum:       uint64(inodeNum),
			Mode:           mode,
			UID:            uid,
			GID:            gid,
			ParentInodeNum: uint64(d.inodeNum),
			NameHash:       hashString(name),
		}); err != nil {
			d.fs.journal.Abort(txn)
			log.Printf("[DEBUG] Create %q: journal LogInodeCreate failed: %v", name, err)
			return nil, nil, 0, syscall.EIO
		}

		// Log directory entry addition intent
		if err := txn.LogDirAddEntry(&journal.DirAddEntryPayload{
			ParentInodeNum: uint64(d.inodeNum),
			ChildInodeNum:  uint64(inodeNum),
			EntryType:      journal.DirEntryRegular,
			Name:           name,
		}); err != nil {
			d.fs.journal.Abort(txn)
			log.Printf("[DEBUG] Create %q: journal LogDirAddEntry failed: %v", name, err)
			return nil, nil, 0, syscall.EIO
		}

		// COMMIT POINT: After this, the operation WILL complete (even after crash)
		if err := d.fs.journal.Commit(txn); err != nil {
			log.Printf("[DEBUG] Create %q: journal commit failed: %v", name, err)
			return nil, nil, 0, syscall.EIO
		}
	}

	// Phase 3: Add directory entry FIRST (may acquire BlockBitmap lock for directory expansion)
	// This respects lock ordering: BlockBitmap (Level 6) before Inode (Level 8)
	if err := d.fs.dirMgr.Add(ctx, dirIno, name, uint64(inodeNum), dir.DT_REG); err != nil {
		log.Printf("[DEBUG] Create %q: dirMgr.Add failed: %v", name, err)
		return nil, nil, 0, syscall.EIO
	}

	// Phase 4: Claim the inode slot (acquires Inode lock)
	// If this fails, another node claimed the slot - rollback the directory entry
	childIno, err := d.fs.inodeTbl.ClaimSlot(ctx, inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Create %q: ClaimSlot %d failed: %v - rolling back", name, inodeNum, err)
		// Rollback: remove the directory entry we just added
		if removeErr := d.fs.dirMgr.Remove(ctx, dirIno, name); removeErr != nil {
			log.Printf("[ERROR] Create %q: rollback Remove failed: %v", name, removeErr)
		}
		// If slot was taken by another node, signal retry with a different slot
		if err == inode.ErrInodeInUse {
			return nil, nil, 0, syscall.EAGAIN
		}
		return nil, nil, 0, syscall.ENOSPC
	}

	// Phase 5: Initialize file inode attributes
	now := time.Now().UnixNano()
	childIno.Mode = inode.S_IFREG | uint16(mode&0777)
	childIno.UID = uid
	childIno.GID = gid
	childIno.LinkCount = 1
	childIno.Atime = now
	childIno.Mtime = now
	childIno.Ctime = now

	// Phase 6: Write updated inode
	if err := d.fs.inodeTbl.Write(ctx, childIno); err != nil {
		log.Printf("[DEBUG] Create %q: Write inode %d failed: %v", name, childIno.InodeNum, err)
		// Rollback both: free inode and remove directory entry
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		d.fs.dirMgr.Remove(ctx, dirIno, name)
		return nil, nil, 0, syscall.EIO
	}

	// Update directory mtime
	dirIno.Mtime = now
	d.fs.inodeTbl.Write(ctx, dirIno)

	// Create the file node
	file := &File{fs: d.fs, inodeNum: childIno.InodeNum}
	ch := d.NewPersistentInode(ctx, file, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  uint64(childIno.InodeNum),
	})

	// Populate entry attributes
	out.EntryValid = 0
	out.AttrValid = 0
	out.Ino = uint64(childIno.InodeNum)
	out.Mode = uint32(fuse.S_IFREG | (mode & 0777))
	out.Nlink = 1
	out.Uid = uid
	out.Gid = gid
	out.Size = 0
	out.Atime = uint64(now / 1e9)
	out.Atimensec = uint32(now % 1e9)
	out.Mtime = uint64(now / 1e9)
	out.Mtimensec = uint32(now % 1e9)
	out.Ctime = uint64(now / 1e9)
	out.Ctimensec = uint32(now % 1e9)
	out.Blksize = BlockSize

	return ch, nil, 0, 0
}

// Mkdir creates a new directory.
//
// This function wraps mkdirInternal() with a retry loop to handle inode slot
// collisions that occur when multiple nodes race to create directories concurrently.
// When another node claims the same inode slot, we rollback and retry with a
// different slot.
func (d *Dir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	for attempt := 0; attempt < MaxCreateRetries; attempt++ {
		node, errno := d.mkdirInternal(ctx, name, mode, out)
		if errno == syscall.EAGAIN {
			// Jittered exponential backoff: base * 2^attempt + random jitter
			// This spreads out retries to reduce thundering herd collisions
			baseDelay := time.Millisecond * time.Duration(1<<attempt) // 1ms, 2ms, 4ms, 8ms, 16ms
			jitter := time.Duration(rand.Int63n(int64(baseDelay)))    // 0 to baseDelay
			backoff := baseDelay + jitter
			log.Printf("[DEBUG] Mkdir %q: inode slot collision, retry %d/%d (backoff %v)", name, attempt+1, MaxCreateRetries, backoff)
			time.Sleep(backoff)
			continue
		}
		return node, errno
	}
	log.Printf("[WARN] Mkdir %q: exhausted %d retries due to inode slot collisions", name, MaxCreateRetries)
	return nil, syscall.EAGAIN
}

// mkdirInternal performs the actual directory creation with two-phase allocation.
//
// Lock ordering: This function uses a two-phase allocation pattern to respect
// the lock hierarchy (BlockBitmap Level 6 < Inode Level 8):
//  1. FindFreeSlot() - no locks
//  2. dirMgr.InitDirectory() - allocates blocks first (Level 6), then acquires inode lock
//  3. dirMgr.Add() - may acquire BlockBitmap lock (Level 6) for parent directory expansion
//  4. ClaimSlot() - acquires Inode lock (Level 8)
//
// WAL Protocol: If journaling is enabled, the function logs intent BEFORE
// actual metadata writes. The commit point ensures crash consistency.
//
// Note: dirMgr.InitDirectory() already handles lock ordering internally by allocating
// blocks before acquiring the inode lock.
//
// Returns syscall.EAGAIN if the inode slot was claimed by another node.
func (d *Dir) mkdirInternal(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Phase 1: Find a free inode slot (no distributed locks held)
	inodeNum, err := d.fs.inodeTbl.FindFreeSlot()
	if err != nil {
		log.Printf("[DEBUG] Mkdir %q: FindFreeSlot failed: %v", name, err)
		return nil, syscall.ENOSPC
	}

	// Phase 2: Read parent directory inode
	dirIno, err := d.fs.inodeTbl.Read(d.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Mkdir %q: Read parent inode %d failed: %v", name, d.inodeNum, err)
		return nil, syscall.EIO
	}

	// Get uid/gid from context or use defaults
	uid := d.fs.uid
	gid := d.fs.gid
	caller, ok := fuse.FromContext(ctx)
	if ok {
		uid = caller.Uid
		gid = caller.Gid
	}

	// === WAL: Begin transaction and log intent ===
	var txn *journal.Transaction
	if d.fs.journal != nil {
		txn = d.fs.journal.Begin()

		// Log directory inode creation intent
		if err := txn.LogInodeCreate(&journal.InodeCreatePayload{
			InodeNum:       uint64(inodeNum),
			Mode:           uint32(inode.S_IFDIR) | (mode & 0777),
			UID:            uid,
			GID:            gid,
			ParentInodeNum: uint64(d.inodeNum),
			NameHash:       hashString(name),
		}); err != nil {
			d.fs.journal.Abort(txn)
			log.Printf("[DEBUG] Mkdir %q: journal LogInodeCreate failed: %v", name, err)
			return nil, syscall.EIO
		}

		// Log directory entry addition intent
		if err := txn.LogDirAddEntry(&journal.DirAddEntryPayload{
			ParentInodeNum: uint64(d.inodeNum),
			ChildInodeNum:  uint64(inodeNum),
			EntryType:      journal.DirEntryDirectory,
			Name:           name,
		}); err != nil {
			d.fs.journal.Abort(txn)
			log.Printf("[DEBUG] Mkdir %q: journal LogDirAddEntry failed: %v", name, err)
			return nil, syscall.EIO
		}

		// Note: We don't log BlockAlloc here because we don't know the block number yet.
		// InitDirectory() will allocate it. In recovery, the block allocation is implicit
		// from the directory inode's extent info.

		// COMMIT POINT: After this, the operation WILL complete (even after crash)
		if err := d.fs.journal.Commit(txn); err != nil {
			log.Printf("[DEBUG] Mkdir %q: journal commit failed: %v", name, err)
			return nil, syscall.EIO
		}
	}

	// Phase 3: Add directory entry in parent FIRST (may acquire BlockBitmap lock)
	// This respects lock ordering: BlockBitmap (Level 6) before Inode (Level 8)
	if err := d.fs.dirMgr.Add(ctx, dirIno, name, uint64(inodeNum), dir.DT_DIR); err != nil {
		log.Printf("[DEBUG] Mkdir %q: dirMgr.Add failed: %v", name, err)
		return nil, syscall.EIO
	}

	// Phase 4: Claim the inode slot (acquires Inode lock)
	childIno, err := d.fs.inodeTbl.ClaimSlot(ctx, inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Mkdir %q: ClaimSlot %d failed: %v - rolling back", name, inodeNum, err)
		// Rollback: remove the directory entry we just added
		if removeErr := d.fs.dirMgr.Remove(ctx, dirIno, name); removeErr != nil {
			log.Printf("[ERROR] Mkdir %q: rollback Remove failed: %v", name, removeErr)
		}
		// If slot was taken by another node, signal retry with a different slot
		if err == inode.ErrInodeInUse {
			return nil, syscall.EAGAIN
		}
		return nil, syscall.ENOSPC
	}

	// Phase 5: Initialize directory inode attributes
	now := time.Now().UnixNano()
	childIno.Mode = inode.S_IFDIR | uint16(mode&0777)
	childIno.UID = uid
	childIno.GID = gid
	childIno.LinkCount = 2 // . and parent
	childIno.Atime = now
	childIno.Mtime = now
	childIno.Ctime = now

	// Phase 6: Initialize directory contents (allocates data block for . and .. entries)
	// Note: InitDirectory() handles lock ordering internally
	if err := d.fs.dirMgr.InitDirectory(ctx, childIno, uint64(d.inodeNum)); err != nil {
		log.Printf("[DEBUG] Mkdir %q: InitDirectory failed: %v", name, err)
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		d.fs.dirMgr.Remove(ctx, dirIno, name)
		return nil, syscall.EIO
	}

	// Phase 7: Write updated inode
	if err := d.fs.inodeTbl.Write(ctx, childIno); err != nil {
		log.Printf("[DEBUG] Mkdir %q: Write inode failed: %v", name, err)
		d.fs.inodeTbl.Free(ctx, childIno.InodeNum)
		d.fs.dirMgr.Remove(ctx, dirIno, name)
		return nil, syscall.EIO
	}

	// Increment parent link count (for ..)
	dirIno.LinkCount++
	dirIno.Mtime = now
	d.fs.inodeTbl.Write(ctx, dirIno)

	// Create the directory node
	childDir := &Dir{fs: d.fs, inodeNum: childIno.InodeNum}
	ch := d.NewPersistentInode(ctx, childDir, fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  uint64(childIno.InodeNum),
	})

	// Populate entry attributes
	out.EntryValid = 0
	out.AttrValid = 0
	out.Ino = uint64(childIno.InodeNum)
	out.Mode = uint32(fuse.S_IFDIR | (mode & 0777))
	out.Nlink = 2
	out.Uid = uid
	out.Gid = gid
	out.Size = childIno.Size
	out.Atime = uint64(now / 1e9)
	out.Atimensec = uint32(now % 1e9)
	out.Mtime = uint64(now / 1e9)
	out.Mtimensec = uint32(now % 1e9)
	out.Ctime = uint64(now / 1e9)
	out.Ctimensec = uint32(now % 1e9)
	out.Blksize = BlockSize

	return ch, 0
}

// Unlink removes a file.
func (d *Dir) Unlink(ctx context.Context, name string) syscall.Errno {
	return d.removeEntry(ctx, name, false)
}

// Rmdir removes an empty directory.
func (d *Dir) Rmdir(ctx context.Context, name string) syscall.Errno {
	return d.removeEntry(ctx, name, true)
}

// removeEntry removes a file or empty directory.
func (d *Dir) removeEntry(ctx context.Context, name string, isDir bool) syscall.Errno {
	// Invalidate parent directory cache before modifying
	d.fs.invalidateInode(d.inodeNum)

	dirIno, err := d.fs.inodeTbl.Read(d.inodeNum)
	if err != nil {
		return syscall.EIO
	}

	// Look up the entry
	childInodeNum, fileType, err := d.fs.dirMgr.Lookup(ctx, dirIno, name)
	if err != nil {
		if err == dir.ErrNotFound {
			return syscall.ENOENT
		}
		return syscall.EIO
	}

	// Invalidate child inode cache - it's being removed
	d.fs.invalidateInode(uint32(childInodeNum))

	// Read child inode early - we need it for validation and WAL logging
	childIno, err := d.fs.inodeTbl.Read(uint32(childInodeNum))
	if err != nil {
		return syscall.EIO
	}

	// Handle directory removal
	if isDir {
		if fileType != dir.DT_DIR {
			return syscall.ENOTDIR
		}

		// Check if directory is empty
		isEmpty, err := d.fs.dirMgr.IsEmpty(ctx, childIno)
		if err != nil {
			return syscall.EIO
		}
		if !isEmpty {
			return syscall.ENOTEMPTY
		}

		// Will decrement parent link count (done after commit)
	} else {
		// Unlink: verify it's not a directory
		if fileType == dir.DT_DIR {
			return syscall.EISDIR
		}
	}

	// Determine if inode will be freed (link count becomes 0)
	willFreeInode := childIno.LinkCount == 1

	// === WAL: Begin transaction and log intent ===
	var txn *journal.Transaction
	if d.fs.journal != nil {
		txn = d.fs.journal.Begin()

		// Determine entry type for directory removal
		entryType := journal.DirEntryRegular
		if isDir {
			entryType = journal.DirEntryDirectory
		}

		// Log directory entry removal
		if err := txn.LogDirRemoveEntry(&journal.DirRemoveEntryPayload{
			ParentInodeNum: uint64(d.inodeNum),
			ChildInodeNum:  uint64(childInodeNum),
			EntryType:      entryType,
			Name:           name,
		}); err != nil {
			d.fs.journal.Abort(txn)
			return syscall.EIO
		}

		// If link count will become 0, log inode deletion and extent freeing
		if willFreeInode {
			// Log block/extent freeing for all extents
			// Note: extents are freed during Truncate(0)
			if childIno.Size > 0 {
				if err := txn.LogBlockFree(&journal.BlockFreePayload{
					InodeNum:   uint64(childInodeNum),
					StartBlock: 0, // Freeing all blocks
					Count:      uint32((childIno.Size + BlockSize - 1) / BlockSize),
				}); err != nil {
					d.fs.journal.Abort(txn)
					return syscall.EIO
				}
			}

			// Log inode deletion
			if err := txn.LogInodeDelete(&journal.InodeDeletePayload{
				InodeNum: uint64(childInodeNum),
			}); err != nil {
				d.fs.journal.Abort(txn)
				return syscall.EIO
			}
		} else {
			// Log inode update for link count decrement
			if err := txn.LogInodeUpdate(&journal.InodeUpdatePayload{
				InodeNum:   uint64(childInodeNum),
				UpdateMask: journal.InodeUpdateNlink,
			}); err != nil {
				d.fs.journal.Abort(txn)
				return syscall.EIO
			}
		}

		// Log parent directory inode update (mtime change, and link count for rmdir)
		parentMask := journal.InodeUpdateMtime
		if isDir {
			parentMask |= journal.InodeUpdateNlink
		}
		if err := txn.LogInodeUpdate(&journal.InodeUpdatePayload{
			InodeNum:   uint64(d.inodeNum),
			UpdateMask: parentMask,
		}); err != nil {
			d.fs.journal.Abort(txn)
			return syscall.EIO
		}

		// === COMMIT POINT: After this, operation is guaranteed recoverable ===
		if err := d.fs.journal.Commit(txn); err != nil {
			return syscall.EIO
		}
	}
	// === End WAL transaction ===

	// Now execute the actual metadata operations (safe - recoverable if crash)

	// Update parent link count for directory removal
	if isDir {
		dirIno.LinkCount--
	}

	// Remove directory entry
	if err := d.fs.dirMgr.Remove(ctx, dirIno, name); err != nil {
		return syscall.EIO
	}

	// Decrement child link count and potentially free
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

	return 0
}

// File represents a file in the filesystem.
// Embeds fs.Inode for go-fuse v2 node identity.
type File struct {
	fs.Inode

	fs       *FS
	inodeNum uint32
}

// Verify interface compliance at compile time
var (
	_ fs.InodeEmbedder = (*File)(nil)
	_ fs.NodeOpener    = (*File)(nil)
	_ fs.NodeGetattrer = (*File)(nil)
	_ fs.NodeSetattrer = (*File)(nil)
	_ fs.NodeReader    = (*File)(nil)
	_ fs.NodeWriter    = (*File)(nil)
	_ fs.NodeFsyncer   = (*File)(nil)
	_ fs.NodeAllocater = (*File)(nil)
)

// Open opens the file for reading/writing.
// Returns nil FileHandle to indicate the File node itself should be used as the handle.
func (f *File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// Verify the inode exists and is a regular file
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	if !ino.IsFile() {
		return nil, 0, syscall.EISDIR
	}
	// Return nil FileHandle - the File node itself implements Read/Write/etc.
	// The fuse.FOPEN_KEEP_CACHE flag tells the kernel to keep cached data valid
	// (we don't use it in cluster mode since other nodes may modify the file).
	return nil, 0, 0
}

// Getattr returns file attributes.
func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// Read directly from disk to get fresh inode state.
	// In multi-node scenarios, another node may have updated the inode.
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		return syscall.EIO
	}

	// Disable FUSE kernel caching for cluster operation.
	// Other nodes may modify attributes at any time.
	out.AttrValid = 0

	out.Ino = uint64(ino.InodeNum)
	out.Mode = uint32(fuse.S_IFREG | (ino.Mode & 0777))
	out.Nlink = uint32(ino.LinkCount)
	out.Uid = ino.UID
	out.Gid = ino.GID
	out.Size = ino.Size
	out.Atime = uint64(ino.Atime / 1e9)
	out.Atimensec = uint32(ino.Atime % 1e9)
	out.Mtime = uint64(ino.Mtime / 1e9)
	out.Mtimensec = uint32(ino.Mtime % 1e9)
	out.Ctime = uint64(ino.Ctime / 1e9)
	out.Ctimensec = uint32(ino.Ctime % 1e9)
	out.Blocks = ino.AllocatedBlocks * (BlockSize / 512)
	out.Blksize = BlockSize

	return 0
}

// Read reads file data.
func (f *File) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Read directly from disk to get fresh inode state.
	// In multi-node scenarios, another node may have updated the inode.
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		return nil, syscall.EIO
	}

	offset := uint64(off)

	// Check bounds using the authoritative size from disk
	if offset >= ino.Size {
		return fuse.ReadResultData(nil), 0
	}

	// Calculate how much to read
	readSize := len(dest)
	if offset+uint64(readSize) > ino.Size {
		readSize = int(ino.Size - offset)
	}

	// Read data block by block
	data := make([]byte, readSize)
	bytesRead := 0

	for bytesRead < readSize {
		blockNum := offset / BlockSize
		blockOffset := offset % BlockSize

		// Find physical block for this logical block (uses extent cache)
		physBlock, err := f.fs.lookupPhysical(ino, blockNum)
		if err != nil {
			return nil, syscall.EIO
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
				return nil, syscall.EIO
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

	return fuse.ReadResultData(data), 0
}

// Write writes file data.
// Uses an optimistic retry pattern to handle concurrent modifications from other nodes.
func (f *File) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	// Retry loop to handle concurrent modifications from other nodes.
	// When multiple nodes try to extend the same file simultaneously, they may race
	// to allocate blocks. If a conflict is detected, errRetryWrite is returned and
	// we retry the operation.
	for attempt := 0; attempt < MaxWriteRetries; attempt++ {
		bytesWritten, err := f.writeInternal(ctx, data, uint64(off))
		if err == errRetryWrite {
			log.Printf("[DEBUG] Write inode %d: retry attempt %d due to concurrent modification",
				f.inodeNum, attempt+1)
			continue
		}
		if err != nil {
			return 0, errToErrno(err)
		}
		return uint32(bytesWritten), 0
	}

	log.Printf("[ERROR] Write inode %d: concurrent modification detected after %d retries",
		f.inodeNum, MaxWriteRetries)
	return 0, syscall.EAGAIN
}

// writeInternal performs the actual write operation.
// Returns errRetryWrite if a concurrent modification is detected and the caller should retry.
func (f *File) writeInternal(ctx context.Context, data []byte, offset uint64) (int, error) {
	bytesWritten := 0

	// Track if we hold the inode lock
	lockHeld := false
	defer func() {
		if lockHeld && f.fs.lockMgr != nil {
			f.fs.lockMgr.Release(lock.LockTypeInode, uint64(f.inodeNum))
		}
	}()

	// === WAL: Track transaction for extent allocations ===
	var txn *journal.Transaction
	allocatedExtents := make([]struct {
		logicalBlock  uint64
		physicalBlock uint64
		count         uint32
	}, 0)
	// Abort transaction on error path
	defer func() {
		if txn != nil && f.fs.journal != nil {
			// Transaction was not committed - abort it
			f.fs.journal.Abort(txn)
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
					goto handleExtent
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

			// === WAL: Track the allocated extent for logging ===
			allocatedExtents = append(allocatedExtents, struct {
				logicalBlock  uint64
				physicalBlock uint64
				count         uint32
			}{
				logicalBlock:  blockNum,
				physicalBlock: ext.PhysicalBlock,
				count:         uint32(ext.Length),
			})
		}

	handleExtent:
		// Check if this is an unwritten extent (from fallocate).
		// If so, we need to clear the Unwritten flag on first write.
		if ext.Flags&extent.FlagUnwritten != 0 {
			// First write to unwritten block - need to clear flag
			if !lockHeld && f.fs.lockMgr != nil {
				// Acquire lock before modifying extent
				if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
					log.Printf("[DEBUG] Write inode %d: failed to acquire lock for unwritten: %v", f.inodeNum, err)
					return 0, syscall.EIO
				}
				lockHeld = true

				// Re-read inode after acquiring lock
				freshIno, err := f.fs.inodeTbl.Read(f.inodeNum)
				if err != nil {
					log.Printf("[DEBUG] Write inode %d: re-read for unwritten failed: %v", f.inodeNum, err)
					return 0, syscall.EIO
				}
				if freshIno.MetaGeneration != ino.MetaGeneration {
					log.Printf("[DEBUG] Write inode %d: MetaGeneration changed during unwritten clear",
						f.inodeNum)
					return 0, errRetryWrite
				}
				ino = freshIno

				// Re-lookup extent - another node may have already cleared it
				ext, extOffset, err = f.fs.extentMgr.Lookup(ino, blockNum)
				if err != nil {
					log.Printf("[DEBUG] Write inode %d: extent disappeared during unwritten clear",
						f.inodeNum)
					return 0, errRetryWrite
				}
			}

			// Clear the Unwritten flag (if still set)
			if f.fs.extentMgr.ClearUnwritten(ino, ext) {
				log.Printf("[DEBUG] Write inode %d: cleared Unwritten flag for block %d",
					f.inodeNum, blockNum)
			}
		}

		// Calculate physical location
		physBlock := ext.PhysicalBlock + extOffset
		diskOffset := int64(f.fs.dataOffset + physBlock*BlockSize + blockOffset)

		// Write to disk
		toWrite := int(BlockSize - blockOffset)
		if toWrite > len(data)-bytesWritten {
			toWrite = len(data) - bytesWritten
		}

		// Acquire extent lock on the physical block to prevent lost-update race.
		// When multiple nodes write to different offsets within the same block,
		// they must serialize their read-modify-write operations to avoid
		// one node's changes being overwritten by another's stale read.
		extentLockHeld := false
		if f.fs.lockMgr != nil {
			if err := f.fs.lockMgr.AcquireExtent(ctx, physBlock, lock.LockModeExclusive); err != nil {
				log.Printf("[DEBUG] Write inode %d: failed to acquire extent lock for block %d: %v",
					f.inodeNum, physBlock, err)
				return 0, syscall.EIO
			}
			extentLockHeld = true
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
				if extentLockHeld {
					f.fs.lockMgr.ReleaseExtent(physBlock)
				}
				return 0, syscall.EIO
			}

			bytesInBlock += copyLen
			alignedOffset += storage.SectorSize
			sectorOffset = 0
		}
		storage.PutBuffer(buf)

		// Release extent lock after completing write to this block
		if extentLockHeld {
			f.fs.lockMgr.ReleaseExtent(physBlock)
		}

		bytesWritten += toWrite
		offset += uint64(toWrite)
	}

	// Update inode size if extended
	// To avoid lost-update race when multiple nodes write to the same file:
	// 1. Acquire exclusive inode lock (if not already held)
	// 2. Re-read inode to get freshest size from other nodes
	// 3. Calculate newSize = max(fresh_size, our_write_end)
	// 4. Write inode with updated size
	// This serializes the read-modify-write cycle.
	acquiredLockForSize := false
	if !lockHeld && f.fs.lockMgr != nil {
		if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
			log.Printf("[DEBUG] Write inode %d: failed to acquire lock for size update: %v", f.inodeNum, err)
			return 0, syscall.EIO
		}
		acquiredLockForSize = true
		lockHeld = true
	}

	// Re-read the inode while holding the lock to get the freshest size
	freshIno, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Write inode %d: re-read inode failed: %v", f.inodeNum, err)
		if acquiredLockForSize {
			f.fs.lockMgr.Release(lock.LockTypeInode, uint64(f.inodeNum))
		}
		return 0, syscall.EIO
	}
	// Use the freshest size from disk, then calculate new size as max(fresh_size, our_write_end)
	newSize := freshIno.Size
	if offset > newSize {
		newSize = offset // Our write extended the file
	}
	newMtime := time.Now().UnixNano()

	// === WAL: Log extent allocations and inode update before committing metadata ===
	if f.fs.journal != nil && (len(allocatedExtents) > 0 || newSize != ino.Size) {
		txn = f.fs.journal.Begin()

		// Log all extent allocations
		for _, alloc := range allocatedExtents {
			if err := txn.LogExtentAdd(&journal.ExtentAddPayload{
				InodeNum:  uint64(f.inodeNum),
				FileBlock: alloc.logicalBlock,
				DiskBlock: alloc.physicalBlock,
				Length:    alloc.count,
			}); err != nil {
				log.Printf("[DEBUG] Write inode %d: LogExtentAdd failed: %v", f.inodeNum, err)
				return 0, syscall.EIO
			}
		}

		// Log inode update (size and mtime)
		mask := journal.InodeUpdateMtime
		if newSize != ino.Size {
			mask |= journal.InodeUpdateSize
		}
		if err := txn.LogInodeUpdate(&journal.InodeUpdatePayload{
			InodeNum:   uint64(f.inodeNum),
			UpdateMask: mask,
		}); err != nil {
			log.Printf("[DEBUG] Write inode %d: LogInodeUpdate failed: %v", f.inodeNum, err)
			return 0, syscall.EIO
		}

		// === COMMIT POINT: After this, operation is guaranteed recoverable ===
		if err := f.fs.journal.Commit(txn); err != nil {
			log.Printf("[DEBUG] Write inode %d: journal commit failed: %v", f.inodeNum, err)
			return 0, syscall.EIO
		}
		txn = nil // Mark as committed so defer doesn't abort
	}
	// === End WAL transaction ===

	// Apply the metadata changes
	ino.Size = newSize
	ino.Mtime = newMtime

	// Write inode - use WriteWithLockHeld if we hold the lock
	if lockHeld {
		if err := f.fs.inodeTbl.WriteWithLockHeld(ino); err != nil {
			log.Printf("[DEBUG] Write inode %d: WriteWithLockHeld failed: %v", f.inodeNum, err)
			if acquiredLockForSize {
				f.fs.lockMgr.Release(lock.LockTypeInode, uint64(f.inodeNum))
			}
			return 0, syscall.EIO
		}
	} else {
		if err := f.fs.inodeTbl.Write(ctx, ino); err != nil {
			log.Printf("[DEBUG] Write inode %d: final inodeTbl.Write failed: %v", f.inodeNum, err)
			return 0, syscall.EIO
		}
	}

	// Release the lock we acquired for size update
	if acquiredLockForSize {
		f.fs.lockMgr.Release(lock.LockTypeInode, uint64(f.inodeNum))
	}

	// Cache the freshly-written inode for subsequent reads
	f.fs.cacheInode(ino)

	// Broadcast inode invalidation to other nodes for immediate visibility.
	// This is fire-and-forget - doesn't block the write operation.
	if f.fs.cacheMgr != nil {
		f.fs.cacheMgr.BroadcastInvalidateInode(uint32(f.inodeNum), ino.DataGeneration, ino.MetaGeneration)
	}

	return bytesWritten, nil
}

// Setattr sets file attributes (used for truncate, chmod, etc.).
func (f *File) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	// Invalidate cache before modifying - ensures we read fresh data
	f.fs.invalidateInode(f.inodeNum)

	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		return syscall.EIO
	}

	now := time.Now().UnixNano()

	// Track what's changing for WAL
	var mask journal.InodeUpdateMask
	truncating := false
	blocksToFree := uint32(0)

	if sz, ok := in.GetSize(); ok {
		mask |= journal.InodeUpdateSize | journal.InodeUpdateMtime
		if sz < ino.Size {
			truncating = true
			// Calculate blocks being freed
			oldBlocks := (ino.Size + BlockSize - 1) / BlockSize
			newBlocks := (sz + BlockSize - 1) / BlockSize
			if oldBlocks > newBlocks {
				blocksToFree = uint32(oldBlocks - newBlocks)
			}
		}
	}

	if mode, ok := in.GetMode(); ok {
		mask |= journal.InodeUpdateMode | journal.InodeUpdateCtime
		_ = mode // Used after WAL commit
	}

	if uid, ok := in.GetUID(); ok {
		mask |= journal.InodeUpdateUID | journal.InodeUpdateCtime
		_ = uid
	}

	if gid, ok := in.GetGID(); ok {
		mask |= journal.InodeUpdateGID | journal.InodeUpdateCtime
		_ = gid
	}

	if _, ok := in.GetATime(); ok {
		mask |= journal.InodeUpdateAtime
	}

	if _, ok := in.GetMTime(); ok {
		mask |= journal.InodeUpdateMtime
	}

	// === WAL: Begin transaction and log intent ===
	var txn *journal.Transaction
	if f.fs.journal != nil && mask != 0 {
		txn = f.fs.journal.Begin()

		// Log block freeing if truncating
		if truncating && blocksToFree > 0 {
			newSize, _ := in.GetSize()
			if err := txn.LogBlockFree(&journal.BlockFreePayload{
				InodeNum:   uint64(f.inodeNum),
				StartBlock: uint64((newSize + BlockSize - 1) / BlockSize),
				Count:      blocksToFree,
			}); err != nil {
				f.fs.journal.Abort(txn)
				return syscall.EIO
			}
		}

		// Log inode update (InodeUpdatePayload only stores inode number and mask)
		if err := txn.LogInodeUpdate(&journal.InodeUpdatePayload{
			InodeNum:   uint64(f.inodeNum),
			UpdateMask: mask,
		}); err != nil {
			f.fs.journal.Abort(txn)
			return syscall.EIO
		}

		// === COMMIT POINT: After this, operation is guaranteed recoverable ===
		if err := f.fs.journal.Commit(txn); err != nil {
			return syscall.EIO
		}
	}
	// === End WAL transaction ===

	// Now execute the actual operations (safe - recoverable if crash)
	if sz, ok := in.GetSize(); ok {
		if sz < ino.Size {
			// Truncate
			if err := f.fs.extentMgr.Truncate(ctx, ino, sz); err != nil {
				return syscall.EIO
			}
		}
		ino.Size = sz
		ino.Mtime = now
	}

	if mode, ok := in.GetMode(); ok {
		ino.Mode = uint16(mode & 0777)
		ino.Ctime = now
	}

	if uid, ok := in.GetUID(); ok {
		ino.UID = uid
		ino.Ctime = now
	}

	if gid, ok := in.GetGID(); ok {
		ino.GID = gid
		ino.Ctime = now
	}

	if atime, ok := in.GetATime(); ok {
		ino.Atime = atime.UnixNano()
	}

	if mtime, ok := in.GetMTime(); ok {
		ino.Mtime = mtime.UnixNano()
	}

	if err := f.fs.inodeTbl.Write(ctx, ino); err != nil {
		return syscall.EIO
	}

	// Cache the freshly-written inode for subsequent reads
	f.fs.cacheInode(ino)

	return f.Getattr(ctx, fh, out)
}

// Fsync flushes file data to disk.
func (f *File) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	if err := f.fs.dev.Sync(); err != nil {
		return syscall.EIO
	}
	return 0
}

// Allocate pre-allocates space for the file using unwritten (lazy-zero) extents.
// Supports mode==0 (allocate and extend size) and FALLOC_FL_KEEP_SIZE (allocate only).
// PUNCH_HOLE and ZERO_RANGE are not yet implemented.
func (f *File) Allocate(ctx context.Context, fh fs.FileHandle, off uint64, size uint64, mode uint32) syscall.Errno {
	// Only support mode 0 (allocate) and KEEP_SIZE for now
	const FALLOC_FL_KEEP_SIZE = 0x01
	if mode != 0 && mode != FALLOC_FL_KEEP_SIZE {
		// PUNCH_HOLE and ZERO_RANGE not yet implemented
		log.Printf("[DEBUG] Allocate inode %d: unsupported mode %d", f.inodeNum, mode)
		return syscall.EOPNOTSUPP
	}

	// Read current inode
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Allocate inode %d: read failed: %v", f.inodeNum, err)
		return syscall.EIO
	}

	// Acquire inode lock for allocation
	if f.fs.lockMgr != nil {
		if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
			log.Printf("[DEBUG] Allocate inode %d: lock failed: %v", f.inodeNum, err)
			return syscall.EIO
		}
		defer f.fs.lockMgr.Release(lock.LockTypeInode, uint64(f.inodeNum))

		// Re-read inode after lock
		ino, err = f.fs.inodeTbl.Read(f.inodeNum)
		if err != nil {
			log.Printf("[DEBUG] Allocate inode %d: re-read failed: %v", f.inodeNum, err)
			return syscall.EIO
		}
	}

	// Calculate block range
	startBlock := off / BlockSize
	endOffset := off + size
	endBlock := (endOffset + BlockSize - 1) / BlockSize // Round up
	totalBlocks := endBlock - startBlock

	log.Printf("[DEBUG] Allocate inode %d: offset=%d length=%d blocks=%d-%d (total=%d)",
		f.inodeNum, off, size, startBlock, endBlock-1, totalBlocks)

	// Find contiguous ranges of unallocated blocks and allocate each as a single extent.
	// This minimizes extent count and avoids hitting inline extent limits.
	blockNum := startBlock
	for blockNum < endBlock {
		// Check if already allocated
		_, _, lookupErr := f.fs.extentMgr.Lookup(ino, blockNum)
		if lookupErr == nil {
			// Already allocated, skip to next block
			blockNum++
			continue
		}

		// Find how many contiguous blocks starting from blockNum are unallocated
		rangeStart := blockNum
		rangeEnd := blockNum + 1
		for rangeEnd < endBlock {
			_, _, lookupErr := f.fs.extentMgr.Lookup(ino, rangeEnd)
			if lookupErr == nil {
				break // This block is allocated, stop the range
			}
			rangeEnd++
		}

		rangeCount := rangeEnd - rangeStart
		log.Printf("[DEBUG] Allocate inode %d: allocating contiguous range blocks=%d-%d (count=%d)",
			f.inodeNum, rangeStart, rangeEnd-1, rangeCount)

		// Allocate the entire contiguous range as a single unwritten extent
		_, allocErr := f.fs.extentMgr.AllocateUnwritten(ctx, ino, rangeStart, rangeCount)
		if allocErr != nil {
			log.Printf("[DEBUG] Allocate inode %d: AllocateUnwritten blocks %d-%d failed: %v",
				f.inodeNum, rangeStart, rangeEnd-1, allocErr)
			return syscall.ENOSPC
		}

		blockNum = rangeEnd
	}

	// Update file size if not KEEP_SIZE and new size is larger
	if mode == 0 {
		newSize := off + size
		if newSize > ino.Size {
			ino.Size = newSize
		}
	}

	// Update timestamps
	now := time.Now().UnixNano()
	ino.Mtime = now
	ino.Ctime = now
	ino.MetaGeneration++

	// Write updated inode
	if err := f.fs.inodeTbl.Write(ctx, ino); err != nil {
		log.Printf("[DEBUG] Allocate inode %d: write inode failed: %v", f.inodeNum, err)
		return syscall.EIO
	}

	log.Printf("[DEBUG] Allocate inode %d: success, AllocatedBlocks=%d WrittenBlocks=%d Size=%d",
		f.inodeNum, ino.AllocatedBlocks, ino.WrittenBlocks, ino.Size)

	return 0
}

// errToErrno converts an error to syscall.Errno.
func errToErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errno, ok := err.(syscall.Errno); ok {
		return errno
	}
	// Map common errors
	switch {
	case errors.Is(err, os.ErrNotExist):
		return syscall.ENOENT
	case errors.Is(err, os.ErrPermission):
		return syscall.EPERM
	case errors.Is(err, os.ErrExist):
		return syscall.EEXIST
	default:
		return syscall.EIO
	}
}
