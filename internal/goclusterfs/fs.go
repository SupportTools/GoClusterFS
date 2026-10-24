// Package goclusterfs implements a FUSE filesystem using GoClusterFS structures.
//
// This package integrates all Phase 2 components (bitmap, inode, extent, dir)
// into a working FUSE filesystem that can be mounted and used.
//
// Uses hanwen/go-fuse v2 for proper FUSE capability negotiation (including fallocate).
package goclusterfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"strings"
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
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/sfb"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
	"github.com/supporttools/GoClusterFS/internal/trim"
)

// errRetryWrite is an internal sentinel error indicating that a concurrent
// modification was detected and the Write operation should be retried.
var errRetryWrite = errors.New("retry write operation")

// beforeCoWCommit lets tests run another node's write after a copy-on-write,
// before this write takes the inode lock and commits the copy.
var beforeCoWCommit func()

// MaxWriteRetries is the maximum number of retry attempts for Write operations
// when concurrent modifications are detected.
const MaxWriteRetries = 3

// errRetryCreate is an internal sentinel error indicating that an inode slot
// collision was detected during Create/Mkdir and the operation should be retried.
var errRetryCreate = errors.New("retry create: inode slot collision")

// MaxCreateRetries is the maximum number of retry attempts for Create/Mkdir
// operations when inode slot collisions occur across cluster nodes.
const MaxCreateRetries = 5

// maxConflictRetries bounds how often an inode update is redone after another
// node changed the same inode's extent tree first.
const maxConflictRetries = 5

func conflictBackoff(attempt int) {
	time.Sleep(time.Duration(1+rand.Intn(5<<attempt)) * time.Millisecond)
}

// BlockSize is the size of data blocks (1MB).
const BlockSize = 1 << 20

const inodeMuStripes = 256

// SFB (Sub-File Block) constants for small file storage.
const (
	// SFBMaxFileSize is the maximum file size that can use SFB storage.
	// Files larger than this use regular extent allocation.
	SFBMaxFileSize = 64 * 1024 // 64KB

	// SFBHeaderSize is the size of the SFB container header.
	SFBHeaderSize = 64
)

// FS represents the GoClusterFS filesystem root.
// Embeds fs.Inode for go-fuse v2 node identity.
type FS struct {
	fs.Inode

	mu sync.RWMutex

	// Storage layer
	dev *storage.Device
	sb  *superblock.Superblock

	// Managers
	lockMgr     *lock.Manager
	bitmapMgr   *bitmap.Region
	inodeTbl    *inode.Table
	extentMgr   *extent.Manager
	dirMgr      *dir.Manager
	cacheMgr    *cache.Manager     // Inode cache for read optimization
	journal     *journal.Journal   // Per-node journal for crash consistency (WAL)
	trimMgr     *trim.Manager      // TRIM manager for batched block discards
	sfbMgr      *sfb.Manager       // SFB manager for small file storage (optional)
	refcountReg *refcount.Region   // Reference count table for CoW/snapshots (optional)
	snapshotReg *snapshot.Registry // Snapshot registry for managing snapshot descriptors
	snapshotWkr *snapshot.Worker   // Background worker for async refcount updates

	// Configuration
	nodeUUID   [16]byte
	dataOffset uint64 // Offset where data blocks start on disk

	// Runtime state
	uid uint32
	gid uint32

	inodeMu [inodeMuStripes]sync.Mutex
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
	extentMgr := extent.NewManager(dev, bitmapMgr, lockMgr, cfg.DataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)

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
	sfbMgr *sfb.Manager, // May be nil if SFB is disabled
	refcountReg *refcount.Region, // May be nil if snapshots not enabled
	lockMgr *lock.Manager, // May be nil for single-node use
	nodeUUID [16]byte,
	dataOffset uint64,
) *FS {
	if refcountReg != nil {
		extentMgr.SetRefcounts(refcountReg)
	}
	return &FS{
		dev:         dev,
		lockMgr:     lockMgr,
		bitmapMgr:   bm,
		inodeTbl:    inodeTbl,
		extentMgr:   extentMgr,
		dirMgr:      dirMgr,
		cacheMgr:    cacheMgr,
		journal:     j,
		trimMgr:     tm,
		sfbMgr:      sfbMgr,
		refcountReg: refcountReg,
		nodeUUID:    nodeUUID,
		dataOffset:  dataOffset,
		uid:         uint32(os.Getuid()),
		gid:         uint32(os.Getgid()),
	}
}

// InitSnapshots initializes the snapshot subsystem.
// Call this after NewWithComponents to enable snapshot functionality.
// Parameters:
//   - snapshotOffset: byte offset where snapshot region starts on device
//   - snapshotSize: size of snapshot region in bytes
//   - lockMgr: serializes registry changes across nodes (nil for single-node use)
//   - nodeSlot: this mount's node registry slot, which keeps snapshot IDs and
//     descriptor sectors apart from other nodes' even without the lock
//
// This must be called before any snapshot operations can succeed.
func (f *FS) InitSnapshots(ctx context.Context, snapshotOffset, snapshotSize uint64, lockMgr *lock.Manager, nodeSlot int) error {
	if f.refcountReg == nil {
		return fmt.Errorf("refcount region must be initialized before snapshots")
	}

	// Create snapshot registry
	f.snapshotReg = snapshot.NewRegistry(f.dev, snapshotOffset, snapshotSize, lockMgr)
	f.snapshotReg.SetNodeSlot(nodeSlot)

	// Scan registry to rebuild state
	stats, err := f.snapshotReg.Scan(ctx)
	if err != nil {
		return fmt.Errorf("failed to scan snapshot registry: %w", err)
	}
	log.Printf("[snapshot] Registry scan: %d active, %d creating, %d deleting",
		stats.ActiveCount, stats.CreatingCount, stats.DeletingCount)

	// Create and start background worker
	cfg := snapshot.DefaultWorkerConfig()
	f.snapshotWkr = snapshot.NewWorker(f.snapshotReg, f.extentMgr, f.inodeTbl, cfg)
	f.snapshotWkr.Start()

	// Recover any incomplete jobs from crash
	if err := f.snapshotWkr.RecoverIncompleteJobs(ctx); err != nil {
		log.Printf("[snapshot] Warning: failed to recover incomplete jobs: %v", err)
		// Continue anyway - snapshots may be in inconsistent state but won't crash
	}

	return nil
}

// Close closes the filesystem and releases resources.
func (f *FS) Close() error {
	// Stop snapshot worker first to complete pending jobs
	if f.snapshotWkr != nil {
		f.snapshotWkr.Stop()
	}

	// Stop TRIM manager to flush pending discards
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

// lockInode serializes this node's read-modify-write of one inode. Call it
// before taking any cluster lock and hold one at a time: inodes share stripes.
func (f *FS) lockInode(num uint32) func() {
	mu := &f.inodeMu[num%inodeMuStripes]
	mu.Lock()
	return mu.Unlock
}

// updateInode re-reads an inode, applies update, and writes it back, starting
// over when another node changed the inode's extent tree in between.
func (f *FS) updateInode(ctx context.Context, num uint32, update func(*inode.Inode) error) (*inode.Inode, error) {
	for attempt := 0; ; attempt++ {
		ino, err := f.inodeTbl.Read(num)
		if err != nil {
			return nil, err
		}
		if err := update(ino); err != nil {
			f.extentMgr.DiscardPending(ctx, ino)
			return nil, err
		}
		if err = f.inodeTbl.Write(ctx, ino); err == nil {
			return ino, nil
		}
		f.extentMgr.DiscardPending(ctx, ino)
		if !errors.Is(err, extent.ErrTreeConflict) || attempt == maxConflictRetries-1 {
			return nil, err
		}
		conflictBackoff(attempt)
	}
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
// Returns extent.Hole for holes (sparse file regions with no data on disk).
// This is the preferred method for read paths.
func (f *FS) lookupPhysical(ino *inode.Inode, logicalBlock uint64) (uint64, error) {
	phys, err := f.lookupPhysicalIn(ino, logicalBlock)
	if errors.Is(err, extent.ErrStaleTree) || errors.Is(err, extent.ErrCorruptTree) {
		// The cached inode may point at tree nodes another node has since rewritten.
		f.invalidateInode(ino.InodeNum)
		fresh, readErr := f.inodeTbl.Read(ino.InodeNum)
		if readErr != nil {
			return 0, err
		}
		return f.lookupPhysicalIn(fresh, logicalBlock)
	}
	return phys, err
}

func (f *FS) lookupPhysicalIn(ino *inode.Inode, logicalBlock uint64) (uint64, error) {
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
	if err := f.inodeTbl.Write(ctx, rootIno); err != nil {
		return err
	}

	// CRITICAL: Update the cache with the freshly-initialized root inode.
	// InitDirectory sets Size=BlockSize and allocates extent data.
	// Without this, Readdir will get stale cached data with Size=0,
	// causing it to return an empty directory listing.
	f.cacheInode(rootIno)

	return nil
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
	log.Printf("[DEBUG] FS.Lookup: name=%q", name)

	dirIno, err := f.getInode(inode.InodeRoot)
	if err != nil {
		log.Printf("[DEBUG] FS.Lookup: failed to get root inode: %v", err)
		return nil, syscall.EIO
	}

	childInodeNum, fileType, err := f.dirMgr.Lookup(ctx, dirIno, name)
	if err != nil {
		if err == dir.ErrNotFound {
			log.Printf("[DEBUG] FS.Lookup: %q not found", name)
			return nil, syscall.ENOENT
		}
		log.Printf("[DEBUG] FS.Lookup: error: %v", err)
		return nil, syscall.EIO
	}
	log.Printf("[DEBUG] FS.Lookup: found %q -> inode %d, type %d", name, childInodeNum, fileType)

	// Read the child inode to populate attributes
	childIno, err := f.inodeTbl.Read(uint32(childInodeNum))
	if err != nil {
		log.Printf("[DEBUG] FS.Lookup: failed to read child inode %d: %v", childInodeNum, err)
		return nil, syscall.EIO
	}

	// Disable FUSE kernel caching for cluster operation.
	out.EntryValid = 0
	out.AttrValid = 0

	// Populate attributes - REQUIRED even with AttrValid=0
	// The kernel uses these for the initial stat response
	out.Attr.Ino = childInodeNum
	out.Attr.Nlink = uint32(childIno.LinkCount)
	out.Attr.Uid = childIno.UID
	out.Attr.Gid = childIno.GID
	out.Attr.Atime = uint64(childIno.Atime / 1e9)
	out.Attr.Atimensec = uint32(childIno.Atime % 1e9)
	out.Attr.Mtime = uint64(childIno.Mtime / 1e9)
	out.Attr.Mtimensec = uint32(childIno.Mtime % 1e9)
	out.Attr.Ctime = uint64(childIno.Ctime / 1e9)
	out.Attr.Ctimensec = uint32(childIno.Ctime % 1e9)

	if fileType == dir.DT_DIR {
		out.Attr.Mode = uint32(fuse.S_IFDIR | (childIno.Mode & 0777))
		out.Attr.Size = BlockSize
		out.Attr.Blocks = 1 * (BlockSize / 512)
		out.Attr.Blksize = BlockSize

		childDir := &Dir{fs: f, inodeNum: uint32(childInodeNum)}
		ch := f.NewPersistentInode(ctx, childDir, fs.StableAttr{
			Mode: fuse.S_IFDIR,
			Ino:  childInodeNum,
		})
		log.Printf("[DEBUG] FS.Lookup: returning dir inode %d", childInodeNum)
		return ch, 0
	}

	out.Attr.Mode = uint32(fuse.S_IFREG | (childIno.Mode & 0777))
	out.Attr.Size = childIno.Size
	out.Attr.Blocks = childIno.AllocatedBlocks * (BlockSize / 512)
	out.Attr.Blksize = BlockSize

	childFile := &File{fs: f, inodeNum: uint32(childInodeNum)}
	ch := f.NewPersistentInode(ctx, childFile, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  childInodeNum,
	})
	log.Printf("[DEBUG] FS.Lookup: returning file inode %d, size=%d", childInodeNum, childIno.Size)
	return ch, 0
}

// Readdir returns a DirStream for the root directory listing.
func (f *FS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("[DEBUG] FS.Readdir: root directory")
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

	// === WAL: Begin transaction and log intent ===
	var txn *journal.Transaction
	var intentSeq uint64
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
		// === COMMIT POINT: After this, operation is guaranteed recoverable ===
		var commitErr error
		intentSeq, commitErr = f.journal.Commit(txn)
		if commitErr != nil {
			return nil, nil, 0, syscall.EIO
		}
	}
	// === End WAL transaction - intent is now durable ===

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

	// Update cache with freshly-created file inode
	f.cacheInode(childIno)

	// CRITICAL: Re-read parent directory inode before updating Mtime.
	// dirMgr.Add() may have allocated a new block and updated extent fields.
	// Only Size is copied back to dirIno; extent fields are stale.
	// Writing the stale dirIno would overwrite the correct extent data on disk.
	dirIno, err = f.updateInode(ctx, inode.InodeRoot, func(ino *inode.Inode) error { ino.Mtime = now; return nil })
	if err != nil {
		log.Printf("[DEBUG] Create %q: updating root inode failed: %v", name, err)
		// The file was created successfully, just log the error
	} else {
		// Update cache with parent directory inode
		f.cacheInode(dirIno)
		// Broadcast invalidation so other nodes refresh their directory cache
		if f.cacheMgr != nil {
			f.cacheMgr.BroadcastInvalidateInode(uint32(inode.InodeRoot), dirIno.DataGeneration, dirIno.MetaGeneration)
		}
	}

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

	// === WAL: Mark intent complete ===
	if f.journal != nil && intentSeq > 0 {
		if err := f.journal.CompleteIntent(intentSeq); err != nil {
			log.Printf("[DEBUG] Create %q: CompleteIntent failed: %v", name, err)
		}
	}

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

	// === WAL: Begin transaction and log intent ===
	var txn *journal.Transaction
	var intentSeq uint64
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
		// === COMMIT POINT: After this, operation is guaranteed recoverable ===
		var commitErr error
		intentSeq, commitErr = f.journal.Commit(txn)
		if commitErr != nil {
			return nil, syscall.EIO
		}
	}
	// === End WAL transaction - intent is now durable ===

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

	// Update cache with freshly-initialized child directory inode
	f.cacheInode(childIno)

	// CRITICAL: Re-read parent directory inode before updating LinkCount/Mtime.
	// dirMgr.Add() may have allocated a new block and updated extent fields.
	// Only Size is copied back to dirIno; extent fields are stale.
	// Writing the stale dirIno would overwrite the correct extent data on disk.
	dirIno, err = f.updateInode(ctx, inode.InodeRoot, func(ino *inode.Inode) error {
		ino.LinkCount++
		ino.Mtime = now
		return nil
	})
	if err != nil {
		log.Printf("[DEBUG] Mkdir %q: updating root inode failed: %v", name, err)
		// The child directory was created successfully, just log the error
	} else {
		// Update cache with parent directory inode
		f.cacheInode(dirIno)
		// Broadcast invalidation so other nodes refresh their directory cache
		if f.cacheMgr != nil {
			f.cacheMgr.BroadcastInvalidateInode(uint32(inode.InodeRoot), dirIno.DataGeneration, dirIno.MetaGeneration)
		}
	}

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

	// === WAL: Mark intent complete ===
	if f.journal != nil && intentSeq > 0 {
		if err := f.journal.CompleteIntent(intentSeq); err != nil {
			log.Printf("[DEBUG] Mkdir %q: CompleteIntent failed: %v", name, err)
		}
	}

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

	// === WAL: Begin transaction and log intent ===
	var txn *journal.Transaction
	var intentSeq uint64
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
		var commitErr error
		intentSeq, commitErr = f.journal.Commit(txn)
		if commitErr != nil {
			return syscall.EIO
		}
	}
	// === End WAL transaction - intent is now durable ===

	if err := f.dirMgr.Remove(ctx, dirIno, name); err != nil {
		return syscall.EIO
	}

	childIno.LinkCount--
	now := time.Now().UnixNano()
	childIno.Ctime = now

	if childIno.LinkCount == 0 {
		sfbBacked, sfbSlot := childIno.IsSFBBacked(), childIno.InlineExtents[0].PhysicalBlock
		if err := f.extentMgr.Delete(ctx, childIno); err != nil {
			log.Printf("[DEBUG] Unlink %q: freeing blocks failed: %v", name, err)
		}
		f.inodeTbl.Free(ctx, childIno.InodeNum)
		if sfbBacked {
			f.freeSFBSlot(ctx, sfbSlot)
		}
	} else if _, err := f.updateInode(ctx, childIno.InodeNum, func(ino *inode.Inode) error {
		ino.LinkCount--
		ino.Ctime = now
		return nil
	}); err != nil {
		log.Printf("[DEBUG] Unlink %q: updating link count failed: %v", name, err)
	}

	if _, err := f.updateInode(ctx, dirIno.InodeNum, func(ino *inode.Inode) error { ino.Mtime = now; return nil }); err != nil {
		log.Printf("[DEBUG] Unlink %q: updating directory failed: %v", name, err)
	}

	// === WAL: Mark intent complete ===
	if f.journal != nil && intentSeq > 0 {
		if err := f.journal.CompleteIntent(intentSeq); err != nil {
			log.Printf("[DEBUG] Unlink %q: CompleteIntent failed: %v", name, err)
		}
	}

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

	// === WAL: Begin transaction and log intent ===
	var txn *journal.Transaction
	var intentSeq uint64
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
		var commitErr error
		intentSeq, commitErr = f.journal.Commit(txn)
		if commitErr != nil {
			return syscall.EIO
		}
	}
	// === End WAL transaction - intent is now durable ===

	if err := f.dirMgr.Remove(ctx, dirIno, name); err != nil {
		return syscall.EIO
	}

	if err := f.extentMgr.Delete(ctx, childIno); err != nil {
		log.Printf("[DEBUG] Rmdir %q: freeing blocks failed: %v", name, err)
	}
	f.inodeTbl.Free(ctx, childIno.InodeNum)

	now := time.Now().UnixNano()
	if _, err := f.updateInode(ctx, dirIno.InodeNum, func(ino *inode.Inode) error {
		ino.LinkCount--
		ino.Mtime = now
		return nil
	}); err != nil {
		log.Printf("[DEBUG] Rmdir %q: updating directory failed: %v", name, err)
	}

	// === WAL: Mark intent complete ===
	if f.journal != nil && intentSeq > 0 {
		if err := f.journal.CompleteIntent(intentSeq); err != nil {
			log.Printf("[DEBUG] Rmdir %q: CompleteIntent failed: %v", name, err)
		}
	}

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
	log.Printf("[DEBUG] Lookup: dir=%d name=%q", d.inodeNum, name)

	dirIno, err := d.fs.getInode(d.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Lookup: failed to get dir inode: %v", err)
		return nil, syscall.EIO
	}

	childInodeNum, fileType, err := d.fs.dirMgr.Lookup(ctx, dirIno, name)
	if err != nil {
		if err == dir.ErrNotFound {
			log.Printf("[DEBUG] Lookup: %q not found", name)
			return nil, syscall.ENOENT
		}
		log.Printf("[DEBUG] Lookup: error: %v", err)
		return nil, syscall.EIO
	}
	log.Printf("[DEBUG] Lookup: found %q -> inode %d, type %d", name, childInodeNum, fileType)

	// Read the child inode to populate attributes
	childIno, err := d.fs.inodeTbl.Read(uint32(childInodeNum))
	if err != nil {
		log.Printf("[DEBUG] Lookup: failed to read child inode %d: %v", childInodeNum, err)
		return nil, syscall.EIO
	}

	// Disable FUSE kernel caching for cluster operation.
	// Other nodes may modify the filesystem at any time.
	out.EntryValid = 0
	out.AttrValid = 0

	// Populate attributes - REQUIRED even with AttrValid=0
	// The kernel uses these for the initial stat response
	out.Attr.Ino = childInodeNum
	out.Attr.Nlink = uint32(childIno.LinkCount)
	out.Attr.Uid = childIno.UID
	out.Attr.Gid = childIno.GID
	out.Attr.Atime = uint64(childIno.Atime / 1e9)
	out.Attr.Atimensec = uint32(childIno.Atime % 1e9)
	out.Attr.Mtime = uint64(childIno.Mtime / 1e9)
	out.Attr.Mtimensec = uint32(childIno.Mtime % 1e9)
	out.Attr.Ctime = uint64(childIno.Ctime / 1e9)
	out.Attr.Ctimensec = uint32(childIno.Ctime % 1e9)

	if fileType == dir.DT_DIR {
		out.Attr.Mode = uint32(fuse.S_IFDIR | (childIno.Mode & 0777))
		out.Attr.Size = BlockSize // Directories have fixed size
		out.Attr.Blocks = 1 * (BlockSize / 512)
		out.Attr.Blksize = BlockSize

		childDir := &Dir{fs: d.fs, inodeNum: uint32(childInodeNum)}
		ch := d.NewPersistentInode(ctx, childDir, fs.StableAttr{
			Mode: fuse.S_IFDIR,
			Ino:  childInodeNum,
		})
		log.Printf("[DEBUG] Lookup: returning dir inode %d", childInodeNum)
		return ch, 0
	}

	out.Attr.Mode = uint32(fuse.S_IFREG | (childIno.Mode & 0777))
	out.Attr.Size = childIno.Size
	out.Attr.Blocks = childIno.AllocatedBlocks * (BlockSize / 512)
	out.Attr.Blksize = BlockSize

	childFile := &File{fs: d.fs, inodeNum: uint32(childInodeNum)}
	ch := d.NewPersistentInode(ctx, childFile, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  childInodeNum,
	})
	log.Printf("[DEBUG] Lookup: returning file inode %d, size=%d", childInodeNum, childIno.Size)
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
	log.Printf("[DEBUG] Readdir: dir=%d", d.inodeNum)
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

		log.Printf("[DEBUG] Readdir: entry %q inode=%d type=%d", entry.Name, entry.InodeNum, entry.FileType)
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
	var intentSeq uint64
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
		var commitErr error
		intentSeq, commitErr = d.fs.journal.Commit(txn)
		if commitErr != nil {
			log.Printf("[DEBUG] Create %q: journal commit failed: %v", name, commitErr)
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

	// Update cache with freshly-created file inode
	d.fs.cacheInode(childIno)

	// CRITICAL: Re-read parent directory inode before updating Mtime.
	// dirMgr.Add() may have allocated a new block and updated extent fields.
	// Only Size is copied back to dirIno; extent fields are stale.
	// Writing the stale dirIno would overwrite the correct extent data on disk.
	dirIno, err = d.fs.updateInode(ctx, d.inodeNum, func(ino *inode.Inode) error { ino.Mtime = now; return nil })
	if err != nil {
		log.Printf("[DEBUG] Create %q: updating parent inode %d failed: %v", name, d.inodeNum, err)
		// The file was created successfully, just log the error
	} else {
		// Update cache with parent directory inode
		d.fs.cacheInode(dirIno)
		// Broadcast invalidation so other nodes refresh their directory cache
		if d.fs.cacheMgr != nil {
			d.fs.cacheMgr.BroadcastInvalidateInode(uint32(d.inodeNum), dirIno.DataGeneration, dirIno.MetaGeneration)
		}
	}

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

	// === WAL: Mark intent complete ===
	// All operations have been successfully executed; mark the intent as complete
	// so recovery won't replay it.
	if d.fs.journal != nil && intentSeq > 0 {
		if err := d.fs.journal.CompleteIntent(intentSeq); err != nil {
			// Log but don't fail - the operation completed successfully,
			// worst case is it gets replayed idempotently on recovery
			log.Printf("[DEBUG] Create %q: CompleteIntent failed: %v", name, err)
		}
	}

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
	var intentSeq uint64
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
		var commitErr error
		intentSeq, commitErr = d.fs.journal.Commit(txn)
		if commitErr != nil {
			log.Printf("[DEBUG] Mkdir %q: journal commit failed: %v", name, commitErr)
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

	// Update cache with freshly-initialized child directory inode
	d.fs.cacheInode(childIno)

	// CRITICAL: Re-read parent directory inode before updating LinkCount/Mtime.
	// dirMgr.Add() may have allocated a new block and updated extent fields.
	// Only Size is copied back to dirIno; extent fields are stale.
	// Writing the stale dirIno would overwrite the correct extent data on disk.
	dirIno, err = d.fs.updateInode(ctx, d.inodeNum, func(ino *inode.Inode) error {
		// Increment parent link count (for ..)
		ino.LinkCount++
		ino.Mtime = now
		return nil
	})
	if err != nil {
		log.Printf("[DEBUG] Mkdir %q: updating parent inode %d failed: %v", name, d.inodeNum, err)
		// The child directory was created successfully, just log the error
		// and skip updating parent LinkCount/Mtime
	} else {
		// Update cache with parent directory inode
		d.fs.cacheInode(dirIno)
		// Broadcast invalidation so other nodes refresh their directory cache
		if d.fs.cacheMgr != nil {
			d.fs.cacheMgr.BroadcastInvalidateInode(uint32(d.inodeNum), dirIno.DataGeneration, dirIno.MetaGeneration)
		}
	}

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

	// === WAL: Mark intent complete ===
	// All operations have been successfully executed; mark the intent as complete
	// so recovery won't replay it.
	if d.fs.journal != nil && intentSeq > 0 {
		if err := d.fs.journal.CompleteIntent(intentSeq); err != nil {
			// Log but don't fail - the operation completed successfully,
			// worst case is it gets replayed idempotently on recovery
			log.Printf("[DEBUG] Mkdir %q: CompleteIntent failed: %v", name, err)
		}
	}

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
	var intentSeq uint64
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
		var commitErr error
		intentSeq, commitErr = d.fs.journal.Commit(txn)
		if commitErr != nil {
			return syscall.EIO
		}
	}
	// === End WAL transaction ===

	// Now execute the actual metadata operations (safe - recoverable if crash)

	// Remove directory entry
	if err := d.fs.dirMgr.Remove(ctx, dirIno, name); err != nil {
		return syscall.EIO
	}

	// Decrement child link count and potentially free
	childIno.LinkCount--
	if childIno.LinkCount == 0 {
		sfbBacked, sfbSlot := childIno.IsSFBBacked(), childIno.InlineExtents[0].PhysicalBlock
		// Free all extents
		d.fs.extentMgr.Delete(ctx, childIno)
		// Free inode
		d.fs.inodeTbl.Free(ctx, uint32(childInodeNum))
		if sfbBacked {
			d.fs.freeSFBSlot(ctx, sfbSlot)
		}
	} else if updated, err := d.fs.updateInode(ctx, uint32(childInodeNum), func(ino *inode.Inode) error { ino.LinkCount--; return nil }); err != nil {
		log.Printf("[DEBUG] removeEntry %q: updating link count failed: %v", name, err)
	} else {
		// Cache the updated child inode
		d.fs.cacheInode(updated)
	}

	// Update directory mtime, and its link count for directory removal
	now := time.Now().UnixNano()
	if updated, err := d.fs.updateInode(ctx, d.inodeNum, func(ino *inode.Inode) error {
		if isDir {
			ino.LinkCount--
		}
		ino.Mtime = now
		return nil
	}); err != nil {
		log.Printf("[DEBUG] removeEntry %q: updating directory failed: %v", name, err)
	} else {
		// Cache the updated parent directory inode
		d.fs.cacheInode(updated)
	}

	// === WAL: Mark intent complete ===
	if d.fs.journal != nil && intentSeq > 0 {
		if err := d.fs.journal.CompleteIntent(intentSeq); err != nil {
			log.Printf("[DEBUG] removeEntry %q: CompleteIntent failed: %v", name, err)
		}
	}

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
	_ fs.InodeEmbedder     = (*File)(nil)
	_ fs.NodeOpener        = (*File)(nil)
	_ fs.NodeGetattrer     = (*File)(nil)
	_ fs.NodeSetattrer     = (*File)(nil)
	_ fs.NodeReader        = (*File)(nil)
	_ fs.NodeWriter        = (*File)(nil)
	_ fs.NodeFsyncer       = (*File)(nil)
	_ fs.NodeAllocater     = (*File)(nil)
	_ fs.NodeSetxattrer    = (*File)(nil)
	_ fs.NodeGetxattrer    = (*File)(nil)
	_ fs.NodeRemovexattrer = (*File)(nil)
)

// Open opens the file for reading/writing.
// Returns nil FileHandle to indicate the File node itself should be used as the handle.
func (f *File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	log.Printf("[DEBUG] File.Open: inode=%d flags=%d", f.inodeNum, flags)
	// Verify the inode exists and is a regular file
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] File.Open: read failed: %v", err)
		return nil, 0, syscall.EIO
	}
	if !ino.IsFile() {
		log.Printf("[DEBUG] File.Open: not a file")
		return nil, 0, syscall.EISDIR
	}
	log.Printf("[DEBUG] File.Open: success, Size=%d", ino.Size)
	// Close-to-open consistency: reads through this handle start from the on-disk inode.
	f.fs.cacheInode(ino)
	// Return nil FileHandle - the File node itself implements Read/Write/etc.
	// The fuse.FOPEN_KEEP_CACHE flag tells the kernel to keep cached data valid
	// (we don't use it in cluster mode since other nodes may modify the file).
	return nil, 0, 0
}

// Getattr returns file attributes.
func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	log.Printf("[DEBUG] File.Getattr: inode=%d", f.inodeNum)
	// Read directly from disk to get fresh inode state.
	// In multi-node scenarios, another node may have updated the inode.
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] File.Getattr: read failed: %v", err)
		return syscall.EIO
	}

	log.Printf("[DEBUG] File.Getattr: inode=%d Size=%d AllocatedBlocks=%d", f.inodeNum, ino.Size, ino.AllocatedBlocks)

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
	// Use cached inode with validation for better performance.
	// The cache manager handles TTL-based staleness checks and generation
	// counter validation, automatically refreshing stale entries from disk.
	// This is safe in multi-node scenarios because:
	// - Cache entries have TTL-based expiry (default 5s)
	// - Generation counters detect modifications by other nodes
	// - Lock acquisition triggers cache invalidation
	// - Broadcast invalidation propagates changes between nodes
	ino, err := f.fs.getInode(f.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Read inode %d: Read failed: %v", f.inodeNum, err)
		return nil, syscall.EIO
	}

	log.Printf("[DEBUG] Read inode %d: Size=%d AllocatedBlocks=%d Extents=[%+v, %+v]",
		f.inodeNum, ino.Size, ino.AllocatedBlocks, ino.InlineExtents[0], ino.InlineExtents[1])

	offset := uint64(off)

	// Check bounds using the authoritative size from disk
	if offset >= ino.Size {
		log.Printf("[DEBUG] Read inode %d: offset %d >= Size %d, returning empty", f.inodeNum, offset, ino.Size)
		return fuse.ReadResultData(nil), 0
	}

	// Calculate how much to read
	readSize := len(dest)
	if offset+uint64(readSize) > ino.Size {
		readSize = int(ino.Size - offset)
	}

	if ino.IsSFBBacked() {
		data, err := f.fs.readSFB(ino, offset, uint64(readSize))
		if err != nil {
			log.Printf("[ERROR] Read inode %d: SFB read failed: %v", f.inodeNum, err)
			return nil, syscall.EIO
		}
		return fuse.ReadResultData(data), 0
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
		if physBlock == extent.Hole {
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
		unlock := f.fs.lockInode(f.inodeNum)
		bytesWritten, err := f.writeInternal(ctx, data, uint64(off))
		unlock()
		if err == errRetryWrite {
			log.Printf("[DEBUG] Write inode %d: retry attempt %d due to concurrent modification",
				f.inodeNum, attempt+1)
			conflictBackoff(attempt)
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

// retryOnTreeConflict turns a lost extent-tree race into a write retry.
func retryOnTreeConflict(err error) error {
	if errors.Is(err, extent.ErrTreeConflict) {
		return errRetryWrite
	}
	return err
}

func (f *FS) zeroBlock(physBlock uint64) error {
	buf := storage.GetBuffer(BlockSize)
	defer storage.PutBuffer(buf)
	clear(buf.Bytes())
	_, err := f.dev.WriteAt(buf.Bytes(), int64(f.dataOffset+physBlock*BlockSize))
	return err
}

// testHookBeforeInodeCommit, when set by a test, runs before writeInternal's final
// inode write; an error it returns is handled as if that write had failed.
var testHookBeforeInodeCommit func() error

// allocateBlockLocked maps a new data block at blockNum and returns the inode
// re-read under the inode lock, which stays held on success. The bitmap comes
// before the inode in lock order, so the data block and any extent-tree blocks
// are allocated before the lock is taken. Call the returned release after
// dropping the inode lock.
func (f *File) allocateBlockLocked(ctx context.Context, ino *inode.Inode, blockNum uint64) (*inode.Inode, *extent.Extent, func(), error) {
	blocks, err := f.fs.bitmapMgr.Alloc(ctx, 1)
	if err != nil {
		return nil, nil, nil, syscall.ENOSPC
	}
	ext := &extent.Extent{LogicalBlock: blockNum, PhysicalBlock: blocks[0], Length: 1, Flags: extent.FlagAllocated}
	treeCtx, releaseTree, err := f.fs.extentMgr.ReserveTreeBlocks(ctx, ino)
	if err != nil {
		f.fs.bitmapMgr.Free(ctx, ext.PhysicalBlock)
		return nil, nil, nil, syscall.ENOSPC
	}

	if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
		f.fs.bitmapMgr.Free(ctx, ext.PhysicalBlock)
		releaseTree()
		return nil, nil, nil, syscall.EIO
	}
	var fresh *inode.Inode
	fail := func(errno error) (*inode.Inode, *extent.Extent, func(), error) {
		f.fs.lockMgr.Release(lock.LockTypeInode, uint64(f.inodeNum))
		if fresh != nil {
			f.fs.extentMgr.DiscardPending(ctx, fresh)
		}
		f.fs.bitmapMgr.Free(ctx, ext.PhysicalBlock)
		releaseTree()
		return nil, nil, nil, errno
	}
	fresh, err = f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		fresh = nil
		return fail(syscall.EIO)
	}
	// Insert fails if another node mapped this block, or grew the tree past the reservation, meanwhile.
	if err := f.fs.extentMgr.Insert(treeCtx, fresh, ext); err != nil {
		switch {
		case errors.Is(err, extent.ErrExtentTreeFull):
			return fail(syscall.EFBIG)
		case errors.Is(err, extent.ErrCorruptTree):
			return fail(syscall.EIO)
		}
		return fail(errRetryWrite)
	}
	fresh.AllocatedBlocks += ext.Length
	fresh.WrittenBlocks += ext.Length
	return fresh, ext, releaseTree, nil
}

// writeInternal performs the actual write operation.
// Returns errRetryWrite if a concurrent modification is detected and the caller should retry.
func (f *File) writeInternal(ctx context.Context, data []byte, offset uint64) (int, error) {
	bytesWritten := 0

	// Track if we hold the inode lock
	lockHeld := false
	// ino has changes made under the inode lock that are not on disk yet
	inoDirty := false
	// Tree-block reservations are released after the inode lock, as lock order requires.
	var releaseReservations []func()
	releaseInodeLock := func() {
		if lockHeld && f.fs.lockMgr != nil {
			f.fs.lockMgr.Release(lock.LockTypeInode, uint64(f.inodeNum))
			lockHeld = false
		}
	}
	defer func() {
		releaseInodeLock()
		for _, release := range releaseReservations {
			release()
		}
	}()

	// === WAL: Track transaction for extent allocations ===
	var txn *journal.Transaction
	allocatedExtents := make([]struct {
		logicalBlock  uint64
		physicalBlock uint64
		count         uint32
	}, 0)
	// Snapshot refcounts drop only after the inode write commits the copies.
	var cowDone []*extent.CoWResult
	// How many of allocatedExtents and cowDone a mid-write persist already put on disk.
	committedAllocs, committedCoW := 0, 0
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
	// persistAndUnlock writes changes made under the inode lock and drops it, so
	// the bitmap lock, which comes first in lock order, can be taken next.
	persistAndUnlock := func() error {
		if !lockHeld || f.fs.lockMgr == nil {
			return nil
		}
		if inoDirty {
			if err := f.fs.inodeTbl.WriteWithLockHeld(ino); err != nil {
				log.Printf("[DEBUG] Write inode %d: persisting inode before unlocking failed: %v", f.inodeNum, err)
				releaseInodeLock()
				f.fs.extentMgr.DiscardPending(ctx, ino)
				return retryOnTreeConflict(err)
			}
			inoDirty = false
			committedAllocs, committedCoW = len(allocatedExtents), len(cowDone)
		}
		releaseInodeLock()
		return nil
	}
	inodeWritten := false
	defer func() {
		if !inodeWritten {
			// Discarding frees tree blocks, which needs the bitmap lock: drop the inode lock first.
			releaseInodeLock()
			f.fs.extentMgr.DiscardPending(ctx, ino)
		}
	}()

	// SFB allocation is disabled: every file uses extents, and a file written by an
	// older build moves out of its SFB slot before the write.
	if ino.IsSFBBacked() {
		if err := f.migrateToExtent(ctx, ino); err != nil {
			log.Printf("[DEBUG] Write inode %d: SFB migration failed: %v", f.inodeNum, err)
			if err == errRetryWrite {
				return 0, err
			}
			return 0, syscall.EIO
		}
		ino, err = f.fs.inodeTbl.Read(f.inodeNum)
		if err != nil {
			return 0, syscall.EIO
		}
	}

	// Regular extent-based write path
	for bytesWritten < len(data) {
		blockNum := offset / BlockSize
		blockOffset := offset % BlockSize
		zeroFirst := false

		// Try to find existing extent first (without lock for read-only check)
		ext, extOffset, err := f.fs.extentMgr.Lookup(ino, blockNum)
		if err != nil && err != extent.ErrExtentNotFound {
			log.Printf("[DEBUG] Write inode %d: extent lookup at block %d failed: %v", f.inodeNum, blockNum, err)
			if errors.Is(err, extent.ErrStaleTree) {
				return 0, errRetryWrite
			}
			return 0, syscall.EIO
		}
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

			// The inode is re-read after allocation, so persist changes made under the lock first.
			if err := persistAndUnlock(); err != nil {
				return 0, err
			}

			if f.fs.lockMgr != nil {
				freshIno, newExt, releaseTree, allocErr := f.allocateBlockLocked(ctx, ino, blockNum)
				if allocErr != nil {
					log.Printf("[DEBUG] Write inode %d: allocating block %d failed: %v", f.inodeNum, blockNum, allocErr)
					return 0, allocErr
				}
				lockHeld = true
				releaseReservations = append(releaseReservations, releaseTree)
				ino = freshIno
				inoDirty = true
				ext = newExt
			} else {
				// Allocate block (acquires bitmap lock internally, then releases it)
				ext, err = f.fs.extentMgr.AllocateBlocks(ctx, ino, blockNum, 1)
				if err != nil {
					log.Printf("[DEBUG] Write inode %d: AllocateBlocks at block %d failed: %v", f.inodeNum, blockNum, err)
					switch {
					case errors.Is(err, extent.ErrExtentOverlap), errors.Is(err, extent.ErrStaleTree):
						// Concurrent allocation by another node
						return 0, errRetryWrite
					case errors.Is(err, extent.ErrExtentTreeFull):
						return 0, syscall.EFBIG
					case errors.Is(err, extent.ErrCorruptTree):
						return 0, syscall.EIO
					default:
						return 0, syscall.ENOSPC
					}
				}
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
		// A shared block is copied before the write, so its other owners keep their data.
		if ext.Flags&extent.FlagShared != 0 && f.fs.refcountReg != nil {
			// Refcounts and the bitmap come before the inode in lock order.
			if err := persistAndUnlock(); err != nil {
				return 0, err
			}
			needsCoW, cowErr := f.fs.extentMgr.NeedsCoW(ctx, blockExtent(ext, extOffset), f.fs.refcountReg)
			if cowErr != nil {
				log.Printf("[DEBUG] Write inode %d: CoW check failed for block %d: %v",
					f.inodeNum, ext.PhysicalBlock, cowErr)
				return 0, syscall.EIO
			}
			if needsCoW {
				cowResult, cowErr := f.fs.extentMgr.CopyOnWrite(ctx, ino, ext, blockNum, f.fs.dataOffset, f.fs.refcountReg)
				if cowErr != nil {
					log.Printf("[DEBUG] Write inode %d: CoW failed for block %d: %v",
						f.inodeNum, ext.PhysicalBlock, cowErr)
					f.fs.extentMgr.DiscardPending(ctx, ino)
					return 0, syscall.EIO
				}
				if cowResult.WasShared {
					if beforeCoWCommit != nil {
						beforeCoWCommit()
					}
					// Copied on an unlocked copy: keep it only if no node wrote the inode meanwhile.
					if f.fs.lockMgr != nil {
						if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
							log.Printf("[DEBUG] Write inode %d: failed to acquire lock for CoW: %v", f.inodeNum, err)
							f.fs.extentMgr.DiscardPending(ctx, ino)
							f.fs.extentMgr.FreePhysicalBlock(ctx, cowResult.NewPhysBlock)
							return 0, syscall.EIO
						}
						lockHeld = true
						freshIno, err := f.fs.inodeTbl.Read(f.inodeNum)
						if err != nil || freshIno.MetaGeneration != ino.MetaGeneration {
							log.Printf("[DEBUG] Write inode %d: inode changed during CoW (err=%v)", f.inodeNum, err)
							releaseInodeLock()
							f.fs.extentMgr.DiscardPending(ctx, ino)
							f.fs.extentMgr.FreePhysicalBlock(ctx, cowResult.NewPhysBlock)
							return 0, errRetryWrite
						}
					}
					inoDirty = true
					cowDone = append(cowDone, cowResult)
					log.Printf("[DEBUG] Write inode %d: CoW completed, old block %d -> new block %d",
						f.inodeNum, cowResult.OldPhysBlock, cowResult.NewPhysBlock)
					if ext, extOffset, cowErr = f.fs.extentMgr.Lookup(ino, blockNum); cowErr != nil {
						log.Printf("[DEBUG] Write inode %d: extent lookup after CoW failed: %v", f.inodeNum, cowErr)
						return 0, syscall.EIO
					}
				}
			}
		}

		// Check if this is an unwritten extent (from fallocate).
		// If so, we need to clear the Unwritten flag on first write.
		if ext.Flags&extent.FlagUnwritten != 0 && f.fs.lockMgr != nil {
			// Splitting the extent may allocate tree blocks, which needs the bitmap lock
			// before the inode lock. Clear on an unlocked copy, then keep it only if no
			// node wrote the inode before the lock was taken.
			if err := persistAndUnlock(); err != nil {
				return 0, err
			}
			cleared, clearErr := f.fs.extentMgr.ClearUnwritten(ctx, ino, blockNum)
			if clearErr != nil {
				log.Printf("[DEBUG] Write inode %d: clearing Unwritten flag at block %d failed: %v",
					f.inodeNum, blockNum, clearErr)
				f.fs.extentMgr.DiscardPending(ctx, ino)
				if errors.Is(clearErr, extent.ErrStaleTree) {
					return 0, errRetryWrite
				}
				return 0, syscall.EIO
			}
			if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
				log.Printf("[DEBUG] Write inode %d: failed to acquire lock for unwritten: %v", f.inodeNum, err)
				f.fs.extentMgr.DiscardPending(ctx, ino)
				return 0, syscall.EIO
			}
			lockHeld = true
			freshIno, err := f.fs.inodeTbl.Read(f.inodeNum)
			if err != nil || freshIno.MetaGeneration != ino.MetaGeneration {
				log.Printf("[DEBUG] Write inode %d: inode changed during unwritten clear (err=%v)", f.inodeNum, err)
				releaseInodeLock()
				f.fs.extentMgr.DiscardPending(ctx, ino)
				return 0, errRetryWrite
			}
			inoDirty = inoDirty || cleared
			zeroFirst = cleared
			if ext, extOffset, err = f.fs.extentMgr.Lookup(ino, blockNum); err != nil {
				log.Printf("[DEBUG] Write inode %d: lookup after unwritten clear failed: %v", f.inodeNum, err)
				return 0, syscall.EIO
			}
		} else if ext.Flags&extent.FlagUnwritten != 0 {
			cleared, clearErr := f.fs.extentMgr.ClearUnwritten(ctx, ino, blockNum)
			if clearErr != nil {
				log.Printf("[DEBUG] Write inode %d: clearing Unwritten flag at block %d failed: %v",
					f.inodeNum, blockNum, clearErr)
				return 0, syscall.EIO
			}
			zeroFirst = cleared
		}

		// Calculate physical location
		physBlock := ext.PhysicalBlock + extOffset
		diskOffset := int64(f.fs.dataOffset + physBlock*BlockSize + blockOffset)

		// Write to disk
		toWrite := int(BlockSize - blockOffset)
		if toWrite > len(data)-bytesWritten {
			toWrite = len(data) - bytesWritten
		}
		zeroFirst = zeroFirst && toWrite < BlockSize

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

		// The rest of a block first written here would otherwise expose old disk contents.
		if zeroFirst {
			if err := f.fs.zeroBlock(physBlock); err != nil {
				log.Printf("[DEBUG] Write inode %d: zeroing block %d failed: %v", f.inodeNum, physBlock, err)
				if extentLockHeld {
					f.fs.lockMgr.ReleaseExtent(physBlock)
				}
				return 0, syscall.EIO
			}
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
		return 0, syscall.EIO
	}
	// ino was read before the lock, so it can miss another node's extents.
	if acquiredLockForSize {
		ino = freshIno
	}
	// Use the freshest size from disk, then calculate new size as max(fresh_size, our_write_end)
	newSize := freshIno.Size
	if offset > newSize {
		newSize = offset // Our write extended the file
	}
	newMtime := time.Now().UnixNano()

	// === WAL: Log extent allocations and inode update before committing metadata ===
	var intentSeq uint64
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
		var commitErr error
		intentSeq, commitErr = f.fs.journal.Commit(txn)
		if commitErr != nil {
			log.Printf("[DEBUG] Write inode %d: journal commit failed: %v", f.inodeNum, commitErr)
			return 0, syscall.EIO
		}
		txn = nil // Mark as committed so defer doesn't abort
	}
	// === End WAL transaction ===

	// Apply the metadata changes
	ino.Size = newSize
	ino.Mtime = newMtime

	// releaseCopied drops this file's ownership of blocks it copied away from.
	// Blocks an older version shared without counting stay allocated.
	releaseCopied := func(cows []*extent.CoWResult) {
		var old []uint64
		for _, cow := range cows {
			if !cow.Uncounted {
				old = append(old, cow.OldPhysBlock)
			}
		}
		if len(old) > 0 {
			f.fs.extentMgr.ReleaseBlocks(ctx, old)
		}
	}

	// On a tree conflict the blocks allocated since the last mid-write persist are
	// unreferenced. Close the intent first so journal replay cannot map them again,
	// then free them. Blocks the persist committed stay mapped on disk.
	writeFailed := func(err error) error {
		// The bitmap lock comes before the inode lock, so free only after dropping it.
		releaseInodeLock()
		f.fs.extentMgr.DiscardPending(ctx, ino)
		if !errors.Is(err, extent.ErrTreeConflict) {
			return syscall.EIO
		}
		if f.fs.journal != nil && intentSeq > 0 {
			if err := f.fs.journal.CompleteIntent(intentSeq); err != nil {
				log.Printf("[DEBUG] Write inode %d: CompleteIntent after tree conflict failed: %v", f.inodeNum, err)
				return errRetryWrite
			}
		}
		var unused []uint64
		for _, alloc := range allocatedExtents[committedAllocs:] {
			for i := uint64(0); i < uint64(alloc.count); i++ {
				unused = append(unused, alloc.physicalBlock+i)
			}
		}
		for _, cow := range cowDone[committedCoW:] {
			unused = append(unused, cow.NewPhysBlock)
		}
		for _, b := range unused {
			if err := f.fs.extentMgr.FreePhysicalBlock(ctx, b); err != nil {
				log.Printf("[DEBUG] Write inode %d: freeing block %d after tree conflict failed: %v", f.inodeNum, b, err)
			}
		}
		// The retry finds these blocks already copied, so release the old ones now.
		releaseCopied(cowDone[:committedCoW])
		return errRetryWrite
	}

	if testHookBeforeInodeCommit != nil {
		if err := testHookBeforeInodeCommit(); err != nil {
			return 0, writeFailed(err)
		}
	}
	// Write inode - use WriteWithLockHeld if we hold the lock
	if lockHeld {
		if err := f.fs.inodeTbl.WriteWithLockHeld(ino); err != nil {
			log.Printf("[DEBUG] Write inode %d: WriteWithLockHeld failed: %v", f.inodeNum, err)
			return 0, writeFailed(err)
		}
	} else {
		if err := f.fs.inodeTbl.Write(ctx, ino); err != nil {
			log.Printf("[DEBUG] Write inode %d: final inodeTbl.Write failed: %v", f.inodeNum, err)
			return 0, writeFailed(err)
		}
	}

	inodeWritten = true

	releaseInodeLock()

	releaseCopied(cowDone)

	// Cache the freshly-written inode for subsequent reads
	f.fs.cacheInode(ino)

	// Broadcast inode invalidation to other nodes for immediate visibility.
	// This is fire-and-forget - doesn't block the write operation.
	if f.fs.cacheMgr != nil {
		f.fs.cacheMgr.BroadcastInvalidateInode(uint32(f.inodeNum), ino.DataGeneration, ino.MetaGeneration)
	}

	// === WAL: Mark intent complete ===
	// Now that all metadata and data have been persisted, mark the intent as complete.
	// On recovery, this prevents replaying already-completed operations.
	if f.fs.journal != nil && intentSeq > 0 {
		if err := f.fs.journal.CompleteIntent(intentSeq); err != nil {
			log.Printf("[DEBUG] Write inode %d: CompleteIntent failed: %v", f.inodeNum, err)
		}
	}

	return bytesWritten, nil
}

// sfbSlotOffset returns the disk offset of an SFB-backed inode's data. SFB
// allocation is disabled; this serves only inodes written by older builds.
func (f *FS) sfbSlotOffset(ino *inode.Inode) (int64, error) {
	ext := ino.InlineExtents[0]
	container, slot := sfb.UnpackPhysicalBlock(ext.PhysicalBlock)
	slotSize, _ := sfb.Tier((ext.Flags >> 4) & 0x3).Info()
	if ino.Size > uint64(slotSize) {
		return 0, fmt.Errorf("SFB inode %d: size %d exceeds its %d-byte slot", ino.InodeNum, ino.Size, slotSize)
	}
	return int64(f.dataOffset + container*BlockSize + SFBHeaderSize + uint64(slot)*uint64(slotSize)), nil
}

// readSFB reads n bytes at off from an SFB-backed inode's slot. Slots are not
// sector-aligned, so it reads the sectors that cover the range.
func (f *FS) readSFB(ino *inode.Inode, off, n uint64) ([]byte, error) {
	start, err := f.sfbSlotOffset(ino)
	if err != nil || n == 0 {
		return nil, err
	}
	from := start + int64(off)
	aligned := from &^ (storage.SectorSize - 1)
	length := (from + int64(n) - aligned + storage.SectorSize - 1) &^ (storage.SectorSize - 1)
	buf := storage.GetBuffer(int(length))
	defer storage.PutBuffer(buf)
	if _, err := f.dev.ReadAt(buf.Bytes()[:length], aligned); err != nil {
		return nil, err
	}
	return bytes.Clone(buf.Bytes()[from-aligned : from-aligned+int64(n)]), nil
}

// freeSFBSlot clears a slot's bit in its container header. A failure leaks the
// slot, which is safe because nothing allocates SFB slots any more.
func (f *FS) freeSFBSlot(ctx context.Context, packed uint64) {
	container, slot := sfb.UnpackPhysicalBlock(packed)
	if f.lockMgr != nil {
		if err := f.lockMgr.Acquire(ctx, lock.LockTypeSFBContainer, container, lock.LockModeExclusive); err != nil {
			log.Printf("[WARN] Free SFB slot %d:%d: container lock failed: %v", container, slot, err)
			return
		}
		defer f.lockMgr.Release(lock.LockTypeSFBContainer, container)
	}

	buf := storage.GetBuffer(storage.SectorSize)
	defer storage.PutBuffer(buf)
	sector := buf.Bytes()[:storage.SectorSize]
	off := int64(f.dataOffset + container*BlockSize)
	if _, err := f.dev.ReadAt(sector, off); err != nil {
		log.Printf("[WARN] Free SFB slot %d:%d: reading container header failed: %v", container, slot, err)
		return
	}
	h, err := sfb.DeserializeHeader(sector)
	if err != nil || !bytes.Equal(h.Serialize(), sector[:sfb.HeaderSize]) || slot >= h.SlotCount {
		log.Printf("[WARN] Free SFB slot %d:%d: no valid container header, leaking the slot", container, slot)
		return
	}
	bit := byte(1) << (slot % 8)
	if h.Bitmap[slot/8]&bit == 0 {
		log.Printf("[WARN] Free SFB slot %d:%d: slot already free", container, slot)
		return
	}
	h.Bitmap[slot/8] &^= bit
	h.FreeSlots++
	copy(sector, h.Serialize())
	if _, err := f.dev.WriteAt(sector, off); err != nil {
		log.Printf("[WARN] Free SFB slot %d:%d: writing container header failed: %v", container, slot, err)
	}
}

// migrateToExtent moves an SFB-backed file's data into a full block and frees
// its slot. It fails rather than map the file to a block missing its data.
func (f *File) migrateToExtent(ctx context.Context, ino *inode.Inode) error {
	if !ino.IsSFBBacked() {
		return nil // Already using extents
	}

	data, err := f.fs.readSFB(ino, 0, ino.Size)
	if err != nil {
		return fmt.Errorf("reading SFB slot: %w", err)
	}
	oldSlot := ino.InlineExtents[0].PhysicalBlock
	oldContainer, oldSlotIndex := sfb.UnpackPhysicalBlock(oldSlot)

	newBlocks, err := f.fs.bitmapMgr.Alloc(ctx, 1)
	if err != nil {
		return fmt.Errorf("failed to allocate block for migration: %w", err)
	}
	newBlock := newBlocks[0]

	buf := storage.GetBuffer(BlockSize)
	copy(buf.Bytes(), data)
	_, err = f.fs.dev.WriteAt(buf.Bytes()[:BlockSize], int64(f.fs.dataOffset+newBlock*BlockSize))
	storage.PutBuffer(buf)
	if err != nil {
		_ = f.fs.bitmapMgr.Free(ctx, newBlock)
		return fmt.Errorf("failed to write migrated data: %w", err)
	}

	// === WAL: Log intent to migrate from SFB to extent ===
	var intentSeq uint64
	if f.fs.journal != nil {
		txn := f.fs.journal.Begin()
		if err := txn.LogSFBMigrate(&journal.SFBMigratePayload{
			InodeNum:     uint64(f.inodeNum),
			OldContainer: oldContainer,
			OldSlot:      oldSlotIndex,
			NewBlock:     newBlock,
		}); err != nil {
			f.fs.journal.Abort(txn)
			_ = f.fs.bitmapMgr.Free(ctx, newBlock)
			return err
		}
		// === COMMIT POINT ===
		var commitErr error
		intentSeq, commitErr = f.fs.journal.Commit(txn)
		if commitErr != nil {
			_ = f.fs.bitmapMgr.Free(ctx, newBlock)
			return commitErr
		}
	}

	ino.InlineExtents[0] = inode.InlineExtent{PhysicalBlock: newBlock, Length: 1, Flags: extent.FlagAllocated}
	ino.AllocatedBlocks = 1
	ino.WrittenBlocks = 1
	ino.Mtime = time.Now().UnixNano()

	if err := f.fs.inodeTbl.Write(ctx, ino); err != nil {
		// Only a conflict proves the inode on disk does not map newBlock.
		if errors.Is(err, extent.ErrTreeConflict) {
			_ = f.fs.bitmapMgr.Free(ctx, newBlock)
		}
		return retryOnTreeConflict(err)
	}

	f.fs.freeSFBSlot(ctx, oldSlot)

	// === WAL: Mark intent complete ===
	if f.fs.journal != nil && intentSeq > 0 {
		if err := f.fs.journal.CompleteIntent(intentSeq); err != nil {
			log.Printf("[DEBUG] migrateToExtent inode %d: CompleteIntent failed: %v", f.inodeNum, err)
		}
	}

	log.Printf("[INFO] Migrated inode %d from SFB (%d:%d) to extent (block %d)",
		f.inodeNum, oldContainer, oldSlotIndex, newBlock)

	return nil
}

// Setattr sets file attributes (used for truncate, chmod, etc.).
func (f *File) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	for attempt := 0; ; attempt++ {
		unlock := f.fs.lockInode(f.inodeNum)
		errno, err := f.setattr(ctx, fh, in, out)
		unlock()
		if err == nil {
			return errno
		}
		if attempt == maxConflictRetries-1 {
			return syscall.EAGAIN
		}
		conflictBackoff(attempt)
	}
}

// setattr applies one Setattr attempt. A non-nil error means another node
// changed the inode's extent tree first and the attempt should be redone.
func (f *File) setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) (syscall.Errno, error) {
	// Invalidate cache before modifying - ensures we read fresh data
	f.fs.invalidateInode(f.inodeNum)

	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		return syscall.EIO, nil
	}

	if _, resizing := in.GetSize(); resizing && ino.IsSFBBacked() {
		if err := f.migrateToExtent(ctx, ino); err != nil {
			log.Printf("[DEBUG] Setattr inode %d: SFB migration failed: %v", f.inodeNum, err)
			if err == errRetryWrite {
				return 0, err
			}
			return syscall.EIO, nil
		}
		if ino, err = f.fs.inodeTbl.Read(f.inodeNum); err != nil {
			return syscall.EIO, nil
		}
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
	var intentSeq uint64
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
				return syscall.EIO, nil
			}
		}

		// Log inode update (InodeUpdatePayload only stores inode number and mask)
		if err := txn.LogInodeUpdate(&journal.InodeUpdatePayload{
			InodeNum:   uint64(f.inodeNum),
			UpdateMask: mask,
		}); err != nil {
			f.fs.journal.Abort(txn)
			return syscall.EIO, nil
		}

		// === COMMIT POINT: After this, operation is guaranteed recoverable ===
		var commitErr error
		intentSeq, commitErr = f.fs.journal.Commit(txn)
		if commitErr != nil {
			return syscall.EIO, nil
		}
	}
	// === End WAL transaction ===

	// Now execute the actual operations (safe - recoverable if crash)
	if sz, ok := in.GetSize(); ok {
		if sz < ino.Size {
			// Truncate
			if err := f.fs.extentMgr.Truncate(ctx, ino, sz); err != nil {
				return syscall.EIO, nil
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
		f.fs.extentMgr.DiscardPending(ctx, ino)
		if errors.Is(err, extent.ErrTreeConflict) {
			// Nothing was applied, so close the intent before the retry logs a new one.
			if f.fs.journal != nil && intentSeq > 0 {
				f.fs.journal.CompleteIntent(intentSeq)
			}
			return 0, err
		}
		return syscall.EIO, nil
	}

	// Cache the freshly-written inode for subsequent reads
	f.fs.cacheInode(ino)

	// === WAL: Mark intent complete ===
	if f.fs.journal != nil && intentSeq > 0 {
		if err := f.fs.journal.CompleteIntent(intentSeq); err != nil {
			log.Printf("[DEBUG] Setattr inode %d: CompleteIntent failed: %v", f.inodeNum, err)
		}
	}

	return f.Getattr(ctx, fh, out), nil
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

	for attempt := 0; ; attempt++ {
		unlock := f.fs.lockInode(f.inodeNum)
		errno, err := f.allocate(ctx, off, size, mode)
		unlock()
		if err == nil {
			return errno
		}
		if attempt == maxConflictRetries-1 {
			return syscall.EAGAIN
		}
		conflictBackoff(attempt)
	}
}

// allocate makes one fallocate attempt. A non-nil error means another node
// changed the inode's extent tree first; the blocks allocated here are freed.
func (f *File) allocate(ctx context.Context, off uint64, size uint64, mode uint32) (syscall.Errno, error) {
	// Read current inode
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		log.Printf("[DEBUG] Allocate inode %d: read failed: %v", f.inodeNum, err)
		return syscall.EIO, nil
	}

	// Until the inode write commits them, blocks allocated here belong to nobody.
	var newBlocks []uint64
	committed := false
	defer func() {
		if !committed {
			f.fs.extentMgr.DiscardPending(ctx, ino)
			for _, b := range newBlocks {
				f.fs.extentMgr.FreePhysicalBlock(ctx, b)
			}
		}
	}()

	// Note: We don't hold the inode lock during block allocation to respect lock ordering.
	// Lock order: bitmap (6) must be acquired before inode (8).
	// AllocateUnwritten acquires bitmap lock internally.
	// We acquire the inode lock only at the end to update metadata atomically.

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
		if lookupErr != extent.ErrExtentNotFound {
			log.Printf("[DEBUG] Allocate inode %d: extent lookup at block %d failed: %v", f.inodeNum, blockNum, lookupErr)
			return syscall.EIO, nil
		}

		// Find how many contiguous blocks starting from blockNum are unallocated
		rangeStart := blockNum
		rangeEnd := blockNum + 1
		for rangeEnd < endBlock {
			_, _, lookupErr := f.fs.extentMgr.Lookup(ino, rangeEnd)
			if lookupErr != extent.ErrExtentNotFound {
				break // Allocated (or unreadable), stop the range
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
			if errors.Is(allocErr, extent.ErrExtentTreeFull) {
				return syscall.EFBIG, nil
			}
			return syscall.ENOSPC, nil
		}
		for b := rangeStart; b < rangeEnd; b++ {
			if ext, inExt, err := f.fs.extentMgr.Lookup(ino, b); err == nil {
				newBlocks = append(newBlocks, ext.PhysicalBlock+inExt)
			}
		}

		blockNum = rangeEnd
	}

	// Acquire inode lock for metadata update
	if f.fs.lockMgr != nil {
		if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
			log.Printf("[DEBUG] Allocate inode %d: lock failed: %v", f.inodeNum, err)
			return syscall.EIO, nil
		}
		defer f.fs.lockMgr.Release(lock.LockTypeInode, uint64(f.inodeNum))

		// ino holds the new extents, so keep it only if no node wrote the inode since it was read.
		fresh, err := f.fs.inodeTbl.Read(f.inodeNum)
		if err != nil {
			log.Printf("[DEBUG] Allocate inode %d: re-read failed: %v", f.inodeNum, err)
			return syscall.EIO, nil
		}
		if fresh.MetaGeneration != ino.MetaGeneration {
			return 0, extent.ErrStaleTree
		}
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
		if errors.Is(err, extent.ErrTreeConflict) {
			return 0, err
		}
		committed = true // the inode on disk may already map newBlocks
		return syscall.EIO, nil
	}
	committed = true

	log.Printf("[DEBUG] Allocate inode %d: success, AllocatedBlocks=%d WrittenBlocks=%d Size=%d",
		f.inodeNum, ino.AllocatedBlocks, ino.WrittenBlocks, ino.Size)

	return 0, nil
}

// Setxattr sets an extended attribute on the file.
// Used for snapshot creation via user.snapshot attribute and clone creation via user.clone.
func (f *File) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	defer f.fs.lockInode(f.inodeNum)()
	switch attr {
	case "user.snapshot":
		return f.createSnapshot(ctx, string(data))
	case "user.snapshot.delete":
		return f.deleteSnapshot(ctx, string(data))
	case "user.clone":
		return f.createClone(ctx, string(data))
	case "user.snapshot.clone":
		return f.createCloneFromSnapshot(ctx, string(data))
	default:
		return syscall.ENODATA
	}
}

// Getxattr gets an extended attribute from the file.
func (f *File) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	log.Printf("[DEBUG] File.Getxattr: inode=%d attr=%s", f.inodeNum, attr)

	switch attr {
	case "user.snapshots":
		return f.listSnapshots(ctx, dest)
	default:
		return 0, syscall.ENODATA
	}
}

// Removexattr removes an extended attribute from the file.
// Used for snapshot deletion via user.snapshot.<name> pattern.
func (f *File) Removexattr(ctx context.Context, attr string) syscall.Errno {
	log.Printf("[DEBUG] File.Removexattr: inode=%d attr=%s", f.inodeNum, attr)

	// Handle user.snapshot.<name> for snapshot deletion
	const snapshotPrefix = "user.snapshot."
	if strings.HasPrefix(attr, snapshotPrefix) {
		snapName := strings.TrimPrefix(attr, snapshotPrefix)
		if snapName == "" {
			return syscall.EINVAL
		}
		return f.deleteSnapshot(ctx, snapName)
	}

	return syscall.ENODATA
}

// markShared marks every extent shared so writes to either copy use CoW.
func (f *FS) markShared(ino *inode.Inode) error {
	if err := f.extentMgr.MarkAllShared(ino); err != nil {
		return err
	}
	ino.Flags |= inode.FlagHasSnapshot
	ino.MetaGeneration++
	return nil
}

// errUncountedSharing means the file shares blocks the way older versions did,
// without refcounts, so its blocks cannot be shared again safely.
var errUncountedSharing = errors.New("an older version shared this file's blocks without refcounts; run goclusterfs-fsck, and copy the file to snapshot or clone it")

// shareInto gives dst the extents of inode srcNum, sharing its data blocks.
// src is marked shared first, then each block gains an owner, durably, before
// dst maps it; a crash in between leaves src copying blocks it alone owns.
// stillValid, if set, runs after the blocks gain owners and before dst maps
// them. On error the caller discards dst, which gives back any blocks it maps.
func (f *FS) shareInto(ctx context.Context, srcNum uint32, dst *inode.Inode, stillValid func() error) error {
	src, err := f.inodeTbl.Read(srcNum)
	if err != nil {
		return err
	}
	uncounted, err := f.extentMgr.HasUncountedSharing(src)
	if err != nil {
		return err
	}
	if uncounted {
		return errUncountedSharing
	}
	// A snapshot's data inode is marked shared when it is created and never written again.
	if src.Flags&inode.FlagSnapshotData == 0 {
		if src, err = f.updateInode(ctx, srcNum, f.markShared); err != nil {
			return err
		}
	}
	blocks, err := f.extentMgr.DataBlocks(src)
	if err != nil {
		return err
	}
	if err := f.refcountReg.Share(ctx, blocks); err != nil {
		return err
	}
	err = f.dev.Sync()
	if err == nil && stillValid != nil {
		err = stillValid()
	}
	if err != nil {
		f.extentMgr.ReleaseBlocks(ctx, blocks)
		return err
	}
	if err := f.extentMgr.CopyExtentsForClone(ctx, src, dst); err != nil {
		f.extentMgr.ReleaseBlocks(ctx, blocks)
		return err
	}
	dst.Size = src.Size
	if err := f.inodeTbl.Write(ctx, dst); err != nil {
		f.extentMgr.DiscardPending(ctx, dst)
		// Only a conflict proves dst on disk does not map the blocks.
		if errors.Is(err, extent.ErrTreeConflict) {
			f.extentMgr.ReleaseBlocks(ctx, blocks)
		}
		return err
	}
	return nil
}

// sharingErrno maps a failure to share a file's blocks to an errno.
func sharingErrno(err error) syscall.Errno {
	if errors.Is(err, errUncountedSharing) {
		return syscall.ENOTSUP
	}
	return syscall.EIO
}

// claimInode allocates an inode slot, retrying when another node takes the
// free slot first.
func (f *FS) claimInode(ctx context.Context) (*inode.Inode, error) {
	for attempt := 0; ; attempt++ {
		num, err := f.inodeTbl.FindFreeSlot()
		if err != nil {
			return nil, err
		}
		ino, err := f.inodeTbl.ClaimSlot(ctx, num)
		if !errors.Is(err, inode.ErrInodeInUse) || attempt == maxConflictRetries-1 {
			return ino, err
		}
	}
}

// discardInode gives back the blocks an unlinked inode maps and frees it. The
// empty extent map is committed before any block is released.
func (f *FS) discardInode(ctx context.Context, num uint32) {
	_, err := f.updateInode(ctx, num, func(ino *inode.Inode) error {
		return f.extentMgr.Truncate(ctx, ino, 0)
	})
	if err == nil {
		err = f.inodeTbl.Free(ctx, num)
	}
	if err != nil {
		log.Printf("[clone] Failed to discard inode %d: %v", num, err)
	}
}

// dropSnapshot marks a snapshot deleting and queues the job that gives back
// its blocks. A snapshot left deleting is finished by the next mount.
func (f *FS) dropSnapshot(ctx context.Context, snapshotID uint64) error {
	if err := f.snapshotReg.Delete(ctx, snapshotID); err != nil {
		return err
	}
	if _, err := f.snapshotWkr.QueueDeleteJob(ctx, snapshotID); err != nil {
		log.Printf("[snapshot] Delete of snapshot %d waits for the next mount: %v", snapshotID, err)
	}
	return nil
}

// sharableInode reads this file's inode, moving it out of an SFB container
// first: container slots are written in place and cannot be shared.
func (f *File) sharableInode(ctx context.Context) (*inode.Inode, error) {
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil || !ino.IsSFBBacked() {
		return ino, err
	}
	if err := f.migrateToExtent(ctx, ino); err != nil {
		return nil, err
	}
	return f.fs.inodeTbl.Read(f.inodeNum)
}

// createSnapshot creates a new snapshot of this file. The snapshot's data is a
// hidden inode that shares the file's blocks as they are now.
func (f *File) createSnapshot(ctx context.Context, name string) syscall.Errno {
	if f.fs.snapshotReg == nil || f.fs.snapshotWkr == nil {
		log.Printf("[DEBUG] createSnapshot: snapshot subsystem not initialized")
		return syscall.ENOTSUP
	}
	if len(name) >= snapshot.MaxSnapshotNameLen {
		return syscall.ENAMETOOLONG
	}

	src, err := f.sharableInode(ctx)
	if err != nil {
		log.Printf("[snapshot] Failed to read inode %d: %v", f.inodeNum, err)
		return syscall.EIO
	}

	data, err := f.fs.claimInode(ctx)
	if err != nil {
		log.Printf("[snapshot] No inode for the data of a snapshot of inode %d: %v", f.inodeNum, err)
		return syscall.ENOSPC
	}
	data.Mode, data.UID, data.GID = src.Mode, src.UID, src.GID
	data.Mtime, data.Ctime = src.Mtime, src.Ctime
	data.Flags |= inode.FlagSnapshotData
	if err := f.fs.inodeTbl.Write(ctx, data); err != nil {
		f.fs.inodeTbl.Free(ctx, data.InodeNum)
		log.Printf("[snapshot] Failed to write data inode %d: %v", data.InodeNum, err)
		return syscall.EIO
	}

	desc, err := f.fs.snapshotReg.Create(ctx, name, f.inodeNum, 0,
		data.InodeNum, src.AllocatedBlocks, src.Size, snapshot.FlagReadOnly)
	if err != nil {
		f.fs.inodeTbl.Free(ctx, data.InodeNum)
		if errors.Is(err, snapshot.ErrSnapshotExists) {
			return syscall.EEXIST
		}
		if errors.Is(err, snapshot.ErrNameTooLong) {
			return syscall.ENAMETOOLONG
		}
		return syscall.EIO
	}

	err = f.fs.shareInto(ctx, f.inodeNum, data, nil)
	if err == nil {
		err = f.fs.snapshotReg.Activate(ctx, desc.SnapshotID)
	}
	if err != nil {
		log.Printf("[snapshot] Failed to create '%s' for inode %d: %v", name, f.inodeNum, err)
		if dropErr := f.fs.dropSnapshot(ctx, desc.SnapshotID); dropErr != nil {
			log.Printf("[snapshot] Failed to roll back snapshot %d: %v", desc.SnapshotID, dropErr)
		}
		return sharingErrno(err)
	}

	log.Printf("[snapshot] Created '%s' (ID=%d) for inode %d", name, desc.SnapshotID, f.inodeNum)
	return 0
}

// deleteSnapshot deletes a snapshot by name.
func (f *File) deleteSnapshot(ctx context.Context, name string) syscall.Errno {
	if f.fs.snapshotReg == nil || f.fs.snapshotWkr == nil {
		return syscall.ENOTSUP
	}

	desc, err := f.fs.snapshotReg.GetByName(ctx, name)
	if err != nil {
		if errors.Is(err, snapshot.ErrSnapshotNotFound) {
			return syscall.ENOENT
		}
		return syscall.EIO
	}

	if desc.TargetInode != f.inodeNum {
		return syscall.EINVAL
	}

	if err := f.fs.dropSnapshot(ctx, desc.SnapshotID); err != nil {
		log.Printf("[snapshot] Failed to delete '%s': %v", name, err)
		return syscall.EIO
	}

	log.Printf("[snapshot] Deleting '%s' (ID=%d)", name, desc.SnapshotID)
	return 0
}

// createClone creates a writable clone of this file at the specified destination path.
// The clone shares physical blocks with the source until either is modified, at which
// point Copy-on-Write allocates new blocks for the modified data.
func (f *File) createClone(ctx context.Context, destPath string) syscall.Errno {
	log.Printf("[clone] Creating clone of inode %d at %s", f.inodeNum, destPath)
	if _, err := f.sharableInode(ctx); err != nil {
		log.Printf("[clone] Failed to read inode %d: %v", f.inodeNum, err)
		return syscall.EIO
	}
	return f.cloneInode(ctx, f.inodeNum, destPath, nil)
}

// cloneInode creates a file at destPath that shares the blocks of inode srcNum.
// stillValid is passed to shareInto.
func (f *File) cloneInode(ctx context.Context, srcNum uint32, destPath string, stillValid func() error) syscall.Errno {
	if f.fs.refcountReg == nil {
		return syscall.ENOTSUP
	}

	// Parse destination path
	parentPath, cloneName, err := parseClonePath(destPath)
	if err != nil {
		log.Printf("[clone] Invalid path %s: %v", destPath, err)
		return syscall.EINVAL
	}

	// Phase 1: Only support clones in root directory
	if parentPath != "/" {
		log.Printf("[clone] Subdirectory clones not yet supported: %s", parentPath)
		return syscall.ENOTSUP
	}

	// Get root directory inode
	rootIno, err := f.fs.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		log.Printf("[clone] Failed to read root inode: %v", err)
		return syscall.EIO
	}

	// Check if destination already exists
	if _, _, err := f.fs.dirMgr.Lookup(ctx, rootIno, cloneName); err == nil {
		log.Printf("[clone] Destination already exists: %s", cloneName)
		return syscall.EEXIST
	}

	srcIno, err := f.fs.inodeTbl.Read(srcNum)
	if err != nil {
		log.Printf("[clone] Failed to read source inode %d: %v", srcNum, err)
		return syscall.EIO
	}

	// Find free inode slot for clone (two-phase allocation)
	cloneInodeNum, err := f.fs.inodeTbl.FindFreeSlot()
	if err != nil {
		log.Printf("[clone] No free inode slots: %v", err)
		return syscall.ENOSPC
	}

	// Add directory entry for clone
	if err := f.fs.dirMgr.Add(ctx, rootIno, cloneName, uint64(cloneInodeNum), dir.DT_REG); err != nil {
		log.Printf("[clone] Failed to add directory entry: %v", err)
		return syscall.EIO
	}

	// Claim the inode slot
	cloneIno, err := f.fs.inodeTbl.ClaimSlot(ctx, cloneInodeNum)
	if err != nil {
		f.fs.dirMgr.Remove(ctx, rootIno, cloneName)
		if err == inode.ErrInodeInUse {
			log.Printf("[clone] Inode slot collision, retry needed")
			return syscall.EAGAIN
		}
		log.Printf("[clone] Failed to claim inode slot: %v", err)
		return syscall.ENOSPC
	}

	now := time.Now().UnixNano()
	cloneIno.Mode = srcIno.Mode
	cloneIno.UID = srcIno.UID
	cloneIno.GID = srcIno.GID
	cloneIno.LinkCount = 1
	cloneIno.Flags |= inode.FlagHasSnapshot
	cloneIno.Atime = now
	cloneIno.Mtime = now
	cloneIno.Ctime = now

	if err := f.fs.shareInto(ctx, srcNum, cloneIno, stillValid); err != nil {
		f.fs.dirMgr.Remove(ctx, rootIno, cloneName)
		f.fs.discardInode(ctx, cloneInodeNum)
		log.Printf("[clone] Failed to share blocks of inode %d: %v", srcNum, err)
		return sharingErrno(err)
	}

	log.Printf("[clone] Created '%s' (inode %d) from inode %d", cloneName, cloneInodeNum, srcNum)
	return 0
}

// createCloneFromSnapshot creates a writable clone from a named snapshot.
// Format: "snapshot_name:/path/to/clone"
func (f *File) createCloneFromSnapshot(ctx context.Context, data string) syscall.Errno {
	parts := strings.SplitN(data, ":", 2)
	if len(parts) != 2 {
		log.Printf("[clone] Invalid snapshot clone format: %s", data)
		return syscall.EINVAL
	}
	snapName, destPath := parts[0], parts[1]

	log.Printf("[clone] Creating clone from snapshot '%s' at %s", snapName, destPath)

	if f.fs.snapshotReg == nil {
		return syscall.ENOTSUP
	}

	desc, err := f.fs.snapshotReg.GetByName(ctx, snapName)
	if err != nil {
		if errors.Is(err, snapshot.ErrSnapshotNotFound) {
			return syscall.ENOENT
		}
		return syscall.EIO
	}

	if desc.TargetInode != f.inodeNum {
		log.Printf("[clone] Snapshot '%s' belongs to inode %d, not %d", snapName, desc.TargetInode, f.inodeNum)
		return syscall.EINVAL
	}
	if desc.State != snapshot.StateActive {
		return syscall.EBUSY
	}
	if !desc.HasData() {
		log.Printf("[clone] Snapshot '%s' was made by an older version and holds no data", snapName)
		return syscall.ENOTSUP
	}

	// A delete that starts before the blocks gain their new owner may free them.
	stillActive := func() error {
		d, err := f.fs.snapshotReg.Get(ctx, desc.SnapshotID)
		if err == nil && d.State != snapshot.StateActive {
			err = snapshot.ErrSnapshotInvalidState
		}
		return err
	}
	return f.cloneInode(ctx, desc.DataInode, destPath, stillActive)
}

// listSnapshots returns snapshot names for this file.
func (f *File) listSnapshots(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	if f.fs.snapshotReg == nil {
		return 0, syscall.ENOTSUP
	}

	snapshots, err := f.fs.snapshotReg.ListForInode(ctx, f.inodeNum)
	if err != nil {
		return 0, syscall.EIO
	}

	var result string
	for i, snap := range snapshots {
		if i > 0 {
			result += "\n"
		}
		result += fmt.Sprintf("%s:%s", snap.GetName(), snap.State.String())
	}

	if len(dest) == 0 {
		return uint32(len(result)), 0
	}

	n := copy(dest, result)
	return uint32(n), 0
}

// blockExtent returns the single block of ext at offset, so per-block checks
// such as refcounts look at the right physical block.
func blockExtent(ext *extent.Extent, offset uint64) *extent.Extent {
	return &extent.Extent{
		LogicalBlock:  ext.LogicalBlock + offset,
		PhysicalBlock: ext.PhysicalBlock + offset,
		Length:        1,
		Flags:         ext.Flags,
	}
}

// parseClonePath splits an absolute path into parent directory path and filename.
// Returns (parentPath, filename, error). For paths in root directory, parentPath is "/".
func parseClonePath(path string) (string, string, error) {
	if !strings.HasPrefix(path, "/") {
		return "", "", fmt.Errorf("path must be absolute")
	}
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", "", fmt.Errorf("path cannot be empty")
	}
	lastSlash := strings.LastIndex(path, "/")
	if lastSlash == -1 {
		return "/", path, nil // File in root directory
	}
	return "/" + path[:lastSlash], path[lastSlash+1:], nil
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
