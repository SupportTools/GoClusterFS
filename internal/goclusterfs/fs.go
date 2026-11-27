// Package goclusterfs implements a FUSE filesystem using GoClusterFS structures.
//
// This package integrates all Phase 2 components (bitmap, inode, extent, dir)
// into a working FUSE filesystem that can be mounted and used.
//
// Uses hanwen/go-fuse v2 for proper FUSE capability negotiation (including fallocate).
package goclusterfs

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/supporttools/GoClusterFS/internal/logging"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/quota"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/reserve"
	"github.com/supporttools/GoClusterFS/internal/sfb"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
	"github.com/supporttools/GoClusterFS/internal/trim"
)

// errRetryWrite is an internal sentinel error indicating that a concurrent
// modification was detected and the Write operation should be retried.
var errRetryWrite = errors.New("retry write operation")

// errGrowsFile means the file is now shorter than a write that planned to stay inside it.
var errGrowsFile = errors.New("the write now grows the file")

// beforeCoWCommit lets tests run another node's write after this write mapped
// its copy of a shared block, before it takes the inode lock to copy the data
// and commit it.
var beforeCoWCommit func()

// beforeInPlaceWrite lets tests run another node's snapshot after a write
// decided to write a block in place, before it writes the data.
var beforeInPlaceWrite func()

// duringInPlaceWrite lets tests act on other nodes while a write holds the
// locks for an in-place write, before its data lands.
var duringInPlaceWrite func()

// afterSectorRead lets tests act while a partial-sector write holds the
// sector's old contents, before it writes them back.
var afterSectorRead func()

// beforeSizeUpdate lets tests hold a write after its data landed, before it
// takes the inode lock to update size and mtime.
var beforeSizeUpdate func(*File)

// beforeShareMark lets tests change a file after a snapshot or clone counted
// its blocks, before it marks the file shared.
var beforeShareMark func()

// beforeActivate lets tests act after a snapshot shared the file's blocks,
// before it becomes active.
var beforeActivate func()

// duringShareMark lets tests act on other nodes after the mark read the
// file's inode, before it writes the inode back.
var duringShareMark func()

// MaxWrite is the largest write request the kernel sends the daemon; a read or snapshot sees each request whole.
const MaxWrite = 1 << 20

// MaxWriteRetries is the maximum number of retry attempts for Write operations
// when concurrent modifications are detected. Back-to-back snapshots on another
// node can force a retry each time; with conflictBackoff, 10 tries span up to 5s.
const MaxWriteRetries = 10

// errRetryCreate is an internal sentinel error indicating that an inode slot
// collision was detected during Create/Mkdir and the operation should be retried.
var errRetryCreate = errors.New("retry create: inode slot collision")

// MaxCreateRetries bounds how often retryRace runs a remove, a rename, or a create that met another node's name, after
// losing a race with another node. A lost inode slot is retried by untilClaimed instead.
const MaxCreateRetries = 5

// maxConflictRetries bounds how often an inode update is redone after another
// node changed the same inode's extent tree first.
const maxConflictRetries = 5

func conflictBackoff(attempt int) {
	time.Sleep(time.Duration(1+rand.Intn(5<<attempt)) * time.Millisecond)
}

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

	// mounted is set by MarkMounted once a FUSE server serves the root, so kernel notifications have somewhere to go.
	mounted atomic.Bool

	// badEntries holds the damaged directory entries childInRange has logged, at most maxBadEntries of them.
	badEntriesMu sync.Mutex
	badEntries   map[badEntry]bool

	// Storage layer
	dev *storage.Device
	sb  *superblock.Superblock
	// releaseSlot empties this node's registry slot at a clean close.
	releaseSlot func()

	// Managers
	lockMgr        *lock.Manager
	flocks         *flockTable
	bitmapMgr      *bitmap.Region
	inodeTbl       *inode.Table
	extentMgr      *extent.Manager
	dirMgr         *dir.Manager
	cacheMgr       *cache.Manager     // Inode cache for read optimization
	journal        *journal.Journal   // Per-node journal for crash consistency (WAL)
	reserve        *reserve.Manager   // this node's batch of data blocks; nil without a journal
	symlinks       bool               // the volume was formatted with RequiredSymlinks
	noSymlinks     sync.Once          // warns once that this volume cannot hold symlinks
	trimMgr        *trim.Manager      // TRIM manager for batched block discards
	sfbMgr         *sfb.Manager       // SFB manager for small file storage (optional)
	refcountReg    *refcount.Region   // Reference count table for CoW/snapshots (optional)
	snapshotReg    *snapshot.Registry // Snapshot registry for managing snapshot descriptors
	snapshotWkr    *snapshot.Worker   // Background worker for async refcount updates
	snapshotsUp    atomic.Bool        // snapshotReg and snapshotWkr are set, for the failure detector's goroutine
	quotaInode     uint32             // holds the quota table; 0 when the volume has no quotas
	quotaBlockSize uint64             // the volume's block size, in which quota usage counts
	quotaGuard     *quota.Guard
	quotaStop      chan struct{}
	quotaDone      chan struct{}
	quotaStuck     atomic.Bool // a flush failed after its commit; the journal keeps it for a replay
	openQuota      func() (quota.Table, error)
	quotaSeenMu    sync.Mutex
	quotaSeen      map[uint32]seenRecord // project records as this node last read them, for enforcement
	live           int64
	liveAt         time.Time
	liveCount      func() int64 // set by tests in place of the registry
	stopNode       func(reason string)

	// Configuration
	nodeUUID   [16]byte
	dataOffset uint64 // Offset where data blocks start on disk
	blockSize  uint64
	failStop   func(error)

	// Runtime state
	uid uint32
	gid uint32

	inodeMu [inodeMuStripes]sync.Mutex
	ioMu    [inodeMuStripes]sync.RWMutex

	// openFlags are the FOPEN flags every open and create of a file returns.
	openFlags uint32

	orphanReg orphanRegistry
	opens     openFiles

	dirGen atomic.Uint64
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
	_ fs.NodeSetattrer = (*FS)(nil)
	_ fs.NodeFsyncer   = (*FS)(nil)
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
	BlockSize     uint64 // Data block size

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
	if cfg.BlockSize == 0 {
		cfg.BlockSize = storage.DefaultBlockSize
	}
	extentMgr := extent.NewManager(dev, bitmapMgr, lockMgr, cfg.DataOffset, cfg.BlockSize)
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
		blockSize:  cfg.BlockSize,
		uid:        uint32(os.Getuid()),
		gid:        uint32(os.Getgid()),
		flocks:     newFlockTable(clusterOf(lockMgr)),
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
		blockSize:   extentMgr.BlockSize(),
		uid:         uint32(os.Getuid()),
		gid:         uint32(os.Getgid()),
		flocks:      newFlockTable(clusterOf(lockMgr)),
	}
}

// BlockSize returns the volume's data block size in bytes.
func (f *FS) BlockSize() uint64 {
	return f.blockSize
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
	f.snapshotReg.SetOwner(f.nodeUUID)

	// Scan registry to rebuild state
	stats, err := f.snapshotReg.Scan(ctx)
	if err != nil {
		return fmt.Errorf("failed to scan snapshot registry: %w", err)
	}
	log.Printf("[snapshot] Registry scan: %d active, %d creating, %d deleting",
		stats.ActiveCount, stats.CreatingCount, stats.DeletingCount)
	if stats.Unreadable != nil {
		log.Printf("[snapshot] WARNING: %v. Snapshot creates, lookups and listings fail with EIO until it reads again; run goclusterfs-fsck", stats.Unreadable)
	}

	// Create and start background worker
	cfg := snapshot.DefaultWorkerConfig()
	f.snapshotWkr = snapshot.NewWorker(f.snapshotReg, f.extentMgr, f.inodeTbl, cfg)
	f.chargeSnapshots()
	f.snapshotWkr.Start()
	f.snapshotsUp.Store(true)

	// Recover any incomplete jobs from crash
	if err := f.snapshotWkr.RecoverIncompleteJobs(ctx, f.creatorGone); err != nil {
		log.Printf("[snapshot] Warning: failed to recover incomplete jobs: %v", err)
		// Continue anyway - snapshots may be in inconsistent state but won't crash
	}

	return nil
}

// creatorGone reports whether the node creating a snapshot can no longer
// finish it: its registry slot is empty or another node holds it. A dead
// creator keeps its slot until its replay, which rolls back its snapshots.
func (f *FS) creatorGone(c snapshot.Creator) bool {
	if f.orphanReg == nil {
		return true
	}
	owner, occupancy := f.orphanReg.Occupant(int(c.Slot), true)
	switch {
	case occupancy == node.Unoccupied:
		return true
	case owner == [16]byte{}:
		return false // Occupant names no owner for a slot it cannot read
	}
	return !c.Is(owner)
}

// RollBackSnapshotsOf rolls back the snapshots that the dead node with UUID
// owner in registry slot slot was still creating. Call it after its replay.
func (f *FS) RollBackSnapshotsOf(ctx context.Context, slot int, owner [16]byte) {
	if f.snapshotsUp.Load() {
		f.snapshotWkr.RollBackCreatedBy(ctx, slot, owner)
	}
}

// SetDirectIO makes the kernel send every read and write of file data to this
// filesystem instead of serving or keeping it in its page cache.
func (f *FS) SetDirectIO(on bool) {
	f.openFlags = 0
	if on {
		f.openFlags = fuse.FOPEN_DIRECT_IO
	}
}

// SetReserve makes writes take data blocks from r. It needs the journal.
func (f *FS) SetReserve(r *reserve.Manager) {
	if f.journal != nil {
		f.reserve = r
	}
}

// SetFailStop sets what stops this node when an operation cannot finish and must not let go of its locks.
func (f *FS) SetFailStop(fn func(error)) {
	f.failStop = fn
}

// Close closes the filesystem and releases resources.
func (f *FS) Close() error {
	// Stop snapshot worker first to complete pending jobs
	if f.snapshotWkr != nil {
		ShutdownStep("snapshot worker stop", f.snapshotWkr.Stop)
	}

	// Unused reserved blocks go back before TRIM and the journal stop.
	if f.reserve != nil {
		ShutdownStep("block reserve return", func() { f.reserve.Stop(context.Background()) })
	}
	if f.extentMgr != nil {
		ShutdownStep("waiting block frees", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if left := f.extentMgr.FlushDeferred(ctx); left > 0 {
				log.Printf("[WARN] %d freed blocks stay allocated: their frees kept waiting for a lock. goclusterfs-fsck -repair gives them back", left)
			}
		})
	}

	// Stop TRIM manager to flush pending discards
	if f.trimMgr != nil {
		ShutdownStep("TRIM stop", func() {
			if err := f.trimMgr.Stop(); err != nil {
				log.Printf("Warning: failed to stop TRIM manager cleanly: %v", err)
			}
		})
	}

	ShutdownStep("quota flush", f.stopQuotaFlush)

	// Stop journal to ensure clean shutdown
	journalClean := true
	if f.journal != nil {
		ShutdownStep("journal stop", func() {
			if err := f.journal.Stop(); err != nil {
				journalClean = false
				log.Printf("Warning: failed to stop journal cleanly: %v", err)
			}
		})
	}
	// Other nodes would otherwise wait for these holds to go stale.
	if f.lockMgr != nil {
		ShutdownStep("lock release", func() {
			f.lockMgr.Stop()
			f.lockMgr.ReleaseCached()
		})
		f.lockMgr.LogStats()
	}
	if f.releaseSlot != nil && journalClean {
		ShutdownStep("registry slot release", f.releaseSlot)
	}
	var err error
	ShutdownStep("device close", func() { err = f.dev.Close() })
	return err
}

// SetSlotRelease makes Close run release after the journal stopped cleanly and before the device closes.
func (f *FS) SetSlotRelease(release func()) {
	f.releaseSlot = release
}

// getInode retrieves an inode, using the cache when available.
// This is the preferred method for read paths where the inode won't be modified.
func (f *FS) getInode(inodeNum uint32) (*inode.Inode, error) {
	if f.cacheMgr != nil {
		return f.cacheMgr.GetInode(inodeNum)
	}
	return f.inodeTbl.Read(inodeNum)
}

// detach keeps an operation running when its FUSE request is interrupted. The
// kernel interrupts a request whenever the calling process gets a signal, and Go
// programs signal themselves for preemption all the time, so ending lock waits
// there turned ordinary creates and writes into EIO.
func detach(ctx context.Context) context.Context {
	return context.WithoutCancel(ctx)
}

// lockInode serializes this node's read-modify-write of one inode. Call it
// before taking any cluster lock and hold one at a time: inodes share stripes.
func (f *FS) lockInode(num uint32) func() {
	mu := &f.inodeMu[num%inodeMuStripes]
	mu.Lock()
	return mu.Unlock
}

// lockFileIO keeps a file's data still for one whole read (shared) or changes
// it alone (exclusive): against this node's requests through a local stripe,
// against other nodes' through the cluster file I/O lock. Take it before
// lockInode and any other lock, and hold one at a time.
func (f *FS) lockFileIO(ctx context.Context, num uint32, mode lock.LockMode) (func(), error) {
	mu := &f.ioMu[num%inodeMuStripes]
	unlockLocal := mu.RUnlock
	if mode == lock.LockModeExclusive {
		mu.Lock()
		unlockLocal = mu.Unlock
	} else {
		mu.RLock()
	}
	if f.lockMgr == nil {
		return unlockLocal, nil
	}
	owner := lock.NewOwner()
	if err := f.lockMgr.AcquireAs(ctx, owner, lock.LockTypeFileIO, uint64(num), mode); err != nil {
		unlockLocal()
		return nil, err
	}
	return func() {
		if err := f.lockMgr.ReleaseAs(owner, lock.LockTypeFileIO, uint64(num)); err != nil {
			log.Printf("[WARN] releasing the file I/O lock on inode %d failed: %v", num, err)
		}
		unlockLocal()
	}, nil
}

// release releases a cluster lock and logs a failure; kind names the resource, such as "inode".
func (f *FS) release(lockType lock.LockType, id uint64, kind string) {
	err := f.lockMgr.Release(lockType, id)
	switch {
	case errors.Is(err, lock.ErrNotLockOwner):
		log.Printf("[ERROR] released the lock on %s %d that this node does not hold (double release or lock bookkeeping bug)", kind, id)
	case err != nil:
		log.Printf("[WARN] releasing the lock on %s %d failed: %v", kind, id, err)
	}
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

// updateInodeLocked is updateInode for changes that allocate and free no
// blocks, such as times, link counts and extent flags. It holds the inode's
// lock from the read to the write, so it cannot write back an inode that
// another node changed in between.
func (f *FS) updateInodeLocked(ctx context.Context, num uint32, update func(*inode.Inode) error) (*inode.Inode, error) {
	defer f.lockInode(num)()
	return f.updateUnderInodeLock(ctx, num, update)
}

// updateUnderInodeLock is updateInodeLocked for a caller that already holds
// lockInode(num).
func (f *FS) updateUnderInodeLock(ctx context.Context, num uint32, update func(*inode.Inode) error) (*inode.Inode, error) {
	var ino *inode.Inode
	err := f.untilReplayed(ctx, func() error {
		var err error
		ino, err = f.updateUnderInodeLockOnce(ctx, num, update)
		return err
	})
	return ino, err
}

// untilReplayed runs op, which takes its own locks, again whenever it gives up on a lock a failing node holds, once
// that node's replay ends. See lock.Manager.UntilReplayed.
func (f *FS) untilReplayed(ctx context.Context, op func() error) error {
	if f.lockMgr == nil {
		return op()
	}
	return f.lockMgr.UntilReplayed(ctx, op)
}

func (f *FS) updateUnderInodeLockOnce(ctx context.Context, num uint32, update func(*inode.Inode) error) (*inode.Inode, error) {
	if f.lockMgr != nil {
		if err := f.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(num), lock.LockModeExclusive); err != nil {
			return nil, fmt.Errorf("failed to acquire inode lock: %w", err)
		}
		defer f.release(lock.LockTypeInode, uint64(num), "inode")
	}

	ino, err := f.inodeTbl.Read(num)
	if err != nil {
		return nil, err
	}
	if err := update(ino); err != nil {
		return nil, err
	}
	if err := f.inodeTbl.WriteWithLockHeld(ino); err != nil {
		return nil, err
	}
	return ino, nil
}

// updateDirLocked is updateInodeLocked for directory num, under the directory lock that keeps this node's entry changes out, so its index can follow the write.
func (f *FS) updateDirLocked(ctx context.Context, num uint32, update func(*inode.Inode) error) (*inode.Inode, error) {
	defer f.dirMgr.LockDir(num)()
	var readGen uint64
	ino, err := f.updateUnderInodeLock(ctx, num, func(ino *inode.Inode) error {
		readGen = ino.MetaGeneration
		return update(ino)
	})
	if err != nil {
		return nil, err
	}
	f.dirMgr.KeepIndex(num, readGen, ino.MetaGeneration)
	return ino, nil
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

// SnapshotRegistry returns the snapshot registry for API endpoints.
// Returns nil if snapshots are not enabled.
// Thread-safe: the returned Registry has its own internal mutex.
func (f *FS) SnapshotRegistry() *snapshot.Registry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.snapshotReg
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
	if f.reserve != nil {
		freeBlocks = min(totalBlocks, freeBlocks+uint64(f.reserve.Stats().Blocks))
	}

	out.Blocks = totalBlocks
	out.Bfree = freeBlocks
	out.Bavail = freeBlocks
	out.Files = f.inodeTbl.TotalInodes()
	out.Ffree = f.inodeTbl.FreeCount()
	out.Bsize = uint32(f.blockSize)
	out.NameLen = 255
	out.Frsize = uint32(f.blockSize)

	return 0
}

// Getattr returns the root directory attributes.
func (f *FS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	ino, err := f.getInode(inode.InodeRoot)
	if err != nil {
		return syscall.EIO
	}

	// Zero means go-fuse applies the mount's -attr-timeout.
	out.AttrValid = 0

	out.Ino = uint64(ino.InodeNum)
	out.Mode = uint32(fuse.S_IFDIR | (ino.Mode & 0o7777))
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
	out.Blocks = ino.AllocatedBlocks * (f.blockSize / 512)
	out.Blksize = uint32(f.blockSize)

	return 0
}

// Lookup finds a child of the root directory by name.
func (f *FS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	ctx = detach(ctx)
	logging.Debugf("FS.Lookup: name=%q", name)

	dirIno, err := f.getInode(inode.InodeRoot)
	if err != nil {
		logging.Debugf("FS.Lookup: failed to get root inode: %v", err)
		return nil, syscall.EIO
	}

	childInodeNum, fileType, err := f.dirMgr.Lookup(ctx, dirIno, name)
	if err != nil {
		if err == dir.ErrNotFound {
			logging.Debugf("FS.Lookup: %q not found", name)
			return nil, syscall.ENOENT
		}
		logging.Debugf("FS.Lookup: error: %v", err)
		return nil, syscall.EIO
	}
	logging.Debugf("FS.Lookup: found %q -> inode %d, type %d", name, childInodeNum, fileType)
	if errno := f.childInRange(inode.InodeRoot, name, childInodeNum); errno != 0 {
		return nil, errno
	}

	// Read the child inode to populate attributes
	childIno, err := f.inodeTbl.Read(uint32(childInodeNum))
	if err != nil {
		logging.Debugf("FS.Lookup: failed to read child inode %d: %v", childInodeNum, err)
		return nil, syscall.EIO
	}

	// Zero means go-fuse applies the mount's -attr-timeout.
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

	if childIno.IsSymlink() {
		return f.linkNode(ctx, &f.Inode, childIno, out), 0
	}
	if fileType == dir.DT_DIR {
		out.Attr.Mode = uint32(fuse.S_IFDIR | (childIno.Mode & 0o7777))
		out.Attr.Size = f.blockSize
		out.Attr.Blocks = 1 * (f.blockSize / 512)
		out.Attr.Blksize = uint32(f.blockSize)

		ch := f.dirNode(ctx, &f.Inode, name, childInodeNum)
		logging.Debugf("FS.Lookup: returning dir inode %d", childInodeNum)
		return ch, 0
	}

	out.Attr.Mode = uint32(fuse.S_IFREG | (childIno.Mode & 0o7777))
	out.Attr.Size = childIno.Size
	out.Attr.Blocks = childIno.AllocatedBlocks * (f.blockSize / 512)
	out.Attr.Blksize = uint32(f.blockSize)

	ch := f.NewInode(ctx, newFile(f, childIno), fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  childInodeNum,
		Gen:  childIno.BirthGeneration,
	})
	logging.Debugf("FS.Lookup: returning file inode %d, size=%d", childInodeNum, childIno.Size)
	return ch, 0
}

// Readdir returns a DirStream for the root directory listing.
func (f *FS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	ctx = detach(ctx)
	logging.Debugf("FS.Readdir: root directory")
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
func (f *FS) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return f.createFile(detach(ctx), &f.Inode, inode.InodeRoot, name, flags, mode, out)
}

// allocErrno maps a failure to allocate data or extent-tree blocks. Only an
// empty bitmap or full backing storage means no space; a lock wait that gave
// up or another I/O error does not.
func allocErrno(err error) syscall.Errno {
	if errors.Is(err, bitmap.ErrNoFreeBlocks) || errors.Is(err, syscall.ENOSPC) {
		return syscall.ENOSPC
	}
	return syscall.EIO
}

// claimErrno maps a failure to find or claim an inode slot. Only a slot another
// node took first is worth a retry; a lock wait that gave up or an I/O error is
// not a full inode table.
func claimErrno(err error) syscall.Errno {
	switch {
	case errors.Is(err, inode.ErrInodeFull):
		return syscall.ENOSPC
	case errors.Is(err, inode.ErrInodeInUse):
		return syscall.EAGAIN
	}
	return syscall.EIO
}

// commitWhenRoom commits txn once the journal has room for it, waiting while open intents fill the ring. The caller
// holds no lock and no reserve block, so its wait cannot keep an open intent from completing.
func (f *FS) commitWhenRoom(ctx context.Context, txn *journal.Transaction) (uint64, error) {
	a, err := f.journal.Admit(ctx, txn.TotalLength())
	if err != nil {
		f.journal.Abort(txn)
		return 0, err
	}
	return f.commitAdmitted(a, txn)
}

// commitAdmitted commits txn into the room a set aside, and drops both if the commit fails.
func (f *FS) commitAdmitted(a *journal.Admission, txn *journal.Transaction) (uint64, error) {
	seq, err := f.journal.CommitAdmitted(a, txn)
	if err != nil {
		f.journal.Abort(txn)
		f.journal.Release(a)
	}
	return seq, err
}

type opRoomKey struct{}

// admitOp waits until the journal has room for one record of up to each of sizes bytes, and returns ctx carrying it
// for commit, with a release for the records the operation did not commit. Room frees only as open intents complete,
// so call it holding no lock and no reserve block: on entry, before the operation takes any.
func (f *FS) admitOp(ctx context.Context, sizes ...uint32) (context.Context, func()) {
	if f.journal == nil {
		return ctx, func() {}
	}
	a, err := f.journal.AdmitRecords(ctx, sizes...)
	if err != nil {
		return ctx, func() {}
	}
	return context.WithValue(ctx, opRoomKey{}, a), func() { f.journal.Release(a) }
}

// commit commits txn into room its operation set aside on entry, or without waiting once it has none left.
func (f *FS) commit(ctx context.Context, txn *journal.Transaction) (uint64, error) {
	a, _ := ctx.Value(opRoomKey{}).(*journal.Admission)
	return f.journal.CommitAdmitted(a, txn)
}

// writeRecordBounds bounds the records a write of n bytes commits: its main record, with an extent per block it may
// map, and the records of two allocation rounds, each naming the reserve blocks it used.
func (f *FS) writeRecordBounds(n int) []uint32 {
	blocks := uint32(uint64(n)/f.blockSize) + 2
	return []uint32{1024 + 64*blocks, 512 + 12*blocks, 512 + 12*blocks}
}

// closeIntent completes an intent its operation left open by returning early, so a failed operation is never replayed.
func (f *FS) closeIntent(seq uint64) {
	if err := f.journal.CompleteIntent(seq); err != nil {
		log.Printf("[WARN] journal: completing intent %d failed: %v", seq, err)
	}
}

// unmapped returns the blocks the inode on disk does not map: a failed inode write may still have landed.
func (f *FS) unmapped(num uint32, blocks []uint64) []uint64 {
	ino, err := f.inodeTbl.Read(num)
	if err != nil {
		log.Printf("[WARN] inode %d: not freeing %d unused blocks, inode unreadable: %v", num, len(blocks), err)
		return nil
	}
	mapped, err := f.extentMgr.DataBlocks(ino)
	if err != nil {
		log.Printf("[WARN] inode %d: not freeing %d unused blocks, extents unreadable: %v", num, len(blocks), err)
		return nil
	}
	inUse := make(map[uint64]bool, len(mapped))
	for _, b := range mapped {
		inUse[b] = true
	}
	var unused []uint64
	for _, b := range blocks {
		if !inUse[b] {
			unused = append(unused, b)
		}
	}
	return unused
}

func (f *FS) freeUnused(ctx context.Context, num uint32, blocks ...uint64) {
	for _, b := range blocks {
		if err := f.extentMgr.FreePhysicalBlock(ctx, b); err != nil {
			log.Printf("[WARN] inode %d: freeing unused block %d failed: %v", num, b, err)
		}
	}
}

// dropNewInode undoes a new directory inode whose setup failed: it frees the inode, then the blocks it no longer maps.
func (f *FS) dropNewInode(ctx context.Context, ino *inode.Inode) {
	blocks, err := f.extentMgr.DataBlocks(ino)
	if err != nil {
		log.Printf("[WARN] inode %d: blocks of the failed new inode unknown, they may leak: %v", ino.InodeNum, err)
	}
	if !f.freeNewInode(ctx, ino.InodeNum) {
		return
	}
	f.freeUnused(ctx, ino.InodeNum, f.unmapped(ino.InodeNum, blocks)...)
}

// freeNewInode frees an inode that a failed operation claimed before anything referenced it.
func (f *FS) freeNewInode(ctx context.Context, num uint32) bool {
	if err := f.inodeTbl.Free(ctx, num); err != nil {
		log.Printf("[WARN] inode %d: freeing the failed new inode failed: %v", num, err)
		return false
	}
	return true
}

// testHookBeforeMkdirInodeWrite, when set by a test, fails a mkdir's inode write with its error.
var testHookBeforeMkdirInodeWrite func() error

// testHookAfterSetattrRead, when set by a test, runs between a setattr's read of the inode and its write.
var testHookAfterSetattrRead func()

func injectedMkdirFailure() error {
	if testHookBeforeMkdirInodeWrite != nil {
		return testHookBeforeMkdirInodeWrite()
	}
	return nil
}

// logExtentFree records that the operation cuts ino to newSize and frees its blocks past it, so that replay can
// finish that if the operation's inode write never landed, and takes them off ino's project. It returns how many.
func (f *FS) logExtentFree(txn *journal.Transaction, ino *inode.Inode, newSize uint64) (int64, error) {
	blocks, err := f.blocksPast(ino, newSize)
	if err == nil {
		err = f.chargeQuota(txn, ino.ProjectID, -blocks, 0)
	}
	if err == nil {
		err = txn.LogExtentFree(&journal.ExtentFreePayload{InodeNum: uint64(ino.InodeNum), MetaGeneration: ino.MetaGeneration, Size: newSize})
	}
	return blocks, err
}

// logInodeFree records that the operation frees inode ino, at MetaGeneration gen, with its blocks, and takes them and
// the inode off its project. It returns how many blocks.
func (f *FS) logInodeFree(txn *journal.Transaction, ino *inode.Inode, gen uint64) (int64, error) {
	blocks, err := f.logExtentFree(txn, ino, 0)
	if err == nil {
		err = f.chargeQuota(txn, ino.ProjectID, 0, -1)
	}
	if err == nil {
		err = txn.LogInodeDelete(&journal.InodeDeletePayload{InodeNum: uint64(ino.InodeNum), MetaGeneration: gen})
	}
	return blocks, err
}

// blocksPast counts the blocks ino maps at or past size, which cutting it to size frees.
func (f *FS) blocksPast(ino *inode.Inode, size uint64) (int64, error) {
	if size == 0 || f.quotaInode == 0 || ino.ProjectID == 0 {
		return int64(ino.AllocatedBlocks), nil
	}
	exts, err := f.extentMgr.ListExtents(ino)
	if err != nil {
		return 0, err
	}
	first := f.extentMgr.BlocksForSize(size)
	var n uint64
	for _, x := range exts {
		if end := x.LogicalBlock + x.Length; end > first {
			n += end - max(x.LogicalBlock, first)
		}
	}
	return int64(min(n, ino.AllocatedBlocks)), nil
}

// freeSlotGeneration returns the MetaGeneration of a free inode slot. A create logs it so that replay can tell whether
// another node used the slot since; a slot it cannot read fails the create's ClaimSlot anyway.
func (f *FS) freeSlotGeneration(num uint32) uint64 {
	if free, err := f.inodeTbl.Read(num); err == nil {
		return free.MetaGeneration
	}
	return 0
}

// afterCommit runs between an operation's journal commit and its first change; tests stop there as a crash would.
var afterCommit func(op string)

// claiming runs just before a create, mkdir or symlink claims free inode num; tests take the inode there as another
// node would.
var claiming func(op string, num uint32)

// retryRace runs try again, after a jittered backoff, while it returns EAGAIN for losing a race with another node.
func retryRace(op, name string, try func() syscall.Errno) syscall.Errno {
	for attempt := 0; attempt < MaxCreateRetries; attempt++ {
		errno := try()
		if errno != syscall.EAGAIN {
			return errno
		}
		baseDelay := time.Millisecond * time.Duration(1<<attempt)
		backoff := baseDelay + time.Duration(rand.Int63n(int64(baseDelay)))
		logging.Debugf("%s %q: lost a race with another node, retry %d/%d (backoff %v)", op, name, attempt+1, MaxCreateRetries, backoff)
		time.Sleep(backoff)
	}
	log.Printf("[WARN] %s %q: lost %d races with other nodes", op, name, MaxCreateRetries)
	return syscall.EAGAIN
}

// untilClaimed runs a create attempt again at once while it returns EAGAIN, which a create, mkdir or symlink attempt
// returns only when another node claimed the free inode it picked. Each loss uses up a free inode, so it ends with a
// slot, or ENOSPC once none is left.
func untilClaimed(op, name string, try func() syscall.Errno) syscall.Errno {
	for lost := 1; ; lost++ {
		errno := try()
		if errno != syscall.EAGAIN {
			return errno
		}
		logging.Debugf("%s %q: another node claimed the free inode it picked (%d so far), trying another", op, name, lost)
	}
}

// nameErrno maps a failure to add, find or remove a directory entry.
func nameErrno(err error) syscall.Errno {
	switch {
	case errors.Is(err, dir.ErrExists):
		return syscall.EEXIST
	case errors.Is(err, dir.ErrNotFound), errors.Is(err, dir.ErrRemoved):
		return syscall.ENOENT
	}
	return syscall.EIO
}

// owner returns the caller's uid and gid, or the mount's when the request has no caller.
func (f *FS) owner(ctx context.Context) (uint32, uint32) {
	if caller, ok := fuse.FromContext(ctx); ok {
		return caller.Uid, caller.Gid
	}
	return f.uid, f.gid
}

// newOwner returns the owner of a file the caller creates in dir: the caller, with dir's group when dir is setgid.
func (f *FS) newOwner(ctx context.Context, dir *inode.Inode) (uid, gid uint32, setgid bool) {
	uid, gid = f.owner(ctx)
	if dir.Mode&inode.S_ISGID != 0 {
		return uid, dir.GID, true
	}
	return uid, gid, false
}

// mayAccess reports whether uid with primary group gid has every permission in want (r=4, w=2, x=1) on ino.
// FUSE does not pass supplementary groups, so they grant nothing here.
func mayAccess(ino *inode.Inode, uid, gid uint32, want uint16) bool {
	shift := 0
	switch {
	case uid == 0:
		return true
	case uid == ino.UID:
		shift = 6
	case gid == ino.GID:
		shift = 3
	}
	return ino.Mode>>shift&want == want
}

// logCreate commits the intent to create inode num with mode and to name it in directory parentNum.
func (f *FS) logCreate(ctx context.Context, num, parentNum uint32, name string, mode, uid, gid, project uint32, entryType journal.DirEntryType) (uint64, error) {
	txn, err := f.createTxn(num, parentNum, name, mode, uid, gid, project, entryType, nil)
	if err != nil {
		return 0, err
	}
	return f.commitWhenRoom(ctx, txn)
}

// createBlocks is how many blocks a create maps: a directory's first block, or a long symlink target's.
func createBlocks(entryType journal.DirEntryType, link *journal.SymlinkTargetPayload) int64 {
	if entryType == journal.DirEntryDirectory || link != nil && link.DiskBlock != 0 {
		return 1
	}
	return 0
}

// createTxn builds the transaction that creates inode num, with link's target when it is a symlink, and names it.
func (f *FS) createTxn(num, parentNum uint32, name string, mode, uid, gid, project uint32, entryType journal.DirEntryType, link *journal.SymlinkTargetPayload) (*journal.Transaction, error) {
	txn := f.journal.Begin()
	err := txn.LogInodeCreate(&journal.InodeCreatePayload{
		InodeNum:       uint64(num),
		Mode:           mode,
		UID:            uid,
		GID:            gid,
		ParentInodeNum: uint64(parentNum),
		NameHash:       hashString(name),
		ProjectID:      project,
		MetaGeneration: f.freeSlotGeneration(num),
	})
	if err == nil {
		err = f.chargeQuota(txn, project, createBlocks(entryType, link), 1)
	}
	if err == nil && link != nil {
		err = txn.LogSymlinkTarget(link)
	}
	if err == nil {
		err = txn.LogDirAddEntry(&journal.DirAddEntryPayload{
			ParentInodeNum: uint64(parentNum),
			ChildInodeNum:  uint64(num),
			EntryType:      entryType,
			Name:           name,
		})
	}
	if err != nil {
		f.journal.Abort(txn)
		return nil, err
	}
	return txn, nil
}

// announceDir caches directory num as it is on disk and tells other nodes it changed.
func (f *FS) announceDir(num uint32) {
	dirIno, err := f.inodeTbl.Read(num)
	if err != nil {
		log.Printf("[WARN] directory inode %d: not refreshed: %v", num, err)
		return
	}
	if f.cacheMgr != nil {
		f.cacheMgr.BroadcastInvalidateInode(num, dirIno.DataGeneration, dirIno.MetaGeneration)
	}
	f.cacheInode(dirIno)
}

func fillEntryOut(out *fuse.EntryOut, ino *inode.Inode, blockSize uint64) {
	out.EntryValid = 0
	out.AttrValid = 0
	out.Ino = uint64(ino.InodeNum)
	out.Mode = uint32(ino.Mode)
	out.Nlink = uint32(ino.LinkCount)
	out.Uid = ino.UID
	out.Gid = ino.GID
	out.Size = ino.Size
	out.Atime, out.Atimensec = uint64(ino.Atime/1e9), uint32(ino.Atime%1e9)
	out.Mtime, out.Mtimensec = uint64(ino.Mtime/1e9), uint32(ino.Mtime%1e9)
	out.Ctime, out.Ctimensec = uint64(ino.Ctime/1e9), uint32(ino.Ctime%1e9)
	out.Blksize = uint32(blockSize)
}

// createFile creates a file called name in directory parentNum, whose go-fuse node is parent.
func (f *FS) createFile(ctx context.Context, parent *fs.Inode, parentNum uint32, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	var node *fs.Inode
	errno := retryRace("Create", name, func() (errno syscall.Errno) {
		errno = untilClaimed("Create", name, func() (errno syscall.Errno) {
			node, errno = f.createFileOnce(ctx, parent, parentNum, name, mode, out)
			return errno
		})
		if errno == syscall.EEXIST && flags&syscall.O_EXCL == 0 {
			node, errno = f.openExisting(ctx, parent, name, flags, out)
		}
		return errno
	})
	if errno != 0 {
		return nil, nil, 0, errno
	}
	return node, fileHandle{}, f.openFlags, 0
}

// openExisting opens name in parent for a create without O_EXCL that found the
// name taken: the kernel saw no such name, but another node or thread made it since.
func (f *FS) openExisting(ctx context.Context, parent *fs.Inode, name string, flags uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	node, errno := parent.Operations().(fs.NodeLookuper).Lookup(ctx, name, out)
	switch {
	case errno == syscall.ENOENT:
		return nil, syscall.EAGAIN
	case errno != 0:
		return nil, errno
	}
	file, ok := node.Operations().(*File)
	if !ok {
		return nil, syscall.EISDIR
	}
	unlock := f.lockInode(file.inodeNum)
	f.addOpen(file.inodeNum, 1)
	ino, err := f.inodeTbl.Read(file.inodeNum)
	if err != nil || !file.current(ino) || ino.LinkCount == 0 {
		f.addOpen(file.inodeNum, -1)
		unlock()
		if err != nil {
			return nil, syscall.EIO
		}
		return nil, syscall.EAGAIN
	}
	unlock()
	f.cacheInode(ino)
	if flags&syscall.O_TRUNC != 0 && ino.Size > 0 {
		in := &fuse.SetAttrIn{}
		in.Valid, in.Size = fuse.FATTR_SIZE, 0
		var attr fuse.AttrOut
		if errno := file.Setattr(ctx, nil, in, &attr); errno != 0 {
			f.addOpen(file.inodeNum, -1)
			return nil, errno
		}
		out.Attr = attr.Attr
	}
	return node, 0
}

// createFileOnce returns EAGAIN when another node claims the free inode slot it picked first.
func (f *FS) createFileOnce(ctx context.Context, parent *fs.Inode, parentNum uint32, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	inodeNum, err := f.inodeTbl.FindFreeSlot()
	if err != nil {
		logging.Debugf("Create %q: FindFreeSlot failed: %v", name, err)
		return nil, syscall.ENOSPC
	}
	defer f.inodeTbl.Release(inodeNum)
	dirIno, err := f.inodeTbl.Read(parentNum)
	if err != nil {
		logging.Debugf("Create %q: Read parent inode %d failed: %v", name, parentNum, err)
		return nil, syscall.EIO
	}
	uid, gid, _ := f.newOwner(ctx, dirIno)
	project := childProject(dirIno)
	if errno := f.checkQuota(ctx, project, 0, 1, true); errno != 0 {
		return nil, errno
	}

	// takeBack gives back the charge of a create that failed after its commit. It runs after the deferred closeIntent,
	// so replay can no longer finish the create it undoes.
	var takeBack func()
	defer func() {
		if takeBack != nil {
			takeBack()
		}
	}()
	if f.journal != nil {
		seq, err := f.logCreate(ctx, inodeNum, parentNum, name, uint32(inode.S_IFREG)|(mode&0o7777), uid, gid, project, journal.DirEntryRegular)
		if err != nil {
			logging.Debugf("Create %q: journal commit failed: %v", name, err)
			return nil, syscall.EIO
		}
		defer f.closeIntent(seq)
		if afterCommit != nil {
			afterCommit("create")
		}
	}

	// Claiming before naming means a crash in between leaks an unnamed inode, not a name for a free slot.
	uncharge := func() { takeBack = func() { f.commitCharge(project, 0, -1) } }
	if claiming != nil {
		claiming("create", inodeNum)
	}
	childIno, err := f.inodeTbl.ClaimSlot(ctx, inodeNum)
	if err != nil {
		logging.Debugf("Create %q: ClaimSlot %d failed: %v", name, inodeNum, err)
		uncharge()
		return nil, claimErrno(err)
	}
	// Counted before the name appears: a peer may unlink it at once, and this node must hold the file for its caller.
	f.addOpen(childIno.InodeNum, 1)
	now := time.Now().UnixNano()
	childIno.Mode = inode.S_IFREG | uint16(mode&0o7777)
	childIno.UID = uid
	childIno.GID = gid
	joinProject(childIno, project)
	childIno.LinkCount = 1
	childIno.Atime = now
	childIno.Mtime = now
	childIno.Ctime = now
	if err := f.inodeTbl.Write(ctx, childIno); err != nil {
		logging.Debugf("Create %q: Write inode %d failed: %v", name, childIno.InodeNum, err)
		f.addOpen(childIno.InodeNum, -1)
		f.freeNewInode(ctx, childIno.InodeNum)
		uncharge()
		return nil, syscall.EIO
	}
	if afterCommit != nil {
		afterCommit("create: inode claimed")
	}

	if err := f.dirMgr.AddAndUpdate(ctx, dirIno, name, uint64(inodeNum), dir.DT_REG, func(p *inode.Inode) { p.Mtime = now }); err != nil {
		logging.Debugf("Create %q: dirMgr.Add failed: %v", name, err)
		f.addOpen(childIno.InodeNum, -1)
		f.freeNewInode(ctx, childIno.InodeNum)
		uncharge()
		return nil, nameErrno(err)
	}
	if afterCommit != nil {
		afterCommit("create: named")
	}
	f.cacheInode(childIno)
	f.announceDir(parentNum)

	ch := parent.NewInode(ctx, newFile(f, childIno), fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  uint64(childIno.InodeNum),
		Gen:  childIno.BirthGeneration,
	})
	fillEntryOut(out, childIno, f.blockSize)
	return ch, 0
}

// makeDir creates a directory called name in directory parentNum, whose go-fuse node is parent.
func (f *FS) makeDir(ctx context.Context, parent *fs.Inode, parentNum uint32, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var node *fs.Inode
	errno := untilClaimed("Mkdir", name, func() (errno syscall.Errno) {
		node, errno = f.makeDirOnce(ctx, parent, parentNum, name, mode, out)
		return errno
	})
	return node, errno
}

// makeDirOnce returns EAGAIN when another node claims the free inode slot it picked first.
func (f *FS) makeDirOnce(ctx context.Context, parent *fs.Inode, parentNum uint32, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	inodeNum, err := f.inodeTbl.FindFreeSlot()
	if err != nil {
		logging.Debugf("Mkdir %q: FindFreeSlot failed: %v", name, err)
		return nil, syscall.ENOSPC
	}
	defer f.inodeTbl.Release(inodeNum)
	dirIno, err := f.inodeTbl.Read(parentNum)
	if err != nil {
		logging.Debugf("Mkdir %q: Read parent inode %d failed: %v", name, parentNum, err)
		return nil, syscall.EIO
	}
	uid, gid, setgid := f.newOwner(ctx, dirIno)
	perm := uint16(mode & 0o7777)
	if setgid {
		perm |= inode.S_ISGID
	}
	project := childProject(dirIno)
	if errno := f.checkQuota(ctx, project, 1, 1, true); errno != 0 {
		return nil, errno
	}

	// takeBack gives back the charge of a create that failed after its commit. It runs after the deferred closeIntent,
	// so replay can no longer finish the create it undoes.
	var takeBack func()
	defer func() {
		if takeBack != nil {
			takeBack()
		}
	}()
	if f.journal != nil {
		seq, err := f.logCreate(ctx, inodeNum, parentNum, name, uint32(inode.S_IFDIR|perm), uid, gid, project, journal.DirEntryDirectory)
		if err != nil {
			logging.Debugf("Mkdir %q: journal commit failed: %v", name, err)
			return nil, syscall.EIO
		}
		defer f.closeIntent(seq)
		if afterCommit != nil {
			afterCommit("mkdir")
		}
	}

	uncharge := func() { takeBack = func() { f.commitCharge(project, -1, -1) } }
	if claiming != nil {
		claiming("mkdir", inodeNum)
	}
	childIno, err := f.inodeTbl.ClaimSlot(ctx, inodeNum)
	if err != nil {
		logging.Debugf("Mkdir %q: ClaimSlot %d failed: %v", name, inodeNum, err)
		uncharge()
		return nil, claimErrno(err)
	}
	now := time.Now().UnixNano()
	childIno.Mode = inode.S_IFDIR | perm
	childIno.UID = uid
	childIno.GID = gid
	joinProject(childIno, project)
	childIno.LinkCount = 2
	childIno.Atime = now
	childIno.Mtime = now
	childIno.Ctime = now
	if err := f.dirMgr.InitDirectory(ctx, childIno, uint64(parentNum)); err != nil {
		logging.Debugf("Mkdir %q: InitDirectory failed: %v", name, err)
		f.dropNewInode(ctx, childIno)
		uncharge()
		return nil, syscall.EIO
	}
	err = injectedMkdirFailure()
	if err == nil {
		err = f.inodeTbl.Write(ctx, childIno)
	}
	if err != nil {
		logging.Debugf("Mkdir %q: Write inode failed: %v", name, err)
		f.dropNewInode(ctx, childIno)
		uncharge()
		return nil, syscall.EIO
	}
	if afterCommit != nil {
		afterCommit("mkdir: inode claimed")
	}

	if err := f.dirMgr.AddAndUpdate(ctx, dirIno, name, uint64(inodeNum), dir.DT_DIR, func(p *inode.Inode) {
		p.LinkCount++
		p.Mtime = now
	}); err != nil {
		logging.Debugf("Mkdir %q: dirMgr.Add failed: %v", name, err)
		f.dropNewInode(ctx, childIno)
		uncharge()
		return nil, nameErrno(err)
	}
	f.cacheInode(childIno)
	f.announceDir(parentNum)

	ch := parent.NewInode(ctx, &Dir{fs: f, inodeNum: childIno.InodeNum}, fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  uint64(childIno.InodeNum),
		Gen:  f.dirGen.Add(1),
	})
	fillEntryOut(out, childIno, f.blockSize)
	return ch, 0
}

// A new node spares the kernel moving the old dentry of a directory another node moved, which fails inside a rename.
func (f *FS) dirNode(ctx context.Context, parent *fs.Inode, name string, num uint64) *fs.Inode {
	attr := fs.StableAttr{Mode: fuse.S_IFDIR, Ino: num}
	if old := parent.GetChild(name); old != nil && old.IsDir() && old.StableAttr().Ino == num {
		attr.Gen = old.StableAttr().Gen
	} else {
		attr.Gen = f.dirGen.Add(1)
	}
	return parent.NewInode(ctx, &Dir{fs: f, inodeNum: uint32(num)}, attr)
}

// Mkdir creates a new directory in the root directory.
func (f *FS) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return f.makeDir(detach(ctx), &f.Inode, inode.InodeRoot, name, mode, out)
}

// Unlink removes a file from the root directory.
func (f *FS) Unlink(ctx context.Context, name string) syscall.Errno {
	return f.RootDir().Unlink(ctx, name)
}

// Rmdir removes an empty directory from the root directory.
func (f *FS) Rmdir(ctx context.Context, name string) syscall.Errno {
	return f.RootDir().Rmdir(ctx, name)
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
	_ fs.NodeSetattrer = (*Dir)(nil)
	_ fs.NodeFsyncer   = (*Dir)(nil)
)

// Getattr returns directory attributes.
func (d *Dir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	ino, err := d.fs.getInode(d.inodeNum)
	if err != nil {
		return syscall.EIO
	}

	// Zero means go-fuse applies the mount's -attr-timeout.
	out.AttrValid = 0

	out.Ino = uint64(ino.InodeNum)
	out.Mode = uint32(fuse.S_IFDIR | (ino.Mode & 0o7777))
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
	out.Blocks = ino.AllocatedBlocks * (d.fs.blockSize / 512)
	out.Blksize = uint32(d.fs.blockSize)

	return 0
}

// Setattr applies chmod, chown and utimes to the directory.
func (d *Dir) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	ctx = detach(ctx)
	if errno := d.fs.setattrDir(ctx, d.inodeNum, in); errno != 0 {
		return errno
	}
	return d.Getattr(ctx, fh, out)
}

// Setattr applies chmod, chown and utimes to the root directory.
func (f *FS) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	ctx = detach(ctx)
	if errno := f.setattrDir(ctx, inode.InodeRoot, in); errno != 0 {
		return errno
	}
	return f.Getattr(ctx, fh, out)
}

func (f *FS) setattrDir(ctx context.Context, num uint32, in *fuse.SetAttrIn) syscall.Errno {
	if _, ok := in.GetSize(); ok {
		return syscall.EISDIR
	}
	now := time.Now().UnixNano()
	updated, err := f.updateDirLocked(ctx, num, func(ino *inode.Inode) error {
		if testHookAfterSetattrRead != nil {
			testHookAfterSetattrRead()
		}
		if mode, ok := in.GetMode(); ok {
			ino.Mode = inode.S_IFDIR | uint16(mode&0o7777)
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
		return nil
	})
	if err != nil {
		logging.Debugf("Setattr directory inode %d failed: %v", num, err)
		return syscall.EIO
	}
	if f.cacheMgr != nil {
		f.cacheMgr.BroadcastInvalidateInode(num, updated.DataGeneration, updated.MetaGeneration)
	}
	f.cacheInode(updated)
	return 0
}

// Lookup finds a child by name.
func (d *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	ctx = detach(ctx)
	logging.Debugf("Lookup: dir=%d name=%q", d.inodeNum, name)

	dirIno, err := d.fs.getInode(d.inodeNum)
	if err != nil {
		logging.Debugf("Lookup: failed to get dir inode: %v", err)
		return nil, syscall.EIO
	}

	childInodeNum, fileType, err := d.fs.dirMgr.Lookup(ctx, dirIno, name)
	if err != nil {
		if err == dir.ErrNotFound {
			logging.Debugf("Lookup: %q not found", name)
			return nil, syscall.ENOENT
		}
		logging.Debugf("Lookup: error: %v", err)
		return nil, syscall.EIO
	}
	logging.Debugf("Lookup: found %q -> inode %d, type %d", name, childInodeNum, fileType)
	if errno := d.fs.childInRange(d.inodeNum, name, childInodeNum); errno != 0 {
		return nil, errno
	}

	// Read the child inode to populate attributes
	childIno, err := d.fs.inodeTbl.Read(uint32(childInodeNum))
	if err != nil {
		logging.Debugf("Lookup: failed to read child inode %d: %v", childInodeNum, err)
		return nil, syscall.EIO
	}

	// Zero means go-fuse applies the mount's -attr-timeout.
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

	if childIno.IsSymlink() {
		return d.fs.linkNode(ctx, &d.Inode, childIno, out), 0
	}
	if fileType == dir.DT_DIR {
		out.Attr.Mode = uint32(fuse.S_IFDIR | (childIno.Mode & 0o7777))
		out.Attr.Size = d.fs.blockSize // Directories have fixed size
		out.Attr.Blocks = 1 * (d.fs.blockSize / 512)
		out.Attr.Blksize = uint32(d.fs.blockSize)

		ch := d.fs.dirNode(ctx, &d.Inode, name, childInodeNum)
		logging.Debugf("Lookup: returning dir inode %d", childInodeNum)
		return ch, 0
	}

	out.Attr.Mode = uint32(fuse.S_IFREG | (childIno.Mode & 0o7777))
	out.Attr.Size = childIno.Size
	out.Attr.Blocks = childIno.AllocatedBlocks * (d.fs.blockSize / 512)
	out.Attr.Blksize = uint32(d.fs.blockSize)

	ch := d.NewInode(ctx, newFile(d.fs, childIno), fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  childInodeNum,
		Gen:  childIno.BirthGeneration,
	})
	logging.Debugf("Lookup: returning file inode %d, size=%d", childInodeNum, childIno.Size)
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
	ctx = detach(ctx)
	logging.Debugf("Readdir: dir=%d", d.inodeNum)
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

		logging.Debugf("Readdir: entry %q inode=%d type=%d", entry.Name, entry.InodeNum, entry.FileType)
		result = append(result, fuse.DirEntry{
			Ino:  entry.InodeNum,
			Name: entry.Name,
			Mode: mode,
		})
	}

	return &dirStream{entries: result}, 0
}

// Create creates a new file.
func (d *Dir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return d.fs.createFile(detach(ctx), &d.Inode, d.inodeNum, name, flags, mode, out)
}

// Mkdir creates a new directory.
func (d *Dir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return d.fs.makeDir(detach(ctx), &d.Inode, d.inodeNum, name, mode, out)
}

// Unlink removes a file.
func (d *Dir) Unlink(ctx context.Context, name string) syscall.Errno {
	ctx = detach(ctx)
	return d.removeEntry(ctx, name, false)
}

// Rmdir removes an empty directory.
func (d *Dir) Rmdir(ctx context.Context, name string) syscall.Errno {
	ctx = detach(ctx)
	return d.removeEntry(ctx, name, true)
}

// removeEntry removes a file or empty directory.
func (d *Dir) removeEntry(ctx context.Context, name string, isDir bool) syscall.Errno {
	// Room for the removal's record and for freeing a file it leaves without a link or an opener.
	ctx, release := d.fs.admitOp(ctx, uint32(len(name))+1024, freeRecordBound)
	defer release()
	return retryRace("Remove", name, func() syscall.Errno { return d.removeEntryOnce(ctx, name, isDir) })
}

// freeRecordBound bounds the record that frees an inode and the extents it maps, for a file that is not fragmented.
const freeRecordBound = 4096

// sameForRemoval reports whether cur still matches the removal's checks: a file's writes don't count, a directory's new entries do.
func sameForRemoval(was, cur *inode.Inode) bool {
	if cur.Magic != inode.InodeMagic || cur.Mode&inode.S_IFMT != was.Mode&inode.S_IFMT || cur.LinkCount != was.LinkCount {
		return false
	}
	return !was.IsDir() || cur.MetaGeneration == was.MetaGeneration
}

// childInRange returns EIO, and logs the entry, for a directory entry that
// names an inode at or past the end of the inode table. Only a damaged entry
// does, and narrowing its number to the table's uint32 could reach a real inode.
func (f *FS) childInRange(dirNum uint32, name string, num uint64) syscall.Errno {
	total := f.inodeTbl.TotalInodes()
	if num < total {
		return 0
	}
	if f.firstSighting(badEntry{dirNum, name}) {
		log.Printf("[ERROR] Directory %d: entry %q names inode %d, past the inode table's %d inodes. "+
			"Unmount the filesystem on every node and run goclusterfs-fsck", dirNum, name, num, total)
	}
	return syscall.EIO
}

type badEntry struct {
	dir  uint32
	name string
}

// maxBadEntries caps how many damaged entries are remembered; past it the set starts over, so each is logged again.
const maxBadEntries = 256

// firstSighting reports whether e is not yet in the set of logged damaged entries, and adds it.
func (f *FS) firstSighting(e badEntry) bool {
	f.badEntriesMu.Lock()
	defer f.badEntriesMu.Unlock()
	if f.badEntries[e] {
		return false
	}
	if f.badEntries == nil || len(f.badEntries) >= maxBadEntries {
		f.badEntries = make(map[badEntry]bool)
	}
	f.badEntries[e] = true
	return true
}

// removeEntryOnce returns EAGAIN when another node changes the child between the lookup and the removal.
func (d *Dir) removeEntryOnce(ctx context.Context, name string, isDir bool) syscall.Errno {
	// Invalidate parent directory cache before modifying
	d.fs.invalidateInode(d.inodeNum)

	dirIno, err := d.fs.inodeTbl.Read(d.inodeNum)
	if err != nil {
		return syscall.EIO
	}

	childInodeNum, fileType, err := d.fs.dirMgr.Lookup(ctx, dirIno, name)
	if err != nil {
		return nameErrno(err)
	}
	if errno := d.fs.childInRange(d.inodeNum, name, childInodeNum); errno != 0 {
		return errno
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

	// Rmdir removes only empty directories, whose link count also counts their own "." entry.
	willFreeInode := isDir
	var live uint64
	// No node can hold a symlink open, so its last unlink frees it at once.
	if !isDir && childIno.LinkCount == 1 && !childIno.IsSymlink() {
		if live, err = d.fs.reserveOrphan(childIno); err != nil {
			if errors.Is(err, errOrphansFull) {
				log.Printf("[WARN] unlink %q: %v", name, err)
				return syscall.EBUSY
			}
			return syscall.EIO
		}
	}
	reserved := live != 0
	defer func() {
		if reserved && d.fs.orphanReg != nil {
			o := orphanOf(childIno)
			_ = d.fs.orphanReg.DropOrphans(func(x node.Orphan) bool { return x == o })
		}
	}()

	// === WAL: Begin transaction and log intent ===
	var txn *journal.Transaction
	var intentSeq uint64
	var freedBlocks int64
	// A free that did not happen gives back its charge after the deferred closeIntent, so replay can no longer free.
	kept := false
	defer func() {
		if kept && willFreeInode && intentSeq != 0 {
			d.fs.commitCharge(childIno.ProjectID, freedBlocks, 1)
		}
	}()
	keepCharge := func() { kept = true }
	if d.fs.journal != nil {
		txn = d.fs.journal.Begin()

		// Determine entry type for directory removal
		entryType := journal.DirEntryRegular
		if isDir {
			entryType = journal.DirEntryDirectory
		}

		// Log directory entry removal
		if err := txn.LogDirRemoveEntry(&journal.DirRemoveEntryPayload{
			ParentInodeNum:      uint64(d.inodeNum),
			ChildInodeNum:       uint64(childInodeNum),
			EntryType:           entryType,
			Name:                name,
			ChildMetaGeneration: childIno.MetaGeneration,
		}); err != nil {
			d.fs.journal.Abort(txn)
			return syscall.EIO
		}

		if willFreeInode {
			if freedBlocks, err = d.fs.logInodeFree(txn, childIno, childIno.MetaGeneration); err != nil {
				d.fs.journal.Abort(txn)
				return syscall.EIO
			}
		} else if err := txn.LogLinkDrop(&journal.LinkDropPayload{
			InodeNum:       uint64(childInodeNum),
			MetaGeneration: childIno.MetaGeneration,
		}); err != nil {
			d.fs.journal.Abort(txn)
			return syscall.EIO
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
		intentSeq, commitErr = d.fs.commit(ctx, txn)
		if commitErr != nil {
			return syscall.EIO
		}
		defer d.fs.closeIntent(intentSeq)
		if afterCommit != nil {
			afterCommit("unlink")
		}
	}
	// === End WAL transaction ===

	// Now execute the actual metadata operations (safe - recoverable if crash)

	// Another node may have removed the name, or changed the child, since the lookup.
	var changed, readFailed bool
	removed, err := d.fs.dirMgr.RemoveIfAndUpdate(ctx, dirIno, name, func(child uint64) bool {
		if child != childInodeNum {
			return false
		}
		cur, err := d.fs.inodeTbl.Read(uint32(child))
		readFailed = err != nil
		changed = err == nil && !sameForRemoval(childIno, cur)
		return err == nil && !changed
	}, func(p *inode.Inode) {
		if isDir {
			p.LinkCount--
		}
		p.Mtime = time.Now().UnixNano()
	})
	switch {
	case err != nil:
		keepCharge()
		return nameErrno(err)
	case readFailed:
		keepCharge()
		return syscall.EIO
	case changed:
		keepCharge()
		return syscall.EAGAIN
	case !removed:
		keepCharge()
		return syscall.ENOENT
	}
	if afterCommit != nil {
		afterCommit("unlink: name removed")
	}

	if willFreeInode {
		// An open handle may write the file after the name is gone, so free what the inode maps when it is freed.
		var freed *inode.Inode
		ok, err := d.fs.inodeTbl.FreeIf(ctx, uint32(childInodeNum), func(ino *inode.Inode) bool {
			freed = ino
			return !isDir || ino.MetaGeneration == childIno.MetaGeneration
		})
		// Free the inode first: once it is gone, replaying this unlink's ExtentFree cannot free the blocks a second time.
		if err != nil {
			log.Printf("[WARN] removeEntry %q: inode %d not freed, keeping its blocks: %v", name, childInodeNum, err)
			keepCharge()
		} else if !ok {
			log.Printf("[WARN] removeEntry %q: directory inode %d changed after its name was removed, keeping it", name, childInodeNum)
			keepCharge()
		} else {
			if afterCommit != nil {
				afterCommit("unlink: inode freed")
			}
			sfbBacked, sfbSlot := freed.IsSFBBacked(), freed.InlineExtents[0].PhysicalBlock
			if err := d.fs.extentMgr.Delete(ctx, freed); err != nil {
				log.Printf("[WARN] removeEntry %q: freeing the blocks of inode %d failed, some may leak: %v", name, childInodeNum, err)
			}
			if sfbBacked {
				d.fs.freeSFBSlot(ctx, sfbSlot)
			}
			if afterCommit != nil {
				afterCommit("unlink: blocks freed")
			}
		}
	} else if err := d.fs.dropLink(ctx, uint32(childInodeNum), live); err != nil {
		log.Printf("[WARN] removeEntry %q: link of inode %d not dropped: %v", name, childInodeNum, err)
	} else {
		reserved = false
	}

	d.fs.announceDir(d.inodeNum)

	// === WAL: Mark intent complete ===
	if d.fs.journal != nil && intentSeq > 0 {
		if err := d.fs.journal.CompleteIntent(intentSeq); err != nil {
			logging.Debugf("removeEntry %q: CompleteIntent failed: %v", name, err)
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
	birth    uint64
	// readFrom is when a read last started reading this file's inode from disk.
	readFrom atomic.Pointer[time.Time]
}

func newFile(fsys *FS, ino *inode.Inode) *File {
	return &File{fs: fsys, inodeNum: ino.InodeNum, birth: ino.BirthGeneration}
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

// fileHandle is empty because the File node serves all I/O, but go-fuse calls Release only for an open that returned a handle.
type fileHandle struct{}

func (f *File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debugf("File.Open: inode=%d flags=%d", f.inodeNum, flags)
	// Counted before the check, so a peer's last unlink sees this open and keeps the file.
	unlock := f.fs.lockInode(f.inodeNum)
	reopen := f.fs.addOpen(f.inodeNum, 1) > 1
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if usable := err == nil && f.current(ino) && (ino.LinkCount > 0 || reopen && f.fs.holdsOrphan(ino)); !usable {
		f.fs.addOpen(f.inodeNum, -1)
		unlock()
		if err != nil {
			logging.Debugf("File.Open: read failed: %v", err)
			return nil, 0, syscall.EIO
		}
		logging.Debugf("File.Open: inode %d is no longer this file", f.inodeNum)
		return nil, 0, syscall.ESTALE
	}
	unlock()
	logging.Debugf("File.Open: success, Size=%d", ino.Size)
	// Close-to-open consistency: reads through this handle start from the on-disk inode.
	f.fs.cacheInode(ino)
	// The kernel stops reads at the size it cached, which a lookup during another
	// node's write can leave short. Expiring it makes fstat and reads ask again.
	if errno := f.NotifyContent(-1, 0); errno != 0 && errno != syscall.ENOSYS {
		logging.Debugf("File.Open: inode %d: expiring kernel attributes failed: %v", f.inodeNum, errno)
	}
	return fileHandle{}, f.fs.openFlags, 0
}

// Release lets go of an unlinked file at its last close on this node.
func (f *File) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	if f.fs.addOpen(f.inodeNum, -1) > 0 {
		return 0
	}
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err == nil && ino.Magic == inode.InodeMagic && ino.LinkCount == 0 && ino.Holders&f.fs.slotBit() != 0 {
		if err := f.fs.clearHolder(detach(ctx), orphanOf(ino), 0); err != nil && !errors.Is(err, errStillLinked) {
			log.Printf("[WARN] inode %d: letting go of it at its last close failed: %v", f.inodeNum, err)
		}
	}
	return 0
}

// current reports whether the slot still holds this node's file; birth 0, from older versions, matches any since a snapshot may assign one.
func (f *File) current(ino *inode.Inode) bool {
	return ino.Magic == inode.InodeMagic && ino.IsFile() && (f.birth == 0 || ino.BirthGeneration == f.birth)
}

func (f *File) stillCurrent() syscall.Errno {
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	switch {
	case err != nil:
		return syscall.EIO
	case !f.current(ino):
		return syscall.ESTALE
	}
	return 0
}

// Getattr returns file attributes.
func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debugf("File.Getattr: inode=%d", f.inodeNum)
	// Read directly from disk to get fresh inode state.
	// In multi-node scenarios, another node may have updated the inode.
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		logging.Debugf("File.Getattr: read failed: %v", err)
		return syscall.EIO
	}
	if !f.current(ino) {
		return syscall.ESTALE
	}

	logging.Debugf("File.Getattr: inode=%d Size=%d AllocatedBlocks=%d", f.inodeNum, ino.Size, ino.AllocatedBlocks)
	// Reads use the cached inode; they must not stop at an older size than stat shows.
	if ino.Magic == inode.InodeMagic {
		f.fs.cacheInode(ino)
	}

	// Zero means go-fuse applies the mount's -attr-timeout.
	out.AttrValid = 0

	out.Ino = uint64(ino.InodeNum)
	out.Mode = uint32(fuse.S_IFREG | (ino.Mode & 0o7777))
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
	out.Blocks = ino.AllocatedBlocks * (f.fs.blockSize / 512)
	out.Blksize = uint32(f.fs.blockSize)

	return 0
}

// Read reads file data.
func (f *File) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	unlockIO, err := f.fs.lockFileIO(detach(ctx), f.inodeNum, lock.LockModeShared)
	if err != nil {
		log.Printf("[ERROR] Read inode %d: %v", f.inodeNum, err)
		return nil, syscall.EIO
	}
	defer unlockIO()
	ino, err := f.inodeForRead()
	if err != nil {
		logging.Debugf("Read inode %d: Read failed: %v", f.inodeNum, err)
		return nil, syscall.EIO
	}
	if !f.current(ino) {
		return nil, syscall.ESTALE
	}

	logging.Debugf("Read inode %d: Size=%d AllocatedBlocks=%d Extents=[%+v, %+v]",
		f.inodeNum, ino.Size, ino.AllocatedBlocks, ino.InlineExtents[0], ino.InlineExtents[1])

	offset := uint64(off)

	// Check bounds using the authoritative size from disk
	if offset >= ino.Size {
		logging.Debugf("Read inode %d: offset %d >= Size %d, returning empty", f.inodeNum, offset, ino.Size)
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

	data := make([]byte, readSize)
	// Bytes on contiguous disk blocks go to the device as one read.
	var runDisk int64
	runStart, runLen := 0, 0
	flush := func() error {
		if runLen == 0 {
			return nil
		}
		err := f.fs.readSpan(data[runStart:runStart+runLen], runDisk)
		runLen = 0
		return err
	}
	for bytesRead := 0; bytesRead < readSize; {
		blockNum, blockOffset := offset/f.fs.blockSize, offset%f.fs.blockSize
		toRead := min(int(f.fs.blockSize-blockOffset), readSize-bytesRead)
		physBlock, err := f.fs.lookupPhysical(ino, blockNum)
		if err != nil {
			return nil, syscall.EIO
		}
		if physBlock == extent.Hole {
			if flush() != nil {
				return nil, syscall.EIO
			}
		} else {
			diskOffset := int64(f.fs.dataOffset + physBlock*f.fs.blockSize + blockOffset)
			if runLen > 0 && runDisk+int64(runLen) != diskOffset && flush() != nil {
				return nil, syscall.EIO
			}
			if runLen == 0 {
				runDisk, runStart = diskOffset, bytesRead
			}
			runLen += toRead
		}
		bytesRead += toRead
		offset += uint64(toRead)
	}
	if flush() != nil {
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(data), 0
}

// inodeForRead returns this file's inode to a read that holds its file I/O
// lock. Another node may have written the file before this node claimed the
// lock from disk, so the first read after each claim reads the inode from disk.
func (f *File) inodeForRead() (*inode.Inode, error) {
	if f.fs.lockMgr == nil {
		return f.fs.getInode(f.inodeNum)
	}
	claimed := f.fs.lockMgr.HeldSince(lock.LockTypeFileIO, uint64(f.inodeNum))
	if from := f.readFrom.Load(); from != nil && !from.Before(claimed) {
		return f.fs.getInode(f.inodeNum)
	}
	start := time.Now()
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil || !f.current(ino) {
		return ino, err
	}
	f.fs.cacheInode(ino)
	f.readFrom.Store(&start)
	return ino, nil
}

// inodeForWrite returns a copy of this file's inode for a write to plan from, from the cache when inodeForRead would
// use it. Each step the plan leads to checks the inode again under the inode lock.
func (f *File) inodeForWrite() (*inode.Inode, error) {
	ino, err := f.inodeForRead()
	if err != nil {
		return nil, err
	}
	plan := *ino
	return &plan, nil
}

// wrote hands the inode a write put on disk to this node's cache and other nodes'. The write held the file I/O lock
// since start, so later reads and writes under the same hold can plan from the cache.
func (f *File) wrote(ino *inode.Inode, start time.Time) {
	if f.fs.cacheMgr != nil {
		f.fs.cacheMgr.BroadcastInvalidateInode(uint32(f.inodeNum), ino.DataGeneration, ino.MetaGeneration)
	}
	f.fs.cacheInode(ino)
	f.readFrom.Store(&start)
}

// Write writes file data.
// Uses an optimistic retry pattern to handle concurrent modifications from other nodes.
func (f *File) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	ctx, release := f.fs.admitOp(detach(ctx), f.fs.writeRecordBounds(len(data))...)
	defer release()
	unlockIO, err := f.fs.lockFileIO(ctx, f.inodeNum, lock.LockModeExclusive)
	if err != nil {
		log.Printf("[ERROR] Write inode %d: %v", f.inodeNum, err)
		return 0, errToErrno(err)
	}
	defer unlockIO()
	return f.writeIOLocked(ctx, data, off)
}

// writeIOLocked is Write for a caller that holds the file's I/O lock.
func (f *File) writeIOLocked(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if errno := f.checkMapQuota(ctx, uint64(off), uint64(off)+uint64(len(data))); errno != 0 {
		return 0, errno
	}
	// Retry loop to handle concurrent modifications from other nodes.
	// When multiple nodes try to extend the same file simultaneously, they may race
	// to allocate blocks. If a conflict is detected, errRetryWrite is returned and
	// we retry the operation.
	for attempt := 0; attempt < MaxWriteRetries; attempt++ {
		unlock := f.fs.lockInode(f.inodeNum)
		bytesWritten, err := f.writeInternal(ctx, data, uint64(off))
		unlock()
		if err == errRetryWrite {
			logging.Debugf("Write inode %d: retry attempt %d due to concurrent modification",
				f.inodeNum, attempt+1)
			f.readFrom.Store(nil)
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

// waitOutReplay waits, holding no lock, for the extent lock key that a failing
// node holds until its replay ends, then has the write retried. A replay that
// needed the inode lock this write gave up can finish meanwhile.
func (f *File) waitOutReplay(ctx context.Context, key uint64) error {
	if err := f.fs.lockMgr.AcquireExtent(ctx, key, lock.LockModeExclusive); err != nil {
		log.Printf("[WARN] Write inode %d: waiting for extent lock %d after its holder's replay failed: %v", f.inodeNum, key, err)
		return syscall.EIO
	}
	f.fs.release(lock.LockTypeExtent, key, "block")
	return errRetryWrite
}

// extentLockKey names the extent lock that covers physBlock: one per 1 MB of the data region, whatever the block
// size, so at 1 MB blocks it is the block number.
func (f *FS) extentLockKey(physBlock uint64) uint64 {
	return physBlock * f.blockSize >> 20
}

// extentLockKeys returns the extent locks that cover n blocks from physBlock, in the record order they must be
// taken in.
func (f *FS) extentLockKeys(physBlock, n uint64) []uint64 {
	var keys []uint64
	for key := f.extentLockKey(physBlock); key <= f.extentLockKey(physBlock+n-1); key++ {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b uint64) int {
		return cmp.Compare(f.lockMgr.Record(lock.LockTypeExtent, a), f.lockMgr.Record(lock.LockTypeExtent, b))
	})
	return keys
}

// stillWritableInPlace checks, under the inode lock, that the slot still holds this file, that n logical blocks
// from blockNum still map to the blocks from physBlock, and that no other file shares them.
func (f *File) stillWritableInPlace(blockNum, physBlock, n uint64) error {
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		logging.Debugf("Write inode %d: re-read for in-place write failed: %v", f.inodeNum, err)
		return syscall.EIO
	}
	if !f.current(ino) {
		return syscall.ESTALE
	}
	ext, off, err := f.fs.extentMgr.Lookup(ino, blockNum)
	switch {
	case errors.Is(err, extent.ErrExtentNotFound), errors.Is(err, extent.ErrStaleTree):
		return errRetryWrite
	case err != nil:
		logging.Debugf("Write inode %d: lookup for in-place write at block %d failed: %v", f.inodeNum, blockNum, err)
		return syscall.EIO
	case ext.PhysicalBlock+off != physBlock, off+n > ext.Length:
		return errRetryWrite
	}
	if ext.Flags&extent.FlagShared == 0 || f.fs.refcountReg == nil {
		return nil
	}
	for b := physBlock; b < physBlock+n; b++ {
		entry, err := f.fs.refcountReg.GetUnlocked(b)
		if err != nil {
			logging.Debugf("Write inode %d: refcount read for block %d failed: %v", f.inodeNum, b, err)
			return syscall.EIO
		}
		if entry.NeedsCoW() {
			return errRetryWrite
		}
	}
	return nil
}

// retryOnTreeConflict turns a lost extent-tree race into a write retry.
func retryOnTreeConflict(err error) error {
	if errors.Is(err, extent.ErrTreeConflict) {
		return errRetryWrite
	}
	return err
}

// zeroRange zeroes bytes [from, to) of a data block; both are multiples of the sector size.
func (f *FS) zeroRange(physBlock, from, to uint64) error {
	if to <= from {
		return nil
	}
	buf := storage.GetBuffer(int(to - from))
	defer storage.PutBuffer(buf)
	clear(buf.Bytes())
	_, err := f.dev.WriteAt(buf.Bytes()[:to-from], int64(f.dataOffset+physBlock*f.blockSize+from))
	return err
}

// readSpan fills dst from disk offset off with one sector-aligned device read.
func (f *FS) readSpan(dst []byte, off int64) error {
	start := int64(alignSector(uint64(off), false))
	end := int64(alignSector(uint64(off)+uint64(len(dst)), true))
	buf := storage.GetBuffer(int(end - start))
	defer storage.PutBuffer(buf)
	span := buf.Bytes()[:end-start]
	if n, err := f.dev.ReadAt(span, start); err != nil || n != len(span) {
		return fmt.Errorf("read %d bytes at %d: n=%d: %w", len(span), start, n, err)
	}
	copy(dst, span[off-start:])
	return nil
}

// writeSpan writes src at disk offset off with one sector-aligned device write,
// first reading the sectors it covers only in part.
func (f *FS) writeSpan(src []byte, off int64) error {
	start := int64(alignSector(uint64(off), false))
	end := int64(alignSector(uint64(off)+uint64(len(src)), true))
	buf := storage.GetBuffer(int(end - start))
	defer storage.PutBuffer(buf)
	span := buf.Bytes()[:end-start]
	partial := false
	readSector := func(at int64) {
		sector := span[at-start : at-start+storage.SectorSize]
		if _, err := f.dev.ReadAt(sector, at); err != nil {
			clear(sector)
		}
		partial = true
	}
	if off != start {
		readSector(start)
	}
	if last := end - storage.SectorSize; off+int64(len(src)) != end && (last != start || !partial) {
		readSector(last)
	}
	if partial && afterSectorRead != nil {
		afterSectorRead()
	}
	copy(span[off-start:], src)
	_, err := f.dev.WriteAt(span, start)
	return err
}

// spanJoiner holds back writes of new blocks, so that writes that follow each other in memory and on the device
// reach it as one.
type spanJoiner struct {
	fs  *FS
	off int64
	buf []byte
}

func (j *spanJoiner) add(src []byte, off int64) error {
	n := len(j.buf)
	if n > 0 && j.off+int64(n) == off && cap(j.buf) > n && &j.buf[:n+1][n] == &src[0] {
		j.buf = j.buf[:n+len(src)]
		return nil
	}
	if err := j.flush(); err != nil {
		return err
	}
	j.off, j.buf = off, src
	return nil
}

func (j *spanJoiner) flush() error {
	if len(j.buf) == 0 {
		return nil
	}
	buf := j.buf
	j.buf = nil
	return j.fs.writeSpan(buf, j.off)
}

// alignSector rounds n to a sector boundary, up or down.
func alignSector(n uint64, up bool) uint64 {
	if up {
		n += storage.SectorSize - 1
	}
	return n &^ (storage.SectorSize - 1)
}

// testHookBeforeInodeCommit, when set by a test, runs before writeInternal's final
// inode write; an error it returns is handled as if that write had failed.
var testHookBeforeInodeCommit func() error

// testHookBeforeFreeingUnused, when set by a test, runs after a failed write closes its intent and before it frees blocks.
var testHookBeforeFreeingUnused func()

// mapReadyBlockLocked lets a write keep the inode lock when the reserve has a block and the tree needs none.
func (f *File) mapReadyBlockLocked(ctx context.Context, ino *inode.Inode, blockNum uint64, lockHeld bool) (*extent.Extent, bool) {
	if f.fs.reserve == nil || !lockHeld {
		return nil, false
	}
	if need, err := f.fs.extentMgr.TreeBlocksForInsert(ino); err != nil || need > 0 {
		return nil, false
	}
	b, ok := f.fs.reserve.TakeReady()
	if !ok {
		return nil, false
	}
	ext := &extent.Extent{LogicalBlock: blockNum, PhysicalBlock: b, Length: 1, Flags: extent.FlagAllocated}
	if err := f.fs.extentMgr.Insert(ctx, ino, ext); err != nil {
		f.fs.reserve.Untake(b)
		return nil, false
	}
	ino.AllocatedBlocks += ext.Length
	ino.WrittenBlocks += ext.Length
	return ext, true
}

// allocateBlockLocked maps a new data block at blockNum and returns the inode
// re-read under the inode lock, which stays held on success. The bitmap comes
// before the inode in lock order, so the data block and any extent-tree blocks
// are allocated before the lock is taken. Call the returned release after
// dropping the inode lock.
func (f *File) allocateBlockLocked(ctx context.Context, ino *inode.Inode, blockNum uint64) (*inode.Inode, *extent.Extent, func(), bool, error) {
	physBlock, reserved := uint64(0), false
	if f.fs.reserve != nil {
		physBlock, reserved = f.fs.reserve.Take(ctx)
	}
	if !reserved {
		blocks, err := f.fs.bitmapMgr.Alloc(ctx, 1)
		if err != nil {
			logging.Debugf("Write inode %d: allocating block %d failed: %v", f.inodeNum, blockNum, err)
			return nil, nil, nil, false, allocErrno(err)
		}
		physBlock = blocks[0]
	}
	// A reserved block goes back to the reserve: its record still lists it.
	giveBack := func() {
		if reserved {
			f.fs.reserve.Untake(physBlock)
		} else {
			f.fs.freeUnused(ctx, f.inodeNum, physBlock)
		}
	}
	ext := &extent.Extent{LogicalBlock: blockNum, PhysicalBlock: physBlock, Length: 1, Flags: extent.FlagAllocated}
	treeCtx, releaseTree, err := f.fs.extentMgr.ReserveTreeBlocks(ctx, ino)
	if err != nil {
		logging.Debugf("Write inode %d: reserving extent-tree blocks failed: %v", f.inodeNum, err)
		giveBack()
		return nil, nil, nil, false, allocErrno(err)
	}

	if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
		giveBack()
		releaseTree()
		return nil, nil, nil, false, syscall.EIO
	}
	var fresh *inode.Inode
	fail := func(errno error) (*inode.Inode, *extent.Extent, func(), bool, error) {
		f.fs.release(lock.LockTypeInode, uint64(f.inodeNum), "inode")
		if fresh != nil {
			f.fs.extentMgr.DiscardPending(ctx, fresh)
		}
		giveBack()
		releaseTree()
		return nil, nil, nil, false, errno
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
	return fresh, ext, releaseTree, reserved, nil
}

// writeInternal performs the actual write operation.
// Returns errRetryWrite if a concurrent modification is detected and the caller should retry.
func (f *File) writeInternal(ctx context.Context, data []byte, offset uint64) (int, error) {
	bytesWritten := 0
	requested, writeStart := len(data), offset
	// writtenEnd is the end of the data this write has put on disk so far; oldEnd is the file's size it started from.
	writtenEnd, oldEnd := uint64(0), uint64(0)

	// Track if we hold the inode lock
	lockHeld := false
	// ino has changes made under the inode lock that are not on disk yet
	inoDirty := false
	// Tree-block reservations are released after the inode lock, as lock order requires.
	var releaseReservations []func()
	releaseInodeLock := func() {
		if lockHeld && f.fs.lockMgr != nil {
			f.fs.release(lock.LockTypeInode, uint64(f.inodeNum), "inode")
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
	// How many of allocatedExtents the file's project was charged for.
	chargedAllocs := 0
	newAllocBlocks := func(from int) int64 {
		var n int64
		for _, alloc := range allocatedExtents[from:] {
			n += int64(alloc.count)
		}
		return n
	}
	// No inode write may map these until their use is logged.
	var reserveOut []uint64
	var intents []uint64
	defer func() {
		for _, seq := range intents {
			f.fs.closeIntent(seq)
		}
	}()
	logReserveUse := func() error {
		if len(reserveOut) == 0 {
			return nil
		}
		var seq uint64
		err := f.fs.reserve.Logged(reserveOut, func() error {
			txn := f.fs.journal.Begin()
			if err := txn.LogBlockUse(reserveOut); err != nil {
				f.fs.journal.Abort(txn)
				return err
			}
			var err error
			if seq, err = f.fs.commit(ctx, txn); err != nil {
				f.fs.journal.Abort(txn)
			}
			return err
		})
		used := len(reserveOut)
		reserveOut = nil
		if err != nil {
			logging.Debugf("Write inode %d: logging the use of %d reserved blocks failed, leaving them allocated: %v", f.inodeNum, used, err)
			return syscall.EIO
		}
		intents = append(intents, seq)
		return nil
	}
	// An inode write that failed may still have reached the disk, so copies it
	// carried can no longer be freed.
	copiesMayBeOnDisk := false
	// Abort transaction on error path
	defer func() {
		if txn != nil && f.fs.journal != nil {
			// Transaction was not committed - abort it
			f.fs.journal.Abort(txn)
		}
	}()

	start := time.Now()
	ino, err := f.inodeForWrite()
	if err != nil {
		logging.Debugf("Write inode %d: Read failed: %v", f.inodeNum, err)
		return 0, syscall.EIO
	}
	if !f.current(ino) {
		return 0, syscall.ESTALE
	}
	// persistAndUnlock writes changes made under the inode lock and drops it, so
	// the bitmap lock, which comes first in lock order, can be taken next.
	// No inode write may map a new block before its data is on the device.
	joiner := spanJoiner{fs: f.fs}
	persistAndUnlock := func() error {
		if err := joiner.flush(); err != nil {
			logging.Debugf("Write inode %d: writing new blocks failed: %v", f.inodeNum, err)
			return syscall.EIO
		}
		if !lockHeld || f.fs.lockMgr == nil {
			return nil
		}
		// Once the lock drops, another node's growth may zero the rest of the block the file ended in. Cover what
		// this write put there; a write that started at a block boundary stays invisible until it completes.
		if oldEnd%f.fs.blockSize != 0 && writtenEnd > ino.Size {
			ino.Size = min(writtenEnd, (oldEnd/f.fs.blockSize+1)*f.fs.blockSize)
			inoDirty = true
		}
		if inoDirty {
			if err := logReserveUse(); err != nil {
				releaseInodeLock()
				f.fs.extentMgr.DiscardPending(ctx, ino)
				return err
			}
			charged := newAllocBlocks(chargedAllocs)
			f.fs.commitCharge(ino.ProjectID, charged, 0)
			chargedAllocs = len(allocatedExtents)
			copiesMayBeOnDisk = true
			if err := f.fs.inodeTbl.WriteWithLockHeld(ino); err != nil {
				logging.Debugf("Write inode %d: persisting inode before unlocking failed: %v", f.inodeNum, err)
				if errors.Is(err, extent.ErrTreeConflict) {
					f.fs.commitCharge(ino.ProjectID, -charged, 0)
				}
				releaseInodeLock()
				f.fs.extentMgr.DiscardPending(ctx, ino)
				return retryOnTreeConflict(err)
			}
			inoDirty = false
			copiesMayBeOnDisk = false
			committedAllocs, committedCoW = len(allocatedExtents), len(cowDone)
		}
		releaseInodeLock()
		// The inode on disk now maps the blocks the round's records named, so they end now, and a long write does not
		// hold the journal's head back.
		for _, seq := range intents {
			f.fs.closeIntent(seq)
		}
		intents = nil
		return nil
	}
	inodeWritten := false
	defer func() {
		if !inodeWritten {
			// Discarding frees tree blocks, which needs the bitmap lock: drop the inode lock first.
			releaseInodeLock()
			f.fs.extentMgr.DiscardPending(ctx, ino)
			if len(reserveOut) > 0 {
				f.fs.reserve.Untake(reserveOut...)
			}
			if !copiesMayBeOnDisk {
				for _, cow := range cowDone[committedCoW:] {
					if err := f.fs.extentMgr.FreePhysicalBlock(ctx, cow.NewPhysBlock); err != nil {
						logging.Debugf("Write inode %d: freeing unused copy %d failed: %v", f.inodeNum, cow.NewPhysBlock, err)
					}
				}
			}
		}
	}()

	// SFB allocation is disabled: every file uses extents, and a file written by an
	// older build moves out of its SFB slot before the write.
	if ino.IsSFBBacked() {
		if err := f.migrateToExtent(ctx, ino); err != nil {
			logging.Debugf("Write inode %d: SFB migration failed: %v", f.inodeNum, err)
			if err == errRetryWrite {
				return 0, err
			}
			return 0, syscall.EIO
		}
		migrated, err := f.fs.inodeTbl.Read(f.inodeNum)
		if err != nil {
			return 0, syscall.EIO
		}
		ino = migrated
	}

	// A write that grows the file holds the inode lock exclusively from its first data write through its size update,
	// so another node's growth cannot zero bytes this write put past the end before its size covers them.
	if f.fs.lockMgr != nil && offset+uint64(len(data)) > ino.Size {
		if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
			logging.Debugf("Write inode %d: failed to acquire inode lock to grow the file: %v", f.inodeNum, err)
			return 0, syscall.EIO
		}
		lockHeld = true
		locked, err := f.fs.inodeTbl.Read(f.inodeNum)
		if err != nil {
			return 0, syscall.EIO
		}
		ino = locked
	}
	oldEnd = ino.Size
	segments, err := f.growthSegments(ino, data, offset)
	if err != nil {
		return 0, err
	}
	offset, data = segments[0].off, segments[0].data

	// Regular extent-based write path
	for {
		if bytesWritten == len(data) {
			if segments = segments[1:]; len(segments) == 0 {
				break
			}
			offset, data, bytesWritten = segments[0].off, segments[0].data, 0
			continue
		}
		blockNum := offset / f.fs.blockSize
		blockOffset := offset % f.fs.blockSize
		zeroFirst := false

		// Try to find existing extent first (without lock for read-only check)
		ext, extOffset, err := f.fs.extentMgr.Lookup(ino, blockNum)
		if err != nil && err != extent.ErrExtentNotFound {
			logging.Debugf("Write inode %d: extent lookup at block %d failed: %v", f.inodeNum, blockNum, err)
			if errors.Is(err, extent.ErrStaleTree) {
				return 0, errRetryWrite
			}
			return 0, syscall.EIO
		}
		// A block allocated by this write stays invisible to other nodes until the inode,
		// whose lock this write holds, is committed, so nobody else can write it meanwhile.
		newBlock := false
		if err != nil {
			// Need to allocate - must acquire lock first
			if !lockHeld && f.fs.lockMgr != nil {
				if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
					logging.Debugf("Write inode %d: failed to acquire lock: %v", f.inodeNum, err)
					return 0, syscall.EIO
				}
				lockHeld = true

				// Re-read inode after acquiring lock to detect concurrent changes
				freshIno, err := f.fs.inodeTbl.Read(f.inodeNum)
				if err != nil {
					logging.Debugf("Write inode %d: re-read failed: %v", f.inodeNum, err)
					return 0, syscall.EIO
				}

				// Check if MetaGeneration changed (concurrent modification)
				if freshIno.MetaGeneration != ino.MetaGeneration {
					logging.Debugf("Write inode %d: MetaGeneration changed %d -> %d, retrying",
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

			if newExt, ok := f.mapReadyBlockLocked(ctx, ino, blockNum, lockHeld); ok {
				reserveOut = append(reserveOut, newExt.PhysicalBlock)
				inoDirty = true
				ext = newExt
				newBlock = true
			} else if f.fs.lockMgr != nil {
				// The inode is re-read after allocation, so persist changes made under the lock first.
				if err := persistAndUnlock(); err != nil {
					return 0, err
				}
				freshIno, newExt, releaseTree, reserved, allocErr := f.allocateBlockLocked(ctx, ino, blockNum)
				if allocErr != nil {
					logging.Debugf("Write inode %d: allocating block %d failed: %v", f.inodeNum, blockNum, allocErr)
					return 0, allocErr
				}
				if reserved {
					reserveOut = append(reserveOut, newExt.PhysicalBlock)
				}
				lockHeld = true
				releaseReservations = append(releaseReservations, releaseTree)
				ino = freshIno
				inoDirty = true
				ext = newExt
				newBlock = true
			} else {
				// Allocate block (acquires bitmap lock internally, then releases it)
				ext, err = f.fs.extentMgr.AllocateBlocks(ctx, ino, blockNum, 1)
				newBlock = true
				if err != nil {
					logging.Debugf("Write inode %d: AllocateBlocks at block %d failed: %v", f.inodeNum, blockNum, err)
					switch {
					case errors.Is(err, extent.ErrExtentOverlap), errors.Is(err, extent.ErrStaleTree):
						// Concurrent allocation by another node
						return 0, errRetryWrite
					case errors.Is(err, extent.ErrExtentTreeFull):
						return 0, syscall.EFBIG
					case errors.Is(err, extent.ErrCorruptTree):
						return 0, syscall.EIO
					default:
						return 0, allocErrno(err)
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
		// A new block may hold a freed file's data; the write zeroes what it does not cover.
		zeroFirst = newBlock
		// A shared block is copied before the write, so its other owners keep their data.
		if ext.Flags&extent.FlagShared != 0 && f.fs.refcountReg != nil {
			// Refcounts and the bitmap come before the inode in lock order.
			if err := persistAndUnlock(); err != nil {
				return 0, err
			}
			needsCoW, cowErr := f.fs.extentMgr.NeedsCoW(ctx, blockExtent(ext, extOffset), f.fs.refcountReg)
			if cowErr != nil {
				logging.Debugf("Write inode %d: CoW check failed for block %d: %v",
					f.inodeNum, ext.PhysicalBlock, cowErr)
				return 0, syscall.EIO
			}
			if needsCoW {
				// Allocating the copy and mapping it may take bitmap and tree blocks, which come before
				// the inode lock; the data is copied under the lock, after any in-place write has landed.
				cowResult, cowErr := f.fs.extentMgr.MapCopy(ctx, ino, blockNum)
				if cowErr != nil {
					logging.Debugf("Write inode %d: CoW failed for block %d: %v",
						f.inodeNum, ext.PhysicalBlock, cowErr)
					f.fs.extentMgr.DiscardPending(ctx, ino)
					// The unlocked copy may point at tree nodes another node has since rewritten.
					if errors.Is(cowErr, extent.ErrStaleTree) {
						return 0, errRetryWrite
					}
					return 0, syscall.EIO
				}
				dropCopy := func() {
					releaseInodeLock()
					f.fs.extentMgr.DiscardPending(ctx, ino)
					f.fs.freeUnused(ctx, f.inodeNum, cowResult.NewPhysBlock)
				}
				if beforeCoWCommit != nil {
					beforeCoWCommit()
				}
				// Mapped on an unlocked copy: keep it only if no node wrote the inode meanwhile.
				if f.fs.lockMgr != nil {
					if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
						logging.Debugf("Write inode %d: failed to acquire lock for CoW: %v", f.inodeNum, err)
						dropCopy()
						return 0, syscall.EIO
					}
					lockHeld = true
					freshIno, err := f.fs.inodeTbl.Read(f.inodeNum)
					if err != nil || freshIno.MetaGeneration != ino.MetaGeneration {
						logging.Debugf("Write inode %d: inode changed during CoW (err=%v)", f.inodeNum, err)
						dropCopy()
						return 0, errRetryWrite
					}
				}
				if err := f.fs.extentMgr.CopyBlockData(cowResult, f.fs.dataOffset); err != nil {
					logging.Debugf("Write inode %d: copying block %d failed: %v", f.inodeNum, cowResult.OldPhysBlock, err)
					dropCopy()
					return 0, syscall.EIO
				}
				inoDirty = true
				cowDone = append(cowDone, cowResult)
				logging.Debugf("Write inode %d: CoW completed, old block %d -> new block %d",
					f.inodeNum, cowResult.OldPhysBlock, cowResult.NewPhysBlock)
				if ext, extOffset, cowErr = f.fs.extentMgr.Lookup(ino, blockNum); cowErr != nil {
					logging.Debugf("Write inode %d: extent lookup after CoW failed: %v", f.inodeNum, cowErr)
					return 0, syscall.EIO
				}
			}
		}

		// The first write to blocks of an unwritten extent (from fallocate) clears
		// the flag on the blocks it covers, and writes them as one run.
		clearedRun := uint64(0)
		uncovered := min(ext.Length-extOffset, (blockOffset+uint64(len(data)-bytesWritten)+f.fs.blockSize-1)/f.fs.blockSize)
		if ext.Flags&extent.FlagUnwritten != 0 && f.fs.lockMgr != nil {
			// Splitting the extent may allocate tree blocks, which needs the bitmap lock
			// before the inode lock. Clear on an unlocked copy, then keep it only if no
			// node wrote the inode before the lock was taken.
			if err := persistAndUnlock(); err != nil {
				return 0, err
			}
			var clearErr error
			clearedRun, clearErr = f.fs.extentMgr.ClearUnwrittenRange(ctx, ino, blockNum, uncovered)
			if clearErr != nil {
				logging.Debugf("Write inode %d: clearing Unwritten flag at block %d failed: %v",
					f.inodeNum, blockNum, clearErr)
				f.fs.extentMgr.DiscardPending(ctx, ino)
				if errors.Is(clearErr, extent.ErrStaleTree) {
					return 0, errRetryWrite
				}
				return 0, syscall.EIO
			}
			if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
				logging.Debugf("Write inode %d: failed to acquire lock for unwritten: %v", f.inodeNum, err)
				f.fs.extentMgr.DiscardPending(ctx, ino)
				return 0, syscall.EIO
			}
			lockHeld = true
			freshIno, err := f.fs.inodeTbl.Read(f.inodeNum)
			if err != nil || freshIno.MetaGeneration != ino.MetaGeneration {
				logging.Debugf("Write inode %d: inode changed during unwritten clear (err=%v)", f.inodeNum, err)
				releaseInodeLock()
				f.fs.extentMgr.DiscardPending(ctx, ino)
				return 0, errRetryWrite
			}
			inoDirty = inoDirty || clearedRun > 0
			zeroFirst = clearedRun > 0
			if ext, extOffset, err = f.fs.extentMgr.Lookup(ino, blockNum); err != nil {
				logging.Debugf("Write inode %d: lookup after unwritten clear failed: %v", f.inodeNum, err)
				return 0, syscall.EIO
			}
		} else if ext.Flags&extent.FlagUnwritten != 0 {
			var clearErr error
			if clearedRun, clearErr = f.fs.extentMgr.ClearUnwrittenRange(ctx, ino, blockNum, uncovered); clearErr != nil {
				logging.Debugf("Write inode %d: clearing Unwritten flag at block %d failed: %v",
					f.inodeNum, blockNum, clearErr)
				return 0, syscall.EIO
			}
			zeroFirst = clearedRun > 0
			if ext, extOffset, err = f.fs.extentMgr.Lookup(ino, blockNum); err != nil {
				logging.Debugf("Write inode %d: lookup after unwritten clear failed: %v", f.inodeNum, err)
				return 0, syscall.EIO
			}
		}

		physBlock := ext.PhysicalBlock + extOffset
		diskOffset := int64(f.fs.dataOffset + physBlock*f.fs.blockSize + blockOffset)

		remaining := uint64(len(data) - bytesWritten)
		toWrite := int(min(f.fs.blockSize-blockOffset, remaining))
		// Contiguous blocks take one check, one lock per megabyte and one device write.
		switch {
		case clearedRun > 0:
			toWrite = int(min(clearedRun*f.fs.blockSize-blockOffset, remaining))
		case !newBlock && !zeroFirst && ext.Flags&(extent.FlagShared|extent.FlagUnwritten) == 0:
			toWrite = int(min((ext.Length-extOffset)*f.fs.blockSize-blockOffset, remaining))
		}
		blocks := (blockOffset + uint64(toWrite) + f.fs.blockSize - 1) / f.fs.blockSize
		var extentKeys []uint64
		if f.fs.lockMgr != nil && !newBlock {
			extentKeys = f.fs.extentLockKeys(physBlock, blocks)
		}
		tail := (blockOffset + uint64(toWrite)) - (blocks-1)*f.fs.blockSize
		zeroFirst = zeroFirst && (blockOffset != 0 || tail != f.fs.blockSize)

		if beforeInPlaceWrite != nil {
			beforeInPlaceWrite()
		}
		// Without the inode lock, ino may predate another node's snapshot, clone or
		// copy of these blocks. A snapshot or clone counts the new owners and then
		// marks the file shared under the exclusive inode lock, so checking again
		// under the shared one orders this write before the mark, or sends it back
		// to copy the blocks.
		inPlaceLockHeld := false
		releaseInPlaceLock := func() {
			if inPlaceLockHeld {
				f.fs.release(lock.LockTypeInode, uint64(f.inodeNum), "inode")
				inPlaceLockHeld = false
			}
		}
		if !lockHeld && f.fs.lockMgr != nil {
			// A shared hold could not take an extent lock below on the same record.
			mode := lock.LockModeShared
			if slices.ContainsFunc(extentKeys, func(key uint64) bool {
				return f.fs.lockMgr.SameRecord(lock.LockTypeInode, uint64(f.inodeNum), lock.LockTypeExtent, key)
			}) {
				mode = lock.LockModeExclusive
			}
			if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), mode); err != nil {
				logging.Debugf("Write inode %d: failed to acquire inode lock: %v", f.inodeNum, err)
				return 0, syscall.EIO
			}
			inPlaceLockHeld = true
			if err := f.stillWritableInPlace(blockNum, physBlock, blocks); err != nil {
				releaseInPlaceLock()
				return 0, err
			}
		}

		// Extent locks serialize in-place writes to the same blocks across nodes,
		// so one node's read-modify-write of a partial sector cannot overwrite
		// another's data. A whole-sector write still takes them, or it lands
		// inside a peer's read-modify-write.
		var heldKeys []uint64
		releaseExtentLocks := func() {
			for _, key := range heldKeys {
				f.fs.release(lock.LockTypeExtent, key, "block")
			}
			heldKeys = nil
		}
		// Holding only the shared inode lock taken above, this write has changed
		// nothing yet that a retry would not redo, so it can step aside for a replay.
		retryable := inPlaceLockHeld
		extentCtx := ctx
		if retryable {
			extentCtx = lock.Retryable(ctx)
		}
		for _, key := range extentKeys {
			if err := f.fs.lockMgr.AcquireExtent(extentCtx, key, lock.LockModeExclusive); err != nil {
				logging.Debugf("Write inode %d: failed to acquire extent lock %d for block %d: %v",
					f.inodeNum, key, physBlock, err)
				releaseExtentLocks()
				releaseInPlaceLock()
				if retryable && errors.Is(err, lock.ErrReplayBlocked) {
					return 0, f.waitOutReplay(ctx, key)
				}
				return 0, syscall.EIO
			}
			heldKeys = append(heldKeys, key)
		}
		if inPlaceLockHeld && duringInPlaceWrite != nil {
			duringInPlaceWrite()
		}

		// Blocks first written here keep their old disk contents outside the write. Zero the head of the first block,
		// and what lies below the end of the file in the last; growing the file later zeroes the rest.
		if zeroFirst {
			lastStart := (blockNum + blocks - 1) * f.fs.blockSize
			below := min(f.fs.blockSize, max(ino.Size, lastStart)-lastStart)
			err := f.fs.zeroRange(physBlock, 0, alignSector(blockOffset, true))
			if from := alignSector(tail, false); err == nil && below > from {
				err = f.fs.zeroRange(physBlock+blocks-1, from, alignSector(below, true))
			}
			if err != nil {
				logging.Debugf("Write inode %d: zeroing block %d failed: %v", f.inodeNum, physBlock, err)
				releaseExtentLocks()
				releaseInPlaceLock()
				return 0, syscall.EIO
			}
		}

		var writeErr error
		if newBlock && !zeroFirst {
			writeErr = joiner.add(data[bytesWritten:bytesWritten+toWrite], diskOffset)
		} else {
			writeErr = f.fs.writeSpan(data[bytesWritten:bytesWritten+toWrite], diskOffset)
		}
		if writeErr != nil {
			logging.Debugf("Write inode %d: writing %d bytes at %d failed: %v", f.inodeNum, toWrite, diskOffset, writeErr)
			releaseExtentLocks()
			releaseInPlaceLock()
			return 0, syscall.EIO
		}
		releaseExtentLocks()
		releaseInPlaceLock()

		bytesWritten += toWrite
		offset += uint64(toWrite)
		writtenEnd = offset
	}

	if err := joiner.flush(); err != nil {
		logging.Debugf("Write inode %d: writing new blocks failed: %v", f.inodeNum, err)
		return 0, syscall.EIO
	}

	// Update inode size if extended
	// To avoid lost-update race when multiple nodes write to the same file:
	// 1. Acquire exclusive inode lock (if not already held)
	// 2. Re-read inode to get freshest size from other nodes
	// 3. Calculate newSize = max(fresh_size, our_write_end)
	// 4. Write inode with updated size
	// This serializes the read-modify-write cycle.
	if beforeSizeUpdate != nil {
		beforeSizeUpdate(f)
	}
	acquiredLockForSize := false
	if !lockHeld && f.fs.lockMgr != nil {
		if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
			logging.Debugf("Write inode %d: failed to acquire lock for size update: %v", f.inodeNum, err)
			return 0, syscall.EIO
		}
		acquiredLockForSize = true
		lockHeld = true
	}

	newMtime := time.Now().UnixNano()
	// An overwrite that mapped nothing new and stays inside the file sets only mtime, in one read and write of the
	// inode's sector.
	if acquiredLockForSize && len(allocatedExtents) == 0 && len(cowDone) == 0 {
		written, err := f.fs.inodeTbl.UpdateWithLockHeld(f.inodeNum, func(fresh *inode.Inode) error {
			if fresh.Size < offset {
				return errGrowsFile
			}
			fresh.Mtime = newMtime
			return nil
		})
		if err == nil {
			inodeWritten = true
			releaseInodeLock()
			f.wrote(written, start)
			return requested, nil
		}
		if !errors.Is(err, errGrowsFile) {
			logging.Debugf("Write inode %d: setting mtime failed: %v", f.inodeNum, err)
			return 0, syscall.EIO
		}
	}

	// Re-read the inode while holding the lock to get the freshest size
	freshIno, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		logging.Debugf("Write inode %d: re-read inode failed: %v", f.inodeNum, err)
		return 0, syscall.EIO
	}
	onDisk := freshIno.Size
	// ino was read before the lock, so it can miss another node's extents.
	if acquiredLockForSize {
		ino = freshIno
	}
	// Use the freshest size from disk, then calculate new size as max(fresh_size, our_write_end)
	newSize := freshIno.Size
	if offset > newSize {
		newSize = offset // Our write extended the file
	}

	// === WAL: Log extent allocations and inode update before committing metadata ===
	var intentSeq uint64
	if f.fs.journal != nil && (len(allocatedExtents) > 0 || newSize != ino.Size) {
		txn = f.fs.journal.Begin()

		// Log all extent allocations
		for _, alloc := range allocatedExtents {
			if err := txn.LogExtentAdd(&journal.ExtentAddPayload{
				InodeNum:       uint64(f.inodeNum),
				FileBlock:      alloc.logicalBlock,
				DiskBlock:      alloc.physicalBlock,
				Length:         alloc.count,
				MetaGeneration: ino.MetaGeneration,
				Size:           newSize,
			}); err != nil {
				logging.Debugf("Write inode %d: LogExtentAdd failed: %v", f.inodeNum, err)
				return 0, syscall.EIO
			}
		}

		if err := f.fs.chargeQuota(txn, ino.ProjectID, newAllocBlocks(chargedAllocs), 0); err != nil {
			return 0, syscall.EIO
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
			logging.Debugf("Write inode %d: LogInodeUpdate failed: %v", f.inodeNum, err)
			return 0, syscall.EIO
		}

		// === COMMIT POINT: After this, operation is guaranteed recoverable ===
		var commitErr error
		if len(reserveOut) > 0 {
			commitErr = f.fs.reserve.Logged(reserveOut, func() error {
				var err error
				intentSeq, err = f.fs.commit(ctx, txn)
				return err
			})
			reserveOut = nil
		} else {
			intentSeq, commitErr = f.fs.commit(ctx, txn)
		}
		if commitErr != nil {
			logging.Debugf("Write inode %d: journal commit failed: %v", f.inodeNum, commitErr)
			return 0, syscall.EIO
		}
		defer f.fs.closeIntent(intentSeq)
		txn = nil // Mark as committed so defer doesn't abort
		if afterCommit != nil {
			afterCommit("write")
		}
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

	// When the write fails, the blocks allocated since the last mid-write persist are
	// unreferenced. Close the intent first so journal replay cannot map them again,
	// then free them. Blocks the persist committed stay mapped on disk.
	unusedBlocks := func() []uint64 {
		var unused []uint64
		for _, alloc := range allocatedExtents[committedAllocs:] {
			for i := uint64(0); i < uint64(alloc.count); i++ {
				unused = append(unused, alloc.physicalBlock+i)
			}
		}
		for _, cow := range cowDone[committedCoW:] {
			unused = append(unused, cow.NewPhysBlock)
		}
		return unused
	}
	// freedAllocs counts the blocks among unused that this write allocated, which its charge counted.
	freedAllocs := func(unused []uint64) int64 {
		allocated := map[uint64]bool{}
		for _, alloc := range allocatedExtents[committedAllocs:] {
			for i := uint64(0); i < uint64(alloc.count); i++ {
				allocated[alloc.physicalBlock+i] = true
			}
		}
		var n int64
		for _, b := range unused {
			if allocated[b] {
				n++
			}
		}
		return n
	}
	writeFailed := func(err error) error {
		conflict := errors.Is(err, extent.ErrTreeConflict)
		var unused []uint64
		if !conflict {
			// Read while the inode lock still keeps other nodes from changing what it maps.
			unused = f.fs.unmapped(f.inodeNum, unusedBlocks())
		}
		// The bitmap lock comes before the inode lock, so free only after dropping it.
		releaseInodeLock()
		f.fs.extentMgr.DiscardPending(ctx, ino)
		if f.fs.journal != nil && intentSeq > 0 {
			if err := f.fs.journal.CompleteIntent(intentSeq); err != nil {
				logging.Debugf("Write inode %d: CompleteIntent after a failed write: %v", f.inodeNum, err)
				if conflict {
					return errRetryWrite
				}
				cowDone = cowDone[:committedCoW]
				return syscall.EIO
			}
		}
		if testHookBeforeFreeingUnused != nil {
			testHookBeforeFreeingUnused()
		}
		if !conflict {
			f.fs.freeUnused(ctx, f.inodeNum, unused...)
			f.fs.commitCharge(ino.ProjectID, -freedAllocs(unused), 0)
			cowDone = cowDone[:committedCoW]
			return syscall.EIO
		}
		for _, b := range unusedBlocks() {
			if err := f.fs.extentMgr.FreePhysicalBlock(ctx, b); err != nil {
				logging.Debugf("Write inode %d: freeing block %d after tree conflict failed: %v", f.inodeNum, b, err)
			}
		}
		if intentSeq > 0 {
			f.fs.commitCharge(ino.ProjectID, -newAllocBlocks(committedAllocs), 0)
		}
		// The retry finds these blocks already copied, so release the old ones now.
		releaseCopied(cowDone[:committedCoW])
		cowDone = cowDone[:committedCoW]
		return errRetryWrite
	}

	// A peer resized the file after growthSegments chose what to zero; the retry zeroes from the new size.
	if onDisk < writeStart && onDisk != oldEnd && onDisk%f.fs.blockSize != 0 {
		return 0, writeFailed(fmt.Errorf("%w: inode %d is %d bytes, not %d as the write planned", extent.ErrTreeConflict, f.inodeNum, onDisk, oldEnd))
	}

	if testHookBeforeInodeCommit != nil {
		if err := testHookBeforeInodeCommit(); err != nil {
			return 0, writeFailed(err)
		}
	}
	// Write inode - use WriteWithLockHeld if we hold the lock
	copiesMayBeOnDisk = true
	if lockHeld {
		if err := f.fs.inodeTbl.WriteWithLockHeld(ino); err != nil {
			logging.Debugf("Write inode %d: WriteWithLockHeld failed: %v", f.inodeNum, err)
			return 0, writeFailed(err)
		}
	} else {
		if err := f.fs.inodeTbl.Write(ctx, ino); err != nil {
			logging.Debugf("Write inode %d: final inodeTbl.Write failed: %v", f.inodeNum, err)
			return 0, writeFailed(err)
		}
	}

	inodeWritten = true

	releaseInodeLock()

	releaseCopied(cowDone)

	f.wrote(ino, start)

	// === WAL: Mark intent complete ===
	// Now that all metadata and data have been persisted, mark the intent as complete.
	// On recovery, this prevents replaying already-completed operations.
	if f.fs.journal != nil && intentSeq > 0 {
		if err := f.fs.journal.CompleteIntent(intentSeq); err != nil {
			logging.Debugf("Write inode %d: CompleteIntent failed: %v", f.inodeNum, err)
		}
	}

	return requested, nil
}

type writeSegment struct {
	off  uint64
	data []byte
}

// growthSegments returns what a write must put on disk. Past the end of the file, the rest of its last block may hold
// old bytes, from before a truncate or a disk block's earlier owner; a write that grows the file over them zeroes them
// first, through the same path, so a block shared with a snapshot or clone is copied rather than changed.
func (f *File) growthSegments(ino *inode.Inode, data []byte, offset uint64) ([]writeSegment, error) {
	end := ino.Size
	if offset <= end || end%f.fs.blockSize == 0 || ino.IsSFBBacked() {
		return []writeSegment{{offset, data}}, nil
	}
	ext, _, err := f.fs.extentMgr.Lookup(ino, end/f.fs.blockSize)
	switch {
	case errors.Is(err, extent.ErrStaleTree):
		return nil, errRetryWrite
	case errors.Is(err, extent.ErrExtentNotFound), err == nil && ext.Flags&extent.FlagUnwritten != 0:
		return []writeSegment{{offset, data}}, nil
	case err != nil:
		return nil, syscall.EIO
	}
	gapEnd := min(offset, (end/f.fs.blockSize+1)*f.fs.blockSize)
	zeros := make([]byte, gapEnd-end)
	if gapEnd == offset {
		return []writeSegment{{end, append(zeros, data...)}}, nil
	}
	return []writeSegment{{end, zeros}, {offset, data}}, nil
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
	return int64(f.dataOffset + container*sfb.BlockSize + SFBHeaderSize + uint64(slot)*uint64(slotSize)), nil
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

func (f *FS) freeSFBSlot(ctx context.Context, packed uint64) {
	FreeSFBSlot(ctx, f.dev, f.lockMgr, f.dataOffset, packed)
}

// FreeSFBSlot clears a slot's bit in its container header. A failure leaks the
// slot, which is safe because nothing allocates SFB slots any more.
func FreeSFBSlot(ctx context.Context, dev *storage.Device, lockMgr *lock.Manager, dataOffset, packed uint64) {
	container, slot := sfb.UnpackPhysicalBlock(packed)
	if lockMgr != nil {
		if err := lockMgr.Acquire(ctx, lock.LockTypeSFBContainer, container, lock.LockModeExclusive); err != nil {
			log.Printf("[WARN] Free SFB slot %d:%d: container lock failed: %v", container, slot, err)
			return
		}
		defer func() {
			if err := lockMgr.Release(lock.LockTypeSFBContainer, container); err != nil {
				log.Printf("[WARN] Free SFB slot %d:%d: releasing the container lock failed: %v", container, slot, err)
			}
		}()
	}

	buf := storage.GetBuffer(storage.SectorSize)
	defer storage.PutBuffer(buf)
	sector := buf.Bytes()[:storage.SectorSize]
	off := int64(dataOffset + container*sfb.BlockSize)
	if _, err := dev.ReadAt(sector, off); err != nil {
		log.Printf("[WARN] Free SFB slot %d:%d: reading container header failed: %v", container, slot, err)
		return
	}
	h, err := sfb.DeserializeHeader(sector)
	if err != nil || !bytes.Equal(h.Serialize(), sector[:sfb.HeaderSize]) || slot >= min(h.SlotCount, uint16(8*len(h.Bitmap))) {
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
	if _, err := dev.WriteAt(sector, off); err != nil {
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

	buf := storage.GetBuffer(int(f.fs.blockSize))
	copy(buf.Bytes(), data)
	_, err = f.fs.dev.WriteAt(buf.Bytes()[:f.fs.blockSize], int64(f.fs.dataOffset+newBlock*f.fs.blockSize))
	storage.PutBuffer(buf)
	if err != nil {
		f.fs.freeUnused(ctx, f.inodeNum, newBlock)
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
			f.fs.freeUnused(ctx, f.inodeNum, newBlock)
			return err
		}
		// === COMMIT POINT ===
		// Inside a write this takes one of the write's admitted rooms, so its main record may commit unadmitted.
		var commitErr error
		intentSeq, commitErr = f.fs.commit(ctx, txn)
		if commitErr != nil {
			f.fs.freeUnused(ctx, f.inodeNum, newBlock)
			return commitErr
		}
		defer f.fs.closeIntent(intentSeq)
	}

	ino.InlineExtents[0] = inode.InlineExtent{PhysicalBlock: newBlock, Length: 1, Flags: extent.FlagAllocated}
	ino.AllocatedBlocks = 1
	ino.WrittenBlocks = 1
	ino.Mtime = time.Now().UnixNano()

	if err := f.fs.inodeTbl.Write(ctx, ino); err != nil {
		// Only a conflict proves the inode on disk does not map newBlock.
		if errors.Is(err, extent.ErrTreeConflict) {
			f.fs.freeUnused(ctx, f.inodeNum, newBlock)
		}
		return retryOnTreeConflict(err)
	}

	f.fs.freeSFBSlot(ctx, oldSlot)

	// === WAL: Mark intent complete ===
	if f.fs.journal != nil && intentSeq > 0 {
		if err := f.fs.journal.CompleteIntent(intentSeq); err != nil {
			logging.Debugf("migrateToExtent inode %d: CompleteIntent failed: %v", f.inodeNum, err)
		}
	}

	log.Printf("[INFO] Migrated inode %d from SFB (%d:%d) to extent (block %d)",
		f.inodeNum, oldContainer, oldSlotIndex, newBlock)

	return nil
}

// Setattr sets file attributes (used for truncate, chmod, etc.).
func (f *File) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	ctx = detach(ctx)
	if _, resizing := in.GetSize(); resizing {
		// Room for the resize's record, and for a write's when it grows the file.
		var release func()
		ctx, release = f.fs.admitOp(ctx, freeRecordBound, f.fs.writeRecordBounds(0)[0])
		defer release()
		unlockIO, err := f.fs.lockFileIO(ctx, f.inodeNum, lock.LockModeExclusive)
		if err != nil {
			log.Printf("[ERROR] Setattr inode %d: %v", f.inodeNum, err)
			return errToErrno(err)
		}
		defer unlockIO()
	}
	return f.setattrIOLocked(ctx, fh, in, out)
}

// setattrIOLocked is Setattr for a caller that holds the file's I/O lock when in changes the size.
func (f *File) setattrIOLocked(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	size, resizing := in.GetSize()
	for attempt := 0; ; attempt++ {
		unlock := f.fs.lockInode(f.inodeNum)
		errno, err := f.growTo(ctx, size, resizing)
		if err == nil && errno == 0 {
			errno, err = f.setattr(ctx, fh, in, out)
		}
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

// growTo grows the file to size, if that is larger, as an empty write at size: it zeroes the old bytes the rest of
// the last block may hold before the size covers them. The caller holds the inode lock.
func (f *File) growTo(ctx context.Context, size uint64, resizing bool) (syscall.Errno, error) {
	if !resizing {
		return 0, nil
	}
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		return syscall.EIO, nil
	}
	if size <= ino.Size {
		return 0, nil
	}
	if _, err := f.writeInternal(ctx, nil, size); err != nil {
		if err == errRetryWrite {
			f.readFrom.Store(nil)
			return 0, err
		}
		return errToErrno(err), nil
	}
	return 0, nil
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
	if !f.current(ino) {
		return syscall.ESTALE, nil
	}
	if testHookAfterSetattrRead != nil {
		testHookAfterSetattrRead()
	}

	if _, resizing := in.GetSize(); resizing && ino.IsSFBBacked() {
		if err := f.migrateToExtent(ctx, ino); err != nil {
			logging.Debugf("Setattr inode %d: SFB migration failed: %v", f.inodeNum, err)
			if err == errRetryWrite {
				return 0, err
			}
			return syscall.EIO, nil
		}
		if ino, err = f.fs.inodeTbl.Read(f.inodeNum); err != nil {
			return syscall.EIO, nil
		}
	}
	readGen := ino.MetaGeneration

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
			oldBlocks := (ino.Size + f.fs.blockSize - 1) / f.fs.blockSize
			newBlocks := (sz + f.fs.blockSize - 1) / f.fs.blockSize
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
	var cutBlocks int64
	// A truncate that failed after its commit gives back its charge after the deferred closeIntent.
	truncateFailed := false
	defer func() {
		if truncateFailed {
			f.fs.commitCharge(ino.ProjectID, cutBlocks, 0)
		}
	}()
	if f.fs.journal != nil && mask != 0 {
		txn = f.fs.journal.Begin()

		if truncating && blocksToFree > 0 {
			newSize, _ := in.GetSize()
			var err error
			if cutBlocks, err = f.fs.logExtentFree(txn, ino, newSize); err != nil {
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
		intentSeq, commitErr = f.fs.commit(ctx, txn)
		if commitErr != nil {
			return syscall.EIO, nil
		}
		defer f.fs.closeIntent(intentSeq)
		if afterCommit != nil {
			afterCommit("setattr")
		}
	}
	// === End WAL transaction ===

	// Now execute the actual operations (safe - recoverable if crash)
	if sz, ok := in.GetSize(); ok {
		if sz < ino.Size {
			// Truncate
			if err := f.fs.extentMgr.Truncate(ctx, ino, sz); err != nil {
				truncateFailed = true
				return syscall.EIO, nil
			}
		}
		ino.Size = sz
		ino.Mtime = now
	}

	if mode, ok := in.GetMode(); ok {
		ino.Mode = inode.S_IFREG | uint16(mode&0o7777)
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

	// Another node's write, such as an append inside an allocated block, may have changed the size since ino was read.
	if err := f.fs.inodeTbl.WriteIfUnchanged(ctx, ino, readGen); err != nil {
		f.fs.extentMgr.DiscardPending(ctx, ino)
		if errors.Is(err, extent.ErrTreeConflict) {
			// Nothing was applied, so close the intent before the retry logs a new one.
			if f.fs.journal != nil && intentSeq > 0 {
				if err := f.fs.journal.CompleteIntent(intentSeq); err != nil {
					log.Printf("[WARN] Setattr inode %d: closing intent %d before the retry failed: %v", f.inodeNum, intentSeq, err)
				}
				f.fs.commitCharge(ino.ProjectID, cutBlocks, 0)
			}
			return 0, err
		}
		return syscall.EIO, nil
	}

	if f.fs.cacheMgr != nil {
		f.fs.cacheMgr.BroadcastInvalidateInode(f.inodeNum, ino.DataGeneration, ino.MetaGeneration)
	}
	f.fs.cacheInode(ino)

	// === WAL: Mark intent complete ===
	if f.fs.journal != nil && intentSeq > 0 {
		if err := f.fs.journal.CompleteIntent(intentSeq); err != nil {
			logging.Debugf("Setattr inode %d: CompleteIntent failed: %v", f.inodeNum, err)
		}
	}

	return f.Getattr(ctx, fh, out), nil
}

// Fsync flushes file data to disk.
func (f *File) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	return f.fs.syncDevice()
}

// Fsync makes the directory's entries durable, as an application's fsync of a directory after a create or unlink
// expects. Without a journal, or with an async one, nothing else flushes them.
func (d *Dir) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	return d.fs.syncDevice()
}

// Fsync makes the root directory's entries durable.
func (f *FS) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	return f.syncDevice()
}

// syncDevice flushes every completed write, including those in the device's write cache.
func (f *FS) syncDevice() syscall.Errno {
	if err := f.dev.Sync(); err != nil {
		return syscall.EIO
	}
	return 0
}

// Allocate pre-allocates space for the file using unwritten (lazy-zero) extents.
// Supports mode==0 (allocate and extend size) and FALLOC_FL_KEEP_SIZE (allocate only).
// PUNCH_HOLE and ZERO_RANGE are not yet implemented.
func (f *File) Allocate(ctx context.Context, fh fs.FileHandle, off uint64, size uint64, mode uint32) syscall.Errno {
	ctx = detach(ctx)
	// Only support mode 0 (allocate) and KEEP_SIZE for now
	const FALLOC_FL_KEEP_SIZE = 0x01
	if mode != 0 && mode != FALLOC_FL_KEEP_SIZE {
		// PUNCH_HOLE and ZERO_RANGE not yet implemented
		logging.Debugf("Allocate inode %d: unsupported mode %d", f.inodeNum, mode)
		return syscall.EOPNOTSUPP
	}
	unlockIO, err := f.fs.lockFileIO(ctx, f.inodeNum, lock.LockModeExclusive)
	if err != nil {
		log.Printf("[ERROR] Allocate inode %d: %v", f.inodeNum, err)
		return errToErrno(err)
	}
	defer unlockIO()

	for attempt := 0; ; attempt++ {
		unlock := f.fs.lockInode(f.inodeNum)
		errno, err := f.growTo(ctx, off+size, mode == 0)
		if err == nil && errno == 0 {
			errno, err = f.allocate(ctx, off, size, mode)
		}
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
		logging.Debugf("Allocate inode %d: read failed: %v", f.inodeNum, err)
		return syscall.EIO, nil
	}
	if !f.current(ino) {
		return syscall.ESTALE, nil
	}

	// Until the inode write commits them, blocks allocated here belong to nobody.
	var newBlocks []uint64
	committed := false
	defer func() {
		if !committed {
			f.fs.extentMgr.DiscardPending(ctx, ino)
			f.fs.freeUnused(ctx, f.inodeNum, newBlocks...)
		}
	}()

	// Note: We don't hold the inode lock during block allocation to respect lock ordering.
	// Lock order: bitmap (6) must be acquired before inode (8).
	// Fallocate acquires the bitmap lock internally.
	// We acquire the inode lock only at the end to update metadata atomically.

	// Calculate block range
	startBlock := off / f.fs.blockSize
	endOffset := off + size
	endBlock := (endOffset + f.fs.blockSize - 1) / f.fs.blockSize // Round up
	totalBlocks := endBlock - startBlock

	logging.Debugf("Allocate inode %d: offset=%d length=%d blocks=%d-%d (total=%d)",
		f.inodeNum, off, size, startBlock, endBlock-1, totalBlocks)

	if errno := f.checkMapQuota(ctx, off, endOffset); errno != 0 {
		return errno, nil
	}
	var allocErr error
	newBlocks, allocErr = f.fs.extentMgr.Fallocate(ctx, ino, startBlock, endBlock)
	if allocErr != nil {
		logging.Debugf("Allocate inode %d: blocks %d-%d: %v", f.inodeNum, startBlock, endBlock-1, allocErr)
		if errors.Is(allocErr, extent.ErrExtentTreeFull) {
			return syscall.EFBIG, nil
		}
		return allocErrno(allocErr), nil
	}

	// Acquire inode lock for metadata update
	if f.fs.lockMgr != nil {
		if err := f.fs.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
			logging.Debugf("Allocate inode %d: lock failed: %v", f.inodeNum, err)
			return syscall.EIO, nil
		}
		defer f.fs.release(lock.LockTypeInode, uint64(f.inodeNum), "inode")

		// ino holds the new extents, so keep it only if no node wrote the inode since it was read.
		fresh, err := f.fs.inodeTbl.Read(f.inodeNum)
		if err != nil {
			logging.Debugf("Allocate inode %d: re-read failed: %v", f.inodeNum, err)
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

	// No record maps the new blocks, so their charge commits before the write that does.
	f.fs.commitCharge(ino.ProjectID, int64(len(newBlocks)), 0)
	if err := f.fs.inodeTbl.Write(ctx, ino); err != nil {
		logging.Debugf("Allocate inode %d: write inode failed: %v", f.inodeNum, err)
		if errors.Is(err, extent.ErrTreeConflict) {
			f.fs.commitCharge(ino.ProjectID, -int64(len(newBlocks)), 0)
			return 0, err
		}
		committed = true // the inode on disk may already map newBlocks
		return syscall.EIO, nil
	}
	committed = true
	f.fs.cacheInode(ino)

	logging.Debugf("Allocate inode %d: success, AllocatedBlocks=%d WrittenBlocks=%d Size=%d",
		f.inodeNum, ino.AllocatedBlocks, ino.WrittenBlocks, ino.Size)

	return 0, nil
}

// Setxattr sets an extended attribute on the file.
// Used for snapshot creation via user.snapshot attribute and clone creation via user.clone.
func (f *File) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	return f.setxattr(ctx, attr, data)
}

// setxattr runs a snapshot or clone attribute, for Setxattr and for the HTTP API, which calls it without the kernel.
func (f *File) setxattr(ctx context.Context, attr string, data []byte) syscall.Errno {
	ctx = detach(ctx)
	if attr == "user.snapshot" || attr == "user.clone" {
		unlockIO, err := f.fs.lockFileIO(ctx, f.inodeNum, lock.LockModeShared)
		if err != nil {
			log.Printf("[ERROR] Setxattr %s inode %d: %v", attr, f.inodeNum, err)
			return errToErrno(err)
		}
		defer unlockIO()
	}
	defer f.fs.lockInode(f.inodeNum)()
	// A snapshot or clone spans several writes; an interrupted caller must not stop it halfway.
	ctx = context.WithoutCancel(ctx)
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	switch {
	case err != nil:
		return syscall.EIO
	case !f.current(ino):
		return syscall.ESTALE
	case ino.LinkCount == 0 && attr != "user.snapshot.delete":
		return syscall.ENOENT
	}
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
	ctx = detach(ctx)
	logging.Debugf("File.Getxattr: inode=%d attr=%s", f.inodeNum, attr)
	// The kernel asks for security.capability before every write.
	if attr != "user.snapshots" {
		return 0, syscall.ENODATA
	}
	if errno := f.stillCurrent(); errno != 0 {
		return 0, errno
	}
	return f.listSnapshots(ctx, dest)
}

// Removexattr removes an extended attribute from the file.
// Used for snapshot deletion via user.snapshot.<name> pattern.
func (f *File) Removexattr(ctx context.Context, attr string) syscall.Errno {
	ctx = detach(ctx)
	logging.Debugf("File.Removexattr: inode=%d attr=%s", f.inodeNum, attr)
	if errno := f.stillCurrent(); errno != 0 {
		return errno
	}

	// Handle user.snapshot.<name> for snapshot deletion
	const snapshotPrefix = "user.snapshot."
	if strings.HasPrefix(attr, snapshotPrefix) {
		snapName := strings.TrimPrefix(attr, snapshotPrefix)
		if snapName == "" {
			return syscall.EINVAL
		}
		return f.deleteSnapshot(context.WithoutCancel(ctx), snapName)
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

// errSharingBusy means writes kept moving the file's blocks while a snapshot
// or clone tried to share them.
var errSharingBusy = errors.New("the file kept changing while it was being shared")

// errUncountedSharing means the file shares blocks the way older versions did,
// without refcounts, so its blocks cannot be shared again safely.
var errUncountedSharing = errors.New("an older version shared this file's blocks without refcounts; run goclusterfs-fsck, and copy the file to snapshot or clone it")

// shareSource counts a new owner of each block of src, then marks src shared
// under the exclusive inode lock. A writer checks a block and writes it in
// place under the shared inode lock, so the write lands before the mark, or
// sees the count and copies the block. If a write copied a block between the
// count and the mark, the counts are undone and the sharing tried again.
func (f *FS) shareSource(ctx context.Context, srcNum uint32) (*inode.Inode, []uint64, error) {
	for attempt := 0; ; attempt++ {
		src, err := f.inodeTbl.Read(srcNum)
		if err != nil {
			return nil, nil, err
		}
		uncounted, err := f.extentMgr.HasUncountedSharing(src)
		if err != nil {
			return nil, nil, err
		}
		if uncounted {
			return nil, nil, errUncountedSharing
		}
		blocks, err := f.extentMgr.DataBlocks(src)
		if err != nil {
			return nil, nil, err
		}
		// Refuse before marking src shared, so its writes stay off the copy-on-write path.
		if err := f.refcountReg.CheckCovered(blocks); err != nil {
			return nil, nil, err
		}
		counted, err := f.refcountReg.Share(ctx, blocks)
		if err != nil {
			return nil, nil, err
		}
		marked, err := src, error(nil)
		// A snapshot's data inode is marked shared when it is created and never written again.
		if src.Flags&inode.FlagSnapshotData == 0 {
			if beforeShareMark != nil {
				beforeShareMark()
			}
			// Setxattr holds lockInode(srcNum).
			marked, err = f.updateUnderInodeLock(ctx, srcNum, func(ino *inode.Inode) error {
				if duringShareMark != nil {
					duringShareMark()
				}
				return f.markShared(ino)
			})
		}
		var now []uint64
		if err == nil {
			now, err = f.extentMgr.DataBlocks(marked)
		}
		// Share skips blocks already freed, so a file that lost blocks is shared again.
		if err == nil && slices.Equal(now, blocks) && len(counted) == len(blocks) {
			return marked, counted, nil
		}
		f.extentMgr.ReleaseBlocks(ctx, counted)
		if err != nil {
			return nil, nil, err
		}
		if attempt == maxConflictRetries-1 {
			return nil, nil, fmt.Errorf("%w: inode %d", errSharingBusy, srcNum)
		}
		conflictBackoff(attempt)
	}
}

// shareInto gives dst the extents of inode srcNum, sharing its data blocks.
// Each block gains an owner, durably, before dst maps it; a crash in between
// leaves blocks counted with an owner too many, which fsck rebuilds.
// stillValid, if set, runs after the blocks gain owners and before dst maps
// them. On error the caller discards dst, which gives back any blocks it maps.
func (f *FS) shareInto(ctx context.Context, srcNum uint32, dst *inode.Inode, stillValid func() error) error {
	src, blocks, err := f.shareSource(ctx, srcNum)
	if err != nil {
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
	// No record maps the shared blocks into dst, so their charge commits before the write that does.
	shared := int64(dst.AllocatedBlocks)
	f.commitCharge(dst.ProjectID, shared, 0)
	if err := f.inodeTbl.Write(ctx, dst); err != nil {
		f.extentMgr.DiscardPending(ctx, dst)
		// Only a conflict proves dst on disk does not map the blocks.
		if errors.Is(err, extent.ErrTreeConflict) {
			f.extentMgr.ReleaseBlocks(ctx, blocks)
			f.commitCharge(dst.ProjectID, -shared, 0)
		}
		return err
	}
	return nil
}

// sharingErrno maps a failure to share a file's blocks to an errno.
func sharingErrno(err error) syscall.Errno {
	switch {
	case errors.Is(err, errUncountedSharing), errors.Is(err, refcount.ErrBlockPastRegion):
		return syscall.ENOTSUP
	case errors.Is(err, errSharingBusy):
		return syscall.EAGAIN
	}
	return syscall.EIO
}

// claimInode allocates an inode slot, retrying when another node takes the
// free slot first.
func (f *FS) claimInode(ctx context.Context, init func(*inode.Inode)) (*inode.Inode, error) {
	for attempt := 0; ; attempt++ {
		num, err := f.inodeTbl.FindFreeSlot()
		if err != nil {
			return nil, err
		}
		ino, err := f.inodeTbl.ClaimSlotAs(ctx, num, init)
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
		logging.Debugf("createSnapshot: snapshot subsystem not initialized")
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
	target, err := f.fs.snapshotTarget(ctx, src)
	if err != nil {
		log.Printf("[snapshot] Failed to record the birth generation of inode %d: %v", f.inodeNum, err)
		return syscall.EIO
	}

	// The inode lock is held, so a snapshot near its project's limit cannot flush first.
	if errno := f.fs.checkQuota(ctx, src.ProjectID, int64(src.AllocatedBlocks), 1, false); errno != 0 {
		return errno
	}
	// No record makes the data inode, so its charge commits before the claim; the delete job gives it back.
	f.fs.commitCharge(src.ProjectID, 0, 1)
	data, err := f.fs.claimInode(ctx, func(ino *inode.Inode) {
		ino.Mode, ino.UID, ino.GID = src.Mode, src.UID, src.GID
		ino.Mtime, ino.Ctime = src.Mtime, src.Ctime
		ino.Flags |= inode.FlagSnapshotData
		ino.ProjectID = src.ProjectID
	})
	if err != nil {
		log.Printf("[snapshot] No inode for the data of a snapshot of inode %d: %v", f.inodeNum, err)
		f.fs.commitCharge(src.ProjectID, 0, -1)
		return claimErrno(err)
	}

	desc, err := f.fs.snapshotReg.CreateFor(ctx, name, target, 0,
		data.InodeNum, src.AllocatedBlocks, src.Size, snapshot.FlagReadOnly)
	if err != nil {
		log.Printf("[snapshot] Failed to add '%s' to the registry: %v", name, err)
		if f.fs.freeNewInode(ctx, data.InodeNum) {
			f.fs.commitCharge(src.ProjectID, 0, -1)
		}
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
		if beforeActivate != nil {
			beforeActivate()
		}
		err = f.fs.snapshotReg.Activate(ctx, desc.SnapshotID)
	}
	if errors.Is(err, snapshot.ErrSnapshotInvalidState) || errors.Is(err, snapshot.ErrSnapshotNotFound) {
		log.Printf("[snapshot] Another node rolled back '%s' (ID=%d) of inode %d before it became active, and deletes it", name, desc.SnapshotID, f.inodeNum)
		return syscall.EAGAIN
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

	if errno := f.ownsSnapshot(desc); errno != 0 {
		return errno
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
	if f.fs.crossesProject(srcIno, rootIno) {
		log.Printf("[clone] Inode %d counts against quota project %d, and its clone would land outside it", srcNum, srcIno.ProjectID)
		return syscall.EXDEV
	}

	mode, uid, gid := srcIno.Mode, srcIno.UID, srcIno.GID
	if caller, ok := fuse.FromContext(ctx); ok && caller.Uid != 0 {
		// The kernel checked only write access to this file; a snapshot's data inode keeps the file's old mode.
		liveIno := srcIno
		if srcNum != f.inodeNum {
			if liveIno, err = f.fs.inodeTbl.Read(f.inodeNum); err != nil {
				return syscall.EIO
			}
		}
		uid, gid, _ = f.fs.newOwner(ctx, rootIno)
		if !mayAccess(srcIno, uid, gid, 4) || !mayAccess(liveIno, uid, gid, 4) || !mayAccess(rootIno, uid, gid, 3) {
			return syscall.EACCES
		}
		mode &^= inode.S_ISUID | inode.S_ISGID
	}

	now := time.Now().UnixNano()
	cloneIno, err := f.fs.claimInode(ctx, func(ino *inode.Inode) {
		ino.Mode, ino.UID, ino.GID = mode, uid, gid
		ino.Atime, ino.Mtime, ino.Ctime = now, now, now
	})
	if err != nil {
		log.Printf("[clone] Failed to claim an inode slot: %v", err)
		return claimErrno(err)
	}
	cloneInodeNum := cloneIno.InodeNum
	cloneIno.LinkCount = 1
	cloneIno.Flags |= inode.FlagHasSnapshot

	if err := f.fs.shareInto(ctx, srcNum, cloneIno, stillValid); err != nil {
		f.fs.discardInode(ctx, cloneInodeNum)
		log.Printf("[clone] Failed to share blocks of inode %d: %v", srcNum, err)
		return sharingErrno(err)
	}
	if err := f.fs.dirMgr.Add(ctx, rootIno, cloneName, uint64(cloneInodeNum), dir.DT_REG); err != nil {
		f.fs.discardInode(ctx, cloneInodeNum)
		log.Printf("[clone] Failed to add directory entry: %v", err)
		return nameErrno(err)
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

	if errno := f.ownsSnapshot(desc); errno != 0 {
		log.Printf("[clone] Snapshot '%s' was not taken of inode %d", snapName, f.inodeNum)
		return errno
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

	snapshots, err := f.fs.FileSnapshots(ctx, f.inodeNum)
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

// ownsSnapshot returns ENOENT unless desc was taken of this file, not of an earlier file in the same inode slot.
func (f *File) ownsSnapshot(desc *snapshot.Descriptor) syscall.Errno {
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		return syscall.EIO
	}
	if !desc.BelongsTo(snapshot.Target{Inode: f.inodeNum, Birth: ino.BirthGeneration}) {
		return syscall.ENOENT
	}
	return 0
}

// snapshotTarget returns what a snapshot of ino records about its file. An inode older versions wrote gets its birth
// generation now: the generation this write gives it, which no later file in the slot can have.
func (f *FS) snapshotTarget(ctx context.Context, ino *inode.Inode) (snapshot.Target, error) {
	if ino.BirthGeneration == 0 {
		updated, err := f.updateInode(ctx, ino.InodeNum, func(i *inode.Inode) error {
			if i.BirthGeneration == 0 {
				i.BirthGeneration = i.MetaGeneration + 1
			}
			return nil
		})
		if err != nil {
			return snapshot.Target{}, err
		}
		ino = updated
	}
	return snapshot.Target{Inode: ino.InodeNum, Birth: ino.BirthGeneration}, nil
}

// FileSnapshots returns the snapshots of the file in inode slot num.
func (f *FS) FileSnapshots(ctx context.Context, num uint32) ([]*snapshot.Descriptor, error) {
	ino, err := f.inodeTbl.Read(num)
	if err != nil {
		return nil, err
	}
	return f.snapshotReg.ListForTarget(ctx, snapshot.Target{Inode: num, Birth: ino.BirthGeneration})
}

// AllSnapshots returns every snapshot, marking those whose file is gone or cannot be proven.
func (f *FS) AllSnapshots(ctx context.Context) ([]snapshot.Entry, error) {
	descs, err := f.snapshotReg.List(ctx)
	if err != nil {
		return nil, err
	}
	entries := make([]snapshot.Entry, 0, len(descs))
	for _, d := range descs {
		orphan, err := f.snapshotOrphan(d)
		if err != nil {
			return nil, err
		}
		entries = append(entries, snapshot.Entry{Descriptor: d, Orphan: orphan})
	}
	return entries, nil
}

// DeleteOrphanSnapshot deletes a snapshot whose file is gone. A snapshot of an existing file is deleted through it.
func (f *FS) DeleteOrphanSnapshot(ctx context.Context, name string) error {
	desc, err := f.snapshotReg.GetByName(ctx, name)
	if errors.Is(err, snapshot.ErrSnapshotNotFound) {
		return fmt.Errorf("snapshot %q: %w", name, syscall.ENOENT)
	}
	if err != nil {
		return err
	}
	orphan, err := f.snapshotOrphan(desc)
	if err != nil {
		return err
	}
	if !orphan {
		return fmt.Errorf("snapshot %q belongs to inode %d, so delete it through that file: %w", name, desc.TargetInode, syscall.EBUSY)
	}
	log.Printf("[snapshot] Deleting orphan '%s' (ID=%d) of inode %d", name, desc.SnapshotID, desc.TargetInode)
	return f.dropSnapshot(ctx, desc.SnapshotID)
}

// snapshotOrphan reports whether d's file has no name left to reach it through:
// it was freed, its slot holds a later file, or it was unlinked while a node
// still holds it open.
func (f *FS) snapshotOrphan(d *snapshot.Descriptor) (bool, error) {
	ino, err := f.inodeTbl.Read(d.TargetInode)
	if errors.Is(err, inode.ErrInodeOutOfRange) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return ino.Magic == 0 || ino.LinkCount == 0 || !d.BelongsTo(snapshot.Target{Inode: d.TargetInode, Birth: ino.BirthGeneration}), nil
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
