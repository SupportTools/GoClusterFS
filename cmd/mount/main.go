// Command mount mounts a GoClusterFS filesystem.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/cache"
	"github.com/supporttools/GoClusterFS/internal/cache/broadcast"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/goclusterfs"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/sfb"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
	"github.com/supporttools/GoClusterFS/internal/trim"
)

func main() {
	var (
		devicePath = flag.String("device", "", "Path to block device or image file")
		mountPoint = flag.String("mount", "", "Mount point directory")
		format     = flag.Bool("format", false, "Format the device (WARNING: destroys data)")
		inodeRatio = flag.Uint64("bytes-per-inode", superblock.DefaultBytesPerInode,
			fmt.Sprintf("With -format: one inode per this many bytes of device (%d to %d inodes)", superblock.MinInodes, superblock.MaxInodes))
		debug = flag.Bool("debug", false, "Enable FUSE debug output")

		// Cache configuration
		cacheStrict = flag.Bool("cache-strict", false, "Enable strict cache mode (always validate)")
		cacheTTL    = flag.Duration("cache-ttl", 5*time.Second, "Cache TTL before validation")
		attrTimeout = flag.Duration("attr-timeout", time.Second, "How long the kernel caches file attributes and directory entries")

		// Journal configuration
		journalEnabled = flag.Bool("journal", true, "Enable write-ahead journal for crash consistency")
		skipRecovery   = flag.Bool("skip-recovery", false, "Skip journal recovery (emergency maintenance only)")

		// Network broadcast configuration
		broadcastEnabled = flag.Bool("broadcast", true, "Enable network cache invalidation broadcast")
		multicastGroup   = flag.String("mcast-group", "239.192.74.75", "Multicast group address")
		multicastPort    = flag.Int("mcast-port", 31337, "Multicast/unicast port")
		broadcastSecret  = flag.String("broadcast-secret", "", "Shared secret for HMAC authentication")
		broadcastIface   = flag.String("broadcast-iface", "", "Network interface for multicast")

		// TRIM configuration
		trimEnabled   = flag.Bool("trim", true, "Enable batched TRIM for freed blocks")
		trimBatchSize = flag.Int("trim-batch", 256, "TRIM batch size (blocks before flush)")
		trimMaxAge    = flag.Duration("trim-age", 5*time.Second, "Max age before TRIM flush")

		// SFB configuration (Sub-File Block containers for small files)
		sfbEnabled = flag.Bool("sfb", true, "Enable SFB containers for small files (<64KB)")

		// Snapshot configuration
		snapshotsEnabled = flag.Bool("snapshots", true, "Enable O(1) file snapshots via xattr")
	)
	flag.Parse()

	if *devicePath == "" || *mountPoint == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -device <path> -mount <mountpoint> [-format] [-debug]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Check mount point exists
	if _, err := os.Stat(*mountPoint); os.IsNotExist(err) {
		log.Fatalf("Mount point does not exist: %s", *mountPoint)
	}

	// Open device
	dev, err := storage.Open(*devicePath)
	if err != nil {
		log.Fatalf("Failed to open device: %v", err)
	}
	defer dev.Close()

	// Get device size (returns int64, no error)
	devSize := dev.Size()

	log.Printf("Device: %s (%d MB)", *devicePath, devSize/(1024*1024))

	var sb *superblock.Superblock
	if *format {
		sb, err = superblock.Format(dev, *inodeRatio)
	} else {
		sb, err = superblock.Read(dev)
	}
	if err != nil {
		log.Fatalf("Cannot use %s: %v", *devicePath, err)
	}

	bitmapOffset := sb.BlockBitmapOffset
	inodeOffset := sb.InodeTableOffset
	nodeRegistryOffset := sb.NodeRegistryOffset
	lockOffset, lockSize := sb.LockBitmapOffset, sb.LockBitmapSize
	refcountOffset := sb.RefCountTableOffset
	snapshotOffset, snapshotSize := sb.SnapshotRegOffset, sb.SnapshotRegSize
	dataOffset := sb.DataRegionOffset
	totalBlocks := sb.TotalBlocks
	inodeCount := sb.InodeCount

	log.Printf("Layout: v%d, %d data blocks (%d MB), %d inodes, %d node slots",
		sb.Version, totalBlocks, totalBlocks, inodeCount, sb.MaxNodes)

	ctx := context.Background()

	// Create node registry for distributed coordination
	nodeReg := node.NewRegistry(dev, nodeRegistryOffset)
	nodeReg.SetJournalArea(sb.JournalRegionOffset, sb.JournalSlotSize())
	hostname, _ := os.Hostname()
	slotIdx, err := nodeReg.Register(ctx, hostname)
	if err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}
	nodeReg.SetSlotLostHandler(func(slot int, owner [16]byte) {
		log.Fatalf("Node registry slot %d is no longer this node's (owner %x); stopping so two nodes never share a journal", slot, owner[:8])
	})
	if err := nodeReg.StartHeartbeat(); err != nil {
		log.Fatalf("Failed to start node heartbeat: %v", err)
	}
	nodeUUID := nodeReg.LocalUUID()
	log.Printf("Registered as node %d (UUID: %x)", slotIdx, nodeUUID[:8])
	journalOffset, journalSize := nodeReg.LocalJournal()

	// Create lock manager for distributed locking
	lockMgr := lock.NewManager(dev, lockOffset, lockSize, nodeUUID, slotIdx)
	nodeReg.SetOwnerDeadHandler(func(owner [16]byte) func() { return adoptLocks(lockMgr, owner) })
	releasePrevious := func() {}
	if owner, ok := nodeReg.DeadPreviousOwner(); ok {
		releasePrevious = adoptLocks(lockMgr, owner)
	}

	// Create managers with distributed locking enabled
	// NOTE: Managers must be created BEFORE journal recovery so we can replay transactions
	bm := bitmap.NewRegion(dev, bitmapOffset, totalBlocks, lockMgr, nodeUUID)
	inodeTbl := inode.NewTable(dev, inodeOffset, inodeCount*inode.InodeSize, lockMgr, nodeUUID)
	inodeTbl.SetAllocationStart(slotIdx, node.MaxNodes)
	extentMgr := extent.NewManager(dev, bm, lockMgr, dataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	dirMgr := dir.NewManager(dev, inodeTbl, extentMgr, lockMgr, dataOffset)

	// Check for and recover any dirty journal from a previous crash
	// This must happen BEFORE we create our own journal to avoid data loss
	if *journalEnabled && !*skipRecovery {
		// Set up recovery context with our managers
		recoveryCtx := &journal.RecoveryContext{
			Dev:       dev,
			InodeTbl:  inodeTbl,
			BitmapMgr: bm,
			DirMgr:    dirMgr,
			ExtentMgr: extentMgr,
		}
		// Use per-transaction handler for self-recovery
		recoveryHandler := journal.CreateTxnRecoveryHandler(recoveryCtx)

		// Replay a dirty journal left in this slot's region by its previous owner
		result, err := journal.ScanAndRecover(dev, journalOffset, journalSize, recoveryHandler)
		if err != nil {
			log.Fatalf("Journal recovery for slot %d failed: %v (mount with -skip-recovery to discard this journal)", slotIdx, err)
		}
		if result != nil && result.WasNeeded {
			log.Printf("Journal recovery: replayed %d committed transactions (%d bytes scanned) from crashed node %x",
				result.Stats.TxnCommitted, result.Stats.BytesScanned, result.NodeUUID[:8])
		}
	} else if *skipRecovery {
		log.Println("Journal: WARNING - skipping recovery (emergency mode)")
	}
	releasePrevious()

	// Create journal for write-ahead logging (crash consistency)
	var j *journal.Journal
	if *journalEnabled {
		generation := uint64(time.Now().UnixNano())
		jConfig := journal.DefaultConfig()

		// Always initialize a fresh journal for this node
		// Recovery (if needed) has already been performed above
		j, err = journal.Init(dev, journalOffset, journalSize, nodeUUID, generation, jConfig)
		if err != nil {
			log.Fatalf("Failed to initialize journal: %v", err)
		}
		log.Printf("Journal: initialized at offset %d (%d MB)", journalOffset, journalSize/(1024*1024))

		j.Start()
		log.Printf("Journal: started (mode=%v, checkpoint=%v)", jConfig.CommitMode, jConfig.CheckpointInterval)
	} else {
		log.Println("Journal: disabled (no crash consistency guarantees)")
	}

	// Build cache configuration from flags
	cacheCfg := cache.DefaultConfig()
	cacheCfg.StrictMode = *cacheStrict
	cacheCfg.InodeTTL = *cacheTTL
	cacheCfg.Network = broadcast.Config{
		Enabled:              *broadcastEnabled,
		MulticastGroup:       *multicastGroup,
		MulticastPort:        *multicastPort,
		UnicastPort:          *multicastPort,
		MulticastTTL:         1, // Same subnet
		MaxMulticastFailures: 3,
		HeartbeatInterval:    30 * time.Second,
		Secret:               *broadcastSecret,
		Interface:            *broadcastIface,
	}

	// Create cache manager for read optimization
	cacheMgr, err := cache.NewManager(cacheCfg, inodeTbl, lockMgr)
	if err != nil {
		log.Fatalf("Failed to create cache manager: %v", err)
	}
	log.Printf("Cache: %d inodes, %d negative entries, TTL=%v, strict=%v",
		cacheMgr.Config().InodeCacheSize,
		cacheMgr.Config().NegativeCacheSize,
		cacheMgr.Config().InodeTTL,
		cacheMgr.Config().StrictMode)

	// Register cache invalidation callback with lock manager.
	// When an exclusive lock is acquired, the cache for that resource
	// is invalidated to ensure we read fresh data from disk.
	lockMgr.SetOnAcquire(func(lockType lock.LockType, resourceID uint64, mode lock.LockMode) {
		cacheMgr.OnLockAcquire(uint8(lockType), resourceID, uint8(mode))
	})

	// Start network broadcast for cross-node cache invalidation
	if *broadcastEnabled {
		if err := cacheMgr.StartBroadcast(ctx, nodeUUID, uint32(slotIdx)); err != nil {
			log.Printf("Warning: Failed to start cache broadcast: %v (continuing without broadcast)", err)
		} else if cacheMgr.BroadcastEnabled() {
			log.Printf("Broadcast: enabled on %s:%d (secret=%v)",
				*multicastGroup, *multicastPort, *broadcastSecret != "")
		}
	} else {
		log.Println("Broadcast: disabled (using storage-only cache validation)")
	}

	// Create TRIM manager for batched block discards
	var trimMgr *trim.Manager
	if *trimEnabled {
		trimCfg := trim.Config{
			BatchSize:  *trimBatchSize,
			MaxAge:     *trimMaxAge,
			DataOffset: dataOffset,
			Enabled:    true,
		}
		trimMgr = trim.NewManager(dev, trimCfg)

		// Connect TRIM manager to bitmap for automatic queueing
		bm.SetTrimManager(trimMgr)

		// Start TRIM background loop
		trimMgr.Start()
		log.Printf("TRIM: enabled (batch=%d, maxAge=%v)", *trimBatchSize, *trimMaxAge)
	} else {
		log.Println("TRIM: disabled")
	}

	// Create SFB manager for small file optimization
	var sfbMgr *sfb.Manager
	if *sfbEnabled {
		sfbMgr = sfb.NewManager(dev, bm, lockMgr)
		log.Println("SFB: enabled (tiers: 4KB/8KB/16KB/64KB)")
	} else {
		log.Println("SFB: disabled (all files use full 1MB blocks)")
	}

	// Refcounts stay on without snapshots: blocks shared earlier still need copy-on-write.
	refcountReg := refcount.NewRegion(dev, refcountOffset, sb.RefCountTableSize, totalBlocks, lockMgr)
	log.Printf("Refcount: region at %dMB (%d blocks)", refcountOffset/(1024*1024), totalBlocks)
	if covered := refcount.BlocksForSize(sb.RefCountTableSize); totalBlocks > covered {
		log.Printf("[WARN] Refcount: region covers blocks 0-%d of %d; snapshots and clones of files with data in higher blocks fail",
			covered-1, totalBlocks)
	}

	// Create filesystem with journal, TRIM, SFB, and refcount for crash consistency
	gfs := goclusterfs.NewWithComponents(dev, bm, inodeTbl, extentMgr, dirMgr, cacheMgr, j, trimMgr, sfbMgr, refcountReg, lockMgr, nodeUUID, dataOffset)

	// Set up cross-node recovery handler for failed node journal replay
	recoveryCtx := &journal.RecoveryContext{
		Dev:       dev,
		InodeTbl:  inodeTbl,
		BitmapMgr: bm,
		DirMgr:    dirMgr,
		ExtentMgr: extentMgr,
	}
	nodeReg.SetRecoveryHandler(journal.CreateRecoveryHandler(recoveryCtx))

	// Start failure detection to monitor for crashed nodes
	nodeReg.StartFailureDetection()
	log.Println("Cross-node recovery enabled: monitoring for failed nodes")

	// Format if requested
	if *format {
		log.Println("Formatting filesystem...")
		if err := gfs.InitRoot(ctx); err != nil {
			log.Fatalf("Failed to initialize root: %v", err)
		}
		log.Println("Filesystem formatted successfully")
	}

	// Verify root exists
	rootIno, err := inodeTbl.Read(inode.InodeRoot)
	if err != nil || rootIno.Magic != inode.InodeMagic {
		log.Fatalf("Filesystem not formatted. Use -format to initialize.")
	}

	// Initialize snapshot subsystem after verifying filesystem is valid
	if *snapshotsEnabled {
		if err := gfs.InitSnapshots(ctx, snapshotOffset, snapshotSize, lockMgr, slotIdx); err != nil {
			log.Fatalf("Failed to initialize snapshots: %v", err)
		}
		log.Printf("Snapshots: enabled (registry at %dMB, %dMB capacity)",
			snapshotOffset/(1024*1024), snapshotSize/(1024*1024))
	} else {
		log.Println("Snapshots: disabled")
	}

	// Configure go-fuse mount options
	// AttrTimeout and EntryTimeout control kernel-side caching
	mountOpts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:        "goclusterfs",
			Name:          "goclusterfs",
			Debug:         *debug,
			MaxReadAhead:  128 * 1024, // 128KB readahead
			DisableXAttrs: false,
			// Note: AllowOther requires 'user_allow_other' in /etc/fuse.conf
		},
		AttrTimeout:  attrTimeout,
		EntryTimeout: attrTimeout,
	}

	// Mount filesystem using go-fuse v2 API
	log.Printf("Mounting at %s...", *mountPoint)
	server, err := fs.Mount(*mountPoint, gfs, mountOpts)
	if err != nil {
		log.Fatalf("Failed to mount: %v", err)
	}

	// Handle signals for clean unmount
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received signal, unmounting...")

		// Close filesystem (stops snapshot worker, TRIM, journal, etc.)
		nodeReg.Stop()
		if err := gfs.Close(); err != nil {
			log.Printf("Warning: Failed to close filesystem: %v", err)
		}

		// Stop broadcaster to notify peers
		if err := cacheMgr.StopBroadcast(); err != nil {
			log.Printf("Warning: Failed to stop broadcast: %v", err)
		}

		// Unmount using go-fuse server method
		server.Unmount()
	}()

	log.Println("Filesystem mounted. Press Ctrl+C to unmount.")

	// Wait for filesystem to be unmounted - this blocks until unmount
	server.Wait()

	// Clean shutdown - close filesystem (handles snapshot worker, TRIM, journal)
	nodeReg.Stop()
	if err := gfs.Close(); err != nil {
		log.Printf("Warning: Failed to close filesystem on exit: %v", err)
	}

	if err := cacheMgr.StopBroadcast(); err != nil {
		log.Printf("Warning: Failed to stop broadcast on exit: %v", err)
	}

	// Print final TRIM stats
	if trimMgr != nil {
		trimStats := trimMgr.Stats()
		log.Printf("TRIM stats: queued=%d, discarded=%d, calls=%d, coalesce=%.1f",
			trimStats.BlocksQueued, trimStats.BlocksDiscarded,
			trimStats.DiscardCalls, trimStats.CoalesceRatio)
	}

	// Print final journal stats
	if j != nil {
		jStats := j.Stats()
		log.Printf("Journal stats: commits=%d, bytes=%d, state=%v",
			jStats.CommitCount, jStats.BytesWritten, jStats.State)
	}

	// Print final SFB stats
	if sfbMgr != nil {
		total, byTier := sfbMgr.GetContainerStats()
		log.Printf("SFB stats: containers=%d (4KB=%d, 8KB=%d, 16KB=%d, 64KB=%d)",
			total, byTier[0], byTier[1], byTier[2], byTier[3])
	}

	// Print final cache stats
	stats := cacheMgr.Stats()
	log.Printf("Cache stats: %s", stats)
	if stats.BroadcastEnabled {
		log.Printf("Broadcast stats: sent=%d, received=%d, duplicates=%d",
			stats.InvalidationsSent, stats.InvalidationsReceived, stats.BroadcastDuplicates)
	}

	log.Println("Filesystem unmounted.")
}

// adoptLocks takes over a dead node's exclusive locks before its journal is
// replayed and returns the function that frees them after the replay.
func adoptLocks(locks *lock.Manager, owner [16]byte) func() {
	adopted, err := locks.AdoptOwner(owner)
	if err != nil {
		log.Printf("Warning: Failed to take over the locks of dead node %x: %v", owner[:8], err)
	}
	return func() {
		if err := locks.ReleaseAdopted(adopted); err != nil {
			log.Printf("Warning: Failed to release the locks taken over from dead node %x: %v", owner[:8], err)
		}
	}
}
