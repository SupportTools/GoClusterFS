// Command mount mounts a GoClusterFS filesystem.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/api"
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
	"github.com/supporttools/GoClusterFS/internal/version"
)

// fsStatusProvider implements api.StatusProvider by wrapping the filesystem.
type fsStatusProvider struct {
	mountPath  string
	devicePath string
	dev        *storage.Device
	bitmapMgr  *bitmap.Region
}

func (p *fsStatusProvider) MountPath() string  { return p.mountPath }
func (p *fsStatusProvider) DevicePath() string { return p.devicePath }

func (p *fsStatusProvider) MountState() api.MountState {
	// If we get here, the filesystem is mounted and running
	return api.MountStateMounted
}

func (p *fsStatusProvider) Capacity() *api.CapacityInfo {
	totalBlocks := p.bitmapMgr.TotalBlocks()
	freeBlocks := p.bitmapMgr.FreeCount()
	usedBlocks := totalBlocks - freeBlocks

	const blockSize uint64 = goclusterfs.BlockSize
	totalBytes := totalBlocks * blockSize
	usedBytes := usedBlocks * blockSize
	freeBytes := freeBlocks * blockSize

	var usedPercent float64
	if totalBlocks > 0 {
		usedPercent = float64(usedBlocks) / float64(totalBlocks) * 100
	}

	return &api.CapacityInfo{
		TotalBytes:     totalBytes,
		UsedBytes:      usedBytes,
		FreeBytes:      freeBytes,
		UsedPercent:    usedPercent,
		TotalBlocks:    totalBlocks,
		UsedBlocks:     usedBlocks,
		FreeBlocks:     freeBlocks,
		BlockSizeBytes: blockSize,
	}
}

func apiConfig(addr, token, corsList string, rateLimit float64) api.Config {
	var origins []string
	for _, origin := range strings.Split(corsList, ",") {
		if origin = strings.TrimSpace(origin); origin != "" {
			origins = append(origins, origin)
		}
	}
	cfg := api.DefaultConfig().
		WithListenAddr(addr).
		WithAuthToken(token).
		WithCORSOrigins(origins...)
	if rateLimit != 0 {
		cfg = cfg.WithRateLimit(rateLimit, int(math.Ceil(2*rateLimit)))
	}
	return cfg
}

func main() {
	var (
		devicePath = flag.String("device", "", "Path to block device or image file")
		mountPoint = flag.String("mount", "", "Mount point directory")
		format     = flag.Bool("format", false, "Format the device (WARNING: destroys data)")
		inodeRatio = flag.Uint64("bytes-per-inode", superblock.DefaultBytesPerInode,
			fmt.Sprintf("With -format: one inode per this many bytes of device (%d to %d inodes)", superblock.MinInodes, superblock.MaxInodes))
		debug   = flag.Bool("debug", false, "Enable FUSE debug output")
		showVer = flag.Bool("version", false, "Print version and exit")

		// Cache configuration
		cacheStrict = flag.Bool("cache-strict", false, "Enable strict cache mode (always validate)")
		cacheTTL    = flag.Duration("cache-ttl", 5*time.Second, "Cache TTL before validation")
		attrTimeout = flag.Duration("attr-timeout", time.Second, "How long the kernel caches file attributes and directory entries")

		// Journal configuration
		journalEnabled = flag.Bool("journal", true, "Enable write-ahead journal for crash consistency")
		skipRecovery   = flag.Int("skip-recovery", -1, "Discard the journal of registry slot N instead of replaying it, if this mount takes slot N (emergency maintenance only)")

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

		// API server configuration
		apiEnabled = flag.Bool("api", true, "Enable HTTP API server for management")
		apiAddr    = flag.String("api-addr", "127.0.0.1:8080", "API server listen address; any non-loopback address needs -api-token")
		apiToken   = flag.String("api-token", "", "Bearer token for API authentication (empty=disabled, loopback only)")
		apiCORS    = flag.String("api-cors", "", "Comma-separated CORS allowed origins (empty=disabled)")
		apiRate    = flag.Float64("api-rate-limit", 0, "API requests per second per client IP, with bursts of twice that (0=off)")
	)
	flag.Parse()

	if *showVer {
		fmt.Println("goclusterfs-mount", version.String())
		os.Exit(0)
	}

	if *devicePath == "" || *mountPoint == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -device <path> -mount <mountpoint> [-format] [-debug]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	apiCfg := apiConfig(*apiAddr, *apiToken, *apiCORS, *apiRate)
	if *apiEnabled {
		if err := apiCfg.Validate(); err != nil {
			log.Fatalf("Invalid API settings: %v", err)
		}
		if err := apiCfg.CheckExposure(); err != nil {
			hint := "set -api-addr to a loopback address such as 127.0.0.1:8080"
			if errors.Is(err, api.ErrWildcardCORSWithoutToken) {
				hint = "list the allowed origins in -api-cors"
			}
			log.Fatalf("Refusing to start the API: %v. Set -api-token, %s, or pass -api=false", err, hint)
		}
	}
	if *broadcastEnabled && *broadcastSecret == "" {
		log.Printf("WARNING: -broadcast is on without -broadcast-secret. Broadcast messages are not authenticated: any host that can send UDP to port %d can make this node drop its caches and add itself as a peer. Set the same -broadcast-secret on every node, or pass -broadcast=false.", *multicastPort)
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
	// -skip-recovery names the slot whose journal the operator chose to discard;
	// a mount that takes any other slot replays that slot's journal.
	skip := *skipRecovery >= 0 && *skipRecovery == slotIdx
	if *skipRecovery >= 0 && !skip {
		log.Printf("Journal: ignoring -skip-recovery=%d: this mount took slot %d, so it replays that slot's journal", *skipRecovery, slotIdx)
	}
	if *journalEnabled && !skip {
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
			log.Fatalf("Journal recovery for slot %d failed: %v (to discard this journal, mount with -skip-recovery=%d)", slotIdx, err, slotIdx)
		}
		if result != nil && result.WasNeeded {
			log.Printf("Journal recovery: replayed %d committed transactions (%d bytes scanned) from crashed node %x",
				result.Stats.TxnCommitted, result.Stats.BytesScanned, result.NodeUUID[:8])
		}
	} else if skip {
		log.Printf("Journal: WARNING - discarding the journal of slot %d (-skip-recovery)", slotIdx)
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

	// Start API server for management and monitoring
	var apiServer *api.Server
	if *apiEnabled {
		var err error
		apiServer, err = api.NewServer(apiCfg)
		if err != nil {
			log.Printf("Warning: Failed to create API server: %v", err)
		} else {
			apiServer.SetStatusProvider(&fsStatusProvider{
				mountPath:  *mountPoint,
				devicePath: *devicePath,
				dev:        dev,
				bitmapMgr:  bm,
			})
			apiServer.RegisterHealthChecker(api.DefaultHealthChecker())
			if err := apiServer.Start(ctx); err != nil {
				log.Printf("Warning: Failed to start API server: %v", err)
				apiServer = nil
			} else {
				log.Printf("API server listening on %s (auth=%v, cors=%v)",
					apiServer.Addr(), apiCfg.AuthToken != "", len(apiCfg.CORSOrigins) > 0)
			}
		}
	} else {
		log.Println("API server: disabled")
	}

	// Handle signals for clean unmount
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received signal, unmounting...")

		// Stop API server first
		if apiServer != nil {
			if err := apiServer.Stop(); err != nil {
				log.Printf("Warning: Failed to stop API server: %v", err)
			}
		}

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

	// Stop API server
	if apiServer != nil {
		if err := apiServer.Stop(); err != nil {
			log.Printf("Warning: Failed to stop API server on exit: %v", err)
		}
	}

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
