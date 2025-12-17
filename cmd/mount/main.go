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

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

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
	"github.com/supporttools/GoClusterFS/internal/storage"
)

func main() {
	var (
		devicePath = flag.String("device", "", "Path to block device or image file")
		mountPoint = flag.String("mount", "", "Mount point directory")
		format     = flag.Bool("format", false, "Format the device (WARNING: destroys data)")
		debug      = flag.Bool("debug", false, "Enable FUSE debug output")

		// Cache configuration
		cacheStrict = flag.Bool("cache-strict", false, "Enable strict cache mode (always validate)")
		cacheTTL    = flag.Duration("cache-ttl", 5*time.Second, "Cache TTL before validation")

		// Network broadcast configuration
		broadcastEnabled = flag.Bool("broadcast", true, "Enable network cache invalidation broadcast")
		multicastGroup   = flag.String("mcast-group", "239.192.74.75", "Multicast group address")
		multicastPort    = flag.Int("mcast-port", 31337, "Multicast/unicast port")
		broadcastSecret  = flag.String("broadcast-secret", "", "Shared secret for HMAC authentication")
		broadcastIface   = flag.String("broadcast-iface", "", "Network interface for multicast")
	)
	flag.Parse()

	if *devicePath == "" || *mountPoint == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -device <path> -mount <mountpoint> [-format] [-debug]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Enable debug logging if requested
	if *debug {
		fuse.Debug = func(msg interface{}) {
			log.Printf("[FUSE] %v", msg)
		}
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

	// Layout configuration
	// 0-1MB: Reserved (superblock area)
	// 1-2MB: Bitmap region
	// 2-3MB: Inode table
	// 3-4MB: Node registry (for distributed lock coordination)
	// 4-5MB: Lock region (CAS-based distributed locks)
	// 5MB+: Data blocks (1MB each)
	const (
		bitmapOffset       = 1 * 1024 * 1024
		inodeOffset        = 2 * 1024 * 1024
		nodeRegistryOffset = 3 * 1024 * 1024
		lockOffset         = 4 * 1024 * 1024
		lockSize           = 1 * 1024 * 1024 // 1MB for lock records
		dataOffset         = 5 * 1024 * 1024
	)

	// Calculate capacities
	dataSize := devSize - dataOffset
	totalBlocks := uint64(dataSize / (1024 * 1024)) // 1MB blocks
	inodeCount := uint64(4096)                      // Fixed for now

	if totalBlocks < 1 {
		log.Fatalf("Device too small: need at least 6MB, have %d MB", devSize/(1024*1024))
	}

	log.Printf("Layout: %d data blocks (%d MB), %d inodes", totalBlocks, totalBlocks, inodeCount)

	ctx := context.Background()

	// Create node registry for distributed coordination
	nodeReg := node.NewRegistry(dev, nodeRegistryOffset)
	hostname, _ := os.Hostname()
	slotIdx, err := nodeReg.Register(ctx, hostname)
	if err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}
	nodeUUID := nodeReg.LocalUUID()
	log.Printf("Registered as node %d (UUID: %x)", slotIdx, nodeUUID[:8])

	// Create lock manager for distributed locking
	lockMgr := lock.NewManager(dev, lockOffset, lockSize, nodeUUID, slotIdx)

	// Create managers with distributed locking enabled
	bm := bitmap.NewRegion(dev, bitmapOffset, totalBlocks, lockMgr, nodeUUID)
	inodeTbl := inode.NewTable(dev, inodeOffset, inodeCount*inode.InodeSize, lockMgr, nodeUUID)
	extentMgr := extent.NewManager(dev, bm, lockMgr)
	dirMgr := dir.NewManager(dev, inodeTbl, extentMgr, lockMgr, dataOffset)

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

	// Create filesystem
	gfs := goclusterfs.NewWithComponents(dev, bm, inodeTbl, extentMgr, dirMgr, cacheMgr, nodeUUID, dataOffset)

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

	// Mount options
	mountOpts := []fuse.MountOption{
		fuse.FSName("goclusterfs"),
		fuse.Subtype("goclusterfs"),
		fuse.MaxReadahead(1 << 17), // 128KB
		fuse.AsyncRead(),
		// Note: AllowOther() requires 'user_allow_other' in /etc/fuse.conf
	}

	// Mount filesystem
	log.Printf("Mounting at %s...", *mountPoint)
	conn, err := fuse.Mount(*mountPoint, mountOpts...)
	if err != nil {
		log.Fatalf("Failed to mount: %v", err)
	}
	defer conn.Close()

	// Handle signals for clean unmount
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received signal, unmounting...")

		// Stop broadcaster first to notify peers
		if err := cacheMgr.StopBroadcast(); err != nil {
			log.Printf("Warning: Failed to stop broadcast: %v", err)
		}

		fuse.Unmount(*mountPoint)
	}()

	log.Println("Filesystem mounted. Press Ctrl+C to unmount.")

	// Serve filesystem - blocks until unmount
	if err := fs.Serve(conn, gfs); err != nil {
		log.Fatalf("Serve failed: %v", err)
	}

	// Clean shutdown
	if err := cacheMgr.StopBroadcast(); err != nil {
		log.Printf("Warning: Failed to stop broadcast on exit: %v", err)
	}

	// Print final cache stats
	stats := cacheMgr.Stats()
	log.Printf("Cache stats: inodes=%d (hit=%.1f%%), extents=%d (hit=%.1f%%)",
		stats.InodeCacheSize, stats.InodeHitRate*100,
		stats.ExtentCacheSize, stats.ExtentHitRate*100)
	if stats.BroadcastEnabled {
		log.Printf("Broadcast stats: sent=%d, received=%d, duplicates=%d",
			stats.InvalidationsSent, stats.InvalidationsReceived, stats.BroadcastDuplicates)
	}

	log.Println("Filesystem unmounted.")
}
