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

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/cache"
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

	// Create cache manager for read optimization
	cacheMgr, err := cache.NewManager(cache.DefaultConfig(), inodeTbl, lockMgr)
	if err != nil {
		log.Fatalf("Failed to create cache manager: %v", err)
	}
	log.Printf("Cache: %d inodes, %d negative entries, TTL=%v",
		cacheMgr.Config().InodeCacheSize,
		cacheMgr.Config().NegativeCacheSize,
		cacheMgr.Config().InodeTTL)

	// Register cache invalidation callback with lock manager.
	// When an exclusive lock is acquired, the cache for that resource
	// is invalidated to ensure we read fresh data from disk.
	lockMgr.SetOnAcquire(func(lockType lock.LockType, resourceID uint64, mode lock.LockMode) {
		cacheMgr.OnLockAcquire(uint8(lockType), resourceID, uint8(mode))
	})

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
		fuse.Unmount(*mountPoint)
	}()

	log.Println("Filesystem mounted. Press Ctrl+C to unmount.")

	// Serve filesystem - blocks until unmount
	if err := fs.Serve(conn, gfs); err != nil {
		log.Fatalf("Serve failed: %v", err)
	}

	log.Println("Filesystem unmounted.")
}
