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
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/goclusterfs"
	"github.com/supporttools/GoClusterFS/internal/inode"
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
	// 3MB+: Data blocks (1MB each)
	const (
		bitmapOffset = 1 * 1024 * 1024
		inodeOffset  = 2 * 1024 * 1024
		dataOffset   = 3 * 1024 * 1024
	)

	// Calculate capacities
	dataSize := devSize - dataOffset
	totalBlocks := uint64(dataSize / (1024 * 1024)) // 1MB blocks
	inodeCount := uint64(4096)                      // Fixed for now

	if totalBlocks < 1 {
		log.Fatalf("Device too small: need at least 4MB, have %d MB", devSize/(1024*1024))
	}

	log.Printf("Layout: %d data blocks (%d MB), %d inodes", totalBlocks, totalBlocks, inodeCount)

	// Create node UUID
	var nodeUUID [16]byte
	hostname, _ := os.Hostname()
	copy(nodeUUID[:], fmt.Sprintf("node-%s-%d", hostname, os.Getpid()))

	// Create managers (no distributed locking for single-node)
	bm := bitmap.NewRegion(dev, bitmapOffset, totalBlocks, nil, nodeUUID)
	inodeTbl := inode.NewTable(dev, inodeOffset, inodeCount*inode.InodeSize, nil, nodeUUID)
	extentMgr := extent.NewManager(dev, bm, nil)
	dirMgr := dir.NewManager(dev, inodeTbl, extentMgr, nil, dataOffset)

	// Create filesystem
	gfs := goclusterfs.NewWithComponents(dev, bm, inodeTbl, extentMgr, dirMgr, nodeUUID, dataOffset)

	ctx := context.Background()

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
