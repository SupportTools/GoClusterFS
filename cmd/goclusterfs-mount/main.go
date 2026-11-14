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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/api"
	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/cache"
	"github.com/supporttools/GoClusterFS/internal/cache/broadcast"
	"github.com/supporttools/GoClusterFS/internal/cli"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/fileops"
	"github.com/supporttools/GoClusterFS/internal/goclusterfs"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/logging"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/stats"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
	"github.com/supporttools/GoClusterFS/internal/trim"
	"github.com/supporttools/GoClusterFS/internal/version"
	"github.com/supporttools/GoClusterFS/internal/webui"
)

type fsStatusProvider struct {
	mountPath  string
	devicePath string
	sampler    *stats.Sampler
}

func (p *fsStatusProvider) MountPath() string           { return p.mountPath }
func (p *fsStatusProvider) DevicePath() string          { return p.devicePath }
func (p *fsStatusProvider) MountState() api.MountState  { return api.MountStateMounted }
func (p *fsStatusProvider) Capacity() *api.CapacityInfo { return p.sampler.Capacity() }

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
		burst := 1
		if rateLimit > 0 && rateLimit <= api.MaxRateLimit {
			burst = int(math.Ceil(2 * rateLimit))
		}
		cfg = cfg.WithRateLimit(rateLimit, burst)
	}
	return cfg
}

func main() {
	var (
		devicePath = flag.String("device", "", "Block device, or image file on a filesystem that supports O_DIRECT, to mount; required")
		mountPoint = flag.String("mount", "", "Directory to mount the filesystem on; required")
		format     = flag.Bool("format", false, "Erase the device and create an empty filesystem, then mount it. Destroys all data and does not check for other nodes: unmount every node first")
		inodeRatio = flag.Uint64("bytes-per-inode", superblock.DefaultBytesPerInode,
			fmt.Sprintf("With -format: one inode per this many bytes of device (%d to %d inodes); fixed until the next format", superblock.MinInodes, superblock.MaxInodes))
		debug      = flag.Bool("debug", false, "Print [DEBUG] log lines")
		fuseDebug  = flag.Bool("fuse-debug", false, "Print every FUSE request and reply")
		showVer    = flag.Bool("version", false, "Print version and exit")
		completion = flag.String("completion", "", "Print a bash or zsh completion script for goclusterfs-mount and exit")
		preflight  = flag.Bool("preflight", false, "Check that this host can mount -device on -mount: FUSE, the device, the mount point and the ports. Prints PASS, WARN or FAIL with a fix for each, changes nothing, and exits 8 if a check fails")

		// Cache configuration
		cacheStrict   = flag.Bool("cache-strict", false, "Check every cached inode against the device before using it; slower, but other nodes' changes show at once")
		cacheTTL      = flag.Duration("cache-ttl", 5*time.Second, "How long a cached inode is used before it is checked against the device; another node's change can take this long to show when its broadcast is lost")
		attrTimeout   = flag.Duration("attr-timeout", time.Second, "How long the kernel caches file attributes and directory entries; stat and ls can show another node's change this late, while opening a file always sees it")
		cacheInodes   = flag.Int("cache-inodes", cache.DefaultConfig().InodeCacheSize, "Most inodes the inode cache holds; 0 turns it off")
		cacheExtents  = flag.Int("cache-extents", cache.DefaultConfig().ExtentCacheSize, "Most block mappings the extent cache holds; 0 turns it off")
		cacheNegative = flag.Int("cache-negative", cache.DefaultConfig().NegativeCacheSize, "Most not-found inode lookups the negative cache holds; 0 turns it off")
		dirIndex      = flag.Int("dir-index-entries", dir.DefaultIndexEntries, "Names kept in in-memory directory indexes, about 100 bytes each; 0 turns them off")

		// Journal configuration
		journalEnabled = flag.Bool("journal", true, "Write-ahead journal for crash consistency. Off, a crash of this node can leave the filesystem inconsistent: testing only")
		skipRecovery   = flag.Int("skip-recovery", -1, "Emergency use only: discard the journal of registry slot N instead of replaying it, if this mount takes slot N, losing the writes it holds; -1 replays as normal")

		// Network broadcast configuration
		broadcastEnabled = flag.Bool("broadcast", true, "Send and receive cache invalidations over UDP so other nodes see changes sooner; correctness does not depend on it")
		multicastGroup   = flag.String("mcast-group", "239.192.74.75", "Multicast group for broadcast messages; the same on every node")
		multicastPort    = flag.Int("mcast-port", 31337, "UDP port for multicast messages; the same on every node")
		unicastPort      = flag.Int("unicast-port", 31338, "UDP port for unicast messages; must differ from -mcast-port")
		broadcastSecret  = flag.String("broadcast-secret", "", "Secret that signs broadcast messages, the same on every node; visible to local users in ps. Empty leaves them unauthenticated: any host that can reach the UDP ports can make this node drop its caches")
		broadcastIface   = flag.String("broadcast-iface", "", "Network interface for multicast; empty lets the kernel choose")
		broadcastPeers   = flag.Int("broadcast-max-peers", broadcast.DefaultMaxPeers, "Most nodes the broadcast peer table and sequence tracking hold")

		// TRIM configuration
		trimEnabled   = flag.Bool("trim", true, "Discard freed blocks on the device (BLKDISCARD) in batches; does nothing on image files")
		trimBatchSize = flag.Int("trim-batch", 256, "Freed blocks to collect before a discard")
		trimMaxAge    = flag.Duration("trim-age", 5*time.Second, "Longest a freed block waits for its discard")

		// SFB configuration (Sub-File Block containers for small files)
		_ = flag.Bool("sfb", true, "Ignored: SFB containers are disabled, and every file uses full 1MB blocks")

		// Snapshot configuration
		snapshotsEnabled = flag.Bool("snapshots", true, "File snapshots and clones through extended attributes (user.snapshot, user.clone)")

		// API server configuration
		apiEnabled   = flag.Bool("api", true, "Serve the HTTP API: health, metrics, status and statistics")
		apiAddr      = flag.String("api-addr", "127.0.0.1:8080", "API server listen address; any non-loopback address needs a token")
		apiTokenFile = flag.String("api-token-file", "", "File (mode 0600) holding the API bearer token; a token also turns on API writes. Or set "+apiTokenEnv)
		apiToken     = flag.String("api-token", "", "API bearer token; visible to all local users in ps, prefer -api-token-file")
		apiCORS      = flag.String("api-cors", "", "Comma-separated origins that browsers may call the API from, or *; empty allows none")
		apiRate      = flag.Float64("api-rate-limit", 0, "API requests per second per client IP, with bursts of twice that; 0 turns the limit off")
		dashboard    = flag.Bool("dashboard", true, "Serve the web dashboard on the API server under /ui/")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -device <path> -mount <mountpoint> [flags]\n\nTurn off a flag that defaults to true with -flag=false.\n\n", os.Args[0])
		cli.PrintFlags(os.Stderr, flag.CommandLine)
		fmt.Fprint(os.Stderr, exitCodesHelp)
	}
	if code, ok := cli.ParseFlags(flag.CommandLine, os.Args[1:]); !ok {
		os.Exit(code)
	}
	logging.SetDebug(*debug)

	if *showVer {
		fmt.Println("goclusterfs-mount", version.String())
		os.Exit(cli.ExitOK)
	}
	if *completion != "" {
		spec := cli.CompletionSpec{Values: map[string]string{"device": cli.CompleteFile, "mount": cli.CompleteDir, "api-token-file": cli.CompleteFile}}
		if err := cli.WriteCompletion(os.Stdout, *completion, "goclusterfs-mount", flag.CommandLine, spec); err != nil {
			usageFailf("-completion: %v", err)
		}
		os.Exit(cli.ExitOK)
	}

	if *devicePath == "" || *mountPoint == "" {
		usageFailf("-device and -mount are required")
	}
	if flag.NArg() > 0 {
		usageFailf("unexpected argument %q; give the device with -device and the mount point with -mount", flag.Arg(0))
	}

	token, tokenSrc, err := loadAPIToken(*apiTokenFile, *apiToken)
	if err != nil {
		usageFailf("Invalid API settings: %v", err)
	}
	if tokenSrc == tokenFromFlag {
		log.Printf("WARNING: -api-token is visible to every local user in ps and /proc/<pid>/cmdline, and it unlocks API writes. Use -api-token-file or %s instead.", apiTokenEnv)
	}
	apiCfg := apiConfig(*apiAddr, token, *apiCORS, *apiRate)
	if *dashboard {
		apiCfg.Dashboard = webui.Handler()
	}
	if *apiEnabled {
		if err := apiCfg.Validate(); err != nil {
			usageFailf("Invalid API settings: %v", err)
		}
		if err := apiCfg.CheckExposure(); err != nil {
			hint := "set -api-addr to a loopback address such as 127.0.0.1:8080"
			if errors.Is(err, api.ErrWildcardCORSWithoutToken) {
				hint = "list the allowed origins in -api-cors"
			}
			usageFailf("Refusing to start the API: %v. Set -api-token-file (or %s), %s, or pass -api=false", err, apiTokenEnv, hint)
		}
	}
	if *broadcastEnabled && *broadcastSecret == "" {
		log.Printf("WARNING: -broadcast is on without -broadcast-secret. Broadcast messages are not authenticated: any host that can send UDP to ports %d and %d can make this node drop its caches and add itself as a peer. Set the same -broadcast-secret on every node, or pass -broadcast=false.", *multicastPort, *unicastPort)
	}

	networkCfg := broadcast.Config{
		Enabled:              *broadcastEnabled,
		MulticastGroup:       *multicastGroup,
		MulticastPort:        *multicastPort,
		UnicastPort:          *unicastPort,
		MulticastTTL:         1, // Same subnet
		MaxMulticastFailures: 3,
		HeartbeatInterval:    30 * time.Second,
		Secret:               *broadcastSecret,
		Interface:            *broadcastIface,
		MaxPeers:             *broadcastPeers,
	}
	if *broadcastPeers < 1 {
		log.Fatalf("-broadcast-max-peers must be at least 1, got %d", *broadcastPeers)
	}
	cacheCfg := cache.DefaultConfig()
	cacheCfg.StrictMode = *cacheStrict
	cacheCfg.InodeTTL = *cacheTTL
	cacheCfg.InodeCacheSize = *cacheInodes
	cacheCfg.ExtentCacheSize = *cacheExtents
	cacheCfg.NegativeCacheSize = *cacheNegative
	cacheCfg.Network = networkCfg
	if err := cacheCfg.Validate(); err != nil {
		log.Fatalf("Invalid cache flags: %v", err)
	}
	if *preflight {
		cfg := defaultPreflightConfig()
		cfg.device, cfg.mountPoint, cfg.format, cfg.network = *devicePath, *mountPoint, *format, networkCfg
		if *apiEnabled {
			cfg.apiAddr = *apiAddr
		}
		os.Exit(runPreflight(os.Stdout, cfg))
	}
	broadcastFailed := func(err error) {
		failf("Failed to start cache broadcast: %v. Free UDP ports %d and %d, choose others with -mcast-port and -unicast-port, or pass -broadcast=false", err, *multicastPort, *unicastPort)
	}
	// Bind and release the ports before registering or formatting, so a taken
	// port fails the mount before it changes anything on disk.
	if err := broadcast.CheckPorts(networkCfg); err != nil {
		broadcastFailed(err)
	}

	if _, err := os.Stat(*mountPoint); os.IsNotExist(err) {
		failf("Mount point %s does not exist; create it with mkdir -p %s", *mountPoint, *mountPoint)
	}

	dev, err := storage.Open(*devicePath)
	if err != nil {
		failf("Failed to open device: %v; %s", err, cli.OpenHint(err))
	}

	// Get device size (returns int64, no error)
	devSize := dev.Size()

	log.Printf("Device: %s (%d MB)", *devicePath, devSize/(1024*1024))

	var sb *superblock.Superblock
	if *format {
		sb, err = superblock.Format(dev, *inodeRatio)
	} else {
		sb, err = superblock.Read(dev)
	}
	if errors.Is(err, superblock.ErrChecksumMismatch) {
		failf("Cannot use %s: %v; check it with goclusterfs-fsck %s", *devicePath, err, *devicePath)
	}
	if err != nil {
		failf("Cannot use %s: %v", *devicePath, err)
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
	if errors.Is(err, node.ErrNoFreeSlots) {
		failf("Cannot join the cluster: all %d node slots are taken. List them with goclusterfs-fsck -list-slots %s, and free the slot of a node that will not come back with -release-slot", node.MaxNodes, *devicePath)
	}
	if err != nil {
		failf("Cannot join the cluster: %v; check the device, then mount again", err)
	}
	nodeReg.SetSlotLostHandler(func(slot int, owner [16]byte) {
		failf("Node registry slot %d is no longer this node's (owner %x); stopping so two nodes never share a journal. "+
			"Check that the device is reachable and that node clocks agree within a few seconds, then mount again", slot, owner[:8])
	})
	if err := nodeReg.StartHeartbeat(); err != nil {
		failf("Failed to start node heartbeat: %v", err)
	}
	dev.SetWriteGate(nodeReg.WriteGate)
	nodeUUID := nodeReg.LocalUUID()
	log.Printf("Registered as node %d (UUID: %x)", slotIdx, nodeUUID[:8])
	journalOffset, journalSize := nodeReg.LocalJournal()

	// Create lock manager for distributed locking
	lockMgr := lock.NewManager(dev, lockOffset, lockSize, nodeUUID, slotIdx)
	lockMgr.SetOwnerView(func(slot int, fresh bool) ([16]byte, lock.OwnerState) {
		occupant, occupancy := nodeReg.Occupant(slot, fresh)
		return occupant, ownerStates[occupancy]
	})
	lockMgr.Start()
	nodeReg.SetOwnerDeadHandler(func(owner [16]byte, slot int) func(bool) { return adoptLocks(lockMgr, owner, slot) })
	releasePrevious := func(bool) {}
	if owner, ok := nodeReg.PreviousOwner(); ok {
		releasePrevious = adoptLocks(lockMgr, owner, slotIdx)
	}

	// Create managers with distributed locking enabled
	// NOTE: Managers must be created BEFORE journal recovery so we can replay transactions
	bm := bitmap.NewRegion(dev, bitmapOffset, totalBlocks, lockMgr, nodeUUID)
	inodeTbl := inode.NewTable(dev, inodeOffset, inodeCount*inode.InodeSize, lockMgr, nodeUUID)
	inodeTbl.SetAllocationStart(slotIdx, node.MaxNodes)
	extentMgr := extent.NewManager(dev, bm, lockMgr, dataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	dirMgr := dir.NewManager(dev, inodeTbl, extentMgr, lockMgr, dataOffset)
	dirMgr.SetIndexLimit(*dirIndex)

	// Refcounts stay on without snapshots: blocks shared earlier still need copy-on-write, and replay frees shared blocks through them.
	refcountReg := refcount.NewRegion(dev, refcountOffset, sb.RefCountTableSize, totalBlocks, lockMgr)
	extentMgr.SetRefcounts(refcountReg)
	log.Printf("Refcount: region at %dMB (%d blocks)", refcountOffset/(1024*1024), totalBlocks)

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
		result, err := recoverOwnJournal(dev, journalOffset, journalSize, recoveryHandler, slotIdx)
		if err != nil {
			failf("Journal recovery for slot %d failed: %v. Check the device, then mount again; to discard this journal instead, mount with -skip-recovery=%d", slotIdx, err, slotIdx)
		}
		if result != nil && result.WasNeeded {
			log.Printf("Journal recovery: replayed %d committed transactions (%d bytes scanned) from crashed node %x",
				result.Stats.TxnCommitted, result.Stats.BytesScanned, result.NodeUUID[:8])
		}
	} else if skip {
		log.Printf("Journal: WARNING - discarding the journal of slot %d (-skip-recovery)", slotIdx)
	}
	releasePrevious(true)
	if err := lockMgr.ClearForeignClaims(); err != nil {
		log.Printf("Warning: clearing lock claims earlier occupants left in slot %d failed: %v", slotIdx, err)
	}

	// Create journal for write-ahead logging (crash consistency)
	var j *journal.Journal
	if *journalEnabled {
		generation := uint64(time.Now().UnixNano())
		jConfig := journal.DefaultConfig()

		// Always initialize a fresh journal for this node
		// Recovery (if needed) has already been performed above
		j, err = journal.Init(dev, journalOffset, journalSize, nodeUUID, generation, jConfig)
		if err != nil {
			failf("Failed to initialize the journal: %v; check the device, then run goclusterfs-fsck %s", err, *devicePath)
		}
		log.Printf("Journal: initialized at offset %d (%d MB)", journalOffset, journalSize/(1024*1024))

		j.Start()
		log.Printf("Journal: started (mode=%v, checkpoint=%v)", jConfig.CommitMode, jConfig.CheckpointInterval)
	} else {
		log.Println("Journal: disabled (no crash consistency guarantees)")
	}

	// Create cache manager for read optimization
	cacheMgr, err := cache.NewManager(cacheCfg, inodeTbl, lockMgr)
	if err != nil {
		failf("Failed to create the cache: %v; check -cache-ttl", err)
	}
	log.Printf("Cache: %d inodes, %d extents, %d negative entries, TTL=%v, strict=%v",
		cacheMgr.Config().InodeCacheSize,
		cacheMgr.Config().ExtentCacheSize,
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
		// Without broadcast, peers see this node's changes only after -cache-ttl,
		// so a mount that asked for broadcast does not continue without it.
		if err := cacheMgr.StartBroadcast(ctx, nodeUUID, uint32(slotIdx)); err != nil {
			broadcastFailed(err)
		}
		log.Printf("Broadcast: enabled on %s:%d, unicast port %d (secret=%v)",
			*multicastGroup, *multicastPort, *unicastPort, *broadcastSecret != "")
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

	log.Println("SFB: disabled (-sfb is ignored); new files use full 1MB blocks, and files older builds stored in SFB slots move to full blocks on their next write")

	// Create filesystem with journal, TRIM, and refcount for crash consistency
	gfs := goclusterfs.NewWithComponents(dev, bm, inodeTbl, extentMgr, dirMgr, cacheMgr, j, trimMgr, nil, refcountReg, lockMgr, nodeUUID, dataOffset)

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
			failf("Failed to create the root directory: %v; check the device, then format again", err)
		}
		log.Println("Filesystem formatted successfully")
	}

	// Verify root exists
	rootIno, err := inodeTbl.Read(inode.InodeRoot)
	if err != nil || rootIno.Magic != inode.InodeMagic {
		failf("The root directory of %s is missing or damaged. Check it with goclusterfs-fsck %s; -format creates a new, empty filesystem and erases this one", *devicePath, *devicePath)
	}

	// Initialize snapshot subsystem after verifying filesystem is valid
	if *snapshotsEnabled {
		if err := gfs.InitSnapshots(ctx, snapshotOffset, snapshotSize, lockMgr, slotIdx); err != nil {
			failf("Failed to load snapshots: %v; check the device with goclusterfs-fsck %s, or mount with -snapshots=false", err, *devicePath)
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
			Debug:         *fuseDebug,
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
		failf("Failed to mount at %s: %v. Check that /dev/fuse exists, fusermount3 is installed, and nothing else is mounted there (fusermount3 -u %s)", *mountPoint, err, *mountPoint)
	}

	// Start API server for management and monitoring
	var apiServer *api.Server
	var sampler *stats.Sampler
	if *apiEnabled {
		var err error
		apiServer, err = api.NewServer(apiCfg)
		if err != nil {
			log.Printf("Warning: Failed to create the API server: %v; the mount continues without it", err)
		} else {
			sampler = stats.StartSampler(statsSources(cacheMgr, lockMgr, dirMgr, j, trimMgr, bm, nodeReg))
			apiServer.SetStatusProvider(&fsStatusProvider{
				mountPath:  *mountPoint,
				devicePath: *devicePath,
				sampler:    sampler,
			})
			apiServer.SetStatsProvider(sampler)
			registerHealthChecks(apiServer, healthSources{
				deviceError:      dev.LastError,
				journal:          sampler.JournalStats,
				replaying:        nodeReg.Replaying,
				broadcastOn:      cacheMgr.BroadcastEnabled(),
				broadcastProblem: cacheMgr.BroadcastProblem,
			})
			apiServer.SetFileService(fileops.New(*mountPoint, gfs.SnapshotRegistry()))
			apiServer.SetAdminService(adminOps{journal: j, trim: trimMgr, cache: cacheMgr})
			if err := apiServer.Start(ctx); err != nil {
				log.Printf("Warning: Failed to start the API server: %v; the mount continues without it. Choose a free -api-addr, or pass -api=false", err)
				if err := apiServer.Stop(); err != nil && !errors.Is(err, api.ErrServerNotStarted) {
					log.Printf("Warning: Failed to stop the API server: %v", err)
				}
				apiServer = nil
				sampler.Stop()
				sampler = nil
			} else {
				log.Printf("API server listening on %s (auth=%v, cors=%v)",
					apiServer.Addr(), apiCfg.AuthToken != "", len(apiCfg.CORSOrigins) > 0)
				if !apiServer.WritesEnabled() {
					log.Printf("API: snapshot, clone and admin endpoints are off; set -api-token-file or %s to turn them on", apiTokenEnv)
				}
				if apiCfg.Dashboard != nil && webui.Built() {
					log.Printf("API: dashboard at /ui/")
				} else if apiCfg.Dashboard != nil {
					log.Printf("API: this binary has no dashboard build; /ui/ explains how to add it")
				}
			}
		}
	} else {
		log.Println("API server: disabled")
	}

	stopAPI := func() {
		if apiServer == nil {
			return
		}
		if err := apiServer.Stop(); err != nil {
			log.Printf("Warning: Failed to stop the API server: %v", err)
		}
		sampler.Stop()
	}

	// The API stops before the unmount: its snapshot and clone requests go
	// through the mount, and the unmount fails while they hold files open.
	var unmounted atomic.Bool
	// The watch runs from the signal, or from an unmount another tool started, to the exit.
	watch := &goclusterfs.ShutdownWatch{Limit: 8 * time.Second}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for range sigChan {
			if unmounted.Load() {
				log.Println("Received signal; already unmounted, finishing the shutdown")
				continue
			}
			log.Println("Received signal, unmounting...")
			watch.Start()
			goclusterfs.ShutdownStep("API server stop", stopAPI)
			goclusterfs.ShutdownStep("unmount", func() {
				if err := server.Unmount(); err != nil {
					log.Printf("Unmount of %s failed: %v. Close the files and shells that use it, then send the signal again or run fusermount3 -u %s", *mountPoint, err, *mountPoint)
					watch.End()
				}
			})
		}
	}()

	log.Println("Filesystem mounted. Press Ctrl+C to unmount.")
	server.Wait()
	unmounted.Store(true)
	watch.Start()
	goclusterfs.ShutdownStep("API server stop", stopAPI)

	exit := cli.ExitOK
	goclusterfs.ShutdownStep("node registry stop", nodeReg.Stop)
	if err := gfs.Close(); err != nil {
		log.Printf("Closing the filesystem failed: %v; run goclusterfs-fsck %s before the next mount", err, *devicePath)
		exit = cli.ExitFailure
	}
	goclusterfs.ShutdownStep("broadcast stop", func() {
		if err := cacheMgr.StopBroadcast(); err != nil {
			log.Printf("Warning: Failed to stop broadcast: %v", err)
		}
	})

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

	// Print final cache stats
	cacheStats := cacheMgr.Stats()
	log.Printf("Cache stats: %s", cacheStats)
	if cacheStats.BroadcastEnabled {
		log.Printf("Broadcast stats: sent=%d, received=%d, duplicates=%d, lost=%d",
			cacheStats.InvalidationsSent, cacheStats.InvalidationsReceived, cacheStats.BroadcastDuplicates, cacheStats.BroadcastLost)
	}

	log.Println("Filesystem unmounted.")
	_ = dev.Close()
	watch.End()
	os.Exit(exit)
}

const exitCodesHelp = `
Exit codes:
  0: The filesystem was mounted and later unmounted cleanly
  8: The mount failed, or stopped on an error
  16: Usage error; nothing was changed
`

// failf logs why the mount cannot go on and exits with cli.ExitFailure.
func failf(format string, args ...any) {
	log.Printf(format, args...)
	os.Exit(cli.ExitFailure)
}

// usageFailf reports a bad flag or setting and exits with cli.ExitUsage.
func usageFailf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "Error: "+format+"\n", args...)
	fmt.Fprintf(os.Stderr, "Run %s -h to see the flags.\n", os.Args[0])
	os.Exit(cli.ExitUsage)
}

func statsSources(cacheMgr *cache.Manager, lockMgr *lock.Manager, dirMgr *dir.Manager, j *journal.Journal, trimMgr *trim.Manager, bm *bitmap.Region, nodeReg *node.Registry) stats.Sources {
	src := stats.Sources{Cache: cacheMgr, Locks: lockMgr, Dirs: dirMgr, Blocks: bm, Nodes: nodeReg, BlockSize: goclusterfs.BlockSize}
	if j != nil {
		src.Journal = j
	}
	if trimMgr != nil {
		src.Trim = trimMgr
	}
	return src
}

var ownerStates = map[node.Occupancy]lock.OwnerState{
	node.OccupiedLive:      lock.OwnerAlive,
	node.OccupiedFailing:   lock.OwnerFailing,
	node.OccupiedReplaying: lock.OwnerRecovering,
	node.OccupiedLapsed:    lock.OwnerLost,
	node.Unoccupied:        lock.OwnerReleased,
}

// ownReplayRetry is the first wait before a mount retries its own journal
// replay; tests shorten it.
var ownReplayRetry = time.Second

// recoverOwnJournal replays the journal this node's slot holds from its
// previous owner. A replay that gave up waiting for another node's lock is
// retried with backoff. Any other failure, such as a record that does not
// decode or an I/O error, cannot get better by waiting, so it stops the mount.
func recoverOwnJournal(dev *storage.Device, offset, size uint64, handler journal.RecoveryHandler, slot int) (*journal.ScanRecoveryResult, error) {
	delay := ownReplayRetry
	for {
		result, err := journal.ScanAndRecover(dev, offset, size, handler)
		if err == nil {
			return result, nil
		}
		if !errors.Is(err, lock.ErrLockTimeout) {
			return nil, err
		}
		log.Printf("[WARN] Journal recovery for slot %d is waiting for another node's lock: %v; retrying in %v", slot, err, delay)
		time.Sleep(delay)
		delay = min(2*delay, 30*time.Second)
	}
}

// adoptLocks takes over a dead node's exclusive locks and clears its read holds
// before its journal is replayed, and returns the function that frees the
// exclusive locks after a replay that succeeded. After a failed replay the locks
// stay held for the next attempt.
func adoptLocks(locks *lock.Manager, owner [16]byte, slot int) func(replayed bool) {
	adopted, err := locks.AdoptOwner(owner, slot)
	if err != nil {
		log.Printf("Warning: Failed to take over the locks of dead node %x: %v", owner[:8], err)
	}
	return func(replayed bool) {
		if !replayed {
			locks.ForgetAdopted(adopted)
			log.Printf("[WARN] The journal replay of dead node %x failed; its %d lock records stay held until a replay succeeds", owner[:8], len(adopted))
			return
		}
		if err := locks.ReleaseAdopted(adopted); err != nil {
			log.Printf("Warning: Failed to release the locks taken over from dead node %x: %v", owner[:8], err)
		}
	}
}
