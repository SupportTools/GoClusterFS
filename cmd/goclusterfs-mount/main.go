// Command mount mounts a GoClusterFS filesystem.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"maps"
	"math"
	"net/netip"
	"os"
	"os/signal"
	"runtime/pprof"
	"slices"
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
	"github.com/supporttools/GoClusterFS/internal/fence"
	"github.com/supporttools/GoClusterFS/internal/fileops"
	"github.com/supporttools/GoClusterFS/internal/goclusterfs"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/logging"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/quota"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/reserve"
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
		allowOther = flag.Bool("allow-other", false, "Let every user on the host, not only the one that mounted, use the filesystem, with the kernel checking each file's owner, group and mode bits (allow_other and default_permissions)")
		preflight  = flag.Bool("preflight", false, "Check that this host can mount -device on -mount: FUSE, the device, the mount point and the ports. Prints PASS, WARN or FAIL with a fix for each, changes nothing, and exits 8 if a check fails")

		// Cache configuration
		cacheStrict   = flag.Bool("cache-strict", false, "Check every cached inode against the device before using it; slower, but other nodes' changes show at once")
		cacheTTL      = flag.Duration("cache-ttl", 5*time.Second, "How long a cached inode is used before it is checked against the device; another node's change can take this long to show when its broadcast is lost")
		attrTimeout   = flag.Duration("attr-timeout", time.Second, "How long the kernel caches file attributes and directory entries; stat and ls can show another node's change this late. Opening a file always sees it, except that a name another node renamed away can still open the moved file")
		directIO      = flag.Bool("direct-io", false, "Send every read and write of file data to the filesystem instead of the kernel page cache, so a read never returns cached pages older than another node's write. Slower for small and repeated reads, and shared mmap of files fails")
		cacheInodes   = flag.Int("cache-inodes", cache.DefaultConfig().InodeCacheSize, "Most inodes the inode cache holds; 0 turns it off")
		cacheExtents  = flag.Int("cache-extents", cache.DefaultConfig().ExtentCacheSize, "Most block mappings the extent cache holds; 0 turns it off")
		cacheNegative = flag.Int("cache-negative", cache.DefaultConfig().NegativeCacheSize, "Most not-found inode lookups the negative cache holds; 0 turns it off")
		dirIndex      = flag.Int("dir-index-entries", dir.DefaultIndexEntries, "Names kept in in-memory directory indexes, about 100 bytes each; 0 turns them off")

		// Journal configuration
		journalEnabled = flag.Bool("journal", true, "Write-ahead journal for crash consistency. Off, a crash of this node can leave the filesystem inconsistent: testing only")
		fencingMode    = flag.String("fencing", "auto", "With -format: pr fences dead nodes with persistent reservations and fails if the device lacks them, none formats without fencing, and auto picks pr when the device supports it")
		requireFencing = flag.Bool("require-fencing", false, "Refuse to mount a volume formatted without fencing, on which a stalled node's last writes can land after another node takes over")
		unsafeUnfenced = flag.Bool("unsafe-allow-unfenced", false, "Let this node mount a volume without storage fencing while other nodes have it mounted. UNSAFE: a node that pauses, as a VM its hypervisor paused, can resume and write after another node replayed its journal, corrupting file data that fsck cannot detect. Without it, such a mount refuses to start; a node mounted alone needs no flag")
		skipRecovery   = flag.Int("skip-recovery", -1, "Emergency use only: discard the journal of registry slot N instead of replaying it, if this mount takes slot N, losing the writes it holds; -1 replays as normal")

		// Network broadcast configuration
		broadcastEnabled = flag.Bool("broadcast", true, "Send and receive cache invalidations over UDP so other nodes see changes sooner; correctness does not depend on it")
		multicastGroup   = flag.String("mcast-group", "239.192.74.75", "Multicast group for broadcast messages; the same on every node")
		multicastPort    = flag.Int("mcast-port", broadcast.DefaultConfig().MulticastPort, "UDP port for multicast messages; the same on every node")
		unicastPort      = flag.Int("unicast-port", broadcast.DefaultConfig().UnicastPort, "UDP port for unicast messages; must differ from -mcast-port")
		broadcastSecret  = flag.String("broadcast-secret", "", "Secret that signs broadcast messages, the same on every node; visible to local users in ps. Empty leaves them unauthenticated: any host that can reach the UDP ports can make this node drop its caches")
		broadcastIface   = flag.String("broadcast-iface", "", "Network interface for multicast; empty lets the kernel choose")
		advertiseAddr    = flag.String("advertise-addr", "", "IP or IP:port where peers that miss this node's multicast send it unicast, written to its registry slot; empty takes the address of -broadcast-iface or of the route to -mcast-group, and -unicast-port. Set it for NAT or a host with several addresses")
		broadcastPeers   = flag.Int("broadcast-max-peers", broadcast.DefaultMaxPeers, "Most nodes the broadcast peer table and sequence tracking hold")
		broadcastBeat    = flag.Duration("broadcast-heartbeat", 30*time.Second, "How often this node sends a broadcast heartbeat; a mounted node not heard for 3 of these is reported")

		// TRIM configuration
		trimEnabled   = flag.Bool("trim", true, "Discard freed blocks on the device (BLKDISCARD) in batches; does nothing on image files")
		trimBatchSize = flag.Int("trim-batch", 256, "Freed blocks to collect before a discard")
		blockReserve  = flag.Int("block-reserve", reserve.DefaultSize, "Data blocks each node takes from the bitmap at once and hands out without the cluster-wide bitmap lock; 0 turns reserves off. Needs the journal")
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
	dumpGoroutinesOnSIGUSR1()

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
		HeartbeatInterval:    *broadcastBeat,
		Secret:               *broadcastSecret,
		Interface:            *broadcastIface,
		MaxPeers:             *broadcastPeers,
	}
	if *broadcastBeat <= 0 {
		log.Fatalf("-broadcast-heartbeat must be positive, got %v", *broadcastBeat)
	}
	if *broadcastPeers < 1 {
		log.Fatalf("-broadcast-max-peers must be at least 1, got %d", *broadcastPeers)
	}
	var advertise netip.AddrPort
	if *broadcastEnabled {
		addr, err := broadcast.AdvertiseAddr(networkCfg, *advertiseAddr)
		switch {
		case err != nil && *advertiseAddr != "":
			log.Fatalf("-advertise-addr: %v", err)
		case err != nil:
			log.Printf("[WARN] Broadcast: no unicast address to advertise (%v), so peers that miss this node's multicast cannot reach it. Set -advertise-addr", err)
		default:
			advertise = addr
		}
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
	dev.SetGoneHandler(func(err error) {
		failf("Device %s went away: %v; remount after the device returns", *devicePath, err)
	})

	// Get device size (returns int64, no error)
	devSize := dev.Size()

	log.Printf("Device: %s (%d MB)", *devicePath, devSize/(1024*1024))

	var sb *superblock.Superblock
	if *format {
		if *fencingMode != "auto" && *fencingMode != "pr" && *fencingMode != "none" {
			usageFailf("-fencing=%s: use auto, pr or none", *fencingMode)
		}
		formatFence := formatFencer(*devicePath, *fencingMode)
		sb, err = superblock.Format(dev, *inodeRatio)
		if err == nil && formatFence.Kind() == "pr" {
			sb.RequiredFlags |= superblock.RequiredReservations
			err = superblock.Write(dev, sb)
		}
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
	fencer := joinFencing(dev, *devicePath, sb, nodeReg, *requireFencing)
	hostname, _ := os.Hostname()
	if *skipRecovery >= 0 {
		if *skipRecovery >= int(sb.MaxNodes) {
			usageFailf("-skip-recovery=%d names no slot; slots are 0 to %d", *skipRecovery, sb.MaxNodes-1)
		}
		offset := sb.JournalRegionOffset + uint64(*skipRecovery)*sb.JournalSlotSize()
		loss, err := skipRecoveryLoss(nodeReg, dev, offset, sb.JournalSlotSize(), *skipRecovery)
		if err != nil {
			failf("-skip-recovery=%d: reading the node registry failed: %v; check the device, then mount again", *skipRecovery, err)
		}
		if loss != "" {
			failf("-skip-recovery=%d refused: %s. Nothing may be lost while nodes use the filesystem, so stop the cluster: "+
				"unmount every node, mount one with -skip-recovery=%d, unmount it, and run goclusterfs-fsck -repair before mounting the others",
				*skipRecovery, loss, *skipRecovery)
		}
		nodeReg.PreferSlot(*skipRecovery)
	}
	nodeReg.SetLockLayout(lock.LayoutID())
	nodeReg.SetAdvertiseAddr(advertise)
	slotIdx, err := nodeReg.Register(ctx, hostname)
	if errors.Is(err, node.ErrLockLayout) {
		failf("Cannot join the cluster: %v. Nodes of different GoClusterFS versions would not lock each other out: unmount that node, then mount every node with one version", err)
	}
	if errors.Is(err, node.ErrNoFreeSlots) {
		failf("Cannot join the cluster: all %d node slots are taken. List them with goclusterfs-fsck -list-slots %s, and free the slot of a node that will not come back with -release-slot", node.MaxNodes, *devicePath)
	}
	if err != nil {
		failf("Cannot join the cluster: %v; check the device, then mount again", err)
	}
	if fencer.Kind() == "none" {
		joinUnfenced(nodeReg, *devicePath, *unsafeUnfenced)
	}
	nodeReg.SetSlotLostHandler(func(loss node.SlotLoss) {
		storage.AwaitStoppedWrite()
		failf("%s; stopping so two nodes never share a journal. "+
			"Check that the device is reachable and that this node did not stall, then mount again", loss)
	})
	dev.SetFencedHandler(nodeReg.Fenced)
	stopReservationCheck := make(chan struct{})
	if pr, ok := fencer.(*fence.PR); ok {
		go keepReserved(pr, stopReservationCheck)
		go warnStaleKeys(pr, nodeReg, *devicePath, stopReservationCheck)
	}
	if err := nodeReg.StartHeartbeat(); err != nil {
		failf("Failed to start node heartbeat: %v", err)
	}
	dev.SetWriteGate(nodeReg.WriteGate)
	dev.SetWriteFresh(nodeReg.WriteFresh)
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
	var mountReplay stats.MountReplay
	if *skipRecovery >= 0 && !skip {
		log.Printf("Journal: ignoring -skip-recovery=%d: this mount took slot %d, so it replays that slot's journal", *skipRecovery, slotIdx)
	}
	// -journal=false only stops this mount journaling its own writes: the slot's journal is still replayed, or reset
	// below, so no later mount replays it over newer data.
	if !skip {
		// Set up recovery context with our managers
		recoveryCtx := &journal.RecoveryContext{
			Dev:       dev,
			InodeTbl:  inodeTbl,
			BitmapMgr: bm,
			DirMgr:    dirMgr,
			ExtentMgr: extentMgr,
			// Nothing is open on this node yet, so only peers can hold a file this replay orphans.
			OrphanHolders: func(uint32) (uint64, error) {
				live, err := nodeReg.LiveSlots()
				return live &^ (1 << slotIdx), err
			},
			AddOrphan: func(num uint32, birth uint64) error {
				return nodeReg.AddOrphan(node.Orphan{Inode: num, Birth: uint32(birth)})
			},
			FreeSFBSlot: func(ctx context.Context, packed uint64) {
				goclusterfs.FreeSFBSlot(ctx, dev, lockMgr, dataOffset, packed)
			},
		}
		// Use per-transaction handler for self-recovery
		recoveryHandler := journal.CreateTxnRecoveryHandler(recoveryCtx)
		releaseReserved := recoveryCtx.ReleaseReserved(lock.ForReplay(context.Background()))

		// Replay a dirty journal left in this slot's region by its previous owner
		result, err := recoverOwnJournal(dev, journalOffset, journalSize, recoveryHandler, releaseReserved, slotIdx)
		if err != nil {
			failf("Journal recovery for slot %d failed: %v. Check the device, then mount again; to discard this journal instead, mount with -skip-recovery=%d", slotIdx, err, slotIdx)
		}
		if result != nil && result.WasNeeded {
			mountReplay.Transactions = result.Stats.TxnCommitted
			log.Printf("Journal recovery: replayed %d committed transactions (%d bytes scanned) from crashed node %x",
				result.Stats.TxnCommitted, result.Stats.BytesScanned, result.NodeUUID[:8])
		}
	} else {
		mountReplay.Discarded = true
		log.Printf("Journal: WARNING - discarding the journal of slot %d (-skip-recovery)", slotIdx)
		if !*journalEnabled {
			if err := resetJournalHeader(dev, journalOffset); err != nil {
				failf("Discarding the journal of slot %d failed: %v; check the device, then mount again", slotIdx, err)
			}
		}
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
		cacheMgr.WatchBroadcastPeers(expectedPeers(nodeReg))
		log.Printf("Broadcast: enabled on %s:%d, unicast port %d, advertising %v (secret=%v)",
			*multicastGroup, *multicastPort, *unicastPort, advertise, *broadcastSecret != "")
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
	gfs.SetSymlinks(sb.RequiredFlags&superblock.RequiredSymlinks != 0)
	gfs.SetFailStop(func(err error) {
		failf("[ERROR] Stopping this node and keeping its locks: %v. A peer replays this node's journal and finishes the rename, or the next mount here does", err)
	})
	var res *reserve.Manager
	if j != nil && *blockReserve > 0 {
		res = reserve.New(reserve.Config{Size: *blockReserve}, bm, j, extentMgr.ReleaseBlocks, func() int {
			active, _ := nodeReg.GetActiveNodes()
			return len(active)
		})
		res.SetBroadcast(cacheMgr.BroadcastReturnReserves)
		cacheMgr.OnReturnReserves(res.AskReturn)
		bm.SetNoSpaceWait(res)
		j.SetCheckpointHook(func() { res.Refresh(context.Background()) })
		gfs.SetReserve(res)
		res.Start()
		log.Printf("Block reserve: up to %d blocks per node", *blockReserve)
	}

	// Set up cross-node recovery handler for failed node journal replay
	recoveryCtx := &journal.RecoveryContext{
		Dev:       dev,
		InodeTbl:  inodeTbl,
		BitmapMgr: bm,
		DirMgr:    dirMgr,
		ExtentMgr: extentMgr,
		OrphanHolders: func(num uint32) (uint64, error) {
			live, err := nodeReg.LiveSlots()
			if !gfs.HasOpen(num) {
				live &^= 1 << slotIdx
			}
			return live, err
		},
		AddOrphan: func(num uint32, birth uint64) error {
			return nodeReg.AddOrphan(node.Orphan{Inode: num, Birth: uint32(birth)})
		},
		FreeSFBSlot: func(ctx context.Context, packed uint64) {
			goclusterfs.FreeSFBSlot(ctx, dev, lockMgr, dataOffset, packed)
		},
	}
	replayPeer := journal.CreateRecoveryHandler(recoveryCtx)
	nodeReg.SetRecoveryHandler(func(info *node.FailedNodeInfo) error {
		err := replayPeer(info)
		if err == nil {
			err = gfs.ForgetSlot(lock.ForReplay(context.Background()), info.SlotIndex, info.Orphans)
		}
		if err == nil {
			gfs.RollBackSnapshotsOf(context.Background(), info.SlotIndex, info.UUID)
		}
		if errors.Is(err, lock.ErrLockTimeout) {
			return fmt.Errorf("%w: %w", node.ErrReplayContended, err)
		}
		return err
	})
	gfs.SetOrphanRegistry(nodeReg)
	gfs.SetDirectIO(*directIO)
	nodeReg.SetOrphanHandler(gfs.LearnOrphans)
	cacheMgr.OnOrphans(nodeReg.KickOrphans)
	// Nothing is open here yet: let go of what this slot held before a crash.
	if err := gfs.ForgetSlot(lock.ForReplay(context.Background()), slotIdx, nil); err != nil {
		log.Printf("[WARN] letting go of orphans this slot held before: %v", err)
	}

	// Start failure detection to monitor for crashed nodes
	nodeReg.StartFailureDetection()
	log.Println("Cross-node recovery enabled: monitoring for failed nodes")

	// Format if requested
	if *format {
		log.Println("Formatting filesystem...")
		if err := gfs.InitRoot(ctx); err != nil {
			failf("Failed to create the root directory: %v; check the device, then format again", err)
		}
		if err := quota.Enable(ctx, dev, sb, inodeTbl, bm); err != nil {
			failf("Failed to create the quota table: %v; check the device, then format again", err)
		}
		log.Printf("Filesystem formatted successfully, with project quotas in inode %d", sb.QuotaInode)
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
			MaxWrite:      goclusterfs.MaxWrite,
			MaxReadAhead:  1 << 20,
			DisableXAttrs: false,
			AllowOther:    *allowOther,
		},
		AttrTimeout:  attrTimeout,
		EntryTimeout: attrTimeout,
	}

	if *allowOther {
		mountOpts.Options = append(mountOpts.Options, "default_permissions")
	}

	// Mount filesystem using go-fuse v2 API
	log.Printf("Mounting at %s...", *mountPoint)
	server, err := fs.Mount(*mountPoint, gfs, mountOpts)
	if err != nil && *allowOther && os.Getuid() != 0 {
		failf("Failed to mount at %s: %v. A mount by a user other than root needs user_allow_other in /etc/fuse.conf for -allow-other", *mountPoint, err)
	}
	if err != nil {
		failf("Failed to mount at %s: %v. Check that /dev/fuse exists, fusermount3 is installed, and nothing else is mounted there (fusermount3 -u %s)", *mountPoint, err, *mountPoint)
	}
	gfs.MarkMounted()

	// Start API server for management and monitoring
	var apiServer *api.Server
	var sampler *stats.Sampler
	if *apiEnabled {
		var err error
		apiServer, err = api.NewServer(apiCfg)
		if err != nil {
			log.Printf("Warning: Failed to create the API server: %v; the mount continues without it", err)
		} else {
			sampler = stats.StartSampler(statsSources(cacheMgr, lockMgr, dirMgr, j, trimMgr, bm, nodeReg, res, mountReplay))
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
				failingReplays:   nodeReg.FailingReplays,
				broadcastOn:      cacheMgr.BroadcastEnabled(),
				broadcastProblem: cacheMgr.BroadcastProblem,
				silentPeers:      cacheMgr.SilentBroadcastPeers,
				heartbeatAge:     func() time.Duration { return nodeReg.Stats().HeartbeatAge },
				fencing:          fencer.Kind(),
			})
			apiServer.SetFileService(fileops.New(gfs))
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

	// The API stops before the unmount, so no snapshot or clone request runs while the filesystem shuts down.
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
	close(stopReservationCheck)
	unmounted.Store(true)
	watch.Start()
	goclusterfs.ShutdownStep("API server stop", stopAPI)

	exit := cli.ExitOK
	goclusterfs.ShutdownStep("orphan release", func() {
		if err := gfs.ForgetSlot(context.Background(), slotIdx, nil); err != nil {
			log.Printf("[WARN] letting go of unlinked files at unmount: %v; the next node in slot %d does it", err, slotIdx)
		}
	})
	goclusterfs.ShutdownStep("node registry stop", nodeReg.Stop)
	leftSlot := false
	gfs.SetSlotRelease(func() {
		if err := nodeReg.Release(); err != nil {
			log.Printf("[INFO] Leaving registry slot %d for a running node to clear: %v", slotIdx, err)
			return
		}
		leftSlot = true
	})
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

	unregister := func() (bool, error) { return false, fencer.Unregister() }
	if leftSlot {
		unregister = fencer.Leave
	}
	switch earlier, err := unregister(); {
	case earlier && err == nil:
		log.Printf("[INFO] This host's session held no reservation key, as after an iSCSI logout and login: registered the key on it, removed the key from the earlier session and unregistered it")
	case errors.Is(err, fence.ErrFenced):
		log.Printf("[INFO] This host's session holds no reservation key to remove: a peer removed it, or the target keeps it under an earlier session, as after an iSCSI logout and login. The node that replays slot %d removes it", slotIdx)
	case err != nil && leftSlot:
		log.Printf("[WARN] Removing this node's reservation key failed: %v; no peer removes it, so remove it with goclusterfs-fsck -clear-reservations once no node runs", err)
	case err != nil:
		log.Printf("[WARN] Removing this node's reservation key failed: %v; the node that replays slot %d removes it", err, slotIdx)
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

// formatFencer registers a reservation key before -format writes anything, if mode and the device allow fencing.
// The node's own key replaces it once the registry exists.
func formatFencer(devicePath, mode string) fence.Fencer {
	if mode == "none" {
		return fence.None{}
	}
	pr := fence.Block(openForFencing(devicePath), fence.ToolKey())
	err := pr.Register()
	switch {
	case errors.Is(err, fence.ErrUnsupported) && mode == "auto":
		log.Printf("[WARN] %s: %v, so this volume is formatted without storage fencing: "+
			"if a node stalls, its last writes can land after another node takes over and corrupt metadata. "+
			"Format a whole disk with persistent reservations, and mount with -require-fencing to refuse unfenced volumes", devicePath, err)
		return fence.None{}
	case errors.Is(err, fence.ErrPartition):
		failf("-fencing=%s: %s: %v; format the whole disk, or pass -fencing=none", mode, devicePath, err)
	case err != nil:
		failf("-fencing=%s: registering a reservation key on %s failed: %v; format as root on a device with persistent reservations, or pass -fencing=none", mode, devicePath, err)
	}
	if err := pr.Reserve(); err != nil {
		failf("-fencing=%s: reserving %s failed: %v; once no node runs, clear it with goclusterfs-fsck -clear-reservations %s, or pass -fencing=none", mode, devicePath, err, devicePath)
	}
	return pr
}

// joinFencing makes the registry register this node's reservation key before its first write on a fenced volume, and
// revoke dead nodes' keys. On an unfenced volume it warns, or with require refuses to mount.
func joinFencing(dev *storage.Device, devicePath string, sb *superblock.Superblock, reg *node.Registry, require bool) fence.Fencer {
	if sb.RequiredFlags&superblock.RequiredReservations == 0 {
		if require {
			failf("%s has no storage fencing and -require-fencing is set; format it with -fencing=pr on a device with persistent reservations", devicePath)
		}
		log.Printf("[WARN] %s has no storage fencing: if a node stalls, its last writes can land after another node takes over and corrupt metadata. "+
			"Format on a device with persistent reservations, and mount with -require-fencing to refuse unfenced volumes", devicePath)
		return fence.None{}
	}
	f := openForFencing(devicePath)
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		failf("Another process on this host has %s open for fencing, maybe a stopped goclusterfs-mount (%v). "+
			"A host has one reservation key per device, so stop that process first", devicePath, err)
	}
	pr := fence.Block(f, fence.NodeKey(reg.LocalUUID()))
	reg.SetJoin(func() error {
		anew, err := pr.Join()
		switch {
		case errors.Is(err, fence.ErrForeignReservation):
			failf("%s is fenced, and reserving it failed: %v; once no node runs, clear it with goclusterfs-fsck -clear-reservations %s", devicePath, err, devicePath)
		case err != nil:
			failf("%s is fenced, and registering this node's reservation key failed: %v; mount as root on a host that reaches the device's persistent reservations", devicePath, err)
		case anew:
			log.Printf("[WARN] Fencing: the device refused this node's reserve although it registered its key, as LIO does after a key this host " +
				"registered from an earlier iSCSI session is revoked; registered the key anew")
		}
		log.Printf("Fencing: persistent reservation key %#x", pr.Key())
		return nil
	}, pr.RegisterFresh)
	// LIO tags every command with the holder's key, so PREEMPT AND ABORT also drops this node's in-flight I/O without a status.
	reg.SetPreempter(func(owner [16]byte) error {
		return dev.Quiesce(func() error { return pr.Preempt(fence.NodeKey(owner)) })
	})
	return pr
}

// openForFencing opens the device again for its reservation ioctls and host lock, which must outlive the device's
// close at unmount: the node removes its key after its last write.
func openForFencing(devicePath string) *os.File {
	f, err := os.OpenFile(devicePath, os.O_RDWR, 0)
	if err != nil {
		failf("Opening %s for its persistent reservations failed: %v", devicePath, err)
	}
	return f
}

// keepReserved checks the device's reservation on every failure scan until stop closes. A device without a reservation
// takes writes from any host, so a node it revoked would go on writing: keepReserved reserves again while this node's
// key stays registered, and stops the node if the key is gone too.
func keepReserved(pr *fence.PR, stop <-chan struct{}) {
	tick := time.NewTicker(node.FailureCheckInterval)
	defer tick.Stop()
	failing := false
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		}
		reserved, err := pr.KeepReserved()
		switch {
		case errors.Is(err, fence.ErrFenced):
			failf("Fencing: the device holds no reservation and no longer lists this node's key %#x, as after a storage reset "+
				"or a revoke while it was unreserved; stopping so two nodes never share a journal. Mount again", pr.Key())
		case errors.Is(err, fence.ErrUnsupported):
			log.Printf("[WARN] Fencing: cannot check that the device keeps its reservation: %v", err)
			return
		case err != nil:
			if !failing {
				log.Printf("[WARN] Fencing: checking the device's reservation failed: %v", err)
			}
			failing = true
		default:
			failing = false
			if reserved {
				log.Printf("[WARN] Fencing: the device had lost its reservation while this node's key %#x stayed registered, "+
					"so a revoked node could have gone on writing; reserved it again", pr.Key())
			}
		}
	}
}

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

func statsSources(cacheMgr *cache.Manager, lockMgr *lock.Manager, dirMgr *dir.Manager, j *journal.Journal, trimMgr *trim.Manager, bm *bitmap.Region, nodeReg *node.Registry, res *reserve.Manager, mountReplay stats.MountReplay) stats.Sources {
	src := stats.Sources{Cache: cacheMgr, Locks: lockMgr, Dirs: dirMgr, Blocks: bm, Nodes: nodeReg, BlockSize: goclusterfs.BlockSize, MountReplay: mountReplay}
	if res != nil {
		src.Reserve = res
	}
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

// runningWatch is how long -skip-recovery watches the registry for a running
// node: past the time peers still count a node with a late heartbeat as alive.
var runningWatch = node.HeartbeatTimeout + node.HeartbeatInterval

// skipRecoveryLoss says why discarding the journal of slot with -skip-recovery
// would drop committed transactions while another node has the filesystem
// mounted, or returns "" when it would not.
func skipRecoveryLoss(reg *node.Registry, dev *storage.Device, offset, size uint64, slot int) (string, error) {
	pending, readErr := journal.PendingCommitted(dev, offset, size)
	if readErr == nil && pending == 0 {
		return "", nil
	}
	running, err := reg.RunningSlots(runningWatch)
	if err != nil {
		return "", err
	}
	delete(running, slot)
	if len(running) == 0 {
		return "", nil
	}
	other := slices.Min(slices.Collect(maps.Keys(running)))
	switch {
	case readErr != nil:
		return fmt.Sprintf("registry slot %d has a running node, and slot %d's journal cannot be read (%v), so this mount cannot tell what discarding it drops", other, slot, readErr), nil
	case pending == 1:
		return fmt.Sprintf("registry slot %d has a running node, and slot %d's journal holds 1 committed transaction that discarding it drops", other, slot), nil
	}
	return fmt.Sprintf("registry slot %d has a running node, and slot %d's journal holds %d committed transactions that discarding it drops", other, slot, pending), nil
}

// resetJournalHeader zeroes the journal header, as goclusterfs-fsck -release-slot does, so no mount replays it.
func resetJournalHeader(dev *storage.Device, offset uint64) error {
	if _, err := dev.WriteAt(storage.NewAlignedBuffer(storage.SectorSize).Bytes(), int64(offset)); err != nil {
		return err
	}
	return dev.Sync()
}

// recoverOwnJournal replays the journal this node's slot holds from its
// previous owner. A replay that gave up waiting for another node's lock is
// retried with backoff. Any other failure, such as a record that does not
// decode or an I/O error, cannot get better by waiting, so it stops the mount.
func recoverOwnJournal(dev *storage.Device, offset, size uint64, handler journal.RecoveryHandler, release func([]uint64) error, slot int) (*journal.ScanRecoveryResult, error) {
	delay := ownReplayRetry
	for {
		result, err := journal.ScanAndRecover(dev, offset, size, handler, release)
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

// dumpGoroutinesOnSIGUSR1 writes every goroutine's stack to the log on SIGUSR1 and keeps the mount running.
func dumpGoroutinesOnSIGUSR1() {
	dumps := make(chan os.Signal, 1)
	signal.Notify(dumps, syscall.SIGUSR1)
	go func() {
		for range dumps {
			log.Println("goroutine dump begins")
			_ = pprof.Lookup("goroutine").WriteTo(log.Writer(), 2)
			log.Println("goroutine dump ends")
		}
	}()
}
