package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

const (
	namespace = "goclusterfs"
)

// MetricsHandler returns an HTTP handler for Prometheus metrics scraping.
// It uses a custom collector that pulls fresh stats from the StatsProvider.
func (s *Server) MetricsHandler() http.Handler {
	// Create a custom registry to avoid default Go metrics
	registry := prometheus.NewRegistry()

	// Register the filesystem collector
	collector := newFSCollector(s)
	registry.MustRegister(collector)

	// Also register standard process and Go metrics if desired
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())

	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// fsCollector implements prometheus.Collector interface to expose filesystem stats.
type fsCollector struct {
	server *Server

	// Descriptors for all metrics
	upDesc *prometheus.Desc

	// Cache metrics
	cacheInodeSizeDesc       *prometheus.Desc
	cacheInodeHitsDesc       *prometheus.Desc
	cacheInodeMissesDesc     *prometheus.Desc
	cacheInodeHitRateDesc    *prometheus.Desc
	cacheExtentSizeDesc      *prometheus.Desc
	cacheExtentHitsDesc      *prometheus.Desc
	cacheExtentMissesDesc    *prometheus.Desc
	cacheExtentHitRateDesc   *prometheus.Desc
	cacheNegativeSizeDesc    *prometheus.Desc
	cacheNegativeHitsDesc    *prometheus.Desc
	cacheNegativeMissesDesc  *prometheus.Desc
	cacheNegativeHitRateDesc *prometheus.Desc
	cacheLockHintSizeDesc    *prometheus.Desc
	cacheLockHintHitsDesc    *prometheus.Desc
	cacheLockHintMissesDesc  *prometheus.Desc
	cacheLockHintHitRateDesc *prometheus.Desc

	// Journal metrics
	journalStateDesc          *prometheus.Desc
	journalHeadOffsetDesc     *prometheus.Desc
	journalTailOffsetDesc     *prometheus.Desc
	journalAvailableSpaceDesc *prometheus.Desc
	journalActiveTxnCountDesc *prometheus.Desc
	journalCommitCountDesc    *prometheus.Desc
	journalBytesWrittenDesc   *prometheus.Desc
	journalLastCheckpointDesc *prometheus.Desc
	journalBatchSyncCountDesc *prometheus.Desc

	// Trim metrics
	trimOperationsDesc    *prometheus.Desc
	trimBytesDesc         *prometheus.Desc
	trimBlocksDesc        *prometheus.Desc
	trimPendingBlocksDesc *prometheus.Desc
	trimErrorsDesc        *prometheus.Desc
	trimEnabledDesc       *prometheus.Desc

	// Lock metrics
	lockHintCacheHitsDesc    *prometheus.Desc
	lockHintCacheMissesDesc  *prometheus.Desc
	lockHintCacheHitRateDesc *prometheus.Desc
	lockActiveLocksDesc      *prometheus.Desc
	lockPendingLocksDesc     *prometheus.Desc

	// SFB (Sparse File Bitmap) metrics
	sfbTotalBlocksDesc        *prometheus.Desc
	sfbAllocatedBlocksDesc    *prometheus.Desc
	sfbFreeBlocksDesc         *prometheus.Desc
	sfbAllocationPercentDesc  *prometheus.Desc
	sfbFragmentationDesc      *prometheus.Desc
	sfbLargestFreeExtentDesc  *prometheus.Desc
	sfbAllocationsDesc        *prometheus.Desc
	sfbDeallocationsDesc      *prometheus.Desc
	sfbAllocationFailuresDesc *prometheus.Desc

	// Cluster metrics
	clusterSizeDesc         *prometheus.Desc
	clusterHealthyNodesDesc *prometheus.Desc
	clusterIsLeaderDesc     *prometheus.Desc
}

func newFSCollector(s *Server) *fsCollector {
	return &fsCollector{
		server: s,

		// Up metric
		upDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "up"),
			"Whether GoClusterFS is up and running",
			nil, nil,
		),

		// Cache - Inode
		cacheInodeSizeDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "inode_size"),
			"Number of entries in the inode cache",
			nil, nil,
		),
		cacheInodeHitsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "inode_hits_total"),
			"Total inode cache hits",
			nil, nil,
		),
		cacheInodeMissesDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "inode_misses_total"),
			"Total inode cache misses",
			nil, nil,
		),
		cacheInodeHitRateDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "inode_hit_rate"),
			"Inode cache hit rate (0.0-1.0)",
			nil, nil,
		),

		// Cache - Extent
		cacheExtentSizeDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "extent_size"),
			"Number of entries in the extent cache",
			nil, nil,
		),
		cacheExtentHitsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "extent_hits_total"),
			"Total extent cache hits",
			nil, nil,
		),
		cacheExtentMissesDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "extent_misses_total"),
			"Total extent cache misses",
			nil, nil,
		),
		cacheExtentHitRateDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "extent_hit_rate"),
			"Extent cache hit rate (0.0-1.0)",
			nil, nil,
		),

		// Cache - Negative
		cacheNegativeSizeDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "negative_size"),
			"Number of entries in the negative cache",
			nil, nil,
		),
		cacheNegativeHitsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "negative_hits_total"),
			"Total negative cache hits",
			nil, nil,
		),
		cacheNegativeMissesDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "negative_misses_total"),
			"Total negative cache misses",
			nil, nil,
		),
		cacheNegativeHitRateDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "negative_hit_rate"),
			"Negative cache hit rate (0.0-1.0)",
			nil, nil,
		),

		// Cache - Lock Hint
		cacheLockHintSizeDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "lock_hint_size"),
			"Number of entries in the lock hint cache",
			nil, nil,
		),
		cacheLockHintHitsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "lock_hint_hits_total"),
			"Total lock hint cache hits",
			nil, nil,
		),
		cacheLockHintMissesDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "lock_hint_misses_total"),
			"Total lock hint cache misses",
			nil, nil,
		),
		cacheLockHintHitRateDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cache", "lock_hint_hit_rate"),
			"Lock hint cache hit rate (0.0-1.0)",
			nil, nil,
		),

		// Journal
		journalStateDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "journal", "state"),
			"Journal state (1=open, 0=closed)",
			nil, nil,
		),
		journalHeadOffsetDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "journal", "head_offset"),
			"Journal head offset in bytes",
			nil, nil,
		),
		journalTailOffsetDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "journal", "tail_offset"),
			"Journal tail offset in bytes",
			nil, nil,
		),
		journalAvailableSpaceDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "journal", "available_space_bytes"),
			"Available space in journal in bytes",
			nil, nil,
		),
		journalActiveTxnCountDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "journal", "active_transactions"),
			"Number of active transactions",
			nil, nil,
		),
		journalCommitCountDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "journal", "commits_total"),
			"Total number of commits",
			nil, nil,
		),
		journalBytesWrittenDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "journal", "bytes_written_total"),
			"Total bytes written to journal",
			nil, nil,
		),
		journalLastCheckpointDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "journal", "last_checkpoint_seq"),
			"Last checkpoint sequence number",
			nil, nil,
		),
		journalBatchSyncCountDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "journal", "batch_syncs_total"),
			"Total number of batch syncs",
			nil, nil,
		),

		// Trim
		trimOperationsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "trim", "operations_total"),
			"Total TRIM operations performed",
			nil, nil,
		),
		trimBytesDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "trim", "bytes_total"),
			"Total bytes trimmed",
			nil, nil,
		),
		trimBlocksDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "trim", "blocks_total"),
			"Total blocks trimmed",
			nil, nil,
		),
		trimPendingBlocksDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "trim", "pending_blocks"),
			"Number of blocks pending TRIM",
			nil, nil,
		),
		trimErrorsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "trim", "errors_total"),
			"Total TRIM errors",
			nil, nil,
		),
		trimEnabledDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "trim", "enabled"),
			"Whether TRIM is enabled (1) or disabled (0)",
			nil, nil,
		),

		// Lock
		lockHintCacheHitsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "lock", "hint_cache_hits_total"),
			"Total lock hint cache hits",
			nil, nil,
		),
		lockHintCacheMissesDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "lock", "hint_cache_misses_total"),
			"Total lock hint cache misses",
			nil, nil,
		),
		lockHintCacheHitRateDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "lock", "hint_cache_hit_rate"),
			"Lock hint cache hit rate (0.0-1.0)",
			nil, nil,
		),
		lockActiveLocksDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "lock", "active_locks"),
			"Number of currently active locks",
			nil, nil,
		),
		lockPendingLocksDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "lock", "pending_locks"),
			"Number of pending lock requests",
			nil, nil,
		),

		// SFB (Sparse File Bitmap)
		sfbTotalBlocksDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sfb", "total_blocks"),
			"Total number of blocks in the filesystem",
			nil, nil,
		),
		sfbAllocatedBlocksDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sfb", "allocated_blocks"),
			"Number of allocated blocks",
			nil, nil,
		),
		sfbFreeBlocksDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sfb", "free_blocks"),
			"Number of free blocks",
			nil, nil,
		),
		sfbAllocationPercentDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sfb", "allocation_ratio"),
			"Block allocation ratio (0.0-1.0)",
			nil, nil,
		),
		sfbFragmentationDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sfb", "fragmentation_ratio"),
			"Block fragmentation ratio (0.0-1.0)",
			nil, nil,
		),
		sfbLargestFreeExtentDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sfb", "largest_free_extent_blocks"),
			"Largest contiguous free extent in blocks",
			nil, nil,
		),
		sfbAllocationsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sfb", "allocations_total"),
			"Total block allocations",
			nil, nil,
		),
		sfbDeallocationsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sfb", "deallocations_total"),
			"Total block deallocations",
			nil, nil,
		),
		sfbAllocationFailuresDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sfb", "allocation_failures_total"),
			"Total block allocation failures",
			nil, nil,
		),

		// Cluster
		clusterSizeDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cluster", "size"),
			"Total number of nodes in the cluster",
			nil, nil,
		),
		clusterHealthyNodesDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cluster", "healthy_nodes"),
			"Number of healthy nodes in the cluster",
			nil, nil,
		),
		clusterIsLeaderDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cluster", "is_leader"),
			"Whether this node is the cluster leader (1) or not (0)",
			nil, nil,
		),
	}
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector to the provided channel.
func (c *fsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.upDesc

	// Cache
	ch <- c.cacheInodeSizeDesc
	ch <- c.cacheInodeHitsDesc
	ch <- c.cacheInodeMissesDesc
	ch <- c.cacheInodeHitRateDesc
	ch <- c.cacheExtentSizeDesc
	ch <- c.cacheExtentHitsDesc
	ch <- c.cacheExtentMissesDesc
	ch <- c.cacheExtentHitRateDesc
	ch <- c.cacheNegativeSizeDesc
	ch <- c.cacheNegativeHitsDesc
	ch <- c.cacheNegativeMissesDesc
	ch <- c.cacheNegativeHitRateDesc
	ch <- c.cacheLockHintSizeDesc
	ch <- c.cacheLockHintHitsDesc
	ch <- c.cacheLockHintMissesDesc
	ch <- c.cacheLockHintHitRateDesc

	// Journal
	ch <- c.journalStateDesc
	ch <- c.journalHeadOffsetDesc
	ch <- c.journalTailOffsetDesc
	ch <- c.journalAvailableSpaceDesc
	ch <- c.journalActiveTxnCountDesc
	ch <- c.journalCommitCountDesc
	ch <- c.journalBytesWrittenDesc
	ch <- c.journalLastCheckpointDesc
	ch <- c.journalBatchSyncCountDesc

	// Trim
	ch <- c.trimOperationsDesc
	ch <- c.trimBytesDesc
	ch <- c.trimBlocksDesc
	ch <- c.trimPendingBlocksDesc
	ch <- c.trimErrorsDesc
	ch <- c.trimEnabledDesc

	// Lock
	ch <- c.lockHintCacheHitsDesc
	ch <- c.lockHintCacheMissesDesc
	ch <- c.lockHintCacheHitRateDesc
	ch <- c.lockActiveLocksDesc
	ch <- c.lockPendingLocksDesc

	// SFB
	ch <- c.sfbTotalBlocksDesc
	ch <- c.sfbAllocatedBlocksDesc
	ch <- c.sfbFreeBlocksDesc
	ch <- c.sfbAllocationPercentDesc
	ch <- c.sfbFragmentationDesc
	ch <- c.sfbLargestFreeExtentDesc
	ch <- c.sfbAllocationsDesc
	ch <- c.sfbDeallocationsDesc
	ch <- c.sfbAllocationFailuresDesc

	// Cluster
	ch <- c.clusterSizeDesc
	ch <- c.clusterHealthyNodesDesc
	ch <- c.clusterIsLeaderDesc
}

// Collect is called by the Prometheus registry when collecting metrics.
// It fetches fresh stats from the server's StatsProvider.
func (c *fsCollector) Collect(ch chan<- prometheus.Metric) {
	// Always report up metric
	ch <- prometheus.MustNewConstMetric(c.upDesc, prometheus.GaugeValue, 1)

	// Get stats provider
	provider := c.server.GetStatsProvider()
	if provider == nil {
		return
	}

	// Collect cache stats
	if cache := provider.CacheStats(); cache != nil {
		c.collectCacheStats(ch, cache)
	}

	// Collect journal stats
	if journal := provider.JournalStats(); journal != nil {
		c.collectJournalStats(ch, journal)
	}

	// Collect trim stats
	if trim := provider.TrimStats(); trim != nil {
		c.collectTrimStats(ch, trim)
	}

	// Collect lock stats
	if lock := provider.LockStats(); lock != nil {
		c.collectLockStats(ch, lock)
	}

	// Collect SFB stats
	if sfb := provider.SFBStats(); sfb != nil {
		c.collectSFBStats(ch, sfb)
	}

	// Collect cluster stats
	if cluster := provider.ClusterInfo(); cluster != nil {
		c.collectClusterStats(ch, cluster)
	}
}

func (c *fsCollector) collectCacheStats(ch chan<- prometheus.Metric, stats *CacheStatsResponse) {
	// Inode cache
	ch <- prometheus.MustNewConstMetric(c.cacheInodeSizeDesc, prometheus.GaugeValue, float64(stats.InodeCache.Size))
	ch <- prometheus.MustNewConstMetric(c.cacheInodeHitsDesc, prometheus.CounterValue, float64(stats.InodeCache.Hits))
	ch <- prometheus.MustNewConstMetric(c.cacheInodeMissesDesc, prometheus.CounterValue, float64(stats.InodeCache.Misses))
	ch <- prometheus.MustNewConstMetric(c.cacheInodeHitRateDesc, prometheus.GaugeValue, stats.InodeCache.HitRate)

	// Extent cache
	ch <- prometheus.MustNewConstMetric(c.cacheExtentSizeDesc, prometheus.GaugeValue, float64(stats.ExtentCache.Size))
	ch <- prometheus.MustNewConstMetric(c.cacheExtentHitsDesc, prometheus.CounterValue, float64(stats.ExtentCache.Hits))
	ch <- prometheus.MustNewConstMetric(c.cacheExtentMissesDesc, prometheus.CounterValue, float64(stats.ExtentCache.Misses))
	ch <- prometheus.MustNewConstMetric(c.cacheExtentHitRateDesc, prometheus.GaugeValue, stats.ExtentCache.HitRate)

	// Negative cache
	ch <- prometheus.MustNewConstMetric(c.cacheNegativeSizeDesc, prometheus.GaugeValue, float64(stats.NegativeCache.Size))
	ch <- prometheus.MustNewConstMetric(c.cacheNegativeHitsDesc, prometheus.CounterValue, float64(stats.NegativeCache.Hits))
	ch <- prometheus.MustNewConstMetric(c.cacheNegativeMissesDesc, prometheus.CounterValue, float64(stats.NegativeCache.Misses))
	ch <- prometheus.MustNewConstMetric(c.cacheNegativeHitRateDesc, prometheus.GaugeValue, stats.NegativeCache.HitRate)

	// Lock hint cache
	ch <- prometheus.MustNewConstMetric(c.cacheLockHintSizeDesc, prometheus.GaugeValue, float64(stats.LockHintCache.Size))
	ch <- prometheus.MustNewConstMetric(c.cacheLockHintHitsDesc, prometheus.CounterValue, float64(stats.LockHintCache.Hits))
	ch <- prometheus.MustNewConstMetric(c.cacheLockHintMissesDesc, prometheus.CounterValue, float64(stats.LockHintCache.Misses))
	ch <- prometheus.MustNewConstMetric(c.cacheLockHintHitRateDesc, prometheus.GaugeValue, stats.LockHintCache.HitRate)
}

func (c *fsCollector) collectJournalStats(ch chan<- prometheus.Metric, stats *JournalStatsResponse) {
	// Convert state to numeric (1=open, 0=other)
	stateValue := 0.0
	if stats.State == "open" {
		stateValue = 1.0
	}
	ch <- prometheus.MustNewConstMetric(c.journalStateDesc, prometheus.GaugeValue, stateValue)
	ch <- prometheus.MustNewConstMetric(c.journalHeadOffsetDesc, prometheus.GaugeValue, float64(stats.HeadOffset))
	ch <- prometheus.MustNewConstMetric(c.journalTailOffsetDesc, prometheus.GaugeValue, float64(stats.TailOffset))
	ch <- prometheus.MustNewConstMetric(c.journalAvailableSpaceDesc, prometheus.GaugeValue, float64(stats.AvailableSpace))
	ch <- prometheus.MustNewConstMetric(c.journalActiveTxnCountDesc, prometheus.GaugeValue, float64(stats.ActiveTxnCount))
	ch <- prometheus.MustNewConstMetric(c.journalCommitCountDesc, prometheus.CounterValue, float64(stats.CommitCount))
	ch <- prometheus.MustNewConstMetric(c.journalBytesWrittenDesc, prometheus.CounterValue, float64(stats.BytesWritten))
	ch <- prometheus.MustNewConstMetric(c.journalLastCheckpointDesc, prometheus.GaugeValue, float64(stats.LastCheckpointSeq))
	ch <- prometheus.MustNewConstMetric(c.journalBatchSyncCountDesc, prometheus.CounterValue, float64(stats.BatchSyncCount))
}

func (c *fsCollector) collectTrimStats(ch chan<- prometheus.Metric, stats *TrimStatsResponse) {
	ch <- prometheus.MustNewConstMetric(c.trimOperationsDesc, prometheus.CounterValue, float64(stats.TrimOperations))
	ch <- prometheus.MustNewConstMetric(c.trimBytesDesc, prometheus.CounterValue, float64(stats.TrimBytes))
	ch <- prometheus.MustNewConstMetric(c.trimBlocksDesc, prometheus.CounterValue, float64(stats.TrimBlocks))
	ch <- prometheus.MustNewConstMetric(c.trimPendingBlocksDesc, prometheus.GaugeValue, float64(stats.PendingTrimBlocks))
	ch <- prometheus.MustNewConstMetric(c.trimErrorsDesc, prometheus.CounterValue, float64(stats.TrimErrors))

	enabledValue := 0.0
	if stats.TrimEnabled {
		enabledValue = 1.0
	}
	ch <- prometheus.MustNewConstMetric(c.trimEnabledDesc, prometheus.GaugeValue, enabledValue)
}

func (c *fsCollector) collectLockStats(ch chan<- prometheus.Metric, stats *LockStatsResponse) {
	ch <- prometheus.MustNewConstMetric(c.lockHintCacheHitsDesc, prometheus.CounterValue, float64(stats.HintCacheHits))
	ch <- prometheus.MustNewConstMetric(c.lockHintCacheMissesDesc, prometheus.CounterValue, float64(stats.HintCacheMisses))
	ch <- prometheus.MustNewConstMetric(c.lockHintCacheHitRateDesc, prometheus.GaugeValue, stats.HintCacheHitRate)
	ch <- prometheus.MustNewConstMetric(c.lockActiveLocksDesc, prometheus.GaugeValue, float64(stats.ActiveLocks))
	ch <- prometheus.MustNewConstMetric(c.lockPendingLocksDesc, prometheus.GaugeValue, float64(stats.PendingLocks))
}

func (c *fsCollector) collectSFBStats(ch chan<- prometheus.Metric, stats *SFBStatsResponse) {
	ch <- prometheus.MustNewConstMetric(c.sfbTotalBlocksDesc, prometheus.GaugeValue, float64(stats.TotalBlocks))
	ch <- prometheus.MustNewConstMetric(c.sfbAllocatedBlocksDesc, prometheus.GaugeValue, float64(stats.AllocatedBlocks))
	ch <- prometheus.MustNewConstMetric(c.sfbFreeBlocksDesc, prometheus.GaugeValue, float64(stats.FreeBlocks))
	ch <- prometheus.MustNewConstMetric(c.sfbAllocationPercentDesc, prometheus.GaugeValue, stats.AllocationPercent/100.0)
	ch <- prometheus.MustNewConstMetric(c.sfbFragmentationDesc, prometheus.GaugeValue, stats.FragmentationRatio)
	ch <- prometheus.MustNewConstMetric(c.sfbLargestFreeExtentDesc, prometheus.GaugeValue, float64(stats.LargestFreeExtent))
	ch <- prometheus.MustNewConstMetric(c.sfbAllocationsDesc, prometheus.CounterValue, float64(stats.AllocationsTotal))
	ch <- prometheus.MustNewConstMetric(c.sfbDeallocationsDesc, prometheus.CounterValue, float64(stats.DeallocationsTotal))
	ch <- prometheus.MustNewConstMetric(c.sfbAllocationFailuresDesc, prometheus.CounterValue, float64(stats.AllocationFailures))
}

func (c *fsCollector) collectClusterStats(ch chan<- prometheus.Metric, stats *ClusterInfoResponse) {
	ch <- prometheus.MustNewConstMetric(c.clusterSizeDesc, prometheus.GaugeValue, float64(stats.ClusterSize))
	ch <- prometheus.MustNewConstMetric(c.clusterHealthyNodesDesc, prometheus.GaugeValue, float64(stats.HealthyNodes))

	isLeader := 0.0
	if stats.Role == "leader" {
		isLeader = 1.0
	}
	ch <- prometheus.MustNewConstMetric(c.clusterIsLeaderDesc, prometheus.GaugeValue, isLeader)
}
