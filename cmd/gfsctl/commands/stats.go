package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/supporttools/GoClusterFS/cmd/gfsctl/client"
)

var (
	statsWatch    bool
	statsInterval time.Duration
)

var statsCmd = &cobra.Command{
	Use:   "stats [component]",
	Short: "Show component statistics",
	Long: `Display statistics for filesystem components.

Available components:
  cache    - Cache hit/miss statistics
  journal  - Journal transaction statistics
  trim     - TRIM operation statistics
  lock     - Lock contention statistics
  sfb      - Sub-File Block allocation statistics
  cluster  - Cluster membership and peers
  all      - All statistics combined

If no component is specified, shows an overview of key stats.

Examples:
  gfsctl stats                  # Show overview
  gfsctl stats cache            # Show cache stats
  gfsctl stats all              # Show all stats
  gfsctl stats -o json          # Show overview in JSON
  gfsctl stats cache -w         # Watch cache stats with 2s refresh
  gfsctl stats all -w -i 5s     # Watch all stats with 5s refresh`,
	Args: cobra.MaximumNArgs(1),
	RunE: runStats,
}

func init() {
	statsCmd.Flags().BoolVarP(&statsWatch, "watch", "w", false, "Refresh the output every --interval until interrupted (default false)")
	statsCmd.Flags().DurationVarP(&statsInterval, "interval", "i", 2*time.Second, "Refresh interval for watch mode")
	rootCmd.AddCommand(statsCmd)
}

func runStats(cmd *cobra.Command, args []string) error {
	// Watch mode not compatible with JSON output
	if statsWatch && GetOutputFormat() == "json" {
		return usageErrorf("--watch does not work with -o json; drop one of them")
	}

	// Determine which stats to show
	component := ""
	if len(args) > 0 {
		component = args[0]
	}

	// Validate component name upfront
	switch component {
	case "", "cache", "journal", "trim", "lock", "sfb", "cluster", "all":
		// valid
	default:
		return usageErrorf("unknown component: %s (valid: cache, journal, trim, lock, sfb, cluster, all)", component)
	}

	// Create the display function
	displayFn := func(ctx context.Context) error {
		return displayStats(cmd, ctx, component)
	}

	// Run with or without watch
	return RunWithWatch(WatchConfig{
		Enabled:  statsWatch,
		Interval: statsInterval,
	}, displayFn)
}

func displayStats(cmd *cobra.Command, ctx context.Context, component string) error {
	apiClient := GetClient()
	f := NewFormatter(cmd.OutOrStdout(), GetOutputFormat())

	switch component {
	case "":
		return showStatsOverview(ctx, apiClient, f)
	case "cache":
		return showCacheStats(ctx, apiClient, f)
	case "journal":
		return showJournalStats(ctx, apiClient, f)
	case "trim":
		return showTrimStats(ctx, apiClient, f)
	case "lock":
		return showLockStats(ctx, apiClient, f)
	case "sfb":
		return showSFBStats(ctx, apiClient, f)
	case "cluster":
		return showClusterStats(ctx, apiClient, f)
	case "all":
		return showAllStats(ctx, apiClient, f)
	default:
		return fmt.Errorf("unknown component: %s", component)
	}
}

func showStatsOverview(ctx context.Context, c *client.Client, f *Formatter) error {
	stats, err := c.StatsOverview(ctx)
	if err != nil {
		return fmt.Errorf("failed to get stats: %w", err)
	}

	if f.IsJSON() {
		return f.JSON(stats)
	}

	f.Header("Statistics Overview")

	if stats.Cache != nil {
		f.Printf("\n  Cache:\n")
		f.KeyValue("  Inode Hit Rate", FormatPercent(stats.Cache.InodeCache.HitRate*100))
		f.KeyValue("  Extent Hit Rate", FormatPercent(stats.Cache.ExtentCache.HitRate*100))
	}

	if stats.Journal != nil {
		f.Printf("\n  Journal:\n")
		f.KeyValue("  State", stats.Journal.State)
		f.KeyValue("  Commits", fmt.Sprintf("%d", stats.Journal.CommitCount))
		f.KeyValue("  Bytes Written", FormatBytes(stats.Journal.BytesWritten))
	}

	f.Println()
	return nil
}

func showCacheStats(ctx context.Context, c *client.Client, f *Formatter) error {
	stats, err := c.CacheStats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cache stats: %w", err)
	}

	if f.IsJSON() {
		return f.JSON(stats)
	}

	f.Header("Cache Statistics")

	table := f.NewTable("Cache", "Size", "Hits", "Misses", "Hit Rate")
	table.AddRow("Inode",
		fmt.Sprintf("%d", stats.InodeCache.Size),
		fmt.Sprintf("%d", stats.InodeCache.Hits),
		fmt.Sprintf("%d", stats.InodeCache.Misses),
		FormatPercent(stats.InodeCache.HitRate*100))
	table.AddRow("Extent",
		fmt.Sprintf("%d", stats.ExtentCache.Size),
		fmt.Sprintf("%d", stats.ExtentCache.Hits),
		fmt.Sprintf("%d", stats.ExtentCache.Misses),
		FormatPercent(stats.ExtentCache.HitRate*100))
	table.AddRow("Negative",
		fmt.Sprintf("%d", stats.NegativeCache.Size),
		fmt.Sprintf("%d", stats.NegativeCache.Hits),
		fmt.Sprintf("%d", stats.NegativeCache.Misses),
		FormatPercent(stats.NegativeCache.HitRate*100))
	if stats.LockHintCache.Size > 0 {
		table.AddRow("Lock Hint",
			fmt.Sprintf("%d", stats.LockHintCache.Size),
			fmt.Sprintf("%d", stats.LockHintCache.Hits),
			fmt.Sprintf("%d", stats.LockHintCache.Misses),
			FormatPercent(stats.LockHintCache.HitRate*100))
	}
	table.Render()

	f.Println()
	return nil
}

func showJournalStats(ctx context.Context, c *client.Client, f *Formatter) error {
	stats, err := c.JournalStats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get journal stats: %w", err)
	}

	if f.IsJSON() {
		return f.JSON(stats)
	}

	f.Header("Journal Statistics")

	stateColor := "ok"
	if stats.State != "open" {
		stateColor = "warning"
	}
	f.KeyValueColor("State", stats.State, stateColor)
	f.KeyValue("Head Offset", fmt.Sprintf("%d", stats.HeadOffset))
	f.KeyValue("Tail Offset", fmt.Sprintf("%d", stats.TailOffset))
	f.KeyValue("Available Space", FormatBytes(uint64(stats.AvailableSpace)))
	f.KeyValue("Active Transactions", fmt.Sprintf("%d", stats.ActiveTxnCount))
	f.KeyValue("Commits", fmt.Sprintf("%d", stats.CommitCount))
	f.KeyValue("Bytes Written", FormatBytes(stats.BytesWritten))
	f.KeyValue("Last Checkpoint", fmt.Sprintf("%d", stats.LastCheckpointSeq))
	f.KeyValue("Batch Syncs", fmt.Sprintf("%d", stats.BatchSyncCount))

	f.Println()
	return nil
}

func showTrimStats(ctx context.Context, c *client.Client, f *Formatter) error {
	stats, err := c.TrimStats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get trim stats: %w", err)
	}

	if f.IsJSON() {
		return f.JSON(stats)
	}

	f.Header("TRIM Statistics")

	enabledStr := "disabled"
	enabledColor := "warning"
	if stats.TrimEnabled {
		enabledStr = "enabled"
		enabledColor = "ok"
	}
	f.KeyValueColor("TRIM", enabledStr, enabledColor)
	f.KeyValue("Operations", fmt.Sprintf("%d", stats.TrimOperations))
	f.KeyValue("Bytes Trimmed", FormatBytes(stats.TrimBytes))
	f.KeyValue("Blocks Trimmed", fmt.Sprintf("%d", stats.TrimBlocks))
	f.KeyValue("Pending Blocks", fmt.Sprintf("%d", stats.PendingTrimBlocks))
	if stats.TrimErrors > 0 {
		f.KeyValueColor("Errors", fmt.Sprintf("%d", stats.TrimErrors), "error")
	} else {
		f.KeyValue("Errors", "0")
	}
	if stats.LastTrimAt != "" {
		f.KeyValue("Last TRIM", stats.LastTrimAt)
	}

	f.Println()
	return nil
}

func showLockStats(ctx context.Context, c *client.Client, f *Formatter) error {
	stats, err := c.LockStats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get lock stats: %w", err)
	}

	if f.IsJSON() {
		return f.JSON(stats)
	}

	f.Header("Lock Statistics")

	f.KeyValue("Hint Cache Hits", fmt.Sprintf("%d", stats.HintCacheHits))
	f.KeyValue("Hint Cache Misses", fmt.Sprintf("%d", stats.HintCacheMisses))
	f.KeyValue("Hint Cache Hit Rate", FormatPercent(stats.HintCacheHitRate*100))
	f.KeyValue("Active Locks", fmt.Sprintf("%d", stats.ActiveLocks))
	f.KeyValue("Pending Locks", fmt.Sprintf("%d", stats.PendingLocks))

	f.Println()
	return nil
}

func showSFBStats(ctx context.Context, c *client.Client, f *Formatter) error {
	stats, err := c.SFBStats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get sfb stats: %w", err)
	}

	if f.IsJSON() {
		return f.JSON(stats)
	}

	f.Header("SFB (Sub-File Block) Statistics")

	f.KeyValue("Total Blocks", fmt.Sprintf("%d", stats.TotalBlocks))
	f.KeyValue("Allocated Blocks", fmt.Sprintf("%d", stats.AllocatedBlocks))
	f.KeyValue("Free Blocks", fmt.Sprintf("%d", stats.FreeBlocks))
	f.KeyValue("Allocation", FormatPercent(stats.AllocationPercent))
	if stats.FragmentationRatio > 0 {
		f.KeyValue("Fragmentation", FormatPercent(stats.FragmentationRatio*100))
	}
	if stats.LargestFreeExtent > 0 {
		f.KeyValue("Largest Free Extent", fmt.Sprintf("%d blocks", stats.LargestFreeExtent))
	}
	f.KeyValue("Total Allocations", fmt.Sprintf("%d", stats.AllocationsTotal))
	f.KeyValue("Total Deallocations", fmt.Sprintf("%d", stats.DeallocationsTotal))
	if stats.AllocationFailures > 0 {
		f.KeyValueColor("Allocation Failures", fmt.Sprintf("%d", stats.AllocationFailures), "error")
	}

	f.Println()
	return nil
}

func showClusterStats(ctx context.Context, c *client.Client, f *Formatter) error {
	stats, err := c.ClusterInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster stats: %w", err)
	}

	if f.IsJSON() {
		return f.JSON(stats)
	}

	f.Header("Cluster")
	f.KeyValue("Node ID", stats.NodeID)
	f.KeyValue("Role", stats.Role)
	f.KeyValue("State", stats.State)
	f.KeyValue("Cluster Size", fmt.Sprintf("%d", stats.ClusterSize))
	f.KeyValue("Healthy Nodes", fmt.Sprintf("%d", stats.HealthyNodes))
	if len(stats.Peers) > 0 {
		f.Printf("\n")
		table := f.NewTable("Peer", "Address", "State", "Last Seen")
		for _, p := range stats.Peers {
			table.AddRow(p.NodeID, p.Address, p.State, p.LastSeen)
		}
		table.Render()
	}

	f.Println()
	return nil
}

func showAllStats(ctx context.Context, c *client.Client, f *Formatter) error {
	stats, err := c.AllStats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get all stats: %w", err)
	}

	if f.IsJSON() {
		return f.JSON(stats)
	}

	f.Header("All Statistics")
	f.KeyValue("Collected At", stats.CollectedAt)

	// Show each component's stats inline
	if stats.Cache != nil {
		f.Printf("\n")
		f.Header("Cache")
		table := f.NewTable("Cache", "Size", "Hits", "Misses", "Hit Rate")
		table.AddRow("Inode",
			fmt.Sprintf("%d", stats.Cache.InodeCache.Size),
			fmt.Sprintf("%d", stats.Cache.InodeCache.Hits),
			fmt.Sprintf("%d", stats.Cache.InodeCache.Misses),
			FormatPercent(stats.Cache.InodeCache.HitRate*100))
		table.AddRow("Extent",
			fmt.Sprintf("%d", stats.Cache.ExtentCache.Size),
			fmt.Sprintf("%d", stats.Cache.ExtentCache.Hits),
			fmt.Sprintf("%d", stats.Cache.ExtentCache.Misses),
			FormatPercent(stats.Cache.ExtentCache.HitRate*100))
		table.Render()
	}

	if stats.Journal != nil {
		f.Printf("\n")
		f.Header("Journal")
		f.KeyValue("State", stats.Journal.State)
		f.KeyValue("Commits", fmt.Sprintf("%d", stats.Journal.CommitCount))
		f.KeyValue("Bytes Written", FormatBytes(stats.Journal.BytesWritten))
	}

	if stats.Trim != nil {
		f.Printf("\n")
		f.Header("TRIM")
		f.KeyValue("Operations", fmt.Sprintf("%d", stats.Trim.TrimOperations))
		f.KeyValue("Bytes Trimmed", FormatBytes(stats.Trim.TrimBytes))
	}

	if stats.Lock != nil {
		f.Printf("\n")
		f.Header("Lock")
		f.KeyValue("Hint Cache Hit Rate", FormatPercent(stats.Lock.HintCacheHitRate*100))
		f.KeyValue("Active Locks", fmt.Sprintf("%d", stats.Lock.ActiveLocks))
	}

	if stats.SFB != nil {
		f.Printf("\n")
		f.Header("SFB")
		f.KeyValue("Allocation", FormatPercent(stats.SFB.AllocationPercent))
		f.KeyValue("Free Blocks", fmt.Sprintf("%d", stats.SFB.FreeBlocks))
	}

	if stats.Cluster != nil {
		f.Printf("\n")
		f.Header("Cluster")
		f.KeyValue("Node ID", stats.Cluster.NodeID)
		f.KeyValue("Role", stats.Cluster.Role)
		f.KeyValue("Cluster Size", fmt.Sprintf("%d", stats.Cluster.ClusterSize))
		f.KeyValue("Healthy Nodes", fmt.Sprintf("%d", stats.Cluster.HealthyNodes))
	}

	f.Println()
	return nil
}
