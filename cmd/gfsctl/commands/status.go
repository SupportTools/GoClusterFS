package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var (
	statusWatch    bool
	statusInterval time.Duration
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show filesystem status",
	Long: `Display comprehensive filesystem status including mount state, version,
uptime, and storage capacity.

Examples:
  gfsctl status                 # Show status in text format
  gfsctl status -o json         # Show status in JSON format
  gfsctl status -w              # Watch mode with 2s refresh
  gfsctl status -w -i 5s        # Watch mode with 5s refresh`,
	RunE: runStatus,
}

func init() {
	statusCmd.Flags().BoolVarP(&statusWatch, "watch", "w", false, "Enable watch mode for live updates")
	statusCmd.Flags().DurationVarP(&statusInterval, "interval", "i", 2*time.Second, "Refresh interval for watch mode")
	rootCmd.AddCommand(statusCmd)
}

func runStatus(cmd *cobra.Command, args []string) error {
	// Watch mode not compatible with JSON output
	if statusWatch && GetOutputFormat() == "json" {
		return usageErrorf("--watch does not work with -o json; drop one of them")
	}

	// Create the display function
	displayFn := func(ctx context.Context) error {
		return displayStatus(cmd, ctx)
	}

	// Run with or without watch
	return RunWithWatch(WatchConfig{
		Enabled:  statusWatch,
		Interval: statusInterval,
	}, displayFn)
}

func displayStatus(cmd *cobra.Command, ctx context.Context) error {
	client := GetClient()

	status, err := client.Status(ctx)
	if err != nil {
		return fmt.Errorf("failed to get status: %w", err)
	}

	f := NewFormatter(cmd.OutOrStdout(), GetOutputFormat())

	if f.IsJSON() {
		return f.JSON(status)
	}

	// Text output with colored status
	f.Header("GoClusterFS Status")

	// Status indicator
	statusColor := "ok"
	if status.Status != "ok" {
		statusColor = "error"
	}
	f.KeyValueColor("Status", status.Status, statusColor)

	// Version info
	f.KeyValue("Version", status.Version)
	if status.GitCommit != "" {
		f.KeyValue("Git Commit", status.GitCommit)
	}
	if status.BuildTime != "" {
		f.KeyValue("Build Time", status.BuildTime)
	}
	if status.GoVersion != "" {
		f.KeyValue("Go Version", status.GoVersion)
	}

	// Uptime
	f.KeyValue("Uptime", FormatDuration(status.UptimeSeconds))

	// Mount info
	if status.MountPath != "" {
		f.Header("Mount")
		f.KeyValue("Mount Path", status.MountPath)
		f.KeyValue("Device Path", status.DevicePath)

		mountColor := "ok"
		if status.MountState != "mounted" {
			mountColor = "warning"
		}
		f.KeyValueColor("Mount State", string(status.MountState), mountColor)

		if status.ReadOnly {
			f.KeyValueColor("Read Only", "yes", "warning")
		}
	}

	// Capacity info
	if status.Capacity != nil {
		c := status.Capacity
		f.Header("Storage Capacity")
		f.KeyValue("Total", FormatBytes(c.TotalBytes))
		f.KeyValue("Used", fmt.Sprintf("%s (%s)", FormatBytes(c.UsedBytes), FormatPercent(c.UsedPercent)))
		f.KeyValue("Free", FormatBytes(c.FreeBytes))
		f.KeyValue("Block Size", FormatBytes(c.BlockSizeBytes))
		f.KeyValue("Total Blocks", fmt.Sprintf("%d", c.TotalBlocks))
		f.KeyValue("Used Blocks", fmt.Sprintf("%d", c.UsedBlocks))
		f.KeyValue("Free Blocks", fmt.Sprintf("%d", c.FreeBlocks))
	}

	// Cluster info
	if status.Cluster != nil {
		c := status.Cluster
		f.Header("Cluster")
		f.KeyValue("Node ID", c.NodeID)
		f.KeyValue("Peer Count", fmt.Sprintf("%d", c.PeerCount))
		if c.LeaderID != "" {
			f.KeyValue("Leader ID", c.LeaderID)
		}
		if c.IsLeader {
			f.KeyValueColor("Role", "leader", "leader")
		}
		if c.ClusterState != "" {
			f.KeyValue("State", c.ClusterState)
		}
	}

	f.Println()
	return nil
}
