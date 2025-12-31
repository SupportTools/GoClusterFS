package commands

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/supporttools/GoClusterFS/cmd/gfsctl/client"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster [subcommand]",
	Short: "Cluster management commands",
	Long: `Manage and inspect cluster state.

Available subcommands:
  status   - Show cluster status
  nodes    - List cluster nodes
  locks    - Show active distributed locks

Examples:
  gfsctl cluster status          # Show cluster status
  gfsctl cluster nodes           # List all nodes
  gfsctl cluster locks           # Show distributed locks`,
	Args: cobra.MaximumNArgs(1),
	RunE: runCluster,
}

func init() {
	rootCmd.AddCommand(clusterCmd)
}

func runCluster(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	apiClient := GetClient()
	f := NewFormatter(cmd.OutOrStdout(), GetOutputFormat())

	// Determine which subcommand
	subcmd := "status"
	if len(args) > 0 {
		subcmd = args[0]
	}

	switch subcmd {
	case "status":
		return showClusterStatus(ctx, apiClient, f)
	case "nodes":
		return showClusterNodes(ctx, apiClient, f)
	case "locks":
		return showClusterLocks(ctx, apiClient, f)
	default:
		return fmt.Errorf("unknown subcommand: %s (valid: status, nodes, locks)", subcmd)
	}
}

func showClusterStatus(ctx context.Context, c *client.Client, f *Formatter) error {
	status, err := c.ClusterStatus(ctx)
	if err != nil {
		printError("failed to get cluster status: %v", err)
		return err
	}

	if f.IsJSON() {
		return f.JSON(status)
	}

	f.Header("Cluster Status")

	f.KeyValue("Node ID", status.NodeID)
	if status.NodeAddress != "" {
		f.KeyValue("Address", status.NodeAddress)
	}

	roleColor := "follower"
	if status.Role == "leader" {
		roleColor = "leader"
	}
	f.KeyValueColor("Role", status.Role, roleColor)

	stateColor := "ok"
	if status.State != "active" {
		stateColor = "warning"
	}
	f.KeyValueColor("State", status.State, stateColor)

	f.KeyValue("Cluster Size", fmt.Sprintf("%d", status.ClusterSize))

	healthColor := "ok"
	if status.HealthyNodes < status.ClusterSize {
		healthColor = "warning"
	}
	f.KeyValueColor("Healthy Nodes", fmt.Sprintf("%d/%d", status.HealthyNodes, status.ClusterSize), healthColor)

	if status.LeaderID != "" {
		f.KeyValue("Leader ID", status.LeaderID)
	}
	if status.LastHeartbeatAt != "" {
		f.KeyValue("Last Heartbeat", status.LastHeartbeatAt)
	}
	if status.JoinedAt != "" {
		f.KeyValue("Joined At", status.JoinedAt)
	}

	// Show peers if present
	if len(status.Peers) > 0 {
		f.Header("Peers")
		table := f.NewTable("Node ID", "Address", "State", "Latency")
		for _, peer := range status.Peers {
			latency := "-"
			if peer.Latency > 0 {
				latency = fmt.Sprintf("%.1fms", peer.Latency)
			}
			table.AddRow(peer.NodeID, peer.Address, peer.State, latency)
		}
		table.Render()
	}

	fmt.Fprintln(os.Stdout)
	return nil
}

func showClusterNodes(ctx context.Context, c *client.Client, f *Formatter) error {
	nodes, err := c.ClusterNodes(ctx)
	if err != nil {
		printError("failed to get cluster nodes: %v", err)
		return err
	}

	if f.IsJSON() {
		return f.JSON(nodes)
	}

	f.Header("Cluster Nodes")

	if nodes.ClusterID != "" {
		f.KeyValue("Cluster ID", nodes.ClusterID)
	}
	f.KeyValue("Total Nodes", fmt.Sprintf("%d", nodes.TotalNodes))

	healthColor := "ok"
	if nodes.HealthyNodes < nodes.TotalNodes {
		healthColor = "warning"
	}
	f.KeyValueColor("Healthy Nodes", fmt.Sprintf("%d", nodes.HealthyNodes), healthColor)

	if nodes.LeaderID != "" {
		f.KeyValue("Leader", nodes.LeaderID)
	}

	if len(nodes.Nodes) > 0 {
		f.Printf("\n")
		table := f.NewTable("Node ID", "Address", "Role", "State", "Latency", "Self")
		for _, node := range nodes.Nodes {
			latency := "-"
			if node.LatencyMs > 0 {
				latency = fmt.Sprintf("%.1fms", node.LatencyMs)
			}
			self := ""
			if node.IsSelf {
				self = "*"
			}
			table.AddRow(node.NodeID, node.Address, node.Role, node.State, latency, self)
		}
		table.Render()
	}

	fmt.Fprintln(os.Stdout)
	return nil
}

func showClusterLocks(ctx context.Context, c *client.Client, f *Formatter) error {
	locks, err := c.ClusterLocks(ctx)
	if err != nil {
		printError("failed to get cluster locks: %v", err)
		return err
	}

	if f.IsJSON() {
		return f.JSON(locks)
	}

	f.Header("Distributed Locks")

	f.KeyValue("Active Locks", fmt.Sprintf("%d", locks.ActiveLocks))
	f.KeyValue("Pending Locks", fmt.Sprintf("%d", locks.PendingLocks))

	if len(locks.Locks) > 0 {
		f.Printf("\n")
		table := f.NewTable("Lock ID", "Resource", "Type", "Owner Node", "Acquired At")
		for _, lock := range locks.Locks {
			table.AddRow(lock.LockID, lock.Resource, lock.LockType, lock.OwnerNodeID, lock.AcquiredAt)
		}
		table.Render()
	} else {
		f.Printf("\n  (no active locks)\n")
	}

	fmt.Fprintln(os.Stdout)
	return nil
}
