package commands

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/supporttools/GoClusterFS/cmd/gfsctl/client"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot [subcommand]",
	Short: "Snapshot management commands",
	Long: `Manage filesystem snapshots.

Available subcommands:
  list     - List all snapshots
  create   - Create a new snapshot
  delete   - Delete a snapshot
  info     - Show snapshot details

Examples:
  gfsctl snapshot list                    # List all snapshots
  gfsctl snapshot create mysnap           # Create snapshot named "mysnap"
  gfsctl snapshot delete snap-123         # Delete snapshot by ID
  gfsctl snapshot info snap-123           # Show snapshot details`,
	Args: cobra.MaximumNArgs(2),
	RunE: runSnapshot,
}

var (
	snapshotDescription string
	snapshotForce       bool
)

func init() {
	snapshotCmd.Flags().StringVarP(&snapshotDescription, "description", "d", "", "Snapshot description (for create)")
	snapshotCmd.Flags().BoolVarP(&snapshotForce, "force", "f", false, "Skip confirmation prompts")
	rootCmd.AddCommand(snapshotCmd)
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	apiClient := GetClient()
	f := NewFormatter(cmd.OutOrStdout(), GetOutputFormat())

	// Determine which subcommand
	subcmd := "list"
	if len(args) > 0 {
		subcmd = args[0]
	}

	switch subcmd {
	case "list":
		return listSnapshots(ctx, apiClient, f)
	case "create":
		if len(args) < 2 {
			return fmt.Errorf("snapshot name required: gfsctl snapshot create <name>")
		}
		return createSnapshot(ctx, apiClient, f, args[1])
	case "delete":
		if len(args) < 2 {
			return fmt.Errorf("snapshot ID required: gfsctl snapshot delete <id>")
		}
		return deleteSnapshot(ctx, apiClient, f, args[1])
	case "info":
		if len(args) < 2 {
			return fmt.Errorf("snapshot ID required: gfsctl snapshot info <id>")
		}
		return showSnapshotInfo(ctx, apiClient, f, args[1])
	default:
		return fmt.Errorf("unknown subcommand: %s (valid: list, create, delete, info)", subcmd)
	}
}

func listSnapshots(ctx context.Context, c *client.Client, f *Formatter) error {
	resp, err := c.SnapshotList(ctx)
	if err != nil {
		printError("failed to list snapshots: %v", err)
		return err
	}

	if f.IsJSON() {
		return f.JSON(resp)
	}

	f.Header("Snapshots")
	f.KeyValue("Total", fmt.Sprintf("%d", resp.Total))

	if len(resp.Snapshots) > 0 {
		f.Printf("\n")
		table := f.NewTable("ID", "Name", "Created", "Size", "Status")
		for _, snap := range resp.Snapshots {
			table.AddRow(snap.ID, snap.Name, snap.CreatedAt, FormatBytes(snap.SizeBytes), snap.Status)
		}
		table.Render()
	} else {
		f.Printf("\n  (no snapshots)\n")
	}

	fmt.Fprintln(os.Stdout)
	return nil
}

func createSnapshot(ctx context.Context, c *client.Client, f *Formatter, name string) error {
	// Show what we're about to do
	if !snapshotForce && !f.IsJSON() {
		f.Printf("Creating snapshot: %s\n", name)
		if snapshotDescription != "" {
			f.Printf("Description: %s\n", snapshotDescription)
		}
		if !confirmAction("Create this snapshot?") {
			f.Printf("Cancelled.\n")
			return nil
		}
	}

	req := &client.SnapshotCreateRequest{
		Name:        name,
		Description: snapshotDescription,
	}

	// Show progress
	if !f.IsJSON() {
		f.Printf("Creating snapshot...")
	}

	resp, err := c.SnapshotCreate(ctx, req)
	if err != nil {
		if !f.IsJSON() {
			f.Printf(" failed\n")
		}
		printError("failed to create snapshot: %v", err)
		return err
	}

	if f.IsJSON() {
		return f.JSON(resp)
	}

	f.Printf(" done\n\n")
	f.Header("Snapshot Created")
	f.KeyValue("ID", resp.Snapshot.ID)
	f.KeyValue("Name", resp.Snapshot.Name)
	f.KeyValue("Created At", resp.Snapshot.CreatedAt)
	f.KeyValueColor("Status", resp.Snapshot.Status, "ok")

	fmt.Fprintln(os.Stdout)
	return nil
}

func deleteSnapshot(ctx context.Context, c *client.Client, f *Formatter, id string) error {
	// Get snapshot info first for confirmation
	if !snapshotForce && !f.IsJSON() {
		info, err := c.SnapshotInfo(ctx, id)
		if err != nil {
			printError("failed to get snapshot info: %v", err)
			return err
		}

		f.Printf("Snapshot to delete:\n")
		f.KeyValue("ID", info.ID)
		f.KeyValue("Name", info.Name)
		f.KeyValue("Size", FormatBytes(info.SizeBytes))
		f.Printf("\n")

		if !confirmAction("Delete this snapshot? This cannot be undone.") {
			f.Printf("Cancelled.\n")
			return nil
		}
	}

	// Show progress
	if !f.IsJSON() {
		f.Printf("Deleting snapshot...")
	}

	resp, err := c.SnapshotDelete(ctx, id)
	if err != nil {
		if !f.IsJSON() {
			f.Printf(" failed\n")
		}
		printError("failed to delete snapshot: %v", err)
		return err
	}

	if f.IsJSON() {
		return f.JSON(resp)
	}

	f.Printf(" done\n")
	f.Printf("Snapshot %s deleted.\n", resp.ID)

	fmt.Fprintln(os.Stdout)
	return nil
}

func showSnapshotInfo(ctx context.Context, c *client.Client, f *Formatter, id string) error {
	info, err := c.SnapshotInfo(ctx, id)
	if err != nil {
		printError("failed to get snapshot info: %v", err)
		return err
	}

	if f.IsJSON() {
		return f.JSON(info)
	}

	f.Header("Snapshot Details")
	f.KeyValue("ID", info.ID)
	f.KeyValue("Name", info.Name)
	f.KeyValue("Created At", info.CreatedAt)
	f.KeyValue("Size", FormatBytes(info.SizeBytes))

	statusColor := "ok"
	if info.Status != "complete" && info.Status != "ready" {
		statusColor = "warning"
	}
	f.KeyValueColor("Status", info.Status, statusColor)

	if info.Description != "" {
		f.KeyValue("Description", info.Description)
	}

	fmt.Fprintln(os.Stdout)
	return nil
}

// confirmAction prompts the user for confirmation.
func confirmAction(prompt string) bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("%s [y/N]: ", prompt)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	response = strings.TrimSpace(strings.ToLower(response))
	return response == "y" || response == "yes"
}
