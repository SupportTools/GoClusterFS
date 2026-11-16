package commands

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

var snapshotsCmd = &cobra.Command{
	Use:   "snapshots",
	Short: "List snapshots",
	Long: `List every snapshot on the filesystem. Orphans, snapshots whose file is
gone or that cannot prove which file they were taken of, are marked; delete
them through the API.

The API lists snapshots only with its token: set GFSCTL_TOKEN, --token or
token in the config file.

Examples:
  gfsctl snapshots              # Show snapshots as a table
  gfsctl snapshots -o json      # Show snapshots in JSON format`,
	Args: cobra.NoArgs,
	RunE: runSnapshots,
}

func init() {
	rootCmd.AddCommand(snapshotsCmd)
}

func runSnapshots(cmd *cobra.Command, args []string) error {
	resp, err := GetClient().Snapshots(context.Background())
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}
	f := NewFormatter(cmd.OutOrStdout(), GetOutputFormat())
	if f.IsJSON() {
		return f.JSON(resp)
	}
	if len(resp.Snapshots) == 0 {
		f.Println("No snapshots")
		return nil
	}
	table := f.NewTable("Name", "ID", "Inode", "State", "Size", "Created", "Orphan")
	for _, s := range resp.Snapshots {
		orphan := ""
		if s.Orphan {
			orphan = "yes"
		}
		table.AddRow(s.Name, fmt.Sprint(s.ID), fmt.Sprint(s.Inode), s.State, FormatBytes(s.SizeBytes), s.CreatedAt, orphan)
	}
	table.Render()
	return nil
}
