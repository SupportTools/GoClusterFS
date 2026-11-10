package commands

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/supporttools/GoClusterFS/cmd/gfsctl/client"
)

var adminCmd = &cobra.Command{
	Use:   "admin [subcommand]",
	Short: "Administrative commands",
	Long: `Administrative operations for filesystem maintenance.

Available subcommands:
  flush-journal  - Flush pending journal transactions to disk
  clear-cache    - Clear filesystem caches
  force-trim     - Force TRIM operation on the block device

These commands require careful consideration and may impact performance
while running. Use --force to skip confirmation prompts.

Examples:
  gfsctl admin flush-journal           # Flush journal with confirmation
  gfsctl admin clear-cache             # Clear all caches
  gfsctl admin clear-cache --type inode  # Clear only inode cache
  gfsctl admin force-trim              # Force TRIM operation`,
	Args: cobra.MaximumNArgs(1),
	RunE: runAdmin,
}

var (
	adminForce     bool
	adminCacheType string
)

func init() {
	adminCmd.Flags().BoolVarP(&adminForce, "force", "f", false, "Skip confirmation prompts")
	adminCmd.Flags().StringVar(&adminCacheType, "type", "all", "Cache type to clear (all, inode, extent, negative)")
	rootCmd.AddCommand(adminCmd)
}

func runAdmin(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	apiClient := GetClient()
	f := NewFormatter(cmd.OutOrStdout(), GetOutputFormat())

	if len(args) == 0 {
		return fmt.Errorf("subcommand required (valid: flush-journal, clear-cache, force-trim)")
	}

	subcmd := args[0]

	switch subcmd {
	case "flush-journal":
		return flushJournal(ctx, apiClient, f)
	case "clear-cache":
		return clearCache(ctx, apiClient, f)
	case "force-trim":
		return forceTrim(ctx, apiClient, f)
	default:
		return fmt.Errorf("unknown subcommand: %s (valid: flush-journal, clear-cache, force-trim)", subcmd)
	}
}

func flushJournal(ctx context.Context, c *client.Client, f *Formatter) error {
	if !adminForce && !f.IsJSON() {
		f.Printf("This will flush all pending journal transactions to disk.\n")
		f.Printf("The operation may cause a brief pause in I/O operations.\n\n")

		if !confirmAction("Proceed with journal flush?") {
			f.Printf("Cancelled.\n")
			return nil
		}
	}

	if !f.IsJSON() {
		f.Printf("Flushing journal...")
	}

	req := &client.FlushJournalRequest{
		Force: adminForce,
	}

	resp, err := c.AdminFlushJournal(ctx, req)
	if err != nil {
		if !f.IsJSON() {
			f.Printf(" failed\n")
		}
		printError("failed to flush journal: %v", err)
		return err
	}

	if f.IsJSON() {
		return f.JSON(resp)
	}

	f.Printf(" done\n\n")
	f.Header("Journal Flush Complete")
	f.KeyValue("Operation", resp.Operation)
	f.KeyValueColor("Status", resp.Status, statusColor(resp.Status))
	if resp.Message != "" {
		f.KeyValue("Message", resp.Message)
	}
	if resp.Details != "" {
		f.KeyValue("Details", resp.Details)
	}

	fmt.Fprintln(os.Stdout)
	return nil
}

func clearCache(ctx context.Context, c *client.Client, f *Formatter) error {
	cacheDesc := adminCacheType
	if cacheDesc == "all" {
		cacheDesc = "all caches"
	} else {
		cacheDesc = adminCacheType + " cache"
	}

	if !adminForce && !f.IsJSON() {
		f.Printf("This will clear %s.\n", cacheDesc)
		f.Printf("Cached data will need to be re-read from disk.\n\n")

		if !confirmAction("Proceed with cache clear?") {
			f.Printf("Cancelled.\n")
			return nil
		}
	}

	if !f.IsJSON() {
		f.Printf("Clearing %s...", cacheDesc)
	}

	req := &client.ClearCacheRequest{
		CacheType: adminCacheType,
	}

	resp, err := c.AdminClearCache(ctx, req)
	if err != nil {
		if !f.IsJSON() {
			f.Printf(" failed\n")
		}
		printError("failed to clear cache: %v", err)
		return err
	}

	if f.IsJSON() {
		return f.JSON(resp)
	}

	f.Printf(" done\n\n")
	f.Header("Cache Clear Complete")
	f.KeyValue("Operation", resp.Operation)
	f.KeyValueColor("Status", resp.Status, statusColor(resp.Status))
	if resp.Message != "" {
		f.KeyValue("Message", resp.Message)
	}
	if resp.Details != "" {
		f.KeyValue("Details", resp.Details)
	}

	fmt.Fprintln(os.Stdout)
	return nil
}

func forceTrim(ctx context.Context, c *client.Client, f *Formatter) error {
	if !adminForce && !f.IsJSON() {
		f.Printf("This will force a TRIM operation on the underlying block device.\n")
		f.Printf("This may take some time depending on the amount of free space.\n\n")

		if !confirmAction("Proceed with TRIM operation?") {
			f.Printf("Cancelled.\n")
			return nil
		}
	}

	if !f.IsJSON() {
		f.Printf("Performing TRIM operation...")
	}

	req := &client.ForceTrimRequest{
		Force: adminForce,
	}

	resp, err := c.AdminForceTrim(ctx, req)
	if err != nil {
		if !f.IsJSON() {
			f.Printf(" failed\n")
		}
		printError("failed to perform TRIM: %v", err)
		return err
	}

	if f.IsJSON() {
		return f.JSON(resp)
	}

	f.Printf(" done\n\n")
	f.Header("TRIM Operation Complete")
	f.KeyValue("Operation", resp.Operation)
	f.KeyValueColor("Status", resp.Status, statusColor(resp.Status))
	if resp.Message != "" {
		f.KeyValue("Message", resp.Message)
	}
	if resp.Details != "" {
		f.KeyValue("Details", resp.Details)
	}

	fmt.Fprintln(os.Stdout)
	return nil
}

// statusColor returns the color indicator for an operation status.
func statusColor(status string) string {
	switch status {
	case "success", "complete", "ok":
		return "ok"
	case "failed", "error":
		return "error"
	default:
		return "warning"
	}
}
