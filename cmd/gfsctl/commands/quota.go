package commands

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/supporttools/GoClusterFS/cmd/gfsctl/client"
)

var quotaCmd = &cobra.Command{
	Use:   "quota",
	Short: "Read or set a directory's project quota",
	Long: `Read or set the project quota of a directory, as getfattr and setfattr do
with trusted.goclusterfs.quota. PATH is absolute within the mount, such as /pvc-1.

Both need the API's token: set GFSCTL_TOKEN, --token or token in the config file.`,
}

var quotaGetCmd = &cobra.Command{
	Use:   "get PATH",
	Short: "Show the limits and usage of the project a directory is in",
	Example: `  gfsctl quota get /pvc-1
  gfsctl quota get /pvc-1 -o json`,
	Args: cobra.ExactArgs(1),
	RunE: runQuotaGet,
}

var quotaSetCmd = &cobra.Command{
	Use:   "set PATH",
	Short: "Set the limits of a project, making an empty directory one",
	Long: `Set the limits of the project whose root is PATH. On an empty directory with
no project, it makes the directory the root of a new project. A limit of 0, or
one left out, means none.`,
	Example: `  gfsctl quota set /pvc-1 --bytes 10737418240 --inodes 100000
  gfsctl quota set /pvc-1 --bytes 21474836480`,
	Args: cobra.ExactArgs(1),
	RunE: runQuotaSet,
}

var quotaBytes, quotaInodes uint64

func init() {
	quotaSetCmd.Flags().Uint64Var(&quotaBytes, "bytes", 0, "block limit in bytes, rounded up to whole blocks; 0 for none")
	quotaSetCmd.Flags().Uint64Var(&quotaInodes, "inodes", 0, "inode limit; 0 for none")
	quotaCmd.AddCommand(quotaGetCmd, quotaSetCmd)
	rootCmd.AddCommand(quotaCmd)
}

func runQuotaGet(cmd *cobra.Command, args []string) error {
	q, err := GetClient().Quota(context.Background(), args[0])
	if err != nil {
		return fmt.Errorf("failed to read the quota of %s: %w", args[0], err)
	}
	return printQuota(cmd, q)
}

func runQuotaSet(cmd *cobra.Command, args []string) error {
	var bytes, inodes *uint64
	if cmd.Flags().Changed("bytes") {
		bytes = &quotaBytes
	}
	if cmd.Flags().Changed("inodes") {
		inodes = &quotaInodes
	}
	if bytes == nil && inodes == nil {
		return usageErrorf("set --bytes, --inodes or both")
	}
	q, err := GetClient().SetQuota(context.Background(), args[0], bytes, inodes)
	if err != nil {
		return fmt.Errorf("failed to set the quota of %s: %w", args[0], err)
	}
	return printQuota(cmd, q)
}

func printQuota(cmd *cobra.Command, q *client.QuotaInfo) error {
	f := NewFormatter(cmd.OutOrStdout(), GetOutputFormat())
	if f.IsJSON() {
		return f.JSON(q)
	}
	limit := func(n uint64, format func(uint64) string) string {
		if n == 0 {
			return "none"
		}
		return format(n)
	}
	f.KeyValue("Path", q.Path)
	f.KeyValue("Project", q.Project)
	f.KeyValue("Bytes used", fmt.Sprintf("%s of %s", FormatBytes(q.UsedBytes), limit(q.Bytes, FormatBytes)))
	f.KeyValue("Inodes used", fmt.Sprintf("%d of %s", q.UsedInodes, limit(q.Inodes, func(n uint64) string { return fmt.Sprint(n) })))
	return nil
}
