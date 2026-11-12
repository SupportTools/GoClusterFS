package commands

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Print Prometheus metrics",
	Long: `Print the mount's Prometheus metrics from /metrics as text.

Examples:
  gfsctl metrics                          # All metrics
  gfsctl metrics | grep '^goclusterfs_'   # Only GoClusterFS metrics`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		body, err := GetClient().Metrics(context.Background())
		if err != nil {
			return fmt.Errorf("failed to get metrics: %w", err)
		}
		_, err = cmd.OutOrStdout().Write(body)
		return err
	},
}

func init() {
	rootCmd.AddCommand(metricsCmd)
}
