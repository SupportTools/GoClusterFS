package commands

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check filesystem health",
	Long: `Check the health status of the filesystem and its components.
Returns exit code 0 if healthy, 1 if unhealthy.

Examples:
  gfsctl health                 # Check health in text format
  gfsctl health -o json         # Check health in JSON format`,
	RunE: runHealth,
}

func init() {
	rootCmd.AddCommand(healthCmd)
}

func runHealth(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	client := GetClient()

	health, err := client.Health(ctx)
	if err != nil {
		return fmt.Errorf("failed to get health: %w", err)
	}

	f := NewFormatter(cmd.OutOrStdout(), GetOutputFormat())

	if f.IsJSON() {
		return f.JSON(health)
	}

	// Text output
	f.Header("Health Status")

	statusColor := string(health.Status)
	f.KeyValueColor("Overall", string(health.Status), statusColor)

	// Component checks
	if len(health.Checks) > 0 {
		f.Header("Component Checks")
		for name, check := range health.Checks {
			checkColor := string(check.Status)
			msg := string(check.Status)
			if check.Message != "" {
				msg = fmt.Sprintf("%s - %s", check.Status, check.Message)
			}
			f.KeyValueColor(name, msg, checkColor)
		}
	}

	f.Println()

	// Return error if unhealthy (for scripting)
	if health.Status == "unhealthy" {
		return fmt.Errorf("filesystem is unhealthy")
	}

	return nil
}
