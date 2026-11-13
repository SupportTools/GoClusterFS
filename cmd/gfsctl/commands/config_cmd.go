package commands

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config [subcommand]",
	Short: "Manage gfsctl configuration",
	Long: `Manage gfsctl configuration file.

The configuration file is located at:
  $XDG_CONFIG_HOME/gfsctl/config.yaml
  or ~/.config/gfsctl/config.yaml

Subcommands:
  path      - Show configuration file path
  show      - Show current configuration values
  init      - Create a sample configuration file

Configuration values can be overridden by:
  1. Environment variables (GFSCTL_HOST, GFSCTL_TOKEN, etc.)
  2. Command-line flags (--host, --token, etc.)

Examples:
  gfsctl config path            # Show config file location
  gfsctl config show            # Show current configuration
  gfsctl config init            # Create sample config file`,
	Args: cobra.MaximumNArgs(1),
	RunE: runConfig,
}

func init() {
	rootCmd.AddCommand(configCmd)
}

func runConfig(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return cmd.Help()
	}

	subcmd := args[0]

	switch subcmd {
	case "path":
		return showConfigPath(cmd)
	case "show":
		return showCurrentConfig(cmd)
	case "init":
		return initConfigFile(cmd)
	default:
		return usageErrorf("unknown subcommand: %s (valid: path, show, init)", subcmd)
	}
}

func showConfigPath(cmd *cobra.Command) error {
	path := configPath()
	if path == "" {
		fmt.Fprintln(cmd.OutOrStdout(), "Unable to determine config path")
		return nil
	}

	fmt.Fprintln(cmd.OutOrStdout(), path)

	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fmt.Fprintln(cmd.OutOrStdout(), "(file does not exist)")
	}

	return nil
}

func showCurrentConfig(cmd *cobra.Command) error {
	f := NewFormatter(cmd.OutOrStdout(), GetOutputFormat())

	if f.IsJSON() {
		return f.JSON(map[string]interface{}{
			"config_path": configPath(),
			"host":        cfgHost,
			"port":        cfgPort,
			"token":       maskToken(cfgToken),
			"output":      cfgOutput,
			"timeout":     cfgTimeout.String(),
		})
	}

	f.Header("Current Configuration")
	f.KeyValue("Config File", configPath())
	f.Printf("\n")

	// Show effective values with source indication
	f.KeyValue("Host", cfgHost)
	f.KeyValue("Port", fmt.Sprintf("%d", cfgPort))
	f.KeyValue("Token", maskToken(cfgToken))
	f.KeyValue("Output", cfgOutput)
	f.KeyValue("Timeout", cfgTimeout.String())

	f.Println()
	return nil
}

func initConfigFile(cmd *cobra.Command) error {
	path := configPath()
	if path == "" {
		return fmt.Errorf("unable to determine config directory")
	}

	// Check if file already exists
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("config file already exists: %s", path)
	}

	// Create directory if needed
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Write sample config
	content := GenerateSampleConfig()
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Created config file: %s\n", path)
	return nil
}

// maskToken masks a token for display, showing only first/last 4 chars.
func maskToken(token string) string {
	if token == "" {
		return "(not set)"
	}
	return fmt.Sprintf("set (%d chars)", len(token))
}
