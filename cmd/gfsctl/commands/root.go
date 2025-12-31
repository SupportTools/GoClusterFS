// Package commands implements the gfsctl CLI commands.
package commands

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/supporttools/GoClusterFS/cmd/gfsctl/client"
)

var (
	// Global flags
	cfgHost    string
	cfgPort    int
	cfgToken   string
	cfgTimeout time.Duration
	cfgOutput  string

	// API client (initialized in PersistentPreRunE)
	apiClient *client.Client
)

// rootCmd is the base command when called without subcommands.
var rootCmd = &cobra.Command{
	Use:   "gfsctl",
	Short: "GoClusterFS management CLI",
	Long: `gfsctl is the management CLI for GoClusterFS.

It communicates with a running GoClusterFS mount via its HTTP API
to query status, statistics, and perform administrative operations.

Examples:
  gfsctl status                    # Show filesystem status
  gfsctl health                    # Check health status
  gfsctl stats cache               # Show cache statistics
  gfsctl cluster nodes             # List cluster nodes
  gfsctl --host 10.0.0.1 status    # Connect to remote host`,
	SilenceUsage:  true,
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Skip client initialization for version command
		if cmd.Name() == "version" {
			return nil
		}

		// Create API client with configured options
		apiClient = client.New(
			client.WithHost(cfgHost),
			client.WithPort(cfgPort),
			client.WithToken(cfgToken),
			client.WithTimeout(cfgTimeout),
		)
		return nil
	},
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Global flags available to all commands
	rootCmd.PersistentFlags().StringVar(&cfgHost, "host", "localhost", "API server host")
	rootCmd.PersistentFlags().IntVar(&cfgPort, "port", 8080, "API server port")
	rootCmd.PersistentFlags().StringVar(&cfgToken, "token", "", "Bearer token for authentication")
	rootCmd.PersistentFlags().DurationVar(&cfgTimeout, "timeout", 30*time.Second, "Request timeout")
	rootCmd.PersistentFlags().StringVarP(&cfgOutput, "output", "o", "text", "Output format (text, json)")

	// Load configuration file (lowest priority)
	initConfig()

	// Environment variable fallbacks (higher priority than config file)
	if host := os.Getenv("GFSCTL_HOST"); host != "" {
		cfgHost = host
	}
	if token := os.Getenv("GFSCTL_TOKEN"); token != "" {
		cfgToken = token
	}
	if output := os.Getenv("GFSCTL_OUTPUT"); output != "" {
		cfgOutput = output
	}
}

// GetClient returns the initialized API client.
// Commands should call this after PersistentPreRunE has executed.
func GetClient() *client.Client {
	return apiClient
}

// GetOutputFormat returns the configured output format.
func GetOutputFormat() string {
	return cfgOutput
}

// printError prints an error message to stderr.
func printError(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "Error: "+format+"\n", args...)
}
