// Package commands implements the gfsctl CLI commands.
package commands

import (
	"fmt"
	"os"
	"strconv"
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

It reads status, health, statistics and metrics from a running
GoClusterFS mount through its HTTP API. It changes nothing.

Settings come from flags, then GFSCTL_HOST, GFSCTL_PORT, GFSCTL_TOKEN,
GFSCTL_OUTPUT and GFSCTL_TIMEOUT, then the config file (gfsctl config path).

Examples:
  gfsctl status                    # Show filesystem status
  gfsctl health                    # Check health status
  gfsctl stats cache               # Show cache statistics
  gfsctl metrics                   # Print Prometheus metrics
  gfsctl --host 10.0.0.1 status    # Connect to remote host`,
	SilenceUsage:  true,
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if err := resolveSettings(cmd); err != nil {
			return err
		}
		apiClient = client.New(
			client.WithHost(cfgHost),
			client.WithPort(cfgPort),
			client.WithToken(cfgToken),
			client.WithTimeout(cfgTimeout),
		)
		return nil
	},
}

// Execute runs the root command and prints any error to stderr.
func Execute() error {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintln(rootCmd.ErrOrStderr(), "Error:", err)
	}
	return err
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgHost, "host", "localhost", "API server host")
	rootCmd.PersistentFlags().IntVar(&cfgPort, "port", 8080, "API server port")
	rootCmd.PersistentFlags().StringVar(&cfgToken, "token", "", "Bearer token for authentication (visible in ps; prefer GFSCTL_TOKEN or the config file)")
	rootCmd.PersistentFlags().DurationVar(&cfgTimeout, "timeout", 30*time.Second, "Request timeout")
	rootCmd.PersistentFlags().StringVarP(&cfgOutput, "output", "o", "text", "Output format (text, json)")
}

// resolveSettings fills every setting whose flag was not given from the
// environment, then from the config file.
func resolveSettings(cmd *cobra.Command) error {
	file, err := loadConfig()
	if err != nil {
		return fmt.Errorf("%w (%s)", err, configPath())
	}
	flags := cmd.Flags()

	if !flags.Changed("host") {
		cfgHost = firstNonEmpty(os.Getenv("GFSCTL_HOST"), file.Host, cfgHost)
	}
	if !flags.Changed("port") {
		if env := os.Getenv("GFSCTL_PORT"); env != "" {
			port, err := strconv.Atoi(env)
			if err != nil || port <= 0 || port > 65535 {
				return fmt.Errorf("GFSCTL_PORT=%q is not a port number", env)
			}
			cfgPort = port
		} else if file.Port != 0 {
			cfgPort = file.Port
		}
	}
	if !flags.Changed("token") {
		cfgToken = firstNonEmpty(os.Getenv("GFSCTL_TOKEN"), file.Token, cfgToken)
	}
	if !flags.Changed("output") {
		cfgOutput = firstNonEmpty(os.Getenv("GFSCTL_OUTPUT"), file.Output, cfgOutput)
	}
	if !flags.Changed("timeout") {
		if env := os.Getenv("GFSCTL_TIMEOUT"); env != "" {
			timeout, err := time.ParseDuration(env)
			if err != nil {
				return fmt.Errorf("GFSCTL_TIMEOUT=%q: %w", env, err)
			}
			cfgTimeout = timeout
		} else if file.Timeout != 0 {
			cfgTimeout = file.Timeout
		}
	}

	if cfgOutput != "text" && cfgOutput != "json" {
		return fmt.Errorf("unknown output format %q (valid: text, json)", cfgOutput)
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
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
