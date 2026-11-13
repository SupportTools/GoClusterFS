package commands

import (
	"errors"
	"fmt"
	"net"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/supporttools/GoClusterFS/cmd/gfsctl/client"
	"github.com/supporttools/GoClusterFS/internal/cli"
)

// gfsctl health adds these to the exit codes in internal/cli.
const (
	exitDegraded  = 1
	exitUnhealthy = 4
)

type usageError struct{ err error }

func (e usageError) Error() string { return e.err.Error() }
func (e usageError) Unwrap() error { return e.err }

func usageErrorf(format string, args ...any) error {
	return usageError{fmt.Errorf(format, args...)}
}

type healthError struct{ status string }

func (e healthError) Error() string { return "filesystem is " + e.status }

// ExitCode returns the process exit code for an error from Execute.
func ExitCode(err error) int {
	var health healthError
	switch {
	case err == nil:
		return cli.ExitOK
	case errors.As(err, &health) && health.status == "degraded":
		return exitDegraded
	case errors.As(err, &health):
		return exitUnhealthy
	case errors.As(err, &usageError{}):
		return cli.ExitUsage
	}
	return cli.ExitFailure
}

// hint says what to do about err, or returns "" when its message says enough.
func hint(err error, cmd *cobra.Command) string {
	var apiErr *client.APIError
	var netErr net.Error
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &usageError{}):
		return "run " + cmd.CommandPath() + " --help"
	case errors.Is(err, syscall.ECONNREFUSED):
		return fmt.Sprintf("is goclusterfs-mount running with its API on %s? Point --host and --port (or GFSCTL_HOST and GFSCTL_PORT) at its -api-addr", apiClient.BaseURL())
	case errors.As(err, &dnsErr):
		return "check --host or GFSCTL_HOST"
	case errors.As(err, &netErr) && netErr.Timeout():
		return fmt.Sprintf("the mount did not answer within %v; check that it is not stuck, or raise --timeout", cfgTimeout)
	case errors.As(err, &apiErr) && apiErr.IsUnauthorized():
		return "set GFSCTL_TOKEN, --token or token in the config file (gfsctl config path) to the mount's API token"
	}
	return ""
}
