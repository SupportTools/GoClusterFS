// Command gfsctl is the management CLI for GoClusterFS.
//
// gfsctl communicates with a running GoClusterFS mount via its HTTP API
// to query status, statistics, and perform administrative operations.
//
// Usage:
//
//	gfsctl [global flags] <command> [command flags]
//
// Global flags:
//
//	--host      API server host (default: localhost)
//	--port      API server port (default: 8080)
//	--token     Bearer token for authentication
//	--timeout   Request timeout (default: 30s)
//
// Commands:
//
//	status      Show filesystem status
//	health      Check filesystem health
//	stats       Show component statistics
//	cluster     Cluster management commands
//	version     Show version information
package main

import (
	"os"

	"github.com/supporttools/GoClusterFS/cmd/gfsctl/commands"
)

func main() {
	if err := commands.Execute(); err != nil {
		os.Exit(1)
	}
}
