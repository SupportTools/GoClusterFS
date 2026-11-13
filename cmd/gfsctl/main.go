// Command gfsctl reads status, health, statistics and metrics from a
// running GoClusterFS mount through its HTTP API. It changes nothing.
package main

import (
	"os"

	"github.com/supporttools/GoClusterFS/cmd/gfsctl/commands"
)

func main() {
	os.Exit(commands.ExitCode(commands.Execute()))
}
