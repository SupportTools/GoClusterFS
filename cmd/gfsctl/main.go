// Command gfsctl reads status, health, statistics, metrics and quotas from a
// running GoClusterFS mount through its HTTP API, and sets a directory's quota.
package main

import (
	"os"

	"github.com/supporttools/GoClusterFS/cmd/gfsctl/commands"
)

func main() {
	os.Exit(commands.ExitCode(commands.Execute()))
}
