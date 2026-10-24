// Package version holds build metadata. Release builds set it with -ldflags -X.
package version

import "fmt"

var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

func String() string {
	return fmt.Sprintf("%s (commit %s, %s)", Version, Commit, Date)
}
