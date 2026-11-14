// Package version holds build metadata. Release builds set it with -ldflags -X;
// other builds fall back to what the Go toolchain embeds.
package version

import (
	"fmt"
	"runtime/debug"
)

var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		fillFromBuildInfo(info)
	}
}

// fillFromBuildInfo sets what -ldflags left at its default: the module version
// from go install, and the commit and time from a build in a git checkout.
func fillFromBuildInfo(info *debug.BuildInfo) {
	if Version == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
		Version = info.Main.Version
	}
	for _, s := range info.Settings {
		switch {
		case s.Key == "vcs.revision" && Commit == "unknown":
			Commit = s.Value
		case s.Key == "vcs.time" && Date == "unknown":
			Date = s.Value
		}
	}
}

func String() string {
	return fmt.Sprintf("%s (commit %s, %s)", Version, Commit, Date)
}
