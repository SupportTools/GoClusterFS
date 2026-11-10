package api

import (
	"runtime"

	"github.com/supporttools/GoClusterFS/internal/version"
)

// BuildInfo contains build version information.
type BuildInfo struct {
	Version   string `json:"version"`
	GitCommit string `json:"git_commit,omitempty"`
	BuildTime string `json:"build_time,omitempty"`
	GoVersion string `json:"go_version"`
}

// GetBuildInfo returns the build metadata from internal/version.
func GetBuildInfo() BuildInfo {
	return BuildInfo{
		Version:   version.Version,
		GitCommit: version.Commit,
		BuildTime: version.Date,
		GoVersion: runtime.Version(),
	}
}
