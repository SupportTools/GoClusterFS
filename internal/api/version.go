package api

import "runtime"

// Build-time variables set via ldflags:
//   -ldflags "-X github.com/supporttools/GoClusterFS/internal/api.Version=1.0.0
//             -X github.com/supporttools/GoClusterFS/internal/api.GitCommit=abc123
//             -X github.com/supporttools/GoClusterFS/internal/api.BuildTime=2024-01-01T00:00:00Z"
var (
	// Version is the semantic version of the build.
	Version = "dev"

	// GitCommit is the git commit hash of the build.
	GitCommit = ""

	// BuildTime is the time the build was created.
	BuildTime = ""
)

// BuildInfo contains build version information.
type BuildInfo struct {
	Version   string `json:"version"`
	GitCommit string `json:"git_commit,omitempty"`
	BuildTime string `json:"build_time,omitempty"`
	GoVersion string `json:"go_version"`
}

// GetBuildInfo returns the current build information.
func GetBuildInfo() BuildInfo {
	return BuildInfo{
		Version:   Version,
		GitCommit: GitCommit,
		BuildTime: BuildTime,
		GoVersion: runtime.Version(),
	}
}
