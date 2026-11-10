package commands

import (
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/supporttools/GoClusterFS/internal/version"
)

// VersionInfo contains version information for JSON output.
type VersionInfo struct {
	Version   string `json:"version"`
	GitCommit string `json:"git_commit"`
	BuildTime string `json:"build_time"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show gfsctl version information",
	Long:  `Display version, build information, and runtime details for gfsctl.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		info := VersionInfo{
			Version:   version.Version,
			GitCommit: version.Commit,
			BuildTime: version.Date,
			GoVersion: runtime.Version(),
			OS:        runtime.GOOS,
			Arch:      runtime.GOARCH,
		}

		if GetOutputFormat() == "json" {
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(info)
		}

		fmt.Fprintf(cmd.OutOrStdout(), "gfsctl version %s\n", info.Version)
		fmt.Fprintf(cmd.OutOrStdout(), "  Git commit: %s\n", info.GitCommit)
		fmt.Fprintf(cmd.OutOrStdout(), "  Built:      %s\n", info.BuildTime)
		fmt.Fprintf(cmd.OutOrStdout(), "  Go version: %s\n", info.GoVersion)
		fmt.Fprintf(cmd.OutOrStdout(), "  OS/Arch:    %s/%s\n", info.OS, info.Arch)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
