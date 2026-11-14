package version

import (
	"runtime/debug"
	"testing"
)

func withDefaults(t *testing.T, version, commit, date string) {
	t.Helper()
	oldV, oldC, oldD := Version, Commit, Date
	Version, Commit, Date = version, commit, date
	t.Cleanup(func() { Version, Commit, Date = oldV, oldC, oldD })
}

func TestFillFromBuildInfo(t *testing.T) {
	goInstall := &debug.BuildInfo{Main: debug.Module{Version: "v0.0.0-20261113220000-abcdef123456"}}
	checkout := &debug.BuildInfo{
		Main: debug.Module{Version: "(devel)"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "0123456789abcdef"},
			{Key: "vcs.time", Value: "2026-11-13T22:00:00Z"},
		},
	}
	tests := []struct {
		name                  string
		ldflags               [3]string
		info                  *debug.BuildInfo
		version, commit, date string
	}{
		{"go install", [3]string{"dev", "unknown", "unknown"}, goInstall, "v0.0.0-20261113220000-abcdef123456", "unknown", "unknown"},
		{"build in a checkout", [3]string{"dev", "unknown", "unknown"}, checkout, "dev", "0123456789abcdef", "2026-11-13T22:00:00Z"},
		{"release build keeps ldflags", [3]string{"v1.0.0", "fedcba", "2026-11-01"}, checkout, "v1.0.0", "fedcba", "2026-11-01"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withDefaults(t, tt.ldflags[0], tt.ldflags[1], tt.ldflags[2])
			fillFromBuildInfo(tt.info)
			if Version != tt.version || Commit != tt.commit || Date != tt.date {
				t.Errorf("got %q %q %q, want %q %q %q", Version, Commit, Date, tt.version, tt.commit, tt.date)
			}
		})
	}
}
