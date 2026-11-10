package api

import (
	"testing"

	"github.com/supporttools/GoClusterFS/internal/version"
)

func TestBuildInfoUsesVersionPackage(t *testing.T) {
	saved := version.Version
	t.Cleanup(func() { version.Version = saved })
	version.Version = "1.2.3"

	if got := GetBuildInfo().Version; got != "1.2.3" {
		t.Fatalf("GetBuildInfo().Version = %q, want internal/version's 1.2.3", got)
	}
}
