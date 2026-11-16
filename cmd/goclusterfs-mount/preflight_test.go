package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/cache/broadcast"
	"github.com/supporttools/GoClusterFS/internal/cli"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// imagePath creates a sparse image of size bytes that storage.Open can open, or skips.
func imagePath(t *testing.T, size int64, format bool) string {
	t.Helper()
	dir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	path := filepath.Join(dir, fmt.Sprintf("preflight-%d.img", time.Now().UnixNano()))
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	if err := os.Truncate(path, size); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	defer func() { _ = dev.Close() }()
	if format {
		if _, err := superblock.Format(dev, superblock.DefaultBytesPerInode); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func usableConfig(t *testing.T) preflightConfig {
	t.Helper()
	fuse := filepath.Join(t.TempDir(), "fuse")
	if err := os.WriteFile(fuse, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return preflightConfig{
		device:     imagePath(t, superblock.MinDeviceSize, true),
		mountPoint: t.TempDir(),
		fuseDevice: fuse,
		lookPath:   func(string) (string, error) { return "/usr/bin/fusermount3", nil },
		uid:        0,
	}
}

func runChecks(t *testing.T, cfg preflightConfig) (int, string) {
	t.Helper()
	var out strings.Builder
	code := runPreflight(&out, cfg)
	return code, out.String()
}

func TestPreflightPassesOnAUsableSetup(t *testing.T) {
	code, out := runChecks(t, usableConfig(t))
	if code != cli.ExitOK || strings.Contains(out, statusFail) {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if !strings.Contains(out, "holds a version 3 filesystem") {
		t.Errorf("the device check does not report the filesystem:\n%s", out)
	}
}

func TestPreflightNamesAFixForEachFailure(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	code, out := runChecks(t, preflightConfig{
		device:     missing,
		mountPoint: missing,
		fuseDevice: missing,
		lookPath:   func(string) (string, error) { return "", errors.New("not found") },
		uid:        0,
	})
	if code != cli.ExitFailure {
		t.Errorf("exit %d, want %d", code, cli.ExitFailure)
	}
	for _, want := range []string{
		"FAIL  FUSE device", "fix: load the FUSE kernel module",
		"FAIL  fusermount", "fix: install fuse3",
		"FAIL  mount point", "fix: mkdir -p " + missing,
		"FAIL  device", "fix: check the path",
		"4 of 6 checks failed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestPreflightDevice(t *testing.T) {
	blank := imagePath(t, superblock.MinDeviceSize, false)
	small := imagePath(t, superblock.MinDeviceSize-storage.DefaultBlockSize, false)
	tests := []struct {
		name, device string
		format       bool
		status, want string
	}{
		{"no filesystem", blank, false, statusFail, "fix: check -device; to create a new filesystem, add -format"},
		{"format warns", blank, true, statusWarn, "-format erases all data on it"},
		{"too small", small, false, statusFail, "fix: use a larger device"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := usableConfig(t)
			cfg.device, cfg.format = tt.device, tt.format
			_, out := runChecks(t, cfg)
			if !strings.Contains(out, tt.status+"  device") || !strings.Contains(out, tt.want) {
				t.Errorf("want %s and %q:\n%s", tt.status, tt.want, out)
			}
		})
	}
}

func TestPreflightMountPointIsAFile(t *testing.T) {
	cfg := usableConfig(t)
	cfg.mountPoint = cfg.fuseDevice
	if _, out := runChecks(t, cfg); !strings.Contains(out, "FAIL  mount point") || !strings.Contains(out, "is not a directory") {
		t.Errorf("a file passed as the mount point:\n%s", out)
	}
}

func TestPreflightFusermountMustBeSetuidForUsers(t *testing.T) {
	plain := filepath.Join(t.TempDir(), "fusermount3")
	if err := os.WriteFile(plain, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := usableConfig(t)
	cfg.lookPath = func(string) (string, error) { return plain, nil }
	cfg.uid = 1000
	if _, out := runChecks(t, cfg); !strings.Contains(out, "is not setuid root") {
		t.Errorf("a plain fusermount passed for a non-root user:\n%s", out)
	}
}

func TestPreflightTakenPorts(t *testing.T) {
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = udp.Close() }()
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tcp.Close() }()

	cfg := usableConfig(t)
	cfg.network = broadcast.Config{Enabled: true, MulticastGroup: "239.192.74.75", MulticastPort: 31399,
		UnicastPort: udp.LocalAddr().(*net.UDPAddr).Port, Secret: "s"}
	cfg.apiAddr = tcp.Addr().String()
	code, out := runChecks(t, cfg)
	if code != cli.ExitFailure {
		t.Errorf("exit %d, want %d", code, cli.ExitFailure)
	}
	for _, want := range []string{"FAIL  broadcast ports", "fix: free UDP ports", "FAIL  API address", "fix: choose a free address with -api-addr"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestPreflightWarnsWithoutBroadcastSecret(t *testing.T) {
	cfg := usableConfig(t)
	cfg.network = broadcast.Config{Enabled: true, MulticastGroup: "239.192.74.75", MulticastPort: 31398, UnicastPort: 31397}
	code, out := runChecks(t, cfg)
	if !strings.Contains(out, "WARN  broadcast secret") {
		t.Errorf("no warning about the missing secret:\n%s", out)
	}
	if strings.Contains(out, "FAIL  broadcast ports") {
		t.Skipf("UDP ports 31397/31398 are taken on this host:\n%s", out)
	}
	if code != cli.ExitOK {
		t.Errorf("a warning failed the preflight: exit %d\n%s", code, out)
	}
}
