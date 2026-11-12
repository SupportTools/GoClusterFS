package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAPIServesHealthOnLoopback(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	addr := freeLoopbackAddr(t)
	mfs.ExtraArgs = []string{"-format", "-api-addr", addr}
	if err := mfs.Mount(false); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := client.Get("http://" + addr + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET /health returned %d, want 200", resp.StatusCode)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("API never answered on %s: %v", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestMountRefusesUnauthenticatedRemoteAPI(t *testing.T) {
	binary, err := findBinary()
	if err != nil {
		t.Fatalf("Failed to find binary: %v", err)
	}
	dir := t.TempDir()
	mountPoint := filepath.Join(dir, "mnt")
	image := filepath.Join(dir, "disk.img")
	if err := os.Mkdir(mountPoint, 0o755); err != nil {
		t.Fatalf("Failed to create mount point: %v", err)
	}
	if err := os.WriteFile(image, nil, 0o644); err != nil {
		t.Fatalf("Failed to create image: %v", err)
	}
	if err := allocateImage(image, 20); err != nil {
		t.Fatalf("Failed to allocate image: %v", err)
	}

	for _, addr := range []string{":8080", "0.0.0.0:8080"} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		out, err := exec.CommandContext(ctx, binary, "-device", image, "-mount", mountPoint, "-format", "-api-addr", addr).CombinedOutput()
		timedOut := ctx.Err() != nil
		cancel()
		if timedOut {
			exec.Command("fusermount", "-u", mountPoint).Run()
			t.Fatalf("-api-addr %s without -api-token: mount kept running", addr)
		}
		if err == nil {
			t.Fatalf("-api-addr %s without -api-token: mount exited 0", addr)
		}
		if !strings.Contains(string(out), "-api-token") {
			t.Errorf("-api-addr %s: error does not mention -api-token:\n%s", addr, out)
		}
	}

	f, err := os.Open(image)
	if err != nil {
		t.Fatalf("Failed to open image: %v", err)
	}
	defer f.Close()
	head := make([]byte, 4096)
	if _, err := io.ReadFull(f, head); err != nil {
		t.Fatalf("Failed to read image: %v", err)
	}
	if !bytes.Equal(head, make([]byte, len(head))) {
		t.Error("refused mount still wrote a superblock")
	}
}

func runMount(t *testing.T, args ...string) (string, error) {
	t.Helper()
	binary, err := findBinary()
	if err != nil {
		t.Fatalf("Failed to find binary: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("goclusterfs-mount %v did not exit", args)
	}
	return string(out), err
}

func TestMountPrintsVersion(t *testing.T) {
	out, err := runMount(t, "-version")
	if err != nil || !strings.HasPrefix(out, "goclusterfs-mount ") {
		t.Fatalf("-version: err=%v output=%q", err, out)
	}
}

func TestMountWarnsWithoutBroadcastSecret(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.img")
	const warning = "WARNING: -broadcast is on without -broadcast-secret"

	if out, _ := runMount(t, "-device", missing, "-mount", dir); !strings.Contains(out, warning) {
		t.Errorf("no warning without a secret:\n%s", out)
	}
	for _, flag := range []string{"-broadcast-secret=x", "-broadcast=false"} {
		if out, _ := runMount(t, "-device", missing, "-mount", dir, flag); strings.Contains(out, warning) {
			t.Errorf("warning with %s:\n%s", flag, out)
		}
	}
}

func TestMountRejectsBadAPISettingsBeforeOpeningDevice(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.img")
	for _, tt := range []struct{ flag, want string }{
		{"-api-addr=127.0.0.1", "Invalid API settings"},
		{"-api-cors=not-a-url", "Invalid API settings"},
		{"-api-cors=*", "-api-cors"},
		{"-api-rate-limit=-1", "Invalid API settings"},
		{"-api-rate-limit=NaN", "Invalid API settings"},
		{"-api-rate-limit=+Inf", "Invalid API settings"},
		{"-api-rate-limit=1e19", "Invalid API settings"},
	} {
		out, err := runMount(t, "-device", missing, "-mount", dir, tt.flag)
		if err == nil || !strings.Contains(out, tt.want) || strings.Contains(out, "Failed to open device") {
			t.Errorf("%s: err=%v, want an error mentioning %q before the device opens:\n%s", tt.flag, err, tt.want, out)
		}
	}
}

func TestGfsctlReadsLiveMount(t *testing.T) {
	mountBinary, err := findBinary()
	if err != nil {
		t.Fatalf("Failed to find binary: %v", err)
	}
	gfsctl := filepath.Join(filepath.Dir(mountBinary), "gfsctl")
	if _, err := os.Stat(gfsctl); err != nil {
		t.Fatalf("gfsctl not found next to %s; run 'make build' first", mountBinary)
	}

	mfs := NewMountedFS(t, 20)
	addr := freeLoopbackAddr(t)
	mfs.ExtraArgs = []string{"-format", "-api-addr", addr}
	if err := mfs.Mount(false); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	host, port, _ := net.SplitHostPort(addr)

	ctl := func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, gfsctl, append([]string{"--host", host, "--port", port}, args...)...)
		cmd.Env = append(os.Environ(), "XDG_CONFIG_HOME="+t.TempDir())
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	var out string
	deadline := time.Now().Add(10 * time.Second)
	for {
		if out, err = ctl("status", "-o", "json"); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("gfsctl status: %v\n%s", err, out)
	}
	var status struct {
		MountState string `json:"mount_state"`
		MountPath  string `json:"mount_path"`
	}
	if err := json.Unmarshal([]byte(out), &status); err != nil || status.MountState != "mounted" || status.MountPath != mfs.MountPoint {
		t.Fatalf("gfsctl status -o json: %v\n%s", err, out)
	}
	if out, err := ctl("health"); err != nil || !strings.Contains(out, "ok") {
		t.Errorf("gfsctl health: %v\n%s", err, out)
	}
	if out, err := ctl("metrics"); err != nil || !strings.Contains(out, "goclusterfs_up 1") {
		t.Errorf("gfsctl metrics: %v\n%s", err, out)
	}
}
