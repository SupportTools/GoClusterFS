package integration

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A file image has no persistent reservations: -fencing=pr refuses to format it, the default formats it unfenced with
// a warning that /health repeats, and -require-fencing refuses to mount it.
func TestFencingOnAFileImage(t *testing.T) {
	binary, err := findBinary()
	if err != nil {
		t.Fatalf("Failed to find binary: %v", err)
	}
	run := func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("goclusterfs-mount %v kept running", args)
		}
		return string(out), err
	}

	mfs := NewMountedFS(t, 20)
	out, err := run("-device", mfs.ImagePath, "-mount", mfs.MountPoint, "-format", "-fencing=pr")
	if err == nil || !strings.Contains(out, "does not support persistent reservations") {
		t.Fatalf("-fencing=pr on a file image: %v\n%s", err, out)
	}

	addr := freeLoopbackAddr(t)
	logs := &syncBuffer{}
	mfs.Log = logs
	mfs.ExtraArgs = []string{"-format", "-api-addr", addr}
	if err := mfs.Mount(false); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	if !strings.Contains(logs.String(), "has no storage fencing") || !strings.Contains(logs.String(), "-require-fencing") {
		t.Errorf("mount of an unfenced volume did not warn about it and name -require-fencing:\n%s", logs.String())
	}
	var health string
	for deadline := time.Now().Add(10 * time.Second); health == "" && time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if resp, err := http.Get("http://" + addr + "/health"); err == nil {
			body, _ := io.ReadAll(resp.Body)
			mustClose(t, resp.Body)
			health = string(body)
		}
	}
	if !strings.Contains(health, `"fencing"`) || !strings.Contains(health, "none: this volume has no storage fencing") {
		t.Errorf("/health does not report the missing fencing:\n%s", health)
	}
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}

	mountPoint := filepath.Join(t.TempDir(), "mnt")
	if err := os.Mkdir(mountPoint, 0o755); err != nil {
		t.Fatal(err)
	}
	out, err = run("-device", mfs.ImagePath, "-mount", mountPoint, "-require-fencing")
	if err == nil || !strings.Contains(out, "-require-fencing is set") {
		t.Errorf("-require-fencing on an unfenced volume: %v\n%s", err, out)
	}
}
