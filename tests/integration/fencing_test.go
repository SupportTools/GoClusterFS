package integration

import (
	"context"
	"encoding/binary"
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
	run := func(args ...string) (string, error) { return runToExit(t, binary, args...) }

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

// A reservation covers the whole disk, and Linux 6.5 and later refuse one on a partition: -fencing=pr refuses to
// format a partition, and the default formats it unfenced and says why.
func TestFencingOnAPartition(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("attaching a loop disk needs root")
	}
	mountBinary, err := findBinary()
	if err != nil {
		t.Fatalf("Failed to find binary: %v", err)
	}
	const size = 400 << 20
	img := filepath.Join(t.TempDir(), "disk.img")
	mbr := make([]byte, 512)
	mbr[446+4] = 0x83
	binary.LittleEndian.PutUint32(mbr[446+8:], 2048)
	binary.LittleEndian.PutUint32(mbr[446+12:], size/512-2048)
	mbr[510], mbr[511] = 0x55, 0xaa
	if err := os.WriteFile(img, mbr, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(img, size); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("losetup", "-P", "-f", "--show", img).Output()
	if err != nil {
		t.Skipf("no loop disk with partitions: %v", err)
	}
	loop := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("losetup", "-d", loop).Run() })
	part := loop + "p1"
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if _, err := os.Stat(part); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("%s did not appear: %v", part, err)
		}
	}

	mountPoint := filepath.Join(t.TempDir(), "mnt")
	if err := os.Mkdir(mountPoint, 0o755); err != nil {
		t.Fatal(err)
	}
	const reason = "on a partition, only on the whole disk"
	out2, err := runToExit(t, mountBinary, "-device", part, "-mount", mountPoint, "-format", "-fencing=pr")
	if err == nil || !strings.Contains(out2, reason) || !strings.Contains(out2, "format the whole disk") {
		t.Fatalf("-fencing=pr on a partition: %v\n%s", err, out2)
	}

	mfs := NewMountedFSWithImage(t, part)
	logs := &syncBuffer{}
	mfs.Log = logs
	mfs.ExtraArgs = []string{"-format"}
	if err := mfs.Mount(false); err != nil {
		t.Fatalf("the default -fencing=auto did not format and mount a partition: %v\n%s", err, logs.String())
	}
	if !strings.Contains(logs.String(), reason) || !strings.Contains(logs.String(), "formatted without storage fencing") {
		t.Errorf("formatting a partition did not say why it is unfenced:\n%s", logs.String())
	}
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}
}

// runToExit runs goclusterfs-mount with args and fails the test if it is still running after 10 seconds.
func runToExit(t *testing.T, binary string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("goclusterfs-mount %v kept running", args)
	}
	return string(out), err
}
