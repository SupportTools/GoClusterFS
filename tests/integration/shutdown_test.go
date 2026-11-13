package integration

import (
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/api"
)

func exitCode(t *testing.T, binary string, args ...string) (int, string) {
	t.Helper()
	out, err := exec.Command(binary, args...).CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	}
	t.Fatalf("run %s %v: %v", binary, args, err)
	return 0, ""
}

func TestHealthAndSignalShutdown(t *testing.T) {
	m := startAPIMount(t, "", 20, "-broadcast=false")
	base := "http://" + m.addr

	var health api.HealthResponse
	if code := getAPI(t, base+"/health", &health); code != http.StatusOK || health.Status != api.HealthStatusOK {
		t.Fatalf("GET /health = %d %+v", code, health)
	}
	for _, name := range []string{"device", "journal", "recovery", "broadcast"} {
		if check, ok := health.Checks[name]; !ok || check.Status != api.HealthStatusOK {
			t.Errorf("check %s = %+v (present %v), want ok", name, check, ok)
		}
	}
	if msg := health.Checks["broadcast"].Message; !strings.Contains(msg, "-broadcast=false") {
		t.Errorf("broadcast check message = %q, want it to say broadcast is off", msg)
	}

	if err := m.WriteFile("held", []byte("data")); err != nil {
		t.Fatal(err)
	}
	held, err := os.Open(m.Path("held"))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.MountCmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- m.MountCmd.Wait() }()
	if _, ok := waitFor(15*time.Second, func() bool { return strings.Contains(m.log.String(), "Close the files and shells that use it") }); !ok {
		mustClose(t, held)
		t.Fatalf("SIGTERM with an open file did not report a busy unmount:\n%s", m.log.String())
	}
	select {
	case err := <-exited:
		mustClose(t, held)
		t.Fatalf("mount exited (%v) while a file under it was open", err)
	case <-time.After(time.Second):
	}
	if !m.IsMounted() {
		t.Fatal("the busy mount was unmounted")
	}
	if code := getAPI(t, base+"/health", nil); code != 0 {
		t.Errorf("GET /health after SIGTERM = %d, want a refused connection", code)
	}

	mustClose(t, held)
	if err := m.MountCmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("mount exited with %v after the second SIGTERM, want 0\n%s", err, m.log.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("mount did not exit within 30 s of the second SIGTERM\n%s", m.log.String())
	}
	if m.IsMounted() {
		t.Error("mount point still mounted after the mount exited")
	}
	if code, out := runFsck(t, m.ImagePath); code != 0 {
		t.Errorf("fsck after the signal shutdown exited %d:\n%s", code, out)
	}
}

func TestCommandsShareExitCodes(t *testing.T) {
	mount, err := findBinary()
	if err != nil {
		t.Fatal(err)
	}
	fsck := findFsckBinary(t)
	gfsctl := filepath.Join(filepath.Dir(mount), "gfsctl")
	if _, err := os.Stat(gfsctl); err != nil {
		t.Fatalf("gfsctl not found next to %s; run 'make build' first", mount)
	}
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.img")

	tests := []struct {
		binary string
		args   []string
		code   int
		says   string
	}{
		{mount, []string{"-h"}, 0, "Exit codes"},
		{mount, []string{"-no-such-flag"}, 16, "flag provided but not defined"},
		{mount, []string{"-device", missing}, 16, "-device and -mount are required"},
		{mount, []string{"-device", missing, "-mount", filepath.Join(dir, "none"), "-broadcast=false", "-api=false"}, 8, "mkdir -p"},
		{mount, []string{"-device", missing, "-mount", dir, "-broadcast=false", "-api=false"}, 8, "check the path"},
		{fsck, []string{"-h"}, 0, "Exit codes"},
		{fsck, []string{"-no-such-flag"}, 16, "flag provided but not defined"},
		{fsck, nil, 16, "give exactly one device"},
		{fsck, []string{missing}, 8, "check the path"},
		{gfsctl, []string{"--no-such-flag"}, 16, "--help"},
		{gfsctl, []string{"--port", "1", "health"}, 8, "is goclusterfs-mount running"},
	}
	for _, tt := range tests {
		code, out := exitCode(t, tt.binary, tt.args...)
		if code != tt.code || !strings.Contains(out, tt.says) {
			t.Errorf("%s %q exited %d, want %d with %q:\n%s", filepath.Base(tt.binary), tt.args, code, tt.code, tt.says, out)
		}
	}
}
