package node

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// zombieChild starts a child that exits at once and is never reaped, so it
// stays a zombie until the test ends, as a mount does whose parent never waits.
func zombieChild(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start true: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Wait() })
	pid := cmd.Process.Pid
	deadline := time.Now().Add(5 * time.Second)
	for {
		stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		if err == nil && strings.Contains(string(stat), ") Z ") {
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("child %d did not become a zombie within 5s: %q, %v", pid, stat, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// In a container whose init does not reap, an exited mount can stay a zombie for
// good, and kill(pid, 0) still finds it. Its host must still count it exited, so
// a remount there takes its own slot back and replays its journal.
func TestZombieMountCountsAsExited(t *testing.T) {
	running := exec.Command("sleep", "30")
	if err := running.Start(); err != nil {
		t.Skipf("cannot start sleep: %v", err)
	}
	t.Cleanup(func() { _ = running.Process.Kill(); _ = running.Wait() })
	zombie := zombieChild(t)

	for pid, gone := range map[int]bool{running.Process.Pid: false, zombie: true, exitedPID(t): true} {
		if got := processGone(pid); got != gone {
			t.Errorf("processGone(%d) = %v, want %v", pid, got, gone)
		}
	}

	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0)
	if _, err := reg.Register(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	slot := &Slot{State: NodeStateActive, BootID: reg.bootID, PIDNamespace: reg.pidNS, PID: uint32(zombie)}
	copy(slot.Hostname[:], reg.hostname)
	if !reg.deadLocalOwner(slot) {
		t.Errorf("a slot of this host whose mount is a zombie does not count as its exited mount")
	}
	slot.PID = uint32(running.Process.Pid)
	if reg.deadLocalOwner(slot) {
		t.Errorf("a slot of this host whose mount runs counts as exited")
	}
}
