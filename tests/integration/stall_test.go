package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// stallAfter is how long a call on the mount may take before watchStall records what every thread waits on.
const stallAfter = 30 * time.Second

// watchStall records goroutines, kernel stacks and FUSE queues if stop isn't called in time, then aborts the connection.
func (m *apiMount) watchStall(t *testing.T, what string) (stop func()) {
	timer := time.AfterFunc(stallAfter, func() { m.captureStall(t, what) })
	return func() { timer.Stop() }
}

var stallOnce sync.Map

func (m *apiMount) captureStall(t *testing.T, what string) {
	if _, seen := stallOnce.LoadOrStore(m, true); seen {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "stall: %s has waited %v\n", what, stallAfter)
	pid := m.PID()
	if pid > 0 {
		_ = syscall.Kill(pid, syscall.SIGUSR1)
	}
	for _, p := range []int{pid, os.Getpid()} {
		tasks, _ := filepath.Glob(fmt.Sprintf("/proc/%d/task/*", p))
		fmt.Fprintf(&b, "== kernel stacks of pid %d\n", p)
		for _, task := range tasks {
			comm, _ := os.ReadFile(task + "/comm")
			wchan, _ := os.ReadFile(task + "/wchan")
			stack, err := os.ReadFile(task + "/stack")
			if err != nil {
				stack = []byte(err.Error() + "\n")
			}
			fmt.Fprintf(&b, "-- %s %s wchan=%s\n%s", filepath.Base(task), strings.TrimSpace(string(comm)), wchan, stack)
		}
	}
	var st unix.Stat_t
	conn := ""
	if unix.Stat(m.MountPoint, &st) == nil {
		conn = fmt.Sprintf("/sys/fs/fuse/connections/%d", unix.Minor(st.Dev))
		waiting, _ := os.ReadFile(conn + "/waiting")
		fmt.Fprintf(&b, "== fuse connection %s waiting=%s\n", conn, waiting)
	}
	time.Sleep(2 * time.Second)
	fmt.Fprintf(&b, "== mount log\n%s", m.log.String())

	record := b.String()
	if dir := os.Getenv("GCFS_STALL_DIR"); dir != "" {
		name := filepath.Join(dir, fmt.Sprintf("stall-%s-%d.txt", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().Unix()))
		if err := os.WriteFile(name, []byte(record), 0o644); err != nil {
			t.Logf("saving the stall record: %v", err)
		}
	}
	t.Errorf("%s", record)
	if conn != "" {
		if err := os.WriteFile(conn+"/abort", []byte("1"), 0); err != nil {
			t.Logf("aborting %s: %v", conn, err)
		}
	}
}
