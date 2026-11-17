package integration

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// A test thread waiting on a FUSE request the daemon has already read cannot
// be killed, so a hung mount keeps the test binary alive past go test's
// -timeout, and with it the host lock. The watchdog acts first: it saves the
// daemon's goroutines and log, aborts the mount's FUSE connection, which fails
// every waiting request, and fails the test.
const (
	hangMargin = 30 * time.Second
	probeEvery = 10 * time.Second
	// Longer than any test stops a daemon or waits for a stopped node's locks.
	noAnswerLimit = 3 * time.Minute
	dumpWait      = 10 * time.Second
	logLines      = 200
	tailKeep      = 4 << 20

	fuseConnections = "/sys/fs/fuse/connections/"
)

type watchdog struct {
	m          *MountedFS
	daemon     *os.Process
	mountPoint string
	connID     string
	stop       chan struct{}
	done       chan struct{}
}

func (m *MountedFS) startWatchdog() {
	m.stopWatchdog()
	w := &watchdog{
		m:          m,
		daemon:     m.MountCmd.Process,
		mountPoint: resolveParent(m.MountPoint),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	id, err := fuseConnID(w.mountPoint)
	if err != nil {
		m.t.Logf("watchdog: %v; a hang on this mount will not be aborted", err)
	}
	w.connID = id
	m.watchdog = w
	go w.run()
}

func (m *MountedFS) stopWatchdog() {
	if m.watchdog == nil {
		return
	}
	close(m.watchdog.stop)
	<-m.watchdog.done
	m.watchdog = nil
}

func (w *watchdog) run() {
	defer close(w.done)
	var overrun <-chan time.Time
	if deadline, ok := w.m.t.Deadline(); ok {
		timer := time.NewTimer(time.Until(deadline) - hangMargin)
		defer timer.Stop()
		overrun = timer.C
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var answered chan struct{}
	asked := time.Now()
	for {
		select {
		case <-w.stop:
			return
		case <-overrun:
			w.fire(fmt.Sprintf("the test was still running %v before go test's -timeout", hangMargin))
			return
		case <-tick.C:
		}
		switch {
		case answered == nil:
			if time.Since(asked) >= probeEvery {
				asked, answered = time.Now(), probeStatfs(w.mountPoint)
			}
		case closed(answered):
			answered = nil
		case time.Since(asked) > noAnswerLimit && !stopped(w.daemon.Pid):
			w.fire(fmt.Sprintf("the daemon did not answer statfs for %v", noAnswerLimit))
			return
		}
	}
}

func probeStatfs(path string) chan struct{} {
	answered := make(chan struct{})
	go func() {
		var st syscall.Statfs_t
		_ = syscall.Statfs(path, &st)
		close(answered)
	}()
	return answered
}

func closed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

func stopped(pid int) bool {
	state := procState(fmt.Sprintf("/proc/%d/stat", pid))
	return state == 'T' || state == 't'
}

func (w *watchdog) fire(reason string) {
	m := w.m
	var r strings.Builder
	fmt.Fprintf(&r, "%s: %s\nmount %s, daemon pid %d, FUSE connection %q\n", m.t.Name(), reason, m.MountPoint, w.daemon.Pid, w.connID)

	at := m.tail.offset()
	if err := w.daemon.Signal(syscall.SIGUSR1); err != nil {
		fmt.Fprintf(&r, "SIGUSR1: %v\n", err)
	}
	before, dump := m.tail.split(at)
	for wait := time.Now().Add(dumpWait); !bytes.Contains(dump, []byte("goroutine dump ends")) && time.Now().Before(wait); {
		time.Sleep(100 * time.Millisecond)
		before, dump = m.tail.split(at)
	}

	fmt.Fprintf(&r, "\n== daemon threads\n%s", threads(strconv.Itoa(w.daemon.Pid), false))
	fmt.Fprintf(&r, "\n== test threads waiting on FUSE or in state D\n%s", threads("self", true))
	if waiting, err := os.ReadFile(fuseConnections + w.connID + "/waiting"); err == nil && w.connID != "" {
		fmt.Fprintf(&r, "\n== FUSE requests waiting: %s", waiting)
	}
	kernel := r.String()
	fmt.Fprintf(&r, "\n== last %d daemon log lines\n%s", logLines, lastLines(before, logLines))
	fmt.Fprintf(&r, "\n== daemon output after SIGUSR1\n%s", dump)

	saved := "the report could not be saved"
	if path, err := saveHangReport(m.t.Name(), w.daemon.Pid, r.String()); err != nil {
		saved += ": " + err.Error()
	} else {
		saved = "goroutines and log saved to " + path
	}
	m.t.Log(kernel)
	m.t.Errorf("watchdog: %s; %s; %s", reason, w.abort(), saved)
}

func (w *watchdog) abort() string {
	if w.connID == "" {
		return "no FUSE connection to abort"
	}
	id, err := fuseConnID(w.mountPoint)
	if err != nil || id != w.connID {
		return fmt.Sprintf("left FUSE connection %s alone because %s no longer mounts it (%q, %v)", w.connID, w.mountPoint, id, err)
	}
	f, err := os.OpenFile(fuseConnections+id+"/abort", os.O_WRONLY, 0)
	if err == nil {
		_, err = f.WriteString("1")
		err = errors.Join(err, f.Close())
	}
	if err != nil {
		return fmt.Sprintf("could not abort FUSE connection %s: %v", id, err)
	}
	return "aborted FUSE connection " + id
}

func threads(pid string, onlyOnFUSE bool) string {
	tasks, _ := filepath.Glob("/proc/" + pid + "/task/*")
	var b strings.Builder
	for _, task := range tasks {
		state := procState(task + "/stat")
		wchan, _ := os.ReadFile(task + "/wchan")
		if onlyOnFUSE && state != 'D' && !bytes.Contains(wchan, []byte("fuse")) && !bytes.Contains(wchan, []byte("request_wait_answer")) {
			continue
		}
		comm, _ := os.ReadFile(task + "/comm")
		fmt.Fprintf(&b, "-- thread %s %s state %c wchan %s\n", filepath.Base(task), bytes.TrimSpace(comm), state, wchan)
		if stack, err := os.ReadFile(task + "/stack"); err == nil {
			b.Write(stack)
		}
	}
	if b.Len() == 0 {
		return "none\n"
	}
	return b.String()
}

// procState returns the state letter from a /proc stat file, or 0 if it cannot be read.
func procState(statPath string) byte {
	stat, err := os.ReadFile(statPath)
	if err != nil {
		return 0
	}
	_, after, ok := strings.Cut(string(stat), ") ")
	if !ok || after == "" {
		return '?'
	}
	return after[0]
}

func lastLines(text []byte, n int) []byte {
	lines := bytes.SplitAfter(text, []byte("\n"))
	return bytes.Join(lines[max(len(lines)-n, 0):], nil)
}

func saveHangReport(test string, pid int, report string) (string, error) {
	dir := os.Getenv("GOCLUSTERFS_HANG_DIR")
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "goclusterfs-hangs")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	name := fmt.Sprintf("%s-%d-%s.txt", strings.ReplaceAll(test, "/", "_"), pid, time.Now().Format("20060102-150405"))
	path := filepath.Join(dir, name)
	return path, os.WriteFile(path, []byte(report), 0o644)
}

// resolveParent resolves symlinks in path's parent, as mountinfo shows them,
// without touching the mount itself.
func resolveParent(path string) string {
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return path
	}
	return filepath.Join(parent, filepath.Base(path))
}

func fuseConnID(mountPoint string) (string, error) {
	mountinfo, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	return connIDFromMountinfo(string(mountinfo), mountPoint)
}

var mountinfoEscapes = strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)

// connIDFromMountinfo returns the FUSE connection of the top mount at
// mountPoint. Its directory under /sys/fs/fuse/connections is named after
// the mount's device number.
func connIDFromMountinfo(mountinfo, mountPoint string) (string, error) {
	id := ""
	for line := range strings.Lines(mountinfo) {
		mount, fs, ok := strings.Cut(line, " - ")
		fields := strings.Fields(mount)
		if !ok || len(fields) < 5 || mountinfoEscapes.Replace(fields[4]) != mountPoint {
			continue
		}
		id = ""
		fstype, _, _ := strings.Cut(fs, " ")
		var major, minor uint64
		if fstype != "fuse" && !strings.HasPrefix(fstype, "fuse.") {
			continue
		}
		if _, err := fmt.Sscanf(fields[2], "%d:%d", &major, &minor); err != nil {
			continue
		}
		id = strconv.FormatUint(major<<20|minor, 10)
	}
	if id == "" {
		return "", fmt.Errorf("no FUSE mount on top of %s in /proc/self/mountinfo", mountPoint)
	}
	return id, nil
}

// logTail keeps the end of the daemon's output for the watchdog's report.
type logTail struct {
	mu   sync.Mutex
	kept []byte
	end  int64
}

func (l *logTail) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.kept = append(l.kept, p...)
	l.end += int64(len(p))
	if len(l.kept) > 2*tailKeep {
		l.kept = slices.Clone(l.kept[len(l.kept)-tailKeep:])
	}
	return len(p), nil
}

func (l *logTail) offset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.end
}

// split returns the kept output written before and after offset at.
func (l *logTail) split(at int64) (before, after []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	start := l.end - int64(len(l.kept))
	cut := int(min(max(at-start, 0), int64(len(l.kept))))
	return slices.Clone(l.kept[:cut]), slices.Clone(l.kept[cut:])
}
