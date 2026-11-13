package goclusterfs

import (
	"bytes"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// logBuffer is a log destination a test can read while other goroutines log.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLog(t *testing.T) *logBuffer {
	t.Helper()
	buf := &logBuffer{}
	log.SetOutput(buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return buf
}

func TestOnlySlowShutdownStepsAreLogged(t *testing.T) {
	defer func(d time.Duration) { slowShutdownStep = d }(slowShutdownStep)
	slowShutdownStep = 20 * time.Millisecond
	logs := captureLog(t)

	ShutdownStep("quick step", func() {})
	ShutdownStep("stalled step", func() { time.Sleep(60 * time.Millisecond) })

	if got := logs.String(); !strings.Contains(got, "shutdown: stalled step took") || strings.Contains(got, "quick step") {
		t.Errorf("want only the stalled step logged, got:\n%s", got)
	}
}

func TestStalledShutdownLogsGoroutines(t *testing.T) {
	logs := captureLog(t)

	stop := WatchShutdown(20 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	stop()
	WatchShutdown(time.Hour)()

	got := logs.String()
	if strings.Count(got, "shutdown still running") != 1 || !strings.Contains(got, "goroutine ") {
		t.Errorf("want one goroutine dump from the stalled shutdown, got:\n%.400s", got)
	}
}

// An unmount that fails ends the watch; the next attempt starts a new one.
func TestEndedShutdownWatchDoesNotDump(t *testing.T) {
	logs := captureLog(t)
	watch := &ShutdownWatch{Limit: 30 * time.Millisecond}

	watch.Start()
	watch.End()
	time.Sleep(90 * time.Millisecond)
	if strings.Contains(logs.String(), "shutdown still running") {
		t.Fatalf("an ended watch dumped goroutines:\n%.400s", logs.String())
	}

	watch.Start()
	watch.Start()
	time.Sleep(90 * time.Millisecond)
	watch.End()
	if got := strings.Count(logs.String(), "shutdown still running"); got != 1 {
		t.Errorf("%d dumps from the second shutdown, want 1", got)
	}
}
