package goclusterfs

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return &buf
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
