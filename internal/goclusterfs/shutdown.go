package goclusterfs

import (
	"log"
	"runtime"
	"sync"
	"time"
)

// slowShutdownStep is how long a shutdown step may take before it is logged.
var slowShutdownStep = 2 * time.Second

// ShutdownStep runs one step of an unmount and logs it if it takes longer than
// slowShutdownStep, so a stalled shutdown names the step that stalled.
func ShutdownStep(name string, step func()) {
	start := time.Now()
	step()
	if took := time.Since(start); took > slowShutdownStep {
		log.Printf("[WARN] shutdown: %s took %v", name, took.Round(time.Millisecond))
	}
}

// WatchShutdown logs every goroutine's stack if the shutdown that starts now is
// still running after limit. Call the returned stop when the shutdown ends.
func WatchShutdown(limit time.Duration) (stop func()) {
	timer := time.AfterFunc(limit, func() {
		buf := make([]byte, 8<<20)
		n := runtime.Stack(buf, true)
		log.Printf("[WARN] shutdown still running after %v; goroutines:\n%s", limit, buf[:n])
	})
	return func() { timer.Stop() }
}

// ShutdownWatch runs WatchShutdown for one shutdown at a time. Start begins a
// watch unless one runs; End stops it, for example when an unmount fails and
// the mount keeps serving, so it does not report a shutdown that is not running.
type ShutdownWatch struct {
	Limit time.Duration
	mu    sync.Mutex
	stop  func()
}

func (w *ShutdownWatch) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stop == nil {
		w.stop = WatchShutdown(w.Limit)
	}
}

func (w *ShutdownWatch) End() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stop != nil {
		w.stop()
		w.stop = nil
	}
}
