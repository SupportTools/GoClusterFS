package goclusterfs

import (
	"log"
	"runtime"
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
