package main

import (
	"log"
	"os"
	"runtime"
	"time"
)

// collectGarbageForTests runs a garbage collection every GOCLUSTERFS_TEST_GC_EVERY, so a test can read the live heap
// from /metrics without waiting for Go's two-minute forced collection.
func collectGarbageForTests() {
	every, _ := time.ParseDuration(os.Getenv("GOCLUSTERFS_TEST_GC_EVERY"))
	if every <= 0 {
		return
	}
	log.Printf("[WARN] GOCLUSTERFS_TEST_GC_EVERY is set: a garbage collection runs every %v; use it only in tests", every)
	go func() {
		for range time.Tick(every) {
			runtime.GC()
		}
	}()
}
