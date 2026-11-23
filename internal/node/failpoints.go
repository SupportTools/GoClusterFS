package node

import (
	"log"
	"os"
	"time"
)

// For tests only. GOCLUSTERFS_TEST_TAKEOVER_DELAY holds a takeover between its preempt and its Dead mark, and
// GOCLUSTERFS_TEST_BEAT_STALL holds a heartbeat between its slot read and its write, so a VM test can resume a paused
// node inside those windows.
var (
	testTakeoverDelay = testDuration("GOCLUSTERFS_TEST_TAKEOVER_DELAY")
	testBeatStall     = testDuration("GOCLUSTERFS_TEST_BEAT_STALL")
)

func testDuration(name string) time.Duration {
	d, _ := time.ParseDuration(os.Getenv(name))
	return d
}

func failpointSleep(d time.Duration, format string, args ...any) {
	if d > 0 {
		log.Printf("[TEST] "+format+"; holding %v", append(args, d)...)
		time.Sleep(d)
	}
}
