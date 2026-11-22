package node

import (
	"log"
	"os"
	"time"
)

// Proof of concept only. GOCLUSTERFS_POC_TAKEOVER_DELAY holds a takeover between its preempt and its Dead mark, and
// GOCLUSTERFS_POC_BEAT_STALL holds a heartbeat between its slot read and its write, so a VM test can resume a paused
// node inside those windows.
var (
	pocTakeoverDelay = pocDuration("GOCLUSTERFS_POC_TAKEOVER_DELAY")
	pocBeatStall     = pocDuration("GOCLUSTERFS_POC_BEAT_STALL")
)

func pocDuration(name string) time.Duration {
	d, _ := time.ParseDuration(os.Getenv(name))
	return d
}

func pocSleep(d time.Duration, format string, args ...any) {
	if d > 0 {
		log.Printf("[POC] "+format+"; holding %v", append(args, d)...)
		time.Sleep(d)
	}
}
