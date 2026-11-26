package goclusterfs

import (
	"log"
	"os"
	"time"
)

// For tests only. GOCLUSTERFS_TEST_QUOTA_FLUSH_HOLD holds a quota flush once its commit is in the journal and before
// it writes the table, so a VM test can power-cut the node in that window.
var testFlushHold, _ = time.ParseDuration(os.Getenv("GOCLUSTERFS_TEST_QUOTA_FLUSH_HOLD"))

func holdFlush(projects int) {
	if testFlushHold > 0 {
		log.Printf("[TEST] quota: the flush of %d projects is committed; holding %v before writing the table", projects, testFlushHold)
		time.Sleep(testFlushHold)
	}
}
