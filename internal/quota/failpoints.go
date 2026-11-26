package quota

import (
	"log"
	"os"
	"time"
)

// For tests only. GOCLUSTERFS_TEST_QUOTA_FOLD_HOLD holds a fold once it is in the journal and before it writes the
// usage, so a VM test can power-cut the replayer in that window.
var testFoldHold, _ = time.ParseDuration(os.Getenv("GOCLUSTERFS_TEST_QUOTA_FOLD_HOLD"))

func holdFold(slot int) {
	if testFoldHold > 0 {
		log.Printf("[TEST] quota: the fold of slot %d is in its journal; holding %v before writing the usage", slot, testFoldHold)
		time.Sleep(testFoldHold)
	}
}
