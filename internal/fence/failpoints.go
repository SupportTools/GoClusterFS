package fence

import (
	"log"
	"os"
	"strings"
	"time"
)

// testRekeyStall is GOCLUSTERFS_TEST_REKEY_STALL, "before-register:3s" or "after-register:3s": Rekey sleeps there with
// writes held, so a VM test can pause the guest at that point. For tests only.
var testRekeyStall = os.Getenv("GOCLUSTERFS_TEST_REKEY_STALL")

func failpointStall(at string) {
	where, d, ok := strings.Cut(testRekeyStall, ":")
	if !ok || where != at {
		return
	}
	if stall, err := time.ParseDuration(d); err == nil {
		log.Printf("[TEST] rekey: stalling %v %s", stall, at)
		time.Sleep(stall)
		log.Printf("[TEST] rekey: stall %s over", at)
	}
}
