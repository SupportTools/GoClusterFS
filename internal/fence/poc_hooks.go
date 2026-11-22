package fence

import (
	"log"
	"os"
	"strings"
	"time"
)

// pocRekeyStall is GOCLUSTERFS_POC_REKEY_STALL, "before-register:3s" or "after-register:3s": Rekey sleeps there with
// writes held, so a VM test can pause the guest at that point. Proof of concept only.
var pocRekeyStall = os.Getenv("GOCLUSTERFS_POC_REKEY_STALL")

func pocStall(at string) {
	where, d, ok := strings.Cut(pocRekeyStall, ":")
	if !ok || where != at {
		return
	}
	if stall, err := time.ParseDuration(d); err == nil {
		log.Printf("[POC] rekey: stalling %v %s", stall, at)
		time.Sleep(stall)
		log.Printf("[POC] rekey: stall %s over", at)
	}
}
