package lock

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLongHoldIsLoggedOnce(t *testing.T) {
	old := longHold
	t.Cleanup(func() { longHold = old })
	longHold = 120 * time.Millisecond
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	holder := newTestNode(dev, 0, 1<<20, 1)

	var logs bytes.Buffer
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	holder.Start()
	requireAcquired(t, holder, LockTypeInode, 2, LockModeExclusive)
	time.Sleep(5 * longHold)
	holder.Stop()
	log.SetOutput(os.Stderr)

	warning := fmt.Sprintf("held record %d for", holder.hashToRecord(LockTypeInode, 2))
	if got := strings.Count(logs.String(), warning); got != 1 {
		t.Fatalf("%d warnings about the long hold, want 1:\n%s", got, logs.String())
	}
	if !strings.Contains(logs.String(), "type=8 res=2 mode=2") {
		t.Errorf("warning does not name the held key:\n%s", logs.String())
	}
}
