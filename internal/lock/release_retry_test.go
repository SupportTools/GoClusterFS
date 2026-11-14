package lock

import (
	"sync/atomic"
	"syscall"
	"testing"
)

// A node whose write clearing its claim fails clears it again later, so other
// nodes are not kept out of the record until it unmounts.
func TestFailedReleaseWriteIsRetried(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	a := newTestNode(dev, 0, 1<<20, 1)
	b := newTestNode(dev, 0, 1<<20, 2)
	reg := newFakeRegistry()
	reg.set(1, OwnerAlive)
	reg.set(2, OwnerAlive)
	b.SetOwnerView(reg.view)
	a.Start()
	defer a.Stop()

	requireAcquired(t, a, LockTypeInode, 3, LockModeExclusive)
	var failNext atomic.Bool
	a.slotWrite = func(buf []byte, offset int64) (int, error) {
		if failNext.CompareAndSwap(true, false) {
			return 0, syscall.EIO
		}
		return a.dev.WriteAt(buf, offset)
	}
	failNext.Store(true)
	release(t, a, LockTypeInode, 3)

	if err := acquireWithin(b, 5*releaseRetry, LockTypeInode, 3, LockModeExclusive); err != nil {
		t.Fatalf("the other node is still kept out after one failed release write: %v", err)
	}
	release(t, b, LockTypeInode, 3)
}
