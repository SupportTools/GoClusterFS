package goclusterfs

import (
	"context"
	"syscall"
	"testing"
)

// A create retries a lost inode slot as often as it takes, and stops at any other answer or when its request is
// interrupted.
func TestUntilClaimedRetriesEveryLostSlot(t *testing.T) {
	ctx := context.Background()
	tries := 0
	errno := untilClaimed(ctx, "Create", "f", func() syscall.Errno {
		if tries++; tries <= 2*MaxCreateRetries {
			return syscall.EAGAIN
		}
		return 0
	})
	if errno != 0 || tries != 2*MaxCreateRetries+1 {
		t.Errorf("after %d lost slots: %v in %d tries, want success on the next", 2*MaxCreateRetries, errno, tries)
	}

	tries = 0
	if errno := untilClaimed(ctx, "Create", "f", func() syscall.Errno { tries++; return syscall.ENOSPC }); errno != syscall.ENOSPC || tries != 1 {
		t.Errorf("ENOSPC: %v in %d tries, want ENOSPC at once", errno, tries)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	tries = 0
	if errno := untilClaimed(cancelled, "Create", "f", func() syscall.Errno { tries++; return syscall.EAGAIN }); errno != syscall.EINTR || tries != 1 {
		t.Errorf("interrupted request: %v in %d tries, want EINTR after the first", errno, tries)
	}
}
