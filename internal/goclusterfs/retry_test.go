package goclusterfs

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// The retry loop runs a lost inode slot again as often as it takes, and stops at any other answer.
func TestUntilClaimedRetriesEveryLostSlot(t *testing.T) {
	tries := 0
	errno := untilClaimed("Create", "f", func() syscall.Errno {
		if tries++; tries <= 2*MaxCreateRetries {
			return syscall.EAGAIN
		}
		return 0
	})
	if errno != 0 || tries != 2*MaxCreateRetries+1 {
		t.Errorf("after %d lost slots: %v in %d tries, want success on the next", 2*MaxCreateRetries, errno, tries)
	}

	tries = 0
	if errno := untilClaimed("Create", "f", func() syscall.Errno { tries++; return syscall.ENOSPC }); errno != syscall.ENOSPC || tries != 1 {
		t.Errorf("ENOSPC: %v in %d tries, want ENOSPC at once", errno, tries)
	}
}

// Another node takes the free inode a create, mkdir or symlink picked more times in a row than retryRace retries; each
// still succeeds.
func TestCreatesOutlastManyLostInodes(t *testing.T) {
	const lose = MaxCreateRetries + 2
	ctx := context.Background()
	for _, tc := range []struct {
		op string
		do func(fsys *FS) syscall.Errno
	}{
		{"create", func(fsys *FS) syscall.Errno {
			_, _, _, errno := fsys.Create(ctx, "x", syscall.O_EXCL, 0o644, &fuse.EntryOut{})
			return errno
		}},
		{"mkdir", func(fsys *FS) syscall.Errno {
			_, errno := fsys.Mkdir(ctx, "x", 0o755, &fuse.EntryOut{})
			return errno
		}},
		{"symlink", func(fsys *FS) syscall.Errno {
			_, errno := fsys.Symlink(ctx, "target", "x", &fuse.EntryOut{})
			return errno
		}},
	} {
		t.Run(tc.op, func(t *testing.T) {
			path := newFormattedImage(t)
			fsys := withBridge(openFormattedFS(t, path))
			peer := openFormattedComponents(t, path)
			var taken []uint32
			claiming = func(op string, num uint32) {
				if op != tc.op || len(taken) == lose {
					return
				}
				if _, err := peer.inodeTbl.ClaimSlot(ctx, num); err != nil {
					t.Errorf("the peer taking inode %d: %v", num, err)
				}
				taken = append(taken, num)
			}
			t.Cleanup(func() { claiming = nil })
			if errno := tc.do(fsys); errno != 0 {
				t.Fatalf("%s after the peer took %d of its inodes: %v", tc.op, len(taken), errno)
			}
			if len(taken) != lose {
				t.Errorf("the peer took %d inodes, want %d", len(taken), lose)
			}
		})
	}
}
