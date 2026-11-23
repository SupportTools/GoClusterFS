package bitmap

import (
	"context"
	"errors"
	"testing"
)

func TestAllocRunsTakesAlignedFreeRuns(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	ctx := context.Background()
	r := NewRegion(dev, 0, 64, nil, [16]byte{1})
	for _, b := range []uint64{3, 12, 21, 30} {
		if err := r.MarkAllocated(ctx, b); err != nil {
			t.Fatal(err)
		}
	}

	starts, err := r.AllocRuns(ctx, 3, 8)
	if err != nil {
		t.Fatal(err)
	}
	if want := []uint64{32, 40, 48}; len(starts) != 3 || starts[0] != want[0] || starts[1] != want[1] || starts[2] != want[2] {
		t.Fatalf("runs start at %v, want %v: blocks 0-31 each hold an allocated block", starts, want)
	}
	for _, s := range starts {
		for b := s; b < s+8; b++ {
			if ok, err := r.IsAllocated(b); err != nil || !ok {
				t.Fatalf("block %d of the run at %d is not allocated: %v", b, s, err)
			}
		}
	}

	if _, err := r.AllocRuns(ctx, 2, 8); !errors.Is(err, ErrNoFreeBlocks) {
		t.Fatalf("second request: err = %v, want ErrNoFreeBlocks, since only blocks 56-63 form a free run", err)
	}
	if got := r.AllocatedCount(); got != 4+24 {
		t.Errorf("%d blocks allocated after the failed request, want 28: it must roll back its run", got)
	}
	if _, err := r.AllocRuns(ctx, 1, 3); err == nil {
		t.Error("a run length of 3 was accepted")
	}
}

// A run never reaches past the last block, even when the block count is not a
// multiple of the run length.
func TestAllocRunsStaysInsideTheBitmap(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	region := NewRegion(dev, 0, 13, nil, [16]byte{})
	starts, err := region.AllocRuns(context.Background(), 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range starts {
		if s+4 > 13 {
			t.Fatalf("AllocRuns returned a run of 4 at block %d, past block 12", s)
		}
	}
	if _, err := region.AllocRuns(context.Background(), 1, 4); !errors.Is(err, ErrNoFreeBlocks) {
		t.Fatalf("a fourth run of 4 in 13 blocks: %v, want ErrNoFreeBlocks", err)
	}
	if free := region.FreeCount(); free != 1 {
		t.Fatalf("%d blocks free after the failed run, want block 12 alone", free)
	}
}
