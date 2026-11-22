package superblock

import (
	"errors"
	"testing"
)

func TestFormatAtEachBlockSize(t *testing.T) {
	const size = 1 << 30
	for _, tc := range []struct {
		blockSize, bytesPerInode uint64
	}{
		{MinBlockSize, 16 << 10},
		{64 << 10, 128 << 10},
		{MaxBlockSize, 1 << 20},
	} {
		dev := testDevice(t, size)
		if _, err := Format(dev, tc.blockSize, 0); err != nil {
			t.Fatalf("Format at %d: %v", tc.blockSize, err)
		}
		sb, err := Read(dev)
		if err != nil {
			t.Fatalf("Read at %d: %v", tc.blockSize, err)
		}
		if uint64(sb.BlockSize) != tc.blockSize {
			t.Errorf("block size %d, want %d", sb.BlockSize, tc.blockSize)
		}
		if want := max(uint64(size)/tc.bytesPerInode, MinInodes); sb.InodeCount != want {
			t.Errorf("%d-byte blocks: %d inodes, want one per %d bytes, %d", tc.blockSize, sb.InodeCount, tc.bytesPerInode, want)
		}
		if want := (uint64(size) - sb.DataRegionOffset) / tc.blockSize; sb.TotalBlocks != want {
			t.Errorf("%d-byte blocks: %d data blocks, want %d", tc.blockSize, sb.TotalBlocks, want)
		}
		if sb.RefCountTableSize < RefCountTableBytes(sb.TotalBlocks) || sb.BlockBitmapSize < BlockBitmapBytes(sb.TotalBlocks) {
			t.Errorf("%d-byte blocks: bitmap or refcount region too small for %d blocks", tc.blockSize, sb.TotalBlocks)
		}
	}
}

func TestFormatRefusesOtherBlockSizes(t *testing.T) {
	for _, bs := range []uint64{4096, 12 << 10, 2 << 20} {
		if _, err := Format(testDevice(t, MinDeviceSize), bs, 0); !errors.Is(err, ErrInvalidBlockSize) {
			t.Errorf("Format at %d: err = %v, want ErrInvalidBlockSize", bs, err)
		}
	}
	dev := testDevice(t, MinDeviceSize)
	sb := New(dev.Size(), 0, 0)
	sb.BlockSize = 4096
	if err := sb.CheckLayout(dev.Size()); !errors.Is(err, ErrUnsupportedLayout) {
		t.Errorf("CheckLayout with 4 KB blocks: err = %v, want ErrUnsupportedLayout", err)
	}
}
