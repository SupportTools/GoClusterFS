package fsck

import (
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// BitmapSectorSize is the size of each bitmap sector.
	BitmapSectorSize = 4096

	// BitmapChecksumSize is the size of the CRC32C checksum per sector.
	BitmapChecksumSize = 4

	// BitmapBytesPerSector is the usable bytes for bitmap data per sector.
	BitmapBytesPerSector = BitmapSectorSize - BitmapChecksumSize

	// BitmapBitsPerSector is the number of bits stored per sector.
	BitmapBitsPerSector = BitmapBytesPerSector * 8 // 32736
)

// checkBitmap validates the block allocation bitmap (Phase 2).
//
// Checks performed:
//   - Sectors covering TotalBlocks have valid CRC32C checksums or are all zero (never written)
//   - Bitmap size matches TotalBlocks from superblock
//   - Track which blocks are allocated (for cross-validation)
//
// The superblock's free block counter is set at format and never updated by
// the runtime, so it is not compared.
func (c *Checker) checkBitmap() error {
	if c.sbData == nil {
		return fmt.Errorf("superblock not validated")
	}

	sb := c.sbData
	totalBlocks := sb.TotalBlocks
	bitmapOffset := sb.BlockBitmapOffset
	bitmapSize := sb.BlockBitmapSize

	// Calculate expected number of sectors
	expectedSectors := (totalBlocks + BitmapBitsPerSector - 1) / BitmapBitsPerSector
	actualSectors := bitmapSize / BitmapSectorSize

	if actualSectors < expectedSectors {
		c.addProblem(PhaseBitmap, Error, "bitmap size",
			fmt.Sprintf("bitmap too small: %d sectors, need %d for %d blocks",
				actualSectors, expectedSectors, totalBlocks),
			false)
	}

	c.logf("  Bitmap offset: 0x%X\n", bitmapOffset)
	c.logf("  Bitmap size: %d bytes (%d sectors)\n", bitmapSize, actualSectors)
	c.logf("  Total blocks tracked: %d\n", totalBlocks)

	// Read and validate each sector
	sectorBuf := storage.NewAlignedBuffer(BitmapSectorSize).Bytes()
	allocatedCount := uint64(0)
	checksumErrors := 0

	for sectorIdx := uint64(0); sectorIdx < min(actualSectors, expectedSectors); sectorIdx++ {
		if err := c.readSector(sectorBuf, bitmapOffset+sectorIdx*BitmapSectorSize); err != nil {
			c.addProblem(PhaseBitmap, Critical, fmt.Sprintf("sector %d", sectorIdx), err.Error(), false)
			continue
		}

		// The runtime refuses to read a sector with a bad checksum. Rewriting
		// the checksum would make it trust bits that may be wrong.
		if !sectorChecksumValid(sectorBuf) {
			checksumErrors++
			c.addProblem(PhaseBitmap, Error, fmt.Sprintf("sector %d", sectorIdx),
				"checksum mismatch: the runtime cannot allocate or free blocks in this sector", false)
		}

		// Count allocated blocks and record for cross-validation
		for byteIdx := 0; byteIdx < BitmapBytesPerSector; byteIdx++ {
			b := sectorBuf[byteIdx]
			for bitIdx := 0; bitIdx < 8; bitIdx++ {
				blockNum := sectorIdx*BitmapBitsPerSector + uint64(byteIdx*8+bitIdx)
				if blockNum >= totalBlocks {
					break // Past the valid block range
				}

				isAllocated := (b & (1 << bitIdx)) != 0
				if isAllocated {
					allocatedCount++
					c.bitmapAllocated[blockNum] = true
				}
			}
		}
	}

	// Update result statistics
	c.result.AllocatedBlocks = allocatedCount
	c.result.FreeBlocks = totalBlocks - allocatedCount

	c.logf("  Allocated blocks: %d\n", allocatedCount)
	c.logf("  Free blocks: %d\n", c.result.FreeBlocks)
	c.logf("  Checksum errors: %d\n", checksumErrors)

	return nil
}

// GetAllocatedBlocks returns a copy of the allocated block set.
// Used for cross-validation with other phases.
func (c *Checker) GetAllocatedBlocks() map[uint64]bool {
	result := make(map[uint64]bool, len(c.bitmapAllocated))
	for k, v := range c.bitmapAllocated {
		result[k] = v
	}
	return result
}
