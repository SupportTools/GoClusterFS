package fsck

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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

var zeroSector [BitmapSectorSize]byte

// checkBitmap validates the block allocation bitmap (Phase 2).
//
// Checks performed:
//   - Sectors covering TotalBlocks have valid CRC32C checksums or are all zero (never written)
//   - Bitmap size matches TotalBlocks from superblock
//   - Track which blocks are allocated (for cross-validation)
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
	sectorBuf := make([]byte, BitmapSectorSize)
	allocatedCount := uint64(0)
	checksumErrors := 0

	for sectorIdx := uint64(0); sectorIdx < min(actualSectors, expectedSectors); sectorIdx++ {
		sectorOffset := bitmapOffset + sectorIdx*BitmapSectorSize

		n, err := c.dev.ReadAt(sectorBuf, int64(sectorOffset))
		if err != nil {
			c.addProblem(PhaseBitmap, Critical, fmt.Sprintf("sector %d", sectorIdx),
				fmt.Sprintf("read error: %v", err),
				false)
			continue
		}
		if n != BitmapSectorSize {
			c.addProblem(PhaseBitmap, Critical, fmt.Sprintf("sector %d", sectorIdx),
				fmt.Sprintf("short read: %d bytes", n),
				false)
			continue
		}

		// Validate checksum
		storedChecksum := binary.LittleEndian.Uint32(sectorBuf[BitmapBytesPerSector:])
		computedChecksum := crc32.Checksum(sectorBuf[:BitmapBytesPerSector], crc32.MakeTable(crc32.Castagnoli))

		if storedChecksum != computedChecksum && !bytes.Equal(sectorBuf, zeroSector[:]) {
			checksumErrors++
			c.addProblem(PhaseBitmap, Error, fmt.Sprintf("sector %d", sectorIdx),
				fmt.Sprintf("checksum mismatch: stored=0x%08X, computed=0x%08X",
					storedChecksum, computedChecksum),
				true)

			if c.opts.Mode == Repair {
				// Repair by recalculating checksum
				binary.LittleEndian.PutUint32(sectorBuf[BitmapBytesPerSector:], computedChecksum)
				if _, err := c.dev.WriteAt(sectorBuf, int64(sectorOffset)); err != nil {
					c.logf("  Failed to repair sector %d: %v\n", sectorIdx, err)
				} else {
					c.result.Problems[len(c.result.Problems)-1].Repaired = true
					c.result.RepairedCount++
				}
			}
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
	freeBlocks := totalBlocks - allocatedCount
	c.result.FreeBlocks = freeBlocks

	c.logf("  Allocated blocks: %d\n", allocatedCount)
	c.logf("  Free blocks: %d\n", freeBlocks)
	c.logf("  Checksum errors: %d\n", checksumErrors)

	// Compare with superblock's FreeBlocks counter
	if freeBlocks != sb.FreeBlocks {
		c.addProblem(PhaseBitmap, Error, "free count",
			fmt.Sprintf("superblock says %d free blocks, bitmap shows %d",
				sb.FreeBlocks, freeBlocks),
			true)
		// This is fixable by updating superblock counter
	}

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
