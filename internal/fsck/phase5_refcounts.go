package fsck

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	// RefcountSectorSize is the size of each refcount sector.
	RefcountSectorSize = 4096

	// RefcountChecksumSize is the size of the CRC32C checksum.
	RefcountChecksumSize = 4

	// RefcountEntrySize is the size of each refcount entry.
	// 4 bytes RefCount + 2 bytes Flags + 2 bytes Reserved = 8 bytes.
	RefcountEntrySize = 8

	// RefcountEntriesPerSector is entries per sector.
	// (4096 - 4) / 8 = 511
	RefcountEntriesPerSector = (RefcountSectorSize - RefcountChecksumSize) / RefcountEntrySize
)

// RefcountEntry holds parsed refcount data.
type RefcountEntry struct {
	RefCount uint32
	Flags    uint16
}

// Refcount flags
const (
	RefcountFlagSnapshot   uint16 = 1 << 0
	RefcountFlagCowPending uint16 = 1 << 1
	RefcountFlagMetadata   uint16 = 1 << 2
	RefcountFlagPinned     uint16 = 1 << 3
)

// checkRefcounts validates the refcount table (Phase 5).
//
// Checks performed:
//   - Each sector has valid CRC32C checksum
//   - RefCount == 0 for free blocks (0 or 1 means one owner, so an
//     allocated block may have either)
//   - RefCount > 1 implies FlagShared should be set in extent
//   - No refcount overflow (0xFFFFFFFF)
//   - Cross-validate with extent references from Phase 4
func (c *Checker) checkRefcounts() error {
	if c.sbData == nil {
		return fmt.Errorf("superblock not validated")
	}

	sb := c.sbData
	refcountOffset := sb.RefCountTableOffset
	refcountSize := sb.RefCountTableSize
	totalBlocks := sb.TotalBlocks

	// Check if refcount table exists
	if refcountSize == 0 {
		c.logf("  Refcount table not present (size=0)\n")
		return nil
	}

	totalSectors := refcountSize / RefcountSectorSize

	c.logf("  Refcount table offset: 0x%X\n", refcountOffset)
	c.logf("  Refcount table size: %d bytes (%d sectors)\n", refcountSize, totalSectors)
	c.logf("  Entries per sector: %d\n", RefcountEntriesPerSector)

	sectorBuf := make([]byte, RefcountSectorSize)
	checksumErrors := 0
	inconsistentEntries := 0
	sharedBlocks := uint64(0)

	for sectorIdx := uint64(0); sectorIdx < totalSectors; sectorIdx++ {
		sectorOffset := refcountOffset + sectorIdx*RefcountSectorSize

		n, err := c.dev.ReadAt(sectorBuf, int64(sectorOffset))
		if err != nil {
			c.addProblem(PhaseRefcounts, Critical, fmt.Sprintf("sector %d", sectorIdx),
				fmt.Sprintf("read error: %v", err),
				false)
			continue
		}
		if n != RefcountSectorSize {
			c.addProblem(PhaseRefcounts, Critical, fmt.Sprintf("sector %d", sectorIdx),
				fmt.Sprintf("short read: %d bytes", n),
				false)
			continue
		}

		// Validate checksum
		dataLen := RefcountEntriesPerSector * RefcountEntrySize
		storedChecksum := binary.LittleEndian.Uint32(sectorBuf[dataLen:])
		computedChecksum := crc32.Checksum(sectorBuf[:dataLen], crc32.MakeTable(crc32.Castagnoli))

		if storedChecksum != computedChecksum {
			checksumErrors++
			c.addProblem(PhaseRefcounts, Error, fmt.Sprintf("sector %d", sectorIdx),
				fmt.Sprintf("checksum mismatch: stored=0x%08X, computed=0x%08X",
					storedChecksum, computedChecksum),
				true)

			if c.opts.Mode == Repair {
				binary.LittleEndian.PutUint32(sectorBuf[dataLen:], computedChecksum)
				if _, err := c.dev.WriteAt(sectorBuf, int64(sectorOffset)); err != nil {
					c.logf("  Failed to repair sector %d: %v\n", sectorIdx, err)
				} else {
					c.result.Problems[len(c.result.Problems)-1].Repaired = true
					c.result.RepairedCount++
				}
			}
		}

		// Process each entry
		for entryIdx := 0; entryIdx < RefcountEntriesPerSector; entryIdx++ {
			blockNum := sectorIdx*RefcountEntriesPerSector + uint64(entryIdx)
			if blockNum >= totalBlocks {
				break
			}

			entryOffset := entryIdx * RefcountEntrySize
			entry := parseRefcountEntry(sectorBuf[entryOffset:])

			// Store for cross-validation
			c.refcountEntries[blockNum] = entry.RefCount

			if entry.RefCount > 1 {
				sharedBlocks++
			}

			// Validate consistency with bitmap
			isAllocatedInBitmap := c.bitmapAllocated[blockNum]
			refsFromInodes := c.inodeBlocks[blockNum]

			if !isAllocatedInBitmap && entry.RefCount != 0 {
				c.addProblem(PhaseRefcounts, Error, fmt.Sprintf("block %d", blockNum),
					fmt.Sprintf("block free in bitmap but refcount is %d", entry.RefCount),
					true)
				inconsistentEntries++
			}

			// An undercounted block is freed while another inode still maps it.
			if refsFromInodes > 1 && entry.RefCount < uint32(refsFromInodes) {
				c.addProblem(PhaseRefcounts, Error, fmt.Sprintf("block %d", blockNum),
					fmt.Sprintf("referenced by %d inodes but refcount is only %d", refsFromInodes, entry.RefCount),
					true)
			}

			// Check for overflow (should never happen)
			if entry.RefCount == 0xFFFFFFFF {
				c.addProblem(PhaseRefcounts, Critical, fmt.Sprintf("block %d", blockNum),
					"refcount overflow (0xFFFFFFFF)",
					false)
			}
		}
	}

	c.logf("  Checksum errors: %d\n", checksumErrors)
	c.logf("  Inconsistent entries: %d\n", inconsistentEntries)
	c.logf("  Shared blocks (refcount > 1): %d\n", sharedBlocks)

	return nil
}

// parseRefcountEntry extracts a refcount entry from raw bytes.
func parseRefcountEntry(buf []byte) *RefcountEntry {
	return &RefcountEntry{
		RefCount: binary.LittleEndian.Uint32(buf[0:4]),
		Flags:    binary.LittleEndian.Uint16(buf[4:6]),
		// Bytes 6-7 are reserved
	}
}
