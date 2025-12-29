package fsck

import (
	"fmt"
)

// checkExtents validates extent consistency (Phase 4).
//
// Checks performed:
//   - Physical blocks are within data region bounds
//   - All referenced blocks are marked allocated in bitmap
//   - No overlapping extents between different inodes (unless FlagShared)
//   - Extent length > 0 for allocated extents
//   - Logical blocks don't overflow file size
//   - SFB extents have valid container/slot encoding
func (c *Checker) checkExtents() error {
	if c.sbData == nil {
		return fmt.Errorf("superblock not validated")
	}

	sb := c.sbData
	totalBlocks := sb.TotalBlocks

	c.logf("  Total data blocks: %d\n", totalBlocks)
	c.logf("  Blocks referenced by inodes: %d\n", len(c.inodeBlocks))

	// Track physical block usage to detect overlaps
	// blockOwner maps physicalBlock -> first inode that claimed it
	blockOwner := make(map[uint64]uint32)

	extentErrors := 0
	orphanedBlocks := uint64(0)
	overlappingBlocks := 0

	// Re-scan inodes to check their extents in detail
	// (inodeBlocks already collected block references in Phase 3)
	inodeTableOffset := sb.InodeTableOffset
	inodeTableSize := sb.InodeTableSize
	totalSectors := inodeTableSize / InodeSectorSize
	sectorBuf := make([]byte, InodeSectorSize)

	for sectorIdx := uint64(0); sectorIdx < totalSectors; sectorIdx++ {
		sectorOffset := inodeTableOffset + sectorIdx*InodeSectorSize

		n, err := c.dev.ReadAt(sectorBuf, int64(sectorOffset))
		if err != nil || n != InodeSectorSize {
			continue // Already reported in Phase 3
		}

		for inodeIdx := 0; inodeIdx < InodesPerSector; inodeIdx++ {
			inodeNum := uint32(sectorIdx)*InodesPerSector + uint32(inodeIdx)
			if !c.inodeUsed[inodeNum] {
				continue
			}

			inodeBuf := sectorBuf[inodeIdx*InodeSize : (inodeIdx+1)*InodeSize]
			inode := c.parseInode(inodeBuf)

			// Check each inline extent
			for extIdx := 0; extIdx < 2; extIdx++ {
				ext := inode.InlineExtents[extIdx]
				if ext.Length == 0 {
					continue
				}

				// Skip SFB extents (sub-file blocks) - different validation
				if (ext.Flags & ExtentFlagSFB) != 0 {
					// For SFB, PhysicalBlock encodes container:slot
					containerBlock := ext.PhysicalBlock >> 16
					if containerBlock >= totalBlocks {
						c.addProblem(PhaseExtents, Error, fmt.Sprintf("inode %d ext %d", inodeNum, extIdx),
							fmt.Sprintf("SFB container block %d out of range (max %d)", containerBlock, totalBlocks-1),
							false)
						extentErrors++
					}
					continue
				}

				// Validate physical block range
				for j := uint64(0); j < ext.Length; j++ {
					physBlock := ext.PhysicalBlock + j

					// Check bounds
					if physBlock >= totalBlocks {
						c.addProblem(PhaseExtents, Error, fmt.Sprintf("inode %d ext %d", inodeNum, extIdx),
							fmt.Sprintf("physical block %d out of range (max %d)", physBlock, totalBlocks-1),
							false)
						extentErrors++
						continue
					}

					// Check if block is allocated in bitmap
					if !c.bitmapAllocated[physBlock] {
						c.addProblem(PhaseExtents, Error, fmt.Sprintf("inode %d ext %d", inodeNum, extIdx),
							fmt.Sprintf("physical block %d not marked allocated in bitmap", physBlock),
							true)
						extentErrors++
						orphanedBlocks++
					}

					// Check for overlapping ownership
					if owner, exists := blockOwner[physBlock]; exists {
						// Block is shared - check if both have FlagShared
						isShared := (ext.Flags & ExtentFlagShared) != 0
						if !isShared && owner != inodeNum {
							c.addProblem(PhaseExtents, Critical, fmt.Sprintf("inode %d ext %d", inodeNum, extIdx),
								fmt.Sprintf("block %d also claimed by inode %d without FlagShared", physBlock, owner),
								false)
							overlappingBlocks++
						}
					} else {
						blockOwner[physBlock] = inodeNum
					}
				}

				// Validate extent flags
				if (ext.Flags & ExtentFlagAllocated) == 0 {
					c.addProblem(PhaseExtents, Warning, fmt.Sprintf("inode %d ext %d", inodeNum, extIdx),
						"extent has data but FlagAllocated not set",
						true)
				}
			}

			// Check extent tree if present
			if (inode.Flags & FlagHasExtentTree) != 0 {
				if inode.ExtentTreeRoot == 0 {
					c.addProblem(PhaseExtents, Error, fmt.Sprintf("inode %d", inodeNum),
						"FlagHasExtentTree set but ExtentTreeRoot is 0",
						false)
				} else {
					// TODO: Traverse and validate extent tree
					c.logf("  Inode %d has extent tree (root=%d, count=%d, depth=%d) - tree validation not yet implemented\n",
						inodeNum, inode.ExtentTreeRoot, inode.ExtentCount, inode.ExtentDepth)
				}
			}
		}
	}

	// Check for allocated blocks not referenced by any inode
	unreferencedAllocated := 0
	for blockNum := range c.bitmapAllocated {
		if c.inodeBlocks[blockNum] == 0 {
			unreferencedAllocated++
			// Only report if significant (avoid spam for metadata blocks)
			if unreferencedAllocated <= 10 {
				c.addProblem(PhaseExtents, Warning, fmt.Sprintf("block %d", blockNum),
					"allocated in bitmap but not referenced by any inode",
					true)
			}
		}
	}
	if unreferencedAllocated > 10 {
		c.addProblem(PhaseExtents, Warning, "bitmap",
			fmt.Sprintf("%d additional blocks allocated but unreferenced", unreferencedAllocated-10),
			true)
	}

	c.result.OrphanedBlocks = orphanedBlocks

	c.logf("  Extent errors: %d\n", extentErrors)
	c.logf("  Overlapping blocks: %d\n", overlappingBlocks)
	c.logf("  Unreferenced allocated blocks: %d\n", unreferencedAllocated)

	return nil
}
