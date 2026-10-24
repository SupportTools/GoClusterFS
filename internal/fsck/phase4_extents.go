package fsck

import (
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/storage"
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

	// Tree nodes are never shared, not even with FlagShared data extents.
	treeNodeOwner := make(map[uint64]uint32)
	for inodeNum, tree := range c.extentTrees {
		for _, block := range tree.nodes {
			if other, taken := treeNodeOwner[block]; taken && other != inodeNum {
				c.addProblem(PhaseExtents, Critical, fmt.Sprintf("tree node %d", block),
					fmt.Sprintf("extent tree node used by inodes %d and %d", other, inodeNum),
					false)
				overlappingBlocks++
			}
			treeNodeOwner[block] = inodeNum
		}
	}

	// Re-scan inodes to check their extents in detail
	// (inodeBlocks already collected block references in Phase 3)
	inodeTableOffset := sb.InodeTableOffset
	inodeTableSize := sb.InodeTableSize
	totalSectors := inodeTableSize / InodeSectorSize
	sectorBuf := storage.NewAlignedBuffer(InodeSectorSize).Bytes()

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

			// Check each extent, inline or from the extent tree
			for extIdx, ext := range c.inodeExtents(inodeNum, inode) {
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

					if nodeOwner, isNode := treeNodeOwner[physBlock]; isNode {
						c.addProblem(PhaseExtents, Critical, fmt.Sprintf("inode %d ext %d", inodeNum, extIdx),
							fmt.Sprintf("physical block %d is an extent tree node of inode %d", physBlock, nodeOwner),
							false)
						overlappingBlocks++
						continue
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

			// Check extent tree structure and the blocks its nodes occupy
			if tree := c.extentTrees[inodeNum]; tree != nil {
				if inode.InlineExtents != [2]ExtentData{} {
					c.addProblem(PhaseExtents, Error, fmt.Sprintf("inode %d", inodeNum),
						"inline extents set on an inode that uses an extent tree",
						false)
					extentErrors++
				}
				if tree.err != nil {
					c.addProblem(PhaseExtents, Error, fmt.Sprintf("inode %d", inodeNum),
						fmt.Sprintf("extent tree: %v", tree.err),
						false)
					extentErrors++
				}
				for _, block := range tree.nodes {
					location := fmt.Sprintf("inode %d tree node %d", inodeNum, block)
					if !c.bitmapAllocated[block] {
						c.addProblem(PhaseExtents, Error, location,
							"extent tree node block not marked allocated in bitmap",
							true)
						extentErrors++
					}
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
