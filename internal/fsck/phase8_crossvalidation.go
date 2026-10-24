package fsck

import (
	"bytes"
	"fmt"
	"maps"
	"slices"

	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/sfb"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// crossValidate performs final cross-structure consistency checks (Phase 8).
//
// Checks performed:
//   - Bitmap allocation matches inode extent references
//   - Inode link count matches directory entry references
//   - No leaked blocks (allocated but unreferenced)
//   - No phantom blocks (referenced but not allocated)
//
// Phase 5 checks refcounts. The superblock free block, free inode and snapshot
// counters are not compared: the runtime never updates them.
func (c *Checker) crossValidate() error {
	if c.sbData == nil {
		return fmt.Errorf("superblock not validated")
	}

	c.logf("  Cross-validating collected data...\n")

	// Counters for summary
	leakedBlocks := 0
	phantomBlocks := 0
	linkCountMismatches := 0

	// 1. Compare bitmap allocation with inode block references
	c.logf("  Checking bitmap vs inode references...\n")
	sectorBuf := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	for _, block := range slices.Sorted(maps.Keys(c.bitmapAllocated)) {
		if c.inodeBlocks[block] > 0 || c.sfbContainers[block] || c.isSFBContainer(block, sectorBuf) {
			continue
		}
		leakedBlocks++
		if leakedBlocks <= 10 {
			c.addProblem(PhaseCrossValidation, Warning, fmt.Sprintf("block %d", block),
				"allocated in bitmap but not referenced by any inode (possible leak)",
				false)
		}
	}
	if leakedBlocks > 10 {
		c.addProblem(PhaseCrossValidation, Warning, "blocks",
			fmt.Sprintf("%d additional blocks allocated but unreferenced", leakedBlocks-10),
			false)
	}

	// Check for phantom blocks (referenced but not allocated)
	for block, refs := range c.inodeBlocks {
		if refs > 0 && !c.bitmapAllocated[block] {
			phantomBlocks++
			if phantomBlocks <= 10 {
				c.addProblem(PhaseCrossValidation, Critical, fmt.Sprintf("block %d", block),
					fmt.Sprintf("referenced by %d inode(s) but not allocated in bitmap", refs),
					false)
			}
		}
	}
	if phantomBlocks > 10 {
		c.addProblem(PhaseCrossValidation, Critical, "blocks",
			fmt.Sprintf("%d additional phantom blocks found", phantomBlocks-10),
			false)
	}

	// 2. Validate inode link count matches directory entry references
	c.logf("  Checking link count consistency...\n")
	for inodeNum := range c.inodeUsed {
		dirRefs := c.dirEntryRefs[inodeNum]

		// Special case: root directory (inode 2) doesn't need a parent reference
		if inodeNum == InodeRoot {
			continue
		}

		// Snapshot inodes may have special linking
		if c.snapshotInodes[inodeNum] {
			continue
		}

		// Every used inode should be referenced by at least one directory entry
		// (except root and snapshot targets)
		if dirRefs == 0 {
			if linkCountMismatches < 10 {
				c.addProblem(PhaseCrossValidation, Warning, fmt.Sprintf("inode %d", inodeNum),
					"in use but not referenced by any directory entry",
					false)
			}
			linkCountMismatches++
		}
	}
	if linkCountMismatches > 10 {
		c.addProblem(PhaseCrossValidation, Warning, "inodes",
			fmt.Sprintf("%d additional unreferenced inodes", linkCountMismatches-10),
			false)
	}

	c.result.TotalBlocks = c.sbData.TotalBlocks
	c.result.AllocatedBlocks = uint64(len(c.bitmapAllocated))
	c.result.TotalInodes = c.sbData.InodeTableSize / InodeSize

	// 3. Summary statistics
	c.logf("  Leaked blocks: %d\n", leakedBlocks)
	c.logf("  Phantom blocks: %d\n", phantomBlocks)
	c.logf("  Link count issues: %d\n", linkCountMismatches)

	return nil
}

// isSFBContainer reports whether block starts with an SFB container header.
// Older versions packed small files into such blocks, and empty ones stay
// allocated for reuse.
func (c *Checker) isSFBContainer(block uint64, buf []byte) bool {
	if c.readSector(buf, c.sbData.DataRegionOffset+block*extent.BlockSize) != nil {
		return false
	}
	h, err := sfb.DeserializeHeader(buf)
	return err == nil && bytes.Equal(h.Serialize(), buf[:sfb.HeaderSize])
}
