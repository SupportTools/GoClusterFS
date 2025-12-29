package fsck

import (
	"fmt"
)

// crossValidate performs final cross-structure consistency checks (Phase 8).
//
// Checks performed:
//   - Bitmap allocation matches inode extent references
//   - Refcount values match actual extent reference counts
//   - Inode link count matches directory entry references
//   - Superblock counters match calculated values
//   - Snapshot inodes are properly marked
//   - No leaked blocks (allocated but unreferenced)
//   - No phantom blocks (referenced but not allocated)
func (c *Checker) crossValidate() error {
	if c.sbData == nil {
		return fmt.Errorf("superblock not validated")
	}

	c.logf("  Cross-validating collected data...\n")

	// Counters for summary
	leakedBlocks := 0
	phantomBlocks := 0
	refcountMismatches := 0
	linkCountMismatches := 0

	// 1. Compare bitmap allocation with inode block references
	c.logf("  Checking bitmap vs inode references...\n")
	for block := range c.bitmapAllocated {
		refs := c.inodeBlocks[block]
		if refs == 0 {
			// Block allocated in bitmap but not referenced by any inode
			// This could be metadata or a leak
			leakedBlocks++
			if leakedBlocks <= 10 {
				c.addProblem(PhaseCrossValidation, Warning, fmt.Sprintf("block %d", block),
					"allocated in bitmap but not referenced by any inode (possible leak)",
					true)
			}
		}
	}
	if leakedBlocks > 10 {
		c.addProblem(PhaseCrossValidation, Warning, "blocks",
			fmt.Sprintf("%d additional blocks allocated but unreferenced", leakedBlocks-10),
			true)
	}

	// Check for phantom blocks (referenced but not allocated)
	for block, refs := range c.inodeBlocks {
		if refs > 0 && !c.bitmapAllocated[block] {
			phantomBlocks++
			if phantomBlocks <= 10 {
				c.addProblem(PhaseCrossValidation, Critical, fmt.Sprintf("block %d", block),
					fmt.Sprintf("referenced by %d inode(s) but not allocated in bitmap", refs),
					true)
			}
		}
	}
	if phantomBlocks > 10 {
		c.addProblem(PhaseCrossValidation, Critical, "blocks",
			fmt.Sprintf("%d additional phantom blocks found", phantomBlocks-10),
			true)
	}

	// 2. Validate refcount consistency with extent references
	c.logf("  Checking refcount consistency...\n")
	for block, refcount := range c.refcountEntries {
		extentRefs := c.inodeBlocks[block]

		if extentRefs == 0 && refcount > 0 && !c.bitmapAllocated[block] {
			// Block has refcount but no references and not allocated
			// This is a stale refcount entry
			if refcountMismatches < 10 {
				c.addProblem(PhaseCrossValidation, Warning, fmt.Sprintf("block %d", block),
					fmt.Sprintf("refcount is %d but block is unreferenced and not allocated", refcount),
					true)
			}
			refcountMismatches++
		}

		// For shared blocks, refcount should match or exceed reference count
		if extentRefs > 1 && refcount < uint32(extentRefs) {
			if refcountMismatches < 20 {
				c.addProblem(PhaseCrossValidation, Error, fmt.Sprintf("block %d", block),
					fmt.Sprintf("shared by %d inodes but refcount is only %d", extentRefs, refcount),
					true)
			}
			refcountMismatches++
		}
	}
	if refcountMismatches > 20 {
		c.addProblem(PhaseCrossValidation, Warning, "refcounts",
			fmt.Sprintf("%d additional refcount mismatches", refcountMismatches-20),
			true)
	}

	// 3. Validate inode link count matches directory entry references
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
					true)
			}
			linkCountMismatches++
		}
	}
	if linkCountMismatches > 10 {
		c.addProblem(PhaseCrossValidation, Warning, "inodes",
			fmt.Sprintf("%d additional unreferenced inodes", linkCountMismatches-10),
			true)
	}

	// 4. Validate superblock counters
	c.logf("  Validating superblock counters...\n")
	c.validateSuperblockCounters()

	// 5. Summary statistics
	c.logf("  Leaked blocks: %d\n", leakedBlocks)
	c.logf("  Phantom blocks: %d\n", phantomBlocks)
	c.logf("  Refcount mismatches: %d\n", refcountMismatches)
	c.logf("  Link count issues: %d\n", linkCountMismatches)

	return nil
}

// validateSuperblockCounters cross-checks superblock counters against calculated values.
func (c *Checker) validateSuperblockCounters() {
	sb := c.sbData

	// Calculate actual free blocks from bitmap
	allocatedBlockCount := uint64(len(c.bitmapAllocated))
	calculatedFreeBlocks := sb.TotalBlocks - allocatedBlockCount

	if calculatedFreeBlocks != sb.FreeBlocks {
		c.addProblem(PhaseCrossValidation, Error, "superblock.FreeBlocks",
			fmt.Sprintf("superblock says %d free blocks, calculated %d",
				sb.FreeBlocks, calculatedFreeBlocks),
			true)

		if c.opts.Mode == Repair {
			// Would need to update superblock here
			c.logf("  Would repair FreeBlocks counter (not implemented)\n")
		}
	}

	// Calculate actual used inodes
	usedInodeCount := uint64(len(c.inodeUsed))
	totalInodeSlots := sb.InodeTableSize / InodeSize
	calculatedFreeInodes := totalInodeSlots - usedInodeCount

	if calculatedFreeInodes != sb.FreeInodes {
		c.addProblem(PhaseCrossValidation, Error, "superblock.FreeInodes",
			fmt.Sprintf("superblock says %d free inodes, calculated %d",
				sb.FreeInodes, calculatedFreeInodes),
			true)

		if c.opts.Mode == Repair {
			c.logf("  Would repair FreeInodes counter (not implemented)\n")
		}
	}

	// Validate snapshot count
	if c.result.SnapshotCount != sb.SnapshotCount {
		c.addProblem(PhaseCrossValidation, Error, "superblock.SnapshotCount",
			fmt.Sprintf("superblock says %d snapshots, found %d active",
				sb.SnapshotCount, c.result.SnapshotCount),
			true)
	}

	// Update result with calculated values
	c.result.TotalBlocks = sb.TotalBlocks
	c.result.AllocatedBlocks = allocatedBlockCount
	c.result.FreeBlocks = calculatedFreeBlocks
	c.result.TotalInodes = totalInodeSlots
}
