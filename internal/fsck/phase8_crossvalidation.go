package fsck

import (
	"fmt"
)

// crossValidate performs final cross-structure consistency checks (Phase 8).
//
// Checks performed:
//   - Bitmap allocation matches inode extent references
//   - Every inode in use has a name in a directory; repair frees unnamed ones
//   - No leaked blocks (allocated but unreferenced); repair frees them
//   - No phantom blocks (referenced but not allocated)
//
// Phase 5 checks refcounts. The superblock free block, free inode and snapshot
// counters are not compared: the runtime never updates them.
func (c *Checker) crossValidate() error {
	if c.sbData == nil {
		return fmt.Errorf("superblock not validated")
	}

	c.logf("  Cross-validating collected data...\n")

	phantomBlocks := 0

	c.logf("  Checking for inodes no directory names...\n")
	if err := c.checkUnnamed(); err != nil {
		return err
	}

	c.logf("  Checking bitmap vs inode references...\n")
	if err := c.checkLeaks(); err != nil {
		return err
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

	c.result.TotalBlocks = c.sbData.TotalBlocks
	c.result.AllocatedBlocks = uint64(len(c.bitmapAllocated))
	c.result.TotalInodes = c.sbData.InodeTableSize / InodeSize

	c.logf("  Phantom blocks: %d\n", phantomBlocks)

	return nil
}
