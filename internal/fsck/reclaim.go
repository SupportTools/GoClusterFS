package fsck

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// reportedLeaks is how many leaked blocks fsck names one by one.
const reportedLeaks = 10

// isLeaked reports whether the bitmap marks block allocated while no inode maps
// it as data or extent-tree nodes and no SFB extent points into it. SFB
// allocation is disabled, so a container nothing points into is leaked too.
func (c *Checker) isLeaked(block uint64) bool {
	return c.bitmapAllocated[block] && c.inodeBlocks[block] == 0 && !c.sfbContainers[block]
}

func (c *Checker) leakedBlocks() []uint64 {
	var leaked []uint64
	for _, block := range slices.Sorted(maps.Keys(c.bitmapAllocated)) {
		if c.isLeaked(block) {
			leaked = append(leaked, block)
		}
	}
	return leaked
}

// referenceGaps says why fsck may not know every block an inode maps: the
// bitmap or inode phase did not finish, or a reference could not be read.
func (c *Checker) referenceGaps() []string {
	var why []string
	for _, p := range []Phase{PhaseBitmap, PhaseInodes} {
		if !c.finished[p] {
			why = append(why, p.String()+" did not finish")
		}
	}
	return append(why, c.refsIncomplete...)
}

// reclaimBlockers adds to referenceGaps the other reasons repair keeps leaked
// blocks: their refcounts may not be cleared, or a node's journal may map them.
func (c *Checker) reclaimBlockers() []string {
	why := c.referenceGaps()
	if !c.finished[PhaseRefcounts] {
		why = append(why, PhaseRefcounts.String()+" did not finish")
	}
	return append(why, c.unclean...)
}

// checkLeaks reports leaked blocks and, in repair mode, frees them in the
// bitmap. Phase 5 has already cleared their refcounts. Repair keeps all of
// them when reclaimBlockers finds a reason. Leaked blocks in bitmap sectors
// with a bad checksum are kept and reported apart.
func (c *Checker) checkLeaks() error {
	var leaked []uint64
	badSector := 0
	for _, block := range c.leakedBlocks() {
		if c.badBitmap[block/BitmapBitsPerSector] {
			badSector++
		} else {
			leaked = append(leaked, block)
		}
	}
	if badSector > 0 {
		c.addProblem(PhaseCrossValidation, Warning, "bitmap",
			fmt.Sprintf("%d leaked blocks are kept: their bitmap sectors have a bad checksum", badSector), false)
	}
	if len(leaked) == 0 {
		return nil
	}
	blockers := c.reclaimBlockers()
	reclaim := len(blockers) == 0

	var problems []int
	for _, block := range leaked[:min(len(leaked), reportedLeaks)] {
		c.addProblem(PhaseCrossValidation, Warning, fmt.Sprintf("block %d", block),
			"leaked: allocated in the bitmap but no inode maps it", reclaim)
		problems = append(problems, c.lastProblem())
	}
	if len(leaked) > reportedLeaks {
		c.addProblem(PhaseCrossValidation, Warning, "blocks",
			fmt.Sprintf("%d more leaked blocks", len(leaked)-reportedLeaks), reclaim)
		problems = append(problems, c.lastProblem())
	}
	if !reclaim {
		c.addProblem(PhaseCrossValidation, Warning, "blocks",
			fmt.Sprintf("%d leaked blocks are kept, because these may map them: %s", len(leaked), strings.Join(blockers, "; ")), false)
	}

	if c.opts.Mode != Repair || !reclaim {
		return nil
	}
	if err := c.beforeWrites(); err != nil {
		return err
	}
	if err := c.freeBlocks(leaked); err != nil {
		return err
	}
	c.markRepaired(problems...)
	return nil
}

// freeBlocks clears the bitmap bits of blocks, which must be sorted, one sector
// write at a time.
func (c *Checker) freeBlocks(blocks []uint64) error {
	sb := c.sbData
	buf := storage.NewAlignedBuffer(BitmapSectorSize).Bytes()
	for len(blocks) > 0 {
		sector := blocks[0] / BitmapBitsPerSector
		n := 1
		for n < len(blocks) && blocks[n]/BitmapBitsPerSector == sector {
			n++
		}
		offset := sb.BlockBitmapOffset + sector*BitmapSectorSize
		if err := c.readSector(buf, offset); err != nil {
			return fmt.Errorf("bitmap sector %d: %w", sector, err)
		}
		if !sectorChecksumValid(buf) {
			return fmt.Errorf("bitmap sector %d: checksum mismatch", sector)
		}
		for _, block := range blocks[:n] {
			bit := block % BitmapBitsPerSector
			buf[bit/8] &^= 1 << (bit % 8)
		}
		setSectorChecksum(buf)
		if err := c.write(buf, offset); err != nil {
			return fmt.Errorf("bitmap sector %d: %w", sector, err)
		}
		blocks = blocks[n:]
	}
	if c.opts.DryRun {
		return nil
	}
	if err := c.dev.Sync(); err != nil {
		return fmt.Errorf("sync bitmap: %w", err)
	}
	return nil
}
