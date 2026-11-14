package fsck

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/supporttools/GoClusterFS/internal/journal"
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
	reserves := c.reserves()
	reserved := make(map[uint64]bool)
	for _, r := range reserves {
		for _, b := range r.blocks {
			reserved[b] = true
		}
	}
	var leaked []uint64
	badSector := 0
	for _, block := range c.leakedBlocks() {
		switch {
		case reserved[block]:
		case c.badBitmap[block/BitmapBitsPerSector]:
			badSector++
		default:
			leaked = append(leaked, block)
		}
	}
	if err := c.checkReserves(reserves); err != nil {
		return err
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

// slotReserve is what a crashed node reserved and never used: the blocks it
// marked allocated, and the journal records that hold them.
type slotReserve struct {
	slot   int
	blocks []uint64
	seqs   []uint64
}

// reserves reads every journal that needs replay for the blocks its node
// reserved and never used. Replay frees those, so they are not leaks.
func (c *Checker) reserves() []slotReserve {
	var out []slotReserve
	for i := 0; i < int(c.sbData.MaxNodes); i++ {
		if c.journalNeedsReplay(i) == "" {
			continue
		}
		region, err := c.slotJournal(i)
		if err != nil {
			continue
		}
		blocks, seqs, err := journal.ReservedUnused(region)
		_ = region.Close()
		if err == nil && len(blocks) > 0 {
			out = append(out, slotReserve{slot: i, blocks: blocks, seqs: seqs})
		}
	}
	return out
}

// checkReserves reports the blocks crashed nodes reserved and never used. In
// repair mode, which refuses while nodes are mounted, it marks their reserves
// done in the node's journal, so replay does not free them again, and then
// frees them.
func (c *Checker) checkReserves(reserves []slotReserve) error {
	repairable := len(c.referenceGaps()) == 0
	for _, r := range reserves {
		var free []uint64
		for _, b := range r.blocks {
			if c.isLeaked(b) && !c.badBitmap[b/BitmapBitsPerSector] {
				free = append(free, b)
			}
		}
		if len(free) == 0 {
			continue
		}
		c.addProblem(PhaseCrossValidation, Info, fmt.Sprintf("node %d", r.slot),
			fmt.Sprintf("%d blocks reserved by node %d and never used; replaying its journal frees them", len(free), r.slot), repairable)
		problem := c.lastProblem()
		if c.opts.Mode != Repair || !repairable {
			continue
		}
		if err := c.beforeWrites(); err != nil {
			return err
		}
		if !c.opts.DryRun {
			region, err := c.slotJournal(r.slot)
			if err == nil {
				err = journal.CompleteReserves(region, r.seqs)
				_ = region.Close()
			}
			if err != nil {
				c.addProblem(PhaseCrossValidation, Warning, fmt.Sprintf("node %d", r.slot),
					fmt.Sprintf("its %d reserved blocks are kept: marking its reserve done in its journal failed: %v", len(free), err), false)
				continue
			}
		}
		if err := c.freeBlocks(free); err != nil {
			return err
		}
		c.markRepaired(problem)
	}
	return nil
}
