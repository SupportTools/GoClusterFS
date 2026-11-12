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

// leakedBlocks returns the blocks the bitmap marks allocated that no inode maps
// as data or extent-tree nodes and no SFB extent points into. SFB allocation is
// disabled, so a container nothing points into is leaked too.
func (c *Checker) leakedBlocks() []uint64 {
	var leaked []uint64
	for _, block := range slices.Sorted(maps.Keys(c.bitmapAllocated)) {
		if c.inodeBlocks[block] == 0 && !c.sfbContainers[block] {
			leaked = append(leaked, block)
		}
	}
	return leaked
}

// reclaimBlockers says why repair cannot tell which blocks are leaked: a phase
// that collects references or clears refcounts did not finish, a reference
// could not be read, or a node's journal may map blocks fsck cannot see.
func (c *Checker) reclaimBlockers() []string {
	var why []string
	for _, p := range []Phase{PhaseBitmap, PhaseInodes, PhaseRefcounts} {
		if !c.finished[p] {
			why = append(why, p.String()+" did not finish")
		}
	}
	return append(append(why, c.refsIncomplete...), c.unclean...)
}

// checkLeaks reports leaked blocks and, in repair mode, frees them in the
// bitmap. Phase 5 has already cleared their refcounts. Repair keeps all of
// them when reclaimBlockers finds a reason, and keeps those in bitmap sectors
// with a bad checksum.
func (c *Checker) checkLeaks() error {
	leaked := c.leakedBlocks()
	if len(leaked) == 0 {
		return nil
	}
	blockers := c.reclaimBlockers()

	var free []uint64
	freeing := make(map[uint64]bool)
	badSector := 0
	for _, block := range leaked {
		switch {
		case c.badBitmap[block/BitmapBitsPerSector]:
			badSector++
		case len(blockers) == 0:
			free = append(free, block)
			freeing[block] = true
		}
	}

	var problems []int
	listed := leaked[:min(len(leaked), reportedLeaks)]
	for _, block := range listed {
		c.addProblem(PhaseCrossValidation, Warning, fmt.Sprintf("block %d", block),
			"leaked: allocated in the bitmap but no inode maps it", freeing[block])
		if freeing[block] {
			problems = append(problems, c.lastProblem())
		}
	}
	if rest := leaked[len(listed):]; len(rest) > 0 {
		all := !slices.ContainsFunc(rest, func(b uint64) bool { return !freeing[b] })
		c.addProblem(PhaseCrossValidation, Warning, "blocks", fmt.Sprintf("%d more leaked blocks", len(rest)), all)
		if all {
			problems = append(problems, c.lastProblem())
		}
	}
	if badSector > 0 {
		c.addProblem(PhaseCrossValidation, Warning, "bitmap",
			fmt.Sprintf("%d leaked blocks are kept: their bitmap sectors have a bad checksum", badSector), false)
	}
	if len(blockers) > 0 {
		c.addProblem(PhaseCrossValidation, Warning, "blocks",
			fmt.Sprintf("%d leaked blocks are kept, because these may map them: %s", len(leaked), strings.Join(blockers, "; ")), false)
	}

	if c.opts.Mode != Repair || len(free) == 0 {
		return nil
	}
	if err := c.beforeWrites(); err != nil {
		return err
	}
	if err := c.freeBlocks(free); err != nil {
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
