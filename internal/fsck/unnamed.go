package fsck

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// unnamedBlockers adds to reclaimBlockers the reasons fsck may not know every
// name or snapshot reference, so that repair keeps every unnamed inode.
func (c *Checker) unnamedBlockers() []string {
	why := c.reclaimBlockers()
	for _, p := range []Phase{PhaseDirectories, PhaseSnapshots} {
		if !c.finished[p] {
			why = append(why, p.String()+" did not finish")
		}
	}
	why = append(why, c.namesIncomplete...)
	return append(why, c.snapsIncomplete...)
}

// keepUnnamed says why repair must keep an inode no directory names, or
// returns "" when repair may free it.
func (c *Checker) keepUnnamed(num uint32, ino *InodeData) string {
	_, uncounted := c.uncounted[num]
	switch {
	case num < inode.InodeFirstUsable:
		return "it is a reserved inode"
	case c.snapshotInodes[num]:
		return "a snapshot refers to it"
	case ino.InodeNum != num:
		return fmt.Sprintf("it stores inode number %d", ino.InodeNum)
	case ino.Mode&S_IFMT == S_IFDIR && c.dirNames[num] > 0:
		return "it is a directory that is not empty"
	case slices.ContainsFunc(c.inodeExtents(num, ino), func(x ExtentData) bool { return x.Length > 0 && x.Flags&ExtentFlagSFB != 0 }):
		return "it maps small-file slots"
	case uncounted:
		return "its sharing must be counted first; run repair again"
	}
	return ""
}

// checkUnnamed reports inodes in use that no directory names and, in repair
// mode, frees them as unlink does: the inode first, then its blocks through
// their refcounts, so a block that another inode still maps keeps its other
// owners. Snapshot data inodes a snapshot refers to are not unnamed.
func (c *Checker) checkUnnamed() error {
	blockers := c.unnamedBlockers()
	reclaim := len(blockers) == 0

	var free []*InodeData
	var problems []int
	kept := 0
	for _, num := range c.unnamed {
		if c.snapshotData[num] && (c.snapshotInodes[num] || !c.finished[PhaseSnapshots]) {
			continue
		}
		loc := fmt.Sprintf("inode %d", num)
		ino, err := c.readInode(num)
		why := ""
		if err != nil {
			why = "it cannot be read: " + err.Error()
		} else {
			why = c.keepUnnamed(num, ino)
		}
		if why != "" {
			if kept++; kept <= reportedLeaks {
				c.addProblem(PhaseCrossValidation, Warning, loc, "unnamed: no directory names it; kept, because "+why, false)
			}
			continue
		}
		if free = append(free, ino); len(free) <= reportedLeaks {
			c.addProblem(PhaseCrossValidation, Warning, loc, "unnamed: no directory names it", reclaim)
			problems = append(problems, c.lastProblem())
		}
	}
	c.result.OrphanedInodes = uint64(kept + len(free))
	if kept > reportedLeaks {
		c.addProblem(PhaseCrossValidation, Warning, "inodes",
			fmt.Sprintf("%d more unnamed inodes are kept", kept-reportedLeaks), false)
	}
	if len(free) > reportedLeaks {
		c.addProblem(PhaseCrossValidation, Warning, "inodes",
			fmt.Sprintf("%d more unnamed inodes", len(free)-reportedLeaks), reclaim)
		problems = append(problems, c.lastProblem())
	}
	if len(free) == 0 {
		return nil
	}
	if !reclaim {
		c.addProblem(PhaseCrossValidation, Warning, "inodes",
			fmt.Sprintf("%d unnamed inodes are kept, because fsck may not know every name or reference: %s",
				len(free), strings.Join(blockers, "; ")), false)
	}

	if c.opts.Mode != Repair || !reclaim {
		return nil
	}
	if err := c.beforeWrites(); err != nil {
		return err
	}
	if err := c.freeInodes(free); err != nil {
		return err
	}
	c.markRepaired(problems...)
	return nil
}

// freeInodes frees inodes and their blocks with the runtime's inode table and
// extent manager, then updates what fsck knows about the blocks, so that phase 8
// frees any block the runtime left allocated as leaked.
func (c *Checker) freeInodes(inodes []*InodeData) error {
	var touched []uint64
	for _, ino := range inodes {
		for _, b := range c.mappedBlocks(ino.InodeNum, ino) {
			c.inodeBlocks[b]--
			touched = append(touched, b)
		}
		delete(c.inodeUsed, ino.InodeNum)
		delete(c.extentTrees, ino.InodeNum)
	}
	slices.Sort(touched)
	touched = slices.Compact(touched)
	if c.opts.DryRun {
		for _, b := range touched {
			if c.inodeBlocks[b] == 0 {
				delete(c.bitmapAllocated, b)
			}
		}
		return nil
	}

	sb := c.sbData
	var uuid [16]byte
	tbl := inode.NewTable(c.dev, sb.InodeTableOffset, sb.InodeTableSize, nil, uuid)
	bm := bitmap.NewRegion(c.dev, sb.BlockBitmapOffset, sb.TotalBlocks, nil, uuid)
	mgr := extent.NewManager(c.dev, bm, nil, sb.DataRegionOffset)
	mgr.SetRefcounts(refcount.NewRegion(c.dev, sb.RefCountTableOffset, sb.RefCountTableSize, sb.TotalBlocks, nil))
	tbl.SetTreeFlusher(mgr)
	ctx := context.Background()

	freed := make([]*inode.Inode, 0, len(inodes))
	for _, ino := range inodes {
		full, err := tbl.Read(ino.InodeNum)
		if err != nil {
			return fmt.Errorf("inode %d: %w", ino.InodeNum, err)
		}
		if err := tbl.Free(ctx, ino.InodeNum); err != nil {
			return fmt.Errorf("free inode %d: %w", ino.InodeNum, err)
		}
		freed = append(freed, full)
	}
	// A crash from here on leaves blocks no inode maps, which repair frees.
	if err := c.dev.Sync(); err != nil {
		return fmt.Errorf("sync inodes: %w", err)
	}
	for _, full := range freed {
		if err := mgr.Delete(ctx, full); err != nil {
			return fmt.Errorf("free blocks of inode %d: %w", full.InodeNum, err)
		}
	}
	if err := c.dev.Sync(); err != nil {
		return fmt.Errorf("sync freed blocks: %w", err)
	}
	return c.rereadAllocated(touched)
}

// rereadAllocated updates bitmapAllocated for blocks, which must be sorted,
// from the bitmap on disk.
func (c *Checker) rereadAllocated(blocks []uint64) error {
	buf := storage.NewAlignedBuffer(BitmapSectorSize).Bytes()
	read := false
	var sector uint64
	for _, b := range blocks {
		if !read || b/BitmapBitsPerSector != sector {
			sector, read = b/BitmapBitsPerSector, true
			if err := c.readSector(buf, c.sbData.BlockBitmapOffset+sector*BitmapSectorSize); err != nil {
				return fmt.Errorf("bitmap sector %d: %w", sector, err)
			}
		}
		bit := b % BitmapBitsPerSector
		if buf[bit/8]&(1<<(bit%8)) != 0 {
			c.bitmapAllocated[b] = true
		} else {
			delete(c.bitmapAllocated, b)
		}
	}
	return nil
}

// readInode reads and parses inode num.
func (c *Checker) readInode(num uint32) (*InodeData, error) {
	buf := storage.NewAlignedBuffer(InodeSectorSize).Bytes()
	if err := c.readSector(buf, c.sbData.InodeTableOffset+uint64(num/InodesPerSector)*InodeSectorSize); err != nil {
		return nil, err
	}
	i := num % InodesPerSector
	return c.parseInode(buf[i*InodeSize : (i+1)*InodeSize]), nil
}
