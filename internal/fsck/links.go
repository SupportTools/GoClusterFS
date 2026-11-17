package fsck

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/supporttools/GoClusterFS/internal/inode"
)

// checkFileLinks reports a regular file whose link count is not the number of names it has, and repair sets the
// count to the names. Too few links lets an unlink free the file while a name remains; too many keeps it forever.
// A file with no name is checkUnnamed's.
func (c *Checker) checkFileLinks() error {
	if len(c.namesIncomplete) > 0 {
		c.logf("  File link counts not checked: %s\n", c.namesIncomplete[0])
		return nil
	}
	fix := map[uint32]uint32{}
	var problems []int
	for _, num := range slices.Sorted(maps.Keys(c.inodeUsed)) {
		names := c.dirEntryRefs[num]
		if names == 0 || c.entryType[num] != DT_REG {
			continue
		}
		ino, err := c.readInode(num)
		if err != nil {
			c.addProblem(PhaseCrossValidation, Warning, fmt.Sprintf("inode %d", num), "link count not checked: "+err.Error(), false)
			continue
		}
		if int(ino.LinkCount) == names {
			continue
		}
		severity, why := Warning, "it will never be freed"
		if int(ino.LinkCount) < names {
			severity, why = Error, "unlinking can free it while a name remains"
		}
		c.addProblem(PhaseCrossValidation, severity, fmt.Sprintf("inode %d", num),
			fmt.Sprintf("regular file has %d links but %d names: %s", ino.LinkCount, names, why), true)
		problems = append(problems, c.lastProblem())
		fix[num] = uint32(names)
	}
	if len(fix) == 0 || c.opts.Mode != Repair {
		return nil
	}
	if err := c.beforeWrites(); err != nil {
		return err
	}
	if !c.opts.DryRun {
		sb := c.sbData
		tbl := inode.NewTable(c.dev, sb.InodeTableOffset, sb.InodeTableSize, nil, [16]byte{})
		for _, num := range slices.Sorted(maps.Keys(fix)) {
			if err := tbl.Update(context.Background(), num, func(ino *inode.Inode) error {
				ino.LinkCount = fix[num]
				ino.Holders = 0
				return nil
			}); err != nil {
				return fmt.Errorf("set the link count of inode %d: %w", num, err)
			}
		}
		if err := c.dev.Sync(); err != nil {
			return fmt.Errorf("sync link counts: %w", err)
		}
	}
	c.markRepaired(problems...)
	return nil
}
