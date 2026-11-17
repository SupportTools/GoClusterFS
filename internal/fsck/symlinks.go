package fsck

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// checkSymlink reports, and never repairs, a symlink whose length, layout or target is invalid.
func (c *Checker) checkSymlink(num uint32, ino *InodeData) {
	report := func(format string, args ...any) {
		c.addProblem(PhaseInodes, Error, fmt.Sprintf("inode %d", num), "symlink "+fmt.Sprintf(format, args...), false)
	}
	if ino.LinkCount != 1 {
		report("has link count %d, want 1", ino.LinkCount)
	}
	if ino.Size == 0 || ino.Size > MaxSymlinkTarget {
		report("target length %d is outside 1 to %d", ino.Size, MaxSymlinkTarget)
		return
	}
	inline := ino.Flags&FlagInlineTarget != 0
	if inline != (ino.Size <= MaxInlineTarget) {
		report("target of %d bytes is stored inline=%v, want inline=%v", ino.Size, inline, !inline)
		return
	}
	target := ino.Target[:min(ino.Size, MaxInlineTarget)]
	if inline {
		if slices.ContainsFunc(ino.Target[ino.Size:], func(b byte) bool { return b != 0 }) {
			report("has bytes after its %d-byte target", ino.Size)
		}
	} else {
		ext := ino.InlineExtents[0]
		if ino.Flags&FlagHasExtentTree != 0 || ext.LogicalBlock != 0 || ext.Length != 1 || ino.InlineExtents[1].Length != 0 {
			report("maps more or less than its one target block")
			return
		}
		if ext.PhysicalBlock >= c.sbData.TotalBlocks {
			report("target block %d is past the end of the volume", ext.PhysicalBlock)
			return
		}
		buf := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
		if err := c.readSector(buf, c.sbData.DataRegionOffset+ext.PhysicalBlock*uint64(c.sbData.BlockSize)); err != nil {
			report("target block %d cannot be read: %v", ext.PhysicalBlock, err)
			return
		}
		target = buf[:ino.Size]
	}
	if bytes.IndexByte(target, 0) >= 0 {
		report("target contains a NUL byte")
	}
}
