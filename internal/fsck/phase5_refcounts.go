package fsck

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// RefcountSectorSize is the size of each refcount sector.
	RefcountSectorSize = 4096

	// RefcountChecksumSize is the size of the CRC32C checksum.
	RefcountChecksumSize = 4

	// RefcountEntrySize is the size of each refcount entry.
	// 4 bytes RefCount + 2 bytes Flags + 2 bytes Reserved = 8 bytes.
	RefcountEntrySize = 8

	// RefcountEntriesPerSector is entries per sector.
	// (4096 - 4) / 8 = 511
	RefcountEntriesPerSector = (RefcountSectorSize - RefcountChecksumSize) / RefcountEntrySize
)

// RefcountEntry holds parsed refcount data.
type RefcountEntry struct {
	RefCount uint32
	Flags    uint16
}

// Refcount flags
const (
	RefcountFlagSnapshot   uint16 = 1 << 0
	RefcountFlagCowPending uint16 = 1 << 1
	RefcountFlagMetadata   uint16 = 1 << 2
	RefcountFlagPinned     uint16 = 1 << 3
)

// refcountFix is a change repair makes to one refcount sector: a new count for
// one block, or, when entry is false, a sector rebuilt from the references.
type refcountFix struct {
	sector  uint64
	entry   bool
	block   uint64
	count   uint32
	problem int // index of the problem the fix repairs
}

// checkRefcounts validates the refcount table (Phase 5).
//
// A block's refcount counts the inodes that map it, where 0 and 1 both mean
// one owner. Checks performed:
//   - Each sector has a valid CRC32C checksum (or is all zero, never written)
//   - Free blocks have a count of 0
//   - Blocks mapped by several inodes count at least that many owners;
//     fewer would free a block another inode still maps
//   - Blocks count no more owners than inodes map them; more would leak them
//   - No refcount overflow (0xFFFFFFFF)
//   - No block past the table, which has no count for it, is mapped by
//     several inodes that count their sharing
//
// In repair mode, and only when every inode's extents were read, the counts
// are rebuilt from those references, and extents an older version shared
// without counting are marked counted.
func (c *Checker) checkRefcounts() error {
	if c.sbData == nil {
		return fmt.Errorf("superblock not validated")
	}

	sb := c.sbData
	if sb.RefCountTableSize == 0 {
		c.logf("  Refcount table not present (size=0)\n")
		return nil
	}
	totalSectors := min(sb.RefCountTableSize/RefcountSectorSize,
		(sb.TotalBlocks+RefcountEntriesPerSector-1)/RefcountEntriesPerSector)

	c.logf("  Refcount table offset: 0x%X\n", sb.RefCountTableOffset)
	c.logf("  Refcount table size: %d bytes (%d sectors)\n", sb.RefCountTableSize, totalSectors)

	sectorBuf := storage.NewAlignedBuffer(RefcountSectorSize).Bytes()
	sharedBlocks := uint64(0)
	var fixes []refcountFix

	for sectorIdx := uint64(0); sectorIdx < totalSectors; sectorIdx++ {
		if err := c.readSector(sectorBuf, sb.RefCountTableOffset+sectorIdx*RefcountSectorSize); err != nil {
			c.addProblem(PhaseRefcounts, Critical, fmt.Sprintf("sector %d", sectorIdx), err.Error(), false)
			c.refsIncomplete = append(c.refsIncomplete, fmt.Sprintf("refcount sector %d is unreadable", sectorIdx))
			continue
		}
		// None of the counts in such a sector can be trusted, so repair
		// rebuilds all of them.
		if !sectorChecksumValid(sectorBuf) {
			c.addProblem(PhaseRefcounts, Error, fmt.Sprintf("sector %d", sectorIdx),
				"checksum mismatch: the runtime cannot read refcounts in this sector", true)
			fixes = append(fixes, refcountFix{sector: sectorIdx, problem: c.lastProblem()})
			continue
		}

		for entryIdx := 0; entryIdx < RefcountEntriesPerSector; entryIdx++ {
			blockNum := sectorIdx*RefcountEntriesPerSector + uint64(entryIdx)
			if blockNum >= sb.TotalBlocks {
				break
			}
			entry := parseRefcountEntry(sectorBuf[entryIdx*RefcountEntrySize:])
			if entry.RefCount > 1 {
				sharedBlocks++
			}
			if want, ok := c.checkRefcount(blockNum, entry.RefCount); ok {
				fixes = append(fixes, refcountFix{sector: sectorIdx, entry: true, block: blockNum, count: want, problem: c.lastProblem()})
			}
		}
	}
	if covered := c.refcountCoverage(); sb.TotalBlocks > covered {
		c.addProblem(PhaseRefcounts, Warning, "refcount table",
			fmt.Sprintf("has entries for blocks 0-%d of %d; snapshots and clones of files with data in higher blocks fail", covered-1, sb.TotalBlocks), false)
		c.checkSharingPastTable(covered)
	}
	c.reportUncounted()

	c.logf("  Shared blocks (refcount > 1): %d\n", sharedBlocks)

	if c.opts.Mode != Repair || len(fixes) == 0 && len(c.uncounted) == 0 {
		return nil
	}
	if len(c.refsIncomplete) > 0 {
		c.addProblem(PhaseRefcounts, Error, "refcount table",
			"not rebuilt, because some references could not be read: "+strings.Join(c.refsIncomplete, "; "), false)
		return nil
	}
	if !c.opts.DryRun {
		if beforeRepairWrites != nil {
			beforeRepairWrites()
		}
		if err := c.registryUnchanged(); err != nil {
			return err
		}
	}
	if err := c.writeRefcounts(fixes); err != nil {
		return err
	}
	// The runtime trusts counted sharing, so the counts must be on disk first.
	if !c.opts.DryRun {
		if err := c.dev.Sync(); err != nil {
			return fmt.Errorf("sync refcounts: %w", err)
		}
	}
	return c.markSharingCounted()
}

// refcountCoverage returns how many blocks the refcount table has entries for.
func (c *Checker) refcountCoverage() uint64 {
	return c.sbData.RefCountTableSize / RefcountSectorSize * RefcountEntriesPerSector
}

// checkSharingPastTable reports blocks past the refcount table that inodes share
// with counts, which the first delete frees. Only-uncounted sharing is never freed.
func (c *Checker) checkSharingPastTable(covered uint64) {
	var blocks []uint64
	for block, refs := range c.inodeBlocks {
		if block >= covered && refs > 1 && c.uncountedRefs[block] < refs {
			blocks = append(blocks, block)
		}
	}
	slices.Sort(blocks)
	for _, block := range blocks[:min(len(blocks), 10)] {
		c.addProblem(PhaseRefcounts, Critical, fmt.Sprintf("block %d", block),
			fmt.Sprintf("mapped by %d inodes but past the refcount table, which has no count for it: deleting one of them frees a block the others still map",
				c.inodeBlocks[block]), false)
	}
	if len(blocks) > 10 {
		c.addProblem(PhaseRefcounts, Critical, "blocks",
			fmt.Sprintf("%d more blocks past the refcount table are mapped by several inodes", len(blocks)-10), false)
	}
}

// checkRefcount compares one block's count with the inodes that map it. It
// reports a problem and returns the count repair should set, if any.
func (c *Checker) checkRefcount(block uint64, count uint32) (uint32, bool) {
	refs := uint32(c.inodeBlocks[block])
	loc := fmt.Sprintf("block %d", block)
	switch {
	case count == 0xFFFFFFFF:
		c.addProblem(PhaseRefcounts, Critical, loc, "refcount overflow (0xFFFFFFFF)", false)
		return 0, false
	case refs > 1 && count < refs:
		c.addProblem(PhaseRefcounts, Error, loc,
			fmt.Sprintf("mapped by %d inodes but refcount is %d: freeing one of them frees a block another still maps", refs, count), true)
		return refs, true
	case refs > 0 && count > max(refs, 1):
		c.addProblem(PhaseRefcounts, Warning, loc,
			fmt.Sprintf("mapped by %d inode(s) but refcount is %d: the block will never be freed", refs, count), true)
		return refs, true
	case refs == 0 && count != 0 && !c.bitmapAllocated[block]:
		c.addProblem(PhaseRefcounts, Error, loc,
			fmt.Sprintf("free in the bitmap but refcount is %d", count), true)
		return 0, true
	}
	return 0, false
}

// writeRefcounts applies fixes one sector at a time, rewriting its checksum.
func (c *Checker) writeRefcounts(fixes []refcountFix) error {
	sectorBuf := storage.NewAlignedBuffer(RefcountSectorSize).Bytes()
	for len(fixes) > 0 {
		n := 1
		for n < len(fixes) && fixes[n].sector == fixes[0].sector {
			n++
		}
		sector := fixes[0].sector
		offset := c.sbData.RefCountTableOffset + sector*RefcountSectorSize
		if err := c.readSector(sectorBuf, offset); err != nil {
			return fmt.Errorf("refcount sector %d: %w", sector, err)
		}
		problems := make([]int, 0, n)
		for _, f := range fixes[:n] {
			if f.entry {
				binary.LittleEndian.PutUint32(sectorBuf[(f.block%RefcountEntriesPerSector)*RefcountEntrySize:], f.count)
			} else {
				c.rebuildRefcountSector(sectorBuf, sector)
			}
			problems = append(problems, f.problem)
		}
		setSectorChecksum(sectorBuf)
		if err := c.write(sectorBuf, offset, problems...); err != nil {
			return fmt.Errorf("refcount sector %d: %w", fixes[0].sector, err)
		}
		fixes = fixes[n:]
	}
	return nil
}

// rebuildRefcountSector sets every count in the sector from the references.
func (c *Checker) rebuildRefcountSector(buf []byte, sector uint64) {
	clear(buf)
	for i := uint64(0); i < RefcountEntriesPerSector; i++ {
		block := sector*RefcountEntriesPerSector + i
		if block >= c.sbData.TotalBlocks {
			break
		}
		if refs := c.inodeBlocks[block]; refs > 1 {
			binary.LittleEndian.PutUint32(buf[i*RefcountEntrySize:], uint32(refs))
		}
	}
}

// parseRefcountEntry extracts a refcount entry from raw bytes.
func parseRefcountEntry(buf []byte) *RefcountEntry {
	return &RefcountEntry{
		RefCount: binary.LittleEndian.Uint32(buf[0:4]),
		Flags:    binary.LittleEndian.Uint16(buf[4:6]),
		// Bytes 6-7 are reserved
	}
}
