package fsck

import (
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// DirEntryHeaderSize is the fixed portion of a directory entry.
	DirEntryHeaderSize = 12

	// MinDirEntrySize is the minimum valid entry size.
	MinDirEntrySize = 16

	// MaxNameLen is the maximum filename length.
	MaxNameLen = 255

	// DirChunkSize is the directory chunk size (4KB).
	DirChunkSize = 4096

	// Directory entry file types (POSIX DT_* values)
	DT_UNKNOWN = 0
	DT_REG     = 1
	DT_DIR     = 2
	DT_LNK     = 7
	DT_BLK     = 6
	DT_CHR     = 3
	DT_FIFO    = 5
	DT_SOCK    = 12
)

// DirEntryData holds parsed directory entry information.
type DirEntryData struct {
	InodeNum uint64
	RecLen   uint16
	NameLen  uint8
	FileType uint8
	Name     string
}

// checkDirectories validates directory structure (Phase 6).
//
// Checks performed:
//   - All directory entries have valid RecLen values
//   - NameLen does not exceed MaxNameLen
//   - Entry names don't contain null bytes
//   - Referenced inodes exist and are in use
//   - File type in entry matches inode mode
//   - Root directory exists and is accessible
//   - The directory tree: one name per directory, "..", link counts, and no loops
//
// It also finds the inodes other than root that no directory names, which
// phase 8 reports. "." and ".." are not names.
func (c *Checker) checkDirectories() error {
	if c.sbData == nil {
		return fmt.Errorf("superblock not validated")
	}

	sb := c.sbData
	dataRegionOffset := sb.DataRegionOffset

	c.logf("  Data region offset: 0x%X\n", dataRegionOffset)

	dirCount := 0
	entryCount := 0
	invalidEntries := 0
	tree := dirTree{dot: map[uint32]uint64{}, dotdot: map[uint32]uint32{}, parents: map[uint32][]uint32{}, subdirs: map[uint32]int{}, links: map[uint32]uint32{}}
	totalInodes := sb.InodeTableSize / InodeSize

	// Find all directory inodes and validate their entries
	totalSectors := sb.InodeTableSize / InodeSectorSize
	sectors := c.inodeSectors()

	for sectorIdx := uint64(0); sectorIdx < totalSectors; sectorIdx++ {
		sectorBuf, err := sectors.at(sectorIdx)
		if err != nil {
			c.namesIncomplete = append(c.namesIncomplete, fmt.Sprintf("inode table sector %d is unreadable", sectorIdx))
			continue
		}

		for inodeIdx := 0; inodeIdx < InodesPerSector; inodeIdx++ {
			inodeNum := uint32(sectorIdx)*InodesPerSector + uint32(inodeIdx)
			if !c.inodeUsed[inodeNum] {
				continue
			}

			inodeBuf := sectorBuf[inodeIdx*InodeSize : (inodeIdx+1)*InodeSize]
			inode := c.parseInode(inodeBuf)

			// Check if this is a directory
			fileType := inode.Mode & S_IFMT
			if fileType != S_IFDIR {
				continue
			}

			dirCount++
			tree.links[inodeNum] = inode.LinkCount

			// Skip empty directories
			if inode.Size == 0 {
				continue
			}

			// Read and validate directory entries
			entries, errs, complete := c.readDirectoryEntries(inodeNum, inode, dataRegionOffset)
			entryCount += len(entries)
			invalidEntries += len(errs)
			if len(errs) > 0 || !complete {
				c.namesIncomplete = append(c.namesIncomplete, fmt.Sprintf("directory inode %d is not fully readable", inodeNum))
			}

			for _, err := range errs {
				c.addProblem(PhaseDirectories, Error, fmt.Sprintf("inode %d", inodeNum), err, false)
			}

			// Validate each entry
			seen := map[string]bool{}
			for _, entry := range entries {
				if entry.InodeNum == 0 {
					continue // Deleted entry
				}
				if seen[entry.Name] {
					c.addProblem(PhaseDirectories, Error, fmt.Sprintf("inode %d", inodeNum),
						fmt.Sprintf("holds the name %q more than once; a lookup finds only one of them", entry.Name), false)
					continue
				}
				seen[entry.Name] = true
				if entry.Name == "." {
					tree.dot[inodeNum] = entry.InodeNum
				}

				if entry.InodeNum >= totalInodes {
					c.addProblem(PhaseDirectories, Error, fmt.Sprintf("inode %d", inodeNum),
						fmt.Sprintf("entry %q references inode %d, past the inode table's %d inodes", entry.Name, entry.InodeNum, totalInodes),
						false)
					invalidEntries++
					continue
				}
				target := uint32(entry.InodeNum)

				// Check if referenced inode exists
				if !c.inodeUsed[target] {
					c.addProblem(PhaseDirectories, Error, fmt.Sprintf("inode %d", inodeNum),
						fmt.Sprintf("entry %q references non-existent inode %d", entry.Name, entry.InodeNum),
						false)
					invalidEntries++
					continue
				}

				switch entry.Name {
				case "..":
					tree.dotdot[inodeNum] = target
				case ".":
				default:
					c.dirEntryRefs[target]++
					c.dirNames[inodeNum]++
					c.noteEntryProject(inodeNum, inode, entry.Name, target)
					if c.entryType[target] == DT_DIR {
						tree.parents[target] = append(tree.parents[target], inodeNum)
						tree.subdirs[inodeNum]++
					}
				}

				// Phase 3 reports an inode whose mode has no valid type.
				if want := c.entryType[target]; want != DT_UNKNOWN && entry.FileType != want {
					c.addProblem(PhaseDirectories, Error, fmt.Sprintf("inode %d", inodeNum),
						fmt.Sprintf("entry %q has file type %d, but inode %d has type %d: lookup, unlink and rmdir go by the entry's type",
							entry.Name, entry.FileType, entry.InodeNum, want),
						false)
				}
			}
		}
	}

	for _, inodeNum := range slices.Sorted(maps.Keys(c.inodeUsed)) {
		if inodeNum != InodeRoot && c.dirEntryRefs[inodeNum] == 0 {
			c.unnamed = append(c.unnamed, inodeNum)
		}
	}
	if len(c.namesIncomplete) == 0 {
		if err := c.checkDirectoryTree(tree); err != nil {
			return err
		}
	} else {
		c.logf("  Directory tree not checked: %s\n", c.namesIncomplete[0])
	}

	c.logf("  Directories validated: %d\n", dirCount)
	c.logf("  Total entries: %d\n", entryCount)
	c.logf("  Invalid entries: %d\n", invalidEntries)
	c.logf("  Inodes without a name, snapshot data included: %d\n", len(c.unnamed))

	return nil
}

// readDirectoryEntries reads all entries from a directory inode.
// Returns parsed entries, a list of error messages for problems found, and
// whether it read every extent the runtime may read entries from.
func (c *Checker) readDirectoryEntries(inodeNum uint32, inode *InodeData, dataOffset uint64) ([]*DirEntryData, []string, bool) {
	var entries []*DirEntryData
	var errors []string
	complete := true
	blockSize := uint64(c.sbData.BlockSize)
	blockBuf := storage.NewAlignedBuffer(int(blockSize)).Bytes()

	for _, ext := range c.inodeExtents(inodeNum, inode) {
		if ext.Length == 0 {
			continue
		}
		// Phase 4 reports extents outside the device.
		if (ext.Flags&ExtentFlagAllocated) == 0 || (ext.Flags&ExtentFlagSFB) != 0 || c.onDevice(ext) < ext.Length {
			complete = false
			continue
		}

		for j := uint64(0); j < ext.Length; j++ {
			physBlock := ext.PhysicalBlock + j
			if err := c.readSector(blockBuf, dataOffset+physBlock*blockSize); err != nil {
				errors = append(errors, fmt.Sprintf("block %d: %v", physBlock, err))
				continue
			}
			blockEntries, blockErrs := parseDirBlock(blockBuf)
			entries = append(entries, blockEntries...)
			for _, e := range blockErrs {
				errors = append(errors, fmt.Sprintf("block %d: %s", physBlock, e))
			}
		}
	}

	return entries, errors, complete
}

// parseDirBlock parses the entries of every 4 KB chunk of a directory block.
// A chunk is unused (RecLen 0 at its start) or its record lengths add up to
// exactly the chunk, so a chain that stops short or runs past it hides or invents entries.
func parseDirBlock(buf []byte) ([]*DirEntryData, []string) {
	var entries []*DirEntryData
	var errors []string
	for start := 0; start+DirEntryHeaderSize <= len(buf); start += DirChunkSize {
		chunkEntries, chunkErrs := parseDirChunk(buf[start:min(len(buf), start+DirChunkSize)], start)
		entries = append(entries, chunkEntries...)
		errors = append(errors, chunkErrs...)
	}
	return entries, errors
}

func parseDirChunk(chunk []byte, base int) ([]*DirEntryData, []string) {
	var entries []*DirEntryData
	var errors []string
	end := len(chunk)
	pos := 0
	for pos < end {
		at := base + pos
		if pos+DirEntryHeaderSize > end {
			errors = append(errors, fmt.Sprintf("entry at offset %d runs past the %d-byte chunk", at, DirChunkSize))
			break
		}
		entryInodeNum := binary.LittleEndian.Uint64(chunk[pos : pos+8])
		recLen := int(binary.LittleEndian.Uint16(chunk[pos+8 : pos+10]))
		nameLen := chunk[pos+10]
		fileType := chunk[pos+11]

		if recLen == 0 {
			if pos > 0 {
				errors = append(errors, fmt.Sprintf("entries end at offset %d, before the end of the %d-byte chunk", at, DirChunkSize))
			}
			break
		}
		if recLen < MinDirEntrySize {
			errors = append(errors, fmt.Sprintf("invalid RecLen %d at offset %d", recLen, at))
			break
		}
		if pos+recLen > end {
			errors = append(errors, fmt.Sprintf("RecLen %d at offset %d runs past the %d-byte chunk", recLen, at, DirChunkSize))
			break
		}
		if DirEntryHeaderSize+int(nameLen) > recLen {
			errors = append(errors, fmt.Sprintf("NameLen %d does not fit RecLen %d at offset %d", nameLen, recLen, at))
			break
		}

		nameBytes := chunk[pos+DirEntryHeaderSize : pos+DirEntryHeaderSize+int(nameLen)]
		for _, b := range nameBytes {
			if b == 0 {
				errors = append(errors, fmt.Sprintf("null byte in name at offset %d", at))
				break
			}
		}

		if entryInodeNum != 0 || nameLen > 0 {
			entries = append(entries, &DirEntryData{
				InodeNum: entryInodeNum,
				RecLen:   uint16(recLen),
				NameLen:  nameLen,
				FileType: fileType,
				Name:     string(nameBytes),
			})
		}
		pos += recLen
	}
	return entries, errors
}

// dirTree is what phase 6 read of every directory: its "." and "..", the directories that name it, its
// subdirectories and its link count.
type dirTree struct {
	dot     map[uint32]uint64
	dotdot  map[uint32]uint32
	parents map[uint32][]uint32
	subdirs map[uint32]int
	links   map[uint32]uint32
}

// checkDirectoryTree reports a directory that has other than one name, whose "." does not name itself, whose ".."
// does not name the directory that names it, whose link count is not 2 plus its subdirectories, or from which ".."
// never reaches the root. Repair points a wrong "." at the directory itself and a wrong ".." at the parent found by
// walking the names down from the root.
func (c *Checker) checkDirectoryTree(t dirTree) error {
	dirs := slices.Sorted(maps.Keys(t.links))
	parent := t.parentsFromRoot()
	var fixes []dotFix
	wrong := func(d uint32, name string, cur uint64, want uint32, found bool, msg string) {
		c.addProblem(PhaseDirectories, Error, fmt.Sprintf("inode %d", d), msg, found)
		if found {
			fixes = append(fixes, dotFix{dir: d, name: name, cur: cur, want: want, problem: c.lastProblem()})
		}
	}
	for _, d := range dirs {
		loc := fmt.Sprintf("inode %d", d)
		if want := 2 + uint32(t.subdirs[d]); t.links[d] != want {
			c.addProblem(PhaseDirectories, Warning, loc, fmt.Sprintf("directory has %d links, want %d: 2 plus one per subdirectory", t.links[d], want), false)
		}
		if dot, ok := t.dot[d]; !ok {
			c.addProblem(PhaseDirectories, Error, loc, "directory has no \".\"", false)
		} else if dot != uint64(d) {
			wrong(d, ".", dot, d, true, fmt.Sprintf("\".\" names inode %d, not directory %d itself", dot, d))
		}
		dots, hasDots := t.dotdot[d]
		p, reached := parent[d]
		switch ps := t.parents[d]; {
		case d == InodeRoot:
			if dots != InodeRoot {
				wrong(d, "..", uint64(dots), InodeRoot, hasDots, fmt.Sprintf("\"..\" of the root names inode %d, not the root", dots))
			}
		case len(ps) > 1:
			c.addProblem(PhaseDirectories, Error, loc, fmt.Sprintf("directory has %d names, in directories %v; a directory has exactly one", len(ps), ps), false)
		case len(ps) == 1 && !hasDots:
			c.addProblem(PhaseDirectories, Error, loc, "directory has no \"..\"", false)
		case len(ps) == 1 && dots != ps[0]:
			wrong(d, "..", uint64(dots), p, reached, fmt.Sprintf("\"..\" names inode %d, but directory %d names it", dots, ps[0]))
		}
	}

	reaches, cut := map[uint32]bool{InodeRoot: true}, map[uint32]bool{}
	for _, d := range dirs {
		var path []uint32
		at := map[uint32]int{}
		cur := d
		for !reaches[cur] && !cut[cur] {
			if i, looped := at[cur]; looped {
				c.addProblem(PhaseDirectories, Error, fmt.Sprintf("inode %d", cur), fmt.Sprintf("\"..\" never reaches the root: it loops through directories %v", path[i:]), false)
				break
			}
			at[cur] = len(path)
			path = append(path, cur)
			next, ok := t.dotdot[cur]
			if _, isDir := t.links[next]; !ok || !isDir {
				break
			}
			cur = next
		}
		for _, p := range path {
			reaches[p], cut[p] = reaches[cur], !reaches[cur]
		}
	}
	if c.opts.Mode != Repair || len(fixes) == 0 {
		return nil
	}
	return c.repairDots(fixes)
}

// parentsFromRoot walks the names down from the root and returns, for each directory it reaches, the directory that
// names it. It does not go through a directory with more than one name.
func (t dirTree) parentsFromRoot() map[uint32]uint32 {
	children := map[uint32][]uint32{}
	for d, ps := range t.parents {
		if len(ps) == 1 {
			children[ps[0]] = append(children[ps[0]], d)
		}
	}
	parent := map[uint32]uint32{}
	for queue := []uint32{InodeRoot}; len(queue) > 0; queue = queue[1:] {
		for _, d := range children[queue[0]] {
			if _, seen := parent[d]; !seen && d != InodeRoot {
				parent[d] = queue[0]
				queue = append(queue, d)
			}
		}
	}
	return parent
}

type dotFix struct {
	dir     uint32
	name    string
	cur     uint64
	want    uint32
	problem int
}

// repairDots points each wrong "." or ".." at want, if it still names the inode phase 6 read.
func (c *Checker) repairDots(fixes []dotFix) error {
	if err := c.beforeWrites(); err != nil {
		return err
	}
	if c.opts.DryRun {
		for _, f := range fixes {
			c.markRepaired(f.problem)
		}
		return nil
	}
	sb := c.sbData
	var uuid [16]byte
	tbl := inode.NewTable(c.dev, sb.InodeTableOffset, sb.InodeTableSize, nil, uuid)
	mgr := extent.NewManager(c.dev, bitmap.NewRegion(c.dev, sb.BlockBitmapOffset, sb.TotalBlocks, nil, uuid), nil, sb.DataRegionOffset, uint64(sb.BlockSize))
	mgr.SetRefcounts(refcount.NewRegion(c.dev, sb.RefCountTableOffset, sb.RefCountTableSize, sb.TotalBlocks, nil))
	tbl.SetTreeFlusher(mgr)
	dirs := dir.NewManager(c.dev, tbl, mgr, nil, sb.DataRegionOffset)
	for _, f := range fixes {
		ino, err := tbl.Read(f.dir)
		if err != nil {
			return fmt.Errorf("directory %d: %w", f.dir, err)
		}
		pointed, err := dirs.PointIf(context.Background(), ino, f.name, uint64(f.want), dir.DT_DIR, func(cur uint64) bool { return cur == f.cur })
		if err != nil {
			return fmt.Errorf("directory %d %q: %w", f.dir, f.name, err)
		}
		if pointed {
			c.markRepaired(f.problem)
		}
	}
	return c.dev.Sync()
}
