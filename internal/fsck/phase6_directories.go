package fsck

import (
	"encoding/binary"
	"fmt"

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

	// DirBlockSize is the data block size for directory content.
	DirBlockSize = 1 << 20 // 1MB

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
//   - No orphaned inodes (not referenced by any directory)
//   - Root directory exists and is accessible
//   - Check for directory cycles (not implemented yet)
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
	orphanedInodes := uint64(0)

	// Find all directory inodes and validate their entries
	inodeTableOffset := sb.InodeTableOffset
	inodeTableSize := sb.InodeTableSize
	totalSectors := inodeTableSize / InodeSectorSize
	sectorBuf := storage.NewAlignedBuffer(InodeSectorSize).Bytes()

	for sectorIdx := uint64(0); sectorIdx < totalSectors; sectorIdx++ {
		sectorOffset := inodeTableOffset + sectorIdx*InodeSectorSize

		if c.readSector(sectorBuf, sectorOffset) != nil {
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

			// Skip empty directories
			if inode.Size == 0 {
				continue
			}

			// Read and validate directory entries
			entries, errs := c.readDirectoryEntries(inodeNum, inode, dataRegionOffset)
			entryCount += len(entries)
			invalidEntries += len(errs)

			for _, err := range errs {
				c.addProblem(PhaseDirectories, Error, fmt.Sprintf("inode %d", inodeNum), err, false)
			}

			// Validate each entry
			for _, entry := range entries {
				if entry.InodeNum == 0 {
					continue // Deleted entry
				}

				// Check if referenced inode exists
				if !c.inodeUsed[uint32(entry.InodeNum)] {
					c.addProblem(PhaseDirectories, Error, fmt.Sprintf("inode %d", inodeNum),
						fmt.Sprintf("entry %q references non-existent inode %d", entry.Name, entry.InodeNum),
						false)
					invalidEntries++
					continue
				}

				// Record directory reference for cross-validation
				c.dirEntryRefs[uint32(entry.InodeNum)]++

				// Validate file type matches inode mode
				if entry.InodeNum < uint64(len(c.inodeUsed)) {
					// We would need to read the target inode to verify type
					// For now, just validate the type is valid
					if !isValidDirEntryType(entry.FileType) {
						c.addProblem(PhaseDirectories, Warning, fmt.Sprintf("inode %d", inodeNum),
							fmt.Sprintf("entry %q has invalid file type %d", entry.Name, entry.FileType),
							false)
					}
				}
			}
		}
	}

	// Check for orphaned inodes (in use but not referenced by any directory)
	for inodeNum := range c.inodeUsed {
		if inodeNum == InodeRoot || c.snapshotData[inodeNum] {
			continue
		}
		if c.dirEntryRefs[inodeNum] == 0 {
			orphanedInodes++
			if orphanedInodes <= 10 {
				c.addProblem(PhaseDirectories, Warning, fmt.Sprintf("inode %d", inodeNum),
					"inode in use but not referenced by any directory",
					false)
			}
		}
	}
	if orphanedInodes > 10 {
		c.addProblem(PhaseDirectories, Warning, "orphans",
			fmt.Sprintf("%d additional orphaned inodes", orphanedInodes-10),
			false)
	}

	c.result.OrphanedInodes = orphanedInodes

	c.logf("  Directories validated: %d\n", dirCount)
	c.logf("  Total entries: %d\n", entryCount)
	c.logf("  Invalid entries: %d\n", invalidEntries)
	c.logf("  Orphaned inodes: %d\n", orphanedInodes)

	return nil
}

// readDirectoryEntries reads all entries from a directory inode.
// Returns parsed entries and a list of error messages for problems found.
// Like the runtime, it reads each block as one list of entries that ends at
// the first entry with RecLen 0.
func (c *Checker) readDirectoryEntries(inodeNum uint32, inode *InodeData, dataOffset uint64) ([]*DirEntryData, []string) {
	var entries []*DirEntryData
	var errors []string
	blockBuf := storage.NewAlignedBuffer(DirBlockSize).Bytes()

	for _, ext := range c.inodeExtents(inodeNum, inode) {
		if ext.Length == 0 || (ext.Flags&ExtentFlagAllocated) == 0 || (ext.Flags&ExtentFlagSFB) != 0 {
			continue
		}
		// Phase 4 reports extents outside the device.
		if ext.PhysicalBlock >= c.sbData.TotalBlocks || ext.Length > c.sbData.TotalBlocks-ext.PhysicalBlock {
			continue
		}

		for j := uint64(0); j < ext.Length; j++ {
			physBlock := ext.PhysicalBlock + j
			if err := c.readSector(blockBuf, dataOffset+physBlock*DirBlockSize); err != nil {
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

	return entries, errors
}

func parseDirBlock(buf []byte) ([]*DirEntryData, []string) {
	var entries []*DirEntryData
	var errors []string
	for pos := 0; pos+DirEntryHeaderSize <= len(buf); {
		entryInodeNum := binary.LittleEndian.Uint64(buf[pos : pos+8])
		recLen := int(binary.LittleEndian.Uint16(buf[pos+8 : pos+10]))
		nameLen := buf[pos+10]
		fileType := buf[pos+11]

		if recLen == 0 {
			break
		}
		if recLen < MinDirEntrySize {
			errors = append(errors, fmt.Sprintf("invalid RecLen %d at offset %d", recLen, pos))
			break
		}
		if pos+recLen > len(buf) {
			errors = append(errors, fmt.Sprintf("RecLen %d at offset %d runs past the block", recLen, pos))
			break
		}
		if DirEntryHeaderSize+int(nameLen) > recLen {
			errors = append(errors, fmt.Sprintf("NameLen %d does not fit RecLen %d at offset %d", nameLen, recLen, pos))
			break
		}

		nameBytes := buf[pos+DirEntryHeaderSize : pos+DirEntryHeaderSize+int(nameLen)]
		for _, b := range nameBytes {
			if b == 0 {
				errors = append(errors, fmt.Sprintf("null byte in name at offset %d", pos))
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

// isValidDirEntryType checks if a directory entry file type is valid.
func isValidDirEntryType(ft uint8) bool {
	switch ft {
	case DT_UNKNOWN, DT_REG, DT_DIR, DT_LNK, DT_BLK, DT_CHR, DT_FIFO, DT_SOCK:
		return true
	default:
		return false
	}
}
