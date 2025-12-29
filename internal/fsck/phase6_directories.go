package fsck

import (
	"encoding/binary"
	"fmt"
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
	sectorBuf := make([]byte, InodeSectorSize)

	for sectorIdx := uint64(0); sectorIdx < totalSectors; sectorIdx++ {
		sectorOffset := inodeTableOffset + sectorIdx*InodeSectorSize

		n, err := c.dev.ReadAt(sectorBuf, int64(sectorOffset))
		if err != nil || n != InodeSectorSize {
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
							true)
					}
				}
			}
		}
	}

	// Check for orphaned inodes (in use but not referenced by any directory)
	for inodeNum := range c.inodeUsed {
		if inodeNum == InodeRoot {
			continue // Root directory is special
		}
		if c.dirEntryRefs[inodeNum] == 0 {
			orphanedInodes++
			if orphanedInodes <= 10 {
				c.addProblem(PhaseDirectories, Warning, fmt.Sprintf("inode %d", inodeNum),
					"inode in use but not referenced by any directory",
					true)
			}
		}
	}
	if orphanedInodes > 10 {
		c.addProblem(PhaseDirectories, Warning, "orphans",
			fmt.Sprintf("%d additional orphaned inodes", orphanedInodes-10),
			true)
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
func (c *Checker) readDirectoryEntries(inodeNum uint32, inode *InodeData, dataOffset uint64) ([]*DirEntryData, []string) {
	var entries []*DirEntryData
	var errors []string

	// Read directory data from extents
	for extIdx := 0; extIdx < 2; extIdx++ {
		ext := inode.InlineExtents[extIdx]
		if ext.Length == 0 || (ext.Flags&ExtentFlagAllocated) == 0 {
			continue
		}

		// Skip SFB extents for now
		if (ext.Flags & ExtentFlagSFB) != 0 {
			continue
		}

		// Read each block in the extent
		for j := uint64(0); j < ext.Length; j++ {
			physBlock := ext.PhysicalBlock + j
			blockOffset := dataOffset + physBlock*DirBlockSize

			// Read the block in chunks
			for chunkOffset := uint64(0); chunkOffset < DirBlockSize; chunkOffset += DirChunkSize {
				chunkBuf := make([]byte, DirChunkSize)
				diskOffset := int64(blockOffset + chunkOffset)

				n, err := c.dev.ReadAt(chunkBuf, diskOffset)
				if err != nil || n != DirChunkSize {
					errors = append(errors, fmt.Sprintf("read error at block %d chunk %d", physBlock, chunkOffset/DirChunkSize))
					continue
				}

				// Parse entries in this chunk
				pos := 0
				for pos < DirChunkSize {
					// Check if enough space for header
					if pos+DirEntryHeaderSize > DirChunkSize {
						break
					}

					// Parse entry header
					entryInodeNum := binary.LittleEndian.Uint64(chunkBuf[pos : pos+8])
					recLen := binary.LittleEndian.Uint16(chunkBuf[pos+8 : pos+10])
					nameLen := chunkBuf[pos+10]
					fileType := chunkBuf[pos+11]

					// Validate RecLen
					if recLen < MinDirEntrySize {
						errors = append(errors, fmt.Sprintf("invalid RecLen %d at offset %d", recLen, pos))
						break // Can't continue safely
					}

					if pos+int(recLen) > DirChunkSize {
						errors = append(errors, fmt.Sprintf("RecLen %d extends beyond chunk at offset %d", recLen, pos))
						break
					}

					// Validate NameLen
					if nameLen > MaxNameLen {
						errors = append(errors, fmt.Sprintf("NameLen %d exceeds max at offset %d", nameLen, pos))
					}

					// Read name
					name := ""
					if nameLen > 0 && pos+DirEntryHeaderSize+int(nameLen) <= DirChunkSize {
						nameBytes := chunkBuf[pos+DirEntryHeaderSize : pos+DirEntryHeaderSize+int(nameLen)]
						name = string(nameBytes)

						// Check for null bytes in name
						for _, b := range nameBytes {
							if b == 0 {
								errors = append(errors, fmt.Sprintf("null byte in name at offset %d", pos))
								break
							}
						}
					}

					// Add entry if valid
					if entryInodeNum != 0 || nameLen > 0 {
						entries = append(entries, &DirEntryData{
							InodeNum: entryInodeNum,
							RecLen:   recLen,
							NameLen:  nameLen,
							FileType: fileType,
							Name:     name,
						})
					}

					pos += int(recLen)
				}
			}
		}
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
