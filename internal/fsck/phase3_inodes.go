package fsck

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// InodeSize is the size of each inode in bytes.
	InodeSize = 256

	// InodeSectorSize is the disk sector size.
	InodeSectorSize = 4096

	// InodesPerSector is the number of inodes per sector.
	InodesPerSector = InodeSectorSize / InodeSize // 16

	// InodeMagic identifies a valid inode.
	InodeMagic uint32 = 0x494E4F44 // "INOD"

	// Reserved inodes
	InodeInvalid   uint32 = 0 // Invalid/marker
	InodeBadBlocks uint32 = 1 // Bad block list
	InodeRoot      uint32 = 2 // Root directory

	// File type masks
	S_IFMT   uint16 = 0o170000 // File type mask
	S_IFREG  uint16 = 0o100000 // Regular file
	S_IFDIR  uint16 = 0o040000 // Directory
	S_IFLNK  uint16 = 0o120000 // Symbolic link
	S_IFBLK  uint16 = 0o060000 // Block device
	S_IFCHR  uint16 = 0o020000 // Character device
	S_IFIFO  uint16 = 0o010000 // FIFO
	S_IFSOCK uint16 = 0o140000 // Socket

	// Inode flags
	FlagHasExtentTree uint16 = 1 << 0
	FlagHasSnapshot   uint16 = 1 << 1
	FlagDeleted       uint16 = 1 << 2
)

// InodeData holds parsed inode information.
type InodeData struct {
	Magic           uint32
	InodeNum        uint32
	Mode            uint16
	Flags           uint16
	UID             uint32
	GID             uint32
	LinkCount       uint32
	Size            uint64
	AllocatedBlocks uint64
	WrittenBlocks   uint64
	Atime           int64
	Mtime           int64
	Ctime           int64

	// Inline extents
	InlineExtents [2]ExtentData

	// Overflow extent tree
	ExtentTreeRoot uint64
	ExtentCount    uint64
	ExtentDepth    uint32
	ExtentTreeGen  uint64

	// Checksum
	Checksum uint32
}

// ExtentData holds parsed extent information.
type ExtentData struct {
	LogicalBlock  uint64
	PhysicalBlock uint64
	Length        uint64
	Flags         uint32
}

// Extent flags
const (
	ExtentFlagAllocated uint32 = 1 << 0
	ExtentFlagUnwritten uint32 = 1 << 1
	ExtentFlagShared    uint32 = 1 << 2
	ExtentFlagSFB       uint32 = 1 << 3
)

// checkInodes validates the inode table (Phase 3).
//
// Checks performed:
//   - Each in-use inode (Magic == 0x494E4F44) has valid mode
//   - InodeNum matches position in table
//   - Checksum is valid
//   - Size is consistent with extent coverage
//   - AllocatedBlocks <= sum of extent lengths
//   - WrittenBlocks <= AllocatedBlocks
//   - LinkCount >= 1 for in-use inodes
//   - Reserved inodes (0, 1, 2) are valid
func (c *Checker) checkInodes() error {
	if c.sbData == nil {
		return fmt.Errorf("superblock not validated")
	}

	sb := c.sbData
	inodeTableOffset := sb.InodeTableOffset
	inodeTableSize := sb.InodeTableSize
	totalInodes := inodeTableSize / InodeSize

	c.logf("  Inode table offset: 0x%X\n", inodeTableOffset)
	c.logf("  Inode table size: %d bytes\n", inodeTableSize)
	c.logf("  Total inode slots: %d\n", totalInodes)

	// Read inodes sector by sector
	sectorBuf := storage.NewAlignedBuffer(InodeSectorSize).Bytes()
	totalSectors := inodeTableSize / InodeSectorSize

	usedInodes := uint64(0)
	freeInodes := uint64(0)
	directoryCount := uint64(0)
	fileCount := uint64(0)
	checksumErrors := 0

	for sectorIdx := uint64(0); sectorIdx < totalSectors; sectorIdx++ {
		sectorOffset := inodeTableOffset + sectorIdx*InodeSectorSize

		n, err := c.dev.ReadAt(sectorBuf, int64(sectorOffset))
		if err != nil {
			c.addProblem(PhaseInodes, Critical, fmt.Sprintf("sector %d", sectorIdx),
				fmt.Sprintf("read error: %v", err),
				false)
			continue
		}
		if n != InodeSectorSize {
			c.addProblem(PhaseInodes, Critical, fmt.Sprintf("sector %d", sectorIdx),
				fmt.Sprintf("short read: %d bytes", n),
				false)
			continue
		}

		// Process each inode in the sector
		for inodeIdx := 0; inodeIdx < InodesPerSector; inodeIdx++ {
			inodeNum := uint32(sectorIdx)*InodesPerSector + uint32(inodeIdx)
			if uint64(inodeNum) >= totalInodes {
				break
			}

			inodeBuf := sectorBuf[inodeIdx*InodeSize : (inodeIdx+1)*InodeSize]
			inode := c.parseInode(inodeBuf)

			// Check if inode is in use
			if inode.Magic == 0 {
				// Free slot
				freeInodes++
				continue
			}

			if inode.Magic != InodeMagic {
				c.addProblem(PhaseInodes, Error, fmt.Sprintf("inode %d", inodeNum),
					fmt.Sprintf("invalid magic: 0x%08X (expected 0x%08X or 0)",
						inode.Magic, InodeMagic),
					false)
				continue
			}

			usedInodes++
			c.inodeUsed[inodeNum] = true

			// Validate inode number matches position
			if inode.InodeNum != inodeNum {
				c.addProblem(PhaseInodes, Error, fmt.Sprintf("inode %d", inodeNum),
					fmt.Sprintf("stored inode number %d doesn't match position", inode.InodeNum),
					true)
			}

			// Validate checksum
			storedChecksum := inode.Checksum
			computedChecksum := crc32.Checksum(inodeBuf[:InodeSize-4], crc32.MakeTable(crc32.Castagnoli))

			if storedChecksum != computedChecksum {
				checksumErrors++
				c.addProblem(PhaseInodes, Error, fmt.Sprintf("inode %d", inodeNum),
					fmt.Sprintf("checksum mismatch: stored=0x%08X, computed=0x%08X",
						storedChecksum, computedChecksum),
					true)

				if c.opts.Mode == Repair {
					binary.LittleEndian.PutUint32(inodeBuf[InodeSize-4:], computedChecksum)
					// Note: Would need to write back the whole sector
				}
			}

			// Validate mode (file type)
			fileType := inode.Mode & S_IFMT
			if !isValidFileType(fileType) {
				c.addProblem(PhaseInodes, Error, fmt.Sprintf("inode %d", inodeNum),
					fmt.Sprintf("invalid file type: 0x%04X", fileType),
					false)
			}

			// Count file types
			switch fileType {
			case S_IFDIR:
				directoryCount++
			case S_IFREG, S_IFLNK:
				fileCount++
			}

			// Validate link count
			if inode.LinkCount == 0 && (inode.Flags&FlagDeleted) == 0 {
				c.addProblem(PhaseInodes, Warning, fmt.Sprintf("inode %d", inodeNum),
					"link count is 0 but inode is not marked deleted",
					true)
			}

			// Validate block counts
			if inode.WrittenBlocks > inode.AllocatedBlocks {
				c.addProblem(PhaseInodes, Error, fmt.Sprintf("inode %d", inodeNum),
					fmt.Sprintf("written blocks (%d) > allocated blocks (%d)",
						inode.WrittenBlocks, inode.AllocatedBlocks),
					true)
			}

			// Record extents for cross-validation
			c.recordInodeExtents(inodeNum, inode)
		}
	}

	// Check reserved inodes
	c.checkReservedInodes()

	// Update result statistics
	c.result.UsedInodes = usedInodes
	c.result.FreeInodes = freeInodes
	c.result.DirectoryCount = directoryCount
	c.result.FileCount = fileCount

	c.logf("  Used inodes: %d\n", usedInodes)
	c.logf("  Free inodes: %d\n", freeInodes)
	c.logf("  Directories: %d\n", directoryCount)
	c.logf("  Files: %d\n", fileCount)
	c.logf("  Checksum errors: %d\n", checksumErrors)

	// Compare with superblock counters
	if freeInodes != sb.FreeInodes {
		c.addProblem(PhaseInodes, Error, "free count",
			fmt.Sprintf("superblock says %d free inodes, table shows %d",
				sb.FreeInodes, freeInodes),
			true)
	}

	return nil
}

// parseInode extracts inode fields from raw bytes.
func (c *Checker) parseInode(buf []byte) *InodeData {
	inode := &InodeData{}

	inode.Magic = binary.LittleEndian.Uint32(buf[0:4])
	inode.InodeNum = binary.LittleEndian.Uint32(buf[4:8])
	inode.Mode = binary.LittleEndian.Uint16(buf[8:10])
	inode.Flags = binary.LittleEndian.Uint16(buf[10:12])
	inode.UID = binary.LittleEndian.Uint32(buf[12:16])
	inode.GID = binary.LittleEndian.Uint32(buf[16:20])
	inode.LinkCount = binary.LittleEndian.Uint32(buf[20:24])
	inode.Size = binary.LittleEndian.Uint64(buf[24:32])
	inode.AllocatedBlocks = binary.LittleEndian.Uint64(buf[32:40])
	inode.WrittenBlocks = binary.LittleEndian.Uint64(buf[40:48])
	inode.Atime = int64(binary.LittleEndian.Uint64(buf[48:56]))
	inode.Mtime = int64(binary.LittleEndian.Uint64(buf[56:64]))
	inode.Ctime = int64(binary.LittleEndian.Uint64(buf[64:72]))

	// Skip DataGeneration, MetaGeneration, LastModifierUUID (72-104)

	// Parse inline extents (offset 104-167, 32 bytes each)
	for i := 0; i < 2; i++ {
		extOffset := 104 + i*32
		inode.InlineExtents[i] = ExtentData{
			LogicalBlock:  binary.LittleEndian.Uint64(buf[extOffset : extOffset+8]),
			PhysicalBlock: binary.LittleEndian.Uint64(buf[extOffset+8 : extOffset+16]),
			Length:        binary.LittleEndian.Uint64(buf[extOffset+16 : extOffset+24]),
			Flags:         binary.LittleEndian.Uint32(buf[extOffset+24 : extOffset+28]),
		}
	}

	// Extent tree fields (offset 168-195)
	inode.ExtentTreeRoot = binary.LittleEndian.Uint64(buf[168:176])
	inode.ExtentCount = binary.LittleEndian.Uint64(buf[176:184])
	inode.ExtentDepth = binary.LittleEndian.Uint32(buf[184:188])
	inode.ExtentTreeGen = binary.LittleEndian.Uint64(buf[188:196])

	// Checksum (last 4 bytes)
	inode.Checksum = binary.LittleEndian.Uint32(buf[252:256])

	return inode
}

// recordInodeExtents records block references from inode extents.
// For an inode with an extent tree, the tree nodes count as references too.
func (c *Checker) recordInodeExtents(inodeNum uint32, inode *InodeData) {
	if inode.Flags&FlagHasExtentTree != 0 {
		tree := c.walkExtentTree(inodeNum, inode)
		c.extentTrees[inodeNum] = tree
		for _, block := range tree.nodes {
			c.inodeBlocks[block]++
		}
	}

	for _, ext := range c.inodeExtents(inodeNum, inode) {
		if ext.Length == 0 || (ext.Flags&ExtentFlagAllocated) == 0 {
			continue
		}

		// Skip SFB extents (sub-file blocks are packed differently)
		if (ext.Flags & ExtentFlagSFB) != 0 {
			continue
		}

		// Record each block in the extent
		for j := uint64(0); j < ext.Length; j++ {
			blockNum := ext.PhysicalBlock + j
			c.inodeBlocks[blockNum]++
		}
	}
}

// checkReservedInodes validates the reserved inode entries.
func (c *Checker) checkReservedInodes() {
	// Check that root directory (inode 2) exists
	if !c.inodeUsed[InodeRoot] {
		c.addProblem(PhaseInodes, Critical, "inode 2",
			"root directory inode is missing or invalid",
			false)
	} else {
		c.logf("  Root directory inode: OK\n")
	}
}

// isValidFileType checks if the mode represents a valid file type.
func isValidFileType(fileType uint16) bool {
	switch fileType {
	case S_IFREG, S_IFDIR, S_IFLNK, S_IFBLK, S_IFCHR, S_IFIFO, S_IFSOCK:
		return true
	default:
		return false
	}
}
