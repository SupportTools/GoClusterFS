package fsck

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// SuperblockSize is the fixed size of the superblock on disk.
const SuperblockSize = 4096

// Expected magic value for GoClusterFS.
var expectedMagic = [12]byte{'G', 'O', 'C', 'L', 'U', 'S', 'T', 'E', 'R', 'F', 'S', 0}

// checkSuperblock validates the filesystem superblock (Phase 1).
//
// Checks performed:
//   - Magic number is "GOCLUSTERFS\0"
//   - Version is supported
//   - RequiredFlags are all understood
//   - BlockSize is 1MB
//   - All region offsets are 4KB-aligned
//   - All region sizes are multiples of 4KB
//   - No region overlaps
//   - Journal region splits into one journal per node registry slot
//   - All regions fit within device
//   - Checksum (CRC32C) is valid
func (c *Checker) checkSuperblock() error {
	// Read superblock from device
	buf := make([]byte, SuperblockSize)
	n, err := c.dev.ReadAt(buf, 0)
	if err != nil {
		return fmt.Errorf("failed to read superblock: %w", err)
	}
	if n != SuperblockSize {
		return fmt.Errorf("short read: got %d bytes, expected %d", n, SuperblockSize)
	}

	// Parse raw fields
	sb := c.parseSuperblock(buf)
	c.sbData = sb

	// Check magic
	if sb.Magic != expectedMagic {
		c.addProblem(PhaseSuperblock, Fatal, "offset 0",
			fmt.Sprintf("invalid magic: got %q, expected %q", sb.Magic, expectedMagic),
			false)
		return fmt.Errorf("invalid superblock magic")
	}
	c.logf("  Magic: OK\n")

	// Check version
	if sb.Version < superblock.MinSupportedVersion || sb.Version > superblock.CurrentVersion {
		c.addProblem(PhaseSuperblock, Fatal, "offset 12",
			fmt.Sprintf("unsupported version: %d (supported: %d-%d)",
				sb.Version, superblock.MinSupportedVersion, superblock.CurrentVersion),
			false)
		return fmt.Errorf("unsupported superblock version")
	}
	c.logf("  Version: %d (OK)\n", sb.Version)

	// Check required flags
	unknownFlags := sb.RequiredFlags &^ superblock.FlagsWeSupport
	if unknownFlags != 0 {
		c.addProblem(PhaseSuperblock, Fatal, "offset 40",
			fmt.Sprintf("unknown required flags: 0x%X", unknownFlags),
			false)
		return fmt.Errorf("unknown required flags")
	}
	c.logf("  Required Flags: 0x%X (OK)\n", sb.RequiredFlags)

	// Check block size
	if sb.BlockSize != 1048576 { // 1MB
		c.addProblem(PhaseSuperblock, Fatal, "offset 48",
			fmt.Sprintf("invalid block size: %d (expected 1MB = 1048576)", sb.BlockSize),
			false)
		return fmt.Errorf("invalid block size")
	}
	c.logf("  Block Size: %d (OK)\n", sb.BlockSize)

	// Get device size
	devSize := uint64(c.dev.Size())
	c.logf("  Device Size: %d bytes\n", devSize)

	// Validate region offsets and sizes
	if err := c.validateRegions(sb, devSize); err != nil {
		return err
	}
	c.checkJournalSlots(sb)

	// Validate checksum
	checksumOffset := SuperblockSize - 4
	storedChecksum := binary.LittleEndian.Uint32(buf[checksumOffset:])
	computedChecksum := crc32.Checksum(buf[:checksumOffset], crc32.MakeTable(crc32.Castagnoli))

	if storedChecksum != computedChecksum {
		c.addProblem(PhaseSuperblock, Critical, "offset 4092",
			fmt.Sprintf("checksum mismatch: stored=0x%08X, computed=0x%08X",
				storedChecksum, computedChecksum),
			true) // Repairable by recalculating

		if c.opts.Mode == Repair {
			// Write correct checksum
			binary.LittleEndian.PutUint32(buf[checksumOffset:], computedChecksum)
			if _, err := c.dev.WriteAt(buf, 0); err != nil {
				return fmt.Errorf("failed to repair checksum: %w", err)
			}
			c.result.Problems[len(c.result.Problems)-1].Repaired = true
			c.result.RepairedCount++
			c.logf("  Checksum: REPAIRED\n")
		}
	} else {
		c.logf("  Checksum: 0x%08X (OK)\n", storedChecksum)
	}

	// Store statistics for result
	c.result.TotalBlocks = sb.TotalBlocks
	c.result.TotalInodes = sb.InodeTableSize / uint64(sb.InodeSize)
	c.result.FreeBlocks = sb.FreeBlocks
	c.result.FreeInodes = sb.FreeInodes
	c.result.SnapshotCount = sb.SnapshotCount

	c.logf("  Total Blocks: %d\n", sb.TotalBlocks)
	c.logf("  Total Inodes: %d\n", c.result.TotalInodes)

	return nil
}

// parseSuperblock extracts fields from raw superblock bytes.
func (c *Checker) parseSuperblock(buf []byte) *SuperblockData {
	sb := &SuperblockData{}

	// Identity (offset 0-31)
	copy(sb.Magic[:], buf[0:12])
	sb.Version = binary.LittleEndian.Uint32(buf[12:16])
	copy(sb.UUID[:], buf[16:32])

	// Feature flags (offset 32-47)
	sb.FeatureFlags = binary.LittleEndian.Uint64(buf[32:40])
	sb.RequiredFlags = binary.LittleEndian.Uint64(buf[40:48])

	// Geometry (offset 48-79)
	sb.BlockSize = binary.LittleEndian.Uint32(buf[48:52])
	sb.TotalBlocks = binary.LittleEndian.Uint64(buf[52:60])
	sb.MetaBlockSize = binary.LittleEndian.Uint32(buf[60:64])
	sb.MaxNodes = binary.LittleEndian.Uint32(buf[64:68])
	sb.InodeSize = binary.LittleEndian.Uint32(buf[68:72])

	// Region offsets (offset 80-143)
	sb.NodeRegistryOffset = binary.LittleEndian.Uint64(buf[80:88])
	sb.JournalRegionOffset = binary.LittleEndian.Uint64(buf[88:96])
	sb.LockBitmapOffset = binary.LittleEndian.Uint64(buf[96:104])
	sb.InodeTableOffset = binary.LittleEndian.Uint64(buf[104:112])
	sb.BlockBitmapOffset = binary.LittleEndian.Uint64(buf[112:120])
	sb.RefCountTableOffset = binary.LittleEndian.Uint64(buf[120:128])
	sb.SnapshotRegOffset = binary.LittleEndian.Uint64(buf[128:136])
	sb.DataRegionOffset = binary.LittleEndian.Uint64(buf[136:144])

	// Region sizes (offset 144-207)
	sb.NodeRegistrySize = binary.LittleEndian.Uint64(buf[144:152])
	sb.JournalRegionSize = binary.LittleEndian.Uint64(buf[152:160])
	sb.LockBitmapSize = binary.LittleEndian.Uint64(buf[160:168])
	sb.InodeTableSize = binary.LittleEndian.Uint64(buf[168:176])
	sb.BlockBitmapSize = binary.LittleEndian.Uint64(buf[176:184])
	sb.RefCountTableSize = binary.LittleEndian.Uint64(buf[184:192])
	sb.SnapshotRegSize = binary.LittleEndian.Uint64(buf[192:200])
	// DataRegionSize is implied by TotalBlocks * BlockSize

	// Counters (offset 208-239)
	sb.InodeCount = binary.LittleEndian.Uint64(buf[208:216])
	sb.FreeInodes = binary.LittleEndian.Uint64(buf[216:224])
	sb.FreeBlocks = binary.LittleEndian.Uint64(buf[224:232])
	sb.SnapshotCount = binary.LittleEndian.Uint32(buf[232:236])
	sb.MountCount = binary.LittleEndian.Uint32(buf[236:240])

	// Timestamps (offset 240-263)
	sb.CreateTime = int64(binary.LittleEndian.Uint64(buf[240:248]))
	sb.LastMountTime = int64(binary.LittleEndian.Uint64(buf[248:256]))
	sb.LastWriteTime = int64(binary.LittleEndian.Uint64(buf[256:264]))

	return sb
}

// validateRegions checks that all region offsets and sizes are valid.
func (c *Checker) validateRegions(sb *SuperblockData, devSize uint64) error {
	const align = 4096 // 4KB alignment

	type region struct {
		name   string
		offset uint64
		size   uint64
	}

	regions := []region{
		{"superblock", 0, SuperblockSize},
		{"node_registry", sb.NodeRegistryOffset, sb.NodeRegistrySize},
		{"journal_region", sb.JournalRegionOffset, sb.JournalRegionSize},
		{"lock_bitmap", sb.LockBitmapOffset, sb.LockBitmapSize},
		{"inode_table", sb.InodeTableOffset, sb.InodeTableSize},
		{"block_bitmap", sb.BlockBitmapOffset, sb.BlockBitmapSize},
		{"refcount_table", sb.RefCountTableOffset, sb.RefCountTableSize},
		{"snapshot_registry", sb.SnapshotRegOffset, sb.SnapshotRegSize},
		{"data_region", sb.DataRegionOffset, sb.TotalBlocks * uint64(sb.BlockSize)},
	}

	// Check alignment
	for _, r := range regions[1:] { // Skip superblock (always at 0)
		if r.offset%align != 0 {
			c.addProblem(PhaseSuperblock, Error, r.name,
				fmt.Sprintf("offset 0x%X is not 4KB-aligned", r.offset),
				false)
		}
		if r.size%align != 0 && r.size > 0 {
			c.addProblem(PhaseSuperblock, Warning, r.name,
				fmt.Sprintf("size 0x%X is not 4KB-aligned", r.size),
				false)
		}
	}

	// Check bounds
	for _, r := range regions {
		end := r.offset + r.size
		if end > devSize {
			c.addProblem(PhaseSuperblock, Critical, r.name,
				fmt.Sprintf("extends beyond device: 0x%X+0x%X = 0x%X > 0x%X",
					r.offset, r.size, end, devSize),
				false)
		}
	}

	// Check for overlaps
	for i, r1 := range regions {
		for j, r2 := range regions {
			if i >= j {
				continue
			}
			if r1.size == 0 || r2.size == 0 {
				continue
			}

			// Check if r1 and r2 overlap
			r1End := r1.offset + r1.size
			r2End := r2.offset + r2.size
			if r1.offset < r2End && r2.offset < r1End {
				c.addProblem(PhaseSuperblock, Critical, "regions",
					fmt.Sprintf("%s [0x%X-0x%X] overlaps with %s [0x%X-0x%X]",
						r1.name, r1.offset, r1End,
						r2.name, r2.offset, r2End),
					false)
			}
		}
	}

	c.logf("  Region validation: complete\n")
	return nil
}

// checkJournalSlots validates that the journal region splits into one journal
// per node registry slot.
func (c *Checker) checkJournalSlots(sb *SuperblockData) {
	if sb.MaxNodes == 0 || sb.JournalRegionSize%uint64(sb.MaxNodes) != 0 {
		c.addProblem(PhaseSuperblock, Critical, "journal_region",
			fmt.Sprintf("%d bytes do not split into %d node journals", sb.JournalRegionSize, sb.MaxNodes),
			false)
		return
	}
	if slots := sb.NodeRegistrySize / node.SlotSize; uint64(sb.MaxNodes) > slots {
		c.addProblem(PhaseSuperblock, Critical, "node_registry",
			fmt.Sprintf("%d node journals but the registry holds %d node slots", sb.MaxNodes, slots),
			false)
	}
	slotSize := sb.JournalRegionSize / uint64(sb.MaxNodes)
	if slotSize%storage.SectorSize != 0 || slotSize < journal.JournalDataOffset+storage.SectorSize {
		c.addProblem(PhaseSuperblock, Critical, "journal_region",
			fmt.Sprintf("node journal size %d is not a multiple of %d bytes of at least %d",
				slotSize, storage.SectorSize, journal.JournalDataOffset+storage.SectorSize),
			false)
		return
	}
	c.logf("  Node journals: %d x %d bytes (OK)\n", sb.MaxNodes, slotSize)
}

// ValidateSuperblockQuick does a quick superblock validation without full fsck.
// Returns nil if superblock is valid, error otherwise.
func ValidateSuperblockQuick(data []byte) error {
	if len(data) < SuperblockSize {
		return fmt.Errorf("data too short: %d bytes", len(data))
	}

	// Check magic
	var magic [12]byte
	copy(magic[:], data[0:12])
	if !bytes.Equal(magic[:], expectedMagic[:]) {
		return fmt.Errorf("invalid magic: %q", magic)
	}

	// Check version
	version := binary.LittleEndian.Uint32(data[12:16])
	if version < superblock.MinSupportedVersion || version > superblock.CurrentVersion {
		return fmt.Errorf("unsupported version: %d", version)
	}

	// Check checksum
	checksumOffset := SuperblockSize - 4
	storedChecksum := binary.LittleEndian.Uint32(data[checksumOffset:])
	computedChecksum := crc32.Checksum(data[:checksumOffset], crc32.MakeTable(crc32.Castagnoli))
	if storedChecksum != computedChecksum {
		return fmt.Errorf("checksum mismatch: stored=0x%08X, computed=0x%08X",
			storedChecksum, computedChecksum)
	}

	return nil
}
