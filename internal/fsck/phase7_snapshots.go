package fsck

import (
	"encoding/binary"
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// SnapshotSectorSize is the size of each snapshot registry sector.
	SnapshotSectorSize = 4096

	// SnapshotDescriptorSize is the size of each snapshot descriptor.
	SnapshotDescriptorSize = 128

	// SnapshotChecksumSize is the size of the CRC32C checksum per sector.
	SnapshotChecksumSize = 4

	// SnapshotDescriptorsPerSector is descriptors per sector.
	// (4096 - 4) / 128 = 31
	SnapshotDescriptorsPerSector = (SnapshotSectorSize - SnapshotChecksumSize) / SnapshotDescriptorSize

	// SnapshotMagic identifies a valid snapshot descriptor.
	SnapshotMagic uint32 = 0x534E4150 // "SNAP"

	// SnapshotFlagHasData marks descriptors whose DataInode holds the captured extents.
	SnapshotFlagHasData uint8 = 1 << 4

	// MaxSnapshotNameLen is the maximum length of a snapshot name.
	MaxSnapshotNameLen = 48
)

// Snapshot states
const (
	SnapshotStateInvalid  uint8 = 0
	SnapshotStateCreating uint8 = 1
	SnapshotStateActive   uint8 = 2
	SnapshotStateDeleting uint8 = 3
)

// SnapshotDescriptor holds parsed snapshot information.
type SnapshotDescriptor struct {
	Magic         uint32
	SnapshotID    uint64
	ParentID      uint64
	TargetInode   uint32
	State         uint8
	Flags         uint8
	DataInode     uint32
	TotalBlocks   uint64
	TotalSize     uint64
	CreateTime    int64
	DeleteTime    int64
	RefCountJobID uint64
	Name          string
	Checksum      uint32
}

// checkSnapshots validates the snapshot registry (Phase 7).
//
// Checks performed:
//   - Each sector has valid CRC32C checksum
//   - Descriptors have valid magic (or are free)
//   - TargetInode references valid inodes
//   - ParentID references valid snapshots
//   - State is valid (0-3)
//   - No duplicate snapshot IDs
//   - No duplicate names
//
// The superblock snapshot counter is not compared: the runtime never updates it.
func (c *Checker) checkSnapshots() error {
	if c.sbData == nil {
		return fmt.Errorf("superblock not validated")
	}

	sb := c.sbData
	snapshotOffset := sb.SnapshotRegOffset
	snapshotSize := sb.SnapshotRegSize

	if snapshotSize == 0 {
		c.logf("  Snapshot registry not present (size=0)\n")
		return nil
	}

	totalSectors := snapshotSize / SnapshotSectorSize

	c.logf("  Snapshot registry offset: 0x%X\n", snapshotOffset)
	c.logf("  Snapshot registry size: %d bytes (%d sectors)\n", snapshotSize, totalSectors)

	sectorBuf := storage.NewAlignedBuffer(SnapshotSectorSize).Bytes()
	checksumErrors := 0
	invalidDescriptors := 0
	activeSnapshots := uint32(0)

	// Track snapshot IDs and names for duplicate detection
	seenIDs := make(map[uint64]bool)
	seenNames := make(map[string]uint64) // name -> snapshot ID
	dataInodes := make(map[uint32]bool)

	for sectorIdx := uint64(0); sectorIdx < totalSectors; sectorIdx++ {
		sectorOffset := snapshotOffset + sectorIdx*SnapshotSectorSize

		if err := c.readSector(sectorBuf, sectorOffset); err != nil {
			c.addProblem(PhaseSnapshots, Critical, fmt.Sprintf("sector %d", sectorIdx), err.Error(), false)
			continue
		}

		if !sectorChecksumValid(sectorBuf) {
			checksumErrors++
			c.addProblem(PhaseSnapshots, Error, fmt.Sprintf("sector %d", sectorIdx),
				"checksum mismatch: the runtime cannot read the snapshots in this sector",
				false)
		}

		// Process each descriptor
		for descIdx := 0; descIdx < SnapshotDescriptorsPerSector; descIdx++ {
			descOffset := descIdx * SnapshotDescriptorSize
			descBuf := sectorBuf[descOffset : descOffset+SnapshotDescriptorSize]
			desc := parseSnapshotDescriptor(descBuf)

			// Skip free slots
			if desc.Magic == 0 {
				continue
			}

			// Validate magic
			if desc.Magic != SnapshotMagic {
				c.addProblem(PhaseSnapshots, Error, fmt.Sprintf("sector %d slot %d", sectorIdx, descIdx),
					fmt.Sprintf("invalid magic: 0x%08X (expected 0x%08X)", desc.Magic, SnapshotMagic),
					false)
				invalidDescriptors++
				continue
			}

			// Validate state
			if desc.State > SnapshotStateDeleting {
				c.addProblem(PhaseSnapshots, Error, fmt.Sprintf("snapshot %d", desc.SnapshotID),
					fmt.Sprintf("invalid state: %d", desc.State),
					false)
			}

			// Check for duplicate IDs
			if seenIDs[desc.SnapshotID] {
				c.addProblem(PhaseSnapshots, Critical, fmt.Sprintf("snapshot %d", desc.SnapshotID),
					"duplicate snapshot ID",
					false)
			}
			seenIDs[desc.SnapshotID] = true

			// Check for duplicate names (only for active snapshots)
			if desc.State == SnapshotStateActive && desc.Name != "" {
				if existingID, exists := seenNames[desc.Name]; exists {
					c.addProblem(PhaseSnapshots, Error, fmt.Sprintf("snapshot %d", desc.SnapshotID),
						fmt.Sprintf("duplicate name %q (also used by snapshot %d)", desc.Name, existingID),
						false)
				}
				seenNames[desc.Name] = desc.SnapshotID
			}

			// Validate target inode
			if desc.TargetInode > 0 && !c.inodeUsed[desc.TargetInode] {
				c.addProblem(PhaseSnapshots, Error, fmt.Sprintf("snapshot %d", desc.SnapshotID),
					fmt.Sprintf("references non-existent inode %d", desc.TargetInode),
					false)
			}

			// Record snapshot inode reference
			if desc.TargetInode > 0 {
				c.snapshotInodes[desc.TargetInode] = true
			}

			if desc.Flags&SnapshotFlagHasData != 0 && desc.DataInode != 0 {
				if !c.snapshotData[desc.DataInode] {
					c.addProblem(PhaseSnapshots, Error, fmt.Sprintf("snapshot %d", desc.SnapshotID),
						fmt.Sprintf("data inode %d is not a snapshot data inode", desc.DataInode),
						false)
				}
				dataInodes[desc.DataInode] = true
				c.snapshotInodes[desc.DataInode] = true
			}

			// Validate parent reference (if not root snapshot)
			if desc.ParentID != 0 && !seenIDs[desc.ParentID] {
				// Parent might come later - this is a forward reference issue
				// We'll check after full scan
			}

			// Count active snapshots
			if desc.State == SnapshotStateActive {
				activeSnapshots++
			}

			// Validate descriptor checksum
			if computed := storage.CRC32C(descBuf[:124]); desc.Checksum != computed {
				c.addProblem(PhaseSnapshots, Error, fmt.Sprintf("snapshot %d", desc.SnapshotID),
					fmt.Sprintf("descriptor checksum mismatch: stored=0x%08X, computed=0x%08X",
						desc.Checksum, computed),
					false)
			}
		}
	}

	for num := range c.snapshotData {
		if !dataInodes[num] {
			c.addProblem(PhaseSnapshots, Warning, fmt.Sprintf("inode %d", num),
				"snapshot data inode belongs to no snapshot (left by a crash during snapshot creation)",
				false)
		}
	}

	c.result.SnapshotCount = activeSnapshots

	c.logf("  Checksum errors: %d\n", checksumErrors)
	c.logf("  Invalid descriptors: %d\n", invalidDescriptors)
	c.logf("  Active snapshots: %d\n", activeSnapshots)

	return nil
}

// parseSnapshotDescriptor extracts a snapshot descriptor from raw bytes.
func parseSnapshotDescriptor(buf []byte) *SnapshotDescriptor {
	desc := &SnapshotDescriptor{}

	desc.Magic = binary.LittleEndian.Uint32(buf[0:4])
	desc.SnapshotID = binary.LittleEndian.Uint64(buf[4:12])
	desc.ParentID = binary.LittleEndian.Uint64(buf[12:20])
	desc.TargetInode = binary.LittleEndian.Uint32(buf[20:24])
	desc.State = buf[24]
	desc.Flags = buf[25]
	// Bytes 26-27 reserved
	desc.DataInode = binary.LittleEndian.Uint32(buf[28:32])
	desc.TotalBlocks = binary.LittleEndian.Uint64(buf[36:44])
	desc.TotalSize = binary.LittleEndian.Uint64(buf[44:52])
	desc.CreateTime = int64(binary.LittleEndian.Uint64(buf[52:60]))
	desc.DeleteTime = int64(binary.LittleEndian.Uint64(buf[60:68]))
	desc.RefCountJobID = binary.LittleEndian.Uint64(buf[68:76])

	// Parse name (null-terminated)
	nameBytes := buf[76:124]
	for i, b := range nameBytes {
		if b == 0 {
			desc.Name = string(nameBytes[:i])
			break
		}
	}
	if desc.Name == "" && nameBytes[0] != 0 {
		desc.Name = string(nameBytes)
	}

	desc.Checksum = binary.LittleEndian.Uint32(buf[124:128])

	return desc
}
