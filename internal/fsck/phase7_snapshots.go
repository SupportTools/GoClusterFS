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
	// SnapshotFlagHasTargetBirth marks descriptors that record the birth generation of the file they were taken of.
	SnapshotFlagHasTargetBirth uint8 = 1 << 5

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
	TargetBirth   uint64
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
	targets := make(map[uint64]uint32)   // snapshot ID -> the inode it was taken of
	var children []*SnapshotDescriptor
	var fixes []snapshotSectorFix

	for sectorIdx := uint64(0); sectorIdx < totalSectors; sectorIdx++ {
		sectorOffset := snapshotOffset + sectorIdx*SnapshotSectorSize

		if err := c.readSector(sectorBuf, sectorOffset); err != nil {
			c.addProblem(PhaseSnapshots, Critical, fmt.Sprintf("sector %d", sectorIdx), err.Error(), false)
			c.snapsIncomplete = append(c.snapsIncomplete, fmt.Sprintf("snapshot registry sector %d is unreadable", sectorIdx))
			continue
		}

		// A sector that fails its checksum blocks every snapshot create, so
		// repair rewrites it with the descriptors that pass their own checksum.
		var fix *snapshotSectorFix
		lost := func(msg string) (string, bool) {
			if fix == nil {
				return msg, false
			}
			return msg + "; repair clears the slot, so the snapshot in it is lost, and the next repair frees any data it kept", true
		}
		if !sectorChecksumValid(sectorBuf) {
			checksumErrors++
			c.addProblem(PhaseSnapshots, Error, fmt.Sprintf("sector %d", sectorIdx),
				"checksum mismatch: the runtime cannot read the snapshots in this sector and refuses every snapshot create",
				true)
			c.snapsIncomplete = append(c.snapsIncomplete, fmt.Sprintf("snapshot registry sector %d fails its checksum", sectorIdx))
			fixes = append(fixes, snapshotSectorFix{offset: sectorOffset, sector: rebuiltSnapshotSector(sectorBuf), problems: []int{c.lastProblem()}})
			fix = &fixes[len(fixes)-1]
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
				msg, repairable := lost(fmt.Sprintf("invalid magic: 0x%08X (expected 0x%08X)", desc.Magic, SnapshotMagic))
				c.addProblem(PhaseSnapshots, Error, fmt.Sprintf("sector %d slot %d", sectorIdx, descIdx), msg, repairable)
				if repairable {
					fix.problems = append(fix.problems, c.lastProblem())
				}
				invalidDescriptors++
				c.snapsIncomplete = append(c.snapsIncomplete, fmt.Sprintf("snapshot registry sector %d slot %d is damaged", sectorIdx, descIdx))
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

			if why := c.snapshotOrphaned(desc); why != "" {
				c.addProblem(PhaseSnapshots, Warning, fmt.Sprintf("snapshot %d", desc.SnapshotID),
					fmt.Sprintf("snapshot %q is an orphan: %s; only the API's full listing shows it, and it can be deleted there", desc.Name, why),
					false)
			}

			if desc.TargetInode > 0 && c.mayHoldTarget(desc) {
				c.snapshotTargets[desc.TargetInode] = true
			}

			if desc.Flags&SnapshotFlagHasData != 0 && desc.DataInode != 0 {
				if !c.snapshotData[desc.DataInode] {
					c.addProblem(PhaseSnapshots, Error, fmt.Sprintf("snapshot %d", desc.SnapshotID),
						fmt.Sprintf("data inode %d is not a snapshot data inode", desc.DataInode),
						false)
				}
				c.snapshotRefs[desc.DataInode] = true
			}

			if desc.ParentID != 0 {
				children = append(children, desc)
			}
			targets[desc.SnapshotID] = desc.TargetInode

			// Count active snapshots
			if desc.State == SnapshotStateActive {
				activeSnapshots++
			}

			// Validate descriptor checksum
			if computed := storage.CRC32C(descBuf[:124]); desc.Checksum != computed {
				msg, repairable := lost(fmt.Sprintf("descriptor checksum mismatch: stored=0x%08X, computed=0x%08X; it reads as %q in sector %d slot %d",
					desc.Checksum, computed, desc.Name, sectorIdx, descIdx))
				c.addProblem(PhaseSnapshots, Error, fmt.Sprintf("snapshot %d", desc.SnapshotID), msg, repairable)
				if repairable {
					fix.problems = append(fix.problems, c.lastProblem())
				}
			}
		}
	}

	if c.opts.Mode == Repair && len(fixes) > 0 {
		if err := c.writeSnapshotSectors(fixes); err != nil {
			return err
		}
	}

	c.checkSnapshotParents(children, targets)

	for num := range c.snapshotData {
		if !c.snapshotRefs[num] {
			c.addProblem(PhaseSnapshots, Warning, fmt.Sprintf("inode %d", num),
				"snapshot data inode belongs to no snapshot, as a crash while a snapshot is created or deleted leaves it",
				true)
			c.orphanData[num] = c.lastProblem()
		}
	}

	c.result.SnapshotCount = activeSnapshots

	c.logf("  Checksum errors: %d\n", checksumErrors)
	c.logf("  Invalid descriptors: %d\n", invalidDescriptors)
	c.logf("  Active snapshots: %d\n", activeSnapshots)

	return nil
}

type snapshotSectorFix struct {
	offset   uint64
	sector   []byte
	problems []int
}

// rebuiltSnapshotSector returns a copy of a registry sector that keeps the
// descriptors passing their own checksum, clears every other slot, and has a
// valid sector checksum.
func rebuiltSnapshotSector(buf []byte) []byte {
	out := storage.NewAlignedBuffer(SnapshotSectorSize).Bytes()
	for i := 0; i < SnapshotDescriptorsPerSector; i++ {
		desc := buf[i*SnapshotDescriptorSize : (i+1)*SnapshotDescriptorSize]
		if binary.LittleEndian.Uint32(desc) == SnapshotMagic && binary.LittleEndian.Uint32(desc[124:]) == storage.CRC32C(desc[:124]) {
			copy(out[i*SnapshotDescriptorSize:], desc)
		}
	}
	setSectorChecksum(out)
	return out
}

func (c *Checker) writeSnapshotSectors(fixes []snapshotSectorFix) error {
	if err := c.beforeWrites(); err != nil {
		return err
	}
	for _, f := range fixes {
		if err := c.write(f.sector, f.offset, f.problems...); err != nil {
			return fmt.Errorf("snapshot registry at 0x%X: %w", f.offset, err)
		}
	}
	if c.opts.DryRun {
		return nil
	}
	return c.dev.Sync()
}

// checkSnapshotParents reports snapshots whose parent is missing, is the
// snapshot itself, or was taken of another file. A delete re-parents the
// deleted snapshot's children to its parent.
func (c *Checker) checkSnapshotParents(children []*SnapshotDescriptor, targets map[uint64]uint32) {
	for _, desc := range children {
		loc := fmt.Sprintf("snapshot %d", desc.SnapshotID)
		parentTarget, found := targets[desc.ParentID]
		switch {
		case desc.ParentID == desc.SnapshotID:
			c.addProblem(PhaseSnapshots, Warning, loc, fmt.Sprintf("snapshot %q is its own parent", desc.Name), false)
		case !found && len(c.snapsIncomplete) == 0:
			c.addProblem(PhaseSnapshots, Warning, loc,
				fmt.Sprintf("snapshot %q has parent snapshot %d, which does not exist", desc.Name, desc.ParentID), false)
		case found && parentTarget != desc.TargetInode:
			c.addProblem(PhaseSnapshots, Warning, loc,
				fmt.Sprintf("snapshot %q of inode %d has parent snapshot %d, which was taken of inode %d", desc.Name, desc.TargetInode, desc.ParentID, parentTarget), false)
		}
	}
}

// snapshotOrphaned says why desc belongs to no existing file, or returns "".
func (c *Checker) snapshotOrphaned(desc *SnapshotDescriptor) string {
	switch {
	case !c.inodeUsed[desc.TargetInode]:
		return fmt.Sprintf("inode %d, the file it was taken of, is free", desc.TargetInode)
	case desc.Flags&SnapshotFlagHasTargetBirth == 0:
		return "an older version wrote it without the birth generation that proves which file it was taken of"
	case !c.sameBirth(desc):
		return fmt.Sprintf("inode %d now holds a later file than the one it was taken of", desc.TargetInode)
	}
	return ""
}

// mayHoldTarget reports whether desc's target slot may still hold the file desc was taken of. Without a birth
// generation fsck cannot tell.
func (c *Checker) mayHoldTarget(desc *SnapshotDescriptor) bool {
	return desc.Flags&SnapshotFlagHasTargetBirth == 0 || c.sameBirth(desc)
}

func (c *Checker) sameBirth(desc *SnapshotDescriptor) bool {
	return desc.TargetBirth == c.inodeBirth[desc.TargetInode]&(1<<48-1)
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
	desc.DataInode = binary.LittleEndian.Uint32(buf[28:32])
	if desc.Flags&SnapshotFlagHasTargetBirth != 0 {
		desc.TargetBirth = uint64(binary.LittleEndian.Uint16(buf[26:28]))<<32 | uint64(binary.LittleEndian.Uint32(buf[32:36]))
	}
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
