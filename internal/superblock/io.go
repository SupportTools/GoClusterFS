package superblock

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// MaxNodes is the number of node slots. Each slot has its own journal.
	MaxNodes = 64

	// NodeJournalSize is the size of each node slot's journal region.
	NodeJournalSize = 4 << 20

	regionSize = 1 << 20
	inodeSize  = 256

	// MinDeviceSize holds all metadata regions and one data block.
	MinDeviceSize = 7*regionSize + MaxNodes*NodeJournalSize + storage.DefaultBlockSize

	legacyRootInodeOffset = 2*regionSize + 2*inodeSize
	legacyInodeMagic      = 0x494E4F44
)

var (
	ErrDeviceTooSmall = errors.New("device too small")
	ErrNotFormatted   = errors.New("no GoClusterFS superblock; format the device with -format")
	ErrLegacyLayout   = errors.New("device has an inode table but no GoClusterFS superblock: it has the pre-v2 layout, " +
		"where all nodes share one journal, or its superblock was destroyed. This version cannot mount it. " +
		"Copy the data off with the version that formatted it, then reformat with -format")
	ErrUnsupportedLayout = errors.New("superblock layout is not supported by this version")
)

// Read reads and validates the superblock from a device.
func Read(dev *storage.Device) (*Superblock, error) {
	buf := storage.GetBuffer(SuperblockSize)
	defer storage.PutBuffer(buf)

	if _, err := dev.ReadAt(buf.Bytes(), 0); err != nil {
		return nil, err
	}
	if string(buf.Bytes()[:len(Magic)]) != Magic {
		return nil, missingSuperblockError(dev)
	}

	sb, err := Deserialize(buf.Bytes())
	if err != nil {
		return nil, err
	}
	if err := sb.CanMount(); err != nil {
		return nil, err
	}
	if err := sb.checkLayout(dev.Size()); err != nil {
		return nil, err
	}
	return sb, nil
}

func missingSuperblockError(dev *storage.Device) error {
	buf := storage.GetBuffer(storage.SectorSize)
	defer storage.PutBuffer(buf)

	sector := int64(legacyRootInodeOffset) &^ (storage.SectorSize - 1)
	if _, err := dev.ReadAt(buf.Bytes(), sector); err != nil {
		return err
	}
	magic := binary.LittleEndian.Uint32(buf.Bytes()[legacyRootInodeOffset-sector:])
	if magic == legacyInodeMagic {
		return ErrLegacyLayout
	}
	return ErrNotFormatted
}

// checkLayout requires the exact layout this version formats; only the data size depends on the device.
func (sb *Superblock) checkLayout(deviceSize int64) error {
	want := New(deviceSize)
	switch {
	case sb.BlockSize != want.BlockSize || sb.MetaBlockSize != want.MetaBlockSize ||
		sb.InodeSize != want.InodeSize || sb.MaxNodes != want.MaxNodes || sb.InodeCount != want.InodeCount:
		return fmt.Errorf("%w: block size %d, metadata block %d, inode size %d, %d node slots, %d inodes",
			ErrUnsupportedLayout, sb.BlockSize, sb.MetaBlockSize, sb.InodeSize, sb.MaxNodes, sb.InodeCount)
	case sb.regions() != want.regions():
		return fmt.Errorf("%w: region offsets or sizes differ from layout v%d", ErrUnsupportedLayout, CurrentVersion)
	case sb.TotalBlocks == 0 || sb.TotalBlocks > want.TotalBlocks:
		return fmt.Errorf("%w: %d data blocks at offset %d do not fit the %d-byte device",
			ErrUnsupportedLayout, sb.TotalBlocks, sb.DataRegionOffset, deviceSize)
	}
	return nil
}

func (sb *Superblock) regions() [15]uint64 {
	return [15]uint64{
		sb.BlockBitmapOffset, sb.BlockBitmapSize, sb.InodeTableOffset, sb.InodeTableSize,
		sb.NodeRegistryOffset, sb.NodeRegistrySize, sb.LockBitmapOffset, sb.LockBitmapSize,
		sb.JournalRegionOffset, sb.JournalRegionSize, sb.RefCountTableOffset, sb.RefCountTableSize,
		sb.SnapshotRegOffset, sb.SnapshotRegSize, sb.DataRegionOffset,
	}
}

// JournalSlotSize returns the size of each node slot's journal region.
func (sb *Superblock) JournalSlotSize() uint64 {
	return sb.JournalRegionSize / uint64(sb.MaxNodes)
}

// Write writes the superblock to the device at offset 0.
// Updates LastWriteTime before writing.
func Write(dev *storage.Device, sb *Superblock) error {
	sb.LastWriteTime = time.Now().Unix()

	data := sb.Serialize()

	// Create aligned buffer and copy data
	buf := storage.GetBuffer(SuperblockSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), data)

	_, err := dev.WriteAt(buf.Bytes(), 0)
	if err != nil {
		return err
	}

	return dev.Sync()
}

// Format zeroes every metadata region and each node slot's journal header,
// then writes a new superblock. The superblock goes last, so an interrupted
// format leaves a device that mount refuses.
func Format(dev *storage.Device) (*Superblock, error) {
	if dev.Size() < MinDeviceSize {
		return nil, fmt.Errorf("%w: %d MB, need at least %d MB",
			ErrDeviceTooSmall, dev.Size()>>20, MinDeviceSize>>20)
	}
	sb := New(dev.Size())

	zero := storage.NewAlignedBuffer(regionSize)
	current := storage.NewAlignedBuffer(regionSize)
	// Writing only non-zero ranges keeps holes in sparse images, which O_DIRECT reads fast.
	zeroRange := func(offset uint64, size int) error {
		if _, err := dev.ReadAt(current.Bytes()[:size], int64(offset)); err != nil {
			return err
		}
		if isZero(current.Bytes()[:size]) {
			return nil
		}
		_, err := dev.WriteAt(zero.Bytes()[:size], int64(offset))
		return err
	}
	for _, offset := range []uint64{
		0, sb.BlockBitmapOffset, sb.InodeTableOffset, sb.NodeRegistryOffset,
		sb.LockBitmapOffset, sb.RefCountTableOffset, sb.SnapshotRegOffset,
	} {
		if err := zeroRange(offset, regionSize); err != nil {
			return nil, fmt.Errorf("zero metadata at %d: %w", offset, err)
		}
	}
	for slot := 0; slot < MaxNodes; slot++ {
		offset := sb.JournalRegionOffset + uint64(slot)*sb.JournalSlotSize()
		if err := zeroRange(offset, storage.SectorSize); err != nil {
			return nil, fmt.Errorf("zero journal header of slot %d: %w", slot, err)
		}
	}

	if err := dev.Sync(); err != nil {
		return nil, err
	}
	if err := Write(dev, sb); err != nil {
		return nil, err
	}
	return sb, nil
}

// New creates a new superblock with default values for the given device size.
// The caller should set additional fields as needed before writing.
func New(deviceSize int64) *Superblock {
	sb := &Superblock{
		Version:       CurrentVersion,
		FeatureFlags:  FeatureSparseFiles | FeatureLargeFiles,
		RequiredFlags: RequiredJournal,

		// Geometry
		BlockSize:     storage.DefaultBlockSize,  // 1MB
		MetaBlockSize: storage.MetadataBlockSize, // 4KB
		MaxNodes:      MaxNodes,
		InodeSize:     inodeSize,

		// Timestamps
		CreateTime: time.Now().Unix(),
	}

	// Set magic
	copy(sb.Magic[:], Magic)

	// Generate random UUID
	rand.Read(sb.UUID[:])

	calculateLayout(sb, deviceSize)

	return sb
}

func calculateLayout(sb *Superblock, deviceSize int64) {
	offset := uint64(regionSize)
	next := func(size uint64) (uint64, uint64) {
		start := offset
		offset += size
		return start, size
	}

	sb.BlockBitmapOffset, sb.BlockBitmapSize = next(regionSize)
	sb.InodeTableOffset, sb.InodeTableSize = next(regionSize)
	sb.NodeRegistryOffset, sb.NodeRegistrySize = next(regionSize)
	sb.LockBitmapOffset, sb.LockBitmapSize = next(regionSize)
	sb.JournalRegionOffset, sb.JournalRegionSize = next(MaxNodes * NodeJournalSize)
	sb.RefCountTableOffset, sb.RefCountTableSize = next(regionSize)
	sb.SnapshotRegOffset, sb.SnapshotRegSize = next(regionSize)
	sb.DataRegionOffset = offset

	sb.InodeCount = sb.InodeTableSize / inodeSize
	sb.FreeInodes = sb.InodeCount
	if uint64(deviceSize) > sb.DataRegionOffset {
		sb.TotalBlocks = (uint64(deviceSize) - sb.DataRegionOffset) / uint64(sb.BlockSize)
	}
	sb.FreeBlocks = sb.TotalBlocks
}

func isZero(b []byte) bool {
	return bytes.Count(b, []byte{0}) == len(b)
}
