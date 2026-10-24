package superblock

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// MaxNodes is the number of node slots. Each slot has its own journal.
	MaxNodes = 64

	// NodeJournalSize is the size of each node slot's journal region.
	NodeJournalSize = 4 << 20

	// DefaultBytesPerInode gives one inode per data block, since every non-empty file takes at least one block.
	DefaultBytesPerInode = storage.DefaultBlockSize

	// MinInodes and MaxInodes bound the inode table to 1 MB and 4 GB.
	MinInodes = regionSize / inodeSize
	MaxInodes = 1 << 24

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

type region struct {
	name         string
	offset, size uint64
	need         uint64
	exact        bool
}

// layout lists the regions after the superblock in disk order, each with the size it needs.
func (sb *Superblock) layout() []region {
	return []region{
		{"node registry", sb.NodeRegistryOffset, sb.NodeRegistrySize, regionSize, true},
		{"lock records", sb.LockBitmapOffset, sb.LockBitmapSize, regionSize, true},
		{"snapshot registry", sb.SnapshotRegOffset, sb.SnapshotRegSize, regionSize, true},
		{"journals", sb.JournalRegionOffset, sb.JournalRegionSize, MaxNodes * NodeJournalSize, true},
		{"block bitmap", sb.BlockBitmapOffset, sb.BlockBitmapSize, BlockBitmapBytes(sb.TotalBlocks), false},
		{"refcount table", sb.RefCountTableOffset, sb.RefCountTableSize, RefCountTableBytes(sb.TotalBlocks), false},
		{"inode table", sb.InodeTableOffset, sb.InodeTableSize, sb.InodeCount * inodeSize, true},
	}
}

// checkLayout requires the v3 region order with every region big enough for the
// volume's data blocks and inodes. The data blocks may end before the device does.
func (sb *Superblock) checkLayout(deviceSize int64) error {
	if sb.BlockSize != storage.DefaultBlockSize || sb.MetaBlockSize != storage.MetadataBlockSize ||
		sb.InodeSize != inodeSize || sb.MaxNodes != MaxNodes || sb.InodeCount < MinInodes || sb.InodeCount > MaxInodes {
		return fmt.Errorf("%w: block size %d, metadata block %d, inode size %d, %d node slots, %d inodes",
			ErrUnsupportedLayout, sb.BlockSize, sb.MetaBlockSize, sb.InodeSize, sb.MaxNodes, sb.InodeCount)
	}
	offset := uint64(regionSize)
	for _, r := range sb.layout() {
		if r.offset != offset || r.size%regionSize != 0 || r.size < r.need || (r.exact && r.size != alignRegion(r.need)) {
			return fmt.Errorf("%w: %s has %d bytes at offset %d, want %d bytes or more at offset %d",
				ErrUnsupportedLayout, r.name, r.size, r.offset, alignRegion(r.need), offset)
		}
		offset += r.size
	}
	if sb.DataRegionOffset != offset {
		return fmt.Errorf("%w: data blocks start at %d, want %d", ErrUnsupportedLayout, sb.DataRegionOffset, offset)
	}
	if sb.TotalBlocks == 0 || uint64(deviceSize) < offset ||
		sb.TotalBlocks > (uint64(deviceSize)-offset)/uint64(sb.BlockSize) {
		return fmt.Errorf("%w: %d data blocks at offset %d do not fit the %d-byte device",
			ErrUnsupportedLayout, sb.TotalBlocks, offset, deviceSize)
	}
	return nil
}

// BlockBitmapBytes is the bitmap size, in whole sectors, that tracks this many data blocks.
func BlockBitmapBytes(blocks uint64) uint64 {
	return (blocks + bitmap.BitsPerSector - 1) / bitmap.BitsPerSector * storage.SectorSize
}

// RefCountTableBytes is the refcount table size, in whole sectors, that covers this many data blocks.
func RefCountTableBytes(blocks uint64) uint64 {
	return (blocks + refcount.EntriesPerSector - 1) / refcount.EntriesPerSector * storage.SectorSize
}

func alignRegion(size uint64) uint64 {
	return max(size+regionSize-1, regionSize) / regionSize * regionSize
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
// format leaves a device that mount refuses. bytesPerInode sets the inode
// count; 0 means DefaultBytesPerInode.
func Format(dev *storage.Device, bytesPerInode uint64) (*Superblock, error) {
	if dev.Size() < MinDeviceSize {
		return nil, fmt.Errorf("%w: %d MB, need at least %d MB",
			ErrDeviceTooSmall, dev.Size()>>20, MinDeviceSize>>20)
	}
	sb := New(dev.Size(), bytesPerInode)

	zero := storage.NewAlignedBuffer(regionSize)
	current := storage.NewAlignedBuffer(regionSize)
	// Writing only non-zero ranges keeps holes in sparse images, which O_DIRECT reads fast.
	zeroRange := func(offset, size uint64) error {
		for end := offset + size; offset < end; offset += regionSize {
			chunk := min(end-offset, regionSize)
			if _, err := dev.ReadAt(current.Bytes()[:chunk], int64(offset)); err != nil {
				return err
			}
			if isZero(current.Bytes()[:chunk]) {
				continue
			}
			if _, err := dev.WriteAt(zero.Bytes()[:chunk], int64(offset)); err != nil {
				return err
			}
		}
		return nil
	}
	if err := zeroRange(0, regionSize); err != nil {
		return nil, fmt.Errorf("zero superblock region: %w", err)
	}
	for _, r := range sb.layout() {
		if r.offset == sb.JournalRegionOffset {
			continue
		}
		if err := zeroRange(r.offset, r.size); err != nil {
			return nil, fmt.Errorf("zero %s at %d: %w", r.name, r.offset, err)
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
// bytesPerInode sets the inode count; 0 means DefaultBytesPerInode.
func New(deviceSize int64, bytesPerInode uint64) *Superblock {
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

	if bytesPerInode == 0 {
		bytesPerInode = DefaultBytesPerInode
	}
	calculateLayout(sb, deviceSize, bytesPerInode)

	return sb
}

// calculateLayout puts the fixed-size regions first, so their offsets never
// change, then the regions sized from the device: the bitmap and refcount
// table cover every block the device could hold, and the inode table gets
// one inode per bytesPerInode of device.
func calculateLayout(sb *Superblock, deviceSize int64, bytesPerInode uint64) {
	maxBlocks := uint64(deviceSize) / uint64(sb.BlockSize)
	inodes := min(max(uint64(deviceSize)/bytesPerInode, MinInodes), MaxInodes)

	offset := uint64(regionSize)
	next := func(size uint64) (uint64, uint64) {
		start := offset
		offset += alignRegion(size)
		return start, alignRegion(size)
	}

	sb.NodeRegistryOffset, sb.NodeRegistrySize = next(regionSize)
	sb.LockBitmapOffset, sb.LockBitmapSize = next(regionSize)
	sb.SnapshotRegOffset, sb.SnapshotRegSize = next(regionSize)
	sb.JournalRegionOffset, sb.JournalRegionSize = next(MaxNodes * NodeJournalSize)
	sb.BlockBitmapOffset, sb.BlockBitmapSize = next(BlockBitmapBytes(maxBlocks))
	sb.RefCountTableOffset, sb.RefCountTableSize = next(RefCountTableBytes(maxBlocks))
	sb.InodeTableOffset, sb.InodeTableSize = next(inodes * inodeSize)
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
