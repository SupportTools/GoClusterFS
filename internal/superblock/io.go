package superblock

import (
	"crypto/rand"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// Read reads and validates the superblock from a device.
// Returns the superblock and any error encountered.
func Read(dev *storage.Device) (*Superblock, error) {
	buf := storage.GetBuffer(SuperblockSize)
	defer storage.PutBuffer(buf)

	_, err := dev.ReadAt(buf.Bytes(), 0)
	if err != nil {
		return nil, err
	}

	sb, err := Deserialize(buf.Bytes())
	if err != nil {
		return nil, err
	}

	// Validate we can mount this version
	if err := sb.CanMount(); err != nil {
		return nil, err
	}

	return sb, nil
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
		MaxNodes:      64,
		InodeSize:     256,

		// Timestamps
		CreateTime: time.Now().Unix(),
	}

	// Set magic
	copy(sb.Magic[:], Magic)

	// Generate random UUID
	rand.Read(sb.UUID[:])

	// Calculate region layout based on device size
	calculateLayout(sb, deviceSize)

	return sb
}

// calculateLayout computes region offsets and sizes based on device size.
// Layout order: Superblock | NodeRegistry | Journal | LockBitmap | InodeTable | BlockBitmap | RefCount | Snapshot | Data
func calculateLayout(sb *Superblock, deviceSize int64) {
	const (
		nodeRegistrySize = 1 << 20 // 1MB for node registry + heartbeats
		journalPerNode   = 4 << 20 // 4MB per node
		maxInodes        = 1 << 20 // 1M inodes max
		inodeSize        = 256     // bytes per inode
	)

	// Calculate data blocks (reserve ~1% for metadata, minimum)
	totalBlocks := uint64(deviceSize) / uint64(sb.BlockSize)

	// Region offsets (sequential layout)
	offset := uint64(SuperblockSize) // Start after superblock

	// Node Registry (1MB)
	sb.NodeRegistryOffset = offset
	sb.NodeRegistrySize = nodeRegistrySize
	offset += sb.NodeRegistrySize

	// Journal Region (4MB * MaxNodes = 256MB for 64 nodes)
	sb.JournalRegionOffset = offset
	sb.JournalRegionSize = uint64(sb.MaxNodes) * journalPerNode
	offset += sb.JournalRegionSize

	// Lock Bitmap - 1 bit per potential lock, rounded to 4KB
	// Estimate: 1M potential locks = 128KB
	sb.LockBitmapOffset = offset
	sb.LockBitmapSize = alignUp(128*1024, uint64(sb.MetaBlockSize))
	offset += sb.LockBitmapSize

	// Inode Table
	sb.InodeTableOffset = offset
	sb.InodeTableSize = alignUp(maxInodes*inodeSize, uint64(sb.MetaBlockSize))
	sb.InodeCount = maxInodes
	sb.FreeInodes = maxInodes - 1 // Reserve inode 0
	offset += sb.InodeTableSize

	// Block Bitmap - 1 bit per data block
	bitmapBits := totalBlocks
	bitmapBytes := (bitmapBits + 7) / 8
	sb.BlockBitmapOffset = offset
	sb.BlockBitmapSize = alignUp(bitmapBytes, uint64(sb.MetaBlockSize))
	offset += sb.BlockBitmapSize

	// RefCount Table - 8 bytes per data block (for snapshots)
	sb.RefCountTableOffset = offset
	sb.RefCountTableSize = alignUp(totalBlocks*8, uint64(sb.MetaBlockSize))
	offset += sb.RefCountTableSize

	// Snapshot Registry - fixed size for snapshot descriptors
	sb.SnapshotRegOffset = offset
	sb.SnapshotRegSize = alignUp(1<<20, uint64(sb.MetaBlockSize)) // 1MB
	offset += sb.SnapshotRegSize

	// Data Region - everything else
	sb.DataRegionOffset = offset

	// Recalculate actual data blocks available
	dataBytes := uint64(deviceSize) - offset
	sb.TotalBlocks = dataBytes / uint64(sb.BlockSize)
	sb.FreeBlocks = sb.TotalBlocks
}

// alignUp rounds n up to the next multiple of alignment.
func alignUp(n, alignment uint64) uint64 {
	return (n + alignment - 1) &^ (alignment - 1)
}
