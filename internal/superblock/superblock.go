// Package superblock defines the GoClusterFS superblock format.
//
// The superblock is the first structure read when mounting. It contains
// volume identity, geometry, and pointers to all other metadata regions.
// It is always located at byte offset 0 and is exactly 4KB.
package superblock

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	// SuperblockSize is the fixed size of the superblock (4KB, sector-aligned).
	SuperblockSize = 4096

	// Magic identifies a valid GoClusterFS superblock.
	Magic = "GOCLUSTERFS"

	// CurrentVersion is the format version this code produces.
	// Version 2 gives each node slot its own journal region.
	CurrentVersion uint32 = 2

	// MinSupportedVersion is the oldest format we can read.
	MinSupportedVersion uint32 = 2
)

// Feature flags - optional features (can mount without understanding)
const (
	FeatureSparseFiles   uint64 = 1 << 0 // Sparse file support
	FeatureExtendedAttrs uint64 = 1 << 1 // Extended attributes
	FeatureLargeFiles    uint64 = 1 << 2 // Files > 2GB
)

// Required flags - must understand to mount safely
const (
	RequiredJournal    uint64 = 1 << 0 // Journaling enabled
	RequiredSnapshots  uint64 = 1 << 1 // Snapshot structures present
	RequiredEncryption uint64 = 1 << 2 // Encryption enabled (future)
	RequiredCompressed uint64 = 1 << 3 // Compression enabled (future)
)

// FlagsWeSupport defines which required flags this version understands.
// Update this as new features are implemented.
var FlagsWeSupport uint64 = RequiredJournal | RequiredSnapshots

// Errors
var (
	ErrInvalidMagic     = errors.New("invalid superblock magic")
	ErrNewerVersion     = errors.New("superblock version newer than supported - upgrade GoClusterFS")
	ErrTooOldVersion    = errors.New("superblock version too old - reformat with -format")
	ErrChecksumMismatch = errors.New("superblock checksum mismatch - possible corruption")
	ErrUnknownRequired  = errors.New("superblock requires features this version doesn't support")
)

// Superblock is the on-disk superblock structure.
// Total size: 4096 bytes (4KB), sector-aligned for O_DIRECT.
type Superblock struct {
	// Identity (offset 0-31)
	Magic   [12]byte // "GOCLUSTERFS\0"
	Version uint32   // Format version
	UUID    [16]byte // Volume UUID

	// Feature flags (offset 32-47)
	FeatureFlags  uint64 // Optional features
	RequiredFlags uint64 // Required features (must understand to mount)

	// Geometry (offset 48-79)
	BlockSize     uint32 // Data block size (default 1MB)
	TotalBlocks   uint64 // Total data blocks
	MetaBlockSize uint32 // Metadata block size (4KB)
	MaxNodes      uint32 // Maximum cluster nodes (default 64)
	InodeSize     uint32 // Inode size in bytes (256)

	// Region offsets in bytes (offset 80-143)
	NodeRegistryOffset  uint64 // Node registry + heartbeat region
	JournalRegionOffset uint64 // Journal region start
	LockBitmapOffset    uint64 // Lock bitmap region
	InodeTableOffset    uint64 // Inode table start
	BlockBitmapOffset   uint64 // Block allocation bitmap
	RefCountTableOffset uint64 // Reference count table (for snapshots)
	SnapshotRegOffset   uint64 // Snapshot registry
	DataRegionOffset    uint64 // Data blocks start

	// Region sizes in bytes (offset 144-207)
	NodeRegistrySize  uint64
	JournalRegionSize uint64
	LockBitmapSize    uint64
	InodeTableSize    uint64
	BlockBitmapSize   uint64
	RefCountTableSize uint64
	SnapshotRegSize   uint64

	// Counters (offset 208-239)
	InodeCount    uint64 // Total inodes allocated
	FreeInodes    uint64 // Free inode count
	FreeBlocks    uint64 // Free block count
	SnapshotCount uint32 // Active snapshots
	MountCount    uint32 // Times mounted

	// Timestamps (offset 240-263)
	CreateTime    int64 // Volume creation time (Unix)
	LastMountTime int64 // Last mount time
	LastWriteTime int64 // Last metadata write

	// Reserved for future use (offset 264-4091)
	Reserved [3828]byte

	// Checksum - last 4 bytes (offset 4092-4095)
	Checksum uint32 // CRC32C of bytes 0-4091
}

// CanMount checks if this version of GoClusterFS can safely mount the volume.
// Returns nil if safe to mount, or an appropriate error.
func (sb *Superblock) CanMount() error {
	// Check if superblock is from a newer version we can't safely read
	if sb.Version > CurrentVersion {
		return fmt.Errorf("%w: volume is v%d, we support up to v%d",
			ErrNewerVersion, sb.Version, CurrentVersion)
	}

	// Check if superblock is too old (we no longer support it)
	if sb.Version < MinSupportedVersion {
		return fmt.Errorf("%w: volume is v%d, minimum supported is v%d",
			ErrTooOldVersion, sb.Version, MinSupportedVersion)
	}

	// Check for required features we don't understand
	// &^ is "AND NOT" - gives us bits set in RequiredFlags but not in FlagsWeSupport
	unknownFlags := sb.RequiredFlags &^ FlagsWeSupport
	if unknownFlags != 0 {
		return fmt.Errorf("%w: unknown required flags 0x%x",
			ErrUnknownRequired, unknownFlags)
	}

	return nil
}

// Serialize converts the superblock to bytes for writing to disk.
// Computes and sets the checksum before returning.
func (sb *Superblock) Serialize() []byte {
	buf := make([]byte, SuperblockSize)

	// Identity
	copy(buf[0:12], sb.Magic[:])
	binary.LittleEndian.PutUint32(buf[12:16], sb.Version)
	copy(buf[16:32], sb.UUID[:])

	// Feature flags
	binary.LittleEndian.PutUint64(buf[32:40], sb.FeatureFlags)
	binary.LittleEndian.PutUint64(buf[40:48], sb.RequiredFlags)

	// Geometry
	binary.LittleEndian.PutUint32(buf[48:52], sb.BlockSize)
	binary.LittleEndian.PutUint64(buf[52:60], sb.TotalBlocks)
	binary.LittleEndian.PutUint32(buf[60:64], sb.MetaBlockSize)
	binary.LittleEndian.PutUint32(buf[64:68], sb.MaxNodes)
	binary.LittleEndian.PutUint32(buf[68:72], sb.InodeSize)

	// Region offsets
	binary.LittleEndian.PutUint64(buf[80:88], sb.NodeRegistryOffset)
	binary.LittleEndian.PutUint64(buf[88:96], sb.JournalRegionOffset)
	binary.LittleEndian.PutUint64(buf[96:104], sb.LockBitmapOffset)
	binary.LittleEndian.PutUint64(buf[104:112], sb.InodeTableOffset)
	binary.LittleEndian.PutUint64(buf[112:120], sb.BlockBitmapOffset)
	binary.LittleEndian.PutUint64(buf[120:128], sb.RefCountTableOffset)
	binary.LittleEndian.PutUint64(buf[128:136], sb.SnapshotRegOffset)
	binary.LittleEndian.PutUint64(buf[136:144], sb.DataRegionOffset)

	// Region sizes
	binary.LittleEndian.PutUint64(buf[144:152], sb.NodeRegistrySize)
	binary.LittleEndian.PutUint64(buf[152:160], sb.JournalRegionSize)
	binary.LittleEndian.PutUint64(buf[160:168], sb.LockBitmapSize)
	binary.LittleEndian.PutUint64(buf[168:176], sb.InodeTableSize)
	binary.LittleEndian.PutUint64(buf[176:184], sb.BlockBitmapSize)
	binary.LittleEndian.PutUint64(buf[184:192], sb.RefCountTableSize)
	binary.LittleEndian.PutUint64(buf[192:200], sb.SnapshotRegSize)

	// Counters
	binary.LittleEndian.PutUint64(buf[208:216], sb.InodeCount)
	binary.LittleEndian.PutUint64(buf[216:224], sb.FreeInodes)
	binary.LittleEndian.PutUint64(buf[224:232], sb.FreeBlocks)
	binary.LittleEndian.PutUint32(buf[232:236], sb.SnapshotCount)
	binary.LittleEndian.PutUint32(buf[236:240], sb.MountCount)

	// Timestamps
	binary.LittleEndian.PutUint64(buf[240:248], uint64(sb.CreateTime))
	binary.LittleEndian.PutUint64(buf[248:256], uint64(sb.LastMountTime))
	binary.LittleEndian.PutUint64(buf[256:264], uint64(sb.LastWriteTime))

	// Reserved (already zero)

	// Compute and store checksum (last 4 bytes)
	checksum := storage.CRC32C(buf[:SuperblockSize-4])
	binary.LittleEndian.PutUint32(buf[SuperblockSize-4:], checksum)
	sb.Checksum = checksum

	return buf
}

// Deserialize parses a superblock from raw bytes.
// Returns an error if magic or checksum is invalid.
func Deserialize(buf []byte) (*Superblock, error) {
	if len(buf) < SuperblockSize {
		return nil, fmt.Errorf("buffer too small: %d < %d", len(buf), SuperblockSize)
	}

	// Verify checksum first
	storedChecksum := binary.LittleEndian.Uint32(buf[SuperblockSize-4:])
	computedChecksum := storage.CRC32C(buf[:SuperblockSize-4])
	if storedChecksum != computedChecksum {
		return nil, ErrChecksumMismatch
	}

	sb := &Superblock{}

	// Identity
	copy(sb.Magic[:], buf[0:12])
	if string(sb.Magic[:11]) != Magic {
		return nil, ErrInvalidMagic
	}
	sb.Version = binary.LittleEndian.Uint32(buf[12:16])
	copy(sb.UUID[:], buf[16:32])

	// Feature flags
	sb.FeatureFlags = binary.LittleEndian.Uint64(buf[32:40])
	sb.RequiredFlags = binary.LittleEndian.Uint64(buf[40:48])

	// Geometry
	sb.BlockSize = binary.LittleEndian.Uint32(buf[48:52])
	sb.TotalBlocks = binary.LittleEndian.Uint64(buf[52:60])
	sb.MetaBlockSize = binary.LittleEndian.Uint32(buf[60:64])
	sb.MaxNodes = binary.LittleEndian.Uint32(buf[64:68])
	sb.InodeSize = binary.LittleEndian.Uint32(buf[68:72])

	// Region offsets
	sb.NodeRegistryOffset = binary.LittleEndian.Uint64(buf[80:88])
	sb.JournalRegionOffset = binary.LittleEndian.Uint64(buf[88:96])
	sb.LockBitmapOffset = binary.LittleEndian.Uint64(buf[96:104])
	sb.InodeTableOffset = binary.LittleEndian.Uint64(buf[104:112])
	sb.BlockBitmapOffset = binary.LittleEndian.Uint64(buf[112:120])
	sb.RefCountTableOffset = binary.LittleEndian.Uint64(buf[120:128])
	sb.SnapshotRegOffset = binary.LittleEndian.Uint64(buf[128:136])
	sb.DataRegionOffset = binary.LittleEndian.Uint64(buf[136:144])

	// Region sizes
	sb.NodeRegistrySize = binary.LittleEndian.Uint64(buf[144:152])
	sb.JournalRegionSize = binary.LittleEndian.Uint64(buf[152:160])
	sb.LockBitmapSize = binary.LittleEndian.Uint64(buf[160:168])
	sb.InodeTableSize = binary.LittleEndian.Uint64(buf[168:176])
	sb.BlockBitmapSize = binary.LittleEndian.Uint64(buf[176:184])
	sb.RefCountTableSize = binary.LittleEndian.Uint64(buf[184:192])
	sb.SnapshotRegSize = binary.LittleEndian.Uint64(buf[192:200])

	// Counters
	sb.InodeCount = binary.LittleEndian.Uint64(buf[208:216])
	sb.FreeInodes = binary.LittleEndian.Uint64(buf[216:224])
	sb.FreeBlocks = binary.LittleEndian.Uint64(buf[224:232])
	sb.SnapshotCount = binary.LittleEndian.Uint32(buf[232:236])
	sb.MountCount = binary.LittleEndian.Uint32(buf[236:240])

	// Timestamps
	sb.CreateTime = int64(binary.LittleEndian.Uint64(buf[240:248]))
	sb.LastMountTime = int64(binary.LittleEndian.Uint64(buf[248:256]))
	sb.LastWriteTime = int64(binary.LittleEndian.Uint64(buf[256:264]))

	sb.Checksum = storedChecksum

	return sb, nil
}
