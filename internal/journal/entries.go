package journal

import (
	"encoding/binary"
)

// TxnOpType represents the type of operation in a transaction entry.
type TxnOpType uint32

const (
	// Inode Operations (0x01xx)
	TxnOpInodeCreate TxnOpType = 0x0100
	TxnOpInodeUpdate TxnOpType = 0x0101
	TxnOpInodeDelete TxnOpType = 0x0102

	// Directory Operations (0x02xx)
	TxnOpDirAddEntry    TxnOpType = 0x0200
	TxnOpDirRemoveEntry TxnOpType = 0x0201
	TxnOpDirRename      TxnOpType = 0x0202

	// Block Allocation (0x03xx)
	TxnOpBlockAlloc TxnOpType = 0x0300
	TxnOpBlockFree  TxnOpType = 0x0301

	// Sub-Block/Small File Allocation (0x04xx)
	TxnOpSFBAlloc   TxnOpType = 0x0400
	TxnOpSFBFree    TxnOpType = 0x0401
	TxnOpSFBMigrate TxnOpType = 0x0402

	// Extent Operations (0x05xx)
	TxnOpExtentAdd    TxnOpType = 0x0500
	TxnOpExtentRemove TxnOpType = 0x0501
	TxnOpExtentUpdate TxnOpType = 0x0502

	// Snapshot Operations (0x06xx)
	TxnOpSnapshotCreate TxnOpType = 0x0600
	TxnOpSnapshotDelete TxnOpType = 0x0601
	TxnOpRefCountUpdate TxnOpType = 0x0602
)

// String returns a human-readable representation of the operation type.
func (op TxnOpType) String() string {
	switch op {
	case TxnOpInodeCreate:
		return "InodeCreate"
	case TxnOpInodeUpdate:
		return "InodeUpdate"
	case TxnOpInodeDelete:
		return "InodeDelete"
	case TxnOpDirAddEntry:
		return "DirAddEntry"
	case TxnOpDirRemoveEntry:
		return "DirRemoveEntry"
	case TxnOpDirRename:
		return "DirRename"
	case TxnOpBlockAlloc:
		return "BlockAlloc"
	case TxnOpBlockFree:
		return "BlockFree"
	case TxnOpSFBAlloc:
		return "SFBAlloc"
	case TxnOpSFBFree:
		return "SFBFree"
	case TxnOpSFBMigrate:
		return "SFBMigrate"
	case TxnOpExtentAdd:
		return "ExtentAdd"
	case TxnOpExtentRemove:
		return "ExtentRemove"
	case TxnOpExtentUpdate:
		return "ExtentUpdate"
	case TxnOpSnapshotCreate:
		return "SnapshotCreate"
	case TxnOpSnapshotDelete:
		return "SnapshotDelete"
	case TxnOpRefCountUpdate:
		return "RefCountUpdate"
	default:
		return "Unknown"
	}
}

// ============================================================================
// Inode Operation Payloads
// ============================================================================

// InodeCreatePayload is logged when creating a new inode.
// Size: 40 bytes
type InodeCreatePayload struct {
	InodeNum       uint64
	Mode           uint32
	UID            uint32
	GID            uint32
	ParentInodeNum uint64
	NameHash       uint64 // Hash of filename (for verification)
	Reserved       uint32
}

// Serialize converts the payload to bytes.
func (p *InodeCreatePayload) Serialize() []byte {
	buf := make([]byte, 40)
	binary.LittleEndian.PutUint64(buf[0:8], p.InodeNum)
	binary.LittleEndian.PutUint32(buf[8:12], p.Mode)
	binary.LittleEndian.PutUint32(buf[12:16], p.UID)
	binary.LittleEndian.PutUint32(buf[16:20], p.GID)
	binary.LittleEndian.PutUint64(buf[20:28], p.ParentInodeNum)
	binary.LittleEndian.PutUint64(buf[28:36], p.NameHash)
	return buf
}

// DeserializeInodeCreate parses an InodeCreatePayload from bytes.
func DeserializeInodeCreate(buf []byte) (*InodeCreatePayload, error) {
	if len(buf) < 40 {
		return nil, ErrTxnTooSmall
	}
	return &InodeCreatePayload{
		InodeNum:       binary.LittleEndian.Uint64(buf[0:8]),
		Mode:           binary.LittleEndian.Uint32(buf[8:12]),
		UID:            binary.LittleEndian.Uint32(buf[12:16]),
		GID:            binary.LittleEndian.Uint32(buf[16:20]),
		ParentInodeNum: binary.LittleEndian.Uint64(buf[20:28]),
		NameHash:       binary.LittleEndian.Uint64(buf[28:36]),
	}, nil
}

// InodeUpdateMask indicates which fields are being updated.
type InodeUpdateMask uint64

const (
	InodeUpdateSize    InodeUpdateMask = 1 << 0
	InodeUpdateMode    InodeUpdateMask = 1 << 1
	InodeUpdateUID     InodeUpdateMask = 1 << 2
	InodeUpdateGID     InodeUpdateMask = 1 << 3
	InodeUpdateAtime   InodeUpdateMask = 1 << 4
	InodeUpdateMtime   InodeUpdateMask = 1 << 5
	InodeUpdateCtime   InodeUpdateMask = 1 << 6
	InodeUpdateExtents InodeUpdateMask = 1 << 7
	InodeUpdateSFB     InodeUpdateMask = 1 << 8
	InodeUpdateNlink   InodeUpdateMask = 1 << 9
)

// InodeUpdatePayload is logged when updating an existing inode.
// Size: 16 bytes header + variable old/new values
type InodeUpdatePayload struct {
	InodeNum   uint64
	UpdateMask InodeUpdateMask
	// The actual updated values follow in a separate structure
	// based on which bits are set in UpdateMask
}

// Serialize converts the payload to bytes (header only).
func (p *InodeUpdatePayload) Serialize() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], p.InodeNum)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(p.UpdateMask))
	return buf
}

// DeserializeInodeUpdate parses an InodeUpdatePayload from bytes (header only).
func DeserializeInodeUpdate(buf []byte) (*InodeUpdatePayload, error) {
	if len(buf) < 16 {
		return nil, ErrTxnTooSmall
	}
	return &InodeUpdatePayload{
		InodeNum:   binary.LittleEndian.Uint64(buf[0:8]),
		UpdateMask: InodeUpdateMask(binary.LittleEndian.Uint64(buf[8:16])),
	}, nil
}

// InodeDeletePayload is logged when deleting an inode.
// Size: 8 bytes
type InodeDeletePayload struct {
	InodeNum uint64
}

// Serialize converts the payload to bytes.
func (p *InodeDeletePayload) Serialize() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf[0:8], p.InodeNum)
	return buf
}

// DeserializeInodeDelete parses an InodeDeletePayload from bytes.
func DeserializeInodeDelete(buf []byte) (*InodeDeletePayload, error) {
	if len(buf) < 8 {
		return nil, ErrTxnTooSmall
	}
	return &InodeDeletePayload{
		InodeNum: binary.LittleEndian.Uint64(buf[0:8]),
	}, nil
}

// ============================================================================
// Directory Operation Payloads
// ============================================================================

// DirEntryType represents the type of directory entry.
type DirEntryType uint8

const (
	DirEntryUnknown   DirEntryType = 0
	DirEntryRegular   DirEntryType = 1
	DirEntryDirectory DirEntryType = 2
	DirEntrySymlink   DirEntryType = 3
	DirEntrySpecial   DirEntryType = 4
)

// DirAddEntryPayload is logged when adding a directory entry.
// Size: 24 bytes header + variable name (padded to 4-byte boundary)
type DirAddEntryPayload struct {
	ParentInodeNum uint64
	ChildInodeNum  uint64
	EntryType      DirEntryType
	NameLen        uint8
	Reserved       [2]byte
	Name           string // Variable length
}

// Serialize converts the payload to bytes.
func (p *DirAddEntryPayload) Serialize() []byte {
	nameLen := len(p.Name)
	if nameLen > 255 {
		nameLen = 255
	}
	// Pad name to 4-byte boundary
	paddedNameLen := (nameLen + 3) &^ 3

	buf := make([]byte, 24+paddedNameLen)
	binary.LittleEndian.PutUint64(buf[0:8], p.ParentInodeNum)
	binary.LittleEndian.PutUint64(buf[8:16], p.ChildInodeNum)
	buf[16] = byte(p.EntryType)
	buf[17] = uint8(nameLen)
	// Reserved at 18-19
	copy(buf[24:], p.Name[:nameLen])
	return buf
}

// DeserializeDirAddEntry parses a DirAddEntryPayload from bytes.
func DeserializeDirAddEntry(buf []byte) (*DirAddEntryPayload, error) {
	if len(buf) < 24 {
		return nil, ErrTxnTooSmall
	}
	nameLen := int(buf[17])
	if len(buf) < 24+nameLen {
		return nil, ErrTxnTooSmall
	}
	return &DirAddEntryPayload{
		ParentInodeNum: binary.LittleEndian.Uint64(buf[0:8]),
		ChildInodeNum:  binary.LittleEndian.Uint64(buf[8:16]),
		EntryType:      DirEntryType(buf[16]),
		NameLen:        buf[17],
		Name:           string(buf[24 : 24+nameLen]),
	}, nil
}

// DirRemoveEntryPayload is logged when removing a directory entry.
// Size: 24 bytes header + variable name (padded to 4-byte boundary)
type DirRemoveEntryPayload struct {
	ParentInodeNum uint64
	ChildInodeNum  uint64
	EntryType      DirEntryType
	NameLen        uint8
	Reserved       [2]byte
	Name           string // Variable length
}

// Serialize converts the payload to bytes.
func (p *DirRemoveEntryPayload) Serialize() []byte {
	nameLen := len(p.Name)
	if nameLen > 255 {
		nameLen = 255
	}
	paddedNameLen := (nameLen + 3) &^ 3

	buf := make([]byte, 24+paddedNameLen)
	binary.LittleEndian.PutUint64(buf[0:8], p.ParentInodeNum)
	binary.LittleEndian.PutUint64(buf[8:16], p.ChildInodeNum)
	buf[16] = byte(p.EntryType)
	buf[17] = uint8(nameLen)
	copy(buf[24:], p.Name[:nameLen])
	return buf
}

// DeserializeDirRemoveEntry parses a DirRemoveEntryPayload from bytes.
func DeserializeDirRemoveEntry(buf []byte) (*DirRemoveEntryPayload, error) {
	if len(buf) < 24 {
		return nil, ErrTxnTooSmall
	}
	nameLen := int(buf[17])
	if len(buf) < 24+nameLen {
		return nil, ErrTxnTooSmall
	}
	return &DirRemoveEntryPayload{
		ParentInodeNum: binary.LittleEndian.Uint64(buf[0:8]),
		ChildInodeNum:  binary.LittleEndian.Uint64(buf[8:16]),
		EntryType:      DirEntryType(buf[16]),
		NameLen:        buf[17],
		Name:           string(buf[24 : 24+nameLen]),
	}, nil
}

// DirRenamePayload is logged when renaming/moving a directory entry.
// Size: 48 bytes header + variable names
type DirRenamePayload struct {
	OldParentInodeNum uint64
	NewParentInodeNum uint64
	InodeNum          uint64
	EntryType         DirEntryType
	OldNameLen        uint8
	NewNameLen        uint8
	Reserved          [5]byte
	OldName           string
	NewName           string
}

// Serialize converts the payload to bytes.
func (p *DirRenamePayload) Serialize() []byte {
	oldNameLen := len(p.OldName)
	newNameLen := len(p.NewName)
	if oldNameLen > 255 {
		oldNameLen = 255
	}
	if newNameLen > 255 {
		newNameLen = 255
	}

	paddedOldNameLen := (oldNameLen + 3) &^ 3
	paddedNewNameLen := (newNameLen + 3) &^ 3

	buf := make([]byte, 32+paddedOldNameLen+paddedNewNameLen)
	binary.LittleEndian.PutUint64(buf[0:8], p.OldParentInodeNum)
	binary.LittleEndian.PutUint64(buf[8:16], p.NewParentInodeNum)
	binary.LittleEndian.PutUint64(buf[16:24], p.InodeNum)
	buf[24] = byte(p.EntryType)
	buf[25] = uint8(oldNameLen)
	buf[26] = uint8(newNameLen)
	// Reserved at 27-31

	offset := 32
	copy(buf[offset:], p.OldName[:oldNameLen])
	offset += paddedOldNameLen
	copy(buf[offset:], p.NewName[:newNameLen])

	return buf
}

// DeserializeDirRename parses a DirRenamePayload from bytes.
func DeserializeDirRename(buf []byte) (*DirRenamePayload, error) {
	if len(buf) < 32 {
		return nil, ErrTxnTooSmall
	}
	oldNameLen := int(buf[25])
	newNameLen := int(buf[26])
	paddedOldNameLen := (oldNameLen + 3) &^ 3

	if len(buf) < 32+paddedOldNameLen+newNameLen {
		return nil, ErrTxnTooSmall
	}

	return &DirRenamePayload{
		OldParentInodeNum: binary.LittleEndian.Uint64(buf[0:8]),
		NewParentInodeNum: binary.LittleEndian.Uint64(buf[8:16]),
		InodeNum:          binary.LittleEndian.Uint64(buf[16:24]),
		EntryType:         DirEntryType(buf[24]),
		OldNameLen:        buf[25],
		NewNameLen:        buf[26],
		OldName:           string(buf[32 : 32+oldNameLen]),
		NewName:           string(buf[32+paddedOldNameLen : 32+paddedOldNameLen+newNameLen]),
	}, nil
}

// ============================================================================
// Block Allocation Payloads
// ============================================================================

// BlockPurpose indicates why a block is being allocated.
type BlockPurpose uint32

const (
	BlockPurposeData     BlockPurpose = 0
	BlockPurposeMetadata BlockPurpose = 1
	BlockPurposeSFB      BlockPurpose = 2 // Small file block container
)

// BlockAllocPayload is logged when allocating blocks.
// Size: 24 bytes
type BlockAllocPayload struct {
	InodeNum   uint64
	StartBlock uint64
	Count      uint32
	Purpose    BlockPurpose
}

// Serialize converts the payload to bytes.
func (p *BlockAllocPayload) Serialize() []byte {
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint64(buf[0:8], p.InodeNum)
	binary.LittleEndian.PutUint64(buf[8:16], p.StartBlock)
	binary.LittleEndian.PutUint32(buf[16:20], p.Count)
	binary.LittleEndian.PutUint32(buf[20:24], uint32(p.Purpose))
	return buf
}

// DeserializeBlockAlloc parses a BlockAllocPayload from bytes.
func DeserializeBlockAlloc(buf []byte) (*BlockAllocPayload, error) {
	if len(buf) < 24 {
		return nil, ErrTxnTooSmall
	}
	return &BlockAllocPayload{
		InodeNum:   binary.LittleEndian.Uint64(buf[0:8]),
		StartBlock: binary.LittleEndian.Uint64(buf[8:16]),
		Count:      binary.LittleEndian.Uint32(buf[16:20]),
		Purpose:    BlockPurpose(binary.LittleEndian.Uint32(buf[20:24])),
	}, nil
}

// BlockFreePayload is logged when freeing blocks.
// Size: 24 bytes
type BlockFreePayload struct {
	InodeNum   uint64
	StartBlock uint64
	Count      uint32
	Reserved   uint32
}

// Serialize converts the payload to bytes.
func (p *BlockFreePayload) Serialize() []byte {
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint64(buf[0:8], p.InodeNum)
	binary.LittleEndian.PutUint64(buf[8:16], p.StartBlock)
	binary.LittleEndian.PutUint32(buf[16:20], p.Count)
	return buf
}

// DeserializeBlockFree parses a BlockFreePayload from bytes.
func DeserializeBlockFree(buf []byte) (*BlockFreePayload, error) {
	if len(buf) < 24 {
		return nil, ErrTxnTooSmall
	}
	return &BlockFreePayload{
		InodeNum:   binary.LittleEndian.Uint64(buf[0:8]),
		StartBlock: binary.LittleEndian.Uint64(buf[8:16]),
		Count:      binary.LittleEndian.Uint32(buf[16:20]),
	}, nil
}

// ============================================================================
// SFB (Sub-File Block) Operation Payloads
// ============================================================================

// SFBTier represents the SFB container tier (slot size).
type SFBTier uint8

const (
	SFBTier4KB  SFBTier = 0 // 256 slots of 4KB each
	SFBTier8KB  SFBTier = 1 // 128 slots of 8KB each
	SFBTier16KB SFBTier = 2 //  64 slots of 16KB each
	SFBTier64KB SFBTier = 3 //  16 slots of 64KB each
)

// SFBAllocPayload is logged when allocating an SFB slot.
// Size: 24 bytes
type SFBAllocPayload struct {
	InodeNum       uint64  // Inode being allocated for
	ContainerBlock uint64  // Block number of SFB container
	SlotIndex      uint16  // Slot index within container
	Tier           SFBTier // Container tier (determines slot size)
	Reserved       [5]byte
}

// Serialize converts the payload to bytes.
func (p *SFBAllocPayload) Serialize() []byte {
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint64(buf[0:8], p.InodeNum)
	binary.LittleEndian.PutUint64(buf[8:16], p.ContainerBlock)
	binary.LittleEndian.PutUint16(buf[16:18], p.SlotIndex)
	buf[18] = byte(p.Tier)
	return buf
}

// DeserializeSFBAlloc parses an SFBAllocPayload from bytes.
func DeserializeSFBAlloc(buf []byte) (*SFBAllocPayload, error) {
	if len(buf) < 24 {
		return nil, ErrTxnTooSmall
	}
	return &SFBAllocPayload{
		InodeNum:       binary.LittleEndian.Uint64(buf[0:8]),
		ContainerBlock: binary.LittleEndian.Uint64(buf[8:16]),
		SlotIndex:      binary.LittleEndian.Uint16(buf[16:18]),
		Tier:           SFBTier(buf[18]),
	}, nil
}

// SFBFreePayload is logged when freeing an SFB slot.
// Size: 16 bytes
type SFBFreePayload struct {
	ContainerBlock uint64 // Block number of SFB container
	SlotIndex      uint16 // Slot index within container
	Reserved       [6]byte
}

// Serialize converts the payload to bytes.
func (p *SFBFreePayload) Serialize() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], p.ContainerBlock)
	binary.LittleEndian.PutUint16(buf[8:10], p.SlotIndex)
	return buf
}

// DeserializeSFBFree parses an SFBFreePayload from bytes.
func DeserializeSFBFree(buf []byte) (*SFBFreePayload, error) {
	if len(buf) < 16 {
		return nil, ErrTxnTooSmall
	}
	return &SFBFreePayload{
		ContainerBlock: binary.LittleEndian.Uint64(buf[0:8]),
		SlotIndex:      binary.LittleEndian.Uint16(buf[8:10]),
	}, nil
}

// SFBMigratePayload is logged when migrating from SFB to full extent.
// Size: 32 bytes
type SFBMigratePayload struct {
	InodeNum     uint64 // Inode being migrated
	OldContainer uint64 // Old SFB container block
	OldSlot      uint16 // Old slot index
	Reserved     [6]byte
	NewBlock     uint64 // New full 1MB block
}

// Serialize converts the payload to bytes.
func (p *SFBMigratePayload) Serialize() []byte {
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint64(buf[0:8], p.InodeNum)
	binary.LittleEndian.PutUint64(buf[8:16], p.OldContainer)
	binary.LittleEndian.PutUint16(buf[16:18], p.OldSlot)
	binary.LittleEndian.PutUint64(buf[24:32], p.NewBlock)
	return buf
}

// DeserializeSFBMigrate parses an SFBMigratePayload from bytes.
func DeserializeSFBMigrate(buf []byte) (*SFBMigratePayload, error) {
	if len(buf) < 32 {
		return nil, ErrTxnTooSmall
	}
	return &SFBMigratePayload{
		InodeNum:     binary.LittleEndian.Uint64(buf[0:8]),
		OldContainer: binary.LittleEndian.Uint64(buf[8:16]),
		OldSlot:      binary.LittleEndian.Uint16(buf[16:18]),
		NewBlock:     binary.LittleEndian.Uint64(buf[24:32]),
	}, nil
}

// ============================================================================
// Extent Operation Payloads
// ============================================================================

// ExtentFlags represents extent properties.
type ExtentFlags uint32

const (
	ExtentFlagWritten    ExtentFlags = 1 << 0 // Data has been written
	ExtentFlagPrealloc   ExtentFlags = 1 << 1 // Preallocated (sparse)
	ExtentFlagShared     ExtentFlags = 1 << 2 // Shared with snapshot
	ExtentFlagCompressed ExtentFlags = 1 << 3 // Compressed (future)
	ExtentFlagEncrypted  ExtentFlags = 1 << 4 // Encrypted (future)
)

// ExtentAddPayload is logged when adding an extent to an inode.
// Size: 32 bytes
type ExtentAddPayload struct {
	InodeNum  uint64
	FileBlock uint64 // Logical block within file
	DiskBlock uint64 // Physical block on device
	Length    uint32 // Number of blocks
	Flags     ExtentFlags
}

// Serialize converts the payload to bytes.
func (p *ExtentAddPayload) Serialize() []byte {
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint64(buf[0:8], p.InodeNum)
	binary.LittleEndian.PutUint64(buf[8:16], p.FileBlock)
	binary.LittleEndian.PutUint64(buf[16:24], p.DiskBlock)
	binary.LittleEndian.PutUint32(buf[24:28], p.Length)
	binary.LittleEndian.PutUint32(buf[28:32], uint32(p.Flags))
	return buf
}

// DeserializeExtentAdd parses an ExtentAddPayload from bytes.
func DeserializeExtentAdd(buf []byte) (*ExtentAddPayload, error) {
	if len(buf) < 32 {
		return nil, ErrTxnTooSmall
	}
	return &ExtentAddPayload{
		InodeNum:  binary.LittleEndian.Uint64(buf[0:8]),
		FileBlock: binary.LittleEndian.Uint64(buf[8:16]),
		DiskBlock: binary.LittleEndian.Uint64(buf[16:24]),
		Length:    binary.LittleEndian.Uint32(buf[24:28]),
		Flags:     ExtentFlags(binary.LittleEndian.Uint32(buf[28:32])),
	}, nil
}

// ExtentRemovePayload is logged when removing an extent from an inode.
// Size: 24 bytes
type ExtentRemovePayload struct {
	InodeNum  uint64
	FileBlock uint64
	Length    uint32
	Reserved  uint32
}

// Serialize converts the payload to bytes.
func (p *ExtentRemovePayload) Serialize() []byte {
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint64(buf[0:8], p.InodeNum)
	binary.LittleEndian.PutUint64(buf[8:16], p.FileBlock)
	binary.LittleEndian.PutUint32(buf[16:20], p.Length)
	return buf
}

// DeserializeExtentRemove parses an ExtentRemovePayload from bytes.
func DeserializeExtentRemove(buf []byte) (*ExtentRemovePayload, error) {
	if len(buf) < 24 {
		return nil, ErrTxnTooSmall
	}
	return &ExtentRemovePayload{
		InodeNum:  binary.LittleEndian.Uint64(buf[0:8]),
		FileBlock: binary.LittleEndian.Uint64(buf[8:16]),
		Length:    binary.LittleEndian.Uint32(buf[16:20]),
	}, nil
}

// ============================================================================
// Snapshot Operation Payloads
// ============================================================================

// SnapshotCreatePayload is logged when creating a snapshot.
// Size: 32 bytes
type SnapshotCreatePayload struct {
	SnapshotID   uint64
	ParentID     uint64 // 0 for base snapshot
	RootInodeNum uint64
	Timestamp    int64
}

// Serialize converts the payload to bytes.
func (p *SnapshotCreatePayload) Serialize() []byte {
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint64(buf[0:8], p.SnapshotID)
	binary.LittleEndian.PutUint64(buf[8:16], p.ParentID)
	binary.LittleEndian.PutUint64(buf[16:24], p.RootInodeNum)
	binary.LittleEndian.PutUint64(buf[24:32], uint64(p.Timestamp))
	return buf
}

// DeserializeSnapshotCreate parses a SnapshotCreatePayload from bytes.
func DeserializeSnapshotCreate(buf []byte) (*SnapshotCreatePayload, error) {
	if len(buf) < 32 {
		return nil, ErrTxnTooSmall
	}
	return &SnapshotCreatePayload{
		SnapshotID:   binary.LittleEndian.Uint64(buf[0:8]),
		ParentID:     binary.LittleEndian.Uint64(buf[8:16]),
		RootInodeNum: binary.LittleEndian.Uint64(buf[16:24]),
		Timestamp:    int64(binary.LittleEndian.Uint64(buf[24:32])),
	}, nil
}

// SnapshotDeletePayload is logged when deleting a snapshot.
// Size: 8 bytes
type SnapshotDeletePayload struct {
	SnapshotID uint64
}

// Serialize converts the payload to bytes.
func (p *SnapshotDeletePayload) Serialize() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf[0:8], p.SnapshotID)
	return buf
}

// DeserializeSnapshotDelete parses a SnapshotDeletePayload from bytes.
func DeserializeSnapshotDelete(buf []byte) (*SnapshotDeletePayload, error) {
	if len(buf) < 8 {
		return nil, ErrTxnTooSmall
	}
	return &SnapshotDeletePayload{
		SnapshotID: binary.LittleEndian.Uint64(buf[0:8]),
	}, nil
}

// RefCountUpdatePayload is logged when updating block reference counts.
// Size: 16 bytes
type RefCountUpdatePayload struct {
	BlockNum uint64
	OldCount uint32
	NewCount uint32
}

// Serialize converts the payload to bytes.
func (p *RefCountUpdatePayload) Serialize() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], p.BlockNum)
	binary.LittleEndian.PutUint32(buf[8:12], p.OldCount)
	binary.LittleEndian.PutUint32(buf[12:16], p.NewCount)
	return buf
}

// DeserializeRefCountUpdate parses a RefCountUpdatePayload from bytes.
func DeserializeRefCountUpdate(buf []byte) (*RefCountUpdatePayload, error) {
	if len(buf) < 16 {
		return nil, ErrTxnTooSmall
	}
	return &RefCountUpdatePayload{
		BlockNum: binary.LittleEndian.Uint64(buf[0:8]),
		OldCount: binary.LittleEndian.Uint32(buf[8:12]),
		NewCount: binary.LittleEndian.Uint32(buf[12:16]),
	}, nil
}

// ============================================================================
// Transaction Builder Convenience Methods
// ============================================================================

// LogInodeCreate adds an inode creation entry to the transaction.
func (txn *Transaction) LogInodeCreate(payload *InodeCreatePayload) error {
	return txn.addEntry(TxnOpInodeCreate, payload.Serialize())
}

// LogInodeUpdate adds an inode update entry to the transaction.
func (txn *Transaction) LogInodeUpdate(payload *InodeUpdatePayload) error {
	return txn.addEntry(TxnOpInodeUpdate, payload.Serialize())
}

// LogInodeDelete adds an inode deletion entry to the transaction.
func (txn *Transaction) LogInodeDelete(payload *InodeDeletePayload) error {
	return txn.addEntry(TxnOpInodeDelete, payload.Serialize())
}

// LogDirAddEntry adds a directory entry addition to the transaction.
func (txn *Transaction) LogDirAddEntry(payload *DirAddEntryPayload) error {
	return txn.addEntry(TxnOpDirAddEntry, payload.Serialize())
}

// LogDirRemoveEntry adds a directory entry removal to the transaction.
func (txn *Transaction) LogDirRemoveEntry(payload *DirRemoveEntryPayload) error {
	return txn.addEntry(TxnOpDirRemoveEntry, payload.Serialize())
}

// LogDirRename adds a directory rename/move to the transaction.
func (txn *Transaction) LogDirRename(payload *DirRenamePayload) error {
	return txn.addEntry(TxnOpDirRename, payload.Serialize())
}

// LogBlockAlloc adds a block allocation to the transaction.
func (txn *Transaction) LogBlockAlloc(payload *BlockAllocPayload) error {
	return txn.addEntry(TxnOpBlockAlloc, payload.Serialize())
}

// LogBlockFree adds a block free to the transaction.
func (txn *Transaction) LogBlockFree(payload *BlockFreePayload) error {
	return txn.addEntry(TxnOpBlockFree, payload.Serialize())
}

// LogExtentAdd adds an extent addition to the transaction.
func (txn *Transaction) LogExtentAdd(payload *ExtentAddPayload) error {
	return txn.addEntry(TxnOpExtentAdd, payload.Serialize())
}

// LogExtentRemove adds an extent removal to the transaction.
func (txn *Transaction) LogExtentRemove(payload *ExtentRemovePayload) error {
	return txn.addEntry(TxnOpExtentRemove, payload.Serialize())
}

// LogSnapshotCreate adds a snapshot creation to the transaction.
func (txn *Transaction) LogSnapshotCreate(payload *SnapshotCreatePayload) error {
	return txn.addEntry(TxnOpSnapshotCreate, payload.Serialize())
}

// LogSnapshotDelete adds a snapshot deletion to the transaction.
func (txn *Transaction) LogSnapshotDelete(payload *SnapshotDeletePayload) error {
	return txn.addEntry(TxnOpSnapshotDelete, payload.Serialize())
}

// LogRefCountUpdate adds a reference count update to the transaction.
func (txn *Transaction) LogRefCountUpdate(payload *RefCountUpdatePayload) error {
	return txn.addEntry(TxnOpRefCountUpdate, payload.Serialize())
}

// LogSFBAlloc adds an SFB slot allocation to the transaction.
func (txn *Transaction) LogSFBAlloc(payload *SFBAllocPayload) error {
	return txn.addEntry(TxnOpSFBAlloc, payload.Serialize())
}

// LogSFBFree adds an SFB slot free to the transaction.
func (txn *Transaction) LogSFBFree(payload *SFBFreePayload) error {
	return txn.addEntry(TxnOpSFBFree, payload.Serialize())
}

// LogSFBMigrate adds an SFB to extent migration to the transaction.
func (txn *Transaction) LogSFBMigrate(payload *SFBMigratePayload) error {
	return txn.addEntry(TxnOpSFBMigrate, payload.Serialize())
}
