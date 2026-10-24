package inode

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func FuzzDeserializeInode(f *testing.F) {
	file := Inode{
		Magic:            InodeMagic,
		InodeNum:         42,
		Mode:             S_IFREG | 0o644,
		UID:              1000,
		GID:              1000,
		LinkCount:        1,
		Size:             3 << 20,
		AllocatedBlocks:  3,
		WrittenBlocks:    3,
		Atime:            1_700_000_000,
		Mtime:            1_700_000_001,
		Ctime:            1_700_000_002,
		DataGeneration:   4,
		MetaGeneration:   5,
		LastModifierUUID: [16]byte{1, 2, 3, 4},
		InlineExtents: [MaxInlineExtents]InlineExtent{
			{LogicalBlock: 0, PhysicalBlock: 100, Length: 2, Flags: 1},
			{LogicalBlock: 2, PhysicalBlock: 300, Length: 1, Flags: 1},
		},
		ExtentCount: 2,
	}

	tree := file
	tree.Flags = FlagHasExtentTree
	tree.ExtentTreeRoot = 999
	tree.ExtentCount = ^uint64(0)
	tree.ExtentDepth = ^uint32(0)
	tree.Size = ^uint64(0)

	sfb := file
	sfb.Size = 100
	sfb.ExtentCount = 1
	sfb.InlineExtents = [MaxInlineExtents]InlineExtent{{PhysicalBlock: 7<<8 | 3, Length: 1, Flags: extentFlagSFB}}

	root := Inode{Magic: InodeMagic, InodeNum: InodeRoot, Mode: S_IFDIR | 0o755, LinkCount: 2}

	for _, ino := range []*Inode{&file, &tree, &sfb, &root} {
		f.Add(serializeInode(ino))
	}
	valid := serializeInode(&file)
	f.Add(valid[:InodeSize-1])
	f.Add(valid[:4])
	f.Add(make([]byte, InodeSize))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		checkInodeDecode(t, data)
		if len(data) >= InodeSize {
			fixed := bytes.Clone(data[:InodeSize])
			binary.LittleEndian.PutUint32(fixed[InodeSize-4:], storage.CRC32C(fixed[:InodeSize-4]))
			checkInodeDecode(t, fixed)
		}
	})
}

func checkInodeDecode(t *testing.T, data []byte) {
	ino, err := deserializeInode(data)
	if err != nil || ino.Magic == 0 {
		return
	}
	_ = ino.IsSFBBacked()

	again, err := deserializeInode(serializeInode(ino))
	if err != nil {
		t.Fatalf("decoding a re-encoded inode: %v", err)
	}
	again.Checksum, ino.Checksum = 0, 0
	if *again != *ino {
		t.Fatalf("inode changed after an encode/decode round trip")
	}
}
