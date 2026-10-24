package superblock

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func FuzzDeserialize(f *testing.F) {
	valid := New(64<<30, 0).Serialize()
	f.Add(valid)
	f.Add(valid[:SuperblockSize-1])
	f.Add(valid[:len(Magic)])
	f.Add([]byte{})
	f.Add(make([]byte, SuperblockSize))

	newer := New(1<<30, 0)
	newer.Version = CurrentVersion + 1
	newer.RequiredFlags = ^uint64(0)
	f.Add(newer.Serialize())

	maxed := New(1<<30, 0)
	maxed.BlockSize = ^uint32(0)
	maxed.TotalBlocks = ^uint64(0)
	maxed.InodeTableOffset = ^uint64(0)
	maxed.InodeTableSize = ^uint64(0)
	maxed.DataRegionOffset = ^uint64(0)
	f.Add(maxed.Serialize())

	f.Fuzz(func(t *testing.T, data []byte) {
		checkSuperblockDecode(t, data)
		if len(data) >= SuperblockSize {
			fixed := bytes.Clone(data[:SuperblockSize])
			binary.LittleEndian.PutUint32(fixed[SuperblockSize-4:], storage.CRC32C(fixed[:SuperblockSize-4]))
			checkSuperblockDecode(t, fixed)
		}
	})
}

func checkSuperblockDecode(t *testing.T, data []byte) {
	sb, err := Deserialize(data)
	if err != nil {
		return
	}
	_ = sb.CanMount()

	again, err := Deserialize(sb.Serialize())
	if err != nil {
		t.Fatalf("decoding a re-encoded superblock: %v", err)
	}
	if *again != *sb {
		t.Fatalf("superblock changed after an encode/decode round trip")
	}
}
