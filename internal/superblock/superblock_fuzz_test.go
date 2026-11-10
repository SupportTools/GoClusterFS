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
	for _, size := range []int64{MinDeviceSize, 1 << 40, int64(min(sb.DataRegionOffset+sb.TotalBlocks<<20, 1<<62))} {
		if sb.CheckLayout(size) == nil {
			checkRegionsDisjoint(t, sb, size)
		}
	}

	again, err := Deserialize(sb.Serialize())
	if err != nil {
		t.Fatalf("decoding a re-encoded superblock: %v", err)
	}
	if *again != *sb {
		t.Fatalf("superblock changed after an encode/decode round trip")
	}
}

func checkRegionsDisjoint(t *testing.T, sb *Superblock, size int64) {
	regions := append(sb.layout(), region{name: "superblock", size: regionSize},
		region{name: "data", offset: sb.DataRegionOffset, size: sb.TotalBlocks * uint64(sb.BlockSize)})
	for i, a := range regions {
		if a.size > uint64(size) || a.offset > uint64(size)-a.size {
			t.Fatalf("CheckLayout(%d) accepted %s at %d with %d bytes past the device", size, a.name, a.offset, a.size)
		}
		for _, b := range regions[i+1:] {
			if a.offset < b.offset+b.size && b.offset < a.offset+a.size {
				t.Fatalf("CheckLayout(%d) accepted %s at %d overlapping %s at %d", size, a.name, a.offset, b.name, b.offset)
			}
		}
	}
}
