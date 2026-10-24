package snapshot

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func FuzzDecodeDescriptor(f *testing.F) {
	active := Descriptor{
		Magic:         DescriptorMagic,
		SnapshotID:    7,
		ParentID:      3,
		TargetInode:   42,
		State:         StateActive,
		Flags:         FlagReadOnly | FlagHasData,
		DataInode:     1234,
		TotalBlocks:   5,
		TotalSize:     5 << 20,
		CreateTime:    1_700_000_000,
		RefCountJobID: 9,
	}
	if err := active.SetName("nightly"); err != nil {
		f.Fatal(err)
	}

	maxed := active
	maxed.SnapshotID = ^uint64(0)
	maxed.ParentID = ^uint64(0)
	maxed.State = StateDeleting
	maxed.Flags = FlagWritable
	maxed.TotalBlocks = ^uint64(0)
	copy(maxed.Name[:], bytes.Repeat([]byte{'x'}, MaxSnapshotNameLen))

	sector := make([]byte, BytesPerSector)
	encodeDescriptor(sector, 0, &active)
	encodeDescriptor(sector, DescriptorsPerSector-1, &maxed)

	f.Add(sector, uint8(0))
	f.Add(sector, uint8(1))
	f.Add(sector, uint8(DescriptorsPerSector-1))
	f.Add(sector, uint8(DescriptorsPerSector))
	f.Add(sector[:DescriptorSize-1], uint8(0))
	f.Add([]byte{}, uint8(0))

	f.Fuzz(func(t *testing.T, sector []byte, idx uint8) {
		checkDescriptorDecode(t, sector, uint64(idx))

		end := (int(idx) + 1) * DescriptorSize
		if end <= len(sector) {
			fixed := bytes.Clone(sector)
			desc := fixed[end-DescriptorSize : end]
			binary.LittleEndian.PutUint32(desc[DescriptorSize-4:], storage.CRC32C(desc[:DescriptorSize-4]))
			checkDescriptorDecode(t, fixed, uint64(idx))
		}
	})
}

func checkDescriptorDecode(t *testing.T, sector []byte, idx uint64) {
	desc, err := decodeDescriptor(sector, idx)
	if err != nil {
		return
	}
	_ = desc.GetName()

	out := make([]byte, BytesPerSector)
	encodeDescriptor(out, 0, desc)
	again, err := decodeDescriptor(out, 0)
	if err != nil {
		t.Fatalf("decoding a re-encoded descriptor: %v", err)
	}
	if *again != *desc {
		t.Fatalf("descriptor changed after an encode/decode round trip")
	}
}
