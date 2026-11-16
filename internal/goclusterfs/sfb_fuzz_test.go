package goclusterfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/sfb"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

func sfbHeaderChecksum(sector []byte) {
	crc := crc32.Update(crc32.ChecksumIEEE(sector[:16]), crc32.IEEETable, sector[20:sfb.HeaderSize])
	binary.LittleEndian.PutUint32(sector[16:20], crc)
}

// FuzzSFBContainer frees and reads slots of SFB containers that older builds
// wrote, with whatever header and slot number the disk holds. Freeing must
// clear exactly the slot's bit of a valid header, or leave the sector alone.
func FuzzSFBContainer(f *testing.F) {
	h := sfb.Header{Magic: sfb.HeaderMagic, Version: 1, Tier: sfb.Tier4KB, SlotCount: 256, FreeSlots: 254}
	h.Bitmap[0], h.Bitmap[31] = 0b1, 0b10000000
	f.Add(h.Serialize(), uint16(0), uint8(0), uint64(4096), false)
	f.Add(h.Serialize(), uint16(255), uint8(0), uint64(100), false)
	big := h
	big.SlotCount = 1000
	f.Add(big.Serialize(), uint16(500), uint8(3), uint64(65536), false)
	f.Add([]byte{0x43, 0x42, 0x46, 0x53}, uint16(7), uint8(1), uint64(1), true)

	dir := f.TempDir()
	f.Fuzz(func(t *testing.T, header []byte, slot uint16, tier uint8, size uint64, fixChecksum bool) {
		sector := make([]byte, storage.SectorSize)
		copy(sector, header)
		if fixChecksum {
			sfbHeaderChecksum(sector)
		}
		dev := sfbDevice(t, dir, sector)
		fs := &FS{dev: dev}

		fs.freeSFBSlot(context.Background(), sfb.PackPhysicalBlock(0, slot))
		after := make([]byte, storage.SectorSize)
		buf := storage.NewAlignedBuffer(storage.SectorSize)
		if _, err := dev.ReadAt(buf.Bytes(), 0); err != nil {
			t.Fatal(err)
		}
		copy(after, buf.Bytes())
		if !bytes.Equal(after, sector) {
			want := bytes.Clone(sector)
			want[20+slot/8] &^= 1 << (slot % 8)
			binary.LittleEndian.PutUint16(want[8:10], binary.LittleEndian.Uint16(want[8:10])+1)
			sfbHeaderChecksum(want)
			if !bytes.Equal(after, want) {
				t.Fatalf("freeing slot %d changed the container header to %x, want %x", slot, after[:sfb.HeaderSize], want[:sfb.HeaderSize])
			}
		}

		ino := &inode.Inode{InodeNum: 9, Size: size, ExtentCount: 1}
		ino.InlineExtents[0] = inode.InlineExtent{PhysicalBlock: sfb.PackPhysicalBlock(0, slot), Length: 1, Flags: uint32(tier&0x3) << 4}
		_, _ = fs.readSFB(ino, 0, min(size, 1<<16))
	})
}

func sfbDevice(t *testing.T, dir string, sector []byte) *storage.Device {
	f, err := os.CreateTemp(dir, "sfb-*.img")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	t.Cleanup(func() { _ = os.Remove(path) })
	if err := f.Truncate(2 << 20); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	buf := storage.NewAlignedBuffer(storage.SectorSize)
	copy(buf.Bytes(), sector)
	if _, err := dev.WriteAt(buf.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	return dev
}
