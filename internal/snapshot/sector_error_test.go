package snapshot

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// breakSectorChecksum makes registry sector 0 fail its checksum, as a read error would make it unreadable.
func breakSectorChecksum(t *testing.T, dev *storage.Device) {
	t.Helper()
	buf := storage.NewAlignedBuffer(SectorSize)
	if _, err := dev.ReadAt(buf.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	buf.Bytes()[SectorSize-1] ^= 0xFF
	if _, err := dev.WriteAt(buf.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
}

// fixSectorChecksum recomputes sector 0's checksum over whatever data it holds now.
func fixSectorChecksum(t *testing.T, dev *storage.Device) {
	t.Helper()
	buf := storage.NewAlignedBuffer(SectorSize)
	if _, err := dev.ReadAt(buf.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	binary.LittleEndian.PutUint32(data[SectorSize-ChecksumSize:], storage.CRC32C(data[:SectorSize-ChecksumSize]))
	if _, err := dev.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
}

// A sector the registry cannot read may hold snapshots. Writing it fails
// instead of replacing it with a sector that holds only the new descriptor.
func TestUnreadableSectorIsNotRewritten(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(reg *Registry) error
	}{
		{"create", func(reg *Registry) error {
			_, err := reg.Create(context.Background(), "s3", 12, 0, 13, 1, 4096, 0)
			return err
		}},
		{"descriptor write", func(reg *Registry) error {
			return reg.writeDescriptor(5, &Descriptor{SnapshotID: 99, State: StateActive})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dev, cleanup := createTestDevice(t, 1<<20)
			defer cleanup()
			reg := NewRegistry(dev, 0, 10*SectorSize, nil)
			ctx := context.Background()
			for _, name := range []string{"s1", "s2"} {
				if _, err := reg.Create(ctx, name, 10, 0, 11, 1, 4096, 0); err != nil {
					t.Fatal(err)
				}
			}

			breakSectorChecksum(t, dev)
			if err := tc.write(reg); err == nil {
				t.Error("writing to an unreadable sector succeeded")
			}
			fixSectorChecksum(t, dev)
			for _, name := range []string{"s1", "s2"} {
				if _, err := reg.GetByName(ctx, name); err != nil {
					t.Errorf("%s is gone after a write to its unreadable sector: %v", name, err)
				}
			}
		})
	}
}
