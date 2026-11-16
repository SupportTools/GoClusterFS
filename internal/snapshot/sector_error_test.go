package snapshot

import (
	"context"
	"encoding/binary"
	"errors"
	"syscall"
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

// A sector the registry cannot read may hold the snapshot a lookup asks for, or
// part of a listing. Lookups that find nothing and every listing fail with EIO
// instead of reporting not found or returning part of the list. A snapshot in a
// readable sector is still found, and a mount's scan still succeeds.
func TestUnreadableSectorFailsLookupsWithEIO(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	reg := NewRegistry(dev, 0, 10*SectorSize, nil)
	ctx := context.Background()
	s1, err := reg.Create(ctx, "s1", 10, 0, 11, 1, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	reg.SetNodeSlot(1)
	if _, err := reg.Create(ctx, "far", 20, 0, 21, 1, 4096, 0); err != nil {
		t.Fatal(err)
	}

	breakSectorChecksum(t, dev)
	if _, err := reg.GetByName(ctx, "far"); err != nil {
		t.Errorf("snapshot in a readable sector: %v", err)
	}
	for name, err := range map[string]error{
		"GetByName":                   second(reg.GetByName(ctx, "s1")),
		"GetByName of a missing name": second(reg.GetByName(ctx, "missing")),
		"Get":                         second(reg.Get(ctx, s1.SnapshotID)),
		"List":                        second(reg.List(ctx)),
		"ListForInode":                second(reg.ListForInode(ctx, 10)),
		"ListForTarget":               second(reg.ListForTarget(ctx, Target{Inode: 10, Birth: 1})),
		"GetChildren":                 second(reg.GetChildren(ctx, s1.SnapshotID)),
		"UpdateState":                 reg.UpdateState(ctx, s1.SnapshotID, StateDeleting),
		"Remove":                      reg.Remove(ctx, s1.SnapshotID),
	} {
		if !errors.Is(err, syscall.EIO) {
			t.Errorf("%s with an unreadable sector: %v, want EIO", name, err)
		}
	}
	stats, err := reg.Scan(ctx)
	if err != nil || !errors.Is(stats.Unreadable, syscall.EIO) {
		t.Errorf("scan with an unreadable sector: err %v, unreadable %v; want no error and EIO", err, stats.Unreadable)
	}

	fixSectorChecksum(t, dev)
	if _, err := reg.GetByName(ctx, "missing"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Errorf("missing name once the sector reads again: %v, want not found", err)
	}
	if all, err := reg.List(ctx); err != nil || len(all) != 2 {
		t.Errorf("list once the sector reads again: %d snapshots, %v; want 2", len(all), err)
	}
}

func second[T any](_ T, err error) error { return err }
