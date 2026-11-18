package snapshot

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

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
	_, err = reg.List(ctx)
	var bad *UnreadableSectorError
	if !errors.As(err, &bad) || bad.Sector != 0 || !strings.Contains(err.Error(), "run goclusterfs-fsck -repair") {
		t.Errorf("list error %q does not name sector 0 and say to run goclusterfs-fsck -repair", err)
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

// A sector whose checksum fails in the one-I/O read, as one a peer was writing, is read again on its own.
func TestRegistryRereadsATornSector(t *testing.T) {
	dev, cleanup := createTestDevice(t, 10<<20)
	defer cleanup()
	ctx := context.Background()
	registry := NewRegistry(dev, 0, 16*SectorSize, nil)
	kept, err := registry.Create(ctx, "kept", 100, InvalidSnapshotID, 0, 1, 1024, FlagReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	counter := &countingDevice{device: dev}
	registry.dev = counter
	afterBulkRead = func(region []byte) { region[100] ^= 0xFF }
	defer func() { afterBulkRead = nil }()

	descs, err := registry.ListForInode(ctx, 100)
	if err != nil || len(descs) != 1 || descs[0].SnapshotID != kept.SnapshotID {
		t.Fatalf("listing with sector 0 torn in the bulk read: %v, %v; want snapshot %d", descs, err, kept.SnapshotID)
	}
	if counter.reads != 2 {
		t.Errorf("%d reads, want the bulk read and one read of the torn sector", counter.reads)
	}

	afterBulkRead = nil
	breakSectorChecksum(t, dev)
	var unreadable *UnreadableSectorError
	if _, err := registry.ListForInode(ctx, 100); !errors.As(err, &unreadable) || unreadable.Sector != 0 {
		t.Errorf("listing with sector 0 damaged on disk: %v, want UnreadableSectorError for sector 0", err)
	}
}

// deviceOutsideTmpfs makes a device in the package directory. tmpfs, where CI keeps other test images, tears an
// O_DIRECT read inside a sector while a write to it runs, which no real device does.
func deviceOutsideTmpfs(t *testing.T, size int64) *storage.Device {
	t.Helper()
	var st unix.Statfs_t
	if err := unix.Statfs(".", &st); err != nil || st.Type == unix.TMPFS_MAGIC {
		t.Skipf("the package directory is on tmpfs or cannot be checked (%v)", err)
	}
	path := fmt.Sprintf("peer-writes-%d.bin", time.Now().UnixNano())
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Skipf("cannot write a device into the package directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	if err := os.Truncate(path, size); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	return dev
}

func TestRegistryListsWhileAPeerRewritesASlot(t *testing.T) {
	dev := deviceOutsideTmpfs(t, 10<<20)
	ctx := context.Background()
	const size = 16 * SectorSize
	peer := NewRegistry(dev, 0, size, nil)
	stable, err := peer.Create(ctx, "stable", 100, InvalidSnapshotID, 0, 1, 1024, FlagReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	busy, err := peer.Create(ctx, "busy", 200, InvalidSnapshotID, 0, 1, 1024, FlagReadOnly)
	if err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	var rewrites atomic.Int64
	writerErr := make(chan error, 1)
	go func() {
		for i := uint64(1); !stop.Load(); i++ {
			if err := peer.SetRefCountJobID(ctx, busy.SnapshotID, i); err != nil {
				writerErr <- err
				return
			}
			rewrites.Add(1)
		}
		writerErr <- nil
	}()
	lister := NewRegistry(dev, 0, size, nil)
	for i := range 500 {
		descs, err := lister.ListForInode(ctx, 100)
		if err != nil || len(descs) != 1 || descs[0].SnapshotID != stable.SnapshotID {
			stop.Store(true)
			t.Fatalf("listing %d while the peer rewrites its slot: %v, %v; want snapshot %d", i, descs, err, stable.SnapshotID)
		}
	}
	stop.Store(true)
	if err := <-writerErr; err != nil {
		t.Fatal(err)
	}
	if rewrites.Load() == 0 {
		t.Error("the peer never rewrote its slot during the listings")
	}
}
