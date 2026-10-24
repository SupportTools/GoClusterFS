package superblock

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func testDevice(t *testing.T, size int64) *storage.Device {
	t.Helper()
	dir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	path := filepath.Join(dir, "superblock-"+filepath.Base(t.Name())+".img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	f.Close()
	t.Cleanup(func() { os.Remove(path) })

	dev, err := storage.Open(path)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	t.Cleanup(func() { dev.Close() })
	return dev
}

func writeAt(t *testing.T, dev *storage.Device, offset int64, data []byte) {
	t.Helper()
	buf := storage.NewAlignedBuffer(storage.SectorSize)
	copy(buf.Bytes(), data)
	if _, err := dev.WriteAt(buf.Bytes(), offset); err != nil {
		t.Fatal(err)
	}
}

func TestFormatThenRead(t *testing.T) {
	dev := testDevice(t, MinDeviceSize+8<<20)
	for _, offset := range []int64{1 << 20, 3 << 20, 5<<20 + 7*NodeJournalSize, 262 << 20} {
		writeAt(t, dev, offset, []byte("stale metadata from an earlier format"))
	}

	formatted, err := Format(dev)
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	got, err := Read(dev)
	if err != nil {
		t.Fatalf("Read after Format: %v", err)
	}
	if got.UUID != formatted.UUID || got.TotalBlocks != 9 || got.Version != 2 {
		t.Errorf("read back UUID %x, %d blocks, v%d; want %x, 9 blocks, v2",
			got.UUID, got.TotalBlocks, got.Version, formatted.UUID)
	}

	buf := storage.NewAlignedBuffer(storage.SectorSize)
	for _, offset := range []int64{1 << 20, 3 << 20, 5<<20 + 7*NodeJournalSize, 262 << 20} {
		if _, err := dev.ReadAt(buf.Bytes(), offset); err != nil {
			t.Fatal(err)
		}
		if !isZero(buf.Bytes()) {
			t.Errorf("Format left data at offset %d", offset)
		}
	}
}

func TestFormatRejectsSmallDevice(t *testing.T) {
	dev := testDevice(t, MinDeviceSize-1<<20)
	if _, err := Format(dev); !errors.Is(err, ErrDeviceTooSmall) {
		t.Errorf("Format on %d-byte device: err = %v, want ErrDeviceTooSmall", dev.Size(), err)
	}
}

func TestReadBlankDevice(t *testing.T) {
	dev := testDevice(t, MinDeviceSize)
	if _, err := Read(dev); !errors.Is(err, ErrNotFormatted) {
		t.Errorf("Read on blank device: err = %v, want ErrNotFormatted", err)
	}
}

func TestReadLegacyLayout(t *testing.T) {
	dev := testDevice(t, MinDeviceSize)
	inode := make([]byte, 2*inodeSize+4)
	binary.LittleEndian.PutUint32(inode[2*inodeSize:], legacyInodeMagic)
	writeAt(t, dev, 2<<20, inode)

	if _, err := Read(dev); !errors.Is(err, ErrLegacyLayout) {
		t.Errorf("Read on pre-v2 device: err = %v, want ErrLegacyLayout", err)
	}
}

func TestReadRejectsVersion1(t *testing.T) {
	dev := testDevice(t, MinDeviceSize)
	sb := New(dev.Size())
	sb.Version = 1
	writeAt(t, dev, 0, sb.Serialize())

	if _, err := Read(dev); !errors.Is(err, ErrTooOldVersion) {
		t.Errorf("Read v1 superblock: err = %v, want ErrTooOldVersion", err)
	}
}

func TestReadRejectsForeignJournalGeometry(t *testing.T) {
	dev := testDevice(t, MinDeviceSize)
	sb := New(dev.Size())
	sb.MaxNodes = 16
	writeAt(t, dev, 0, sb.Serialize())

	if _, err := Read(dev); !errors.Is(err, ErrUnsupportedLayout) {
		t.Errorf("Read with 16 node slots: err = %v, want ErrUnsupportedLayout", err)
	}
}
