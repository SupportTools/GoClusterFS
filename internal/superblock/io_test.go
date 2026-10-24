package superblock

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func testDevice(t *testing.T, size int64) *storage.Device {
	dev, _ := testDeviceAt(t, size)
	return dev
}

func testDeviceAt(t *testing.T, size int64) (*storage.Device, string) {
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
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })

	dev, err := storage.Open(path)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	return dev, path
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
	for _, offset := range []int64{1 << 20, 3 << 20, 4<<20 + 7*NodeJournalSize, 260 << 20, 262 << 20} {
		writeAt(t, dev, offset, []byte("stale metadata from an earlier format"))
	}

	formatted, err := Format(dev, 0)
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	got, err := Read(dev)
	if err != nil {
		t.Fatalf("Read after Format: %v", err)
	}
	if got.UUID != formatted.UUID || got.TotalBlocks != 9 || got.Version != 3 {
		t.Errorf("read back UUID %x, %d blocks, v%d; want %x, 9 blocks, v3",
			got.UUID, got.TotalBlocks, got.Version, formatted.UUID)
	}

	buf := storage.NewAlignedBuffer(storage.SectorSize)
	for _, offset := range []int64{1 << 20, 3 << 20, 4<<20 + 7*NodeJournalSize, 260 << 20, 262 << 20} {
		if _, err := dev.ReadAt(buf.Bytes(), offset); err != nil {
			t.Fatal(err)
		}
		if !isZero(buf.Bytes()) {
			t.Errorf("Format left data at offset %d", offset)
		}
	}
}

func TestFormatKeepsSparseImageSparse(t *testing.T) {
	dev, path := testDeviceAt(t, MinDeviceSize)
	if _, err := Format(dev, 0); err != nil {
		t.Fatalf("Format: %v", err)
	}
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatal(err)
	}
	if allocated := st.Blocks * 512; allocated > 64<<10 {
		t.Errorf("Format allocated %d bytes of a blank sparse image, want only the superblock", allocated)
	}
}

func TestFormatRejectsSmallDevice(t *testing.T) {
	dev := testDevice(t, MinDeviceSize-1<<20)
	if _, err := Format(dev, 0); !errors.Is(err, ErrDeviceTooSmall) {
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
	sb := New(dev.Size(), 0)
	sb.Version = 1
	writeAt(t, dev, 0, sb.Serialize())

	if _, err := Read(dev); !errors.Is(err, ErrTooOldVersion) {
		t.Errorf("Read v1 superblock: err = %v, want ErrTooOldVersion", err)
	}
}

func TestReadRejectsVersion2(t *testing.T) {
	dev := testDevice(t, MinDeviceSize)
	sb := New(dev.Size(), 0)
	sb.Version = 2
	writeAt(t, dev, 0, sb.Serialize())

	_, err := Read(dev)
	if !errors.Is(err, ErrTooOldVersion) || !strings.Contains(err.Error(), "reformat with -format") {
		t.Errorf("Read v2 superblock: err = %v, want ErrTooOldVersion saying to reformat", err)
	}
}

func TestReadAfterDeviceGrows(t *testing.T) {
	dev, path := testDeviceAt(t, MinDeviceSize+8<<20)
	formatted, err := Format(dev, 64<<10)
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	if err := os.Truncate(path, 2<<30); err != nil {
		t.Fatal(err)
	}
	grown, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = grown.Close() })

	got, err := Read(grown)
	if err != nil {
		t.Fatalf("Read after the device grew: %v", err)
	}
	if got.TotalBlocks != formatted.TotalBlocks || got.InodeCount != formatted.InodeCount {
		t.Errorf("read %d blocks and %d inodes, want the formatted %d and %d",
			got.TotalBlocks, got.InodeCount, formatted.TotalBlocks, formatted.InodeCount)
	}
}

func TestReadRejectsForeignJournalGeometry(t *testing.T) {
	dev := testDevice(t, MinDeviceSize)
	sb := New(dev.Size(), 0)
	sb.MaxNodes = 16
	writeAt(t, dev, 0, sb.Serialize())

	if _, err := Read(dev); !errors.Is(err, ErrUnsupportedLayout) {
		t.Errorf("Read with 16 node slots: err = %v, want ErrUnsupportedLayout", err)
	}
}

func TestReadRejectsMovedRegion(t *testing.T) {
	dev := testDevice(t, MinDeviceSize)
	sb := New(dev.Size(), 0)
	sb.RefCountTableOffset += 4096
	writeAt(t, dev, 0, sb.Serialize())

	if _, err := Read(dev); !errors.Is(err, ErrUnsupportedLayout) {
		t.Errorf("Read with a moved refcount region: err = %v, want ErrUnsupportedLayout", err)
	}
}

func TestReadLegacyLayoutWithLeftoverBytes(t *testing.T) {
	dev := testDevice(t, MinDeviceSize)
	writeAt(t, dev, 0, []byte("leftover partition table"))
	inode := make([]byte, 2*inodeSize+4)
	binary.LittleEndian.PutUint32(inode[2*inodeSize:], legacyInodeMagic)
	writeAt(t, dev, 2<<20, inode)

	if _, err := Read(dev); !errors.Is(err, ErrLegacyLayout) {
		t.Errorf("Read on pre-v2 device with bytes at offset 0: err = %v, want ErrLegacyLayout", err)
	}
}
