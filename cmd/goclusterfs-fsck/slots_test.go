package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// A live slot another node marked to take over shows that node in -list-slots and -inspect-slot.
func TestSlotsShowATakeoverIntent(t *testing.T) {
	dev, sb := formatted(t)
	reg := node.NewRegistry(dev, sb.NodeRegistryOffset)
	slot, err := reg.Register(context.Background(), "victim")
	if err != nil {
		t.Fatal(err)
	}
	taker := [16]byte{0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89}
	marked := time.Now().Add(-time.Minute).Unix()
	off := int64(sb.NodeRegistryOffset) + int64(slot)*node.SlotSize
	buf := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	if _, err := dev.ReadAt(buf, off); err != nil {
		t.Fatal(err)
	}
	copy(buf[156:172], taker[:])
	binary.LittleEndian.PutUint64(buf[172:180], uint64(marked))
	binary.LittleEndian.PutUint32(buf[node.SlotSize-4:], storage.CRC32C(buf[:node.SlotSize-4]))
	if _, err := dev.WriteAt(buf, off); err != nil {
		t.Fatal(err)
	}

	checker := fsck.New(dev, fsck.Options{Output: &bytes.Buffer{}})
	var list, inspect bytes.Buffer
	if err := printSlots(&list, checker); err != nil {
		t.Fatal(err)
	}
	if err := inspectSlot(&inspect, checker, slot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(inspect.String(), "Volume: takeover intents required") {
		t.Errorf("-inspect-slot does not show the volume's takeover intent flag:\n%s", inspect.String())
	}
	if !strings.Contains(list.String(), "being taken over by abcdef0123456789") {
		t.Errorf("-list-slots does not show the intent:\n%s", list.String())
	}
	if want := "node abcdef0123456789 began taking it over " + unixTime(marked); !strings.Contains(inspect.String(), want) ||
		strings.Contains(inspect.String(), "journal replay lease") {
		t.Errorf("-inspect-slot does not show the intent as %q, or calls it a replay lease:\n%s", want, inspect.String())
	}
}

func formatted(t *testing.T) (*storage.Device, *superblock.Superblock) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "fsck-slots-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "test.img")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, superblock.MinDeviceSize+64<<20); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Skipf("O_DIRECT not supported here: %v", err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	sb, err := superblock.Format(dev, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	return dev, sb
}
