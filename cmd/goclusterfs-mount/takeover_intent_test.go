package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// The first node of this build to join a volume formatted before takeover intents sets RequiredTakeoverIntent, once;
// a volume that has it stays as it is.
func TestJoinSetsRequiredTakeoverIntentOnce(t *testing.T) {
	dir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	path := filepath.Join(dir, "takeover-intent.img")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	if err := os.Truncate(path, superblock.MinDeviceSize); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Skipf("O_DIRECT not supported: %v", err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	sb, err := superblock.Format(dev, 0)
	if err != nil {
		t.Fatal(err)
	}
	sb.RequiredFlags &^= superblock.RequiredTakeoverIntent
	if err := superblock.Write(dev, sb); err != nil {
		t.Fatal(err)
	}

	if err := requireTakeoverIntent(dev, sb); err != nil {
		t.Fatal(err)
	}
	onDisk, err := superblock.Read(dev)
	if err != nil {
		t.Fatal(err)
	}
	if onDisk.RequiredFlags&superblock.RequiredTakeoverIntent == 0 || sb.RequiredFlags&superblock.RequiredTakeoverIntent == 0 {
		t.Fatalf("flag on disk %v, in memory %v; want both set",
			onDisk.RequiredFlags&superblock.RequiredTakeoverIntent != 0, sb.RequiredFlags&superblock.RequiredTakeoverIntent != 0)
	}
	onDisk.LastWriteTime = 12345
	buf := storage.NewAlignedBuffer(superblock.SuperblockSize)
	copy(buf.Bytes(), onDisk.Serialize())
	if _, err := dev.WriteAt(buf.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	if err := requireTakeoverIntent(dev, sb); err != nil {
		t.Fatal(err)
	}
	if again, err := superblock.Read(dev); err != nil || again.LastWriteTime != 12345 {
		t.Errorf("a second join rewrote the superblock (%v)", err)
	}
}
