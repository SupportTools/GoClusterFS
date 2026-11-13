package inode

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// openSharedDevice opens one image twice, as two nodes sharing a device do.
func openSharedDevice(t *testing.T, size int64) (*storage.Device, *storage.Device) {
	t.Helper()
	dir := os.Getenv("GOCLUSTERFS_TEST_DIR")
	if dir == "" {
		dir = "."
	}
	f, err := os.CreateTemp(dir, "test_inode_sector_*.bin")
	if err != nil {
		t.Skipf("Cannot create test file: %v", err)
	}
	path := f.Name()
	t.Cleanup(func() { os.Remove(path) })
	err = f.Truncate(size)
	f.Close()
	if err != nil {
		t.Skipf("Cannot truncate test file: %v", err)
	}

	devs := make([]*storage.Device, 2)
	for i := range devs {
		if devs[i], err = storage.Open(path); err != nil {
			t.Skipf("O_DIRECT not supported: %v", err)
		}
		t.Cleanup(func() { devs[i].Close() })
	}
	return devs[0], devs[1]
}

func newNodeTable(dev *storage.Device, name string, slot int) *Table {
	var uuid [16]byte
	copy(uuid[:], name)
	return NewTable(dev, 0, 4<<20, lock.NewManager(dev, 8<<20, 1<<20, uuid, slot), uuid)
}

// TestNeighborInodeWritesFromTwoNodes has two nodes update two inodes in one
// table sector at once. Each write rewrites the whole sector, so an unlocked
// sector lets one node put back a stale copy of the other node's inode.
func TestNeighborInodeWritesFromTwoNodes(t *testing.T) {
	devA, devB := openSharedDevice(t, 16<<20)
	tables := []*Table{newNodeTable(devA, "node-a", 1), newNodeTable(devB, "node-b", 2)}
	ctx := context.Background()

	nums := make([]uint32, len(tables))
	for i, table := range tables {
		ino, err := table.ClaimSlot(ctx, InodeFirstUsable+uint32(i))
		if err != nil {
			t.Fatalf("node %d: ClaimSlot: %v", i, err)
		}
		nums[i] = ino.InodeNum
	}

	const writes = 300
	var wg sync.WaitGroup
	errs := make([]error, len(tables))
	for i, table := range tables {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < writes; n++ {
				ino, err := table.Read(nums[i])
				if err == nil {
					ino.Size++
					err = table.Write(ctx, ino)
				}
				if err != nil {
					errs[i] = err
					return
				}
			}
		}()
	}
	wg.Wait()

	for i, num := range nums {
		if errs[i] != nil {
			t.Fatalf("node %d: %v", i, errs[i])
		}
		ino, err := tables[0].Read(num)
		if err != nil {
			t.Fatal(err)
		}
		if ino.Size != writes {
			t.Errorf("inode %d: Size %d after %d increments; another node's sector write undid the rest", num, ino.Size, writes)
		}
	}
}
