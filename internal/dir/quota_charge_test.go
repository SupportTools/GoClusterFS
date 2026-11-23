package dir

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// Growing a directory charges its project once per block it adds. A growth whose inode write loses to another node's
// change gives its charge back before it retries, so only the retry's charge counts.
func TestGrowthChargesOncePerBlockAfterAConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dir.img")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, 64<<20); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	const blockSize, dataOffset = 8 << 10, 10 << 20
	bm := bitmap.NewRegion(dev, 2<<20, 1000, nil, [16]byte{})
	inodes := inode.NewTable(dev, 3<<20, 100*inode.InodeSize, nil, [16]byte{})
	extents := extent.NewManager(dev, bm, nil, dataOffset, blockSize)
	inodes.SetTreeFlusher(extents)
	m := NewManager(dev, inodes, extents, nil, dataOffset)
	var charged int64
	m.SetCharge(func(project uint32, blocks int64) {
		if project != 7 {
			t.Errorf("charged project %d, want the directory's 7", project)
		}
		charged += blocks
	})

	ctx := t.Context()
	d, err := claimFree(ctx, inodes)
	if err != nil {
		t.Fatal(err)
	}
	d.Mode, d.ProjectID = inode.S_IFDIR|0o755, 7
	if err := m.InitDirectory(ctx, d, 2); err != nil {
		t.Fatal(err)
	}
	conflicts := 0
	testHookBeforeGrowthWrite = func() error {
		if conflicts++; conflicts == 1 {
			return fmt.Errorf("injected: %w", extent.ErrTreeConflict)
		}
		return nil
	}
	t.Cleanup(func() { testHookBeforeGrowthWrite = nil })
	for i := range 80 {
		if err := m.Add(ctx, d, fmt.Sprintf("%s%04d", strings.Repeat("n", 196), i), uint64(10+i), DT_REG); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	fresh, err := inodes.Read(d.InodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if conflicts < 2 || fresh.AllocatedBlocks < 2 {
		t.Fatalf("the directory grew to %d blocks with %d growth writes; want a growth, and a conflict before it", fresh.AllocatedBlocks, conflicts)
	}
	if want := int64(fresh.AllocatedBlocks) - 1; charged != want {
		t.Errorf("growth charged %d blocks, want the %d the directory grew by", charged, want)
	}
}
