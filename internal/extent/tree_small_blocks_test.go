package extent

import (
	"context"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/inode"
)

// A node of half an 8 KB block holds 126 entries, which capped a file at
// 126 × 126 = 15,876 extents. Nodes of at least 64 KB lift that cap.
func TestFragmentedFileAtSmallBlocksOutgrowsHalfBlockNodes(t *testing.T) {
	const blockSize, blocks, extents = 8 << 10, 4096, 20000
	dev, cleanup := createTestDevice(t, treeTestDataOffset+blocks*blockSize)
	t.Cleanup(cleanup)
	var uuid [16]byte
	env := &treeEnv{
		t:   t,
		ctx: context.Background(),
		dev: dev,
		bm:  bitmap.NewRegion(dev, 0, blocks, nil, uuid),
		tbl: inode.NewTable(dev, treeTestInodeOffset, 64*inode.InodeSize, nil, uuid),
	}
	env.mgr = NewManager(dev, env.bm, nil, treeTestDataOffset, blockSize)
	env.tbl.SetTreeFlusher(env.mgr)

	ino := env.newInode()
	env.insertScattered(ino, extents)
	env.write(ino)
	ino = env.reread(ino)
	env.mgr = NewManager(dev, env.bm, nil, treeTestDataOffset, blockSize)
	env.checkScattered(ino, extents)

	if ino.ExtentTreeRoot%8 != 0 {
		t.Errorf("tree root at block %d, want the start of an aligned run of 8", ino.ExtentTreeRoot)
	}
	root, err := ReadTreeNode(dev, treeTestDataOffset, blockSize, ino.ExtentTreeRoot, ino.ExtentTreeGen)
	if err != nil {
		t.Fatal(err)
	}
	leaves := len(root.Children)
	if ino.ExtentDepth != 2 || leaves*MaxNodeEntries(blockSize) < extents {
		t.Fatalf("depth %d with %d leaves of %d entries holds fewer than %d extents", ino.ExtentDepth, leaves, MaxNodeEntries(blockSize), extents)
	}
	if got, want := env.bm.AllocatedCount(), uint64(leaves+1)*8; got != want {
		t.Errorf("%d blocks allocated for the tree, want %d: %d leaves and an index of 8 blocks each", got, want, leaves)
	}

	if err := env.mgr.Truncate(env.ctx, ino, 0); err != nil {
		t.Fatal(err)
	}
	env.write(ino)
	if got := env.bm.AllocatedCount(); got != 0 {
		t.Errorf("%d blocks still allocated after truncating to 0, want every tree node block freed", got)
	}
}
