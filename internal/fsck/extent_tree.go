package fsck

import (
	"fmt"
	"math"

	"github.com/supporttools/GoClusterFS/internal/extent"
)

// extentTree is what walking one inode's extent tree found.
type extentTree struct {
	nodes   []uint64 // every block of every tree node
	extents []ExtentData
	err     error
}

// inodeExtents returns an inode's extents: inline ones, or those found in its tree.
func (c *Checker) inodeExtents(inodeNum uint32, inode *InodeData) []ExtentData {
	if inode.Flags&FlagHasExtentTree == 0 {
		return inode.InlineExtents[:]
	}
	if t := c.extentTrees[inodeNum]; t != nil {
		return t.extents
	}
	return nil
}

// walkExtentTree reads and validates every node of an inode's extent tree.
// It stops at the first broken node; extents read up to that point are kept.
func (c *Checker) walkExtentTree(inodeNum uint32, inode *InodeData) *extentTree {
	t := &extentTree{}
	root, err := c.readTreeNode(inodeNum, inode.ExtentTreeRoot, inode.ExtentTreeGen)
	if err != nil {
		t.err = fmt.Errorf("root node %d: %w", inode.ExtentTreeRoot, err)
		return t
	}
	t.nodes = append(t.nodes, c.nodeBlocks(inode.ExtentTreeRoot)...)

	switch {
	case inode.ExtentDepth == 1 && root.Level == extent.LevelLeaf:
		t.err = t.addLeaf(inode.ExtentTreeRoot, root, 0, math.MaxUint64)
	case inode.ExtentDepth == 2 && root.Level == extent.LevelIndex:
		for i, child := range root.Children {
			end := uint64(math.MaxUint64)
			if i+1 < len(root.Children) {
				end = root.Children[i+1].First
			}
			leaf, err := c.readTreeNode(inodeNum, child.Block, child.Gen)
			if err != nil {
				t.err = fmt.Errorf("leaf node %d: %w", child.Block, err)
				return t
			}
			if leaf.Level != extent.LevelLeaf || uint64(len(leaf.Extents)) != child.Count {
				t.err = fmt.Errorf("leaf node %d: level %d with %d extents, index expects a leaf with %d",
					child.Block, leaf.Level, len(leaf.Extents), child.Count)
				return t
			}
			t.nodes = append(t.nodes, c.nodeBlocks(child.Block)...)
			if t.err = t.addLeaf(child.Block, leaf, child.First, end); t.err != nil {
				return t
			}
		}
	default:
		t.err = fmt.Errorf("depth %d does not match root node level %d", inode.ExtentDepth, root.Level)
	}
	if t.err == nil && uint64(len(t.extents)) != inode.ExtentCount {
		t.err = fmt.Errorf("tree holds %d extents, inode says %d", len(t.extents), inode.ExtentCount)
	}
	return t
}

func (t *extentTree) addLeaf(block uint64, leaf *extent.TreeNode, first, end uint64) error {
	for _, x := range leaf.Extents {
		if x.LogicalBlock < first || x.LogicalBlock+x.Length > end {
			return fmt.Errorf("leaf node %d: extent at logical block %d is outside the leaf range [%d, %d)",
				block, x.LogicalBlock, first, end)
		}
		t.extents = append(t.extents, ExtentData{x.LogicalBlock, x.PhysicalBlock, x.Length, x.Flags})
	}
	return nil
}

// nodeBlocks returns every block of the tree node that starts at start.
func (c *Checker) nodeBlocks(start uint64) []uint64 {
	n := extent.NodeBlocks(uint64(c.sbData.BlockSize))
	blocks := make([]uint64, n)
	for i := range n {
		blocks[i] = start + i
	}
	return blocks
}

func (c *Checker) readTreeNode(inodeNum uint32, block, gen uint64) (*extent.TreeNode, error) {
	if n := extent.NodeBlocks(uint64(c.sbData.BlockSize)); block+n > c.sbData.TotalBlocks || block+n < block {
		return nil, fmt.Errorf("node blocks %d-%d out of range (max %d)", block, block+n-1, c.sbData.TotalBlocks-1)
	}
	n, err := extent.ReadTreeNode(c.dev, c.sbData.DataRegionOffset, uint64(c.sbData.BlockSize), block, gen)
	if err != nil {
		return nil, err
	}
	if n.Inode != inodeNum || n.Gen != gen {
		return nil, fmt.Errorf("node holds inode %d generation %d, want inode %d generation %d", n.Inode, n.Gen, inodeNum, gen)
	}
	return n, nil
}
