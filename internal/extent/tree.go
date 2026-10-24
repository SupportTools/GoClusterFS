package extent

// Overflow extent tree.
//
// An inode that needs more than MaxInlineExtents extents keeps all of them in a
// tree of 1MB nodes taken from the block bitmap. At depth 1 the root is a leaf;
// at depth 2 the root is an index whose children are leaves. Each extent lies
// inside the key range of exactly one leaf.
//
// A node block has two 512KB halves. Generation g of a node lives in half g&1,
// so writing a new generation never touches the committed copy. The inode
// records the root block and generation; index entries record each leaf's
// generation. Edits stay in inode.PendingTree until inode.Table writes the
// inode: FlushTree writes the new node versions first, and the inode sector
// write commits them.

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	treeMagic   uint32 = 0x544E5458 // "XTNT"
	treeVersion uint16 = 1

	LevelLeaf  uint16 = 0
	LevelIndex uint16 = 1

	nodeHalfSize   = BlockSize / 2
	nodeHeaderSize = 64
	nodeEntrySize  = 32

	// MaxNodeEntries is the number of extents (leaf) or children (index) a node holds.
	MaxNodeEntries = (nodeHalfSize - nodeHeaderSize) / nodeEntrySize

	// Every commit rewrites each changed leaf, so leaves split well below
	// MaxNodeEntries to keep that write at 64KB.
	defaultLeafCap = 2048

	nodeCacheEntries  = 1 << 20
	nodeReadAttempts  = 3
	nodeRetryInterval = time.Millisecond
)

var (
	ErrExtentTreeFull = errors.New("extent tree full")
	ErrCorruptTree    = errors.New("corrupt extent tree node")
	ErrStaleTree      = errors.New("extent tree changed since the inode was read")
	ErrTreeConflict   = errors.New("extent tree changed concurrently")
)

// TreeNode is one decoded generation of an extent tree node.
type TreeNode struct {
	Level    uint16
	Inode    uint32
	Gen      uint64
	Extents  []Extent    // leaf entries, sorted by LogicalBlock
	Children []TreeChild // index entries, sorted by First
}

// TreeChild is an index entry for a leaf that covers logical blocks from First
// up to the next child's First.
type TreeChild struct {
	First uint64
	Block uint64
	Gen   uint64
	Count uint64
}

func (n *TreeNode) count() int {
	if n.Level == LevelLeaf {
		return len(n.Extents)
	}
	return len(n.Children)
}

// TreeNodeOffset returns the device offset of the half that holds generation gen.
func TreeNodeOffset(dataOffset, block, gen uint64) int64 {
	return int64(dataOffset + block*BlockSize + (gen&1)*nodeHalfSize)
}

func treeNodeSize(count int) int {
	n := nodeHeaderSize + count*nodeEntrySize
	return (n + storage.SectorSize - 1) &^ (storage.SectorSize - 1)
}

func treeNodeHeader(buf []byte) (level uint16, inodeNum uint32, count int, gen uint64, err error) {
	if len(buf) < nodeHeaderSize {
		return 0, 0, 0, 0, fmt.Errorf("%w: short header", ErrCorruptTree)
	}
	if magic := binary.LittleEndian.Uint32(buf[0:4]); magic != treeMagic {
		return 0, 0, 0, 0, fmt.Errorf("%w: bad magic %#x", ErrCorruptTree, magic)
	}
	if v := binary.LittleEndian.Uint16(buf[6:8]); v != treeVersion {
		return 0, 0, 0, 0, fmt.Errorf("%w: unknown version %d", ErrCorruptTree, v)
	}
	level = binary.LittleEndian.Uint16(buf[4:6])
	if level != LevelLeaf && level != LevelIndex {
		return 0, 0, 0, 0, fmt.Errorf("%w: bad level %d", ErrCorruptTree, level)
	}
	count = int(binary.LittleEndian.Uint32(buf[12:16]))
	if count > MaxNodeEntries {
		return 0, 0, 0, 0, fmt.Errorf("%w: %d entries", ErrCorruptTree, count)
	}
	return level, binary.LittleEndian.Uint32(buf[8:12]), count, binary.LittleEndian.Uint64(buf[16:24]), nil
}

// parseTreeNode decodes and validates one node half.
func parseTreeNode(buf []byte) (*TreeNode, error) {
	level, inodeNum, count, gen, err := treeNodeHeader(buf)
	if err != nil {
		return nil, err
	}
	end := nodeHeaderSize + count*nodeEntrySize
	if len(buf) < end {
		return nil, fmt.Errorf("%w: short node", ErrCorruptTree)
	}
	if binary.LittleEndian.Uint32(buf[60:64]) != nodeChecksum(buf, count) {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptTree)
	}

	n := &TreeNode{Level: level, Inode: inodeNum, Gen: gen}
	for off := nodeHeaderSize; off < end; off += nodeEntrySize {
		e := buf[off : off+nodeEntrySize]
		if level == LevelLeaf {
			x, _ := DeserializeExtent(e)
			n.Extents = append(n.Extents, *x)
			continue
		}
		n.Children = append(n.Children, TreeChild{
			First: binary.LittleEndian.Uint64(e[0:8]),
			Block: binary.LittleEndian.Uint64(e[8:16]),
			Gen:   binary.LittleEndian.Uint64(e[16:24]),
			Count: binary.LittleEndian.Uint64(e[24:32]),
		})
	}
	if err := n.validate(); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *TreeNode) validate() error {
	if n.Level == LevelLeaf {
		for i, x := range n.Extents {
			if x.Length == 0 {
				return fmt.Errorf("%w: empty extent at %d", ErrCorruptTree, i)
			}
			if i > 0 {
				prev := n.Extents[i-1]
				if x.LogicalBlock < prev.LogicalBlock+prev.Length {
					return fmt.Errorf("%w: extents %d and %d overlap", ErrCorruptTree, i-1, i)
				}
			}
		}
		return nil
	}
	if len(n.Children) == 0 || n.Children[0].First != 0 {
		return fmt.Errorf("%w: index must start at logical block 0", ErrCorruptTree)
	}
	for i, c := range n.Children {
		if c.Count == 0 {
			return fmt.Errorf("%w: empty child %d", ErrCorruptTree, i)
		}
		if i > 0 && c.First <= n.Children[i-1].First {
			return fmt.Errorf("%w: children %d and %d out of order", ErrCorruptTree, i-1, i)
		}
	}
	return nil
}

func encodeTreeNode(buf []byte, n *TreeNode) {
	count := n.count()
	binary.LittleEndian.PutUint32(buf[0:4], treeMagic)
	binary.LittleEndian.PutUint16(buf[4:6], n.Level)
	binary.LittleEndian.PutUint16(buf[6:8], treeVersion)
	binary.LittleEndian.PutUint32(buf[8:12], n.Inode)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(count))
	binary.LittleEndian.PutUint64(buf[16:24], n.Gen)
	for i := 0; i < count; i++ {
		e := buf[nodeHeaderSize+i*nodeEntrySize : nodeHeaderSize+(i+1)*nodeEntrySize]
		if n.Level == LevelLeaf {
			copy(e, SerializeExtent(&n.Extents[i]))
			continue
		}
		c := n.Children[i]
		binary.LittleEndian.PutUint64(e[0:8], c.First)
		binary.LittleEndian.PutUint64(e[8:16], c.Block)
		binary.LittleEndian.PutUint64(e[16:24], c.Gen)
		binary.LittleEndian.PutUint64(e[24:32], c.Count)
	}
	binary.LittleEndian.PutUint32(buf[60:64], nodeChecksum(buf, count))
}

func nodeChecksum(buf []byte, count int) uint32 {
	crc := storage.CRC32C(buf[:60])
	return storage.CRC32CUpdate(crc, buf[nodeHeaderSize:nodeHeaderSize+count*nodeEntrySize])
}

// nextGen returns a generation newer than committed with the opposite parity, so
// the new version lands in the other half. Seeding it from the clock keeps
// (inode, block, gen) unique when blocks are freed and reused, which the node
// cache relies on.
func nextGen(committed uint64) uint64 {
	g := uint64(time.Now().UnixNano())
	if g <= committed {
		g = committed + 1
	}
	if g&1 == committed&1 {
		g++
	}
	return g
}

type nodeKey struct {
	inode      uint32
	block, gen uint64
}

// nodeCache is an LRU of immutable decoded nodes, bounded by total entry count.
type nodeCache struct {
	mu      sync.Mutex
	max     int
	size    int
	order   *list.List
	entries map[nodeKey]*list.Element
}

type cachedNode struct {
	key  nodeKey
	node *TreeNode
}

func newNodeCache(max int) *nodeCache {
	return &nodeCache{max: max, order: list.New(), entries: make(map[nodeKey]*list.Element)}
}

func (c *nodeCache) get(k nodeKey) (*TreeNode, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[k]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*cachedNode).node, true
}

func (c *nodeCache) put(k nodeKey, n *TreeNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[k]; ok {
		c.size -= el.Value.(*cachedNode).node.count() + 1
		el.Value = &cachedNode{key: k, node: n}
		c.order.MoveToFront(el)
	} else {
		c.entries[k] = c.order.PushFront(&cachedNode{key: k, node: n})
	}
	c.size += n.count() + 1
	for c.size > c.max && c.order.Len() > 1 {
		oldest := c.order.Back()
		cn := c.order.Remove(oldest).(*cachedNode)
		delete(c.entries, cn.key)
		c.size -= cn.node.count() + 1
	}
}

func (m *Manager) loadNode(inodeNum uint32, block, gen uint64, level uint16) (*TreeNode, error) {
	k := nodeKey{inodeNum, block, gen}
	if n, ok := m.nodes.get(k); ok {
		return n, nil
	}
	var n *TreeNode
	var err error
	for attempt := 0; attempt < nodeReadAttempts; attempt++ {
		if n, err = m.readNode(inodeNum, block, gen); err == nil || !errors.Is(err, ErrCorruptTree) {
			break
		}
		// A concurrent writer may be replacing this half; read it again.
		time.Sleep(nodeRetryInterval)
	}
	if err != nil {
		return nil, fmt.Errorf("inode %d tree node %d: %w", inodeNum, block, err)
	}
	if n.Level != level {
		return nil, fmt.Errorf("%w: inode %d node %d has level %d, want %d", ErrCorruptTree, inodeNum, block, n.Level, level)
	}
	m.nodes.put(k, n)
	return n, nil
}

func (m *Manager) readNode(inodeNum uint32, block, gen uint64) (*TreeNode, error) {
	n, err := ReadTreeNode(m.dev, m.dataOffset, block, gen)
	if err != nil {
		return nil, err
	}
	if n.Inode != inodeNum || n.Gen != gen {
		return nil, fmt.Errorf("%w: half holds inode %d gen %d, want inode %d gen %d", ErrStaleTree, n.Inode, n.Gen, inodeNum, gen)
	}
	return n, nil
}

// ReadTreeNode reads and decodes the half of a node block that holds
// generation gen. Callers must check the returned Inode and Gen.
func ReadTreeNode(dev *storage.Device, dataOffset, block, gen uint64) (*TreeNode, error) {
	off := TreeNodeOffset(dataOffset, block, gen)
	head := storage.GetBuffer(storage.SectorSize)
	defer storage.PutBuffer(head)
	buf := head.Bytes()[:storage.SectorSize]
	if _, err := dev.ReadAt(buf, off); err != nil {
		return nil, err
	}
	_, _, count, _, err := treeNodeHeader(buf)
	if err != nil {
		return nil, err
	}
	if size := treeNodeSize(count); size > storage.SectorSize {
		full := storage.GetBuffer(size)
		defer storage.PutBuffer(full)
		big := full.Bytes()[:size]
		copy(big, buf)
		if _, err := dev.ReadAt(big[storage.SectorSize:], off+storage.SectorSize); err != nil {
			return nil, err
		}
		buf = big
	}
	return parseTreeNode(buf)
}

func (m *Manager) writeNode(block uint64, n *TreeNode) error {
	size := treeNodeSize(n.count())
	ab := storage.GetBuffer(size)
	defer storage.PutBuffer(ab)
	buf := ab.Bytes()[:size]
	encodeTreeNode(buf, n)
	_, err := m.dev.WriteAt(buf, TreeNodeOffset(m.dataOffset, block, n.Gen))
	return err
}
