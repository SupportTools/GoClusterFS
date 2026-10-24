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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sort"
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

var _ inode.TreeFlusher = (*Manager)(nil)

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

// treeRef identifies a committed tree as recorded in an inode.
type treeRef struct {
	present   bool
	root, gen uint64
	depth     uint32
}

func committedTree(ino *inode.Inode) treeRef {
	if !ino.HasExtentTree() {
		return treeRef{}
	}
	return treeRef{present: true, root: ino.ExtentTreeRoot, gen: ino.ExtentTreeGen, depth: ino.ExtentDepth}
}

// leafRef describes one leaf. exts holds the working copy once a leaf has been
// edited; otherwise the leaf is read from disk by (block, gen).
type leafRef struct {
	first  uint64
	block  uint64
	placed bool // block is allocated
	gen    uint64
	count  int
	exts   []Extent
	owned  bool // exts belongs to the current editor
}

func (l *leafRef) size() int {
	if l.exts != nil {
		return len(l.exts)
	}
	return l.count
}

// pendingTree is the uncommitted layout stored in inode.PendingTree.
// No leaves means the extents folded back into the inode.
type pendingTree struct {
	base     treeRef
	index    uint64
	hasIndex bool
	leaves   []*leafRef
}

func pendingOf(ino *inode.Inode) *pendingTree {
	p, _ := ino.PendingTree.(*pendingTree)
	return p
}

// layout returns ino's leaves, or nil when its extents are inline.
func (m *Manager) layout(ino *inode.Inode) ([]*leafRef, error) {
	if p := pendingOf(ino); p != nil {
		return p.leaves, nil
	}
	return m.committedLayout(ino.InodeNum, committedTree(ino))
}

func (m *Manager) committedLayout(inodeNum uint32, t treeRef) ([]*leafRef, error) {
	if !t.present {
		return nil, nil
	}
	switch t.depth {
	case 1:
		root, err := m.loadNode(inodeNum, t.root, t.gen, LevelLeaf)
		if err != nil {
			return nil, err
		}
		return []*leafRef{{block: t.root, placed: true, gen: t.gen, count: len(root.Extents)}}, nil
	case 2:
		root, err := m.loadNode(inodeNum, t.root, t.gen, LevelIndex)
		if err != nil {
			return nil, err
		}
		leaves := make([]*leafRef, len(root.Children))
		for i, c := range root.Children {
			leaves[i] = &leafRef{first: c.First, block: c.Block, placed: true, gen: c.Gen, count: int(c.Count)}
		}
		return leaves, nil
	default:
		return nil, fmt.Errorf("%w: inode %d has tree depth %d", ErrCorruptTree, inodeNum, t.depth)
	}
}

// lookupLeaf returns the extents of the leaf whose range contains lb, or nil
// when ino's extents are inline. The result must not be modified.
func (m *Manager) lookupLeaf(ino *inode.Inode, lb uint64) ([]Extent, error) {
	if p := pendingOf(ino); p != nil {
		if len(p.leaves) == 0 {
			return nil, nil
		}
		return m.leafExtents(ino.InodeNum, p.leaves[route(p.leaves, lb)])
	}
	t := committedTree(ino)
	if !t.present || t.depth != 2 {
		leaves, err := m.committedLayout(ino.InodeNum, t)
		if err != nil || len(leaves) == 0 {
			return nil, err
		}
		return m.leafExtents(ino.InodeNum, leaves[0])
	}
	root, err := m.loadNode(ino.InodeNum, t.root, t.gen, LevelIndex)
	if err != nil {
		return nil, err
	}
	i := sort.Search(len(root.Children), func(i int) bool { return root.Children[i].First > lb }) - 1
	child := root.Children[i]
	leaf, err := m.loadNode(ino.InodeNum, child.Block, child.Gen, LevelLeaf)
	if err != nil {
		return nil, err
	}
	return leaf.Extents, nil
}

// leafExtents returns a leaf's extents. The result must not be modified.
func (m *Manager) leafExtents(inodeNum uint32, l *leafRef) ([]Extent, error) {
	if l.exts != nil {
		return l.exts, nil
	}
	n, err := m.loadNode(inodeNum, l.block, l.gen, LevelLeaf)
	if err != nil {
		return nil, err
	}
	return n.Extents, nil
}

// route returns the index of the leaf whose key range contains lb.
func route(leaves []*leafRef, lb uint64) int {
	return sort.Search(len(leaves), func(i int) bool { return leaves[i].first > lb }) - 1
}

// search returns the index of the first extent that ends after lb.
func search(exts []Extent, lb uint64) int {
	return sort.Search(len(exts), func(i int) bool { return exts[i].LogicalBlock+exts[i].Length > lb })
}

func find(exts []Extent, lb uint64) (int, bool) {
	i := search(exts, lb)
	return i, i < len(exts) && exts[i].LogicalBlock <= lb
}

func overlapsRange(exts []Extent, from, to uint64) bool {
	i := search(exts, from)
	return i < len(exts) && exts[i].LogicalBlock < to
}

func mergeable(a, b Extent) bool {
	return a.Flags == b.Flags && a.Flags&FlagSFB == 0 &&
		a.LogicalBlock+a.Length == b.LogicalBlock &&
		a.PhysicalBlock+a.Length == b.PhysicalBlock
}

// insertSorted adds x to a sorted, non-overlapping slice, merging it with
// adjacent neighbours. x must not overlap any extent in exts.
func insertSorted(exts []Extent, x Extent) []Extent {
	j := search(exts, x.LogicalBlock)
	if j > 0 && mergeable(exts[j-1], x) {
		exts[j-1].Length += x.Length
		if j < len(exts) && mergeable(exts[j-1], exts[j]) {
			exts[j-1].Length += exts[j].Length
			exts = append(exts[:j], exts[j+1:]...)
		}
		return exts
	}
	if j < len(exts) && mergeable(x, exts[j]) {
		exts[j].LogicalBlock = x.LogicalBlock
		exts[j].PhysicalBlock = x.PhysicalBlock
		exts[j].Length += x.Length
		return exts
	}
	exts = append(exts, Extent{})
	copy(exts[j+1:], exts[j:])
	exts[j] = x
	return exts
}

// cutRange removes logical blocks [from, to) from exts, splitting extents at the
// boundaries. It returns the remaining extents and the removed pieces.
func cutRange(exts []Extent, from, to uint64) (kept, removed []Extent) {
	kept = make([]Extent, 0, len(exts)+1)
	for _, x := range exts {
		end := x.LogicalBlock + x.Length
		if end <= from || x.LogicalBlock >= to {
			kept = append(kept, x)
			continue
		}
		lo, hi := max(x.LogicalBlock, from), min(end, to)
		if x.LogicalBlock < lo {
			kept = append(kept, Extent{x.LogicalBlock, x.PhysicalBlock, lo - x.LogicalBlock, x.Flags})
		}
		removed = append(removed, Extent{lo, x.PhysicalBlock + (lo - x.LogicalBlock), hi - lo, x.Flags})
		if hi < end {
			kept = append(kept, Extent{hi, x.PhysicalBlock + (hi - x.LogicalBlock), end - hi, x.Flags})
		}
	}
	return kept, removed
}

func inlineExtents(ino *inode.Inode) []Extent {
	exts := make([]Extent, 0, MaxInlineExtents+1)
	for _, ie := range ino.InlineExtents {
		if ie.Length > 0 {
			exts = append(exts, Extent{ie.LogicalBlock, ie.PhysicalBlock, ie.Length, ie.Flags})
		}
	}
	sort.Slice(exts, func(i, j int) bool { return exts[i].LogicalBlock < exts[j].LogicalBlock })
	return exts
}

// editor applies changes to an inode's extent map. Inline maps are edited as
// a single unplaced leaf; commit decides whether the result stays inline,
// becomes a depth-1 tree, or needs an index.
type editor struct {
	m        *Manager
	ctx      context.Context
	ino      *inode.Inode
	base     treeRef
	index    uint64
	hasIndex bool
	leaves   []*leafRef
	changed  bool
}

func (m *Manager) edit(ctx context.Context, ino *inode.Inode) (*editor, error) {
	e := &editor{m: m, ctx: ctx, ino: ino}
	var leaves []*leafRef
	if p := pendingOf(ino); p != nil {
		e.base, e.index, e.hasIndex = p.base, p.index, p.hasIndex
		leaves = p.leaves
	} else {
		e.base = committedTree(ino)
		var err error
		if leaves, err = m.committedLayout(ino.InodeNum, e.base); err != nil {
			return nil, err
		}
		if e.base.depth == 2 {
			e.index, e.hasIndex = e.base.root, true
		}
	}
	if len(leaves) == 0 {
		e.leaves = []*leafRef{{exts: inlineExtents(ino), owned: true}}
		return e, nil
	}
	e.leaves = make([]*leafRef, len(leaves))
	for i, l := range leaves {
		c := *l
		c.owned = false
		e.leaves[i] = &c
	}
	return e, nil
}

func (e *editor) extentsAt(i int) ([]Extent, error) {
	return e.m.leafExtents(e.ino.InodeNum, e.leaves[i])
}

func (e *editor) mutable(i int) ([]Extent, error) {
	l := e.leaves[i]
	if !l.owned {
		exts, err := e.extentsAt(i)
		if err != nil {
			return nil, err
		}
		l.exts = append(make([]Extent, 0, len(exts)+1), exts...)
		l.owned = true
	}
	e.changed = true
	return l.exts, nil
}

func (e *editor) set(i int, exts []Extent) {
	e.leaves[i].exts = exts
	e.changed = true
}

func (e *editor) find(lb uint64) (leaf, idx int, ok bool, err error) {
	leaf = route(e.leaves, lb)
	exts, err := e.extentsAt(leaf)
	if err != nil {
		return 0, 0, false, err
	}
	idx, ok = find(exts, lb)
	return leaf, idx, ok, nil
}

type piece struct {
	leaf int
	ext  Extent
}

// split cuts x at leaf boundaries so every piece fits in one leaf.
func (e *editor) split(x Extent) []piece {
	var out []piece
	for i := route(e.leaves, x.LogicalBlock); ; i++ {
		end := x.LogicalBlock + x.Length
		if i+1 >= len(e.leaves) || e.leaves[i+1].first >= end || x.Flags&FlagSFB != 0 {
			return append(out, piece{i, x})
		}
		n := e.leaves[i+1].first - x.LogicalBlock
		out = append(out, piece{i, Extent{x.LogicalBlock, x.PhysicalBlock, n, x.Flags}})
		x = Extent{x.LogicalBlock + n, x.PhysicalBlock + n, x.Length - n, x.Flags}
	}
}

func (e *editor) insert(x Extent) error {
	pieces := e.split(x)
	for _, p := range pieces {
		exts, err := e.extentsAt(p.leaf)
		if err != nil {
			return err
		}
		if overlapsRange(exts, p.ext.LogicalBlock, p.ext.LogicalBlock+p.ext.Length) {
			return ErrExtentOverlap
		}
	}
	for _, p := range pieces {
		exts, err := e.mutable(p.leaf)
		if err != nil {
			return err
		}
		e.set(p.leaf, insertSorted(exts, p.ext))
	}
	return nil
}

// removeRange unmaps logical blocks [from, to) and returns the removed pieces.
func (e *editor) removeRange(from, to uint64) ([]Extent, error) {
	var removed []Extent
	for i := route(e.leaves, from); i < len(e.leaves) && e.leaves[i].first < to; i++ {
		exts, err := e.extentsAt(i)
		if err != nil {
			return nil, err
		}
		if !overlapsRange(exts, from, to) {
			continue
		}
		kept, cut := cutRange(exts, from, to)
		e.leaves[i].owned = true
		e.set(i, kept)
		removed = append(removed, cut...)
	}
	return removed, nil
}

// update applies fn to the extent containing lb and reports whether one exists.
func (e *editor) update(lb uint64, fn func(*Extent)) (bool, error) {
	leaf, idx, ok, err := e.find(lb)
	if err != nil || !ok {
		return false, err
	}
	exts, err := e.mutable(leaf)
	if err != nil {
		return false, err
	}
	fn(&exts[idx])
	return true, nil
}

func (e *editor) updateAll(fn func(*Extent)) error {
	for i := range e.leaves {
		exts, err := e.mutable(i)
		if err != nil {
			return err
		}
		for j := range exts {
			fn(&exts[j])
		}
	}
	return nil
}

// commit stores the edited map back into the inode: inline when it fits,
// otherwise as a tree. Blocks are allocated before anything is changed and
// dropped tree blocks are freed last, so a failed commit leaves the inode as it was.
func (e *editor) commit() error {
	if !e.changed {
		return nil
	}
	leaves, dropped := e.normalize()

	total := 0
	for _, l := range leaves {
		total += l.size()
	}
	if total <= MaxInlineExtents {
		return e.fold(leaves, dropped)
	}
	if len(leaves) > e.m.indexCap {
		return ErrExtentTreeFull
	}

	var unplaced []*leafRef
	for _, l := range leaves {
		if !l.placed {
			unplaced = append(unplaced, l)
		}
	}
	needIndex := len(leaves) > 1 && !e.hasIndex
	need := len(unplaced)
	if needIndex {
		need++
	}
	blocks, err := e.m.allocTreeBlocks(e.ctx, need)
	if err != nil {
		return err
	}
	for i, l := range unplaced {
		l.block, l.placed, l.gen = blocks[i], true, 0
	}
	if needIndex {
		e.index, e.hasIndex = blocks[len(unplaced)], true
	}
	if len(leaves) == 1 && e.hasIndex {
		dropped = append(dropped, e.index)
		e.index, e.hasIndex = 0, false
	}

	ino := e.ino
	ino.InlineExtents = [inode.MaxInlineExtents]inode.InlineExtent{}
	ino.Flags |= inode.FlagHasExtentTree
	if e.hasIndex {
		ino.ExtentDepth, ino.ExtentTreeRoot = 2, e.index
	} else {
		ino.ExtentDepth, ino.ExtentTreeRoot = 1, leaves[0].block
	}
	ino.ExtentCount = uint64(total)
	ino.PendingTree = &pendingTree{base: e.base, index: e.index, hasIndex: e.hasIndex, leaves: leaves}
	e.m.freeTreeBlocks(e.ctx, dropped)
	return nil
}

// normalize drops empty leaves and splits leaves that exceed the node capacity.
// The last leaf is split into full chunks because appends land there; other
// leaves are split evenly.
func (e *editor) normalize() (leaves []*leafRef, dropped []uint64) {
	maxEntries := e.m.leafCap
	for i, l := range e.leaves {
		switch {
		case l.exts == nil || len(l.exts) <= maxEntries && len(l.exts) > 0:
			leaves = append(leaves, l)
		case len(l.exts) == 0:
			if l.placed {
				dropped = append(dropped, l.block)
			}
		default:
			per := maxEntries
			if i < len(e.leaves)-1 {
				parts := (len(l.exts) + maxEntries - 1) / maxEntries
				per = (len(l.exts) + parts - 1) / parts
			}
			all := l.exts
			l.exts = all[:per:per]
			leaves = append(leaves, l)
			for start := per; start < len(all); start += per {
				end := min(start+per, len(all))
				chunk := all[start:end:end]
				leaves = append(leaves, &leafRef{first: chunk[0].LogicalBlock, exts: chunk, owned: true})
			}
		}
	}
	if len(leaves) > 0 {
		leaves[0].first = 0
	}
	return leaves, dropped
}

// fold moves all extents back into the inode and releases the tree blocks.
func (e *editor) fold(leaves []*leafRef, dropped []uint64) error {
	var all []Extent
	for _, l := range leaves {
		exts, err := e.m.leafExtents(e.ino.InodeNum, l)
		if err != nil {
			return err
		}
		all = append(all, exts...)
		if l.placed {
			dropped = append(dropped, l.block)
		}
	}
	if e.hasIndex {
		dropped = append(dropped, e.index)
	}

	ino := e.ino
	ino.InlineExtents = [inode.MaxInlineExtents]inode.InlineExtent{}
	for i, x := range all {
		ino.InlineExtents[i] = inode.InlineExtent{
			LogicalBlock:  x.LogicalBlock,
			PhysicalBlock: x.PhysicalBlock,
			Length:        x.Length,
			Flags:         x.Flags,
		}
	}
	ino.Flags &^= inode.FlagHasExtentTree
	ino.ExtentTreeRoot, ino.ExtentDepth = 0, 0
	ino.ExtentCount = uint64(len(all))
	if e.base.present {
		ino.PendingTree = &pendingTree{base: e.base}
	} else {
		ino.PendingTree = nil
	}
	e.m.freeTreeBlocks(e.ctx, dropped)
	return nil
}

// FlushTree implements inode.TreeFlusher. It writes every edited node into the
// half its committed copy does not use and points the inode at the new root.
func (m *Manager) FlushTree(ino, onDisk *inode.Inode) (func(), error) {
	p := pendingOf(ino)
	if p == nil {
		return nil, fmt.Errorf("extent: unexpected pending tree type %T", ino.PendingTree)
	}
	if committedTree(onDisk) != p.base {
		return nil, fmt.Errorf("%w: inode %d", ErrTreeConflict, ino.InodeNum)
	}
	if len(p.leaves) == 0 {
		ino.ExtentTreeGen = 0
		return nil, nil
	}

	committed, err := m.committedGens(ino.InodeNum, p.base)
	if err != nil {
		return nil, err
	}
	type written struct {
		block uint64
		node  *TreeNode
	}
	var out []written
	write := func(block uint64, n *TreeNode) error {
		n.Inode = ino.InodeNum
		n.Gen = nextGen(committed[block])
		if err := m.writeNode(block, n); err != nil {
			return err
		}
		out = append(out, written{block, n})
		return nil
	}

	gens := make([]uint64, len(p.leaves))
	total := 0
	for i, l := range p.leaves {
		total += l.size()
		if l.exts == nil {
			gens[i] = l.gen
			continue
		}
		n := &TreeNode{Level: LevelLeaf, Extents: l.exts}
		if err := write(l.block, n); err != nil {
			return nil, err
		}
		gens[i] = n.Gen
	}

	root, rootGen, depth := p.leaves[0].block, gens[0], uint32(1)
	if p.hasIndex {
		kids := make([]TreeChild, len(p.leaves))
		for i, l := range p.leaves {
			kids[i] = TreeChild{First: l.first, Block: l.block, Gen: gens[i], Count: uint64(l.size())}
		}
		n := &TreeNode{Level: LevelIndex, Children: kids}
		if err := write(p.index, n); err != nil {
			return nil, err
		}
		root, rootGen, depth = p.index, n.Gen, 2
	}

	ino.Flags |= inode.FlagHasExtentTree
	ino.ExtentTreeRoot, ino.ExtentTreeGen, ino.ExtentDepth = root, rootGen, depth
	ino.ExtentCount = uint64(total)
	return func() {
		for _, w := range out {
			m.nodes.put(nodeKey{ino.InodeNum, w.block, w.node.Gen}, w.node)
		}
	}, nil
}

// committedGens maps each block of a committed tree to its committed generation.
func (m *Manager) committedGens(inodeNum uint32, t treeRef) (map[uint64]uint64, error) {
	gens := make(map[uint64]uint64)
	if !t.present {
		return gens, nil
	}
	gens[t.root] = t.gen
	if t.depth == 2 {
		root, err := m.loadNode(inodeNum, t.root, t.gen, LevelIndex)
		if err != nil {
			return nil, err
		}
		for _, c := range root.Children {
			gens[c.Block] = c.Gen
		}
	}
	return gens, nil
}

type reservationKey struct{}

type reservation struct {
	mu     sync.Mutex
	blocks []uint64
}

// ReserveTreeBlocks pre-allocates the tree blocks that inserting one more
// extent into ino may need. Callers that insert while holding an inode lock
// must reserve before taking it, because allocating takes the bitmap lock,
// which comes earlier in the lock order. Pass the returned context to Insert
// and call release after the inode lock is dropped.
func (m *Manager) ReserveTreeBlocks(ctx context.Context, ino *inode.Inode) (context.Context, func(), error) {
	noop := func() {}
	need, err := m.treeBlocksForInsert(ino)
	if err != nil || need == 0 {
		return ctx, noop, err
	}
	blocks, err := m.bitmap.Alloc(ctx, need)
	if err != nil {
		return ctx, noop, err
	}
	r := &reservation{blocks: blocks}
	release := func() {
		r.mu.Lock()
		left := r.blocks
		r.blocks = nil
		r.mu.Unlock()
		m.freeTreeBlocks(context.Background(), left)
	}
	return context.WithValue(ctx, reservationKey{}, r), release, nil
}

func (m *Manager) treeBlocksForInsert(ino *inode.Inode) (int, error) {
	leaves, err := m.layout(ino)
	if err != nil {
		return 0, err
	}
	switch {
	case len(leaves) == 0:
		if len(inlineExtents(ino)) < MaxInlineExtents {
			return 0, nil
		}
		n := (MaxInlineExtents + m.leafCap) / m.leafCap
		if n > 1 {
			n++
		}
		return n, nil
	case len(leaves) == 1 && leaves[0].size() < m.leafCap:
		return 0, nil
	case len(leaves) == 1:
		return 2, nil
	default:
		return 1, nil
	}
}

func (m *Manager) allocTreeBlocks(ctx context.Context, n int) ([]uint64, error) {
	if n == 0 {
		return nil, nil
	}
	var out []uint64
	r, _ := ctx.Value(reservationKey{}).(*reservation)
	if r != nil {
		r.mu.Lock()
		k := min(n, len(r.blocks))
		out = append(out, r.blocks[len(r.blocks)-k:]...)
		r.blocks = r.blocks[:len(r.blocks)-k]
		r.mu.Unlock()
	}
	if len(out) < n {
		more, err := m.bitmap.Alloc(ctx, n-len(out))
		if err != nil {
			if r != nil {
				r.mu.Lock()
				r.blocks = append(r.blocks, out...)
				r.mu.Unlock()
			}
			return nil, fmt.Errorf("allocate extent tree block: %w", err)
		}
		out = append(out, more...)
	}
	return out, nil
}

func (m *Manager) freeTreeBlocks(ctx context.Context, blocks []uint64) {
	for _, b := range blocks {
		if err := m.bitmap.Free(ctx, b); err != nil {
			log.Printf("[extent] free tree block %d: %v", b, err)
		}
	}
}
