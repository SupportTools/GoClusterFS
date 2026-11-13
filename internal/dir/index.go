package dir

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// DefaultIndexEntries is how many names a mount keeps in directory indexes by default.
const DefaultIndexEntries = 1 << 20

const chunksPerBlock = BlockSize / DirChunkSize

// slot is where a name lives; chunk counts 4 KB chunks from the directory's first block.
type slot struct {
	child uint64
	chunk uint32
	ftype uint8
}

// index holds each name's slot and each chunk's free room, true while the directory inode's MetaGeneration is gen.
type index struct {
	mu    sync.RWMutex
	num   uint32
	gen   uint64
	names map[string]slot
	room  []uint16
}

func (x *index) validAt(gen uint64) bool {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return x.gen == gen
}

func (x *index) find(gen uint64, name string) (s slot, found, valid bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	if x.gen != gen {
		return slot{}, false, false
	}
	s, found = x.names[name]
	return s, found, true
}

// chunkWithRoom returns the first chunk that takes an entry of size need.
func (x *index) chunkWithRoom(need uint16) (uint32, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	for c, room := range x.room {
		if room >= need {
			return uint32(c), true
		}
	}
	return 0, false
}

func (x *index) len() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return len(x.names)
}

// indexCache keeps directory indexes up to limit names in all, dropping the least recently used first.
type indexCache struct {
	mu      sync.Mutex
	limit   int
	entries int
	dirs    map[uint32]*list.Element
	lru     list.List

	hits, misses, rebuilds atomic.Uint64
}

// IndexStats counts directory index use since mount.
type IndexStats struct {
	Dirs     int
	Entries  int
	Limit    int
	Hits     uint64
	Misses   uint64
	Rebuilds uint64
}

func newIndexCache(limit int) *indexCache {
	return &indexCache{limit: limit, dirs: map[uint32]*list.Element{}}
}

func (c *indexCache) get(num uint32) *index {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.dirs[num]
	if !ok {
		return nil
	}
	c.lru.MoveToFront(e)
	return e.Value.(*index)
}

func (c *indexCache) put(x *index) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(x.num)
	if len(x.names) > c.limit {
		return
	}
	c.dirs[x.num] = c.lru.PushFront(x)
	c.entries += len(x.names)
	c.evictLocked()
}

func (c *indexCache) drop(num uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(num)
}

// grew records that x, changed in place, holds delta more names.
func (c *indexCache) grew(x *index, delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.dirs[x.num]; ok && e.Value == x {
		c.entries += delta
		c.evictLocked()
	}
}

func (c *indexCache) removeLocked(num uint32) {
	if e, ok := c.dirs[num]; ok {
		c.entries -= e.Value.(*index).len()
		c.lru.Remove(e)
		delete(c.dirs, num)
	}
}

func (c *indexCache) evictLocked() {
	for c.entries > c.limit && c.lru.Len() > 0 {
		c.removeLocked(c.lru.Back().Value.(*index).num)
	}
}

func (c *indexCache) setLimit(limit int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.limit = limit
	c.evictLocked()
}

func (c *indexCache) stats() IndexStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return IndexStats{
		Dirs:     c.lru.Len(),
		Entries:  c.entries,
		Limit:    c.limit,
		Hits:     c.hits.Load(),
		Misses:   c.misses.Load(),
		Rebuilds: c.rebuilds.Load(),
	}
}

// SetIndexLimit sets how many names this mount keeps in directory indexes; 0 turns them off.
func (m *Manager) SetIndexLimit(entries int) {
	m.indexes.setLimit(max(entries, 0))
}

// IndexStats reports directory index use.
func (m *Manager) IndexStats() IndexStats {
	return m.indexes.stats()
}

// KeepIndex moves directory num's index from generation from to to, for an inode write under the directory lock that changed no entries.
func (m *Manager) KeepIndex(num uint32, from, to uint64) {
	if x := m.indexes.get(num); x != nil {
		x.mu.Lock()
		if x.gen == from {
			x.gen = to
		}
		x.mu.Unlock()
	}
}

// cachedLookup needs no lock: entry changes write the directory inode first, and a torn inode read fails its checksum.
func (m *Manager) cachedLookup(num uint32, name string) (s slot, found, ok bool) {
	x := m.indexes.get(num)
	if x != nil {
		cur, err := m.inodeTbl.Read(num)
		if err == nil && cur.Magic == inode.InodeMagic && cur.IsDir() {
			s, found, ok = x.find(cur.MetaGeneration, name)
		}
	}
	if ok {
		m.indexes.hits.Add(1)
	} else {
		m.indexes.misses.Add(1)
	}
	return s, found, ok
}

// indexOf returns the index of dirIno, read under the directory lock, rebuilding it unless the cached one is current.
func (m *Manager) indexOf(dirIno *inode.Inode) (*index, error) {
	if x := m.indexes.get(dirIno.InodeNum); x != nil && x.validAt(dirIno.MetaGeneration) {
		return x, nil
	}
	x, _, err := m.scan(dirIno)
	if err != nil {
		return nil, err
	}
	m.indexes.put(x)
	return x, nil
}

// scan reads every block of dirIno, read under the directory lock, into an index and a list in disk order.
func (m *Manager) scan(dirIno *inode.Inode) (*index, []DirEntry, error) {
	m.indexes.rebuilds.Add(1)
	blocks := extent.BlocksForSize(dirIno.Size)
	x := &index{
		num:   dirIno.InodeNum,
		gen:   dirIno.MetaGeneration,
		names: map[string]slot{},
		room:  make([]uint16, blocks*chunksPerBlock),
	}
	var entries []DirEntry
	for block := uint64(0); block < blocks; block++ {
		physBlock, err := m.physicalBlock(dirIno, block)
		if err == extent.ErrExtentNotFound {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		data, err := m.readBlock(physBlock)
		if err != nil {
			return nil, nil, err
		}
		for c := range uint64(chunksPerBlock) {
			chunk := data[c*DirChunkSize : (c+1)*DirChunkSize]
			at := uint32(block*chunksPerBlock + c)
			x.room[at] = chunkRoom(chunk)
			walkChunk(chunk, func(_ int, e *DirEntry) bool {
				if e.InodeNum != 0 {
					x.names[e.Name] = slot{child: e.InodeNum, chunk: at, ftype: e.FileType}
					entries = append(entries, *e)
				}
				return false
			})
		}
	}
	return x, entries, nil
}

func (m *Manager) physicalBlock(dirIno *inode.Inode, block uint64) (uint64, error) {
	ext, offset, err := m.extentMgr.Lookup(dirIno, block)
	if err != nil {
		return 0, err
	}
	return ext.PhysicalBlock + offset, nil
}

// editChunk writes the directory inode before the chunk, so no node can read the change under the old generation.
func (m *Manager) editChunk(dirIno *inode.Inode, x *index, at uint32, edit func(chunk []byte) bool, update func(names map[string]slot)) (changed bool, err error) {
	physBlock, err := m.physicalBlock(dirIno, uint64(at/chunksPerBlock))
	if err != nil {
		return false, err
	}
	start := int(at%chunksPerBlock) * DirChunkSize
	chunk, err := m.readChunk(physBlock, start)
	if err != nil {
		return false, err
	}
	if !edit(chunk) {
		m.indexes.drop(x.num)
		return false, nil
	}
	if err := m.inodeTbl.WriteWithLockHeld(dirIno); err != nil {
		m.indexes.drop(x.num)
		return false, fmt.Errorf("failed to write directory inode: %w", err)
	}
	if err := m.writeChunk(physBlock, start, chunk); err != nil {
		m.indexes.drop(x.num)
		return false, err
	}
	x.mu.Lock()
	before := len(x.names)
	update(x.names)
	x.room[at] = chunkRoom(chunk)
	x.gen = dirIno.MetaGeneration
	delta := len(x.names) - before
	x.mu.Unlock()
	m.indexes.grew(x, delta)
	return true, nil
}

func (m *Manager) readChunk(physBlock uint64, start int) ([]byte, error) {
	buf := storage.GetBuffer(DirChunkSize)
	defer storage.PutBuffer(buf)
	if _, err := m.dev.ReadAt(buf.Bytes()[:DirChunkSize], m.blockOffset(physBlock)+int64(start)); err != nil {
		return nil, fmt.Errorf("read block %d chunk %d: %w", physBlock, start/DirChunkSize, err)
	}
	return append([]byte(nil), buf.Bytes()[:DirChunkSize]...), nil
}

// chunkRoom is the size of the largest entry addToChunk can put in chunk.
func chunkRoom(chunk []byte) uint16 {
	if chunkUnused(chunk) {
		return DirChunkSize
	}
	var room uint16
	walkChunk(chunk, func(_ int, e *DirEntry) bool {
		free := e.RecLen
		if e.InodeNum != 0 {
			free = e.RecLen - min(e.RecLen, entrySize(e))
		}
		room = max(room, free)
		return false
	})
	return room
}
