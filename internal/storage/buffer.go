package storage

import (
	"sync"
	"unsafe"
)

// AlignedBuffer provides sector-aligned memory for O_DIRECT I/O.
// Go's make([]byte, n) doesn't guarantee alignment, so we must
// over-allocate and find an aligned starting address.
type AlignedBuffer struct {
	raw     []byte // Over-allocated backing array
	aligned []byte // Aligned slice within raw
}

// NewAlignedBuffer allocates a buffer of the given size with sector alignment.
// The returned buffer is safe for use with O_DIRECT.
func NewAlignedBuffer(size int) *AlignedBuffer {
	// Round size up to sector boundary
	alignedSize := (size + SectorSize - 1) &^ (SectorSize - 1)

	// Over-allocate to ensure we can find an aligned address
	raw := make([]byte, alignedSize+SectorSize)

	// Find the aligned starting address
	addr := uintptr(unsafe.Pointer(&raw[0]))
	offset := (SectorSize - (addr % SectorSize)) % SectorSize

	return &AlignedBuffer{
		raw:     raw,
		aligned: raw[offset : offset+uintptr(alignedSize)],
	}
}

// Bytes returns the aligned byte slice for I/O operations.
func (b *AlignedBuffer) Bytes() []byte {
	return b.aligned
}

// Len returns the length of the aligned buffer.
func (b *AlignedBuffer) Len() int {
	return len(b.aligned)
}

// BufferPool manages a pool of aligned buffers for reuse.
// This reduces allocation overhead for frequent I/O operations.
type BufferPool struct {
	pools map[int]*sync.Pool
	mu    sync.RWMutex
}

// NewBufferPool creates a new buffer pool.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pools: make(map[int]*sync.Pool),
	}
}

// Get retrieves an aligned buffer of at least the specified size.
// The buffer may be larger than requested due to alignment.
func (p *BufferPool) Get(size int) *AlignedBuffer {
	// Round up to common sizes for better pool reuse
	poolSize := roundToPoolSize(size)

	p.mu.RLock()
	pool, ok := p.pools[poolSize]
	p.mu.RUnlock()

	if !ok {
		p.mu.Lock()
		// Double-check after acquiring write lock
		pool, ok = p.pools[poolSize]
		if !ok {
			pool = &sync.Pool{
				New: func() interface{} {
					return NewAlignedBuffer(poolSize)
				},
			}
			p.pools[poolSize] = pool
		}
		p.mu.Unlock()
	}

	return pool.Get().(*AlignedBuffer)
}

// Put returns a buffer to the pool for reuse.
func (p *BufferPool) Put(buf *AlignedBuffer) {
	if buf == nil {
		return
	}

	// Zero the buffer before returning to pool (security)
	for i := range buf.aligned {
		buf.aligned[i] = 0
	}

	poolSize := roundToPoolSize(len(buf.aligned))

	p.mu.RLock()
	pool, ok := p.pools[poolSize]
	p.mu.RUnlock()

	if ok {
		pool.Put(buf)
	}
}

// roundToPoolSize rounds up to common buffer sizes for efficient pooling.
func roundToPoolSize(size int) int {
	switch {
	case size <= SectorSize:
		return SectorSize
	case size <= MetadataBlockSize:
		return MetadataBlockSize
	case size <= 64*1024:
		return 64 * 1024
	case size <= 256*1024:
		return 256 * 1024
	case size <= DefaultBlockSize:
		return DefaultBlockSize
	default:
		// Round up to next MB for very large buffers
		return ((size + DefaultBlockSize - 1) / DefaultBlockSize) * DefaultBlockSize
	}
}

// DefaultPool is a shared buffer pool for general use.
var DefaultPool = NewBufferPool()

// GetBuffer gets an aligned buffer from the default pool.
func GetBuffer(size int) *AlignedBuffer {
	return DefaultPool.Get(size)
}

// PutBuffer returns a buffer to the default pool.
func PutBuffer(buf *AlignedBuffer) {
	DefaultPool.Put(buf)
}
