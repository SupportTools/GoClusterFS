package storage

import (
	"testing"
	"unsafe"
)

func TestAlignedBuffer(t *testing.T) {
	sizes := []int{512, 4096, 65536, 1 << 20}

	for _, size := range sizes {
		t.Run(formatSize(size), func(t *testing.T) {
			buf := NewAlignedBuffer(size)

			// Check length is at least requested size (may be rounded up)
			if buf.Len() < size {
				t.Errorf("buffer length %d < requested %d", buf.Len(), size)
			}

			// Check alignment
			addr := uintptr(unsafe.Pointer(&buf.Bytes()[0]))
			if addr%SectorSize != 0 {
				t.Errorf("buffer address %x not aligned to %d bytes", addr, SectorSize)
			}

			// Check we can write to the buffer
			for i := 0; i < size; i++ {
				buf.Bytes()[i] = byte(i)
			}

			// Verify writes
			for i := 0; i < size; i++ {
				if buf.Bytes()[i] != byte(i) {
					t.Errorf("buffer[%d] = %d, want %d", i, buf.Bytes()[i], byte(i))
					break
				}
			}
		})
	}
}

func TestAlignedBufferSmallSizes(t *testing.T) {
	// Even tiny sizes should produce aligned buffers
	for size := 1; size <= 512; size++ {
		buf := NewAlignedBuffer(size)

		addr := uintptr(unsafe.Pointer(&buf.Bytes()[0]))
		if addr%SectorSize != 0 {
			t.Errorf("size %d: address %x not aligned", size, addr)
		}

		// Buffer should be at least sector size (rounded up)
		if buf.Len() < SectorSize {
			t.Errorf("size %d: buffer length %d < sector size %d", size, buf.Len(), SectorSize)
		}
	}
}

func TestBufferPool(t *testing.T) {
	pool := NewBufferPool()

	// Get buffers of various sizes
	sizes := []int{512, 4096, 65536, 1 << 20}
	var buffers []*AlignedBuffer

	for _, size := range sizes {
		buf := pool.Get(size)
		if buf == nil {
			t.Fatalf("pool.Get(%d) returned nil", size)
		}
		if buf.Len() < size {
			t.Errorf("pool.Get(%d) returned buffer of length %d", size, buf.Len())
		}

		// Write test pattern
		for i := 0; i < min(size, 100); i++ {
			buf.Bytes()[i] = byte(i + size)
		}

		buffers = append(buffers, buf)
	}

	// Return buffers to pool
	for _, buf := range buffers {
		pool.Put(buf)
	}

	// Get them again - should be zeroed
	for _, size := range sizes {
		buf := pool.Get(size)
		for i := 0; i < min(size, 100); i++ {
			if buf.Bytes()[i] != 0 {
				t.Errorf("recycled buffer not zeroed at position %d", i)
				break
			}
		}
		pool.Put(buf)
	}
}

func TestDefaultPool(t *testing.T) {
	// Test the global default pool
	buf := GetBuffer(4096)
	if buf == nil {
		t.Fatal("GetBuffer returned nil")
	}

	addr := uintptr(unsafe.Pointer(&buf.Bytes()[0]))
	if addr%SectorSize != 0 {
		t.Errorf("default pool buffer not aligned: %x", addr)
	}

	// Should not panic
	PutBuffer(buf)
	PutBuffer(nil) // nil should be safe
}

func TestRoundToPoolSize(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{1, SectorSize},                            // 1 -> 4096
		{512, SectorSize},                          // 512 -> 4096
		{4096, SectorSize},                         // 4096 -> 4096 (SectorSize = MetadataBlockSize now)
		{4097, 64 * 1024},                          // 4097 -> 64KB
		{65536, 64 * 1024},                         // 64KB -> 64KB
		{65537, 256 * 1024},                        // 65537 -> 256KB
		{262144, 256 * 1024},                       // 256KB -> 256KB
		{262145, DefaultBlockSize},                 // 262145 -> 1MB
		{DefaultBlockSize, DefaultBlockSize},       // 1MB -> 1MB
		{DefaultBlockSize + 1, 2 * DefaultBlockSize}, // 1MB+1 -> 2MB
	}

	for _, tt := range tests {
		got := roundToPoolSize(tt.input)
		if got != tt.expected {
			t.Errorf("roundToPoolSize(%d) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

func BenchmarkAlignedBufferAlloc(b *testing.B) {
	sizes := []int{512, 4096, 1 << 20}

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				buf := NewAlignedBuffer(size)
				_ = buf.Bytes()
			}
		})
	}
}

func BenchmarkBufferPool(b *testing.B) {
	pool := NewBufferPool()
	sizes := []int{512, 4096, 1 << 20}

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				buf := pool.Get(size)
				pool.Put(buf)
			}
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
