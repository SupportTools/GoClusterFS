package fsck

import (
	"iter"
	"math/bits"
)

// blockSet holds one bit per data block, so a volume of many small blocks
// costs fsck an eighth of a byte per block instead of a map entry.
type blockSet struct {
	words []uint64
	count uint64
}

func newBlockSet(blocks uint64) blockSet {
	return blockSet{words: make([]uint64, (blocks+63)/64)}
}

func (s *blockSet) has(b uint64) bool {
	return b/64 < uint64(len(s.words)) && s.words[b/64]&(1<<(b%64)) != 0
}

func (s *blockSet) add(b uint64) {
	if !s.has(b) {
		s.words[b/64] |= 1 << (b % 64)
		s.count++
	}
}

func (s *blockSet) remove(b uint64) {
	if s.has(b) {
		s.words[b/64] &^= 1 << (b % 64)
		s.count--
	}
}

// all yields the blocks in the set in ascending order.
func (s *blockSet) all() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		for i, w := range s.words {
			for w != 0 {
				if !yield(uint64(i)*64 + uint64(bits.TrailingZeros64(w))) {
					return
				}
				w &= w - 1
			}
		}
	}
}

// blockRefs counts references to each data block in one byte per block, and
// keeps the rare counts above 254 in a map.
type blockRefs struct {
	small    []uint8
	large    map[uint64]int
	referred uint64
}

const largeRefs = 255

func newBlockRefs(blocks uint64) blockRefs {
	return blockRefs{small: make([]uint8, blocks), large: make(map[uint64]int)}
}

func (r *blockRefs) get(b uint64) int {
	if b >= uint64(len(r.small)) {
		return 0
	}
	if r.small[b] == largeRefs {
		return r.large[b]
	}
	return int(r.small[b])
}

func (r *blockRefs) add(b uint64, delta int) {
	old := r.get(b)
	n := max(old+delta, 0)
	switch {
	case old == 0 && n > 0:
		r.referred++
	case old > 0 && n == 0:
		r.referred--
	}
	if n >= largeRefs {
		r.small[b] = largeRefs
		r.large[b] = n
		return
	}
	r.small[b] = uint8(n)
	delete(r.large, b)
}

// all yields every block with references, in ascending order.
func (r *blockRefs) all() iter.Seq2[uint64, int] {
	return func(yield func(uint64, int) bool) {
		for b, n := range r.small {
			if n != 0 && !yield(uint64(b), r.get(uint64(b))) {
				return
			}
		}
	}
}
