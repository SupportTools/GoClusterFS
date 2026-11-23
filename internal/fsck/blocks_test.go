package fsck

import (
	"maps"
	"slices"
	"testing"
)

func TestBlockSetMatchesAMap(t *testing.T) {
	const blocks = 200
	s := newBlockSet(blocks)
	want := map[uint64]bool{}
	for _, b := range []uint64{0, 63, 64, 65, 127, 128, 199, 64} {
		s.add(b)
		want[b] = true
	}
	s.remove(65)
	s.remove(66)
	delete(want, 65)

	if got := slices.Collect(s.all()); !slices.Equal(got, slices.Sorted(maps.Keys(want))) {
		t.Fatalf("all() = %v, want %v", got, slices.Sorted(maps.Keys(want)))
	}
	if s.count != uint64(len(want)) {
		t.Fatalf("count = %d, want %d", s.count, len(want))
	}
	for b := range uint64(blocks + 70) {
		if s.has(b) != want[b] {
			t.Fatalf("has(%d) = %v, want %v", b, s.has(b), want[b])
		}
	}
}

func TestBlockRefsCountsPastOneByte(t *testing.T) {
	r := newBlockRefs(10)
	r.add(3, 1)
	for range 300 {
		r.add(7, 1)
	}
	if r.get(7) != 300 || r.referred != 2 {
		t.Fatalf("get(7) = %d, referred = %d, want 300 and 2", r.get(7), r.referred)
	}
	r.add(7, -100)
	if r.get(7) != 200 || len(r.large) != 0 {
		t.Fatalf("get(7) = %d with %d large counts, want 200 kept in a byte", r.get(7), len(r.large))
	}
	r.add(3, -1)
	if r.get(3) != 0 || r.referred != 1 {
		t.Fatalf("get(3) = %d, referred = %d, want 0 and 1", r.get(3), r.referred)
	}

	var got []uint64
	for b, n := range r.all() {
		got = append(got, b, uint64(n))
	}
	if !slices.Equal(got, []uint64{7, 200}) {
		t.Fatalf("all() = %v, want [7 200]", got)
	}
	if r.get(50) != 0 {
		t.Fatalf("get past the end = %d, want 0", r.get(50))
	}
}
