package goclusterfs

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/sfb"
)

const staleMarker = "FILE-A-DATA!"

// markerData fills n bytes with a pattern no other test writes, so its bytes are recognizable wherever they show up.
func markerData(n int) []byte {
	return bytes.Repeat([]byte(staleMarker), n/len(staleMarker)+1)[:n]
}

// freedMarkerBlocks writes a file of marker data, deletes it, and returns the blocks it freed, which the next
// allocations reuse.
func freedMarkerBlocks(t *testing.T, fsys *FS) []uint64 {
	t.Helper()
	a := createFile(t, fsys, "a")
	writeAt(t, a, markerData(8*BlockSize), 0)
	var blocks []uint64
	for i := range uint64(8) {
		blocks = append(blocks, physicalBlock(t, fsys, a.inodeNum, i))
	}
	if errno := fsys.Unlink(context.Background(), "a"); errno != 0 {
		t.Fatalf("unlink a: %v", errno)
	}
	return blocks
}

// expectContent requires f to hold exactly want, and never the deleted file's marker.
func expectContent(t *testing.T, f *File, want []byte) {
	t.Helper()
	got := readWhole(t, f)
	if i := bytes.Index(got, []byte(staleMarker)); i >= 0 {
		t.Fatalf("inode %d shows the deleted file's data at byte %d", f.inodeNum, i)
	}
	if !bytes.Equal(got, want) {
		i := 0
		for i < min(len(got), len(want)) && got[i] == want[i] {
			i++
		}
		t.Fatalf("inode %d: %d bytes, want %d; first difference at byte %d", f.inodeNum, len(got), len(want), i)
	}
}

// expectReused requires logical block n of f to sit in one of the freed blocks, so the test reads reused disk space.
func expectReused(t *testing.T, fsys *FS, f *File, n uint64, freed []uint64) {
	t.Helper()
	if b := physicalBlock(t, fsys, f.inodeNum, n); !slices.Contains(freed, b) {
		t.Fatalf("block %d of inode %d is disk block %d, none of the freed blocks %v", n, f.inodeNum, b, freed)
	}
}

func zerosThen(n int, data []byte) []byte { return append(make([]byte, n), data...) }

// No read of a file returns bytes nobody wrote to it: not a deleted file's data in a reused block, and not the
// file's own data from before a truncate.
func TestReadsNeverShowStaleBytes(t *testing.T) {
	data := pattern(9, 100)
	cases := []struct {
		name string
		run  func(t *testing.T, fsys *FS, b *File, freed []uint64) []byte
	}{
		{"SparseWriteIntoNewBlock", func(t *testing.T, fsys *FS, b *File, freed []uint64) []byte {
			writeAt(t, b, data, BlockSize+BlockSize/2)
			expectReused(t, fsys, b, 1, freed)
			return zerosThen(BlockSize+BlockSize/2, data)
		}},
		{"GrowWithinANewBlock", func(t *testing.T, fsys *FS, b *File, freed []uint64) []byte {
			writeAt(t, b, data, 0)
			expectReused(t, fsys, b, 0, freed)
			writeAt(t, b, data, 600000)
			return append(append(slices.Clone(data), make([]byte, 600000-len(data))...), data...)
		}},
		{"TruncateDownThenUp", func(t *testing.T, fsys *FS, b *File, _ []uint64) []byte {
			writeAt(t, b, pattern(3, 900000), 0)
			truncateFile(t, b, 1000)
			truncateFile(t, b, 900000)
			return append(pattern(3, 1000), make([]byte, 899000)...)
		}},
		{"TruncateDownThenSparseWrite", func(t *testing.T, fsys *FS, b *File, _ []uint64) []byte {
			writeAt(t, b, pattern(3, 50000), 825473)
			truncateFile(t, b, 291147)
			writeAt(t, b, data, 1736920)
			return zerosThen(1736920, data)
		}},
		{"FallocateThenPartialWrite", func(t *testing.T, fsys *FS, b *File, freed []uint64) []byte {
			if errno := b.Allocate(context.Background(), nil, 0, 2*BlockSize, 0); errno != 0 {
				t.Fatalf("fallocate: %v", errno)
			}
			writeAt(t, b, data, BlockSize+BlockSize/2)
			expectReused(t, fsys, b, 1, freed)
			return append(zerosThen(BlockSize+BlockSize/2, data), make([]byte, BlockSize/2-len(data))...)
		}},
		{"CopyOnWriteOfAPartialBlock", func(t *testing.T, fsys *FS, b *File, _ []uint64) []byte {
			writeAt(t, b, data, 0)
			setxattr(t, b, "user.snapshot", "s")
			writeAt(t, b, data, 200000)
			return append(append(slices.Clone(data), make([]byte, 200000-len(data))...), data...)
		}},
		{"CloneThenGrow", func(t *testing.T, fsys *FS, b *File, _ []uint64) []byte {
			writeAt(t, b, data, 0)
			setxattr(t, b, "user.clone", "/c")
			c := lookupFile(t, fsys, "c")
			writeAt(t, c, data, 700000)
			want := append(append(slices.Clone(data), make([]byte, 700000-len(data))...), data...)
			expectContent(t, c, want)
			return data
		}},
		{"CloneOfASnapshotOfATruncatedFile", func(t *testing.T, fsys *FS, b *File, _ []uint64) []byte {
			writeAt(t, b, pattern(3, 900000), 0)
			truncateFile(t, b, 1000)
			setxattr(t, b, "user.snapshot", "s")
			setxattr(t, b, "user.snapshot.clone", "s:/sc")
			sc := lookupFile(t, fsys, "sc")
			truncateFile(t, sc, 900000)
			expectContent(t, sc, append(pattern(3, 1000), make([]byte, 899000)...))
			return pattern(3, 1000)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fsys := openFormattedFS(t, newFormattedImage(t))
			freed := freedMarkerBlocks(t, fsys)
			b := createFile(t, fsys, "b")
			want := c.run(t, fsys, b, freed)
			expectContent(t, b, want)
		})
	}
}

// A write that takes its block from the node's reserve zeroes it like any new block.
func TestReservedBlockNeverShowsStaleBytes(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	freed := freedMarkerBlocks(t, fsys)
	withReserve(t, fsys, 16, time.Hour)
	b := createFile(t, fsys, "b")
	data := pattern(9, 100)
	writeAt(t, b, data, BlockSize/2)
	expectReused(t, fsys, b, 0, freed)
	expectContent(t, b, zerosThen(BlockSize/2, data))
}

func shrinkIgnoringErrors(f *File, size uint64) {
	in := &fuse.SetAttrIn{}
	in.Valid, in.Size = fuse.FATTR_SIZE, size
	_ = f.Setattr(context.Background(), nil, in, &fuse.AttrOut{})
}

// Power fails at each write of a truncate into the middle of a block. Growing the file afterwards reads zeros past
// the cut, or the file's own bytes where an unacknowledged truncate had not zeroed yet; never anything else.
func TestPowerCutDuringTruncateIntoABlock(t *testing.T) {
	const cut, full = 1000, 900000
	old := pattern(3, full)
	var b *File
	var sizeAfterCut uint64
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			b = createFile(t, fsys, "b")
			writeAt(t, b, old, 0)
		},
		op: func(fsys *FS, _ *power) { shrinkIgnoringErrors(b, cut) },
		afterMount: func(t *testing.T, fsys *FS) {
			f := lookupFile(t, fsys, "b")
			ino, err := fsys.inodeTbl.Read(f.inodeNum)
			if err != nil {
				t.Fatal(err)
			}
			sizeAfterCut = ino.Size
			truncateFile(t, f, full)
		},
		check: func(t *testing.T, fsys *FS) {
			got := readWhole(t, lookupFile(t, fsys, "b"))
			if len(got) != full || !bytes.Equal(got[:cut], old[:cut]) {
				t.Fatalf("%d bytes, want %d starting with the file's first %d", len(got), full, cut)
			}
			zero := make([]byte, 4096)
			for off := cut; off < full; {
				end := min((off/4096+1)*4096, full)
				isZero := bytes.Equal(got[off:end], zero[:end-off])
				if !isZero && (sizeAfterCut == cut || !bytes.Equal(got[off:end], old[off:end])) {
					t.Fatalf("after a cut that left size %d, bytes %d-%d are not zeros", sizeAfterCut, off, end)
				}
				off = end
			}
		},
	})
}

// Power fails at each write of a partial first write into a block a deleted file freed, with and without the reserve.
func TestPowerCutDuringFirstWriteIntoAReusedBlock(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		t.Run(fmt.Sprintf("reserve=%v", reserved), func(t *testing.T) {
			data := pattern(9, 100)
			var b *File
			runPowerCuts(t, powerCutCase{
				setup: func(t *testing.T, fsys *FS) {
					freedMarkerBlocks(t, fsys)
					if reserved {
						withReserve(t, fsys, 16, time.Hour)
					}
					b = createFile(t, fsys, "b")
				},
				op: func(fsys *FS, _ *power) { _, _ = b.Write(context.Background(), nil, data, BlockSize/2) },
				check: func(t *testing.T, fsys *FS) {
					got := readWhole(t, lookupFile(t, fsys, "b"))
					if bytes.Contains(got, []byte(staleMarker)) {
						t.Fatal("the new file shows the deleted file's data")
					}
					if len(got) != 0 && !bytes.Equal(got, zerosThen(BlockSize/2, data)) {
						t.Fatalf("%d bytes that are neither empty nor what was written", len(got))
					}
				},
			})
		})
	}
}

// A truncate moves a small file older versions kept in an SFB slot into a block of its own. Growing it again must
// read zeros past the cut, not the bytes the move copied.
func TestSFBFileTruncatedThenGrownReadsZeros(t *testing.T) {
	fsys := openTreeFS(t, newTreeFSImage(t))
	container := newSFBContainer(t, fsys, sfb.Tier16KB)
	data := pattern(5, 10000)
	f := newSFBFile(t, fsys, container, sfb.Tier16KB, 1, data)
	truncateFile(t, f, 10)
	truncateFile(t, f, 10000)
	expectContent(t, f, append(slices.Clone(data[:10]), make([]byte, 9990)...))
}
