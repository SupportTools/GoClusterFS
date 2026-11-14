package dir

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

type liveEntry struct {
	name  string
	inode uint64
}

// liveEntries walks chunk and returns its live entries, checking that each
// lies inside the chunk, after the one before, with its name inside its record.
func liveEntries(t *testing.T, chunk []byte) []liveEntry {
	var live []liveEntry
	last := -1
	walkChunk(chunk, func(offset int, e *DirEntry) bool {
		switch {
		case offset <= last:
			t.Fatalf("entry at %d does not follow the one at %d", offset, last)
		case offset+int(e.RecLen) > len(chunk):
			t.Fatalf("entry at %d with RecLen %d runs past the chunk", offset, e.RecLen)
		case DirEntryHeaderSize+int(e.NameLen) > int(e.RecLen):
			t.Fatalf("entry at %d has a %d-byte name that does not fit RecLen %d", offset, e.NameLen, e.RecLen)
		}
		last = offset
		if e.InodeNum != 0 {
			live = append(live, liveEntry{e.Name, e.InodeNum})
		}
		return false
	})
	return live
}

func entryForName(name string, inode uint64) *DirEntry {
	return &DirEntry{InodeNum: inode, NameLen: uint8(len(name)), FileType: DT_REG, Name: name}
}

// FuzzDirChunk edits a directory chunk read from disk the way Add and Remove
// do, and checks that chunkRoom, which picks the chunk for an add, agrees with
// what addToChunk can place.
func FuzzDirChunk(f *testing.F) {
	chunk := make([]byte, DirChunkSize)
	off := 0
	for i, name := range []string{".", "..", "file.txt", "another"} {
		e := entryForName(name, uint64(i+2))
		e.RecLen = entrySize(e)
		if name == "another" {
			e.RecLen = uint16(DirChunkSize - off)
		}
		serializeEntry(e, chunk[off:])
		off += int(e.RecLen)
	}
	f.Add(chunk, "new-name", uint64(9), "file.txt")
	f.Add(chunk[:40], "x", uint64(1), "..")
	f.Add([]byte{}, "fresh", uint64(5), "fresh")
	torn := bytes.Clone(chunk)
	binary.LittleEndian.PutUint16(torn[8:10], 3)
	f.Add(torn, strings.Repeat("y", 255), uint64(7), "missing")

	f.Fuzz(func(t *testing.T, data []byte, name string, inode uint64, remove string) {
		chunk := make([]byte, DirChunkSize)
		copy(chunk, data)
		before := liveEntries(t, chunk)

		if name != "" && len(name) <= MaxNameLen && inode != 0 {
			added := bytes.Clone(chunk)
			e := entryForName(name, inode)
			room, need := chunkRoom(chunk), entrySize(e)
			ok := addToChunk(added, e)
			if ok != (need <= room) {
				t.Fatalf("chunkRoom is %d and the entry needs %d, but addToChunk returned %v", room, need, ok)
			}
			if ok {
				after := liveEntries(t, added)
				if !containsAll(after, before) || !containsAll(after, []liveEntry{{name, inode}}) || len(after) != len(before)+1 {
					t.Fatalf("adding %q to a chunk with %v left %v", name, before, after)
				}
			} else if !bytes.Equal(added, chunk) {
				t.Fatal("addToChunk changed a chunk it could not add to")
			}
		}

		removed := bytes.Clone(chunk)
		ok := removeFromChunk(removed, remove)
		after := liveEntries(t, removed)
		want := slicesWithout(before, remove)
		if ok != (len(want) < len(before)) || !sameEntries(after, want) {
			t.Fatalf("removing %q from %v returned %v and left %v", remove, before, ok, after)
		}
	})
}

// slicesWithout drops the first live entry called name.
func slicesWithout(entries []liveEntry, name string) []liveEntry {
	for i, e := range entries {
		if e.name == name {
			return append(append([]liveEntry(nil), entries[:i]...), entries[i+1:]...)
		}
	}
	return entries
}

func containsAll(have, want []liveEntry) bool {
	count := make(map[liveEntry]int)
	for _, e := range have {
		count[e]++
	}
	for _, e := range want {
		if count[e] == 0 {
			return false
		}
		count[e]--
	}
	return true
}

func sameEntries(a, b []liveEntry) bool {
	return len(a) == len(b) && containsAll(a, b)
}
