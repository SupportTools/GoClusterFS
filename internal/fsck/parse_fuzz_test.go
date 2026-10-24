package fsck

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

func dirEntry(inodeNum uint64, recLen int, name string) []byte {
	e := make([]byte, recLen)
	binary.LittleEndian.PutUint64(e[0:8], inodeNum)
	binary.LittleEndian.PutUint16(e[8:10], uint16(recLen))
	e[10] = byte(len(name))
	e[11] = DT_REG
	copy(e[DirEntryHeaderSize:], name)
	return e
}

func TestParseDirBlockChunkBoundary(t *testing.T) {
	head := append(dirEntry(2, 16, "."), dirEntry(2, 16, "..")...)
	for name, tc := range map[string]struct {
		block   []byte
		entries int
		errText string
	}{
		"fills the chunk":     {append(head, dirEntry(5, DirChunkSize-32, "a.txt")...), 3, ""},
		"empty block":         {nil, 0, ""},
		"stops short":         {append(head, dirEntry(5, 32, "a.txt")...), 3, "entries end at offset 64"},
		"runs past the chunk": {append(dirEntry(2, DirChunkSize+16, "."), make([]byte, 16)...), 0, "runs past the 4096-byte chunk"},
	} {
		buf := make([]byte, DirBlockSize)
		copy(buf, tc.block)
		entries, errs := parseDirBlock(buf)
		got := strings.Join(errs, "; ")
		if len(entries) != tc.entries || (tc.errText == "") != (got == "") || !strings.Contains(got, tc.errText) {
			t.Errorf("%s: %d entries, errors %q; want %d entries, errors containing %q", name, len(entries), got, tc.entries, tc.errText)
		}
	}
}

func FuzzParseDirBlock(f *testing.F) {
	block := append(append(dirEntry(2, 16, "."), dirEntry(2, 16, "..")...), dirEntry(5, DirChunkSize-32, "file.txt")...)
	f.Add(block)
	f.Add(block[:20])
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, buf []byte) {
		entries, _ := parseDirBlock(buf)
		for _, e := range entries {
			if int(e.RecLen) < MinDirEntrySize || len(e.Name) != int(e.NameLen) {
				t.Fatalf("parsed an invalid entry: %+v", e)
			}
		}
	})
}

// FuzzParseRecords feeds arbitrary bytes to the fixed-size record parsers.
// Phases hand them whole records read from disk, so short input is padded.
func FuzzParseRecords(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, storage.SectorSize))
	f.Add([]byte("GOCLUSTERFS\x00\x02\x00\x00\x00"))

	f.Fuzz(func(t *testing.T, data []byte) {
		buf := make([]byte, storage.SectorSize)
		copy(buf, data)
		c := &Checker{}
		c.parseSuperblock(buf)
		c.parseInode(buf[:InodeSize])
		parseRefcountEntry(buf[:RefcountEntrySize])
		parseSnapshotDescriptor(buf[:SnapshotDescriptorSize])
		sectorChecksumValid(buf)
		if err := ValidateSuperblockQuick(data); err == nil && len(data) < SuperblockSize {
			t.Fatalf("accepted a %d-byte superblock", len(data))
		}
	})
}
