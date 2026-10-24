package fsck

import (
	"encoding/binary"
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
