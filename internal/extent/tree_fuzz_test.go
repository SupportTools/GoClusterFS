package extent

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func FuzzParseTreeNode(f *testing.F) {
	leaf := &TreeNode{Level: LevelLeaf, Inode: 7, Gen: 3, Extents: []Extent{
		{LogicalBlock: 0, PhysicalBlock: 100, Length: 2, Flags: FlagAllocated},
		{LogicalBlock: 5, PhysicalBlock: 200, Length: 1, Flags: FlagAllocated | FlagShared | FlagCounted},
	}}
	index := &TreeNode{Level: LevelIndex, Inode: 7, Gen: 4, Children: []TreeChild{
		{First: 0, Block: 10, Gen: 3, Count: 2},
		{First: 50, Block: 11, Gen: 5, Count: 1},
	}}
	for _, n := range []*TreeNode{leaf, index} {
		buf := make([]byte, treeNodeSize(n.count()))
		encodeTreeNode(buf, n)
		f.Add(buf, false)
		f.Add(buf[:nodeHeaderSize+nodeEntrySize], true)
	}
	f.Add([]byte{}, true)

	f.Fuzz(func(t *testing.T, buf []byte, fixChecksum bool) {
		if fixChecksum && len(buf) >= nodeHeaderSize {
			count := int(binary.LittleEndian.Uint32(buf[12:16]))
			if count <= MaxNodeEntries && nodeHeaderSize+count*nodeEntrySize <= len(buf) {
				buf = bytes.Clone(buf)
				binary.LittleEndian.PutUint32(buf[60:64], nodeChecksum(buf, count))
			}
		}
		n, err := parseTreeNode(buf)
		if err != nil {
			return
		}
		out := make([]byte, treeNodeSize(n.count()))
		encodeTreeNode(out, n)
		again, err := parseTreeNode(out)
		if err != nil {
			t.Fatalf("decoding a re-encoded node: %v", err)
		}
		if !reflect.DeepEqual(n, again) {
			t.Fatalf("node changed after an encode/decode round trip: %+v != %+v", n, again)
		}
	})
}
