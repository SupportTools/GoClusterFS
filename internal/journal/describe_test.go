package journal

import (
	"fmt"
	"testing"
)

func TestEntryTouches(t *testing.T) {
	for _, tt := range []struct {
		op      TxnOpType
		payload []byte
		want    Touches
	}{
		{TxnOpInodeCreate, (&InodeCreatePayload{InodeNum: 12, ParentInodeNum: 2}).Serialize(), Touches{Inodes: []uint64{12, 2}}},
		{TxnOpInodeUpdate, (&InodeUpdatePayload{InodeNum: 12}).Serialize(), Touches{Inodes: []uint64{12}}},
		{TxnOpLinkAdd, (&LinkAddPayload{InodeNum: 12}).Serialize(), Touches{Inodes: []uint64{12}}},
		{TxnOpDirRename, (&DirRenamePayload{OldParentInodeNum: 2, NewParentInodeNum: 7, InodeNum: 12, TargetInodeNum: 30, OldName: "a", NewName: "b"}).Serialize(),
			Touches{Inodes: []uint64{2, 7, 12, 30}}},
		{TxnOpBlockAlloc, (&BlockAllocPayload{InodeNum: 12, StartBlock: 100, Count: 4}).Serialize(), Touches{Inodes: []uint64{12}, Blocks: []BlockRun{{100, 4}}}},
		{TxnOpExtentAdd, (&ExtentAddPayload{InodeNum: 12, DiskBlock: 300, Length: 2}).Serialize(), Touches{Inodes: []uint64{12}, Blocks: []BlockRun{{300, 2}}}},
		{TxnOpBlockReserve, serializeBlockRuns([]uint64{9, 5, 6, 7}), Touches{Blocks: []BlockRun{{5, 3}, {9, 1}}}},
		{TxnOpRefCountUpdate, (&RefCountUpdatePayload{BlockNum: 44, OldCount: 1, NewCount: 2}).Serialize(), Touches{Blocks: []BlockRun{{44, 1}}}},
		{TxnOpSnapshotDelete, (&SnapshotDeletePayload{SnapshotID: 3}).Serialize(), Touches{}},
	} {
		got, err := EntryTouches(tt.op, tt.payload)
		if err != nil || fmt.Sprint(got) != fmt.Sprint(tt.want) {
			t.Errorf("%s: %+v, %v; want %+v", tt.op, got, err, tt.want)
		}
	}
	if _, err := EntryTouches(TxnOpExtentAdd, []byte{1, 2}); err == nil {
		t.Error("a truncated ExtentAdd payload decoded")
	}
	if _, err := EntryTouches(TxnOpType(0x7777), nil); err == nil {
		t.Error("an unknown entry type decoded")
	}
}
