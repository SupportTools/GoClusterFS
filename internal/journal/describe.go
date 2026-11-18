package journal

import (
	"fmt"
	"slices"
)

// BlockRun is Count blocks from Start.
type BlockRun struct {
	Start, Count uint64
}

// Touches is what one journal entry changes.
type Touches struct {
	Inodes []uint64
	Blocks []BlockRun
}

// EntryTouches decodes an entry's payload and returns the inodes and blocks it changes.
func EntryTouches(op TxnOpType, payload []byte) (Touches, error) {
	var t Touches
	inodes := func(nums ...uint64) {
		for _, n := range nums {
			if n != 0 {
				t.Inodes = append(t.Inodes, n)
			}
		}
	}
	blocks := func(start, count uint64) {
		if count > 0 {
			t.Blocks = append(t.Blocks, BlockRun{start, count})
		}
	}
	var err error
	switch op {
	case TxnOpInodeCreate:
		var p *InodeCreatePayload
		if p, err = DeserializeInodeCreate(payload); err == nil {
			inodes(p.InodeNum, p.ParentInodeNum)
		}
	case TxnOpInodeUpdate:
		var p *InodeUpdatePayload
		if p, err = DeserializeInodeUpdate(payload); err == nil {
			inodes(p.InodeNum)
		}
	case TxnOpInodeDelete:
		var p *InodeDeletePayload
		if p, err = DeserializeInodeDelete(payload); err == nil {
			inodes(p.InodeNum)
		}
	case TxnOpLinkDrop, TxnOpLinkAdd:
		var p *LinkDropPayload
		if p, err = DeserializeLinkDrop(payload); err == nil {
			inodes(p.InodeNum)
		}
	case TxnOpSymlinkTarget:
		var p *SymlinkTargetPayload
		if p, err = DeserializeSymlinkTarget(payload); err == nil {
			inodes(p.InodeNum)
			if p.DiskBlock != 0 {
				blocks(p.DiskBlock, 1)
			}
		}
	case TxnOpDirAddEntry:
		var p *DirAddEntryPayload
		if p, err = DeserializeDirAddEntry(payload); err == nil {
			inodes(p.ParentInodeNum, p.ChildInodeNum)
		}
	case TxnOpDirRemoveEntry:
		var p *DirRemoveEntryPayload
		if p, err = DeserializeDirRemoveEntry(payload); err == nil {
			inodes(p.ParentInodeNum, p.ChildInodeNum)
		}
	case TxnOpDirRename:
		var p *DirRenamePayload
		if p, err = DeserializeDirRename(payload); err == nil {
			inodes(p.OldParentInodeNum, p.NewParentInodeNum, p.InodeNum, p.TargetInodeNum)
		}
	case TxnOpBlockAlloc:
		var p *BlockAllocPayload
		if p, err = DeserializeBlockAlloc(payload); err == nil {
			inodes(p.InodeNum)
			blocks(p.StartBlock, uint64(p.Count))
		}
	case TxnOpBlockFree:
		var p *BlockFreePayload
		if p, err = DeserializeBlockFree(payload); err == nil {
			inodes(p.InodeNum)
			blocks(p.StartBlock, uint64(p.Count))
		}
	case TxnOpBlockReserve, TxnOpBlockUse:
		var list []uint64
		if list, err = DeserializeBlockRuns(payload); err == nil {
			t.Blocks = runsOf(list)
		}
	case TxnOpSFBAlloc:
		var p *SFBAllocPayload
		if p, err = DeserializeSFBAlloc(payload); err == nil {
			inodes(p.InodeNum)
			blocks(p.ContainerBlock, 1)
		}
	case TxnOpSFBFree:
		var p *SFBFreePayload
		if p, err = DeserializeSFBFree(payload); err == nil {
			blocks(p.ContainerBlock, 1)
		}
	case TxnOpSFBMigrate:
		var p *SFBMigratePayload
		if p, err = DeserializeSFBMigrate(payload); err == nil {
			inodes(p.InodeNum)
			blocks(p.OldContainer, 1)
			blocks(p.NewBlock, 1)
		}
	case TxnOpExtentAdd:
		var p *ExtentAddPayload
		if p, err = DeserializeExtentAdd(payload); err == nil {
			inodes(p.InodeNum)
			blocks(p.DiskBlock, uint64(p.Length))
		}
	case TxnOpExtentRemove:
		var p *ExtentRemovePayload
		if p, err = DeserializeExtentRemove(payload); err == nil {
			inodes(p.InodeNum)
		}
	case TxnOpExtentFree:
		var p *ExtentFreePayload
		if p, err = DeserializeExtentFree(payload); err == nil {
			inodes(p.InodeNum)
		}
	case TxnOpSnapshotCreate:
		var p *SnapshotCreatePayload
		if p, err = DeserializeSnapshotCreate(payload); err == nil {
			inodes(p.RootInodeNum)
		}
	case TxnOpSnapshotDelete:
		_, err = DeserializeSnapshotDelete(payload)
	case TxnOpRefCountUpdate:
		var p *RefCountUpdatePayload
		if p, err = DeserializeRefCountUpdate(payload); err == nil {
			blocks(p.BlockNum, 1)
		}
	default:
		err = fmt.Errorf("unknown entry type %#x", uint32(op))
	}
	if err != nil {
		return Touches{}, fmt.Errorf("%s entry: %w", op, err)
	}
	return t, nil
}

// runsOf turns block numbers into runs of consecutive blocks.
func runsOf(list []uint64) []BlockRun {
	list = slices.Clone(list)
	slices.Sort(list)
	var runs []BlockRun
	for _, b := range list {
		if n := len(runs); n > 0 && runs[n-1].Start+runs[n-1].Count == b {
			runs[n-1].Count++
			continue
		}
		runs = append(runs, BlockRun{b, 1})
	}
	return runs
}
