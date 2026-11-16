package fsck_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

const (
	replayInodes  = 64
	replayTimeout = 30 * time.Second
)

var replayOwner = [16]byte{0x7e, 0x9a}

// sectorRun is a stretch of non-zero sectors of the pristine image.
type sectorRun struct {
	off  int64
	data []byte
}

// pristineImage is the fuzz image before any replay. Restoring it punches the
// whole sparse file out and writes its non-zero sectors back.
type pristineImage struct {
	size int64
	runs []sectorRun
	gens map[uint64]uint64          // inode -> MetaGeneration
	maps map[uint64]map[uint64]bool // inode -> blocks it maps
}

func snapshotImage(tb testing.TB, dev *storage.Device) *pristineImage {
	p := &pristineImage{size: dev.Size()}
	chunk := storage.NewAlignedBuffer(1 << 20)
	for off := int64(0); off < p.size; off += 1 << 20 {
		n := int(min(1<<20, p.size-off))
		if _, err := dev.ReadAt(chunk.Bytes()[:n], off); err != nil {
			tb.Fatal(err)
		}
		for s := 0; s < n; s += storage.SectorSize {
			sector := chunk.Bytes()[s : s+storage.SectorSize]
			if bytes.Count(sector, []byte{0}) == len(sector) {
				continue
			}
			at := off + int64(s)
			if last := len(p.runs) - 1; last >= 0 && p.runs[last].off+int64(len(p.runs[last].data)) == at {
				p.runs[last].data = append(p.runs[last].data, sector...)
				continue
			}
			p.runs = append(p.runs, sectorRun{off: at, data: bytes.Clone(sector)})
		}
	}
	p.gens, p.maps = inodeMaps(newReplayContext(dev))
	return p
}

func (p *pristineImage) restore(t *testing.T, dev *storage.Device) {
	if err := unix.Fallocate(int(dev.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 0, p.size); err != nil {
		t.Fatalf("punch out the image: %v", err)
	}
	for _, r := range p.runs {
		buf := storage.NewAlignedBuffer(len(r.data))
		copy(buf.Bytes(), r.data)
		if _, err := dev.WriteAt(buf.Bytes(), r.off); err != nil {
			t.Fatal(err)
		}
	}
}

// newReplayContext builds the managers a mount hands to replay, without locks.
func newReplayContext(dev *storage.Device) *journal.RecoveryContext {
	var uuid [16]byte
	bm := bitmap.NewRegion(dev, fuzzBitmap, fuzzBlocks, nil, uuid)
	tbl := inode.NewTable(dev, fuzzInodes, testLayout.InodeTableSize, nil, uuid)
	mgr := extent.NewManager(dev, bm, nil, fuzzDataOffset)
	tbl.SetTreeFlusher(mgr)
	mgr.SetRefcounts(refcount.NewRegion(dev, fuzzRefcounts, refcount.SizeForBlocks(fuzzBlocks), fuzzBlocks, nil))
	return &journal.RecoveryContext{Dev: dev, InodeTbl: tbl, BitmapMgr: bm, DirMgr: dir.NewManager(dev, tbl, mgr, nil, fuzzDataOffset), ExtentMgr: mgr}
}

// inodeMaps reads the generation and the blocks of every in-use inode below replayInodes.
func inodeMaps(rctx *journal.RecoveryContext) (map[uint64]uint64, map[uint64]map[uint64]bool) {
	gens := make(map[uint64]uint64)
	maps := make(map[uint64]map[uint64]bool)
	for num := uint32(0); num < replayInodes; num++ {
		ino, err := rctx.InodeTbl.Read(num)
		if err != nil {
			continue
		}
		gens[uint64(num)] = ino.MetaGeneration
		if ino.Magic != inode.InodeMagic {
			continue
		}
		blocks := map[uint64]bool{}
		if ino.HasExtentTree() {
			blocks[ino.ExtentTreeRoot] = true
		}
		if data, err := rctx.ExtentMgr.DataBlocks(ino); err == nil {
			for _, b := range data {
				blocks[b] = true
			}
		}
		maps[uint64(num)] = blocks
	}
	return gens, maps
}

// recordGen turns fuzz bytes into journal transactions that name the image's
// inodes, blocks and names, and notes every inode and block they name.
type recordGen struct {
	data   []byte
	gens   map[uint64]uint64
	inodes map[uint64]bool
	blocks map[uint64]bool
}

func (g *recordGen) byte() byte {
	if len(g.data) == 0 {
		return 0
	}
	b := g.data[0]
	g.data = g.data[1:]
	return b
}

func (g *recordGen) u64() uint64 {
	var buf [8]byte
	for i := range buf {
		buf[i] = g.byte()
	}
	return binary.LittleEndian.Uint64(buf[:])
}

func (g *recordGen) inode() uint64 {
	var num uint64
	switch b := g.byte(); b % 12 {
	case 7:
		num = 1
	case 8:
		num = 0
	case 9:
		num = 4095
	case 10:
		num = 4096
	case 11:
		num = g.u64()
	default:
		num = 2 + uint64(b%7)
	}
	g.inodes[num] = true
	return num
}

// gen is usually the generation the image has for num, so that the replay
// guards let the record through.
func (g *recordGen) gen(num uint64) uint64 {
	if b := g.byte(); b%4 == 0 {
		return g.u64()
	}
	return g.gens[num]
}

func (g *recordGen) block() uint64 {
	var b uint64
	switch v := g.byte(); v % 10 {
	case 8:
		b = g.u64()
	case 9:
		b = fuzzBlocks + uint64(v)
	default:
		b = uint64(v) % (fuzzBlocks + 2)
	}
	g.blocks[b] = true
	return b
}

func (g *recordGen) span(start uint64, count uint64) {
	for i := uint64(0); i < count && i < 8; i++ {
		g.blocks[start+i] = true
	}
}

func (g *recordGen) name() string {
	known := []string{"plain", "a", "b", "clone", ".", ".."}
	if b := g.byte(); int(b%8) < len(known) {
		return known[b%8]
	}
	n := 1 + int(g.byte()%8)
	name := make([]byte, n)
	for i := range name {
		name[i] = 'a' + g.byte()%30
	}
	return string(name)
}

// add logs one record into txn and reports whether it ended the transaction.
func (g *recordGen) add(txn *journal.Transaction) (bool, error) {
	switch op := g.byte(); op % 12 {
	case 0:
		return true, nil
	case 1:
		num := g.inode()
		return false, txn.LogInodeCreate(&journal.InodeCreatePayload{InodeNum: num, Mode: uint32(inode.S_IFREG | 0o644),
			ParentInodeNum: g.inode(), MetaGeneration: g.gen(num)})
	case 2:
		return false, txn.LogInodeUpdate(&journal.InodeUpdatePayload{InodeNum: g.inode(), UpdateMask: journal.InodeUpdateMask(g.byte())})
	case 3:
		num := g.inode()
		return false, txn.LogInodeDelete(&journal.InodeDeletePayload{InodeNum: num, MetaGeneration: g.gen(num)})
	case 4:
		return false, txn.LogDirAddEntry(&journal.DirAddEntryPayload{ParentInodeNum: g.inode(), ChildInodeNum: g.inode(),
			EntryType: journal.DirEntryType(1 + g.byte()%2), Name: g.name()})
	case 5:
		parent, child := g.inode(), g.inode()
		return false, txn.LogDirRemoveEntry(&journal.DirRemoveEntryPayload{ParentInodeNum: parent, ChildInodeNum: child,
			EntryType: journal.DirEntryRegular, Name: g.name(), ChildMetaGeneration: g.gen(child)})
	case 6, 7:
		num, start, count := g.inode(), g.block(), uint32(1+g.byte()%3)
		g.span(start, uint64(count))
		if op%12 == 6 {
			return false, txn.LogBlockAlloc(&journal.BlockAllocPayload{InodeNum: num, StartBlock: start, Count: count})
		}
		return false, txn.LogBlockFree(&journal.BlockFreePayload{InodeNum: num, StartBlock: start, Count: count})
	case 8:
		num, disk, length := g.inode(), g.block(), uint32(1+g.byte()%3)
		g.span(disk, uint64(length))
		return false, txn.LogExtentAdd(&journal.ExtentAddPayload{InodeNum: num, FileBlock: uint64(g.byte() % 8), DiskBlock: disk,
			Length: length, Flags: journal.ExtentFlags(g.byte() % 4), MetaGeneration: g.gen(num), Size: uint64(g.byte()) << 18})
	case 9:
		num := g.inode()
		return false, txn.LogExtentFree(&journal.ExtentFreePayload{InodeNum: num, MetaGeneration: g.gen(num), Size: uint64(g.byte()) << 18})
	default:
		blocks := make([]uint64, 1+g.byte()%4)
		for i := range blocks {
			blocks[i] = g.block()
		}
		if op%12 == 10 {
			return false, txn.LogBlockReserve(blocks)
		}
		return false, txn.LogBlockUse(blocks)
	}
}

// writeJournal writes the transactions the fuzz bytes describe into the
// journal of registry slot 0, as a node that then crashed would have left
// them, and tears the bytes tear names.
func writeJournal(t *testing.T, dev *storage.Device, sb *superblock.Superblock, g *recordGen, completeMask uint8, tear []byte) {
	j, err := journal.Init(dev, sb.JournalRegionOffset, sb.JournalSlotSize(), replayOwner, 7, journal.Config{CommitMode: journal.CommitSync})
	if err != nil {
		t.Fatal(err)
	}
	for txns := 0; len(g.data) > 0 && txns < 32; txns++ {
		txn := j.Begin()
		entries := 0
		for len(g.data) > 0 && entries < 16 {
			end, err := g.add(txn)
			if err != nil {
				t.Fatalf("log a record: %v", err)
			}
			if end {
				break
			}
			entries++
		}
		if entries == 0 {
			j.Abort(txn)
			continue
		}
		seq, err := j.Commit(txn)
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		if completeMask&(1<<(txns%8)) != 0 {
			if err := j.CompleteIntent(seq); err != nil {
				t.Fatalf("complete intent %d: %v", seq, err)
			}
		}
	}
	for len(tear) >= 3 {
		off := int64(sb.JournalRegionOffset) + journal.JournalDataOffset + int64(binary.LittleEndian.Uint16(tear))
		sector := off &^ (storage.SectorSize - 1)
		buf := storage.NewAlignedBuffer(storage.SectorSize)
		if _, err := dev.ReadAt(buf.Bytes(), sector); err != nil {
			t.Fatal(err)
		}
		buf.Bytes()[off-sector] ^= tear[2] | 1
		if _, err := dev.WriteAt(buf.Bytes(), sector); err != nil {
			t.Fatal(err)
		}
		tear = tear[3:]
	}
}

var (
	inodeNumRe = regexp.MustCompile(`inodes? (\d+)`)
	blockNumRe = regexp.MustCompile(`(?:blocks?|tree node) (\d+)(?:-(\d+))?`)
)

// unexplained returns the fsck errors that name no inode or block the
// records named or that those objects map, before or after the replay.
func unexplained(problems []fsck.Problem, inodes, blocks map[uint64]bool) []string {
	var out []string
	for _, p := range problems {
		if p.Severity < fsck.Error || p.Location == "blocks" || p.Location == "inodes" {
			continue
		}
		text := p.Location + ": " + p.Message
		explained := false
		for _, m := range inodeNumRe.FindAllStringSubmatch(text, -1) {
			if n, err := strconv.ParseUint(m[1], 10, 64); err == nil && inodes[n] {
				explained = true
			}
		}
		for _, m := range blockNumRe.FindAllStringSubmatch(text, -1) {
			first, err := strconv.ParseUint(m[1], 10, 64)
			if err != nil {
				continue
			}
			last := first
			if m[2] != "" {
				last, _ = strconv.ParseUint(m[2], 10, 64)
			}
			for b := first; b <= last && b < first+64; b++ {
				if blocks[b] {
					explained = true
				}
			}
		}
		if !explained {
			out = append(out, p.String())
		}
	}
	return out
}

// FuzzReplay replays journals of well-formed records over a small filesystem,
// as a mount does for a crashed node. Replay must finish, and every error fsck
// then finds must name an inode or block the records named, or one that such
// an inode maps or that maps such a block: replay must not damage what the
// records never touched.
func FuzzReplay(f *testing.F) {
	f.Cleanup(fsck.SetHeartbeatWatch(0, 0))
	dev, _ := fuzzImage(f)
	sb, err := superblock.Read(dev)
	if err != nil {
		f.Fatal(err)
	}
	pristine := snapshotImage(f, dev)

	const plain, a, b, clone, free = 3, 4, 5, 6, 7
	create := concat(op(1, inodeByte(free), inodeByte(2), 1), op(4, inodeByte(2), inodeByte(free), 0, 6, 1, 'n', 'w'), []byte{0})
	grow := concat(op(10, 1, blockByte(20), blockByte(21)), []byte{0},
		op(8, inodeByte(a), blockByte(20), 0, 5, 1, 1, 24), op(11, 0, blockByte(20)), []byte{0})
	unlink := concat(op(5, inodeByte(2), inodeByte(b), 2, 1), op(3, inodeByte(b), 1), op(9, inodeByte(b), 1, 0), []byte{0})
	shared := concat(op(5, inodeByte(2), inodeByte(plain), 0, 1), op(3, inodeByte(plain), 1), op(9, inodeByte(plain), 1, 0), []byte{0})
	f.Add(create, uint8(0), []byte{})
	f.Add(concat(create, grow), uint8(0), []byte{})
	f.Add(concat(grow, unlink), uint8(1), []byte{})
	f.Add(concat(unlink, shared, op(8, inodeByte(clone), blockByte(22), 1, 0, 1, 1, 8)), uint8(0), []byte{})
	f.Add(concat(create, unlink), uint8(0), []byte{0x40, 0, 0x55})
	f.Add(concat(grow, create), uint8(0xfe), []byte{0x10, 0x10, 1})

	f.Fuzz(func(t *testing.T, records []byte, completeMask uint8, tear []byte) {
		log.SetOutput(io.Discard)
		defer log.SetOutput(io.Discard)
		pristine.restore(t, dev)
		g := &recordGen{data: records, gens: pristine.gens, inodes: map[uint64]bool{}, blocks: map[uint64]bool{}}
		writeJournal(t, dev, sb, g, completeMask, tear)

		rctx := newReplayContext(dev)
		info := &node.FailedNodeInfo{SlotIndex: 0, UUID: replayOwner, JournalOffset: sb.JournalRegionOffset, JournalSize: sb.JournalSlotSize()}
		done := make(chan *fsck.Result, 1)
		go func() {
			_, _ = journal.ReplayJournal(context.Background(), rctx, info)
			done <- fsck.New(dev, fsck.Options{Output: io.Discard}).Check()
		}()
		var result *fsck.Result
		select {
		case result = <-done:
		case <-time.After(replayTimeout):
			t.Fatalf("replay and fsck did not finish within %v", replayTimeout)
		}

		_, after := inodeMaps(newReplayContext(dev))
		for _, maps := range []map[uint64]map[uint64]bool{pristine.maps, after} {
			for num := range g.inodes {
				for b := range maps[num] {
					g.blocks[b] = true
				}
			}
		}
		for _, maps := range []map[uint64]map[uint64]bool{pristine.maps, after} {
			for num, blocks := range maps {
				for b := range blocks {
					if g.blocks[b] {
						g.inodes[num] = true
					}
				}
			}
		}
		if bad := unexplained(result.Problems, g.inodes, g.blocks); len(bad) > 0 {
			t.Fatalf("fsck found damage no record explains (records named inodes %v, blocks %v):\n%s",
				keys(g.inodes), keys(g.blocks), fmt.Sprint(bad))
		}
	})
}

// inodeByte and blockByte are the record bytes that make recordGen pick
// inode num (2 to 8) or block b.
func inodeByte(num uint64) byte { return byte(num - 2) }

func blockByte(b uint64) byte {
	for v := b; v < 256; v += fuzzBlocks + 2 {
		if v%10 < 8 {
			return byte(v)
		}
	}
	panic(fmt.Sprintf("no record byte picks block %d", b))
}

func op(code byte, args ...byte) []byte { return append([]byte{code}, args...) }

func concat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func keys(m map[uint64]bool) []uint64 {
	var out []uint64
	for k := range m {
		out = append(out, k)
	}
	return out
}
