package goclusterfs

import (
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/sfb"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

func fillBlock(t *testing.T, fsys *FS, physBlock uint64, seed byte) {
	t.Helper()
	buf := storage.GetBuffer(BlockSize)
	defer storage.PutBuffer(buf)
	copy(buf.Bytes(), pattern(seed, BlockSize))
	if _, err := fsys.dev.WriteAt(buf.Bytes(), int64(fsys.dataOffset+physBlock*BlockSize)); err != nil {
		t.Fatalf("fill block %d: %v", physBlock, err)
	}
}

// A partial write into one block of a fallocated range must not expose what
// the disk held before, in that block or in the other fallocated blocks.
func TestFallocatedBlocksReadZerosAfterPartialWrite(t *testing.T) {
	path := newTreeFSImage(t)
	fsys := openTreeFS(t, path)
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatalf("InitRoot: %v", err)
	}

	f := newRegularFile(t, fsys)
	if errno := f.Allocate(ctx, nil, 0, 3*BlockSize, 0); errno != 0 {
		t.Fatalf("Allocate: %v", errno)
	}
	ino, err := fsys.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	for lb := uint64(0); lb < 3; lb++ {
		ext, off, err := fsys.extentMgr.Lookup(ino, lb)
		if err != nil {
			t.Fatalf("lookup block %d: %v", lb, err)
		}
		fillBlock(t, fsys, ext.PhysicalBlock+off, byte(0xA0+lb))
	}

	data := pattern(7, 4096)
	writeAt(t, f, data, BlockSize+8192)

	verify := func(f *File) {
		t.Helper()
		checkAt(t, f, make([]byte, BlockSize), 0)
		checkAt(t, f, make([]byte, 8192), BlockSize)
		checkAt(t, f, data, BlockSize+8192)
		checkAt(t, f, make([]byte, BlockSize-8192-4096), BlockSize+8192+4096)
		checkAt(t, f, make([]byte, BlockSize), 2*BlockSize)
	}
	verify(f)
	verify(&File{fs: openTreeFS(t, path), inodeNum: f.inodeNum})
}

// Physical block 0 is an ordinary data block, not a hole.
func TestReadDataInPhysicalBlockZero(t *testing.T) {
	fsys := openTreeFS(t, newTreeFSImage(t))
	f := newRegularFile(t, fsys)
	data := pattern(3, 4096)
	writeAt(t, f, data, 0)

	ino, err := fsys.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	ext, off, err := fsys.extentMgr.Lookup(ino, 0)
	if err != nil {
		t.Fatal(err)
	}
	if phys := ext.PhysicalBlock + off; phys != 0 {
		t.Fatalf("file got physical block %d; this test needs block 0", phys)
	}
	checkAt(t, f, data, 0)
}

func editBlock(t *testing.T, fsys *FS, block uint64, edit func([]byte)) {
	t.Helper()
	buf := storage.GetBuffer(BlockSize)
	defer storage.PutBuffer(buf)
	off := int64(fsys.dataOffset + block*BlockSize)
	if _, err := fsys.dev.ReadAt(buf.Bytes(), off); err != nil {
		t.Fatalf("read block %d: %v", block, err)
	}
	edit(buf.Bytes())
	if _, err := fsys.dev.WriteAt(buf.Bytes(), off); err != nil {
		t.Fatalf("write block %d: %v", block, err)
	}
}

// newSFBContainer lays out an empty SFB container the way older builds meant to.
func newSFBContainer(t *testing.T, fsys *FS, tier sfb.Tier) uint64 {
	t.Helper()
	blocks, err := fsys.bitmapMgr.Alloc(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	h := sfb.Header{Magic: sfb.ContainerMagic, Version: sfb.ContainerVersion, Tier: tier,
		SlotCount: tier.SlotCount(), FreeSlots: tier.SlotCount()}
	editBlock(t, fsys, blocks[0], func(b []byte) { copy(b, h.Serialize()) })
	return blocks[0]
}

// newSFBFile creates a linked file whose data lives in an SFB slot.
func newSFBFile(t *testing.T, fsys *FS, container uint64, tier sfb.Tier, slot uint16, data []byte) *File {
	t.Helper()
	editBlock(t, fsys, container, func(b []byte) {
		h, err := sfb.DeserializeHeader(b)
		if err != nil {
			t.Fatal(err)
		}
		h.Bitmap[slot/8] |= 1 << (slot % 8)
		h.FreeSlots--
		copy(b, h.Serialize())
		copy(b[sfb.SlotOffset(tier, slot):], data)
	})
	f := newRegularFile(t, fsys)
	if _, err := fsys.updateInode(context.Background(), f.inodeNum, func(ino *inode.Inode) error {
		ino.InlineExtents[0] = inode.InlineExtent{
			PhysicalBlock: sfb.PackPhysicalBlock(container, slot),
			Length:        1,
			Flags:         extent.FlagAllocated | extent.FlagSFB | uint32(tier)<<4,
		}
		ino.ExtentCount = 1
		ino.Size = uint64(len(data))
		ino.LinkCount = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return f
}

func sfbSlotUsed(t *testing.T, fsys *FS, container uint64, slot uint16) bool {
	t.Helper()
	var used bool
	editBlock(t, fsys, container, func(b []byte) {
		h, err := sfb.DeserializeHeader(b)
		if err != nil {
			t.Fatal(err)
		}
		used = h.Bitmap[slot/8]&(1<<(slot%8)) != 0
	})
	return used
}

func TestSFBInodeReadable(t *testing.T) {
	fsys := openTreeFS(t, newTreeFSImage(t))
	container := newSFBContainer(t, fsys, sfb.Tier16KB)
	data := pattern(5, 10000)
	f := newSFBFile(t, fsys, container, sfb.Tier16KB, 1, data)

	checkAt(t, f, data, 0)
	checkAt(t, f, data[4000:4100], 4000)
}

// Growing an SFB file past its slot moves the data into a full block without
// losing any of it, and frees the slot.
func TestSFBMigrationKeepsData(t *testing.T) {
	path := newTreeFSImage(t)
	fsys := openTreeFS(t, path)
	container := newSFBContainer(t, fsys, sfb.Tier16KB)
	data := pattern(5, 10000)
	f := newSFBFile(t, fsys, container, sfb.Tier16KB, 1, data)

	tail := pattern(9, 4096)
	writeAt(t, f, tail, 70000)

	reopened := &File{fs: openTreeFS(t, path), inodeNum: f.inodeNum}
	checkAt(t, reopened, data, 0)
	checkAt(t, reopened, make([]byte, 70000-len(data)), int64(len(data)))
	checkAt(t, reopened, tail, 70000)
	ino, err := reopened.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if ino.IsSFBBacked() {
		t.Fatal("inode is still SFB-backed after growing past its slot")
	}
	if sfbSlotUsed(t, fsys, container, 1) {
		t.Fatal("SFB slot still allocated after migration")
	}
}

// Unlinking or truncating an SFB file frees its slot.
func TestSFBSlotFreed(t *testing.T) {
	fsys := openTreeFS(t, newTreeFSImage(t))
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatalf("InitRoot: %v", err)
	}
	container := newSFBContainer(t, fsys, sfb.Tier4KB)
	files := map[string]*File{}
	for slot, name := range []string{"unlinked-at-root", "unlinked-in-dir", "truncated"} {
		f := newSFBFile(t, fsys, container, sfb.Tier4KB, uint16(slot), pattern(byte(slot), 3000))
		root, err := fsys.inodeTbl.Read(inode.InodeRoot)
		if err != nil {
			t.Fatal(err)
		}
		if err := fsys.dirMgr.Add(ctx, root, name, uint64(f.inodeNum), dir.DT_REG); err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
		files[name] = f
	}

	if errno := fsys.Unlink(ctx, "unlinked-at-root"); errno != 0 {
		t.Fatalf("FS.Unlink: %v", errno)
	}
	if errno := (&Dir{fs: fsys, inodeNum: inode.InodeRoot}).Unlink(ctx, "unlinked-in-dir"); errno != 0 {
		t.Fatalf("Dir.Unlink: %v", errno)
	}
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE
	if errno := files["truncated"].Setattr(ctx, nil, in, &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("truncate: %v", errno)
	}

	for slot := uint16(0); slot < 3; slot++ {
		if sfbSlotUsed(t, fsys, container, slot) {
			t.Errorf("SFB slot %d still allocated", slot)
		}
	}
}
