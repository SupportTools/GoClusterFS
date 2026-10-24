package goclusterfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/refcount"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	snapFSBitmapOffset   = 1 << 20
	snapFSInodeOffset    = 2 << 20
	snapFSRefcountOffset = 3 << 20
	snapFSRegistryOffset = 3<<20 + 64<<10
	snapFSRegistrySize   = 64 << 10
	snapFSDataOffset     = 4 << 20
	snapFSDataBlocks     = 60
)

func newSnapshotFSImage(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fs.img")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, snapFSDataOffset+snapFSDataBlocks*BlockSize); err != nil {
		t.Fatal(err)
	}
	return path
}

// openSnapshotFS mounts an image with snapshots enabled, as a fresh node would.
func openSnapshotFS(t *testing.T, path string) *FS {
	t.Helper()
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { dev.Close() })

	var nodeUUID [16]byte
	copy(nodeUUID[:], "testnode12345678")
	bm := bitmap.NewRegion(dev, snapFSBitmapOffset, snapFSDataBlocks, nil, nodeUUID)
	inodeTbl := inode.NewTable(dev, snapFSInodeOffset, 4096*inode.InodeSize, nil, nodeUUID)
	extentMgr := extent.NewManager(dev, bm, nil, snapFSDataOffset)
	inodeTbl.SetTreeFlusher(extentMgr)
	refs := refcount.NewRegion(dev, snapFSRefcountOffset, snapFSDataBlocks, nil)
	dirMgr := dir.NewManager(dev, inodeTbl, extentMgr, nil, snapFSDataOffset)
	fsys := NewWithComponents(dev, bm, inodeTbl, extentMgr, dirMgr, nil, nil, nil, nil, refs, nodeUUID, snapFSDataOffset)
	if err := fsys.InitSnapshots(context.Background(), snapFSRegistryOffset, snapFSRegistrySize); err != nil {
		t.Fatalf("InitSnapshots: %v", err)
	}
	t.Cleanup(fsys.snapshotWkr.Stop)
	return fsys
}

func setxattr(t *testing.T, f *File, attr, value string) {
	t.Helper()
	if errno := f.Setxattr(context.Background(), attr, []byte(value), 0); errno != 0 {
		t.Fatalf("setxattr %s=%q on inode %d: %v", attr, value, f.inodeNum, errno)
	}
}

func lookupFile(t *testing.T, fsys *FS, name string) *File {
	t.Helper()
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	num, _, err := fsys.dirMgr.Lookup(context.Background(), root, name)
	if err != nil {
		t.Fatalf("lookup %s: %v", name, err)
	}
	return &File{fs: fsys, inodeNum: uint32(num)}
}

func truncateFile(t *testing.T, f *File, size uint64) {
	t.Helper()
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE
	in.Size = size
	if errno := f.Setattr(context.Background(), nil, in, &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("truncate inode %d to %d: %v", f.inodeNum, size, errno)
	}
}

// checkFile compares the whole file, including its size, with want.
func checkFile(t *testing.T, f *File, want []byte) {
	t.Helper()
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if ino.Size != uint64(len(want)) {
		t.Fatalf("inode %d: size %d, want %d", f.inodeNum, ino.Size, len(want))
	}
	checkAt(t, f, want, 0)
}

func overwritten(base []byte, off int, data []byte) []byte {
	out := bytes.Clone(base)
	copy(out[off:], data)
	return out
}

func TestSnapshotKeepsDataThroughWrites(t *testing.T) {
	path := newSnapshotFSImage(t)
	fsys := openSnapshotFS(t, path)
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatalf("InitRoot: %v", err)
	}
	original := pattern(1, 3*BlockSize)

	t.Run("SnapshotSurvivesWrites", func(t *testing.T) {
		f := newRegularFile(t, fsys)
		writeAt(t, f, original, 0)
		setxattr(t, f, "user.snapshot", "before")

		writeAt(t, f, pattern(2, 4096), BlockSize+100)
		writeAt(t, f, pattern(3, 2*BlockSize), 3*BlockSize)
		truncateFile(t, f, BlockSize+BlockSize/2)
		setxattr(t, f, "user.snapshot.clone", "before:/before-clone")

		live := overwritten(original[:BlockSize+BlockSize/2], BlockSize+100, pattern(2, 4096))
		for _, fsys := range []*FS{fsys, openSnapshotFS(t, path)} {
			checkFile(t, lookupFile(t, fsys, "before-clone"), original)
			checkFile(t, &File{fs: fsys, inodeNum: f.inodeNum}, live)
		}
	})

	t.Run("CloneWritesStayInClone", func(t *testing.T) {
		src := newRegularFile(t, fsys)
		writeAt(t, src, original[:2*BlockSize], 0)
		setxattr(t, src, "user.clone", "/clone")
		clone := lookupFile(t, fsys, "clone")

		writeAt(t, clone, pattern(4, 100), 10)
		writeAt(t, src, pattern(5, 100), BlockSize+10)

		for _, fsys := range []*FS{fsys, openSnapshotFS(t, path)} {
			checkFile(t, &File{fs: fsys, inodeNum: src.inodeNum}, overwritten(original[:2*BlockSize], BlockSize+10, pattern(5, 100)))
			checkFile(t, &File{fs: fsys, inodeNum: clone.inodeNum}, overwritten(original[:2*BlockSize], 10, pattern(4, 100)))
		}
	})

	t.Run("DeleteAfterOverwriteFreesSnapshotBlocks", func(t *testing.T) {
		f := newRegularFile(t, fsys)
		writeAt(t, f, original[:2*BlockSize], 0)
		free := fsys.bitmapMgr.FreeCount()
		setxattr(t, f, "user.snapshot", "temp")

		writeAt(t, f, pattern(6, 2*BlockSize), 0)
		if got := fsys.bitmapMgr.FreeCount(); got != free-2 {
			t.Fatalf("free blocks after overwriting 2 shared blocks = %d, want %d", got, free-2)
		}

		if errno := f.Removexattr(ctx, "user.snapshot.temp"); errno != 0 {
			t.Fatalf("delete snapshot: %v", errno)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			_, err := fsys.snapshotReg.GetByName(ctx, "temp")
			if errors.Is(err, snapshot.ErrSnapshotNotFound) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("snapshot still listed 10s after delete: %v", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if got := fsys.bitmapMgr.FreeCount(); got != free {
			t.Fatalf("free blocks after deleting the snapshot = %d, want %d", got, free)
		}
		checkFile(t, f, pattern(6, 2*BlockSize))
	})
}
