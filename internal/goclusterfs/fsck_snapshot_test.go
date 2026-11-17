package goclusterfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// A registry sector that fails its checksum makes every snapshot create fail.
// A dry run changes nothing. Repair rewrites the sector with the descriptors
// that pass their own checksum and reports the one it lost, and the next
// repair frees the lost snapshot's data.
func TestFsckRepairRewritesDamagedSnapshotSector(t *testing.T) {
	t.Parallel()
	path := newFormattedImage(t)
	fsys := openFormattedFS(t, path)
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatal(err)
	}
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	f := createFile(t, fsys, "file.bin")
	writeAt(t, f, pattern(3, BlockSize), 0)
	setxattr(t, f, "user.snapshot", "kept")
	setxattr(t, f, "user.snapshot", "lost")
	lost, err := fsys.snapshotReg.GetByName(ctx, "lost")
	if err != nil {
		t.Fatal(err)
	}

	reg := readRegion(t, fsys, sb.SnapshotRegOffset, sb.SnapshotRegSize)
	name := bytes.Index(reg, []byte("lost\x00"))
	if name < 0 {
		t.Fatal("no descriptor named lost in the registry")
	}
	flipByte(t, fsys, int64(sb.SnapshotRegOffset)+int64(name))
	damaged := readRegion(t, fsys, sb.SnapshotRegOffset, sb.SnapshotRegSize)
	if _, err := fsys.snapshotReg.Create(ctx, "blocked", f.inodeNum, 0, 0, 1, BlockSize, snapshot.FlagReadOnly); err == nil {
		t.Fatal("snapshot create succeeded with a damaged registry sector")
	}

	const sectorProblem = "refuses every snapshot create"
	dry := repairOpts()
	dry.DryRun = true
	expectProblem(t, runFsck(t, fsys, dry), sectorProblem+" [WOULD FIX]")
	if !bytes.Equal(readRegion(t, fsys, sb.SnapshotRegOffset, sb.SnapshotRegSize), damaged) {
		t.Fatal("dry run changed the registry")
	}

	r := runFsck(t, fsys, repairOpts())
	if r.UnrepairedErrors() > 0 {
		t.Fatalf("repair left errors:%s", problemList(r))
	}
	expectProblem(t, r, sectorProblem+" [FIXED]")
	expectProblem(t, r, "the snapshot in it is lost, and the next repair frees any data it kept [FIXED]")

	reopened := openFormattedFS(t, path)
	if _, err := reopened.snapshotReg.GetByName(ctx, "kept"); err != nil {
		t.Errorf("repair lost the intact snapshot: %v", err)
	}
	if _, err := reopened.snapshotReg.GetByName(ctx, "lost"); !errors.Is(err, snapshot.ErrSnapshotNotFound) {
		t.Errorf("damaged snapshot after repair: %v, want not found", err)
	}
	setxattr(t, lookupFile(t, reopened, "file.bin"), "user.snapshot", "after")

	if !inodeInUse(t, reopened, lost.DataInode) {
		t.Fatalf("repair freed the lost snapshot's data inode %d in the same run", lost.DataInode)
	}
	r = runFsck(t, reopened, repairOpts())
	if r.UnrepairedErrors() > 0 {
		t.Fatalf("second repair left errors:%s", problemList(r))
	}
	final := openFormattedFS(t, path)
	if inodeInUse(t, final, lost.DataInode) {
		t.Errorf("second repair kept the lost snapshot's data inode %d:%s", lost.DataInode, problemList(r))
	}
	checkFile(t, lookupFile(t, final, "file.bin"), pattern(3, BlockSize))
	expectCleanFsck(t, final)
}

// fsck reports a snapshot whose parent is missing, is itself, or was taken of
// another file, and accepts a parent taken of the same file.
func TestFsckChecksSnapshotParents(t *testing.T) {
	t.Parallel()
	fsys := openFormattedFS(t, newFormattedImage(t))
	ctx := context.Background()
	if err := fsys.InitRoot(ctx); err != nil {
		t.Fatal(err)
	}
	a, b := createFile(t, fsys, "a.bin"), createFile(t, fsys, "b.bin")
	writeAt(t, a, pattern(1, BlockSize), 0)
	writeAt(t, b, pattern(2, BlockSize), 0)
	id := make(map[string]uint64)
	for _, s := range []struct {
		f    *File
		name string
	}{{a, "a1"}, {a, "a2"}, {a, "a3"}, {b, "b1"}} {
		setxattr(t, s.f, "user.snapshot", s.name)
		desc, err := fsys.snapshotReg.GetByName(ctx, s.name)
		if err != nil {
			t.Fatal(err)
		}
		id[s.name] = desc.SnapshotID
	}
	const missing = 999_999
	for child, parent := range map[string]uint64{"a1": missing, "a2": id["a1"], "a3": id["a3"], "b1": id["a1"]} {
		if err := fsys.snapshotReg.UpdateParentID(ctx, id[child], parent); err != nil {
			t.Fatal(err)
		}
	}

	r := runFsck(t, fsys, fsck.DefaultOptions())
	expectProblem(t, r, fmt.Sprintf(`snapshot %d: snapshot "a1" has parent snapshot %d, which does not exist`, id["a1"], missing))
	expectProblem(t, r, fmt.Sprintf(`snapshot %d: snapshot "a3" is its own parent`, id["a3"]))
	expectProblem(t, r, fmt.Sprintf(`snapshot "b1" of inode %d has parent snapshot %d, which was taken of inode %d`, b.inodeNum, id["a1"], a.inodeNum))
	if list := problemList(r); strings.Contains(list, fmt.Sprintf("snapshot %d:", id["a2"])) {
		t.Errorf("fsck reported a2, whose parent a1 was taken of the same file:%s", list)
	}
}
