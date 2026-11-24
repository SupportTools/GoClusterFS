package goclusterfs

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// stopClean flushes the quota ledger and stops the journal in order, which marks it clean as an unmount does.
func stopClean(t *testing.T, fsys *FS) {
	t.Helper()
	flushQuota(t, fsys)
	if err := fsys.journal.Stop(); err != nil {
		t.Fatal(err)
	}
}

func quotaProblems(r *fsck.Result) []fsck.Problem {
	var out []fsck.Problem
	for _, p := range r.Problems {
		if strings.HasPrefix(p.Location, "quota ") || strings.Contains(p.Message, "project") {
			out = append(out, p)
		}
	}
	return out
}

func repairOptions() fsck.Options {
	opts := fsck.DefaultOptions()
	opts.Mode = fsck.Repair
	return opts
}

// After a clean stop fsck's recount of each project agrees with what the runtime charged: writes, a fallocate, a
// truncate, a mkdir, directory growth, short and long symlinks, a snapshot and a copy on write, and an unlink. The
// project's root, the one inode in it that its directory does not give the project, is not reported.
func TestFsckRecountAgreesWithQuotaCharges(t *testing.T) {
	fsys, _, _, _ := journaledImage(t, newFormattedImageAt(t, superblock.MinDeviceSize+64<<20, superblock.MinBlockSize))
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	ctx := context.Background()
	bs := int64(fsys.dataBlockSize())

	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(1, int(3*bs)), 0)
	writeAt(t, f, pattern(2, 10), 5*bs)
	if errno := f.Allocate(ctx, nil, uint64(6*bs), uint64(2*bs), 0); errno != 0 {
		t.Fatalf("fallocate: %v", errno)
	}
	in := &fuse.SetAttrIn{}
	in.Valid, in.Size = fuse.FATTR_SIZE, uint64(7*bs)
	if errno := f.Setattr(ctx, nil, in, &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("truncate: %v", errno)
	}
	setxattr(t, f, "user.snapshot", "s1")
	writeAt(t, f, pattern(3, 10), 0)

	sub := mkdirIn(t, pvc, "sub")
	for i := range 80 {
		if errno := createIn(t, sub, fmt.Sprintf("%0200d", i)).Release(ctx, nil); errno != 0 {
			t.Fatalf("release: %v", errno)
		}
		flushQuota(t, fsys)
	}
	if ino, err := fsys.inodeTbl.Read(sub.inodeNum); err != nil || ino.AllocatedBlocks < 2 {
		t.Fatalf("80 long names did not grow the directory: %+v, %v", ino, err)
	}
	if _, errno := pvc.Symlink(ctx, "short", "s", &fuse.EntryOut{}); errno != 0 {
		t.Fatalf("symlink: %v", errno)
	}
	if _, errno := pvc.Symlink(ctx, strings.Repeat("t", 200), "l", &fuse.EntryOut{}); errno != 0 {
		t.Fatalf("long symlink: %v", errno)
	}
	gone := createIn(t, sub, "gone")
	writeAt(t, gone, pattern(4, int(bs)), 0)
	if errno := sub.Unlink(ctx, "gone"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	if errno := gone.Release(ctx, nil); errno != 0 {
		t.Fatalf("release: %v", errno)
	}

	stopClean(t, fsys)
	expectCleanFsck(t, fsys)
	if r := runFsck(t, fsys, repairOptions()); len(r.Problems) > 0 || r.RepairedCount != 0 {
		t.Fatalf("fsck -repair after a clean stop: exit %d, repaired %d:%s", r.ExitCode(), r.RepairedCount, problemList(r))
	}
}

// A project whose usage in the table drifted from what its inodes use, as an I/O error or a crash between a usage
// change's commit and its write leaves it, is reported as an error, and fsck -repair writes the recount.
func TestFsckRepairsDriftedQuotaUsage(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	writeAt(t, createIn(t, pvc, "f"), pattern(1, int(2*fsys.dataBlockSize())), 0)
	stopClean(t, fsys)
	want := tableUsage(t, fsys, 1)
	tbl, err := fsys.quotaTable()
	if err != nil {
		t.Fatal(err)
	}
	if err := tbl.SetUsage(1, want[0]+5, want[1]+1); err != nil {
		t.Fatal(err)
	}

	r := runFsck(t, fsys, fsck.DefaultOptions())
	if ps := quotaProblems(r); len(ps) != 1 || ps[0].Location != "quota project 1" || ps[0].Severity != fsck.Error || !ps[0].Repairable {
		t.Fatalf("fsck on a project whose usage drifted:%s", problemList(r))
	}
	if r.ExitCode() != fsck.ExitUncorrected {
		t.Errorf("fsck exit %d on drifted usage, want %d", r.ExitCode(), fsck.ExitUncorrected)
	}
	if r := runFsck(t, fsys, repairOptions()); r.RepairedCount != 1 || r.ExitCode() != fsck.ExitCorrected {
		t.Fatalf("fsck -repair on drifted usage: exit %d, repaired %d:%s", r.ExitCode(), r.RepairedCount, problemList(r))
	}
	if got := tableUsage(t, fsys, 1); got != want {
		t.Errorf("after repair the table holds %v blocks and inodes, want the recount %v", got, want)
	}
	expectCleanFsck(t, fsys)
}

// A fallocate that keeps the file size and whose inode write fails keeps its charge, since the inode on disk may map the new blocks. When it does
// not, the project's usage drifts, and fsck -repair writes the recount.
func TestFsckRepairsQuotaDriftAfterAFailedInodeWrite(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	f := createIn(t, pvc, "f")
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	sector := int64(sb.InodeTableOffset + uint64(f.inodeNum)*inode.InodeSize&^(storage.SectorSize-1))
	fsys.dev.SetWriteFault(func(off int64) error {
		if off == sector {
			return syscall.EIO
		}
		return nil
	})
	errno := f.Allocate(context.Background(), nil, 0, 2*fsys.dataBlockSize(), 1)
	fsys.dev.SetWriteFault(func(int64) error { return nil })
	if errno != syscall.EIO {
		t.Fatalf("fallocate with a failing inode write: %v, want EIO", errno)
	}
	stopClean(t, fsys)
	root, err := fsys.inodeTbl.Read(pvc.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	want := [2]uint64{root.AllocatedBlocks, 2}
	if got := tableUsage(t, fsys, 1); got != [2]uint64{want[0] + 2, want[1]} {
		t.Fatalf("after the failed fallocate the table holds %v blocks and inodes, want its 2 blocks over the recount %v", got, want)
	}

	if ps := quotaProblems(runFsck(t, fsys, fsck.DefaultOptions())); len(ps) != 1 || ps[0].Location != "quota project 1" {
		t.Fatalf("fsck after the failed fallocate reports %v, want project 1's drift", ps)
	}
	if r := runFsck(t, fsys, repairOptions()); r.UnrepairedErrors() > 0 {
		t.Fatalf("fsck -repair after the failed fallocate:%s", problemList(r))
	}
	if got := tableUsage(t, fsys, 1); got != want {
		t.Errorf("after repair the table holds %v blocks and inodes, want the recount %v", got, want)
	}
	expectCleanFsck(t, fsys)
}

// A journal that is not clean may hold usage changes the table does not show yet, so fsck does not compare usage
// then: it says so and reports no error.
func TestFsckSkipsQuotaUsageWhileAJournalIsDirty(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	createIn(t, pvc, "f")

	r := runFsck(t, fsys, fsck.DefaultOptions())
	ps := quotaProblems(r)
	if len(ps) != 1 || ps[0].Severity != fsck.Info || !strings.Contains(ps[0].Message, "project usage not checked") {
		t.Fatalf("fsck with an unflushed charge in a dirty journal:%s", problemList(r))
	}
	if r.ExitCode() != fsck.ExitClean {
		t.Errorf("fsck exit %d, want %d", r.ExitCode(), fsck.ExitClean)
	}
}

// An inode fsck cannot trust may count other usage than its project was charged, so fsck does not compare usage then.
func TestFsckSkipsQuotaUsageWithAnInodeFailingItsChecksum(t *testing.T) {
	fsys, path, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(1, int(fsys.dataBlockSize())), 0)
	stopClean(t, fsys)
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	allocated := make([]byte, 8)
	binary.LittleEndian.PutUint64(allocated, 7)
	if err := writeImageRange(path, sb.InodeTableOffset+uint64(f.inodeNum)*inode.InodeSize+32, allocated); err != nil {
		t.Fatal(err)
	}

	r := runFsck(t, fsys, fsck.DefaultOptions())
	ps := quotaProblems(r)
	if len(ps) != 1 || ps[0].Severity != fsck.Info || !strings.Contains(ps[0].Message, fmt.Sprintf("inode %d fails its checksum", f.inodeNum)) {
		t.Fatalf("fsck with a project inode failing its checksum:%s", problemList(r))
	}
}

// Repair frees a file unlinked while open on a node that stopped, which still counted against its project, and writes
// the recount without it.
func TestFsckRepairTakesFreedInodesOutOfTheirProject(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	f := createIn(t, pvc, "f")
	writeAt(t, f, pattern(1, int(2*fsys.dataBlockSize())), 0)
	if errno := pvc.Unlink(context.Background(), "f"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	stopClean(t, fsys)
	root, err := fsys.inodeTbl.Read(pvc.inodeNum)
	if err != nil {
		t.Fatal(err)
	}

	if r := runFsck(t, fsys, repairOptions()); r.UnrepairedErrors() > 0 {
		t.Fatalf("fsck -repair with an orphan in a project:%s", problemList(r))
	}
	if got, want := tableUsage(t, fsys, 1), [2]uint64{root.AllocatedBlocks, 1}; got != want {
		t.Errorf("after repair freed the orphan the table holds %v blocks and inodes, want the project root's %v", got, want)
	}
	expectCleanFsck(t, fsys)
}

// fsck reports a name whose inode is in another project than its directory gives, and inodes in a project the table
// has no record for.
func TestFsckReportsInodesInTheWrongProject(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	fsys = withBridge(fsys)
	pvc := quotaProject(t, fsys, "pvc")
	outside := createFile(t, fsys, "outside")
	inside := createIn(t, pvc, "inside")
	stopClean(t, fsys)
	for num, project := range map[uint32]uint32{outside.inodeNum: 1, inside.inodeNum: 9} {
		ino, err := fsys.inodeTbl.Read(num)
		if err != nil {
			t.Fatal(err)
		}
		ino.ProjectID = project
		if err := fsys.inodeTbl.Write(context.Background(), ino); err != nil {
			t.Fatal(err)
		}
	}

	r := runFsck(t, fsys, fsck.DefaultOptions())
	want := []string{
		fmt.Sprintf(`entry "outside" names inode %d, which counts against project 1, but the directory gives its entries project 0`, outside.inodeNum),
		fmt.Sprintf(`entry "inside" names inode %d, which counts against project 9, but the directory gives its entries project 1`, inside.inodeNum),
		fmt.Sprintf("1 inodes count against it, inode %d first, but the quota table has no record in use for it", inside.inodeNum),
	}
	ps := quotaProblems(r)
	for _, w := range want {
		found := false
		for _, p := range ps {
			found = found || p.Message == w && p.Severity == fsck.Error && !p.Repairable
		}
		if !found {
			t.Errorf("fsck did not report %q:%s", w, problemList(r))
		}
	}
	if len(ps) != len(want) {
		t.Errorf("fsck reported %d quota problems, want %d:%s", len(ps), len(want), problemList(r))
	}
}
