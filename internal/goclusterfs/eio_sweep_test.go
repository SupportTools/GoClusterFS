package goclusterfs

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

type eioRun struct {
	fsys  *FS
	ok    map[string]bool
	steps *atomic.Int32
}

func (r *eioRun) file(dirNum uint32, name string) *File {
	parent, err := r.fsys.inodeTbl.Read(dirNum)
	if err != nil {
		return nil
	}
	num, _, err := r.fsys.dirMgr.Lookup(context.Background(), parent, name)
	if err != nil {
		return nil
	}
	return fileNode(r.fsys, uint32(num))
}

func (r *eioRun) step(name string, op func() syscall.Errno) {
	r.steps.Add(1)
	r.ok[name] = op() == 0
}

// eioWorkload runs the workload and counts its steps in steps, so writes can be told apart by step.
func eioWorkload(fsys *FS, steps *atomic.Int32) map[string]bool {
	ctx := context.Background()
	r := &eioRun{fsys: fsys, ok: map[string]bool{}, steps: steps}
	var sub *Dir
	create := func(dir uint32, name string) syscall.Errno {
		if dir == inode.InodeRoot {
			_, _, _, errno := fsys.Create(ctx, name, 0, 0o644, &fuse.EntryOut{})
			return errno
		}
		_, _, _, errno := sub.Create(ctx, name, 0, 0o644, &fuse.EntryOut{})
		return errno
	}
	write := func(dir uint32, name string, data []byte, off int64) syscall.Errno {
		f := r.file(dir, name)
		if f == nil {
			return syscall.ENOENT
		}
		n, errno := f.Write(ctx, nil, data, off)
		if errno == 0 && int(n) != len(data) {
			return syscall.EIO
		}
		return errno
	}
	r.step("create f1", func() syscall.Errno { return create(inode.InodeRoot, "f1") })
	r.step("write f1", func() syscall.Errno { return write(inode.InodeRoot, "f1", pattern(1, 2*BlockSize), 0) })
	r.step("create f2", func() syscall.Errno { return create(inode.InodeRoot, "f2") })
	r.step("write f2", func() syscall.Errno { return write(inode.InodeRoot, "f2", pattern(2, BlockSize), 0) })
	r.step("overwrite f2", func() syscall.Errno { return write(inode.InodeRoot, "f2", pattern(3, BlockSize), 0) })
	r.step("create f3", func() syscall.Errno { return create(inode.InodeRoot, "f3") })
	r.step("write f3", func() syscall.Errno { return write(inode.InodeRoot, "f3", pattern(4, 3*BlockSize), 0) })
	r.step("truncate f3", func() syscall.Errno {
		f := r.file(inode.InodeRoot, "f3")
		if f == nil {
			return syscall.ENOENT
		}
		in := &fuse.SetAttrIn{}
		in.Valid = fuse.FATTR_SIZE
		in.Size = BlockSize
		return f.Setattr(ctx, nil, in, &fuse.AttrOut{})
	})
	dNum := uint32(0)
	r.step("mkdir d", func() syscall.Errno {
		node, errno := fsys.Mkdir(ctx, "d", 0o755, &fuse.EntryOut{})
		if errno == 0 {
			sub = node.Operations().(*Dir)
			dNum = sub.inodeNum
		}
		return errno
	})
	r.step("create d/g", func() syscall.Errno {
		if dNum == 0 {
			return syscall.ENOENT
		}
		return create(dNum, "g")
	})
	r.step("write d/g", func() syscall.Errno {
		if dNum == 0 {
			return syscall.ENOENT
		}
		return write(dNum, "g", pattern(5, BlockSize/2), 0)
	})
	r.step("create r1", func() syscall.Errno { return create(inode.InodeRoot, "r1") })
	r.step("write r1", func() syscall.Errno { return write(inode.InodeRoot, "r1", pattern(7, BlockSize), 0) })
	r.step("create d/r2", func() syscall.Errno {
		if dNum == 0 {
			return syscall.ENOENT
		}
		return create(dNum, "r2")
	})
	r.step("rename r1 over d/r2", func() syscall.Errno {
		if sub == nil {
			return syscall.ENOENT
		}
		return fsys.Rename(ctx, "r1", sub, "r2", 0)
	})
	var e *Dir
	r.step("mkdir e", func() syscall.Errno {
		node, errno := fsys.Mkdir(ctx, "e", 0o755, &fuse.EntryOut{})
		if errno == 0 {
			e = node.Operations().(*Dir)
		}
		return errno
	})
	r.step("rename d to e/d", func() syscall.Errno {
		if e == nil {
			return syscall.ENOENT
		}
		return fsys.Rename(ctx, "d", e, "d", 0)
	})
	r.step("create f4", func() syscall.Errno { return create(inode.InodeRoot, "f4") })
	r.step("write f4", func() syscall.Errno { return write(inode.InodeRoot, "f4", pattern(6, BlockSize), 0) })
	r.step("unlink f4", func() syscall.Errno { return fsys.Unlink(ctx, "f4") })
	r.step("unlink f1", func() syscall.Errno { return fsys.Unlink(ctx, "f1") })
	return r.ok
}

func eioContent(f *File) ([]byte, error) {
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		return nil, err
	}
	res, errno := f.Read(context.Background(), nil, make([]byte, ino.Size), 0)
	if errno != 0 {
		return nil, errno
	}
	got, _ := res.Bytes(make([]byte, ino.Size))
	return got, nil
}

func checkEIOResults(t *testing.T, fsys *FS, ok map[string]bool) {
	t.Helper()
	r := &eioRun{fsys: fsys}
	expect := func(dir uint32, name string, acked bool, wants ...[]byte) {
		t.Helper()
		f := r.file(dir, name)
		if f == nil {
			if acked {
				t.Errorf("%s is gone after its acknowledged write", name)
			}
			return
		}
		got, err := eioContent(f)
		if err != nil {
			t.Errorf("read %s: %v", name, err)
			return
		}
		for _, w := range wants {
			if bytes.Equal(got, w) {
				return
			}
		}
		if acked {
			t.Errorf("%s holds %d bytes starting %v, not an acknowledged version", name, len(got), got[:min(8, len(got))])
		}
	}
	if !ok["unlink f1"] {
		expect(inode.InodeRoot, "f1", ok["write f1"], pattern(1, 2*BlockSize))
	} else if r.file(inode.InodeRoot, "f1") != nil {
		t.Error("f1 exists after its acknowledged unlink")
	}
	switch {
	case ok["overwrite f2"]:
		expect(inode.InodeRoot, "f2", true, pattern(3, BlockSize))
	case ok["write f2"]:
		expectSectorsFrom(t, r.file(inode.InodeRoot, "f2"), pattern(2, BlockSize), pattern(3, BlockSize))
	}
	switch {
	case ok["truncate f3"] && ok["write f3"]:
		expect(inode.InodeRoot, "f3", true, pattern(4, 3*BlockSize)[:BlockSize])
	case ok["write f3"]:
		expect(inode.InodeRoot, "f3", true, pattern(4, 3*BlockSize), pattern(4, 3*BlockSize)[:BlockSize])
	}
	d := oneName(t, r, "d", inode.InodeRoot, "d", r.file(inode.InodeRoot, "e"), "d")
	if ok["rename d to e/d"] && r.file(inode.InodeRoot, "d") != nil {
		t.Error("d is still in the root after its acknowledged move")
	}
	if ok["write d/g"] {
		if d == nil {
			t.Error("d is gone after an acknowledged write inside it")
		} else {
			expect(d.inodeNum, "g", true, pattern(5, BlockSize/2))
		}
	}
	if d != nil {
		renamed := oneName(t, r, "r1", inode.InodeRoot, "r1", d, "r2")
		if ok["rename r1 over d/r2"] && r.file(inode.InodeRoot, "r1") != nil {
			t.Error("r1 is still in the root after its acknowledged rename")
		}
		switch {
		case !ok["write r1"]:
		case renamed == nil:
			t.Error("r1 is gone after its acknowledged write")
		default:
			if got, err := eioContent(renamed); err != nil || !bytes.Equal(got, pattern(7, BlockSize)) {
				t.Errorf("r1 holds %d bytes (%v) after its acknowledged write", len(got), err)
			}
		}
	}
	if ok["unlink f4"] && r.file(inode.InodeRoot, "f4") != nil {
		t.Error("f4 exists after its acknowledged unlink")
	}
	for _, name := range []string{"create f2", "create f3"} {
		if ok[name] && r.file(inode.InodeRoot, name[len("create "):]) == nil {
			t.Errorf("%s was acknowledged but the name is gone", name)
		}
	}
}

// oneName fails when name in dir and newName in newDir both name one inode, as a half-done rename would leave them,
// and returns the file under the old name, or else under the new one.
func oneName(t *testing.T, r *eioRun, what string, dir uint32, name string, newDir *File, newName string) *File {
	t.Helper()
	old := r.file(dir, name)
	var moved *File
	if newDir != nil {
		moved = r.file(newDir.inodeNum, newName)
	}
	if old != nil && moved != nil && old.inodeNum == moved.inodeNum {
		t.Errorf("%s has two names after a rename", what)
	}
	if old != nil {
		return old
	}
	return moved
}

// expectSectorsFrom requires each 4 KB sector of f to match old or new: an overwrite in place that failed may have
// written some of its sectors.
func expectSectorsFrom(t *testing.T, f *File, old, new []byte) {
	t.Helper()
	if f == nil {
		t.Error("a file is gone after its acknowledged write")
		return
	}
	got, err := eioContent(f)
	if err != nil || len(got) != len(old) {
		t.Errorf("inode %d: %d bytes, %v; want %d", f.inodeNum, len(got), err, len(old))
		return
	}
	for off := 0; off < len(got); off += storage.SectorSize {
		end := off + storage.SectorSize
		if !bytes.Equal(got[off:end], old[off:end]) && !bytes.Equal(got[off:end], new[off:end]) {
			t.Errorf("inode %d: the sector at %d is neither the old nor the new data", f.inodeNum, off)
			return
		}
	}
}

func failedSteps(ok map[string]bool) string {
	var failed []string
	for name, good := range ok {
		if !good {
			failed = append(failed, name)
		}
	}
	return fmt.Sprint(failed)
}

// region returns the index of the on-disk region that holds off: superblock, registry, lock records, snapshot
// registry, journals, bitmap, refcounts, inode table or data.
func region(sb *superblock.Superblock, off uint64) int {
	starts := []uint64{0, sb.NodeRegistryOffset, sb.LockBitmapOffset, sb.SnapshotRegOffset, sb.JournalRegionOffset,
		sb.BlockBitmapOffset, sb.RefCountTableOffset, sb.InodeTableOffset, sb.DataRegionOffset}
	in := 0
	for i, start := range starts {
		if off >= start && start >= starts[in] {
			in = i
		}
	}
	return in
}

// eioWrite is where a write of the workload went and which step made it.
type eioWrite struct{ region, step int }

// runEIO fails write n of the workload with EIO and lets everything else land. With crash set the node then stops
// as if it crashed and a new node replays its journal; otherwise the same node keeps running. It returns every
// write's region and step.
func runEIO(t *testing.T, n int64, crash bool) []eioWrite {
	fsys, path, offset, size := journaledFS(t)
	withBridge(fsys)
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var steps atomic.Int32
	var writes []eioWrite
	fsys.dev.SetWriteGate(func(off int64) error {
		mu.Lock()
		defer mu.Unlock()
		writes = append(writes, eioWrite{region(sb, uint64(off)), int(steps.Load())})
		if int64(len(writes)-1) == n {
			return syscall.EIO
		}
		return nil
	})
	ok := eioWorkload(fsys, &steps)
	mu.Lock()
	written := slices.Clone(writes)
	mu.Unlock()
	if n < int64(len(written)) {
		t.Logf("failed steps: %s", failedSteps(ok))
	}
	if crash {
		fsys.snapshotWkr.Stop()
		fsys.dev.SetWriteGate(func(int64) error { return errPowerCut })
		fsys = mountAfterPowerCut(t, path, offset, size, false)
	} else {
		// A write the workload never reached must not fail fsck's repair instead.
		fsys.dev.SetWriteGate(func(int64) error { return nil })
	}
	checkEIOResults(t, fsys, ok)
	expectOnlyLeaks(t, fsys)
	return written
}

// eioPoints picks which writes to fail. By default, so the test stays short under the race detector, it fails the
// last inode table write of each step, where a step commits, after which the node crashes, and the first and last
// write to each region, after which the node keeps running. GOCLUSTERFS_EIO_SWEEP=metadata fails every write outside
// the data region and 20 data writes, and GOCLUSTERFS_EIO_SWEEP=full fails every write.
func eioPoints(writes []eioWrite, crash bool) []int {
	const inodeTable, dataRegion = 7, 8
	var points []int
	switch os.Getenv("GOCLUSTERFS_EIO_SWEEP") {
	case "full":
		for n := range writes {
			points = append(points, n)
		}
	case "metadata":
		var data []int
		for n, w := range writes {
			if w.region == dataRegion {
				data = append(data, n)
			} else {
				points = append(points, n)
			}
		}
		for i := 0; i < len(data); i += max(1, len(data)/20) {
			points = append(points, data[i])
		}
	default:
		last := map[int]int{}
		first := map[int]int{}
		for n, w := range writes {
			key := w.region
			if crash {
				if w.region != inodeTable {
					continue
				}
				key = w.step
			} else if _, ok := first[key]; !ok {
				first[key] = n
			}
			last[key] = n
		}
		for _, n := range first {
			points = append(points, n)
		}
		for _, n := range last {
			points = append(points, n)
		}
	}
	slices.Sort(points)
	return slices.Compact(points)
}

// One write fails with EIO, at each point that matters in a workload that creates, writes, overwrites, truncates and unlinks.
// Every result the workload acknowledged must hold, while the node keeps running and after it crashes and is
// replayed, and fsck may find nothing but leaks.
func TestOneFailedWriteKeepsAcknowledgedResults(t *testing.T) {
	for _, crash := range []bool{false, true} {
		t.Run("crash="+strconv.FormatBool(crash), func(t *testing.T) {
			for _, n := range eioPoints(runEIO(t, math.MaxInt64, crash), crash) {
				t.Run(fmt.Sprintf("eio_at_write_%d", n), func(t *testing.T) { runEIO(t, int64(n), crash) })
			}
		})
	}
}

// A write that fails mkdir with EIO leaves the root's link count matching its entries.
func TestFailedMkdirWriteKeepsParentLinks(t *testing.T) {
	t.Parallel()
	failEachWrite(t, func(*testing.T, *FS) {}, func(fsys *FS) {
		_, _ = fsys.Mkdir(context.Background(), "d", 0o755, &fuse.EntryOut{})
	}, rootLinks)
}

// A write that fails rmdir with EIO leaves the root's link count matching its entries.
func TestFailedRmdirWriteKeepsParentLinks(t *testing.T) {
	t.Parallel()
	failEachWrite(t, func(t *testing.T, fsys *FS) { newSubdir(t, fsys, "d") }, func(fsys *FS) {
		_ = fsys.Rmdir(context.Background(), "d")
	}, rootLinks)
}

// A write that fails a rename over a file in another directory with EIO leaves the file under exactly one name, and
// fsck finds nothing but leaks, whether or not the node crashes after.
func TestFailedRenameWriteLeavesOneName(t *testing.T) {
	t.Parallel()
	var p1, p2 *Dir
	var a, b uint32
	failEachWrite(t, func(t *testing.T, fsys *FS) {
		p1, p2 = newSubdir(t, fsys, "p1"), newSubdir(t, fsys, "p2")
		a, b = fileIn(t, fsys, p1.inodeNum, "a").inodeNum, fileIn(t, fsys, p2.inodeNum, "b").inodeNum
	}, func(fsys *FS) {
		_ = p1.Rename(context.Background(), "a", p2, "b", 0)
	}, func(t *testing.T, fsys *FS) {
		if !expectOneName(t, fsys, p1.inodeNum, "a", p2.inodeNum, "b", a) {
			if got := named(t, fsys, p2.inodeNum, "b"); got != b {
				t.Errorf("the rename did not happen, but b names inode %d, not %d", got, b)
			}
		}
		expectOnlyLeaks(t, fsys)
	})
}

// A write that fails a directory move with EIO leaves the directory under exactly one name, its ".." naming that
// parent, and both parents' link counts right, whether or not the node crashes after.
func TestFailedDirectoryMoveWriteLeavesOneName(t *testing.T) {
	t.Parallel()
	var p1, p2, d *Dir
	failEachWrite(t, func(t *testing.T, fsys *FS) {
		p1, p2 = newSubdir(t, fsys, "p1"), newSubdir(t, fsys, "p2")
		d = newDirIn(t, fsys, p1.inodeNum, "d")
	}, func(fsys *FS) {
		_ = p1.Rename(context.Background(), "d", p2, "e", 0)
	}, func(t *testing.T, fsys *FS) {
		parent := p1.inodeNum
		if expectOneName(t, fsys, p1.inodeNum, "d", p2.inodeNum, "e", d.inodeNum) {
			parent = p2.inodeNum
		}
		if got := named(t, fsys, d.inodeNum, ".."); got != parent {
			t.Errorf("\"..\" of the moved directory names %d, want %d", got, parent)
		}
		expectDirLinks(t, fsys, p1.inodeNum)
		expectDirLinks(t, fsys, p2.inodeNum)
		expectOnlyLeaks(t, fsys)
	})
}

func rootLinks(t *testing.T, fsys *FS) { expectDirLinks(t, fsys, inode.InodeRoot) }

// failEachWrite runs op once for each of its writes, failing that write with EIO, and checks the volume, both on the
// node that keeps running and after it crashes and its journal is replayed.
func failEachWrite(t *testing.T, setup func(*testing.T, *FS), op func(*FS), check func(*testing.T, *FS)) {
	total := failWrite(t, -1, false, setup, op, check)
	for _, crash := range []bool{false, true} {
		for n := 0; n < total; n++ {
			t.Run(fmt.Sprintf("crash=%v/eio_at_%d", crash, n), func(t *testing.T) { failWrite(t, n, crash, setup, op, check) })
		}
	}
}

func failWrite(t *testing.T, n int, crash bool, setup func(*testing.T, *FS), op func(*FS), check func(*testing.T, *FS)) int {
	t.Helper()
	fsys, path, offset, size := journaledFS(t)
	withBridge(fsys)
	setup(t, fsys)
	var count atomic.Int64
	fsys.dev.SetWriteGate(func(int64) error {
		if count.Add(1)-1 == int64(n) {
			return syscall.EIO
		}
		return nil
	})
	op(fsys)
	writes := int(count.Load())
	fsys.dev.SetWriteGate(func(int64) error { return nil })
	if crash {
		fsys.snapshotWkr.Stop()
		fsys.dev.SetWriteGate(func(int64) error { return errPowerCut })
		fsys = mountAfterPowerCut(t, path, offset, size, false)
	}
	check(t, fsys)
	return writes
}

// A write that fails one create with EIO, then a second create that may reuse its inode slot, then a crash: replay
// must not run the failed create on the second file's inode.
func TestFailedCreateThenCrashKeepsNamesApart(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, n int) int {
		fsys, path, offset, size := journaledFS(t)
		withBridge(fsys)
		var count atomic.Int64
		fsys.dev.SetWriteGate(func(int64) error {
			if count.Add(1)-1 == int64(n) {
				return syscall.EIO
			}
			return nil
		})
		_, _, _, errA := fsys.Create(context.Background(), "a", 0, 0o644, &fuse.EntryOut{})
		writes := int(count.Load())
		fsys.dev.SetWriteGate(func(int64) error { return nil })
		_, _, _, errB := fsys.Create(context.Background(), "b", 0, 0o644, &fuse.EntryOut{})
		fsys.snapshotWkr.Stop()
		fsys.dev.SetWriteGate(func(int64) error { return errPowerCut })
		replayed := mountAfterPowerCut(t, path, offset, size, false)
		if a, b := named(t, replayed, inode.InodeRoot, "a"), named(t, replayed, inode.InodeRoot, "b"); a != 0 && a == b {
			t.Errorf("create a returned %v and create b %v; after replay both name inode %d", errA, errB, a)
		}
		expectOnlyLeaks(t, replayed)
		return writes
	}
	total := run(t, -1)
	for n := 0; n < total; n++ {
		t.Run(fmt.Sprintf("eio_at_%d", n), func(t *testing.T) { run(t, n) })
	}
}

// A write that fails a rename with EIO while a peer waits to change the old parent: the rename finishes under its
// locks before the peer gets them, so the file keeps exactly one name.
func TestFailedRenameFinishesBeforeAPeerChangesTheOldParent(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, n int) int {
		nodeA, path, _, _ := journaledFS(t)
		withBridge(nodeA)
		nodeB := openFormattedFS(t, path)
		p1, p2 := newSubdir(t, nodeA, "p1"), newSubdir(t, nodeA, "p2")
		a, b := fileIn(t, nodeA, p1.inodeNum, "a").inodeNum, fileIn(t, nodeA, p2.inodeNum, "b").inodeNum
		peer := newRegularFile(t, nodeB)
		if _, err := nodeB.updateInode(context.Background(), peer.inodeNum, func(ino *inode.Inode) error { ino.LinkCount = 1; return nil }); err != nil {
			t.Fatal(err)
		}
		p1Ino := readInode(t, nodeB, p1.inodeNum)
		added := make(chan error, 1)
		var count atomic.Int64
		nodeA.dev.SetWriteGate(func(int64) error {
			if count.Add(1)-1 != int64(n) {
				return nil
			}
			go func() {
				added <- nodeB.dirMgr.Add(context.Background(), p1Ino, "peer", uint64(peer.inodeNum), dir.DT_REG)
			}()
			time.Sleep(50 * time.Millisecond)
			return syscall.EIO
		})
		_ = p1.Rename(context.Background(), "a", p2, "b", 0)
		writes := int(count.Load())
		nodeA.dev.SetWriteGate(func(int64) error { return nil })
		if n >= 0 {
			if err := <-added; err != nil {
				t.Fatalf("peer add: %v", err)
			}
		}
		if !expectOneName(t, nodeA, p1.inodeNum, "a", p2.inodeNum, "b", a) {
			if got := named(t, nodeA, p2.inodeNum, "b"); got != b {
				t.Errorf("the rename did not happen, but b names inode %d, not %d", got, b)
			}
		}
		if n >= 0 && named(t, nodeA, p1.inodeNum, "peer") != peer.inodeNum {
			t.Error("the peer's file is missing from the old parent")
		}
		expectDirLinks(t, nodeA, p1.inodeNum)
		expectDirLinks(t, nodeA, p2.inodeNum)
		expectOnlyLeaks(t, nodeA)
		return writes
	}
	total := run(t, -1)
	for n := 0; n < total; n++ {
		t.Run(fmt.Sprintf("eio_at_%d", n), func(t *testing.T) { run(t, n) })
	}
}

// When every write fails from a rename's old-name removal on, the rename cannot finish. It stops the node with its
// locks held, so a peer waiting for the old parent stays blocked until replay finishes the rename.
func TestStuckRenameKeepsItsLocksUntilReplay(t *testing.T) {
	t.Parallel()
	nodeA, path, offset, size := journaledFS(t)
	withBridge(nodeA)
	nodeB := openFormattedFS(t, path)
	p1, p2 := newSubdir(t, nodeA, "p1"), newSubdir(t, nodeA, "p2")
	a := fileIn(t, nodeA, p1.inodeNum, "a").inodeNum
	peer := newRegularFile(t, nodeB)
	if _, err := nodeB.updateInode(context.Background(), peer.inodeNum, func(ino *inode.Inode) error { ino.LinkCount = 1; return nil }); err != nil {
		t.Fatal(err)
	}
	blocks, err := nodeA.extentMgr.DataBlocks(readInode(t, nodeA, p1.inodeNum))
	if err != nil || len(blocks) != 1 {
		t.Fatalf("old parent's blocks: %v, %v", blocks, err)
	}
	oldChunks := int64(nodeA.dataOffset + blocks[0]*BlockSize)

	stuck := make(chan error, 1)
	nodeA.SetFailStop(func(err error) {
		stuck <- err
		select {}
	})
	var dead atomic.Bool
	nodeA.dev.SetWriteGate(func(off int64) error {
		if off >= oldChunks && off < oldChunks+BlockSize {
			dead.Store(true)
		}
		if dead.Load() {
			return errPowerCut
		}
		return nil
	})
	go func() { _ = p1.Rename(context.Background(), "a", p2, "b", 0) }()
	select {
	case err := <-stuck:
		t.Logf("stopped: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the rename neither finished nor stopped the node")
	}

	added := make(chan error, 1)
	go func() {
		added <- nodeB.dirMgr.Add(context.Background(), readInode(t, nodeB, p1.inodeNum), "peer", uint64(peer.inodeNum), dir.DT_REG)
	}()
	select {
	case err := <-added:
		t.Fatalf("the peer changed the old parent of a half-done rename (%v)", err)
	case <-time.After(300 * time.Millisecond):
	}

	nodeA.snapshotWkr.Stop()
	replayed := remountAndReplay(t, path, offset, size)
	select {
	case err := <-added:
		if err != nil {
			t.Fatalf("peer add after replay: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the peer still waits after replay")
	}
	expectOneName(t, replayed, p1.inodeNum, "a", p2.inodeNum, "b", a)
	expectDirLinks(t, replayed, p1.inodeNum)
	expectDirLinks(t, replayed, p2.inodeNum)
	expectOnlyLeaks(t, replayed)
}
