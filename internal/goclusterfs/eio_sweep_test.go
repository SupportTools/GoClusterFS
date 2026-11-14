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

	"github.com/hanwen/go-fuse/v2/fuse"

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
	return &File{fs: r.fsys, inodeNum: uint32(num)}
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
	if ok["write d/g"] {
		if d := r.file(inode.InodeRoot, "d"); d == nil {
			t.Error("d is gone after an acknowledged write inside it")
		} else {
			expect(d.inodeNum, "g", true, pattern(5, BlockSize/2))
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
