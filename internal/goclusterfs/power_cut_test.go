package goclusterfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
	"github.com/supporttools/GoClusterFS/internal/superblock"
	"github.com/supporttools/GoClusterFS/internal/trim"
)

var errPowerCut = errors.New("power cut")

type power struct {
	limit  int64
	writes atomic.Int64
}

func (p *power) cut() bool { return p.writes.Load() > p.limit }

// cutPowerAfter lets the first n writes to fsys's device land and refuses the rest, as a power failure would.
func cutPowerAfter(fsys *FS, n int64) *power {
	p := &power{limit: n}
	fsys.dev.SetWriteGate(func(int64) error {
		if p.writes.Add(1) > p.limit {
			return errPowerCut
		}
		return nil
	})
	return p
}

type powerCutCase struct {
	trim bool
	// setup builds the state before the operation; all of its writes land.
	setup func(t *testing.T, fsys *FS)
	// op is the operation power fails during. It ignores errors, which the cut causes.
	op func(fsys *FS, p *power)
	// afterMount runs on the remounted node before fsck.
	afterMount func(t *testing.T, fsys *FS)
	// check runs after fsck found nothing but leaks and repair fixed them.
	check func(t *testing.T, fsys *FS)
}

// runPowerCuts runs the operation to completion once to count its writes, then once per write with power cut just
// before it.
func runPowerCuts(t *testing.T, c powerCutCase) {
	total := runPowerCut(t, c, math.MaxInt64)
	if total == 0 {
		t.Fatal("the operation wrote nothing")
	}
	t.Logf("the operation makes %d writes", total)
	for n := range total {
		t.Run(fmt.Sprintf("cut_after_%d_writes", n), func(t *testing.T) { runPowerCut(t, c, n) })
	}
}

func runPowerCut(t *testing.T, c powerCutCase, n int64) int64 {
	t.Helper()
	fsys, path, offset, size := journaledFS(t)
	if c.trim {
		attachHolePunchingTrim(t, fsys, path)
	}
	c.setup(t, fsys)
	p := cutPowerAfter(fsys, n)
	c.op(fsys, p)
	fsys.snapshotWkr.Stop()
	writes := p.writes.Load()
	fsys.dev.SetWriteGate(func(int64) error { return errPowerCut })

	remounted := mountAfterPowerCut(t, path, offset, size, c.trim)
	if c.afterMount != nil {
		c.afterMount(t, remounted)
	}
	expectOnlyLeaks(t, remounted)
	c.check(t, remounted)
	return writes
}

// mountAfterPowerCut opens the image as a new node in mount order: journal replay first, then snapshot recovery.
func mountAfterPowerCut(t *testing.T, path string, offset, size uint64, withTrim bool) *FS {
	t.Helper()
	fsys := openFormattedComponents(t, path)
	if withTrim {
		attachHolePunchingTrim(t, fsys, path)
	}
	replayCrashedJournal(t, fsys, offset, size)
	startSnapshots(t, fsys)
	deadline := time.Now().Add(10 * time.Second)
	for {
		pending := 0
		snaps, err := fsys.snapshotReg.List(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range snaps {
			if s.State != snapshot.StateActive {
				pending++
			}
		}
		if pending == 0 {
			return fsys
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d snapshots still being created or deleted 10s after mount", pending)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// attachHolePunchingTrim gives fsys a TRIM manager that flushes only when asked and discards by punching holes in the
// image, so a discard of a block in use shows as zeros.
func attachHolePunchingTrim(t *testing.T, fsys *FS, path string) {
	t.Helper()
	img, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { img.Close() })
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	const keepSize, punchHole = 0x01, 0x02
	tm := trim.NewManager(fsys.dev, trim.Config{
		BatchSize:  math.MaxInt32,
		MaxAge:     time.Hour,
		DataOffset: sb.DataRegionOffset,
		Enabled:    true,
		Discard: func(off, length uint64) error {
			return syscall.Fallocate(int(img.Fd()), keepSize|punchHole, int64(off), int64(length))
		},
	})
	fsys.bitmapMgr.SetTrimManager(tm)
	fsys.trimMgr = tm
}

// expectOnlyLeaks requires fsck to find nothing but leaked blocks or inodes, and repair to fix those.
func expectOnlyLeaks(t *testing.T, fsys *FS) {
	t.Helper()
	r := runFsck(t, fsys, fsck.DefaultOptions())
	if len(r.Problems) == 0 {
		return
	}
	for _, p := range r.Problems {
		if !isLeak(p.Message) {
			t.Errorf("fsck after the power cut found more than leaks:%s", problemList(r))
			break
		}
	}
	t.Logf("leaks to repair:%s", problemList(r))
	repair := fsck.DefaultOptions()
	repair.Mode = fsck.Repair
	runFsck(t, fsys, repair)
	expectCleanFsck(t, fsys)
}

func isLeak(msg string) bool {
	for _, leak := range []string{"leaked", "will never be freed", "unnamed: no directory names it", "belongs to no snapshot"} {
		if strings.Contains(msg, leak) {
			return true
		}
	}
	return false
}

func readWhole(t *testing.T, f *File) []byte {
	t.Helper()
	ino, err := f.fs.inodeTbl.Read(f.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	res, errno := f.Read(context.Background(), nil, make([]byte, ino.Size), 0)
	if errno != 0 {
		t.Fatalf("inode %d: read: %v", f.inodeNum, errno)
	}
	got, _ := res.Bytes(make([]byte, ino.Size))
	return got
}

// expectEachBlock requires every block of f to hold either its old or its new data.
func expectEachBlock(t *testing.T, f *File, old, updated []byte) {
	t.Helper()
	got := readWhole(t, f)
	if len(got) != len(old) {
		t.Fatalf("inode %d: %d bytes, want %d", f.inodeNum, len(got), len(old))
	}
	for i := 0; i < len(old); i += BlockSize {
		end := min(i+BlockSize, len(old))
		if !bytes.Equal(got[i:end], old[i:end]) && !bytes.Equal(got[i:end], updated[i:end]) {
			t.Fatalf("inode %d: block %d holds neither its old nor its new data", f.inodeNum, i/BlockSize)
		}
	}
}

// expectSnapshot requires the named snapshot to be active with want as its data, and reports whether it exists.
func expectSnapshot(t *testing.T, fsys *FS, name string, want []byte) bool {
	t.Helper()
	desc, err := fsys.snapshotReg.GetByName(context.Background(), name)
	if errors.Is(err, snapshot.ErrSnapshotNotFound) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	if desc.State != snapshot.StateActive {
		t.Fatalf("snapshot %s is %s after recovery", name, desc.State)
	}
	if !bytes.Equal(readWhole(t, &File{fs: fsys, inodeNum: desc.DataInode}), want) {
		t.Fatalf("snapshot %s changed", name)
	}
	return true
}

func lookupMaybe(t *testing.T, fsys *FS, name string) *File {
	t.Helper()
	root, err := fsys.inodeTbl.Read(inode.InodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	num, _, err := fsys.dirMgr.Lookup(context.Background(), root, name)
	if err != nil {
		return nil
	}
	return &File{fs: fsys, inodeNum: uint32(num)}
}

// waitForSnapshotDelete waits until the delete job freed the snapshot's data inode or power failed.
func waitForSnapshotDelete(fsys *FS, p *power, dataInode uint32) {
	deadline := time.Now().Add(10 * time.Second)
	for !p.cut() && time.Now().Before(deadline) {
		if ino, err := fsys.inodeTbl.Read(dataInode); err == nil && ino.Magic == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// Power fails during a write that copies two blocks a snapshot shares. Each block keeps its old or its new data, and
// the snapshot keeps the data it captured.
func TestPowerCutDuringCopyOnWrite(t *testing.T) {
	old := pattern(1, 3*BlockSize)
	update := pattern(9, BlockSize)
	updated := overwritten(old, BlockSize+4096, update)
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			f = createFile(t, fsys, "f")
			writeAt(t, f, old, 0)
			setxattr(t, f, "user.snapshot", "s1")
		},
		op: func(fsys *FS, _ *power) {
			f.Write(context.Background(), nil, update, BlockSize+4096)
		},
		check: func(t *testing.T, fsys *FS) {
			expectEachBlock(t, lookupFile(t, fsys, "f"), old, updated)
			if !expectSnapshot(t, fsys, "s1", old) {
				t.Fatal("snapshot s1 is gone")
			}
		},
	})
}

// Power fails while a snapshot is created. The file and the older snapshot keep their data, and the new snapshot is
// either gone or complete.
func TestPowerCutDuringSnapshotCreate(t *testing.T) {
	old := pattern(1, 3*BlockSize)
	current := overwritten(old, 0, pattern(5, BlockSize))
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			f = createFile(t, fsys, "f")
			writeAt(t, f, old, 0)
			setxattr(t, f, "user.snapshot", "s0")
			writeAt(t, f, current[:BlockSize], 0)
		},
		op: func(fsys *FS, _ *power) {
			f.Setxattr(context.Background(), "user.snapshot", []byte("s1"), 0)
		},
		check: func(t *testing.T, fsys *FS) {
			checkFile(t, lookupFile(t, fsys, "f"), current)
			if !expectSnapshot(t, fsys, "s0", old) {
				t.Fatal("snapshot s0 is gone")
			}
			expectSnapshot(t, fsys, "s1", current)
		},
	})
}

// Power fails while a snapshot is deleted, including its background job. The file and the other snapshot keep their
// data, and the deleted snapshot is either gone or intact.
func TestPowerCutDuringSnapshotDelete(t *testing.T) {
	old := pattern(1, 3*BlockSize)
	mid := overwritten(old, BlockSize, pattern(5, BlockSize))
	current := overwritten(mid, 2*BlockSize, pattern(7, BlockSize))
	var f *File
	var s1Data uint32
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			f = createFile(t, fsys, "f")
			writeAt(t, f, old, 0)
			setxattr(t, f, "user.snapshot", "s0")
			writeAt(t, f, mid[BlockSize:2*BlockSize], BlockSize)
			setxattr(t, f, "user.snapshot", "s1")
			writeAt(t, f, current[2*BlockSize:], 2*BlockSize)
			desc, err := fsys.snapshotReg.GetByName(context.Background(), "s1")
			if err != nil {
				t.Fatal(err)
			}
			s1Data = desc.DataInode
		},
		op: func(fsys *FS, p *power) {
			if errno := f.Removexattr(context.Background(), "user.snapshot.s1"); errno == 0 {
				waitForSnapshotDelete(fsys, p, s1Data)
			}
		},
		check: func(t *testing.T, fsys *FS) {
			checkFile(t, lookupFile(t, fsys, "f"), current)
			if !expectSnapshot(t, fsys, "s0", old) {
				t.Fatal("snapshot s0 is gone")
			}
			expectSnapshot(t, fsys, "s1", mid)
		},
	})
}

// Power fails while an unlink frees blocks that TRIM has queued. After replay a new file reuses the freed blocks and
// TRIM flushes: no discard may reach a block in use.
func TestPowerCutDuringUnlinkThenTrimAfterReuse(t *testing.T) {
	kept := pattern(2, 2*BlockSize)
	doomed := pattern(4, 3*BlockSize)
	fresh := pattern(6, 3*BlockSize)
	runPowerCuts(t, powerCutCase{
		trim: true,
		setup: func(t *testing.T, fsys *FS) {
			writeAt(t, createFile(t, fsys, "keep"), kept, 0)
			writeAt(t, createFile(t, fsys, "old"), doomed, 0)
		},
		op: func(fsys *FS, _ *power) {
			fsys.Unlink(context.Background(), "old")
		},
		afterMount: func(t *testing.T, fsys *FS) {
			writeAt(t, createFile(t, fsys, "new"), fresh, 0)
			fsys.trimMgr.Flush()
		},
		check: func(t *testing.T, fsys *FS) {
			checkFile(t, lookupFile(t, fsys, "keep"), kept)
			checkFile(t, lookupFile(t, fsys, "new"), fresh)
			if old := lookupMaybe(t, fsys, "old"); old != nil {
				checkFile(t, old, doomed)
			}
		},
	})
}

// Power fails while a file is cloned. The file keeps its data, and the clone is either gone or a full copy.
func TestPowerCutDuringClone(t *testing.T) {
	data := pattern(3, 3*BlockSize)
	var f *File
	runPowerCuts(t, powerCutCase{
		setup: func(t *testing.T, fsys *FS) {
			f = createFile(t, fsys, "f")
			writeAt(t, f, data, 0)
		},
		op: func(fsys *FS, _ *power) {
			f.Setxattr(context.Background(), "user.clone", []byte("/c"), 0)
		},
		check: func(t *testing.T, fsys *FS) {
			checkFile(t, lookupFile(t, fsys, "f"), data)
			if c := lookupMaybe(t, fsys, "c"); c != nil {
				checkFile(t, c, data)
			}
		},
	})
}
