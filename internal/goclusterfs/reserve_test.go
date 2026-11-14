package goclusterfs

import (
	"context"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/reserve"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// withReserve gives fsys, which must have a journal, a block reserve as mount does.
func withReserve(t *testing.T, fsys *FS, size int, idle time.Duration) *reserve.Manager {
	t.Helper()
	res := reserve.New(reserve.Config{Size: size, Idle: idle}, fsys.bitmapMgr, fsys.journal, fsys.extentMgr.ReleaseBlocks, func() int { return 1 })
	fsys.bitmapMgr.SetNoSpaceWait(res)
	fsys.journal.SetCheckpointHook(func() { res.Refresh(context.Background()) })
	fsys.SetReserve(res)
	res.Start()
	return res
}

// journaledNode opens another node on path with its own journal slot.
func journaledNode(t *testing.T, path string, slot uint64) *FS {
	t.Helper()
	fsys := openFormattedFS(t, path)
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	size := sb.JournalSlotSize()
	if fsys.journal, err = journal.Init(fsys.dev, sb.JournalRegionOffset+slot*size, size, fsys.nodeUUID, 1, journal.DefaultConfig()); err != nil {
		t.Fatal(err)
	}
	return fsys
}

// reserveFS opens a journaled node on an image large enough for 64-block
// reserves: below two reserves per node, or 1% of the blocks, there are none.
func reserveFS(t *testing.T) (fsys *FS, path string, offset, size uint64) {
	t.Helper()
	path = newFormattedImageOfSize(t, superblock.MinDeviceSize+1<<30)
	fsys = journaledNode(t, path, 0)
	if err := fsys.InitRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	sb, err := superblock.Read(fsys.dev)
	if err != nil {
		t.Fatal(err)
	}
	return fsys, path, sb.JournalRegionOffset, sb.JournalSlotSize()
}

func expectRefills(t *testing.T, res *reserve.Manager) {
	t.Helper()
	if res.Stats().Refills == 0 {
		t.Fatal("the reserve never took blocks, so the test did not use it")
	}
}

func bitmapAcquires(fsys *FS) uint64 {
	return lockAcquires(fsys)["bitmap"]
}

// Writes take their blocks from the node's reserve, so ten one-block files
// take the bitmap lock once, and unmounting gives the rest back.
func TestWritesTakeBlocksFromReserve(t *testing.T) {
	fsys, path, _, _ := reserveFS(t)
	free := fsys.bitmapMgr.FreeCount()
	withReserve(t, fsys, 64, time.Hour)
	files := make([]*File, 10)
	for i := range files {
		files[i] = createFile(t, fsys, fmt.Sprintf("f%d", i))
	}
	before := bitmapAcquires(fsys)
	for i, f := range files {
		writeAt(t, f, pattern(byte(i), 4096), 0)
	}
	if got := bitmapAcquires(fsys) - before; got != 1 {
		t.Errorf("ten one-block writes took the bitmap lock %d times, want 1", got)
	}
	for i, f := range files {
		checkFile(t, f, pattern(byte(i), 4096))
	}
	if err := fsys.Close(); err != nil {
		t.Fatal(err)
	}
	after := openFormattedFS(t, path)
	if got := after.bitmapMgr.FreeCount(); got != free-10 {
		t.Errorf("%d free blocks after unmount, want %d: the reserve was not given back", got, free-10)
	}
	expectCleanFsck(t, after)
}

// statfs counts the node's unused reserve as free, so a one-block write lowers
// it by one block even though the write took a whole reserve from the bitmap.
func TestStatfsCountsOwnReserveAsFree(t *testing.T) {
	fsys, _, _, _ := reserveFS(t)
	f := createFile(t, fsys, "one")
	var before, after fuse.StatfsOut
	if errno := fsys.Statfs(context.Background(), &before); errno != 0 {
		t.Fatal(errno)
	}
	res := withReserve(t, fsys, 64, time.Hour)
	writeAt(t, f, pattern(1, 4096), 0)
	expectRefills(t, res)
	if errno := fsys.Statfs(context.Background(), &after); errno != 0 {
		t.Fatal(errno)
	}
	if after.Bfree != before.Bfree-1 || after.Bavail != after.Bfree {
		t.Errorf("statfs Bfree %d, Bavail %d after a one-block write, want both %d", after.Bfree, after.Bavail, before.Bfree-1)
	}
}

// A reserve nobody takes from goes back to the bitmap after the idle time.
func TestIdleReserveGoesBack(t *testing.T) {
	fsys, _, _, _ := reserveFS(t)
	free := fsys.bitmapMgr.FreeCount()
	res := withReserve(t, fsys, 64, 200*time.Millisecond)
	writeAt(t, createFile(t, fsys, "one"), pattern(1, 4096), 0)
	deadline := time.Now().Add(10 * time.Second)
	for res.Stats().Returns == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if s := res.Stats(); s.Returns != 1 || s.Blocks != 0 {
		t.Fatalf("reserve stats %+v after the idle time, want it given back once", s)
	}
	if got := fsys.bitmapMgr.FreeCount(); got != free-1 {
		t.Errorf("%d free blocks after the idle return, want %d", got, free-1)
	}
}

// Node A takes block b from its reserve for a file, truncates the file, which
// frees b, and node B allocates b. A then stops without returning its reserve.
// Replaying A's journal frees A's unused reserved blocks and leaves B's block alone.
func TestReplayKeepsReservedBlockUsedFreedAndRetaken(t *testing.T) {
	nodeA, path, offset, size := reserveFS(t)
	resA := withReserve(t, nodeA, 8, time.Hour)
	f := createFile(t, nodeA, "f")
	writeAt(t, f, pattern(1, 4096), 0)
	expectRefills(t, resA)
	b := physicalBlock(t, nodeA, f.inodeNum, 0)
	truncateFile(t, f, 0)

	nodeB := openFormattedFS(t, path)
	got, err := nodeB.bitmapMgr.Alloc(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != b {
		t.Fatalf("node B got block %d, want the block %d node A used and freed", got[0], b)
	}
	nodeA.snapshotWkr.Stop()
	nodeA.dev.SetWriteGate(func(int64) error { return errPowerCut })

	nodeC := openFormattedFS(t, path)
	replayCrashedJournal(t, nodeC, offset, size)
	if !allocated(t, nodeC, b) {
		t.Fatalf("replay freed block %d, which node B allocated after node A used and freed it", b)
	}
	// Node B allocated b without mapping it, so b is the one leak fsck may report.
	r := runFsck(t, nodeC, fsck.DefaultOptions())
	for _, p := range r.Problems {
		if p.Location != fmt.Sprintf("block %d", b) {
			t.Errorf("after the replay fsck found more than node B's unmapped block %d:%s", b, problemList(r))
			break
		}
	}
}

// Node A writes two files from its reserve, the second through two blocks, and
// crashes holding the rest. After replay the files keep their data and no
// allocation can take their blocks.
func TestReplayKeepsMappedBlocksOfCrashedNodesReserve(t *testing.T) {
	nodeA, path, offset, size := reserveFS(t)
	fOne, fTwo := createFile(t, nodeA, "one"), createFile(t, nodeA, "two")
	free := nodeA.bitmapMgr.FreeCount()
	resA := withReserve(t, nodeA, 8, time.Hour)
	one, two := pattern(1, 4096), pattern(2, 8192)
	writeAt(t, fOne, one, 0)
	writeAt(t, fTwo, two, BlockSize-4096)
	expectRefills(t, resA)
	nodeA.snapshotWkr.Stop()
	nodeA.dev.SetWriteGate(func(int64) error { return errPowerCut })

	nodeB := openFormattedFS(t, path)
	replayCrashedJournal(t, nodeB, offset, size)
	fOne, fTwo = lookupFile(t, nodeB, "one"), lookupFile(t, nodeB, "two")
	checkFile(t, fOne, one)
	checkFile(t, fTwo, append(make([]byte, BlockSize-4096), two...))
	mapped := map[uint64]bool{
		physicalBlock(t, nodeB, fOne.inodeNum, 0): true,
		physicalBlock(t, nodeB, fTwo.inodeNum, 0): true,
		physicalBlock(t, nodeB, fTwo.inodeNum, 1): true,
	}
	if got, want := nodeB.bitmapMgr.FreeCount(), free-uint64(len(mapped)); got != want {
		t.Errorf("%d free blocks after the replay, want %d: only the %d mapped blocks stay allocated", got, want, len(mapped))
	}
	expectCleanFsck(t, nodeB)
	rest, err := nodeB.bitmapMgr.Alloc(context.Background(), int(nodeB.bitmapMgr.FreeCount()))
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range rest {
		if mapped[b] {
			t.Fatalf("node B allocated block %d, which node A's file maps", b)
		}
	}
}

// Node B holds the bitmap lock while node A's refill waits for it. Statfs and a
// write that maps a block under its inode lock on A must not wait for the refill:
// if B died holding the lock, its replay could need that inode lock.
func TestInodeLockHoldersDoNotWaitForRefill(t *testing.T) {
	nodeA, path, _, _ := reserveFS(t)
	nodeB := openFormattedFS(t, path)
	resA := withReserve(t, nodeA, 8, time.Hour)
	f := createFile(t, nodeA, "f")
	writeAt(t, f, pattern(1, 4096), 0)
	expectRefills(t, resA)
	var held []uint64
	for {
		b, ok := resA.TakeReady()
		if !ok {
			break
		}
		held = append(held, b)
	}

	ctx := context.Background()
	if err := nodeB.lockMgr.Acquire(ctx, lock.LockTypeBlockBitmap, 0, lock.LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	refilled := make(chan struct{})
	go func() {
		defer close(refilled)
		resA.Take(ctx)
	}()
	time.Sleep(100 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		var st fuse.StatfsOut
		nodeA.Statfs(ctx, &st)
		if err := nodeA.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(f.inodeNum), lock.LockModeExclusive); err != nil {
			t.Error(err)
			return
		}
		resA.Untake(held[0])
		resA.TakeReady()
		_ = resA.Logged(held[1:], func() error { return nil })
		nodeA.release(lock.LockTypeInode, uint64(f.inodeNum), "inode")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("statfs or an inode-lock holder waited for the refill")
	}
	nodeB.release(lock.LockTypeBlockBitmap, 0, "bitmap")
	<-refilled
}

// Node B finds the bitmap full while node A holds blocks in reserve. B asks for
// them back and waits; A gives them back, and B's write gets a block instead of
// failing with ENOSPC.
func TestFullBitmapWaitsForPeersReserve(t *testing.T) {
	path := newFormattedImageOfSize(t, superblock.MinDeviceSize+128<<20)
	nodeA, nodeB := journaledNode(t, path, 0), journaledNode(t, path, 1)
	if err := nodeA.InitRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	resA := withReserve(t, nodeA, 4, time.Hour)
	resB := withReserve(t, nodeB, 4, time.Hour)
	defer expectRefills(t, resA)
	resB.SetBroadcast(resA.AskReturn)
	writeAt(t, createFile(t, nodeA, "a"), pattern(1, 4096), 0)
	if resA.Stats().Blocks == 0 {
		t.Fatal("node A holds no reserve")
	}
	free := nodeB.bitmapMgr.FreeCount()
	if _, err := nodeB.bitmapMgr.Alloc(context.Background(), int(free)); err != nil {
		t.Fatalf("filling the bitmap: %v", err)
	}

	start := time.Now()
	n, errno := createFile(t, nodeB, "b").Write(context.Background(), nil, pattern(2, 4096), 0)
	if errno != 0 || n != 4096 {
		t.Fatalf("write with blocks in node A's reserve: n=%d errno=%v, want a block from the reserve", n, errno)
	}
	if s := resB.Stats(); s.Waits != 1 || time.Since(start) > reserve.MaxPeerWait {
		t.Errorf("node B's waits: %+v after %v, want one wait shorter than %v", s, time.Since(start), reserve.MaxPeerWait)
	}
}

// With nothing left in any reserve, a write still fails with ENOSPC, after the
// wait for other nodes.
func TestFullBitmapWithoutReservesIsENOSPC(t *testing.T) {
	if testing.Short() {
		t.Skip("waits reserve.MaxPeerWait")
	}
	path := newFormattedImageOfSize(t, superblock.MinDeviceSize+128<<20)
	fsys := journaledNode(t, path, 0)
	if err := fsys.InitRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	withReserve(t, fsys, 4, time.Hour)
	f := createFile(t, fsys, "f")
	if _, err := fsys.bitmapMgr.Alloc(context.Background(), int(fsys.bitmapMgr.FreeCount())); err != nil {
		t.Fatal(err)
	}
	if _, errno := f.Write(context.Background(), nil, pattern(1, 4096), 0); errno != syscall.ENOSPC {
		t.Fatalf("write on a full device: %v, want ENOSPC", errno)
	}
}

// Many writers share one reserve while it refills and goes back when idle.
// Every file keeps its data and fsck finds nothing after unmount.
func TestConcurrentWritersShareReserve(t *testing.T) {
	fsys, path, _, _ := reserveFS(t)
	res := withReserve(t, fsys, 8, 100*time.Millisecond)
	const writers, perWriter = 4, 12
	files := make([][]*File, writers)
	for w := range files {
		for i := range perWriter {
			files[w] = append(files[w], createFile(t, fsys, fmt.Sprintf("w%d-%d", w, i)))
		}
	}
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i, f := range files[w] {
				data := pattern(byte(w*perWriter+i), 2*4096)
				if n, errno := f.Write(context.Background(), nil, data, int64(i%2)*BlockSize); errno != 0 || int(n) != len(data) {
					t.Errorf("writer %d file %d: n=%d errno=%v", w, i, n, errno)
				}
				if i%5 == 0 {
					time.Sleep(150 * time.Millisecond)
				}
			}
		})
	}
	wg.Wait()
	expectRefills(t, res)
	for w := range writers {
		for i, f := range files[w] {
			checkAt(t, f, pattern(byte(w*perWriter+i), 2*4096), int64(i%2)*BlockSize)
		}
	}
	if err := fsys.Close(); err != nil {
		t.Fatal(err)
	}
	expectCleanFsck(t, openFormattedFS(t, path))
}

// A reserve record stays pending while the reserve lasts, which keeps the
// journal head from passing it. Many small writes, as FUSE sends, fill the ring
// faster than the periodic checkpoint runs, so the record must be renewed as
// soon as the ring fills up, or commits fail with a full journal.
func TestReserveRecordDoesNotFillJournal(t *testing.T) {
	fsys, _, _, _ := reserveFS(t)
	withReserve(t, fsys, 64, time.Hour)
	const files, writes, chunk = 100, 8, 128 << 10
	for i := range files {
		f := createFile(t, fsys, fmt.Sprintf("f%d", i))
		for w := range writes {
			if n, errno := f.Write(context.Background(), nil, pattern(byte(i), chunk), int64(w*chunk)); errno != 0 || n != chunk {
				t.Fatalf("file %d write %d: n=%d errno=%v journal=%+v", i, w, n, errno, fsys.journal.Stats())
			}
		}
	}
}
