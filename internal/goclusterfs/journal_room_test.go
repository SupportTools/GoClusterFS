package goclusterfs

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
)

// fillJournal commits records it leaves open until the journal refuses another, and returns them.
func fillJournal(t *testing.T, j *journal.Journal) []uint64 {
	t.Helper()
	var open []uint64
	for n := uint64(1); ; n++ {
		txn := j.Begin()
		if err := txn.LogBlockUse([]uint64{n}); err != nil {
			t.Fatal(err)
		}
		seq, err := j.Commit(txn)
		if errors.Is(err, journal.ErrJournalFull) {
			j.Abort(txn)
			return open
		}
		if err != nil {
			t.Fatal(err)
		}
		open = append(open, seq)
	}
}

// A write that finds the journal full of open intents waits until they complete, then succeeds, instead of failing
// with EIO.
func TestWriteWaitsForJournalRoom(t *testing.T) {
	fsys, _, _, _ := journaledFS(t)
	f := createFile(t, fsys, "f")
	open := fillJournal(t, fsys.journal)
	wrote := make(chan syscall.Errno, 1)
	go func() {
		_, errno := f.Write(context.Background(), nil, pattern(1, 4096), 0)
		wrote <- errno
	}()
	select {
	case errno := <-wrote:
		t.Fatalf("a write into a journal full of open intents returned %v before any of them completed", errno)
	case <-time.After(300 * time.Millisecond):
	}
	for _, seq := range open {
		if err := fsys.journal.CompleteIntent(seq); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case errno := <-wrote:
		if errno != 0 {
			t.Fatalf("write after the open intents completed: %v", errno)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the write still waits 10s after every open intent completed")
	}
}

// A write that stops between allocation rounds, here for the bitmap lock another node holds, does not hold the
// journal head back with the records of its earlier rounds: the journal goes on taking records meanwhile.
func TestWriteBetweenRoundsDoesNotHoldTheJournalHead(t *testing.T) {
	nodeA, path, _, size := reserveFS(t)
	nodeB := openFormattedFS(t, path)
	resA := withReserve(t, nodeA, 8, time.Hour)
	f := createFile(t, nodeA, "f")
	writeAt(t, f, pattern(1, 4096), 0)
	expectRefills(t, resA)

	ctx := context.Background()
	if err := nodeB.lockMgr.Acquire(ctx, lock.LockTypeBlockBitmap, 0, lock.LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	bs := int(nodeA.dataBlockSize())
	wrote := make(chan syscall.Errno, 1)
	go func() {
		_, errno := f.Write(ctx, nil, pattern(2, 16*bs), int64(bs))
		wrote <- errno
	}()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if ino, err := nodeA.inodeTbl.Read(f.inodeNum); err == nil && ino.AllocatedBlocks > 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the write mapped no reserve blocks in 10s")
		}
	}

	records := 2 * int(size/8192)
	for n := range records {
		admitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		a, err := nodeA.journal.Admit(admitCtx, 512)
		cancel()
		if err != nil {
			t.Fatalf("record %d of %d while the write waits between rounds: %v", n, records, err)
		}
		txn := nodeA.journal.Begin()
		if err := txn.LogBlockUse([]uint64{uint64(n)}); err != nil {
			t.Fatal(err)
		}
		seq, err := nodeA.journal.CommitAdmitted(a, txn)
		if err != nil {
			t.Fatal(err)
		}
		if err := nodeA.journal.CompleteIntent(seq); err != nil {
			t.Fatal(err)
		}
	}
	nodeB.release(lock.LockTypeBlockBitmap, 0, "bitmap")
	if errno := <-wrote; errno != 0 {
		t.Fatalf("the write after the bitmap lock was freed: %v", errno)
	}
}
