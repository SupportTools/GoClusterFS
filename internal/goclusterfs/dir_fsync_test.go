package goclusterfs

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// cacheLoss models a device whose volatile write cache loses every write since the last flush when power fails.
type cacheLoss struct {
	dev  *storage.Device
	mu   sync.Mutex
	undo []replaced
}

type replaced struct {
	offset int64
	old    []byte
}

func loseUnflushedWrites(t *testing.T, dev *storage.Device) *cacheLoss {
	t.Helper()
	c := &cacheLoss{dev: dev}
	dev.SetWriteObserver(func(offset int64, n int) {
		buf := storage.GetBuffer(n)
		defer storage.PutBuffer(buf)
		if _, err := dev.ReadAtUnsharded(buf.Bytes()[:n], offset); err != nil {
			t.Errorf("save what the write at %d replaces: %v", offset, err)
			return
		}
		c.mu.Lock()
		c.undo = append(c.undo, replaced{offset, slices.Clone(buf.Bytes()[:n])})
		c.mu.Unlock()
	})
	dev.SetSyncObserver(func() {
		c.mu.Lock()
		c.undo = nil
		c.mu.Unlock()
	})
	t.Cleanup(func() {
		dev.SetWriteObserver(nil)
		dev.SetSyncObserver(nil)
	})
	return c
}

// cutPower stops the device's writes, then puts back, through another handle on the image, what every write since
// the last flush replaced.
func (c *cacheLoss) cutPower(t *testing.T, path string) {
	t.Helper()
	c.dev.SetWriteGate(func(int64) error { return errPowerCut })
	c.mu.Lock()
	undo := c.undo
	c.undo = nil
	c.mu.Unlock()

	dev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := dev.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, w := range slices.Backward(undo) {
		buf := storage.GetBuffer(len(w.old))
		copy(buf.Bytes(), w.old)
		_, err := dev.WriteAt(buf.Bytes()[:len(w.old)], w.offset)
		storage.PutBuffer(buf)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Without a journal, a directory fsync is the only flush. Entries made or removed before it survive a power cut that
// loses every later write; the create after the last fsync does not.
func TestDirFsyncSurvivesPowerCut(t *testing.T) {
	ctx := context.Background()
	path := newFormattedImage(t)
	fsys := openFormattedFS(t, path)
	createFile(t, fsys, "gone")
	sub := newSubdir(t, fsys, "sub")
	if errno := fsys.Fsync(ctx, nil, 0); errno != 0 {
		t.Fatalf("fsync of the root: %v", errno)
	}

	cache := loseUnflushedWrites(t, fsys.dev)
	createFile(t, fsys, "kept")
	if errno := fsys.RootDir().Unlink(ctx, "gone"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	if errno := fsys.Fsync(ctx, nil, 0); errno != 0 {
		t.Fatalf("fsync of the root: %v", errno)
	}
	newDirIn(t, fsys, sub.inodeNum, "inner")
	if errno := sub.Fsync(ctx, nil, 0); errno != 0 {
		t.Fatalf("fsync of a subdirectory: %v", errno)
	}
	createFile(t, fsys, "unsynced")
	cache.cutPower(t, path)

	after := openFormattedFS(t, path)
	names := expectOneNamePerInode(t, after)
	for name, want := range map[string]bool{"kept": true, "sub": true, "gone": false, "unsynced": false} {
		if _, ok := names[name]; ok != want {
			t.Errorf("%q in the root after the power cut: %v, want %v", name, ok, want)
		}
	}
	subIno, err := after.inodeTbl.Read(sub.inodeNum)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := after.dirMgr.Lookup(ctx, subIno, "inner"); err != nil {
		t.Errorf("%q in the subdirectory after the power cut: %v", "inner", err)
	}
	expectCleanFsck(t, after)
}
