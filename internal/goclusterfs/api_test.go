package goclusterfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
)

// apiTree holds /f, /d/g, /d/sub, a symlink /ln to f and a symlink /dl to d.
func apiTree(t *testing.T) (*FS, *File, *File) {
	t.Helper()
	fsys := symlinkFS(t)
	f := createFile(t, fsys, "f")
	d := newSubdir(t, fsys, "d")
	g := fileIn(t, fsys, d.inodeNum, "g")
	newDirIn(t, fsys, d.inodeNum, "sub")
	makeSymlink(t, fsys, "ln", "f")
	makeSymlink(t, fsys, "dl", "d")
	return fsys, f, g
}

func readFileAt(t *testing.T, fsys *FS, p string, n int) []byte {
	t.Helper()
	file, err := fsys.lookupFile(context.Background(), p)
	if err != nil {
		t.Fatalf("lookup %s: %v", p, err)
	}
	buf := make([]byte, n)
	res, errno := file.Read(context.Background(), nil, buf, 0)
	if errno != 0 {
		t.Fatalf("read %s: %v", p, errno)
	}
	data, _ := res.Bytes(buf)
	return data
}

func TestLookupFile(t *testing.T) {
	ctx := context.Background()
	fsys, f, g := apiTree(t)
	for p, want := range map[string]uint32{"/f": f.inodeNum, "/d/g": g.inodeNum} {
		file, err := fsys.lookupFile(ctx, p)
		if err != nil || file.inodeNum != want {
			t.Errorf("lookup %s: inode %v, %v; want %d", p, file, err, want)
		}
	}
	for p, want := range map[string]syscall.Errno{
		"/d":         syscall.EISDIR,
		"/d/sub":     syscall.EISDIR,
		"/ln":        syscall.ELOOP,
		"/dl/g":      syscall.ELOOP,
		"/f/x":       syscall.ENOTDIR,
		"/missing":   syscall.ENOENT,
		"/d/missing": syscall.ENOENT,
		"/d/../f":    syscall.EINVAL,
		"d/g":        syscall.EINVAL,
		"/":          syscall.EINVAL,
		"/d/g/":      syscall.EINVAL,
	} {
		if _, err := fsys.lookupFile(ctx, p); !errors.Is(err, want) {
			t.Errorf("lookup %s: %v, want %v", p, err, want)
		}
	}
}

// The API methods reach the same code as setfattr, so they return what setxattr returns.
func TestAPIMethodsDoWhatSetxattrDoes(t *testing.T) {
	ctx := context.Background()
	fsys, _, g := apiTree(t)
	data := pattern(7, 3*BlockSize/2)
	writeAt(t, g, data, 0)

	if err := fsys.SnapshotFile(ctx, "/d/g", "s"); err != nil {
		t.Fatal(err)
	}
	if err := fsys.SnapshotFile(ctx, "/d/g", "s"); !errors.Is(err, syscall.EEXIST) {
		t.Errorf("second snapshot named s: %v, want EEXIST", err)
	}
	descs, err := fsys.SnapshotsAt(ctx, "/d/g")
	if err != nil || len(descs) != 1 || descs[0].GetName() != "s" {
		t.Fatalf("snapshots of /d/g: %v, %v; want s", descs, err)
	}
	if other, err := fsys.SnapshotsAt(ctx, "/f"); err != nil || len(other) != 0 {
		t.Errorf("snapshots of /f: %v, %v; want none", other, err)
	}

	writeAt(t, g, pattern(9, BlockSize), 0)
	if err := fsys.CloneFileSnapshot(ctx, "/d/g", "s", "/from-snap"); err != nil {
		t.Fatal(err)
	}
	if got := readFileAt(t, fsys, "/from-snap", len(data)); !bytes.Equal(got, data) {
		t.Error("the clone of snapshot s does not hold the data the file had when s was taken")
	}
	if err := fsys.CloneFile(ctx, "/d/g", "/copy"); err != nil {
		t.Fatal(err)
	}
	want := append(pattern(9, BlockSize), data[BlockSize:]...)
	if got := readFileAt(t, fsys, "/copy", len(want)); !bytes.Equal(got, want) {
		t.Error("the clone of /d/g does not hold its current data")
	}
	if err := fsys.CloneFile(ctx, "/d/g", "/copy"); !errors.Is(err, syscall.EEXIST) {
		t.Errorf("clone onto an existing name: %v, want EEXIST", err)
	}
	if err := fsys.CloneFileSnapshot(ctx, "/f", "s", "/stolen"); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("clone of /d/g's snapshot through /f: %v, want ENOENT", err)
	}
	if err := fsys.DeleteFileSnapshot(ctx, "/f", "s"); !errors.Is(err, syscall.ENOENT) {
		t.Errorf("delete of /d/g's snapshot through /f: %v, want ENOENT", err)
	}
	if err := fsys.DeleteFileSnapshot(ctx, "/d/g", "s"); err != nil {
		t.Fatal(err)
	}
	if desc, err := fsys.snapshotReg.GetByName(ctx, "s"); err == nil && desc.State != snapshot.StateDeleting {
		t.Errorf("snapshot s after delete: state %v, want deleting or gone", desc.State)
	}
	if err := fsys.SnapshotFile(ctx, "/ln", "t"); !errors.Is(err, syscall.ELOOP) {
		t.Errorf("snapshot through a symlink: %v, want ELOOP", err)
	}
}

// A file replaced between the lookup and the inode lock must not get the request.
func TestAPIRequestOnReplacedFileIsStale(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	old := createFile(t, fsys, "old")
	looked, err := fsys.lookupFile(ctx, "/old")
	if err != nil {
		t.Fatal(err)
	}
	if errno := fsys.Unlink(ctx, "old"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	reused := createFile(t, fsys, "new")
	if reused.inodeNum != old.inodeNum {
		t.Fatalf("the new file got inode %d, want the freed inode %d", reused.inodeNum, old.inodeNum)
	}
	if errno := looked.setxattr(ctx, "user.snapshot", []byte("s")); errno != syscall.ESTALE {
		t.Fatalf("snapshot through the old lookup: %v, want ESTALE", errno)
	}
	if descs, err := fsys.FileSnapshots(ctx, reused.inodeNum); err != nil || len(descs) != 0 {
		t.Fatalf("the new file has snapshots %v, %v", descs, err)
	}
}

// Without the kernel's i_rwsem, the inode lock alone must keep each write whole: a snapshot or clone taken while this
// node writes holds exactly one version. A write of up to 128 KiB is one FUSE request.
func TestAPISnapshotWhileTheNodeWrites(t *testing.T) {
	ctx := context.Background()
	fsys := openFormattedFS(t, newFormattedImage(t))
	const size = 128 << 10
	file := createFile(t, fsys, "w")
	writeAt(t, file, pattern(0, size), 0)

	var stop atomic.Bool
	var writes sync.WaitGroup
	var writeErr atomic.Value
	writes.Add(1)
	go func() {
		defer writes.Done()
		for v := 1; !stop.Load(); v++ {
			if _, errno := file.Write(ctx, nil, pattern(byte(v), size), 0); errno != 0 {
				writeErr.Store(fmt.Errorf("write version %d: %v", v, errno))
				return
			}
		}
	}()

	whole := func(what string, data []byte) {
		t.Helper()
		if len(data) != size || !bytes.Equal(data, pattern(data[0], size)) {
			t.Errorf("%s holds a mix of writes, not one version", what)
		}
	}
	retry := func(op func() error) error {
		for {
			err := op()
			if !errors.Is(err, syscall.EAGAIN) {
				return err
			}
		}
	}
	for k := 0; k < 12; k++ {
		name := fmt.Sprintf("s%d", k)
		if err := retry(func() error { return fsys.SnapshotFile(ctx, "/w", name) }); err != nil {
			t.Fatalf("snapshot %s: %v", name, err)
		}
		snapClone, copyName := "/"+name+"-clone", fmt.Sprintf("/copy%d", k)
		if err := fsys.CloneFileSnapshot(ctx, "/w", name, snapClone); err != nil {
			t.Fatalf("clone of %s: %v", name, err)
		}
		whole("the clone of snapshot "+name, readFileAt(t, fsys, snapClone, size))
		if err := retry(func() error { return fsys.CloneFile(ctx, "/w", copyName) }); err != nil {
			t.Fatalf("clone %s: %v", copyName, err)
		}
		whole("clone "+copyName, readFileAt(t, fsys, copyName, size))
		for _, n := range []string{snapClone[1:], copyName[1:]} {
			if errno := fsys.Unlink(ctx, n); errno != 0 {
				t.Fatalf("unlink %s: %v", n, errno)
			}
		}
		if err := fsys.DeleteFileSnapshot(ctx, "/w", name); err != nil {
			t.Fatalf("delete %s: %v", name, err)
		}
	}
	stop.Store(true)
	writes.Wait()
	if err, ok := writeErr.Load().(error); ok {
		t.Fatal(err)
	}
	ino, err := fsys.inodeTbl.Read(file.inodeNum)
	if err != nil || ino.Magic != inode.InodeMagic {
		t.Fatalf("the written file afterwards: %v, %v", ino, err)
	}
}
