//go:build linux

package fileops

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/supporttools/GoClusterFS/internal/api"
)

func TestPathChecks(t *testing.T) {
	for _, p := range []string{"", "f", "/", "/a/../b", "/a/", "//a", "/a/./b", "/a\x00b"} {
		if _, err := relative(p); !errors.Is(err, api.ErrInvalidPath) {
			t.Errorf("relative(%q) = %v, want ErrInvalidPath", p, err)
		}
	}
	for _, p := range []string{"/f", "/dir/f", "/a b/c"} {
		if _, err := relative(p); err != nil {
			t.Errorf("relative(%q) = %v", p, err)
		}
	}
	for _, n := range []string{"", "a:b", "a/b", "a\x00"} {
		if err := checkName(n); !errors.Is(err, api.ErrInvalidPath) {
			t.Errorf("checkName(%q) = %v, want ErrInvalidPath", n, err)
		}
	}
	for _, d := range []string{"", "c", "/", "/dir/c", "/../c", "/c/", "/a:b"} {
		if err := checkDest(d); !errors.Is(err, api.ErrInvalidPath) {
			t.Errorf("checkDest(%q) = %v, want ErrInvalidPath", d, err)
		}
	}
	if err := checkDest("/copy"); err != nil {
		t.Errorf("checkDest(/copy) = %v", err)
	}
}

func TestOpenStaysInsideTheRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "mnt")
	outside := filepath.Join(base, "secret")
	for _, dir := range []string{root, filepath.Join(root, "dir"), outside} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{filepath.Join(root, "f"), filepath.Join(root, "dir", "g"), filepath.Join(outside, "passwd")} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(outside, "passwd"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "linkdir")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("f", filepath.Join(root, "inside-link")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(root, "fifo"), 0o644); err != nil {
		t.Fatal(err)
	}
	o := New(root, nil)
	if _, err := o.open("/f"); !errors.Is(err, api.ErrNotMounted) {
		t.Fatalf("open on a plain directory: %v, want ErrNotMounted", err)
	}
	o.checkRoot = nil

	for _, p := range []string{"/f", "/dir/g"} {
		f, err := o.open(p)
		if err != nil {
			t.Errorf("open(%q): %v", p, err)
			continue
		}
		_ = f.Close()
	}
	for _, p := range []string{"/link", "/linkdir/passwd", "/inside-link"} {
		if f, err := o.open(p); !errors.Is(err, api.ErrInvalidPath) {
			if f != nil {
				_ = f.Close()
			}
			t.Errorf("open(%q) = %v, want ErrInvalidPath", p, err)
		}
	}
	for _, p := range []string{"/dir", "/fifo"} {
		if err := o.CreateSnapshot(context.Background(), p, "s"); !errors.Is(err, api.ErrInvalidPath) {
			t.Errorf("snapshot of %s: %v, want ErrInvalidPath", p, err)
		}
	}
	if _, err := o.ListSnapshots(context.Background(), ""); !errors.Is(err, api.ErrDisabled) {
		t.Errorf("list without a registry: %v, want ErrDisabled", err)
	}
}

func TestSameDevice(t *testing.T) {
	dir := t.TempDir()
	fd, err := unix.Open(dir, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(fd) }()
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		t.Fatal(err)
	}
	if err := sameDevice(fd, uint64(st.Dev)); err != nil {
		t.Errorf("same device: %v", err)
	}
	if err := sameDevice(fd, uint64(st.Dev)+1); !errors.Is(err, api.ErrNotMounted) {
		t.Errorf("other device: %v, want ErrNotMounted", err)
	}
}
