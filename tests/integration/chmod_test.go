package integration

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// chmod changes only the permission bits: a file stays a readable regular
// file, a directory stays a directory, and fsck finds nothing wrong.
func TestChmodKeepsFileType(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount: %v", err)
	}
	data := []byte("chmod keeps the file type")
	if err := mfs.WriteFile("f.txt", data); err != nil {
		t.Fatal(err)
	}
	if err := mfs.Mkdir("d"); err != nil {
		t.Fatal(err)
	}
	if err := mfs.WriteFile("d/inner.txt", data); err != nil {
		t.Fatal(err)
	}

	chmod := func(name string, mode os.FileMode) {
		t.Helper()
		if err := os.Chmod(mfs.Path(name), mode); err != nil {
			t.Fatalf("chmod %o %s: %v", mode, name, err)
		}
	}
	read := func(name string) {
		t.Helper()
		got, err := mfs.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("read %s: got %q", name, got)
		}
	}

	chmod("f.txt", 0)
	chmod("f.txt", 0o640)
	read("f.txt")
	chmod("d", 0o700)
	chmod("", 0o750)

	for name, want := range map[string]os.FileMode{
		"f.txt":       0o640,
		"d":           os.ModeDir | 0o700,
		"":            os.ModeDir | 0o750,
		"d/inner.txt": 0o644,
	} {
		fi, err := mfs.Stat(name)
		if err != nil {
			t.Fatalf("stat %q: %v", name, err)
		}
		if got := fi.Mode() & (os.ModeType | os.ModePerm); got != want {
			t.Errorf("stat %q: mode %v, want %v", name, got, want)
		}
	}
	read("d/inner.txt")

	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}
	out := expectFsck(t, 0, "Status: CLEAN", mfs.ImagePath)
	if lines := problemLines(out); len(lines) > 0 {
		t.Fatalf("fsck found problems after chmod:\n%s", strings.Join(lines, "\n"))
	}
}
