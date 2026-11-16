package fileops

import (
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// fuzzTree builds a mount root with files, a directory, a FIFO and symlinks
// that point inside it, outside it, and back up.
func fuzzTree(tb testing.TB) string {
	base := tb.TempDir()
	root := filepath.Join(base, "mnt")
	outside := filepath.Join(base, "secret")
	for _, d := range []string{root, filepath.Join(root, "dir", "sub"), outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			tb.Fatal(err)
		}
	}
	for _, f := range []string{"f", "dir/g", "dir/sub/h", "a b", "x:y"} {
		if err := os.WriteFile(filepath.Join(root, f), []byte("x"), 0o644); err != nil {
			tb.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(outside, "passwd"), []byte("x"), 0o644); err != nil {
		tb.Fatal(err)
	}
	for link, to := range map[string]string{
		"link": filepath.Join(outside, "passwd"), "linkdir": outside, "inside": "f",
		"dir/up": "..", "dir/sub/loop": "../sub", "dir/abs": root,
	} {
		if err := os.Symlink(to, filepath.Join(root, link)); err != nil {
			tb.Fatal(err)
		}
	}
	if err := unix.Mkfifo(filepath.Join(root, "fifo"), 0o644); err != nil {
		tb.Fatal(err)
	}
	return root
}

// FuzzFileOpsPaths checks the path, name and destination checks the API's
// writes go through, and that a path they accept opens the file it names
// under the mount root and nothing else.
func FuzzFileOpsPaths(f *testing.F) {
	for _, p := range []string{"/f", "/dir/g", "/link", "/linkdir/passwd", "/dir/up/f", "/dir/sub/loop/h", "/dir/abs/f", "/fifo", "/a b", "/x:y", "/../f", "//f"} {
		f.Add(p, "snap", "/copy")
	}
	f.Add("/f", "a:b", "/c")
	f.Add("/f", "s", "/c:d")
	f.Add("/f", "s\x00", "/..")

	root := fuzzTree(f)
	o := &Ops{root: root}
	f.Fuzz(func(t *testing.T, p, name, dest string) {
		rel, err := relative(p)
		if err == nil && (!strings.HasPrefix(p, "/") || path.Clean(p) != p || p == "/" || strings.ContainsRune(p, 0) || rel != p[1:]) {
			t.Fatalf("relative accepted %q as %q", p, rel)
		}
		if checkName(name) == nil && (name == "" || strings.ContainsAny(name, ":/\x00")) {
			t.Fatalf("checkName accepted %q", name)
		}
		if checkDest(dest) == nil {
			if !strings.HasPrefix(dest, "/") || strings.Count(dest, "/") != 1 || path.Clean(dest) != dest || strings.ContainsAny(dest, ":\x00") {
				t.Fatalf("checkDest accepted %q", dest)
			}
			if checkName(name) == nil {
				if parts := strings.SplitN(name+":"+dest, ":", 2); parts[0] != name || parts[1] != dest {
					t.Fatalf("snapshot clone value %q splits into %q", name+":"+dest, parts)
				}
			}
		}

		file, err := o.open(p)
		if err != nil {
			return
		}
		defer func() { _ = file.Close() }()
		opened, err := os.Readlink("/proc/self/fd/" + strconv.Itoa(int(file.Fd())))
		if err != nil {
			t.Fatal(err)
		}
		if want := filepath.Join(root, rel); opened != want {
			t.Fatalf("open(%q) opened %s, want %s", p, opened, want)
		}
	})
}
