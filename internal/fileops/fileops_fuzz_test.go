package fileops

import (
	"path"
	"strings"
	"testing"
)

// FuzzFileOpsPaths checks that the path, name and destination checks the API's writes go through pass only the forms
// they promise, and that a snapshot clone value splits back into its name and destination.
func FuzzFileOpsPaths(f *testing.F) {
	for _, p := range []string{"/f", "/dir/g", "/link", "/dir/up/f", "/a b", "/x:y", "/../f", "//f"} {
		f.Add(p, "snap", "/copy")
	}
	f.Add("/f", "a:b", "/c")
	f.Add("/f", "s", "/c:d")
	f.Add("/f", "s\x00", "/..")

	f.Fuzz(func(t *testing.T, p, name, dest string) {
		if checkPath(p) == nil && (!strings.HasPrefix(p, "/") || path.Clean(p) != p || p == "/" || strings.ContainsRune(p, 0)) {
			t.Fatalf("checkPath accepted %q", p)
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
	})
}
