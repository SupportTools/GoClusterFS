//go:build linux

package main

import (
	"bytes"
	"math/rand/v2"
	"strings"
	"syscall"
	"testing"
)

func step(t *testing.T, m *Model, op *Op, want int) {
	t.Helper()
	if got := m.expect(op); got != want {
		t.Fatalf("%s %s: expect %d, want %d", op.Kind, op.Path, got, want)
	}
	if want == 0 {
		m.apply(op)
	}
}

func TestModelFollowsTheFilesystemRules(t *testing.T) {
	m := newModel()
	f := "/soak/d0/l0-f1"
	step(t, m, &Op{Kind: "create", Path: f}, 0)
	step(t, m, &Op{Kind: "create", Path: f}, int(syscall.EEXIST))
	step(t, m, &Op{Kind: "write", Path: f, Off: 10, Len: 5, Seed: 7}, 0)
	if data := m.Files[f].Data; len(data) != 15 || data[0] != 0 || string(data[10:]) != string(genData(7, 5)) {
		t.Fatalf("write at 10 of an empty file left %v", data)
	}
	step(t, m, &Op{Kind: "snapshot", Path: f, Name: "l0-s1"}, 0)
	step(t, m, &Op{Kind: "snapshot", Path: f, Name: "l0-s1"}, int(syscall.EEXIST))
	step(t, m, &Op{Kind: "mkdir", Path: "/soak/d0/l0-d1"}, 0)
	step(t, m, &Op{Kind: "create", Path: "/soak/d0/l0-d1/l0-f2"}, 0)
	step(t, m, &Op{Kind: "rmdir", Path: "/soak/d0/l0-d1"}, int(syscall.ENOTEMPTY))
	step(t, m, &Op{Kind: "unlink", Path: f}, 0)
	if m.Snaps["l0-s1"].Owner != nil {
		t.Fatal("the deleted file's snapshot still has an owner")
	}
	step(t, m, &Op{Kind: "create", Path: f}, 0)
	step(t, m, &Op{Kind: "snapclone", Path: f, Name: "l0-s1", Path2: "/l0-c1"}, int(syscall.ENOENT))
	step(t, m, &Op{Kind: "snapdelete", Path: f, Name: "l0-s1"}, int(syscall.ENOENT))
	step(t, m, &Op{Kind: "rename", Path: f, Path2: "/soak/d0/l0-r1"}, anyErrno)

	link := "/soak/d0/l0-d1/l0-ln1"
	step(t, m, &Op{Kind: "symlink", Path: link, Path2: "../l0-f1"}, 0)
	step(t, m, &Op{Kind: "symlink", Path: link, Path2: "l0-x"}, int(syscall.EEXIST))
	step(t, m, &Op{Kind: "create", Path: link}, int(syscall.EEXIST))
	step(t, m, &Op{Kind: "symlink", Path: "/soak/d0/l0-gone/l0-ln2", Path2: "l0-x"}, int(syscall.ENOENT))
	step(t, m, &Op{Kind: "unlink", Path: "/soak/d0/l0-d1/l0-f2"}, 0)
	step(t, m, &Op{Kind: "rmdir", Path: "/soak/d0/l0-d1"}, int(syscall.ENOTEMPTY))
	if got := m.listing("/soak/d0/l0-d1"); len(got) != 1 || got[0] != "l:l0-ln1:../l0-f1" {
		t.Fatalf("listing with a symlink: %v", got)
	}
	step(t, m, &Op{Kind: "unlink", Path: link}, 0)
	step(t, m, &Op{Kind: "rmdir", Path: "/soak/d0/l0-d1"}, 0)

	hard := "/soak/d0/l0-hl1"
	step(t, m, &Op{Kind: "snapshot", Path: f, Name: "l0-s2"}, 0)
	step(t, m, &Op{Kind: "link", Path: f, Path2: hard}, 0)
	step(t, m, &Op{Kind: "link", Path: f, Path2: hard}, int(syscall.EEXIST))
	step(t, m, &Op{Kind: "link", Path: "/soak/d0/l0-missing", Path2: "/soak/d0/l0-hl2"}, int(syscall.ENOENT))
	step(t, m, &Op{Kind: "write", Path: hard, Off: 0, Len: 10, Seed: 7}, 0)
	if !bytes.Equal(m.Files[f].Data, m.Files[hard].Data) || m.Files[f] != m.Files[hard] {
		t.Fatal("a write through one hard link must show through the other")
	}
	step(t, m, &Op{Kind: "unlink", Path: f}, 0)
	step(t, m, &Op{Kind: "snapclone", Path: hard, Name: "l0-s2", Path2: "/l0-c2"}, 0)
}

// A lane only names and touches its own files, directories and snapshots, the same way for the same seed.
func TestLaneStaysInItsNamesAndRepeats(t *testing.T) {
	gen := func() []string {
		m := newModel()
		l := &lane{id: 1, rng: rand.New(rand.NewPCG(42, 1)), budget: 1000}
		var trace []string
		for range 2000 {
			op := l.generate(m)
			for _, p := range []string{op.Path, op.Path2, op.Name} {
				if p != "" && !strings.Contains(p, "/l1-") && !strings.HasPrefix(p, "l1-") {
					t.Fatalf("%s names %q, outside lane 1", op.Kind, p)
				}
			}
			if op.Kind != "rename" && m.expect(op) == 0 {
				m.apply(op)
			}
			trace = append(trace, op.Kind+" "+op.Path)
		}
		return trace
	}
	a, b := gen(), gen()
	if strings.Join(a, "\n") != strings.Join(b, "\n") {
		t.Fatal("the same seed generated different operations")
	}
}
