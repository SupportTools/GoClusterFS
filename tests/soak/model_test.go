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
	step(t, m, &Op{Kind: "orphandelete", Name: "l0-s1"}, 0)
	step(t, m, &Op{Kind: "orphandelete", Name: "l0-s1"}, int(syscall.ENOENT))

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
	step(t, m, &Op{Kind: "orphandelete", Name: "l0-s2"}, int(syscall.EBUSY))
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

// A rename between two names of one file changes nothing, and replacing one name of a linked file keeps its snapshots.
func TestModelRenamesHardLinks(t *testing.T) {
	m := newModel()
	a, b, c := "/soak/d0/l0-f1", "/soak/d0/l0-hl1", "/soak/d1/l0-f2"
	step(t, m, &Op{Kind: "create", Path: a}, 0)
	step(t, m, &Op{Kind: "snapshot", Path: a, Name: "l0-s1"}, 0)
	step(t, m, &Op{Kind: "link", Path: a, Path2: b}, 0)
	step(t, m, &Op{Kind: "rename", Path: a, Path2: b}, 0)
	if m.Files[a] == nil || m.Files[a] != m.Files[b] || m.Snaps["l0-s1"].Owner != m.Files[a] {
		t.Fatal("a rename between two names of one file must change nothing")
	}
	step(t, m, &Op{Kind: "create", Path: c}, 0)
	step(t, m, &Op{Kind: "rename", Path: c, Path2: b}, 0)
	if m.Snaps["l0-s1"].Owner != m.Files[a] {
		t.Fatal("a rename over one name of a linked file must keep the file's snapshots")
	}
	step(t, m, &Op{Kind: "unlink", Path: a}, 0)
	if m.Snaps["l0-s1"].Owner != nil {
		t.Fatal("removing the file's last name must orphan its snapshots")
	}
}

func TestModelRenamesAsTheFilesystemDoes(t *testing.T) {
	m := newModel()
	a, b, d, e := "/soak/d0/l0-f1", "/soak/d1/l0-f2", "/soak/d0/l0-d1", "/soak/d1/l0-d2"
	link := "/soak/d0/l0-ln1"
	step(t, m, &Op{Kind: "create", Path: a}, 0)
	step(t, m, &Op{Kind: "write", Path: a, Len: 5, Seed: 1}, 0)
	step(t, m, &Op{Kind: "snapshot", Path: a, Name: "l0-s1"}, 0)
	step(t, m, &Op{Kind: "create", Path: b}, 0)
	step(t, m, &Op{Kind: "snapshot", Path: b, Name: "l0-s2"}, 0)
	step(t, m, &Op{Kind: "mkdir", Path: d}, 0)
	step(t, m, &Op{Kind: "mkdir", Path: e}, 0)
	step(t, m, &Op{Kind: "symlink", Path: link, Path2: "l0-x"}, 0)

	step(t, m, &Op{Kind: "rename", Path: "/soak/d0/l0-missing", Path2: "/soak/d0/l0-r1"}, int(syscall.ENOENT))
	step(t, m, &Op{Kind: "rename", Path: a, Path2: "/soak/d0/l0-gone/l0-r1"}, int(syscall.ENOENT))
	step(t, m, &Op{Kind: "rename", Path: a, Path2: a}, 0)
	step(t, m, &Op{Kind: "rename", Path: a, Path2: d}, int(syscall.EISDIR))
	step(t, m, &Op{Kind: "rename", Path: d, Path2: b}, int(syscall.ENOTDIR))
	step(t, m, &Op{Kind: "rename", Path: d, Path2: d + "/l0-d3"}, int(syscall.EINVAL))
	step(t, m, &Op{Kind: "create", Path: d + "/l0-f9"}, 0)
	step(t, m, &Op{Kind: "rename", Path: d + "/l0-f9", Path2: d}, int(syscall.ENOTEMPTY))
	step(t, m, &Op{Kind: "unlink", Path: d + "/l0-f9"}, 0)

	step(t, m, &Op{Kind: "rename", Path: a, Path2: b}, 0)
	if m.Files[a] != nil || string(m.Files[b].Data) != string(genData(1, 5)) || m.Snaps["l0-s2"].Owner != nil || m.Snaps["l0-s1"].Owner != m.Files[b] {
		t.Fatal("a rename over a file moves the file, its data and its snapshots, and orphans the replaced file's snapshots")
	}
	step(t, m, &Op{Kind: "rename", Path: e, Path2: d}, 0)
	if !m.Dirs[d] || m.Dirs[e] {
		t.Fatal("an empty directory renamed over another replaces it")
	}
	step(t, m, &Op{Kind: "mkdir", Path: e}, 0)
	step(t, m, &Op{Kind: "create", Path: e + "/l0-f3"}, 0)
	step(t, m, &Op{Kind: "rename", Path: d, Path2: e}, int(syscall.ENOTEMPTY))
	step(t, m, &Op{Kind: "rename", Path: b, Path2: d + "/l0-f2"}, 0)
	step(t, m, &Op{Kind: "rename", Path: link, Path2: d + "/l0-ln1"}, 0)
	step(t, m, &Op{Kind: "rename", Path: d, Path2: "/soak/d2/l0-d4"}, 0)
	want := []string{"f:l0-f2:5", "l:l0-ln1:l0-x"}
	if got := m.listing("/soak/d2/l0-d4"); strings.Join(got, ",") != strings.Join(want, ",") || m.Dirs[d] || m.pathOf(m.Snaps["l0-s1"].Owner) != "/soak/d2/l0-d4/l0-f2" {
		t.Fatalf("moving a directory moves what is in it, snapshots included: lists %v, want %v", got, want)
	}
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

// A write a fault cut short may leave each sector as before or as after, and nothing else.
func TestTornWriteIsSectorsOfBeforeOrAfter(t *testing.T) {
	before := genData(1, 10000)
	op := &Op{Kind: "write", Off: 2000, Len: 9000, Seed: 2}
	after := bytes.Clone(before)
	applyData(op, &after)
	torn := bytes.Clone(after[:8192])
	torn = append(torn, before[8192:]...)
	if !tornBetween(torn, before, after) {
		t.Error("first two sectors written and the rest not is a torn write")
	}
	mixed := bytes.Clone(after)
	mixed[5000] ^= 1
	if tornBetween(mixed, before, after) {
		t.Error("a sector that is neither before nor after passed")
	}
	if tornBetween(after[:len(before)-1], before, after) {
		t.Error("a result shorter than the file was before passed")
	}
}

// Allocations inside a project may meet its limit, nothing moves or links between projects, and a write the limit
// stops part way keeps the bytes it wrote.
func TestModelFollowsQuotaRules(t *testing.T) {
	m := newModel()
	in, out := "/soak/qa/l0-f1", "/soak/d0/l0-f2"
	step(t, m, &Op{Kind: "create", Path: in}, okOrEDQUOT)
	m.apply(&Op{Kind: "create", Path: in})
	step(t, m, &Op{Kind: "create", Path: out}, 0)
	step(t, m, &Op{Kind: "create", Path: in}, int(syscall.EEXIST))
	step(t, m, &Op{Kind: "write", Path: "/soak/qa/l0-missing", Len: 5}, int(syscall.ENOENT))
	step(t, m, &Op{Kind: "write", Path: in, Len: 5}, okOrEDQUOT)
	step(t, m, &Op{Kind: "truncate", Path: in, Len: 100}, 0)
	step(t, m, &Op{Kind: "snapshot", Path: in, Name: "l0-s1"}, okOrEDQUOT)
	step(t, m, &Op{Kind: "mkdir", Path: "/soak/qb/l0-d1"}, okOrEDQUOT)
	m.apply(&Op{Kind: "mkdir", Path: "/soak/qb/l0-d1"})

	step(t, m, &Op{Kind: "link", Path: in, Path2: "/soak/d1/l0-hl1"}, int(syscall.EXDEV))
	step(t, m, &Op{Kind: "link", Path: out, Path2: "/soak/qa/l0-hl2"}, int(syscall.EXDEV))
	step(t, m, &Op{Kind: "link", Path: in, Path2: "/soak/qb/l0-hl3"}, int(syscall.EXDEV))
	step(t, m, &Op{Kind: "link", Path: in, Path2: "/soak/qa/l0-hl4"}, 0)
	step(t, m, &Op{Kind: "rename", Path: in, Path2: "/soak/d1/l0-r1"}, int(syscall.EXDEV))
	step(t, m, &Op{Kind: "rename", Path: out, Path2: "/soak/qb/l0-d1/l0-r2"}, int(syscall.EXDEV))
	step(t, m, &Op{Kind: "rename", Path: in, Path2: "/soak/qa/l0-r3"}, 0)
	step(t, m, &Op{Kind: "rename", Path: "/soak/qb/l0-d1", Path2: "/soak/qa/l0-d2"}, int(syscall.EXDEV))
	step(t, m, &Op{Kind: "rename", Path: "/soak/qb/l0-d1", Path2: out}, int(syscall.ENOTDIR))
	step(t, m, &Op{Kind: "rename", Path: "/soak/qa/l0-missing", Path2: "/soak/d1/l0-r4"}, int(syscall.ENOENT))
	step(t, m, &Op{Kind: "clone", Path: "/soak/qa/l0-r3", Path2: "/l0-c1"}, int(syscall.EXDEV))
	step(t, m, &Op{Kind: "clone", Path: out, Path2: "/l0-c2"}, 0)
	step(t, m, &Op{Kind: "clone", Path: "/soak/qa/l0-r3", Path2: "/l0-c2"}, int(syscall.EEXIST))

	f := m.Files["/soak/qa/l0-r3"]
	m.apply(&Op{Kind: "write", Path: "/soak/qa/l0-r3", Off: 200, Len: 50, Seed: 9, Got: int(syscall.EDQUOT), Wrote: 20})
	if len(f.Data) != 220 || !bytes.Equal(f.Data[200:], genData(9, 20)) {
		t.Fatalf("a write that wrote 20 of 50 bytes before EDQUOT left %d bytes", len(f.Data))
	}
	m.apply(&Op{Kind: "write", Path: "/soak/qa/l0-r3", Off: 500, Len: 50, Seed: 9, Got: int(syscall.EDQUOT)})
	if len(f.Data) != 220 {
		t.Fatalf("a write that EDQUOT stopped before its first byte grew the file to %d bytes", len(f.Data))
	}
}

func TestParseQuotaUsage(t *testing.T) {
	u, err := parseQuotaUsage("project=3 bytes=67108864 inodes=0 used_bytes=1048576 used_inodes=7")
	if err != nil || u != (quotaUsage{limitBytes: 64 << 20, usedBytes: 1 << 20, usedInodes: 7}) {
		t.Fatalf("got %+v, %v", u, err)
	}
	if _, err := parseQuotaUsage("project=3 bytes=1"); err == nil {
		t.Fatal("a value without usage parsed")
	}
}
