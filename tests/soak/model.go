//go:build linux

package main

import (
	"bytes"
	"fmt"
	"maps"
	"math/rand/v2"
	"path"
	"slices"
	"strings"
	"sync"
	"syscall"
)

const (
	blockSize = 1 << 20 // the unit of the budget and of write sizes, whatever the volume's block size
	maxFile   = 8 << 20
	// anyErrno is the expected result of an operation that must fail, with any errno.
	anyErrno = -2
	// okOrEDQUOT is the expected result of an operation that allocates in a quota project: it works, or the project's
	// limit refuses it.
	okOrEDQUOT = -3
	// maxInlineTarget is the longest symlink target an inode holds; longer ones take a block.
	maxInlineTarget = 92
)

// quotaDirs are project roots and their limits. qa limits bytes and qb inodes, so lanes reach both limits.
var quotaDirs = map[string]string{"/soak/qa": "bytes=67108864 inodes=0", "/soak/qb": "bytes=0 inodes=24"}

// sharedDirs hold files of every lane and are never removed.
var sharedDirs = []string{"/soak/d0", "/soak/d1", "/soak/d2", "/soak/d3", "/soak/qa", "/soak/qb"}

// projectOf returns the project root that p is or lies under, or "" outside every project.
func projectOf(p string) string {
	for root := range quotaDirs {
		if p == root || strings.HasPrefix(p, root+"/") {
			return root
		}
	}
	return ""
}

// allocates reports whether a successful op adds blocks or inodes to the project it lands in.
func allocates(op *Op) bool {
	switch op.Kind {
	case "create", "mkdir", "symlink":
		return projectOf(path.Dir(op.Path)) != ""
	case "write", "snapshot":
		return projectOf(op.Path) != ""
	}
	return false
}

// Op is one operation as the driver runs and logs it.
type Op struct {
	Seq   uint64 `json:"seq"`
	Lane  int    `json:"lane"`
	Node  int    `json:"node"`
	Kind  string `json:"op"`
	Path  string `json:"path,omitempty"`
	Path2 string `json:"path2,omitempty"`
	Name  string `json:"name,omitempty"`
	Off   int64  `json:"off,omitempty"`
	Len   int64  `json:"len,omitempty"`
	Seed  uint64 `json:"seed,omitempty"`
	Want  int    `json:"want"`
	Got   int    `json:"got"`
	Wrote int64  `json:"wrote,omitempty"` // bytes a write refused by EDQUOT wrote first
	Err   string `json:"err,omitempty"`
	At    int64  `json:"at"`
	Ms    int64  `json:"ms"`
}

func (op *Op) request() request {
	r := request{Op: op.Kind, Path: op.Path, Path2: op.Path2, Off: op.Off, Len: op.Len, Seed: op.Seed}
	switch op.Kind {
	case "snapshot":
		r.Op, r.Attr, r.Value = "setxattr", "user.snapshot", op.Name
	case "clone":
		r.Op, r.Attr, r.Value = "setxattr", "user.clone", op.Path2
	case "snapclone":
		r.Op, r.Attr, r.Value = "setxattr", "user.snapshot.clone", op.Name+":"+op.Path2
	case "snapdelete":
		r.Op, r.Attr, r.Value = "setxattr", "user.snapshot.delete", op.Name
	case "orphandelete":
		r.Op, r.Value = "apidelete", op.Name
	}
	return r
}

// File is a file in the model. Its pointer is its identity: a later file at the same path is another File.
type File struct {
	Data  []byte
	Snaps map[string]bool
}

// Snap is a snapshot. Owner is nil once its file is gone, which makes it an orphan.
type Snap struct {
	Data  []byte
	Owner *File
}

// Model is the state every node must show once an acknowledged operation is visible.
type Model struct {
	mu    sync.Mutex
	Files map[string]*File
	Dirs  map[string]bool
	Snaps map[string]*Snap
	Links map[string]string // symlink path -> target
}

func newModel() *Model {
	m := &Model{Files: map[string]*File{}, Dirs: map[string]bool{"/": true, "/soak": true}, Snaps: map[string]*Snap{}, Links: map[string]string{}}
	for _, d := range sharedDirs {
		m.Dirs[d] = true
	}
	return m
}

// named reports whether a path still names file f, which a hard link shares between names.
func (m *Model) named(f *File) bool {
	for _, g := range m.Files {
		if g == f {
			return true
		}
	}
	return false
}

func (m *Model) exists(p string) bool { return m.Files[p] != nil || m.Dirs[p] || m.Links[p] != "" }

func (m *Model) hasChildren(dir string) bool {
	for p := range m.Files {
		if path.Dir(p) == dir {
			return true
		}
	}
	for p := range m.Dirs {
		if p != dir && path.Dir(p) == dir {
			return true
		}
	}
	for p := range m.Links {
		if path.Dir(p) == dir {
			return true
		}
	}
	return false
}

// expect returns the errno the operation must return on the model's current state.
func (m *Model) expect(op *Op) int {
	want := m.expectErrno(op)
	if want == 0 && allocates(op) {
		return okOrEDQUOT
	}
	return want
}

func (m *Model) expectErrno(op *Op) int {
	f := m.Files[op.Path]
	switch op.Kind {
	case "create", "mkdir", "symlink":
		switch {
		case m.exists(op.Path):
			return int(syscall.EEXIST)
		case !m.Dirs[path.Dir(op.Path)]:
			return int(syscall.ENOENT)
		}
	case "write", "truncate", "unlink":
		switch {
		case m.Dirs[op.Path]:
			return int(syscall.EISDIR)
		case op.Kind == "unlink" && m.Links[op.Path] != "":
			return 0
		case f == nil:
			return int(syscall.ENOENT)
		}
	case "rmdir":
		switch {
		case f != nil:
			return int(syscall.ENOTDIR)
		case !m.Dirs[op.Path]:
			return int(syscall.ENOENT)
		case m.hasChildren(op.Path):
			return int(syscall.ENOTEMPTY)
		}
	case "rename":
		return m.expectRename(op.Path, op.Path2)
	case "snapshot":
		switch {
		case f == nil:
			return int(syscall.ENOENT)
		case m.Snaps[op.Name] != nil:
			return int(syscall.EEXIST)
		}
	case "link":
		switch {
		case f == nil:
			return int(syscall.ENOENT)
		case m.exists(op.Path2):
			return int(syscall.EEXIST)
		case !m.Dirs[path.Dir(op.Path2)]:
			return int(syscall.ENOENT)
		case projectOf(op.Path) != projectOf(path.Dir(op.Path2)):
			return int(syscall.EXDEV)
		}
	case "clone", "snapclone":
		switch {
		case f == nil:
			return int(syscall.ENOENT)
		case op.Kind == "snapclone" && (m.Snaps[op.Name] == nil || m.Snaps[op.Name].Owner != f):
			return int(syscall.ENOENT)
		case m.exists(op.Path2):
			return int(syscall.EEXIST)
		case projectOf(op.Path) != "":
			return int(syscall.EXDEV)
		}
	case "snapdelete":
		if f == nil || m.Snaps[op.Name] == nil || m.Snaps[op.Name].Owner != f {
			return int(syscall.ENOENT)
		}
	case "orphandelete":
		switch s := m.Snaps[op.Name]; {
		case s == nil:
			return int(syscall.ENOENT)
		case s.Owner != nil:
			return int(syscall.EBUSY)
		}
	}
	return 0
}

// apply changes the model as a successful operation changed the filesystem.
func (m *Model) apply(op *Op) {
	f := m.Files[op.Path]
	switch op.Kind {
	case "create":
		m.Files[op.Path] = &File{Snaps: map[string]bool{}}
	case "write":
		n := op.Len
		if op.Got == int(syscall.EDQUOT) {
			n = op.Wrote
		}
		if n == 0 {
			return
		}
		data := genData(op.Seed, n)
		if end := op.Off + n; end > int64(len(f.Data)) {
			f.Data = append(f.Data, make([]byte, end-int64(len(f.Data)))...)
		}
		copy(f.Data[op.Off:], data)
	case "truncate":
		if op.Len < int64(len(f.Data)) {
			f.Data = f.Data[:op.Len]
		} else {
			f.Data = append(f.Data, make([]byte, op.Len-int64(len(f.Data)))...)
		}
	case "symlink":
		m.Links[op.Path] = op.Path2
	case "link":
		m.Files[op.Path2] = f
	case "unlink":
		if m.Links[op.Path] != "" {
			delete(m.Links, op.Path)
			return
		}
		delete(m.Files, op.Path)
		if !m.named(f) {
			for name := range f.Snaps {
				m.Snaps[name].Owner = nil
			}
		}
	case "mkdir":
		m.Dirs[op.Path] = true
	case "rmdir":
		delete(m.Dirs, op.Path)
	case "rename":
		m.applyRename(op.Path, op.Path2)
	case "snapshot":
		m.Snaps[op.Name] = &Snap{Data: bytes.Clone(f.Data), Owner: f}
		f.Snaps[op.Name] = true
	case "clone":
		m.Files[op.Path2] = &File{Data: bytes.Clone(f.Data), Snaps: map[string]bool{}}
	case "snapclone":
		m.Files[op.Path2] = &File{Data: bytes.Clone(m.Snaps[op.Name].Data), Snaps: map[string]bool{}}
	case "snapdelete":
		delete(m.Snaps, op.Name)
		delete(f.Snaps, op.Name)
	case "orphandelete":
		delete(m.Snaps, op.Name)
	}
}

// blocks estimates the MB the model holds, counting each snapshot as a full copy.
func (m *Model) blocks() int64 {
	var n int64
	counted := map[*File]bool{}
	for _, f := range m.Files {
		if !counted[f] {
			counted[f] = true
			n += (int64(len(f.Data)) + blockSize - 1) / blockSize
		}
	}
	for _, s := range m.Snaps {
		n += (int64(len(s.Data)) + blockSize - 1) / blockSize
	}
	for _, target := range m.Links {
		if len(target) > maxInlineTarget {
			n++
		}
	}
	return n
}

// lane generates operations on its own files, directories and snapshots, whose names start with its prefix.
type lane struct {
	id     int
	rng    *rand.Rand
	next   int
	budget int64
}

func (l *lane) prefix() string { return fmt.Sprintf("l%d-", l.id) }

func (l *lane) name(kind string) string {
	l.next++
	return fmt.Sprintf("%s%s%d", l.prefix(), kind, l.next)
}

func (l *lane) owns(p string) bool { return strings.HasPrefix(path.Base(p), l.prefix()) }

func pick[T any](r *rand.Rand, xs []T) T { return xs[r.IntN(len(xs))] }

// generate picks the lane's next operation from the model's current state. The caller holds m.mu.
func (l *lane) generate(m *Model) *Op {
	var files, dirs, snaps, links, orphans []string
	for p := range m.Files {
		if l.owns(p) {
			files = append(files, p)
		}
	}
	for p := range m.Links {
		if l.owns(p) {
			links = append(links, p)
		}
	}
	for p := range m.Dirs {
		if l.owns(p) {
			dirs = append(dirs, p)
		}
	}
	for name, s := range m.Snaps {
		switch {
		case !l.owns(name):
		case s.Owner != nil:
			snaps = append(snaps, name)
		default:
			orphans = append(orphans, name)
		}
	}
	slices.Sort(files)
	slices.Sort(orphans)
	slices.Sort(links)
	slices.Sort(dirs)
	slices.Sort(snaps)
	parents := append(slices.Clone(sharedDirs), dirs...)
	full := m.blocks() > l.budget
	r := l.rng

	weights := []struct {
		kind string
		w    int
	}{
		{"create", 15}, {"write", 30}, {"truncate", 8}, {"unlink", 8}, {"mkdir", 4}, {"rmdir", 4},
		{"rename", 6}, {"snapshot", 6}, {"clone", 3}, {"snapclone", 3}, {"snapdelete", 6}, {"invalid", 3}, {"symlink", 5},
		{"link", 4}, {"orphandelete", 3},
	}
	total := 0
	for _, w := range weights {
		total += w.w
	}
	n := r.IntN(total)
	kind := ""
	for _, w := range weights {
		if n -= w.w; n < 0 {
			kind = w.kind
			break
		}
	}
	if full && slices.Contains([]string{"create", "write", "snapshot", "clone", "snapclone", "symlink"}, kind) {
		kind = pick(r, []string{"unlink", "truncate", "snapdelete"})
	}
	if len(files) == 0 || len(files) < 6 && kind != "create" && kind != "mkdir" {
		kind = "create"
	}
	if len(files) > 40 && kind == "create" {
		kind = "unlink"
	}

	op := &Op{Lane: l.id, Kind: kind}
	switch kind {
	case "create":
		op.Path = path.Join(pick(r, parents), l.name("f"))
	case "link":
		op.Path, op.Path2 = pick(r, files), path.Join(pick(r, parents), l.name("hl"))
	case "write":
		op.Path = pick(r, files)
		size := int64(len(m.Files[op.Path].Data))
		op.Len = pickLen(r)
		switch v := r.IntN(10); {
		case v < 5 && size > 0:
			op.Off = r.Int64N(size)
		case v < 8:
			op.Off = size
		default:
			op.Off = size + r.Int64N(2*blockSize)
		}
		if op.Off+op.Len > maxFile {
			op.Off, op.Len = 0, min(op.Len, 64<<10)
		}
		op.Seed = r.Uint64()
	case "truncate":
		op.Path = pick(r, files)
		size := int64(len(m.Files[op.Path].Data))
		if r.IntN(2) == 0 || full {
			op.Len = r.Int64N(size + 1)
		} else {
			op.Len = min(size+r.Int64N(blockSize), maxFile)
		}
	case "symlink":
		if len(links) >= 20 {
			op.Kind, op.Path = "unlink", pick(r, links)
			break
		}
		op.Path, op.Path2 = path.Join(pick(r, parents), l.name("ln")), l.target(r, files)
	case "unlink":
		if len(links) > 0 && r.IntN(4) == 0 {
			op.Path = pick(r, links)
			break
		}
		op.Path = pick(r, files)
		if f := m.Files[op.Path]; len(f.Snaps) > 0 && (len(orphans) >= 20 || r.IntN(10) > 0) {
			op.Kind, op.Name = "snapdelete", pickKey(r, f.Snaps)
		}
	case "mkdir":
		if len(dirs) >= 6 {
			op.Kind, op.Path = "rmdir", pick(r, dirs)
		} else {
			op.Path = path.Join(pick(r, sharedDirs), l.name("d"))
		}
	case "rmdir":
		if len(dirs) == 0 {
			op.Kind, op.Path = "mkdir", path.Join(pick(r, sharedDirs), l.name("d"))
			break
		}
		op.Path = pick(r, dirs)
		if m.hasChildren(op.Path) && r.IntN(2) == 0 {
			for _, p := range files {
				if path.Dir(p) == op.Path {
					op.Kind, op.Path = "unlink", p
					break
				}
			}
		}
	case "rename":
		op.Path, op.Path2 = l.renamePaths(r, files, links, dirs, parents)
	case "snapshot":
		op.Path, op.Name = pick(r, files), l.name("s")
	case "orphandelete":
		// Like an operator, leave a few orphans and delete the rest through the API.
		if len(orphans) < 5 {
			op.Kind, op.Path, op.Name = "snapshot", pick(r, files), l.name("s")
			break
		}
		op.Name = pick(r, orphans)
	case "clone":
		op.Path, op.Path2 = pick(r, files), "/"+l.name("c")
	case "snapclone", "snapdelete":
		if len(snaps) == 0 {
			op.Kind, op.Path, op.Name = "snapshot", pick(r, files), l.name("s")
			break
		}
		op.Name = pick(r, snaps)
		op.Path = m.pathOf(m.Snaps[op.Name].Owner)
		if kind == "snapclone" {
			op.Path2 = "/" + l.name("c")
		}
	case "invalid":
		switch r.IntN(3) {
		case 0:
			op.Kind, op.Path = "create", pick(r, files)
		case 1:
			op.Kind, op.Path, op.Off, op.Len, op.Seed = "write", path.Join(pick(r, parents), l.name("missing")), 0, 4096, r.Uint64()
		default:
			op.Kind, op.Path, op.Name, op.Path2 = "snapclone", pick(r, files), l.name("nosnap"), "/"+l.name("c")
		}
	}
	return op
}

// expectRename returns what rename(2) returns, in the order the kernel and goclusterfs check. goclusterfs refuses a
// move between projects after the kernel's checks and before it looks at the target.
func (m *Model) expectRename(src, dst string) int {
	srcDir, dstThere := m.Dirs[src], m.exists(dst)
	switch {
	case !m.exists(src) || !m.Dirs[path.Dir(dst)]:
		return int(syscall.ENOENT)
	case src == dst, m.Files[src] != nil && m.Files[src] == m.Files[dst]:
		return 0
	case srcDir && strings.HasPrefix(dst, src+"/"):
		return int(syscall.EINVAL)
	case strings.HasPrefix(src, dst+"/"):
		return int(syscall.ENOTEMPTY)
	case dstThere && srcDir && !m.Dirs[dst]:
		return int(syscall.ENOTDIR)
	case dstThere && !srcDir && m.Dirs[dst]:
		return int(syscall.EISDIR)
	case path.Dir(src) != path.Dir(dst) && projectOf(src) != projectOf(path.Dir(dst)):
		return int(syscall.EXDEV)
	case dstThere && srcDir && m.hasChildren(dst):
		return int(syscall.ENOTEMPTY)
	}
	return 0
}

// applyRename moves src, and everything under it, to dst. A rename between two names of one file changes nothing. A
// file it replaces loses its snapshots with its last name, as unlink does.
func (m *Model) applyRename(src, dst string) {
	if src == dst || m.Files[src] != nil && m.Files[src] == m.Files[dst] {
		return
	}
	replaced := m.Files[dst]
	delete(m.Files, dst)
	delete(m.Links, dst)
	delete(m.Dirs, dst)
	moved := func(p string) (string, bool) {
		if p == src || strings.HasPrefix(p, src+"/") {
			return dst + strings.TrimPrefix(p, src), true
		}
		return "", false
	}
	for _, p := range slices.Collect(maps.Keys(m.Files)) {
		if q, ok := moved(p); ok {
			m.Files[q] = m.Files[p]
			delete(m.Files, p)
		}
	}
	for _, p := range slices.Collect(maps.Keys(m.Links)) {
		if q, ok := moved(p); ok {
			m.Links[q] = m.Links[p]
			delete(m.Links, p)
		}
	}
	for _, p := range slices.Collect(maps.Keys(m.Dirs)) {
		if q, ok := moved(p); ok {
			delete(m.Dirs, p)
			m.Dirs[q] = true
		}
	}
	if replaced != nil && !m.named(replaced) {
		for name := range replaced.Snaps {
			m.Snaps[name].Owner = nil
		}
	}
}

// renamePaths picks a rename of one of the lane's files, links or directories. Most move to a new name; the rest
// replace another of the lane's entries, or fail: a missing source or parent, a type mismatch, a directory with
// entries, or a directory moved into itself. Directories stay directly under the shared directories, which only
// lanes' own directories sit in, so no lane moves another lane's entries.
func (l *lane) renamePaths(r *rand.Rand, files, links, dirs, parents []string) (string, string) {
	switch v := r.IntN(10); {
	case v < 2 && len(dirs) > 0:
		src := pick(r, dirs)
		switch r.IntN(5) {
		case 0:
			return src, path.Join(src, l.name("d"))
		case 1:
			return src, pick(r, dirs)
		case 2:
			return src, pick(r, files)
		}
		return src, path.Join(pick(r, sharedDirs), l.name("d"))
	case v == 2 && len(links) > 0:
		return pick(r, links), l.renameTarget(r, files, links, dirs, parents)
	case v == 3:
		return path.Join(pick(r, parents), l.name("missing")), path.Join(pick(r, parents), l.name("r"))
	}
	return pick(r, files), l.renameTarget(r, files, links, dirs, parents)
}

// renameTarget picks where a file or link goes: usually a new name, sometimes over the lane's own entries.
func (l *lane) renameTarget(r *rand.Rand, files, links, dirs, parents []string) string {
	switch r.IntN(8) {
	case 0:
		return pick(r, files)
	case 1:
		if len(dirs) > 0 {
			return pick(r, dirs)
		}
	case 2:
		if len(links) > 0 {
			return pick(r, links)
		}
	case 3:
		return path.Join(pick(r, sharedDirs), l.name("missing"), l.name("r"))
	}
	return path.Join(pick(r, parents), l.name("r"))
}

// target picks a symlink target in the lane's names: a lane file, a missing name, or one too long for the inode.
func (l *lane) target(r *rand.Rand, files []string) string {
	switch r.IntN(3) {
	case 0:
		return "../" + path.Base(pick(r, files))
	case 1:
		return l.name("missing")
	default:
		return strings.Repeat(l.prefix()+"x/", 20+r.IntN(600)) + l.name("t")
	}
}

func pickLen(r *rand.Rand) int64 {
	switch v := r.IntN(20); {
	case v < 14:
		return 1 + r.Int64N(16<<10)
	case v < 19:
		return 64<<10 + r.Int64N(blockSize)
	default:
		return blockSize + r.Int64N(2*blockSize)
	}
}

func pickKey(r *rand.Rand, m map[string]bool) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return pick(r, keys)
}

// pathOf returns the first of f's names in order; a hard link gives a file several.
func (m *Model) pathOf(f *File) string {
	first := ""
	for p, g := range m.Files {
		if g == f && (first == "" || p < first) {
			first = p
		}
	}
	return first
}

// snapsOf returns the names of the snapshots a node must list for the file at p.
func (m *Model) snapsOf(p string) []string {
	f := m.Files[p]
	if f == nil {
		return nil
	}
	names := make([]string, 0, len(f.Snaps))
	for name := range f.Snaps {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// listing returns what a node must list in dir, as the agent describes entries.
func (m *Model) listing(dir string) []string {
	var out []string
	for p, f := range m.Files {
		if path.Dir(p) == dir {
			out = append(out, fmt.Sprintf("f:%s:%d", path.Base(p), len(f.Data)))
		}
	}
	for p := range m.Dirs {
		if p != dir && path.Dir(p) == dir {
			out = append(out, "d:"+path.Base(p))
		}
	}
	for p, target := range m.Links {
		if path.Dir(p) == dir {
			out = append(out, "l:"+path.Base(p)+":"+target)
		}
	}
	slices.Sort(out)
	return out
}
